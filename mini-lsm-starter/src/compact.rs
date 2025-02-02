#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }
            iter.next()?;

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // lock dropped here
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                // gather all l0 and l1 ssts which we will simply write out as the new l1
                let iter = {
                    let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                    for id in l0_sstables.iter() {
                        l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(id).unwrap().clone(),
                        )?));
                    }
                    let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                    for id in l1_sstables.iter() {
                        l1_iters.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    // as usual, l0 gets more priority
                    // than l1 if a key is repeated.
                    // And the more recent occurance of the same
                    // key in l0 will have further preference.
                    // note that key wont be repeated in l1.
                    TwoMergeIterator::create(
                        MergeIterator::create(l0_iters),
                        SstConcatIterator::create_and_seek_to_first(l1_iters)?,
                    )?
                };
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    // These involve L1 and below- keys are already sorted in ssts
                    // due to a previous compact- so we'll just use ConcatIterators
                    let upper_ssts = upper_level_sst_ids
                        .iter()
                        .map(|sst_id| snapshot.sstables.get(sst_id).unwrap())
                        .cloned()
                        .collect();
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let lower_ssts = lower_level_sst_ids
                        .iter()
                        .map(|sst_id| snapshot.sstables.get(sst_id).unwrap())
                        .cloned()
                        .collect();
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
                None => {
                    // Upper level is l0, so we can't use a ConcatIterator! We need to check every sst
                    // for a key- so that the most recent update wins!!
                    let l0_ssts_iters = upper_level_sst_ids
                        .iter()
                        .map(|sst_id| snapshot.sstables.get(sst_id).unwrap())
                        .cloned()
                        .map(|sst| SsTableIterator::create_and_seek_to_first(sst).map(Box::new))
                        .collect::<Result<_, _>>()?;
                    let upper_iter = MergeIterator::create(l0_ssts_iters);

                    let lower_ssts = lower_level_sst_ids
                        .iter()
                        .map(|sst_id| snapshot.sstables.get(sst_id).unwrap())
                        .cloned()
                        .collect();
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;

                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            },
            _ => unimplemented!(),
        }
    }

    fn finalize_sst(&self, builder: SsTableBuilder) -> Result<Arc<SsTable>> {
        let claimed_sst_id = self.next_sst_id();
        builder
            .build(
                claimed_sst_id,
                Some(Arc::clone(&self.block_cache)),
                self.path_of_sst(claimed_sst_id),
            )
            .map(Arc::new)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // perform expensive compaction work outside the read lock
        let (l0_sstables, l1_sstables) = {
            let state = {
                let state = self.state.read();
                Arc::clone(&state)
            };
            // we only handle having 1 level for now..
            assert!(state.levels.len() == 1);
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };

        let sst_ids_to_delete = l1_sstables.clone().into_iter().chain(l0_sstables.clone());

        let compacted = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        })?;
        let compacted_sst_ids: Vec<_> = compacted.iter().map(|s| s.sst_id()).collect();

        // we need this to be atomic- as we are going to replace the l0 and l1 sstables entirely
        let files_to_delete = {
            let mut guard = self.state.write();
            let _state_lock = self.state_lock.lock();

            let mut new_state = guard.as_ref().clone();
            new_state.l0_sstables.clear();
            // remember, we only have 1 level
            new_state.levels[0] = (1, compacted_sst_ids);

            let mut files_to_delete = Vec::new();
            sst_ids_to_delete.for_each(|sst_id| {
                new_state.sstables.remove(&sst_id);
                files_to_delete.push(self.path_of_sst(sst_id));
            });

            // now, insert the new sstables
            compacted.into_iter().for_each(|sst| {
                new_state.sstables.insert(sst.sst_id(), sst);
            });

            *guard = Arc::new(new_state);

            files_to_delete
        };

        // delete files outside of the state lock
        //
        // THIS IS WRONG? SOMEONE COULD HAVE an Arc(SST) and try to read it?
        // do need a Drop handler?
        files_to_delete
            .into_iter()
            .for_each(|path| _ = std::fs::remove_file(path));

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        println!("running compaction task: {:?}", task);
        let compacted_new_level = self.compact(&task)?;
        let compacted_new_level_len = compacted_new_level.len();
        let compacted_new_level_sst_ids = compacted_new_level
            .iter()
            .map(|x| x.sst_id())
            .collect::<Vec<_>>();

        let ssts_to_remove = {
            let _state_lock = self.state_lock.lock();
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&self.state.read(), &task, &compacted_new_level_sst_ids, false);
            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some());
                ssts_to_remove.push(result.unwrap());
            }
            let mut new_sst_ids = Vec::new();
            for file_to_add in compacted_new_level {
                new_sst_ids.push(file_to_add.sst_id());
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
                assert!(result.is_none());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            ssts_to_remove
        };
        println!(
            "compaction finished: {} files removed, {} files added",
            ssts_to_remove.len(),
            compacted_new_level_len
        );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let should_force = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if should_force {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
