#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::StorageIterator;
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
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut merge_iter = {
                    let iters = {
                        let state = { self.state.read().clone() };
                        l0_sstables
                            .iter()
                            .chain(l1_sstables)
                            .map(|sst_id| state.sstables[sst_id].clone())
                            .map(|sst| SsTableIterator::create_and_seek_to_first(sst).map(Box::new))
                            .collect::<Result<Vec<_>>>()?
                    };

                    MergeIterator::create(iters)
                };

                let mut compacted = Vec::new();

                let mut builder = SsTableBuilder::new(self.options.block_size);
                while merge_iter.is_valid() {
                    if merge_iter.value().is_empty() {
                        // we can ignore the tombstone, we are performing a full compaction
                        merge_iter.next()?;
                        continue;
                    }
                    if !builder.is_empty()
                        && builder.estimated_size() >= self.options.target_sst_size
                    {
                        compacted.push(self.finalize_sst(builder)?);
                        builder = SsTableBuilder::new(self.options.block_size);
                    }
                    builder.add(merge_iter.key(), merge_iter.value());

                    merge_iter.next()?;
                }
                if !builder.is_empty() {
                    compacted.push(self.finalize_sst(builder)?);
                }

                Ok(compacted)
            }
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
        unimplemented!()
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
