use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
/// SimpleLeveledCompactionTask represents the work to
/// compact two adjacent levels (which would empty the lower
/// and replace the higher.). We only compact two levels
/// at a given time.
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let options = &self.options;

        // l0 is special- we compact just based on the number of ssts
        {
            // level0_file_num_compaction_trigger: when the number of SSTs in L0 is
            // larger than or equal to this number, trigger a compaction of L0 and L1.
            if snapshot.l0_sstables.len() >= options.level0_file_num_compaction_trigger {
                return Some(self.compact_l0_task(snapshot));
            }
        }

        // l0 doesn't need compacting.. how about the rest of the levels?
        for (idx, (lower_lvl, lower_lvl_ssts)) in snapshot.levels.iter().enumerate() {
            if idx == 0 {
                continue;
            }
            let (upper_lvl, upper_lvl_ssts) = &snapshot.levels[idx - 1];
            if !self.exceeds_size_ratio(upper_lvl_ssts, lower_lvl_ssts) {
                continue;
            }
            return Some(self.compact_l1_or_lower_level_task(
                upper_lvl,
                upper_lvl_ssts,
                lower_lvl,
                lower_lvl_ssts,
            ));
        }
        None
    }

    fn compact_l0_task(&self, snapshot: &LsmStorageState) -> SimpleLeveledCompactionTask {
        assert!(self.options.max_levels >= 1);
        let upper_level_sst_ids = snapshot.l0_sstables.clone();
        let lower_level_sst_ids = snapshot.levels[0].1.clone();
        SimpleLeveledCompactionTask {
            upper_level: None,
            upper_level_sst_ids,
            lower_level: 1,
            lower_level_sst_ids,
            is_lower_level_bottom_level: snapshot.levels.len() == 1,
        }
    }
    fn compact_l1_or_lower_level_task(
        &self,
        upper_lvl: &usize,
        upper_lvl_sst_ids: &Vec<usize>,
        lower_lvl: &usize,
        lower_level_sst_ids: &Vec<usize>,
    ) -> SimpleLeveledCompactionTask {
        SimpleLeveledCompactionTask {
            upper_level: Some(*upper_lvl),
            upper_level_sst_ids: upper_lvl_sst_ids.clone(),
            lower_level: *lower_lvl,
            lower_level_sst_ids: lower_level_sst_ids.clone(),
            is_lower_level_bottom_level: *lower_lvl == self.options.max_levels,
        }
    }

    fn exceeds_size_ratio(&self, upper_lvl_ssts: &[usize], lower_lvl_ssts: &[usize]) -> bool {
        if upper_lvl_ssts.is_empty() {
            // nothing to compact here
            return false;
        }
        let ratio = lower_lvl_ssts.len() as f64 / upper_lvl_ssts.len() as f64;
        (ratio * 100.0) < (self.options.size_ratio_percent as f64)
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        compated_new_sst_ids: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        if let Some(upper_level) = task.upper_level {
            assert_eq!(
                task.upper_level_sst_ids,
                snapshot.levels[upper_level - 1].1,
                "sst mismatched"
            );
            files_to_remove.extend(&snapshot.levels[upper_level - 1].1);
            snapshot.levels[upper_level - 1].1.clear();
        } else {
            assert_eq!(
                task.upper_level_sst_ids, snapshot.l0_sstables,
                "sst mismatched"
            );
            files_to_remove.extend(&snapshot.l0_sstables);
            snapshot.l0_sstables.clear();
        }
        assert_eq!(
            task.lower_level_sst_ids,
            snapshot.levels[task.lower_level - 1].1,
            "sst mismatched"
        );
        files_to_remove.extend(&snapshot.levels[task.lower_level - 1].1);
        snapshot.levels[task.lower_level - 1].1 = compated_new_sst_ids.to_vec();
        (snapshot, files_to_remove)
    }
}
