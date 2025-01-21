#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
///
/// ASSUMES each SsTable is valid (i.e. contains at least one item) as an optimization
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let Some(first_sst) = sstables.first() else {
            return Ok(Self {
                current: None,
                next_sst_idx: 0,
                sstables,
            });
        };
        let iter = SsTableIterator::create_and_seek_to_first(Arc::clone(first_sst))?;
        // we assume that each SsTable is valid and has at least one item
        assert!(iter.is_valid());

        Ok(Self {
            current: Some(iter),
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, target: KeySlice) -> Result<Self> {
        // binary search for the first sstable that may contain the target key
        let first_sst_idx = {
            let (mut lo, mut hi) = (0, sstables.len());
            while lo < hi {
                let mi = lo + (hi - lo) / 2;
                let sst = &sstables[mi];
                // BINARY CONDITION: last key is >= target key this will skip over all the sstables
                // that we know for sure do not contain the target key.
                // test cases:
                // [2, 30], [35, 35], [41, 60]
                // and target = 1 -> [2, 30]
                // and target = 31 -> [35, 35]
                // and target = 35 -> [35, 35]
                // and target = 36 -> [41, 60]
                // and target = 18 -> [2, 30]
                #[allow(clippy::nonminimal_bool)]
                if !(sst.last_key().raw_ref() >= target.raw_ref()) {
                    lo = mi + 1;
                } else {
                    hi = mi;
                }
            }
            hi
        };

        if first_sst_idx == sstables.len() {
            return Ok(Self {
                current: None,
                next_sst_idx: sstables.len(),
                sstables,
            });
        }

        let iter =
            SsTableIterator::create_and_seek_to_key(Arc::clone(&sstables[first_sst_idx]), target)?;

        // we assume that each SsTable is valid and has at least one item
        assert!(iter.is_valid());
        Ok(Self {
            current: Some(iter),
            next_sst_idx: first_sst_idx + 1,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().expect("invalid iterator").key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().expect("invalid iterator").value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some() || self.next_sst_idx < self.sstables.len()
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().expect("invalid iterator");
        current.next()?;
        if !current.is_valid() {
            if self.next_sst_idx == self.sstables.len() {
                self.current = None; // makes `is_valid()` return false
            } else {
                self.current = Some(SsTableIterator::create_and_seek_to_first(Arc::clone(
                    &self.sstables[self.next_sst_idx],
                ))?);
            }
            self.next_sst_idx += 1;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
