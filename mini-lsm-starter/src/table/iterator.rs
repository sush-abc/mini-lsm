use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        Self::create_and_seek_to_block(table, 0)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let table = self.table.clone();
        let _ = std::mem::replace(self, Self::create_and_seek_to_first(table)?);
        Ok(())
    }

    fn create_and_seek_to_block(table: Arc<SsTable>, blk_idx: usize) -> Result<Self> {
        assert!(blk_idx < table.block_meta.len());
        let blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(blk_idx)?);
        Ok(Self {
            table,
            blk_iter,
            blk_idx,
        })
    }

    fn create_and_seek_inside_block(
        table: Arc<SsTable>,
        blk_idx: usize,
        key: KeySlice,
    ) -> Result<Self> {
        let mut self_sst_iter = Self::create_and_seek_to_block(table, blk_idx)?;
        self_sst_iter.blk_iter.seek_to_key(key);
        Ok(self_sst_iter)
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, target: KeySlice) -> Result<Self> {
        /*
            -------------------------------
            | block 1 | block 2 | block 3 |
            -------------------------------
            | a, b, c | e, p, q | x, y    |
            --------------------------------
        keys are unique, and sorted in ascending order in each block
          - keys are unique as each SST started with 1 memtable
          - sorted cause we built them that way
          - sorted between blocks too- as we once again added each key to
            the SST in order, and built the blocks in order

        binary condition: target <= block.last_key()

        test cases: first, last and existing keys
               T  T  T
        a ->   b1 b2 b3

               T  T  T
        b ->   b1 b2 b3

               T  T  T
        c ->   b1 b2 b3

        test case: after last but before start of next
               F  T  T
        d ->   b1 b2 b3

        test case: missing in the middle
               F  T  T
        f ->   b1 b2 b3

        test case: after everything  --> NEEDS SPECIAL HANDLING!
               F  F  F
        z ->   b1 b2 b3
         */

        let (mut lo, mut hi) = (0, table.block_meta.len());
        while lo < hi {
            let mi = lo + (hi - lo) / 2;
            let mi_blk_meta = &table.block_meta[mi];
            if !(target <= mi_blk_meta.last_key.as_key_slice()) {
                lo = mi + 1;
            } else {
                hi = mi;
            }
        }

        if hi == table.block_meta.len() {
            // special handling to mark iterator as invalid- seeking to
            // a key that doesn't exist in the block marks it as invalid
            Self::create_and_seek_inside_block(table, hi - 1, target)
        } else {
            Self::create_and_seek_inside_block(table, hi, target)
        }
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let table = self.table.clone();
        let _ = std::mem::replace(self, Self::create_and_seek_to_key(table, key)?);
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx == self.table.block_meta.len() {
                // we've reached the end of the SSTable
                return Ok(());
            }
            // seek to the the next block
            self.blk_iter = BlockIterator::create_and_seek_to_first(
                self.table.read_block_cached(self.blk_idx)?,
            );
        }

        Ok(())
    }
}
