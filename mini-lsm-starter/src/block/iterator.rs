#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let first_key = Self::first_key(&block);
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key,
        }
    }

    fn first_key(block: &Block) -> KeyVec {
        if block.offsets.is_empty() {
            return KeyVec::new();
        }
        // first two bytes contain the len fo the first key
        let key_len = u16::from_le_bytes([block.data[0], block.data[1]]) as usize;
        KeyVec::from_vec(block.data[2..2 + key_len].to_vec())
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    fn seek_to_idx(&mut self, idx: usize) {
        self.idx = idx;
        if self.block.offsets.is_empty() || idx >= self.block.offsets.len() {
            // invalid block.. or idx out of range, set key to empty
            // this will make the iterator invalid
            self.key = KeyVec::new();
            return;
        }
        let entry_start = self.block.offsets[idx] as usize;
        // extract key
        let key_len = u16::from_le_bytes([
            self.block.data[entry_start],
            self.block.data[entry_start + 1],
        ]) as usize;
        let key_start = entry_start + 2;
        let key_end = key_start + key_len;

        let key_bytes = &self.block.data[key_start..key_end];
        self.key = KeyVec::from_vec(key_bytes.to_vec());

        // extract value range
        let val_len =
            u16::from_le_bytes([self.block.data[key_end], self.block.data[key_end + 1]]) as usize;

        let val_start = key_end + 2;
        let val_end = val_start + val_len;
        self.value_range = (val_start, val_end);
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to_idx(0)
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_to_idx(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, target: KeySlice) {
        fn binary_function(key: &[u8], target: &[u8]) -> bool {
            key >= target
        }

        let (mut lo, mut hi) = (0, self.block.offsets.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            self.seek_to_idx(mid);
            if !binary_function(self.key.raw_ref(), target.raw_ref()) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.seek_to_idx(hi);
    }
}
