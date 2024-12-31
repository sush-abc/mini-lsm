#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
#[derive(Default)]
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            ..Default::default()
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full
    /// (does not add the key-value pair if false was returned).
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // special handling for the first key for some reason..
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec();
        }

        if !self.data.is_empty() && self.will_exceed_block_size(key, value) {
            return false; // indicate that the key-value pair was not added
        }

        /*
        ----------------------------------------------------------------------------------------------------
        |             Data Section             |              Offset Section             |      Extra      |
        ----------------------------------------------------------------------------------------------------
        | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
        ----------------------------------------------------------------------------------------------------
         */

        self.offsets.push(self.data.len() as u16);
        /*
        -----------------------------------------------------------------------
        |                           Entry #1                            | ... |
        -----------------------------------------------------------------------
        | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
        -----------------------------------------------------------------------
         */

        // key_len (2B)
        self.data
            .extend_from_slice(&(key.len() as u16).to_le_bytes());
        // key
        self.data.extend_from_slice(key.raw_ref());
        // value_len (2B)
        self.data
            .extend_from_slice(&(value.len() as u16).to_le_bytes());
        // value
        self.data.extend_from_slice(value);

        true // indicate that the key-value pair was added
    }

    fn will_exceed_block_size(&self, key: KeySlice, value: &[u8]) -> bool {
        // 2B for key_len, 2B for value_len, 2B for offset
        let expected_incr_size = 2 + key.len() + 2 + value.len() + 2;

        let trailer_len = 2; // 2B for num_of_elements
        let current_size = self.data.len() + self.offsets.len() * 2 + trailer_len;
        current_size + expected_incr_size > self.block_size
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }
}
