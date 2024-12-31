#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Bytes, BytesMut};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        /*
        ----------------------------------------------------------------------------------------------------
        |             Data Section             |              Offset Section             |      Extra      |
        ----------------------------------------------------------------------------------------------------
        | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
        ----------------------------------------------------------------------------------------------------
        */
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&self.data);

        for offsert in &self.offsets {
            buf.extend_from_slice(&offsert.to_le_bytes());
        }

        let num_of_elements = self.offsets.len() as u16;
        buf.extend_from_slice(&num_of_elements.to_le_bytes());

        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(buf: &[u8]) -> Self {
        // 1) Last 2 bytes → number of elements
        let num_elements = u16::from_le_bytes([buf[buf.len() - 2], buf[buf.len() - 1]]) as usize;

        // 2) The offset section sits right before those 2 bytes
        //    and is 2 bytes per offset entry.
        let offset_section_start = buf.len() - 2 - 2 * num_elements;
        let offset_section_end = buf.len() - 2; // exclude trailer

        // 3) Everything before offset_section_start is the "data section."
        let data_section = &buf[..offset_section_start];

        // 4) Parse offsets in the same order they were written
        let offset_slice = &buf[offset_section_start..offset_section_end];
        let mut offsets = Vec::with_capacity(num_elements);
        for i in 0..num_elements {
            let start = i * 2;
            let end = start + 2;
            let offset = u16::from_le_bytes([offset_slice[start], offset_slice[start + 1]]);
            offsets.push(offset);
        }

        // 5) Put everything into a Block
        Self {
            data: data_section.to_vec(), // store data section as Vec<u8>
            offsets,
        }
    }
}
