use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{bloom::Bloom, BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
#[derive(Default)]
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    key_hashes: Vec<u32>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

const FPR: f64 = 0.01;

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            builder: BlockBuilder::new(block_size),
            ..Default::default()
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.to_key_vec().into_inner();
        }

        self.key_hashes.push(farmhash::fingerprint32(key.raw_ref()));

        if self.builder.add(key, value) {
            self.last_key = key.to_key_vec().into_inner();
            return;
        }

        // create a new block builder and append block data
        self.finish_block();

        // add the key-value pair to the next block
        assert!(self.builder.add(key, value));
        self.first_key = key.to_key_vec().into_inner();
        self.last_key = key.to_key_vec().into_inner();
    }

    fn finish_block(&mut self) {
        let builder = std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        let encoded_block = builder.build().encode();
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.first_key))),
            last_key: KeyBytes::from_bytes(Bytes::from(std::mem::take(&mut self.last_key))),
        });
        self.data.extend(encoded_block);
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        use bytes::BufMut;

        self.finish_block();

        /*
        -----------------------------------------------------------------------------------------------------
        |         Block Section         |                            Meta Section                           |
        -----------------------------------------------------------------------------------------------------
        | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
        |                               |  varlen  |         u32       |    varlen    |        u32          |
        -----------------------------------------------------------------------------------------------------
         */

        let bloom = self.create_bloom_filter();

        let mut buf = self.data;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);

        let bloom_offset = buf.len();
        bloom.encode(&mut buf);
        buf.put_u32(bloom_offset as u32);

        let file = FileObject::create(path.as_ref(), buf)?;
        Ok(SsTable {
            id,
            file,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            block_meta_offset: meta_offset,
            block_cache,
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    fn create_bloom_filter(&self) -> Bloom {
        assert!(!self.key_hashes.is_empty());

        let bits_per_key = Bloom::bloom_bits_per_key(self.key_hashes.len(), FPR);
        Bloom::build_from_key_hashes(&self.key_hashes, bits_per_key)
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
