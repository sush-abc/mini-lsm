#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod
use std::cmp::{self};
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::{Key, KeySlice};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            // THIS MAKES THE HEAP A MIN HEAP!!!
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();

        // Push all valid iterators into the heap
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }

        // Pop the smallest key to become our current
        let current = heap.pop();
        Self {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().expect("invalid iterator").1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().expect("invalid iterator").1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        let mut prev_current = self.current.take().expect("invalid iterator");
        let prev_key = prev_current.1.key().into_inner().to_vec();

        // consume `current` which we already returned,
        // and then push the iterator back into the heap
        prev_current.1.next()?;
        if prev_current.1.is_valid() {
            self.iters.push(prev_current);
        }
        while let Some(mut iter) = self.iters.pop() {
            if iter.1.key() == Key::from_slice(&prev_key) {
                // we just returned this key, skip it
                // as we returned the more recent value
                // from the iterator with the smaller idx
                iter.1.next()?;
                if iter.1.is_valid() {
                    self.iters.push(iter);
                }
                continue;
            } else {
                self.current = Some(iter);
                break;
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|heap_wrapper| heap_wrapper.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|c| c.1.num_active_iterators())
                .unwrap_or(0)
    }
}
