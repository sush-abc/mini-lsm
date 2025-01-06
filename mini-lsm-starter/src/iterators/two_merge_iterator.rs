#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    use_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(mut a: A, mut b: B) -> Result<Self> {
        // - Remember that each key is strictly increasing in each of the iterators.
        // - This helps us maintain the invarient we'll maintain with self.a and self.b
        // at any given time, the key() of one of them will be lower than the other-
        // they will **never** be equal.
        // both of these will make the implementation here easier.

        // this enforces the invarient- if a.key() == b.key(), skip b
        // this will give precedence to a.
        Self::skip_b_maybe(&mut a, &mut b)?;

        // now we'll have to decide which iterator to use first
        // it's still possible we use b- if we didn't skip it
        // ealier, and if it has a lower key
        let use_a = Self::should_use_a(&a, &b);
        Ok(Self { a, b, use_a })
    }

    fn skip_b_maybe(a: &mut A, b: &mut B) -> Result<()> {
        if !a.is_valid() {
            return Ok(());
        }
        if !b.is_valid() {
            return Ok(());
        }
        if a.key() == b.key() {
            b.next()?;
        }
        Ok(())
    }

    fn should_use_a(a: &A, b: &B) -> bool {
        if !a.is_valid() {
            return false;
        }
        if !b.is_valid() {
            return true;
        }
        assert!(a.key() != b.key(), "a.key() == b.key()- invarient violated");
        a.key() < b.key()
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        if self.use_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.use_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        if self.use_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }

        Self::skip_b_maybe(&mut self.a, &mut self.b)?;

        self.use_a = Self::should_use_a(&self.a, &self.b);
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.a.num_active_iterators() + self.b.num_active_iterators()
    }
}
