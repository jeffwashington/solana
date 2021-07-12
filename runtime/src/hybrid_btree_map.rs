use solana_sdk::pubkey::Pubkey;
use std::borrow::Borrow;
use std::collections::btree_map::{BTreeMap, Entry, Keys, Values};
type K = Pubkey;

#[derive(Debug)]
pub struct HybridBTreeMap<V> {
    in_memory: BTreeMap<K, V>,
}

impl<V> Default for HybridBTreeMap<V> {
    /// Creates an empty `BTreeMap`.
    fn default() -> HybridBTreeMap<V> {
        Self {
            in_memory: BTreeMap::<K, V>::default(),
        }
    }
}

/*
impl<'a, K: 'a, V: 'a> Iterator for HybridBTreeMap<'a, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if self.length == 0 {
            None
        } else {
            self.length -= 1;
            Some(unsafe { self.range.inner.next_unchecked() })
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.length, Some(self.length))
    }

    fn last(mut self) -> Option<(&'a K, &'a V)> {
        self.next_back()
    }

    fn min(mut self) -> Option<(&'a K, &'a V)> {
        self.next()
    }

    fn max(mut self) -> Option<(&'a K, &'a V)> {
        self.next_back()
    }
}
*/

impl<V> HybridBTreeMap<V> {
    pub fn keys(&self) -> Keys<'_, K, V> {
        panic!("todo");
        self.in_memory.keys()
    }
    pub fn values(&self) -> Values<'_, K, V> {
        panic!("todo");
        self.in_memory.values()
    }
    pub fn len(&self) -> usize {
        panic!("todo");
        self.in_memory.len()
    }
    pub fn entry(&mut self, key: K) -> Entry<'_, K, V>
    {
        self.in_memory.entry(key)
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        self.in_memory.get(key)
    }
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        self.in_memory.remove(key)
    }
}
