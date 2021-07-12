use solana_sdk::pubkey::Pubkey;
use std::borrow::Borrow;
use std::collections::btree_map::{BTreeMap, Entry, Keys, Values, OccupiedEntry, VacantEntry};
use solana_bucket_map::bucket_map::BucketMap;
use crate::accounts_index::BINS;
use crate::pubkey_bins::PubkeyBinCalculator16;

use std::fmt::Debug;
type K = Pubkey;

#[derive(Clone, Debug)]
pub struct HybridAccountEntry<V: Clone + Debug> {
    entry: V,
    exists_on_disk: bool,
}
type V2<T: Clone + Debug> = HybridAccountEntry<T>;

#[derive(Debug)]
pub struct HybridBTreeMap<V: Clone + Debug> {
    in_memory: BTreeMap<K, HybridAccountEntry<V>>,
    disk: BucketMap<V>,
}

// TODO: we need a bit for 'exists on disk' for updates

impl<V: Clone + Debug> Default for HybridBTreeMap<V> {
    /// Creates an empty `BTreeMap`.
    fn default() -> HybridBTreeMap<V> {
        Self {
            in_memory: BTreeMap::default(),
            disk: BucketMap::new_buckets(PubkeyBinCalculator16::log_2(BINS as u32) as u8),
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

pub enum HybridEntry<'a, V: 'a + Clone + Debug> {
    /// A vacant entry.
    Vacant(HybridVacantEntry<'a, V>),

    /// An occupied entry.
    Occupied(HybridOccupiedEntry<'a, V>),
}

pub struct HybridOccupiedEntry<'a, V: 'a + Clone + Debug> {
    entry: OccupiedEntry<'a, K, V2<V>>,
}
pub struct HybridVacantEntry<'a, V: 'a + Clone + Debug> {
    entry: VacantEntry<'a, K, V2<V>>,
}

impl<'a, V: 'a + Clone + Debug> HybridOccupiedEntry<'a, V>
{
    pub fn get(&self) -> &V {
        &self.entry.get().entry
    }
    pub fn get_mut(&mut self) -> &mut V {
        &mut self.entry.get_mut().entry
    }
    pub fn key(&self) -> &K {
        self.entry.key()
    }
    pub fn remove(self) -> V {
        panic!("todo");
        // TODO: remember something that was deleted!?!?
        self.entry.remove().entry
    }
}

impl<'a, V: 'a + Clone + Debug> HybridVacantEntry<'a, V>
{
    pub fn insert(self, value: V) -> &'a mut V {
        let value = V2::<V> {
            entry: value,
            exists_on_disk: false,
        };
        &mut self.entry.insert(value).entry
    }
}


impl<V: Clone + Debug> HybridBTreeMap<V> {
    pub fn keys(&self) -> Keys<'_, K, V2<V>> {
        panic!("todo keys");
        //self.in_memory.keys()
    }
    pub fn values(&self) -> Values<'_, K, V> {
        panic!("todo values");
        //self.in_memory.values()
    }
    pub fn len_inaccurate(&self) -> usize {
        self.in_memory.len()
    }
    pub fn entry(&mut self, key: K) -> HybridEntry<'_, V>
    {
        match self.in_memory.entry(key) {
            Entry::Occupied(entry) => HybridEntry::Occupied(HybridOccupiedEntry {
                entry
            }),
            Entry::Vacant(entry) => HybridEntry::Vacant(HybridVacantEntry {
                entry
            }),
        }
    }

    pub fn get<Q: ?Sized>(&self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        self.in_memory.get(key).map(|x| &x.entry)
    }
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        panic!("todo");
        self.in_memory.remove(key).map(|x| x.entry)
    }
}
