use crate::accounts_index::AccountMapEntry;
use crate::accounts_index::BINS;
use crate::pubkey_bins::PubkeyBinCalculator16;
use solana_bucket_map::bucket_map::BucketMap;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::borrow::Borrow;
use std::collections::btree_map::{BTreeMap, Entry, Keys, OccupiedEntry, VacantEntry, Values};
use std::fmt::Debug;
use std::sync::Arc;
use log::*;
type K = Pubkey;

#[derive(Clone, Debug)]
pub struct HybridAccountEntry<V: Clone + Debug> {
    entry: V,
    //exists_on_disk: bool,
}
//type V2<T: Clone + Debug> = HybridAccountEntry<T>;
type V2<T> = AccountMapEntry<T>;
/*
trait RealEntry<T: Clone + Debug> {
    fn real_entry(&self) -> T;
}

impl<T:Clone + Debug> RealEntry<T> for T {
    fn real_entry(&self) -> T
    {
        self
    }
}
*/

pub type SlotT<T> = (Slot, T);

#[derive(Debug)]
pub struct HybridBTreeMap<V: 'static + Clone + Debug> {
    in_memory: BTreeMap<K, V2<V>>,
    disk: Arc<BucketMap<SlotT<V>>>,
}

// TODO: we need a bit for 'exists on disk' for updates
/*
impl<V: Clone + Debug> Default for HybridBTreeMap<V> {
    /// Creates an empty `BTreeMap`.
    fn default() -> HybridBTreeMap<V> {
        Self {
            in_memory: BTreeMap::default(),
            disk: BucketMap::new_buckets(PubkeyBinCalculator16::log_2(BINS as u32) as u8),
        }
    }
}
*/

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

pub enum HybridEntry<'a, V: 'static + Clone + Debug> {
    /// A vacant entry.
    Vacant(HybridVacantEntry<'a, V>),

    /// An occupied entry.
    Occupied(HybridOccupiedEntry<'a, V>),
}

pub struct HybridOccupiedEntry<'a, V: 'static + Clone + Debug> {
    entry: OccupiedEntry<'a, K, V2<V>>,
}
pub struct HybridVacantEntry<'a, V: 'static + Clone + Debug> {
    entry: VacantEntry<'a, K, V2<V>>,
}

impl<'a, V: 'a + Clone + Debug> HybridOccupiedEntry<'a, V> {
    pub fn get(&self) -> &V2<V> {
        &self.entry.get() //.entry
    }
    pub fn get_mut(&mut self) -> &mut V2<V> {
        self.entry.get_mut()
    }
    pub fn key(&self) -> &K {
        self.entry.key()
    }
    pub fn remove(self) -> V2<V> {
        panic!("todo");
        // TODO: remember something that was deleted!?!?
        self.entry.remove() //.entry
    }
}

impl<'a, V: 'a + Clone + Debug> HybridVacantEntry<'a, V> {
    pub fn insert(self, value: V2<V>) -> &'a mut V2<V> {
        /*
        let value = V2::<V> {
            entry: value,
            //exists_on_disk: false,
        };
        */
        self.entry.insert(value) //.entry
    }
}

impl<V: Clone + Debug> HybridBTreeMap<V> {
    /// Creates an empty `BTreeMap`.
    pub fn new(bucket_map: &Arc<BucketMap<SlotT<V>>>) -> HybridBTreeMap<V> {
        Self {
            in_memory: BTreeMap::default(),
            disk: bucket_map.clone(),
        }
    }
    pub fn new_bucket_map() -> Arc<BucketMap<SlotT<V>>> {
        let mut buckets = PubkeyBinCalculator16::log_2(BINS as u32) as u8; // make more buckets to try to spread things out
        buckets = std::cmp::min(buckets + 11, 11); // max # that works with open file handles and such
        error!("creating: {} for {}", buckets, BINS);
        Arc::new(BucketMap::new_buckets(buckets))
    }

    pub fn flush(&self) {
        {
            // put entire contents of this map into the disk backing
            let mut keys = Vec::with_capacity(self.in_memory.len());
            for k in self.in_memory.keys() {
                keys.push(k);
            }
            self.disk.update_batch(&keys[..], |previous, key, orig_i| {
                let item = self.in_memory.get(key);
    
                item.map(|item| (item.slot_list.clone(), item.ref_count()))
            });
        }
    }
    pub fn distribution(&self) {
        let dist = self.disk.distribution();
        let mut sum = 0;
        let mut min = usize::MAX;
        let mut max = 0;
        for d in &dist {
            let d = *d;
            sum += d;
            min = std::cmp::min(min, d);
            max = std::cmp::max(max, d);
        }
        error!("distribution: sum: {}, min: {}, max: {}, bins: {}", sum, min, max, dist.len());
    }
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
    pub fn entry(&mut self, key: K) -> HybridEntry<'_, V> {
        match self.in_memory.entry(key) {
            Entry::Occupied(entry) => HybridEntry::Occupied(HybridOccupiedEntry { entry }),
            Entry::Vacant(entry) => HybridEntry::Vacant(HybridVacantEntry { entry }),
        }
    }

    pub fn get(&self, key: &K) -> Option<V2<V>>
    {
        let in_mem = self.in_memory.get(key);
        match in_mem {
            Some(in_mem) => Some(in_mem.clone()),
            None => {
                // we have to load this into the in-mem cache so we can get a ref_count, if nothing else
                let disk = self.disk.get(key);
                disk.map(|disk| 
                AccountMapEntry {
                    ref_count: disk.0,
                    slot_list: disk.1,
                })
                /*
                disk.map(|item| {
                    self.in_memory.entry(*key).map(|entry| {

                    }
                })*/
            }
        }
    }
    pub fn remove<Q: ?Sized>(&mut self, key: &Q) -> Option<V2<V>>
    where
        K: Borrow<Q> + Ord,
        Q: Ord,
    {
        panic!("todo");
        self.in_memory.remove(key) //.map(|x| x.entry)
    }
}
