use crate::accounts_index::AccountMapEntry;
use crate::accounts_index::{SlotList, BINS, RefCount};
use crate::pubkey_bins::PubkeyBinCalculator16;
use log::*;
use solana_bucket_map::bucket_map::BucketMap;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::borrow::Borrow;
use std::collections::btree_map::{BTreeMap};
use std::fmt::Debug;
use std::sync::Arc;
use std::ops::RangeBounds;
use std::ops::Bound;
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
    bin_index: usize,
    bins: usize,
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

pub struct Keys {
    keys: Vec<K>,
    index: usize,
}

impl Iterator for Keys {
    type Item = Pubkey;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.keys.len() {
            None
        }
        else {
            let r = Some(self.keys[self.index]);
            self.index += 1;
            r
        }
    }
}

pub struct Values<V: Clone + std::fmt::Debug> {
    values: Vec<(SlotList<V>)>,
    index: usize,
}

impl<V: Clone + std::fmt::Debug> Iterator for Values<V> {
    type Item = V2<V>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.values.len() {
            None
        }
        else {
            let r = Some(AccountMapEntry {
                slot_list: self.values[self.index].clone(), ref_count: RefCount::MAX, // todo: no clone
            });
            self.index += 1;
            r
        }
    }
}

pub struct HybridOccupiedEntry<'a, V: 'static + Clone + Debug> {
    pubkey: Pubkey,
    entry: AccountMapEntry<V>,
    map: &'a HybridBTreeMap<V>,
}
pub struct HybridVacantEntry<'a, V: 'static + Clone + Debug> {
    pubkey: Pubkey,
    map: &'a HybridBTreeMap<V>,
}

impl<'a, V: 'a + Clone + Debug> HybridOccupiedEntry<'a, V> {
    pub fn get(&self) -> &V2<V> {
        &self.entry
    }
    pub fn update(&mut self, new_data: &SlotList<V>) {
        //error!("update: {}", self.pubkey);
        self.map.disk.update(&self.pubkey, |previous| {
            error!("update {} to {:?}", self.pubkey, new_data);
            Some((new_data.clone(), self.entry.ref_count)) // TODO no clone here
        });
    }
    pub fn addref(&mut self) {
        self.entry.ref_count += 1;
        let result = self.map.disk.addref(&self.pubkey);
        //error!("addref: {}, {}, {:?}", self.pubkey, self.entry.ref_count(), result);
    }
    pub fn unref(&mut self) {
        self.entry.ref_count -= 1;
        let result = self.map.disk.unref(&self.pubkey);
        //error!("addref: {}, {}, {:?}", self.pubkey, self.entry.ref_count(), result);
    }
    /*
    pub fn get_mut(&mut self) -> &mut V2<V> {
        self.entry.get_mut()
    }
    */
    pub fn key(&self) -> &K {
        &self.pubkey
    }
    pub fn remove(self) {
        self.map.disk.delete_key(&self.pubkey)
    }
}

impl<'a, V: 'a + Clone + Debug> HybridVacantEntry<'a, V> {
    pub fn insert(self, value: V2<V>) {
        // -> &'a mut V2<V> {
        /*
        let value = V2::<V> {
            entry: value,
            //exists_on_disk: false,
        };
        */
        //let mut sl = SlotList::default();
        //std::mem::swap(&mut sl, &mut value.slot_list);
        self.map.disk.update(&self.pubkey, |_previous| {
            Some((value.slot_list.clone() /* todo bad */, value.ref_count))
        });
    }
}

impl<V: Clone + Debug> HybridBTreeMap<V> {
    /// Creates an empty `BTreeMap`.
    pub fn new(bucket_map: &Arc<BucketMap<SlotT<V>>>, bin_index: usize, bins: usize) -> HybridBTreeMap<V> {
        Self {
            in_memory: BTreeMap::default(),
            disk: bucket_map.clone(),
            bin_index,
            bins,
        }
    }
    pub fn new_bucket_map() -> Arc<BucketMap<SlotT<V>>> {
        let mut buckets = PubkeyBinCalculator16::log_2(BINS as u32) as u8; // make more buckets to try to spread things out
        buckets = std::cmp::min(buckets + 11, 16); // max # that works with open file handles and such
        error!("creating: {} for {}", buckets, BINS);
        Arc::new(BucketMap::new_buckets(buckets))
    }

    pub fn flush(&mut self) {
        /*
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
            self.in_memory.clear();
        }*/
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
        error!(
            "distribution: sum: {}, min: {}, max: {}, bins: {}",
            sum,
            min,
            max,
            dist.len()
        );
    }
    fn bound<'a, T>(bound: Bound<&'a T>, unbounded: &'a T) -> &'a T {
        match bound {
            Bound::Included(b) | Bound::Excluded(b) => b,
        _ => unbounded
        }
    }
    pub fn range<R>(&self, range: R) -> Keys
    where
        R: RangeBounds<Pubkey>,
    {
        let start = self.disk.bucket_ix(Self::bound(range.start_bound(), &Pubkey::default()));
        let end = self.disk.bucket_ix(Self::bound(range.end_bound(), &Pubkey::new(&[0; 32]))); // ugly
        let len = (start..end).into_iter().map(|ix| self.disk.bucket_len(ix) as usize).sum::<usize>();
        let mut keys = Vec::with_capacity(len);
        let len = (start..end).into_iter().for_each(|ix| {
            for k in self.disk.keys(ix).unwrap_or_default().into_iter() {
                range.contains(&k);
                keys.push(k);
            }
        });
        keys.sort_unstable();
        Keys {
            keys,
            index: 0,
        }
    }

    pub fn keys(&self) -> Keys {
        let num_buckets = self.disk.num_buckets();
        let start = num_buckets * self.bin_index / self.bins;
        let end = num_buckets * (self.bin_index + 1) / self.bins;
        let len = (start..end).into_iter().map(|ix| self.disk.bucket_len(ix) as usize).sum::<usize>();
        let mut keys = Vec::with_capacity(len);
        let len = (start..end).into_iter().for_each(|ix| keys.append(&mut self.disk.keys(ix).unwrap_or_default()));
        keys.sort_unstable();
        Keys {
            keys,
            index: 0,
        }
    }
    pub fn values(&self) -> Values<V> {
        let num_buckets = self.disk.num_buckets();
        let start = num_buckets * self.bin_index / self.bins;
        let end = num_buckets * (self.bin_index + 1) / self.bins;
        let len = (start..end).into_iter().map(|ix| self.disk.bucket_len(ix) as usize).sum::<usize>();
        let mut values = Vec::with_capacity(len);
        (start..end).into_iter().for_each(|ix| values.append(&mut self.disk.values(ix).unwrap_or_default()));
        error!("getting values: {}, bin: {}, bins: {}, start: {}, end: {}", values.len(), self.bin_index, self.bins, start, end);
        //keys.sort_unstable();
        Values {
            values,
            index: 0,
        }
    }
    pub fn len_inaccurate(&self) -> usize {
        1 // ??? wrong
        //self.in_memory.len()
    }
    pub fn entry(&mut self, key: K) -> HybridEntry<'_, V> {
        match self.disk.get(&key) {
            Some(entry) => HybridEntry::Occupied(HybridOccupiedEntry {
                pubkey: key,
                entry: AccountMapEntry::<V> {
                    slot_list: entry.1,
                    ref_count: entry.0,
                },
                map: self,
            }),
            None => HybridEntry::Vacant(HybridVacantEntry {
                pubkey: key,
                map: self,
            }),
        }
    }

    pub fn get(&self, key: &K) -> Option<V2<V>> {
        let lookup = || {
            let disk = self.disk.get(key);
            disk.map(|disk| AccountMapEntry {
                ref_count: disk.0,
                slot_list: disk.1,
            })
        };

        if true {
            lookup()
        } else {
            let in_mem = self.in_memory.get(key);
            match in_mem {
                Some(in_mem) => Some(in_mem.clone()),
                None => {
                    // we have to load this into the in-mem cache so we can get a ref_count, if nothing else
                    lookup()
                    /*
                    disk.map(|item| {
                        self.in_memory.entry(*key).map(|entry| {

                        }
                    })*/
                }
            }
        }
    }
    pub fn remove(&mut self, key: &K)
    {
        self.disk.delete_key(key); //.map(|x| x.entry)
    }
}
