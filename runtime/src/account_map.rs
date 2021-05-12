use crate::message_processor::ExecuteDetailsTimings2;
use crate::{
    contains::Contains,
    inline_spl_token_v2_0::{self, SPL_TOKEN_ACCOUNT_MINT_OFFSET, SPL_TOKEN_ACCOUNT_OWNER_OFFSET},
    secondary_index::*,
};
use bv::BitVec;
use dashmap::DashSet;
use log::*;
use ouroboros::self_referencing;
use solana_measure::measure::Measure;
use solana_sdk::{
    clock::Slot,
    pubkey::{Pubkey, PUBKEY_BYTES},
};
use rand::{thread_rng, Rng};

use std::cmp::Ordering;
use std::fmt::Debug;
use std::{
    collections::{
        btree_map::{self, BTreeMap},
        HashMap, HashSet,
    },
    hash::Hash,
    marker::PhantomData,
    ops::{
        Bound,
        Bound::{Excluded, Included, Unbounded},
        Index, Range, RangeBounds,
    },
    sync::{atomic::AtomicU64, Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

impl<V: Clone, K: Clone + Debug + PartialOrd> Default for MyAccountMap<V, K> {
    /// Creates an empty `BTreeMap`.
    fn default() -> MyAccountMap<V, K> {
        MyAccountMap::new(100_000)
    }
}

#[derive(Debug, Clone)]
pub enum AccountMapEntryBtree<K: Clone + Debug + PartialOrd> {
    /// A vacant entry.
    Vacant(VacantEntry<K>),

    /// An occupied entry.
    Occupied(OccupiedEntry),
}

#[derive(Clone)]
pub struct PubkeyHasher {
    seed1: u128,
    seed2: u128,
}

impl Default for PubkeyHasher {
    fn default() -> Self {
        Self {
            seed1: thread_rng().gen::<u128>(),
            seed2: thread_rng().gen::<u128>(),
        }
    }
}

impl PubkeyHasher {
    pub fn hash_packet(&self, packet: &Pubkey) -> u64 {
        let mut hasher = AHasher::new_with_keys(self.seed1, self.seed2);
        hasher.write(&packet.as_ref());
        hasher.finish()
    }

    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

impl<K: Clone + Debug + PartialOrd> AccountMapEntryBtree<K> {
    pub fn or_insert_with<V: Clone, F>(self, default: F, btree: &mut MyAccountMap<V, K>) -> &V
    where
        F: FnOnce() -> V,
    {
        match self {
            AccountMapEntryBtree::Occupied(entry) => entry.into_mut(btree),
            AccountMapEntryBtree::Vacant(entry) => entry.insert(default(), btree),
        }
    }
}

#[derive(Clone)]
pub struct VacantEntry<K> {
    pub index: AccountMapIndex,
    pub key: K,
}

impl<K: Clone + Debug + PartialOrd> Debug for VacantEntry<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("VacantEntry").field(self.key()).finish()
    }
}

impl<K: Clone + Debug + PartialOrd> VacantEntry<K> {
    /// Creates an empty `BTreeMap`.
    pub fn key(&self) -> &K {
        &self.key
    }
    pub fn new(key: K, index: AccountMapIndex) -> Self {
        Self { key, index }
    }
    pub fn insert<V: Clone>(self, value: V, btree: &mut MyAccountMap<V, K>) -> &V {
        btree.insert_at_index(&self.index, self.key, value, &mut Timings::default())
    }
}

/// A view into an occupied entry in a `BTreeMap`.
/// It is part of the [`Entry`] enum.
#[derive(Debug, Clone)]
pub struct OccupiedEntry {
    pub index: AccountMapIndex,
}

impl OccupiedEntry {
    pub fn new(index: AccountMapIndex) -> Self {
        Self { index }
    }
    pub fn into_mut<V: Clone, K: Clone + Debug + PartialOrd>(
        self,
        btree: &MyAccountMap<V, K>,
    ) -> &V {
        btree.get_at_index(&self.index).unwrap()
    }
    pub fn get<'a, 'b, V: Clone, K: Clone + Debug + PartialOrd>(
        &'a self,
        btree: &'b MyAccountMap<V, K>,
    ) -> &'b V {
        btree.get_at_index(&self.index).unwrap()
    }
    pub fn remove<V: Clone, K: Clone + Debug + PartialOrd>(self, btree: &mut MyAccountMap<V, K>) {
        btree.remove_at_index(&self.index);
    }
}

#[derive(Debug, Clone, Default, Copy)]
pub struct AccountMapIndex {
    pub insert: bool,
    pub total: InnerAccountMapIndex,
}

#[derive(Debug, Clone, Default, Copy)]
pub struct InnerAccountMapIndex {
    pub outer_index: usize,
    pub inner_index: usize,
}

#[derive(Debug, Default)]
pub struct Timings {
    pub update_lens_us: u64,
    pub find_vec_ms: u64,
    pub outer_index_find_ms: u64,
    pub insert_vec_ms: u64,
    pub lookups: u64,
    pub lookup_bin_searches: u64,
    pub outer_bin_searches: u64,
    pub mv: u64,
    pub insert: u64,
}
use byteorder::{BigEndian, LittleEndian, ReadBytesExt}; // 1.2.7

type kkk = [u8; 30];
/*
trait MyAsSlice {
    type Item;
    fn as_slice(&self) -> &[Self::Item];
}

pub trait subset<K: Clone + Debug + PartialOrd>: AsRef<[u8]> + AsSlice<u8> + Clone + Debug + PartialOrd {
    fn split(&self) -> (u32, K);
}

impl<K: Clone + Debug + PartialOrd> subset<K> for Pubkey {
    fn split(&self) -> (u32, K)
    {
        let raw = self.as_ref();
        let num = raw.read_u16::<LittleEndian>().unwrap();
        (num as u32, &raw[2..32])
    }
}
*/
use std::collections::hash_map::DefaultHasher;
use ahash::AHasher;

//pub type MyAccountMap<K, V> = BTreeMap<K, V>;
type outer_key_type = Pubkey;
pub struct AMap<V> {
    keys: Vec<Vec<Pubkey>>,
    values: Vec<Vec<V>>,
    count: usize,
    timings: RwLock<Timings>,
    bins: usize,
    hasher: PubkeyHasher,
}
use std::hash::Hasher;
impl<V: Clone> AMap<V> {
    pub fn new(bins: usize) -> Self {
        let keys = (0..bins)
            .into_iter()
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let values = (0..bins)
            .into_iter()
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        Self {
            keys,
            values,
            count: 0,
            timings: RwLock::new(Timings::default()),
            bins,
            hasher: PubkeyHasher::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.count
    }

    fn hash(&self, key: &Pubkey) -> u64 {
        let data = key.as_ref();
        /*
        let d1 = u128::from_ne_bytes(data[0..15]);
        let d2 = u128::from_ne_bytes(data[16..32]);
        let d3 = d1 ^ d2;
        */
        let d1 = u64::from_ne_bytes(*arrayref::array_ref![data[0..8], 0, 8]);
        let d2 = u64::from_ne_bytes(*arrayref::array_ref![data[8..16], 0, 8]);
        let d3 = u64::from_ne_bytes(*arrayref::array_ref![data[16..24], 0, 8]);
        let d4 = u64::from_ne_bytes(*arrayref::array_ref![data[24..32], 0, 8]);
        let dall = d1 ^ d2 ^ d3 ^ d4;
        //self.hasher.hash_packet(key)
        dall
    }

    pub fn insert(&mut self, key: &outer_key_type, mut value: V) -> Option<V> {
        let bucket = (self.hash(key) as usize) % self.bins;
        for (i, item) in self.keys[bucket].iter().enumerate() {
            if item == key {
                // already exists
                std::mem::swap(&mut self.values[bucket][i], &mut value);
                return Some(value);
            }
        }
        self.keys[bucket].push(*key);
        self.values[bucket].push(value);
        self.count += 1;
        None
    }
    pub fn remove(&mut self, key: &outer_key_type) {
        panic!("do this");
    }
    pub fn get_fast(&self, key: &outer_key_type) -> Option<&V> {
        let bucket = (self.hash(key) as usize) % self.bins;
        for (i, item) in self.keys[bucket].iter().enumerate() {
            if item == key {
                // already exists
                let v = &self.values[bucket][i];
                return Some(v);
            }
        }
        None
    }
    pub fn get(&self, key: &outer_key_type) -> Option<&V> {
        self.get_fast(key)
    }
    /*
    pub fn keys(&self) -> AccountMapIterKeys<'_, V, outer_key_type> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterKeys::new(&self, AccountMapIndex::default()) // {index: 0, insert: false,})
    }
    */

    /*
    pub fn entry(&self, key: K) -> AccountMapEntryBtree<K> {
        let (pre, post) = key.split();
        self.data[0].entry(post)
    }
    pub fn contains_key(&self, key: &K) -> bool {
        let find = self.find(&key, &mut Timings::default());
        !find.insert
    }
    pub fn get_fast(&self, key: &outer_key_type) -> Option<&V> {
        None
    }
    pub fn get_at_index(&self, index: &AccountMapIndex) -> Option<&V> {
        None
    }
    pub fn remove(&mut self, key: &K) {}
    pub fn keys(&self) -> AccountMapIterKeys<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterKeys::new(&self, AccountMapIndex::default()) // {index: 0, insert: false,})
    }
    pub fn values(&self) -> AccountMapIterValues<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterValues::new(&self, AccountMapIndex::default())
    }
    pub fn iter(&self) -> AccountMapIter<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIter::new(&self, AccountMapIndex::default())
    }
    pub fn range<T, R>(&self, range: R) -> Option<(K, K)>
    where
        T: Ord + ?Sized,
        R: RangeBounds<T>,
    {
        panic!("not supported");
        None
    }
    */
}

pub type AccountMap<V, K> = BTreeMap<K, V>;
#[derive(Debug)]
pub struct AccountMapSlicer<V> {
    //timings: RwLock<Timings>,
    data: Vec<MyAccountMap<V, slicer_key_type>>,
}

type slicer_key_type = [u8; 30];
use std::convert::TryInto;
impl<V: Clone> AccountMapSlicer<V> {
    pub fn new(vec_size_max: usize) -> Self {
        let div = 65536;
        let data = (0..div)
            .into_iter()
            .map(|_| MyAccountMap::<V, slicer_key_type>::new(vec_size_max / div))
            .collect::<Vec<_>>();
        Self { data }
    }

    pub fn len(&self) -> usize {
        self.data.iter().map(|d| d.len()).sum::<usize>()
    }

    pub fn max_sub_len(&self) -> (usize, usize) {
        (
            self.data.iter().map(|d| d.len()).max().unwrap(),
            self.data.iter().map(|d| d.len()).min().unwrap(),
        )
    }

    fn split(key: &outer_key_type) -> (usize, &[u8; 30]) {
        let raw = key.as_ref();
        let num = (raw[0] as usize) * 256 + (raw[1] as usize);
        const x: usize = 2;
        let aref = arrayref::array_ref![raw, x, 32 - x];
        (num, aref)
    }

    pub fn insert(&mut self, key: &outer_key_type, value: V) -> &V {
        let (pre, post) = Self::split(key);
        self.data[pre].insert(post.try_into().unwrap(), value)
    }
    pub fn remove(&mut self, key: &outer_key_type) {
        let (pre, post) = Self::split(key);
        self.data[pre].remove(post.try_into().unwrap())
    }
    pub fn get_fast(&self, key: &outer_key_type) -> Option<&V> {
        let (pre, post) = Self::split(&key);
        self.data[pre].get_fast(post.try_into().unwrap())
    }
    pub fn get(&self, key: &outer_key_type) -> Option<&V> {
        let (pre, post) = Self::split(&key);
        self.data[pre].get_fast(post.try_into().unwrap())
    }
    /*
    pub fn keys(&self) -> AccountMapIterKeys<'_, V, outer_key_type> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterKeys::new(&self, AccountMapIndex::default()) // {index: 0, insert: false,})
    }
    */

    /*
    pub fn entry(&self, key: K) -> AccountMapEntryBtree<K> {
        let (pre, post) = key.split();
        self.data[0].entry(post)
    }
    pub fn contains_key(&self, key: &K) -> bool {
        let find = self.find(&key, &mut Timings::default());
        !find.insert
    }
    pub fn get_fast(&self, key: &outer_key_type) -> Option<&V> {
        None
    }
    pub fn get_at_index(&self, index: &AccountMapIndex) -> Option<&V> {
        None
    }
    pub fn remove(&mut self, key: &K) {}
    pub fn keys(&self) -> AccountMapIterKeys<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterKeys::new(&self, AccountMapIndex::default()) // {index: 0, insert: false,})
    }
    pub fn values(&self) -> AccountMapIterValues<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterValues::new(&self, AccountMapIndex::default())
    }
    pub fn iter(&self) -> AccountMapIter<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIter::new(&self, AccountMapIndex::default())
    }
    pub fn range<T, R>(&self, range: R) -> Option<(K, K)>
    where
        T: Ord + ?Sized,
        R: RangeBounds<T>,
    {
        panic!("not supported");
        None
    }
    */
}

//pub type MyAccountMap<K, V> = BTreeMap<K, V>;
#[derive(Debug)]
pub struct MyAccountMap<V, K> {
    keys: Vec<Vec<K>>,
    values: Vec<Vec<V>>,
    count: usize,
    cumulative_min_key: Vec<K>,
    cumulative_lens: Vec<usize>,
    vec_size_max: usize,
    timings: RwLock<Timings>,
}

impl<V: Clone, K: Clone + Debug + PartialOrd> MyAccountMap<V, K> {
    pub fn new(mut vec_size_max: usize) -> Self {
        vec_size_max = std::cmp::max(vec_size_max, 1);
        Self {
            keys: vec![Self::new_vec(vec_size_max)],
            values: vec![Self::new_vec(vec_size_max)],
            cumulative_min_key: Vec::new(),
            count: 0,
            cumulative_lens: vec![0],
            vec_size_max,
            timings: RwLock::new(Timings::default()),
        }
    }

    fn new_vec<T>(size: usize) -> Vec<T> {
        Vec::with_capacity(size)
    }

    pub fn get_index(&self, index: &AccountMapIndex) -> Option<(&K, &V)> {
        if index.insert {
            None
        } else {
            assert!(self.keys.len() == self.values.len());
            let mut outer = index.total.outer_index;
            let mut inner = index.total.inner_index;
            let mut keys = &self.keys[outer];

            while inner >= keys.len() {
                inner = 0;
                outer += 1;
                if outer >= self.keys.len() {
                    return None;
                }
                keys = &self.keys[outer];
            }
            let key = &keys[inner];
            let value = &self.values[outer];
            let value = &value[inner];
            Some((key, value))
        }
    }

    fn mv<U: Clone>(vec: &mut Vec<Vec<U>>, outer: usize, inner: usize, to_move: usize, value: U) {
        let mut new_vec;
        if to_move == 0 {
            // could look forward to see if next outer can fit this new item without growing too big
            new_vec = vec![value];
        } else {
            new_vec = vec[outer][inner..].to_vec();
            vec[outer][inner] = value;
            vec[outer].truncate(inner + 1);
        }
        vec.insert(outer + 1, new_vec);
    }

    pub fn insert_at_index_alloc(
        &mut self,
        index: &AccountMapIndex,
        key: K,
        value: V,
        outer: &mut InnerAccountMapIndex,
        timings: &mut Timings,
    ) {
        let max = self.vec_size_max;
        let max_move = max / 100; // tune this later
        let size = self.keys[outer.outer_index].len();
        let to_move = size - outer.inner_index;
        //error!("insert_at_index_alloc, outer: {}, inner: {}, len: {}, to_move: {}, size: {}, values: {:?}", outer.outer_index, outer.inner_index, self.count, to_move, size, self.keys);
        if size > 0 && (to_move > max_move || size + 1 >= max) {
            // have to add a new vector
            let mut m1 = Measure::start("");
            Self::mv(
                &mut self.keys,
                outer.outer_index,
                outer.inner_index,
                to_move,
                key.clone(),
            );
            Self::mv(
                &mut self.values,
                outer.outer_index,
                outer.inner_index,
                to_move,
                value,
            );
            m1.stop();
            timings.mv += m1.as_ns();
            //error!("new keys: {:?}", self.keys);
            if to_move == 0 {
                self.cumulative_min_key.insert(outer.outer_index + 1, key);
            } else {
                self.cumulative_min_key.insert(
                    outer.outer_index + 1,
                    self.keys[outer.outer_index + 1][0].clone(),
                );
            }
            self.cumulative_lens
                .insert(outer.outer_index, self.cumulative_lens[outer.outer_index]); // we inserted here
            if to_move == 0 {
                //self.cumulative_lens[outer.outer_index + 1] -= outer.inner_index;
                outer.outer_index += 1;
                outer.inner_index = 0;
            } else {
                self.cumulative_lens[outer.outer_index] -= to_move;
            }
        } else {
            // no new vector - just insert
            let mut m1 = Measure::start("");
            self.keys[outer.outer_index].insert(outer.inner_index, key.clone());
            self.values[outer.outer_index].insert(outer.inner_index, value);
            m1.stop();
            timings.insert += m1.as_ns();
            if outer.inner_index == 0 {
                self.cumulative_min_key.insert(outer.outer_index, key);
            }
        }
        // shift the rest of the cumulative offsets since we inserted
        for outer_index in &mut self.cumulative_lens[outer.outer_index..] {
            *outer_index += 1;
        }
        //error!("Inserted, lens: {:?}, keys: {:?}", self.cumulative_lens, self.keys);
        self.count += 1;
    }

    pub fn get_outer_index(
        &self,
        given_index: usize,
        timings: &mut Timings,
    ) -> InnerAccountMapIndex {
        let mut l = 0;
        let mut index = l;
        let mut inner_index = 0;
        let mut count = 0;
        if self.count > 0 {
            let mut r = self.cumulative_lens.len();
            let mut iteration = 0;
            loop {
                count += 1;
                index = (l + r) / 2;
                let val = self.cumulative_lens[index];
                let cmp = given_index.partial_cmp(&val).unwrap();
                //error!("outer: left: {}, right: {}, count: {}, index: {}, cmp: {:?}, val: {:?}, iteration: {}", l, r, self.cumulative_lens.len(), index, cmp, val, iteration);
                iteration += 1;
                match cmp {
                    Ordering::Equal => {
                        if index + 1 < self.cumulative_lens.len() {
                            index += 1;
                        }
                        break;
                    }
                    Ordering::Less => {
                        r = index;
                    }
                    Ordering::Greater => {
                        if index == r - 1 {
                            index = r;
                            break;
                        }
                        l = index;
                    }
                }
                if r == l {
                    break;
                }
            }
            //error!("returning: index: {}, inner_index: {}, given_index: {}, cumulative_lens: {:?}", index, inner_index, given_index, self.cumulative_lens);
            if index > 0 {
                inner_index = given_index - self.cumulative_lens[index - 1];
            } else {
                inner_index = given_index;
            }
            //error!("returned: outer: {}, inner_index: {}, given_index: {}, cumulative_lens: {:?}", index, inner_index, given_index, self.cumulative_lens);
        }
        timings.lookups += 1;
        timings.lookup_bin_searches += count;

        InnerAccountMapIndex {
            outer_index: index,
            inner_index,
        }
    }
    /*
    pub fn increment(&self, outer: &mut InnerAccountMapIndex) {
        while outer.outer_index < self.values.len() {
            let current = self.values[outer.outer_index];
            outer.inner_index += 1;
            if outer.inner_index < current.len() {
                return;
            }
            outer.inner_index = 0;
            outer.outer_index += 1;
            // loop because we could have an empty inner array and we may need to increment outer again
        }
    }
    */
    pub fn find_outer_index(&self, key: &K, timings: &mut Timings) -> usize {
        let mut l = 0;
        let mut index = l;
        let mut insert = true;

        let mut iteration = 0;
        if self.count > 0 {
            let mut r = self.cumulative_min_key.len();
            info!("keys: {:?}", self.keys);
            info!("cumulative_min_key: {:?}", self.cumulative_min_key);
            loop {
                index = (l + r) / 2;
                info!("keys2: {:?}, outer: {:?}", self.keys, index);
                let val = &self.cumulative_min_key[index];
                let cmp = key.partial_cmp(val).unwrap();
                info!("fo: left: {}, right: {}, count: {}, index: {}, cmp: {:?}, key: {:?} val: {:?}, iteration: {}, keys: {:?}", l, r, self.count, index, cmp, val, key, iteration, self.keys);
                iteration += 1;
                match cmp {
                    Ordering::Equal => {
                        insert = false;
                        break;
                    }
                    Ordering::Less => {
                        r = index;
                    }
                    Ordering::Greater => {
                        if index == r - 1 {
                            // we only compared min index, so if we are greater and down to this is the only option, then it fits here
                            break;
                        }
                        l = index;
                    }
                }
                if r == l {
                    break;
                }
            }
        }
        timings.lookup_bin_searches += iteration;
        timings.outer_bin_searches += iteration;

        index
    }
    pub fn find_outer_index_fast(&self, key: &K) -> (usize, bool) {
        let mut l = 0;
        let mut index = l;
        let mut insert = true;

        if self.count > 0 {
            let mut r = self.cumulative_min_key.len();
            info!("keys: {:?}", self.keys);
            info!("cumulative_min_key: {:?}", self.cumulative_min_key);
            loop {
                index = (l + r) / 2;
                info!("keys2: {:?}, outer: {:?}", self.keys, index);
                let val = &self.cumulative_min_key[index];
                let cmp = key.partial_cmp(val).unwrap();
                //info!("fo: left: {}, right: {}, count: {}, index: {}, cmp: {:?}, key: {:?} val: {:?}, iteration: {}, keys: {:?}", l, r, self.count, index, cmp, val, key, iteration, self.keys);
                match cmp {
                    Ordering::Equal => {
                        insert = false;
                        break;
                    }
                    Ordering::Less => {
                        r = index;
                    }
                    Ordering::Greater => {
                        if index == r - 1 {
                            // we only compared min index, so if we are greater and down to this is the only option, then it fits here
                            break;
                        }
                        l = index;
                    }
                }
                if r == l {
                    break;
                }
            }
        }

        (index, insert)
    }
    pub fn find(&self, key: &K, timings: &mut Timings) -> AccountMapIndex {
        let mut l = 0;
        let mut index = l;
        let mut insert = true;

        let mut m1 = Measure::start("");
        let outer_index = self.find_outer_index(key, timings);
        m1.stop();
        timings.outer_index_find_ms += m1.as_ns();
        if outer_index >= self.keys.len() {
        } else {
            let keys = &self.keys[outer_index];
            let len = keys.len();

            if len > 0 {
                let mut r = len;
                let mut iteration = 0;
                //error!("keys: {:?}", self.keys);
                loop {
                    index = (l + r) / 2;
                    let val = &keys[index];
                    let cmp = key.partial_cmp(val).unwrap();
                    //error!("left: {}, right: {}, count: {}, index: {}, cmp: {:?}, key: {:?} val: {:?}, iteration: {}, keys: {:?}", l, r, self.count, index, cmp, val, key, iteration, self.keys);
                    iteration += 1;
                    match cmp {
                        Ordering::Equal => {
                            insert = false;
                            break;
                        }
                        Ordering::Less => {
                            r = index;
                        }
                        Ordering::Greater => {
                            if index == r - 1 {
                                index = r;
                                break;
                            }
                            l = index;
                        }
                    }
                    if r == l {
                        break;
                    }
                }
                timings.lookup_bin_searches += iteration;
            }
        }
        timings.lookups += 1;

        AccountMapIndex {
            insert,
            total: InnerAccountMapIndex {
                outer_index,
                inner_index: index,
            },
        }
    }
    pub fn find_fast(&self, key: &K) -> AccountMapIndex {
        let mut l = 0;
        let mut index = l;

        let (outer_index, mut insert) = self.find_outer_index_fast(key);
        if !insert {
            index = 0;
        } else {
            if outer_index >= self.keys.len() {
            } else {
                let keys = &self.keys[outer_index];
                let len = keys.len();

                if len > 0 {
                    let mut r = len;
                    let mut iteration = 0;
                    //error!("keys: {:?}", self.keys);
                    l = 1; // if we weren't equal, then we are after the first
                    loop {
                        index = (l + r) / 2;
                        let val = &keys[index];
                        let cmp = key.partial_cmp(val).unwrap();
                        //error!("left: {}, right: {}, count: {}, index: {}, cmp: {:?}, key: {:?} val: {:?}, iteration: {}, keys: {:?}", l, r, self.count, index, cmp, val, key, iteration, self.keys);
                        iteration += 1;
                        match cmp {
                            Ordering::Equal => {
                                insert = false;
                                break;
                            }
                            Ordering::Less => {
                                r = index;
                            }
                            Ordering::Greater => {
                                if index == r - 1 {
                                    index = r;
                                    break;
                                }
                                l = index;
                            }
                        }
                        if r == l {
                            break;
                        }
                    }
                }
            }
        }
        AccountMapIndex {
            insert,
            total: InnerAccountMapIndex {
                outer_index,
                inner_index: index,
            },
        }
    }
    pub fn len(&self) -> usize {
        self.count
    }
    pub fn insert(&mut self, key: &K, value: V) -> &V {
        let mut timings = Timings::default();
        let find = self.find(key, &mut timings);
        self.insert_at_index(&find, key.clone(), value, &mut timings)
    }
    pub fn insert_at_index(
        &mut self,
        index: &AccountMapIndex,
        key: K,
        value: V,
        timings: &mut Timings,
    ) -> &V {
        let mut m1 = Measure::start("");
        let mut outer = index.total;
        m1.stop();

        let mut m2 = Measure::start("");
        if index.insert {
            self.insert_at_index_alloc(index, key, value, &mut outer, timings);
        } else {
            panic!("do this");
            self.values[outer.outer_index][outer.inner_index] = value;
        }
        m2.stop();
        {
            let mut m = self.timings.write().unwrap();
            m.find_vec_ms += m1.as_ns();
            m.outer_index_find_ms += timings.outer_index_find_ms;
            m.insert_vec_ms += m2.as_ns();
            m.lookups += timings.lookups;
            m.lookup_bin_searches += timings.lookup_bin_searches;
            m.insert += timings.insert;
            m.mv += timings.mv;
            m.outer_bin_searches += timings.outer_bin_searches;
        }
        //error!("outer: {}, inner: {}, len: {}, insert: {}", outer.outer_index, outer.inner_index, self.values.len(), index.insert);
        if false {
            //self.count % 5_000_000 == 0 {
            let mut m = self.timings.write().unwrap();
            m.find_vec_ms /= 1000_000;
            m.insert_vec_ms /= 1000_000;
            m.outer_index_find_ms /= 1000_000;
            m.mv /= 1000_000;
            m.insert /= 1000_000;
            if m.lookups > 0 {
                m.lookup_bin_searches /= m.lookups;
                m.outer_bin_searches /= m.lookups;
            }

            error!(
                "count: {}, lens: {:?}, {:?}",
                self.count,
                self.cumulative_lens.len(),
                *m
            );
            *m = Timings::default();
        }
        &self.values[outer.outer_index][outer.inner_index]
    }
    pub fn entry(&self, key: K) -> AccountMapEntryBtree<K> {
        let find = self.find(&key, &mut Timings::default());
        if find.insert {
            AccountMapEntryBtree::Vacant(VacantEntry::new(key, find))
        } else {
            AccountMapEntryBtree::Occupied(OccupiedEntry::new(find))
        }
    }
    pub fn contains_key(&self, key: &K) -> bool {
        let find = self.find(&key, &mut Timings::default());
        !find.insert
    }
    pub fn get(&self, key: &K) -> Option<&V> {
        let mut timings = Timings::default();
        let mut m1 = Measure::start("");
        let find = self.find(&key, &mut timings);
        m1.stop();
        let res = self.get_at_index(&find);

        {
            let mut m = self.timings.write().unwrap();
            m.find_vec_ms += m1.as_ns();
            //m.insert_vec_ms += m1.as_ns();
            m.lookups += timings.lookups;
            m.outer_index_find_ms += timings.outer_index_find_ms;
            m.lookup_bin_searches += timings.lookup_bin_searches;
            m.insert += timings.insert;
            m.mv += timings.mv;
            m.outer_bin_searches += timings.outer_bin_searches;
            //error!("outer: {}, inner: {}, len: {}, insert: {}", outer.outer_index, outer.inner_index, self.values.len(), index.insert);
            if m.lookups % 5_000_000 == 0 {
                //let mut m = self.timings.write().unwrap();
                m.find_vec_ms /= 1000_000;
                m.insert_vec_ms /= 1000_000;
                m.outer_index_find_ms /= 1000_000;
                m.mv /= 1000_000;
                m.insert /= 1000_000;
                if m.lookups > 0 {
                    m.lookup_bin_searches /= m.lookups;
                    m.outer_bin_searches /= m.lookups;
                }

                error!(
                    "count: {}, lens: {:?}, {:?}, ideal: {}",
                    self.count,
                    self.cumulative_lens.len(),
                    *m,
                    (self.count as f64).log2()
                );
                *m = Timings::default();
            }
        }
        res
    }
    pub fn get_fast(&self, key: &K) -> Option<&V> {
        let find = self.find_fast(&key);
        let res = self.get_at_index(&find);
        res
    }
    pub fn get_at_index(&self, index: &AccountMapIndex) -> Option<&V> {
        if index.insert {
            None
        } else {
            let outer = index.total;
            Some(&self.values[outer.outer_index][outer.inner_index])
        }
    }
    pub fn remove(&mut self, key: &K) {
        let find = self.find(&key, &mut Timings::default());
        self.remove_at_index(&find)
    }
    pub fn remove_at_index(&mut self, index: &AccountMapIndex) {
        if index.insert {
        } else {
            let mut outer = index.total; // self.get_outer_index(index.index, &mut Timings::default());
            self.count -= 1;
            // TODO - could shrink to zero size here
            self.keys[outer.outer_index].remove(outer.inner_index);
            self.values[outer.outer_index].remove(outer.inner_index);
            for cumulative in &mut self.cumulative_lens[outer.outer_index..] {
                *cumulative -= 1;
            }
        }
    }
    pub fn keys(&self) -> AccountMapIterKeys<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterKeys::new(&self, AccountMapIndex::default()) // {index: 0, insert: false,})
    }
    pub fn values(&self) -> AccountMapIterValues<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIterValues::new(&self, AccountMapIndex::default())
    }
    pub fn iter(&self) -> AccountMapIter<'_, V, K> {
        //std::slice::Iter<'_, (K, V)> {
        AccountMapIter::new(&self, AccountMapIndex::default())
    }
    pub fn range<T, R>(&self, range: R) -> Option<(K, K)>
    where
        T: Ord + ?Sized,
        R: RangeBounds<T>,
    {
        panic!("not supported");
        None
    }
}

pub struct AccountMapIter<'a, V, K> {
    pub index: AccountMapIndex,
    pub btree: &'a MyAccountMap<V, K>,
    //_dummy: PhantomData<V>,
}
impl<'a, V, K> AccountMapIter<'a, V, K> {
    pub fn new(btree: &'a MyAccountMap<V, K>, index: AccountMapIndex) -> Self {
        Self { index, btree }
    }
}
impl<'a, V: Clone, K: Clone + Debug + PartialOrd> Iterator for AccountMapIter<'a, V, K> {
    type Item = (&'a K, &'a V);

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.btree.get_index(&self.index);
        if result.is_some() {
            panic!("do this");
            //self.index.index += 1;
        }

        result
    }
}
pub struct AccountMapIterValues<'a, V, K> {
    pub index: AccountMapIndex,
    pub btree: &'a MyAccountMap<V, K>,
    //_dummy: PhantomData<V>,
}
impl<'a, V, K> AccountMapIterValues<'a, V, K> {
    pub fn new(btree: &'a MyAccountMap<V, K>, index: AccountMapIndex) -> Self {
        Self { index, btree }
    }
}
impl<'a, V: Clone, K: Clone + Debug + PartialOrd> Iterator for AccountMapIterValues<'a, V, K> {
    type Item = &'a V;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.btree.get_index(&self.index).map(|(_, v)| v);
        if result.is_some() {
            self.index.total.inner_index += 1;
        }

        result
    }
}

pub struct AccountMapIterKeys<'a, V: Clone, K> {
    pub index: AccountMapIndex,
    pub btree: &'a MyAccountMap<V, K>,
    //_dummy: PhantomData<V>,
}
impl<'a, V: Clone, K> AccountMapIterKeys<'a, V, K> {
    pub fn new(btree: &'a MyAccountMap<V, K>, index: AccountMapIndex) -> Self {
        Self { index, btree }
    }
}
impl<'a, V: Clone, K: Clone + Debug + PartialOrd> Iterator for AccountMapIterKeys<'a, V, K> {
    type Item = &'a K;

    // next() is the only required method
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.btree.get_index(&self.index).map(|(k, _)| k);
        if result.is_some() {
            self.index.total.inner_index += 1;
        }

        result
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use dashmap::DashMap;
    use log::*;
    use solana_sdk::signature::{Keypair, Signer};
    use std::collections::HashMap;

    #[test]
    fn test_perf_dashmap() {
        solana_logger::setup();

        let mx = 27u32; //26u32;
        let mx_key_count = 2usize.pow(mx);
        let mut keys_orig = Vec::with_capacity(mx_key_count);
        let mut first = true;
        for key_pow in 15..mx {
            let key_count = 2usize.pow(key_pow);
            while keys_orig.len() < key_count {
                let mut pk = Pubkey::new_rand();
                pk.as_mut()[0] = (keys_orig.len() % 256) as u8;
                pk.as_mut()[1] = ((keys_orig.len() / 256) % 256) as u8;
                keys_orig.push(pk);
            }
            let mut keys = keys_orig.clone();
            keys.sort();

            let mut m = BTreeMap::new();
            let value = vec![0; 60];
            let mut m13 = Measure::start("");
            for i in 0..key_count {
                m.insert(&keys[i], vec![keys[i].as_ref()[0]; 60]);
            }
            m13.stop();

            let mut m23 = Measure::start("");
            for i in 0..key_count {
                m.get(&keys_orig[i]);
            }
            m23.stop();
            for i in 0..(key_count / 1000) {
                assert_eq!(m.get(&keys_orig[i]).unwrap()[0], keys_orig[i].as_ref()[0]);
            }

            let mut m = HashMap::new();
            let value = vec![0; 60];
            let mut m14 = Measure::start("");
            for i in 0..key_count {
                m.insert(&keys[i], vec![keys[i].as_ref()[0]; 60]);
            }
            m14.stop();

            let mut m24 = Measure::start("");
            for i in 0..key_count {
                m.get(&keys_orig[i]);
            }
            m24.stop();
            for i in 0..(key_count / 1000) {
                assert_eq!(m.get(&keys_orig[i]).unwrap()[0], keys_orig[i].as_ref()[0]);
            }

            for vec_size in [100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000]
                .iter()
                .cloned()
            {
                // anatoly
                let good = (key_count / vec_size) < 10_000;

                let mut m = AMap::new(vec_size);
                let value = vec![0; 60];
                let mut mati = Measure::start("");
                if good {
                        for i in 0..key_count {
                        m.insert(&keys[i], vec![keys[i].as_ref()[0]; 60]);
                    }
                    mati.stop();
                }

                let mut mat = Measure::start("");
                if good {
                    for i in 0..key_count {
                        m.get_fast(&keys_orig[i]);
                    }
                    mat.stop();
                    for i in 0..(key_count / 1000) {
                        assert_eq!(
                            m.get_fast(&keys_orig[i]).unwrap()[0],
                            keys_orig[i].as_ref()[0]
                        );
                    }
                }
                else {
                    mat.stop();
                }

                let mut m = MyAccountMap::new(vec_size / 256);
                let value = vec![0; 60];
                let mut m1 = Measure::start("");
                for i in 0..key_count {
                    m.insert(&keys[i], vec![keys[i].as_ref()[0]; 60]);
                }
                m1.stop();

                let mut m2 = Measure::start("");
                for i in 0..key_count {
                    m.get_fast(&keys_orig[i]);
                }
                m2.stop();
                for i in 0..(key_count / 1000) {
                    assert_eq!(
                        m.get_fast(&keys_orig[i]).unwrap()[0],
                        keys_orig[i].as_ref()[0]
                    );
                }

                //error!("insert: {} insert: {}, get: {}, size: {}", 1, m1.as_ms(), m2.as_ms(), key_count);
                /*
                            // log find metrics
                            for i in 0..key_count {
                                m.get(&keys_orig[i]);
                            }
                */
                let mut m = AccountMapSlicer::new(vec_size);
                let value = vec![0; 60];
                let mut m11 = Measure::start("");
                for i in 0..key_count {
                    m.insert(&keys[i], vec![keys[i].as_ref()[0]; 60]);
                }
                m11.stop();

                let mut m22 = Measure::start("");
                for i in 0..key_count {
                    m.get_fast(&keys_orig[i]);
                }
                m22.stop();
                for i in 0..(key_count / 1000) {
                    assert_eq!(
                        m.get_fast(&keys_orig[i]).unwrap()[0],
                        keys_orig[i].as_ref()[0]
                    );
                }
                //error!("insert: {} insert: {}, get: {}, size: {}", 0, m11.as_ms(), m22.as_ms(), key_count);

                if first {
                    error!("bt get ratio size BTreeMap MyAccountMap AccountMapSlicer HashMap anatoly parameter(vec_size)");
                    first = false;
                }
                error!(
                "bt get {} {} {} {} {} {} {} {} time_insert_ms {} time_get_ms {} data lens {:?}, minmax {:?}, insert {}",
                (m22.as_ns() as f64) / (m2.as_ns() as f64),
                key_count,
                m23.as_ms(),
                m2.as_ms(),
                m22.as_ms(),
                m24.as_ms(),
                mat.as_ms(),
                vec_size,
                m1.as_ms(),
                m2.as_ms(),
                [0],//m.data.iter().map(|a| a.len()).collect::<Vec<_>>(),
                m.max_sub_len(),
                (m11.as_ns() as f64) / (m1.as_ns() as f64),
            );
                /*
                let mut m = HashMap::new();
                let value = vec![0; 60];
                let mut m111 = Measure::start("");
                for i in 0..key_count {
                    m.insert(keys[i], value.clone());
                }
                m111.stop();

                let mut m222 = Measure::start("");
                for i in 0..key_count {
                    m.get(&keys_orig[i]);
                }
                m222.stop();
                //error!("insert: {} insert: {}, get: {}, size: {}", 0, m11.as_ms(), m22.as_ms(), key_count);
                error!("hm insert: {} get: {}, size: {}", (m111.as_ns() as f64) / (m1.as_ns() as f64), (m222.as_ns() as f64) / (m2.as_ns() as f64), key_count);
                */
            }
        }
    }

    #[test]
    fn test_perf_bt() {
        solana_logger::setup();

        let mx = 28u32; //26u32;
        let mx_key_count = 2usize.pow(mx);
        let mut keys_orig = Vec::with_capacity(mx_key_count);
        let mut first = true;
        let mut size = 0;
        for key_pow in 15..mx {
            let key_count = 2usize.pow(key_pow);
            let sz = std::cmp::min(key_count * 5000, 40_000_000_000);
            error!("allocating: {}, {}", key_count, size);
            let mut m = Vec::with_capacity(sz);
            m.push(32u8);
            size = m.len();
            std::thread::sleep(std::time::Duration::from_secs(3));
        }
        error!("m: {}", size);

        for pass in 0..2 {
            for key_pow in 15..mx {
                let key_count = 2usize.pow(key_pow);
                if pass == 0 {
                    error!("creating keys: {}", key_count - keys_orig.len());
                    while keys_orig.len() < key_count {
                        let mut pk = Pubkey::new_rand();
                        pk.as_mut()[0] = (keys_orig.len() % 256) as u8;
                        pk.as_mut()[1] = ((keys_orig.len() / 256) % 256) as u8;
                        keys_orig.push(pk);
                    }
                }
                else {
                    std::thread::sleep(std::time::Duration::from_secs(3));

                    //let mut keys = keys_orig.clone();
                    //keys.sort();
                    let keys = &keys_orig;

                    let mut m = BTreeMap::new();
                    let value = vec![0; 60];
                    let mut m13 = Measure::start("");
                    error!("inserting: {}", key_count);
                    for i in 0..key_count {
                        m.insert(&keys[i], vec![keys[i].as_ref()[0]; 60]);
                    }
                    m13.stop();
                    std::thread::sleep(std::time::Duration::from_secs(1));
                    drop(m);
                    std::thread::sleep(std::time::Duration::from_secs(1));
                }
            }
        }
    }

    #[test]
    fn test_account_map123() {
        solana_logger::setup();
        let key0 = Pubkey::new(&[0u8; 32]);
        let key1 = Pubkey::new(&[1u8; 32]);
        let key2 = Pubkey::new(&[2u8; 32]);
        let key3 = Pubkey::new(&[3u8; 32]);

        let mut m = AccountMapSlicer::new(100_000);
        let val0 = 0;
        let val1 = 1;
        let val2 = 1;
        let val3 = 1;
        assert_eq!(0, m.len());
        m.insert(&key1, val1);
        assert_eq!(m.get(&key1).unwrap(), &val1);
        assert_eq!(1, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1]);
        m.insert(&key3, val3);
        assert_eq!(m.get(&key3).unwrap(), &val3);
        assert_eq!(2, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key3]);
        m.insert(&key2, val2);
        assert_eq!(m.get(&key2).unwrap(), &val2);
        assert_eq!(3, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key2, &key3]);
        //assert_eq!(m.values().collect::<Vec<_>>(), vec![&val1, &val2, &val3]);
        m.remove(&key2);
        assert_eq!(2, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key3]);
        m.insert(&key0, val0);
        assert_eq!(m.get(&key0).unwrap(), &val0);
        assert_eq!(3, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key0, &key1, &key3]);
        //assert_eq!(m.values().collect::<Vec<_>>(), vec![&val0, &val1, &val3]);
    }

    #[test]
    fn test_account_map_entry() {
        solana_logger::setup();
        let key1 = Pubkey::new(&[1u8; 32]);
        let key2 = Pubkey::new(&[2u8; 32]);
        let key3 = Pubkey::new(&[3u8; 32]);

        let mut m = AccountMapSlicer::new(100_000);
        let val1 = 1;
        let val2 = 1;
        let val3 = 1;
        assert_eq!(0, m.len());
        /*
        let entry = m.entry(&key1).or_insert_with(|| val1, &mut m);
        assert_eq!(m.get(&key1).unwrap(), &val1);
        */

        assert_eq!(1, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1]);
        m.insert(&key3, val3);
        assert_eq!(m.get(&key3).unwrap(), &val3);
        assert_eq!(2, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key3]);
        m.insert(&key2, val2);
        assert_eq!(m.get(&key2).unwrap(), &val2);
        assert_eq!(3, m.len());
        //assert_eq!(m.keys().collect::<Vec<_>>(), vec![&key1, &key2, &key3]);
    }
}
