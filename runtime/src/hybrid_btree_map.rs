use crate::accounts_index::{AccountMapEntry, AccountMapEntryInner};
use crate::accounts_index::{IsCached, RefCount, SlotList, WriteAccountMapEntry, BINS};
use crate::pubkey_bins::PubkeyBinCalculator16;
use solana_bucket_map::bucket_map::BucketMap;
use solana_measure::measure::Measure;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::fmt::Debug;
use std::ops::Bound;
use std::ops::{Range, RangeBounds};
use std::sync::atomic::Ordering;
use std::sync::{Arc, RwLock}; //, RwLockReadGuard, RwLockWriteGuard};

use std::time::Instant;

type K = Pubkey;
use std::collections::{hash_map::Entry, HashMap};
use std::convert::TryInto;

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
use std::hash::{BuildHasherDefault, Hasher};
#[derive(Debug, Default)]
pub struct MyHasher {
    hash: u64,
}

impl Hasher for MyHasher {
    fn write(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        let start = len - 8;
        self.hash ^= u64::from_be_bytes(bytes[start..].try_into().unwrap());
        //error!("hash bytes: {}", bytes.len());
        //assert_eq!(self.hash, 0);
        //self.hash = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        /*
        for i in 0..bytes.len() {
            self.hash = (self.hash << 1) ^ (bytes[i] as u64);
        }
        */
    }

    fn finish(&self) -> u64 {
        self.hash
    }
}
/*
pub struct myhash {

}
impl BuildHasher for myhash {
    type Hasher = MyHasher;
    fn build_hasher(&self) -> Self::Hasher
    {
        MyHasher::default()
    }
}*/
type MyBuildHasher = BuildHasherDefault<MyHasher>;

pub const DEFAULT_AGE: u8 = 2;
pub const VERIFY_GET_ON_INSERT: bool = false;
pub const BUCKET_BINS: usize = BINS;
pub const UPDATE_CACHING: bool = true;
pub const INSERT_CACHING: bool = true;
pub const GET_CACHING: bool = true;

pub type SlotT<T> = (Slot, T);

pub type WriteCache<V> = HashMap<Pubkey, V, MyBuildHasher>;
use crate::waitable_condvar::WaitableCondvar;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8};
use std::time::Duration;

pub type WriteCacheEntry<V> = AccountMapEntryInner<V>;

pub type WriteCacheEntryArc<V> = Arc<WriteCacheEntry<V>>;

type WriteCacheOuter<V> = WriteCache<WriteCacheEntryArc<V>>;

#[derive(Debug)]
pub struct BucketMapWriteHolder<V> {
    pub current_age: AtomicU8,
    pub disk: BucketMap<SlotT<V>>,
    pub write_cache: Vec<RwLock<WriteCacheOuter<V>>>,
    pub get_disk_us: AtomicU64,
    pub update_dist_us: AtomicU64,
    pub get_cache_us: AtomicU64,
    pub update_cache_us: AtomicU64,
    pub write_cache_flushes: AtomicU64,
    pub reinserted_in_flush: AtomicU64,
    pub bucket_flush_calls: AtomicU64,
    pub get_purges: AtomicU64,
    pub gets_from_disk: AtomicU64,
    pub gets_from_disk_empty: AtomicU64,
    pub gets_from_cache: AtomicU64,
    pub updates: AtomicU64,
    pub using_empty_get: AtomicU64,
    pub insert_without_lookup: AtomicU64,
    pub updates_in_cache: AtomicU64,
    pub addrefs: AtomicU64,
    pub unrefs: AtomicU64,
    pub range: AtomicU64,
    pub keys: AtomicU64,
    pub inserts: AtomicU64,
    pub inserts_without_checking_disk: AtomicU64,
    pub deletes: AtomicU64,
    pub bins: usize,
    pub wait: WaitableCondvar,
    binner: PubkeyBinCalculator16,
    pub unified_backing: bool,
}

impl<V: 'static + Clone + IsCached + Debug> BucketMapWriteHolder<V> {
    fn add_age(age: u8, inc: u8) -> u8 {
        age.wrapping_add(inc)
    }

    pub fn bg_flusher(&self, exit: Arc<AtomicBool>) {
        let mut found_one = false;
        let mut last = Instant::now();
        let mut aging = Instant::now();
        let mut current_age: u8 = 0;
        loop {
            if last.elapsed().as_millis() > 1000 {
                self.distribution2();
                last = Instant::now();
            }
            let mut age = None;
            if aging.elapsed().as_millis() > 400 {
                // time of 1 slot
                current_age = Self::add_age(current_age, 1); // % DEFAULT_AGE; // no reason to pass by something too often if we accidentally miss it...
                self.current_age.store(current_age, Ordering::Relaxed);
                age = Some(current_age);
                aging = Instant::now();
            }
            if exit.load(Ordering::Relaxed) {
                break;
            }
            if age.is_none() && (found_one || self.wait.wait_timeout(Duration::from_millis(500))) {
                continue;
            }
            found_one = false;
            for ix in 0..self.bins {
                if self.flush(ix, true, age) {
                    found_one = true;
                }
            }
        }
    }
    fn new(bucket_map: BucketMap<SlotT<V>>) -> Self {
        let current_age = AtomicU8::new(0);
        let get_disk_us = AtomicU64::new(0);
        let update_dist_us = AtomicU64::new(0);
        let get_cache_us = AtomicU64::new(0);
        let update_cache_us = AtomicU64::new(0);

        let addrefs = AtomicU64::new(0);
        let unrefs = AtomicU64::new(0);
        let range = AtomicU64::new(0);
        let keys = AtomicU64::new(0);
        let write_cache_flushes = AtomicU64::new(0);
        let reinserted_in_flush = AtomicU64::new(0);
        let bucket_flush_calls = AtomicU64::new(0);
        let get_purges = AtomicU64::new(0);
        let gets_from_disk = AtomicU64::new(0);
        let gets_from_disk_empty = AtomicU64::new(0);
        let using_empty_get = AtomicU64::new(0);
        let insert_without_lookup = AtomicU64::new(0);
        let gets_from_cache = AtomicU64::new(0);
        let updates_in_cache = AtomicU64::new(0);
        let inserts_without_checking_disk = AtomicU64::new(0);
        let updates = AtomicU64::new(0);
        let inserts = AtomicU64::new(0);
        let deletes = AtomicU64::new(0);
        let bins = bucket_map.num_buckets();
        let write_cache = (0..bins)
            .map(|_i| RwLock::new(WriteCache::default()))
            .collect::<Vec<_>>();
        let wait = WaitableCondvar::default();
        let binner = PubkeyBinCalculator16::new(BUCKET_BINS);
        let unified_backing = false;
        assert_eq!(BUCKET_BINS, bucket_map.num_buckets());

        Self {
            current_age,
            disk: bucket_map,
            write_cache,
            bins,
            wait,
            using_empty_get,
            insert_without_lookup,
            gets_from_cache,
            updates_in_cache,
            inserts_without_checking_disk,
            gets_from_disk,
            gets_from_disk_empty,
            deletes,
            write_cache_flushes,
            reinserted_in_flush,
            updates,
            inserts,
            binner,
            unified_backing,
            get_purges,
            bucket_flush_calls,
            get_disk_us,
            update_dist_us,
            get_cache_us,
            update_cache_us,
            addrefs,
            unrefs,
            range,
            keys,
        }
    }
    pub fn flush(&self, ix: usize, bg: bool, age: Option<u8>) -> bool {
        //error!("start flusha: {}", ix);
        let read_lock = self.write_cache[ix].read().unwrap();
        //error!("start flusha: {}", ix);
        let mut had_dirty = false;
        if read_lock.is_empty() {
            return false;
        }

        //error!("start flush: {}", ix);
        let (age_comp, do_age, next_age) = age
            .map(|age| (age, true, Self::add_age(age, DEFAULT_AGE)))
            .unwrap_or((0, false, 0));
        self.bucket_flush_calls.fetch_add(1, Ordering::Relaxed);
        let len = read_lock.len();
        let mut flush_keys = Vec::with_capacity(len);
        let mut delete_keys = Vec::with_capacity(len);
        let mut flushed = 0;
        let mut get_purges = 0;
        for (k, v) in read_lock.iter() {
            let instance = v; //.read().unwrap();
            let dirty = instance.dirty.load(Ordering::Relaxed);
            let mut flush = dirty;
            let keep_this_in_cache = Self::in_cache(&instance.slot_list.read().unwrap()); // this keeps 30M accts in mem on mnb || instance.slot_list.len() > 1;
            if bg && keep_this_in_cache {
                // for all account indexes that are in the write cache instead of storage, don't write them to disk yet - they will be updated soon
                flush = false;
            }
            if flush {
                flush_keys.push(*k);
                had_dirty = true;
            }
            if do_age {
                if !dirty && instance.age.load(Ordering::Relaxed) == age_comp && !keep_this_in_cache {
                    // clear the cache of things that have aged out
                    delete_keys.push(*k);
                    get_purges += 1;
                    /*
                    // if we should age this key out, then go ahead and flush it
                    if !flush && dirty {
                        flush_keys.push(*k);
                    }
                    */
                }
            }
        }
        drop(read_lock);

        {
            let wc = &mut self.write_cache[ix].write().unwrap(); // maybe get lock for each item?
            for k in flush_keys.into_iter() {
                match wc.entry(k) {
                    Entry::Occupied(occupied) => {
                        let instance = occupied.get();
                        if instance.dirty.load(Ordering::Relaxed) {
                            self.update_no_cache(
                                occupied.key(),
                                |_current| {
                                    Some((
                                        instance.slot_list.read().unwrap().clone(), // unfortunate copy
                                        instance.ref_count.load(Ordering::Relaxed), // does relaxed work?
                                    ))
                                },
                                None,
                                true,
                            );
                            instance.age.store(next_age, Ordering::Relaxed); // keep newly updated stuff around
                            instance.dirty.store(false, Ordering::Relaxed);
                            flushed += 1;
                        }
                    }
                    Entry::Vacant(_vacant) => { // do nothing, item is no longer in cache somehow, so obviously not dirty
                    }
                }
            }
        }
        if true
        {
            let wc = &mut self.write_cache[ix].write().unwrap(); // maybe get lock for each item?
            for k in delete_keys.iter() {
                if let Some(item) = wc.remove(k) {
                    // if someone else dirtied it or aged it newer, so re-insert it into the cache
                    // we only had a read lock when we gathered this list.
                    // In transitioning to a write lock, we could have had someone modify something or update its age.
                    if item.dirty.load(Ordering::Relaxed)
                        || (do_age && item.age.load(Ordering::Relaxed) != age_comp)
                    {
                        self.reinserted_in_flush.fetch_add(1, Ordering::Relaxed);
                        wc.insert(*k, item);
                    }
                    // otherwise, we were ok to delete that key from the cache
                }
            }
        }

        if flushed != 0 {
            self.write_cache_flushes
                .fetch_add(flushed as u64, Ordering::Relaxed);
        }
        if get_purges != 0 {
            self.get_purges
                .fetch_add(get_purges as u64, Ordering::Relaxed);
        }
        //error!("end flush: {}", ix);

        had_dirty
    }
    pub fn bucket_len(&self, ix: usize) -> u64 {
        self.disk.bucket_len(ix)
    }
    fn read_be_u64(input: &[u8]) -> u64 {
        assert!(input.len() >= std::mem::size_of::<u64>());
        u64::from_be_bytes(input[0..std::mem::size_of::<u64>()].try_into().unwrap())
    }
    pub fn bucket_ix(&self, key: &Pubkey) -> usize {
        let b = self.binner.bin_from_pubkey(key);
        assert_eq!(
            b,
            self.disk.bucket_ix(key),
            "be {}",
            Self::read_be_u64(key.as_ref()),
        );
        b
    }
    pub fn keys3<R: RangeBounds<Pubkey>>(
        &self,
        ix: usize,
        _range: Option<&R>,
    ) -> Option<Vec<Pubkey>> {
        self.keys.fetch_add(1, Ordering::Relaxed);
        self.flush(ix, false, None);
        let values = self.disk.keys(ix);
        if false {
            let read = self.write_cache[ix].read().unwrap();
            let mut values_temp = read.keys().collect::<Vec<_>>();
            if values.is_none() {
                assert!(values_temp.is_empty());
                return None;
            }
            if false {
                assert_eq!(values.as_ref().unwrap().len(), values_temp.len());
                for i in values.as_ref().unwrap().iter() {
                    for k in 0..values_temp.len() {
                        if values_temp[k] == i {
                            values_temp.remove(k);
                            break;
                        }
                    }
                }
                assert!(values_temp.is_empty());
            }
        }
        values
    }
    pub fn values<R: RangeBounds<Pubkey>>(
        &self,
        ix: usize,
        _range: Option<&R>, // todo: use range, or maybe not
    ) -> Option<Vec<Vec<SlotT<V>>>> {
        self.flush(ix, false, None);
        let values = self.disk.values(ix);
        if false {
            if false {
                let read = self.write_cache[ix].read().unwrap();
                let mut values_temp = read.values().collect::<Vec<_>>();
                if values.is_none() {
                    assert!(values_temp.is_empty());
                    return None;
                }
                assert_eq!(values.as_ref().unwrap().len(), values_temp.len());
                for i in values.as_ref().unwrap().iter() {
                    let mut found = false;
                    for k in 0..values_temp.len() {
                        if &values_temp[k].slot_list.read().unwrap().clone() == i {
                            values_temp.remove(k);
                            found = true;
                            break;
                        }
                    }
                    assert!(found);
                }
                assert!(values_temp.is_empty());
            }
            else {
                let read = self.write_cache[ix].read().unwrap();
                let mut values_temp = read.values().collect::<Vec<_>>();
                if values.is_none() {
                    assert!(values_temp.is_empty());
                    return None;
                }
                assert_eq!(values.as_ref().unwrap().len(), values_temp.len());
                for i in values.as_ref().unwrap().iter() {
                    for k in 0..values_temp.len() {
                        assert!(!values_temp[k].dirty.load(Ordering::Relaxed));
                        if &values_temp[k].slot_list.read().unwrap().clone() == i {
                            values_temp.remove(k);
                            break;
                        }
                    }
                }
                assert!(values_temp.is_empty());
            }
        }
        values
    }

    pub fn num_buckets(&self) -> usize {
        self.disk.num_buckets()
    }
    /*
        fn update_lazy_from_disk(pubkey: &Pubkey,        entry: &WriteCacheEntryArc<V>,
            mut new_value: AccountMapEntry<V>,
            reclaims: &mut SlotList<V>,
            reclaims_must_be_empty: bool,
        ) {
            // we have 1 entry for this pubkey. it was previously inserted, but we haven't checked to see if there is more info for this pubkey on disk already
            // todo
        }
    */

    fn upsert_item_in_cache(
        &self,
        instance: &WriteCacheEntryArc<V>,
        new_value: AccountMapEntry<V>,
        reclaims: &mut SlotList<V>,
        reclaims_must_be_empty: bool,
        mut m1: Measure,
    ) {
        /*
        if instance.must_do_lookup_from_disk {
            // todo
        }
        */

        instance
            .age
            .store(self.set_age_to_future(), Ordering::Relaxed);
        instance.dirty.store(true, Ordering::Relaxed);
        if instance.confirmed_not_on_disk.load(Ordering::Relaxed) {
            self.using_empty_get.fetch_add(1, Ordering::Relaxed);
            instance.ref_count.store(0, Ordering::Relaxed);
            assert!(instance.slot_list.read().unwrap().is_empty());
            instance
                .confirmed_not_on_disk
                .store(false, Ordering::Relaxed); // we are inserted now if we were 'confirmed_not_on_disk' before. update_static below handles this fine
        }
        self.updates_in_cache.fetch_add(1, Ordering::Relaxed);

        let current = instance;
        let (slot, new_entry) = new_value.slot_list.write().unwrap().remove(0);
        let addref = WriteAccountMapEntry::update_static(
            &mut current.slot_list.write().unwrap(),
            slot,
            new_entry,
            reclaims,
            reclaims_must_be_empty,
        );
        if addref {
            current.ref_count.fetch_add(1, Ordering::Relaxed);
        }
        m1.stop();
        self.update_cache_us
            .fetch_add(m1.as_ns(), Ordering::Relaxed);
    }

    pub fn upsert(
        &self,
        key: &Pubkey,
        new_value: AccountMapEntry<V>,
        reclaims: &mut SlotList<V>,
        reclaims_must_be_empty: bool,
    ) {
        let must_do_lookup_from_disk = false;
        let confirmed_not_on_disk = false;
        /*
        let k = Pubkey::from_str("5x3NHJ4VEu2abiZJ5EHEibTc2iqW22Lc245Z3fCwCxRS").unwrap();
        if key == k {
            error!("{} {} upsert {}, {:?}", file!(), line!(), key, new_value);
        }
        */

        let ix = self.bucket_ix(&key);
        // try read lock first
        {
            let m1 = Measure::start("");
            let wc = &mut self.write_cache[ix].read().unwrap();
            let res = wc.get(&key);
            if let Some(occupied) = res {
                // already in cache, so call update_static function
                self.upsert_item_in_cache(
                    occupied,
                    new_value,
                    reclaims,
                    reclaims_must_be_empty,
                    m1,
                );
                return;
            }
        }

        // try write lock
        let mut m1 = Measure::start("");
        let wc = &mut self.write_cache[ix].write().unwrap();
        let res = wc.entry(key.clone());
        match res {
            Entry::Occupied(occupied) => {
                // already in cache, so call update_static function
                self.upsert_item_in_cache(
                    occupied.get(),
                    new_value,
                    reclaims,
                    reclaims_must_be_empty,
                    m1,
                );
                return;
            }
            Entry::Vacant(vacant) => {
                if !reclaims_must_be_empty {
                    if 1000 <= self.insert_without_lookup.fetch_add(1, Ordering::Relaxed) {
                        //panic!("upsert without reclaims");
                    }
                }
                if reclaims_must_be_empty && false {
                    // todo
                    // we don't have to go to disk to look this thing up yet
                    // not on disk, not in cache, so add to cache
                    let must_do_lookup_from_disk = true;
                    self.inserts_without_checking_disk
                        .fetch_add(1, Ordering::Relaxed);
                    vacant.insert(self.allocate(
                        &new_value.slot_list.read().unwrap(),
                        new_value.ref_count.load(Ordering::Relaxed),
                        true,
                        true,
                        must_do_lookup_from_disk,
                        confirmed_not_on_disk,
                    ));
                    return; // we can be satisfied that this index will be looked up later
                }
                let r = self.get_no_cache(&key); // maybe move this outside lock - but race conditions unclear
                if let Some(mut current) = r {
                    if 1000 <= self.updates.fetch_add(1, Ordering::Relaxed) {
                        //panic!("1000th update without loading");
                    }
                    let (slot, new_entry) = new_value.slot_list.write().unwrap().remove(0);
                    let addref = WriteAccountMapEntry::update_static(
                        &mut current.1,
                        slot,
                        new_entry,
                        reclaims,
                        reclaims_must_be_empty,
                    );
                    if addref {
                        current.0 += 1;
                    }
                    vacant.insert(self.allocate(
                        &current.1,
                        current.0,
                        true,
                        false,
                        must_do_lookup_from_disk,
                        confirmed_not_on_disk,
                    ));
                    m1.stop();
                    self.update_cache_us
                        .fetch_add(m1.as_ns(), Ordering::Relaxed);
                } else {
                    // not on disk, not in cache, so add to cache
                    self.inserts.fetch_add(1, Ordering::Relaxed);
                    new_value.dirty.store(true, Ordering::Relaxed);
                    new_value.insert.store(true, Ordering::Relaxed);
                    new_value
                        .insert
                        .store(must_do_lookup_from_disk, Ordering::Relaxed);
                    new_value
                        .insert
                        .store(confirmed_not_on_disk, Ordering::Relaxed);
                    vacant.insert(new_value);
                }
            }
        }
    }

    fn in_cache(slot_list: &SlotList<V>) -> bool {
        slot_list.iter().any(|(_slot, info)| info.is_cached())
    }

    pub fn update_no_cache<F>(
        &self,
        key: &Pubkey,
        updatefn: F,
        current_value: Option<&V2<V>>,
        force_not_insert: bool,
    ) where
        F: Fn(Option<(&[SlotT<V>], u64)>) -> Option<(Vec<SlotT<V>>, u64)>,
    {
        let must_do_lookup_from_disk = false;
        let confirmed_not_on_disk = false;

        //let k = Pubkey::from_str("D5vcuUjK4uPSg1Cy9hoTPWCodFcLv6MyfWFNmRtbEofL").unwrap();
        if current_value.is_none() && !force_not_insert {
            if true {
                // send straight to disk. if we try to keep it in the write cache, then 'keys' will be incorrect
                self.disk.update(key, updatefn);
            } else {
                let result = updatefn(None);
                if let Some(result) = result {
                    // stick this in the write cache and flush it later
                    let ix = self.bucket_ix(key);
                    let wc = &mut self.write_cache[ix].write().unwrap();
                    wc.insert(
                        key.clone(),
                        self.allocate(
                            &result.0,
                            result.1,
                            true,
                            true,
                            must_do_lookup_from_disk,
                            confirmed_not_on_disk,
                        ),
                    );
                } else {
                    panic!("should have returned a value from updatefn");
                }
            }
        } else {
            // update
            //let current_value = current_value.map(|entry| (&entry.slot_list[..], entry.ref_count));
            self.disk.update(key, updatefn); //|_current| Some((v.0.clone(), v.1)));
        }
    }

    fn allocate(
        &self,
        slot_list: &SlotList<V>,
        ref_count: RefCount,
        dirty: bool,
        insert: bool,
        must_do_lookup_from_disk: bool,
        confirmed_not_on_disk: bool,
    ) -> WriteCacheEntryArc<V> {
        assert!(!(insert && !dirty));
        AccountMapEntryInner::new(
            slot_list.clone(), /* clone here */
            ref_count,
            dirty,
            insert,
            self.set_age_to_future(),
            must_do_lookup_from_disk,
            confirmed_not_on_disk,
        )
    }

    fn upsert_in_cache<F>(&self, key: &Pubkey, updatefn: F, current_value: Option<&V2<V>>)
    where
        F: Fn(Option<(&[SlotT<V>], u64)>) -> Option<(Vec<SlotT<V>>, u64)>,
    {
        let must_do_lookup_from_disk = false;
        let confirmed_not_on_disk = false;
        let result = current_value.and_then(|entry| {
            let current_value = (
                &entry.slot_list.read().unwrap()[..],
                entry.ref_count.load(Ordering::Relaxed),
            );
            updatefn(Some(current_value))
        });
        // we are an update
        if let Some(result) = result {
            // stick this in the write cache and flush it later
            let ix = self.bucket_ix(key);
            let wc = &mut self.write_cache[ix].write().unwrap();

            match wc.entry(key.clone()) {
                Entry::Occupied(mut occupied) => {
                    let gm = occupied.get_mut();
                    *gm = self.allocate(
                        &result.0,
                        result.1,
                        true,
                        false,
                        must_do_lookup_from_disk,
                        confirmed_not_on_disk,
                    );
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(self.allocate(
                        &result.0,
                        result.1,
                        true,
                        true,
                        must_do_lookup_from_disk,
                        confirmed_not_on_disk,
                    ));
                    self.wait.notify_all(); // we have put something in the write cache that needs to be flushed sometime
                }
            }
        } else {
            panic!("expected value");
        }
    }

    pub fn update<F>(&self, key: &Pubkey, updatefn: F, current_value: Option<&V2<V>>)
    where
        F: Fn(Option<(&[SlotT<V>], u64)>) -> Option<(Vec<SlotT<V>>, u64)>,
    {
        /*
        let k = Pubkey::from_str("5x3NHJ4VEu2abiZJ5EHEibTc2iqW22Lc245Z3fCwCxRS").unwrap();
        if key == &k {
            error!("{} {} update {}", file!(), line!(), key);
        }
        */

        if current_value.is_none() {
            // we are an insert
            self.inserts.fetch_add(1, Ordering::Relaxed);

            if INSERT_CACHING {
                self.upsert_in_cache(key, updatefn, current_value);
            } else {
                self.update_no_cache(key, updatefn, current_value, false);
            }
        } else {
            if VERIFY_GET_ON_INSERT {
                panic!("");
                //assert!(self.get(key).is_some());
            }
            // if we have a previous value, then that item is currently open and locked, so it could not have been changed. Thus, this is an in-cache update as long as we are caching gets.
            self.updates_in_cache.fetch_add(1, Ordering::Relaxed);
            if UPDATE_CACHING {
                self.upsert_in_cache(key, updatefn, current_value);
            } else {
                self.update_no_cache(key, updatefn, current_value, false);
            }
        }
    }
    pub fn get_no_cache(&self, key: &Pubkey) -> Option<(u64, Vec<SlotT<V>>)> {
        let mut m1 = Measure::start("");
        let r = self.disk.get(key);
        m1.stop();
        self.get_disk_us.fetch_add(m1.as_ns(), Ordering::Relaxed);
        if r.is_some() {
            self.gets_from_disk.fetch_add(1, Ordering::Relaxed);
        } else {
            self.gets_from_disk_empty.fetch_add(1, Ordering::Relaxed);
        }
        r
    }

    fn set_age_to_future(&self) -> u8 {
        let current_age = self.current_age.load(Ordering::Relaxed);
        Self::add_age(current_age, DEFAULT_AGE)
    }

    pub fn get2(&self, key: &Pubkey) -> Option<(u64, Vec<SlotT<V>>)> {
        self.disk.get(key)
        /*
        /*
        let k = Pubkey::from_str("5x3NHJ4VEu2abiZJ5EHEibTc2iqW22Lc245Z3fCwCxRS").unwrap();
        if key == &k {
            error!("{} {} get {}", file!(), line!(), key);
        }
        */
        let must_do_lookup_from_disk = false;
        let mut confirmed_not_on_disk = false;
        let ix = self.bucket_ix(key);
        {
            let mut m1 = Measure::start("");
            let wc = &mut self.write_cache[ix].read().unwrap();
            let res = wc.get(key);
            m1.stop();
            if let Some(instance) = res {
                if GET_CACHING {
                    instance.age.store(self.set_age_to_future(), Ordering::Relaxed);
                    self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                    if instance.confirmed_not_on_disk.load(Ordering::Relaxed) {
                        return None; // does not really exist
                    }
                    panic!("");
                    //return None;
                    /*
                    let r = Some((instance.ref_count, instance.slot_list.clone()));
                    self.get_cache_us.fetch_add(m1.as_ns(), Ordering::Relaxed);
                    return r;
                    */
                } else {
                    panic!("");
                    /*
                    self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                    return Some((instance.ref_count, instance.slot_list.clone()));
                    */
                }
            }
            if !GET_CACHING {
                return self.get_no_cache(key);
            }
        }
        panic!("");
        /*
        // get caching
        {
            let mut m1 = Measure::start("");
            let wc = &mut self.write_cache[ix].write().unwrap();
            let res = wc.entry(key.clone());
            match res {
                Entry::Occupied(occupied) => {
                    let mut instance = occupied.get().write().unwrap();
                    instance.age = self.set_age_to_future();
                    if instance.confirmed_not_on_disk {
                        return None; // does not really exist
                    }
                    self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                    m1.stop();
                    self.update_cache_us
                        .fetch_add(m1.as_ns(), Ordering::Relaxed);
                    return Some((instance.ref_count, instance.slot_list.clone()));
                }
                Entry::Vacant(vacant) => {
                    let r = self.get_no_cache(key);
                    if let Some(loaded) = &r {
                        vacant.insert(self.allocate(&loaded.1, loaded.0, false, false, must_do_lookup_from_disk, confirmed_not_on_disk));
                    }
                    else {
                        // we looked this up. it does not exist. let's insert a marker in the cache saying it doesn't exist. otherwise, we have to look it up again on insert!
                        confirmed_not_on_disk = true;
                        vacant.insert(self.allocate(&SlotList::default(), RefCount::MAX, false, false, must_do_lookup_from_disk, confirmed_not_on_disk));
                    }
                    m1.stop();
                    self.update_cache_us
                        .fetch_add(m1.as_ns(), Ordering::Relaxed);
                    r
                }
            }
        }
        */
        */
    }
    fn addunref(&self, key: &Pubkey, _ref_count: RefCount, _slot_list: &Vec<SlotT<V>>, add: bool) {
        // todo: measure this and unref
        let ix = self.bucket_ix(key);
        let mut m1 = Measure::start("");
        let wc = &mut self.write_cache[ix].write().unwrap();

        let res = wc.entry(key.clone());
        m1.stop();
        self.get_cache_us.fetch_add(m1.as_ns(), Ordering::Relaxed);
        match res {
            Entry::Occupied(mut occupied) => {
                self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                let instance = occupied.get_mut();
                if !instance.confirmed_not_on_disk.load(Ordering::Relaxed) {
                    if add {
                        instance.ref_count.fetch_add(1, Ordering::Relaxed);
                    } else {
                        instance.ref_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    instance.dirty.store(true, Ordering::Relaxed);
                }
            }
            Entry::Vacant(_vacant) => {
                self.gets_from_disk.fetch_add(1, Ordering::Relaxed);
                if add {
                    self.disk.addref(key);
                } else {
                    self.disk.unref(key);
                }
            }
        }
    }
    pub fn addref(&self, key: &Pubkey, ref_count: RefCount, slot_list: &Vec<SlotT<V>>) {
        self.addrefs.fetch_add(1, Ordering::Relaxed);
        self.addunref(key, ref_count, slot_list, true);
    }

    pub fn unref(&self, key: &Pubkey, ref_count: RefCount, slot_list: &Vec<SlotT<V>>) {
        self.unrefs.fetch_add(1, Ordering::Relaxed);
        self.addunref(key, ref_count, slot_list, false);
    }
    fn delete_key(&self, key: &Pubkey) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
        let ix = self.bucket_ix(key);
        {
            let wc = &mut self.write_cache[ix].write().unwrap();
            wc.remove(key);
            //error!("remove: {}", key);
        }
        self.disk.delete_key(key)
    }
    pub fn distribution(&self) {}
    pub fn distribution2(&self) {
        let mut ct = 0;
        for i in 0..self.bins {
            ct += self.write_cache[i].read().unwrap().len();
        }
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
        datapoint_info!(
            "accounts_index",
            ("items_in_write_cache", ct, i64),
            ("min", min, i64),
            ("max", max, i64),
            ("sum", sum, i64),
            (
                "updates_not_in_cache",
                self.updates.swap(0, Ordering::Relaxed),
                i64
            ),
            //("updates_not_in_cache", self.updates.load(Ordering::Relaxed), i64),
            (
                "updates_in_cache",
                self.updates_in_cache.swap(0, Ordering::Relaxed),
                i64
            ),
            ("inserts", self.inserts.swap(0, Ordering::Relaxed), i64),
            (
                "inserts_without_checking_disk",
                self.inserts_without_checking_disk
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            ("deletes", self.deletes.swap(0, Ordering::Relaxed), i64),
            (
                "using_empty_get",
                self.using_empty_get.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "insert_without_lookup",
                self.insert_without_lookup.swap(0, Ordering::Relaxed),
                i64
            ),
            //("insert_without_lookup", self.insert_without_lookup.load(Ordering::Relaxed), i64),
            (
                "gets_from_disk_empty",
                self.gets_from_disk_empty.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "gets_from_disk",
                self.gets_from_disk.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "gets_from_cache",
                self.gets_from_cache.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bucket_index_resizes",
                self.disk.stats.index.resizes.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bucket_index_resize_us",
                self.disk.stats.index.resize_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bucket_index_max",
                *self.disk.stats.index.max_size.lock().unwrap(),
                i64
            ),
            (
                "bucket_data_resizes",
                self.disk.stats.data.resizes.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bucket_data_resize_us",
                self.disk.stats.data.resize_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bucket_data_max",
                *self.disk.stats.data.max_size.lock().unwrap(),
                i64
            ),
            (
                "reinserted_in_flush",
                self.reinserted_in_flush.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "flushes",
                self.write_cache_flushes.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bin_flush_calls",
                self.bucket_flush_calls.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_purges",
                self.get_purges.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "get_disk_us",
                self.get_disk_us.swap(0, Ordering::Relaxed) / 1000,
                i64
            ),
            (
                "update_dist_us",
                self.update_dist_us.swap(0, Ordering::Relaxed) / 1000,
                i64
            ),
            (
                "get_cache_us",
                self.get_cache_us.swap(0, Ordering::Relaxed) / 1000,
                i64
            ),
            (
                "update_cache_us",
                self.update_cache_us.swap(0, Ordering::Relaxed) / 1000,
                i64
            ),
            ("addrefs", self.addrefs.swap(0, Ordering::Relaxed), i64),
            ("unrefs", self.unrefs.swap(0, Ordering::Relaxed), i64),
            ("range", self.range.swap(0, Ordering::Relaxed), i64),
            ("keys", self.keys.swap(0, Ordering::Relaxed), i64),
            ("get", self.updates.swap(0, Ordering::Relaxed), i64),
            //("buckets", self.num_buckets(), i64),
        );
    }
}

#[derive(Debug)]
pub struct HybridBTreeMap<V: 'static + Clone + IsCached + Debug> {
    disk: Arc<BucketMapWriteHolder<V>>,
    bin_index: usize,
    bins: usize,
}

pub struct Keys {
    keys: Vec<K>,
    index: usize,
}

impl Keys {
    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

impl Iterator for Keys {
    type Item = Pubkey;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.keys.len() {
            None
        } else {
            let r = Some(self.keys[self.index]);
            self.index += 1;
            r
        }
    }
}

pub struct Values<V: Clone + std::fmt::Debug> {
    values: Vec<SlotList<V>>,
    index: usize,
}

impl<V: Clone + std::fmt::Debug> Iterator for Values<V> {
    type Item = V2<V>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.values.len() {
            None
        } else {
            let r = Some(
                AccountMapEntryInner::new_simple(self.values[self.index].clone(), RefCount::MAX,)) // todo: no clone
            ;
            self.index += 1;
            r
        }
    }
}

impl<V: IsCached> HybridBTreeMap<V> {
    pub fn new2(
        bucket_map: &Arc<BucketMapWriteHolder<V>>,
        bin_index: usize,
        bins: usize,
    ) -> HybridBTreeMap<V> {
        Self {
            disk: bucket_map.clone(),
            bin_index,
            bins: bins, //bucket_map.num_buckets(),
        }
    }
    pub fn new_bucket_map() -> Arc<BucketMapWriteHolder<V>> {
        let buckets = PubkeyBinCalculator16::log_2(BUCKET_BINS as u32) as u8; // make more buckets to try to spread things out
        Arc::new(BucketMapWriteHolder::new(BucketMap::new_buckets(buckets)))
    }

    pub fn flush(&self) {
        let num_buckets = self.disk.num_buckets();
        let mystart = num_buckets * self.bin_index / self.bins;
        let myend = num_buckets * (self.bin_index + 1) / self.bins;
        (mystart..myend).for_each(|ix| {
            self.disk.flush(ix, false, None);
        });

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
        self.disk.distribution();
    }
    fn bound<'a, T>(bound: Bound<&'a T>, unbounded: &'a T) -> &'a T {
        match bound {
            Bound::Included(b) | Bound::Excluded(b) => b,
            _ => unbounded,
        }
    }
    pub fn range<R>(&self, range: Option<R>) -> Keys
    where
        R: RangeBounds<Pubkey> + Debug,
    {
        //self.disk.range.fetch_add(1, Ordering::Relaxed);

        let num_buckets = self.disk.num_buckets();
        if self.bin_index != 0 && self.disk.unified_backing {
            return Keys {
                keys: vec![],
                index: 0,
            };
        }
        let mut start = 0;
        let mut end = num_buckets;
        if let Some(range) = &range {
            let default = Pubkey::default();
            let max = Pubkey::new(&[0xff; 32]);
            let start_bound = Self::bound(range.start_bound(), &default);
            start = self.disk.bucket_ix(start_bound);
            // end is exclusive, so it is end + 1 we care about here
            let end_bound = Self::bound(range.end_bound(), &max);
            end = std::cmp::min(num_buckets, 1 + self.disk.bucket_ix(end_bound)); // ugly
            assert!(
                start_bound <= end_bound,
                "range start is greater than range end"
            );
        }
        let len = (start..end)
            .into_iter()
            .map(|ix| self.disk.bucket_len(ix) as usize)
            .sum::<usize>();

        let mystart = num_buckets * self.bin_index / self.bins;
        let myend = num_buckets * (self.bin_index + 1) / self.bins;
        start = std::cmp::max(start, mystart);
        end = std::cmp::min(end, myend);
        let mut keys = Vec::with_capacity(len);
        (start..end).into_iter().for_each(|ix| {
            let ks = self.disk.keys3(ix, range.as_ref()).unwrap_or_default();
            for k in ks.into_iter() {
                if range.is_none() || range.as_ref().unwrap().contains(&k) {
                    keys.push(k);
                }
            }
        });
        keys.sort_unstable();
        Keys { keys, index: 0 }
    }

    pub fn keys2(&self) -> Keys {
        // used still?
        let num_buckets = self.disk.num_buckets();
        let start = num_buckets * self.bin_index / self.bins;
        let end = num_buckets * (self.bin_index + 1) / self.bins;
        let len = (start..end)
            .into_iter()
            .map(|ix| self.disk.bucket_len(ix) as usize)
            .sum::<usize>();
        let mut keys = Vec::with_capacity(len);
        let _len = (start..end).into_iter().for_each(|ix| {
            keys.append(
                &mut self
                    .disk
                    .keys3(ix, None::<&Range<Pubkey>>)
                    .unwrap_or_default(),
            )
        });
        keys.sort_unstable();
        Keys { keys, index: 0 }
    }
    pub fn values(&self) -> Values<V> {
        let num_buckets = self.disk.num_buckets();
        if self.bin_index != 0 && self.disk.unified_backing {
            return Values {
                values: vec![],
                index: 0,
            };
        }
        // todo: this may be unsafe if we are asking for things with an update cache active. thankfully, we only call values at startup right now
        let start = num_buckets * self.bin_index / self.bins;
        let end = num_buckets * (self.bin_index + 1) / self.bins;
        let len = (start..end)
            .into_iter()
            .map(|ix| self.disk.bucket_len(ix) as usize)
            .sum::<usize>();
        let mut values = Vec::with_capacity(len);
        (start..end).into_iter().for_each(|ix| {
            values.append(
                &mut self
                    .disk
                    .values(ix, None::<&Range<Pubkey>>)
                    .unwrap_or_default(),
            )
        });
        //error!("getting values: {}, bin: {}, bins: {}, start: {}, end: {}", values.len(), self.bin_index, self.bins, start, end);
        //keys.sort_unstable();
        if self.bin_index == 0 {
            //error!("getting values: {}, {}, {}", values.len(), start, end);
        }
        Values { values, index: 0 }
    }
    pub fn len_inaccurate(&self) -> usize {
        1 // ??? wrong
          //self.in_memory.len()
    }

    pub fn upsert(
        &self,
        pubkey: &Pubkey,
        new_value: AccountMapEntry<V>,
        reclaims: &mut SlotList<V>,
        reclaims_must_be_empty: bool,
    ) {
        self.disk
            .upsert(pubkey, new_value, reclaims, reclaims_must_be_empty);
    }

    pub fn entry<'a, F, R, S>(&'a self, key: K, data: S, mut process_entry: F) -> R
    where
        F: FnMut(Entry<'_, K, V2<V>>, S) -> R,
        R: 'a,
        S: 'a,
    {
        let wc2 = &self.disk.write_cache[self.bin_index];
        let mut wc = wc2.write().unwrap();
        let entry = wc.entry(key);
        if matches!(entry, Entry::Occupied(_)) {
            /* todo
            if occupied.confirmed_not_on_disk.load(Ordering::Relaxed) {
                return
            }
            */
            return process_entry(entry, data);
        }
        match entry {
            Entry::Occupied(_occupied) => {
                panic!("");
            }
            Entry::Vacant(vacant) => {
                // not in cache, so create it
                let get = self.disk.get2(vacant.key());
                match get {
                    Some((ref_count, slot_list)) => {
                        vacant.insert(AccountMapEntryInner::new(
                            slot_list,
                            ref_count,
                            false,
                            false,
                            self.disk.set_age_to_future(),
                            false,
                            false,
                        ));
                    }
                    None => {}
                }
                drop(wc);
                return self.entry_direct(key, data, process_entry); // have to look up the entry again
            }
        }
    }

    pub fn entry_direct<'a, F, R, S>(&'a self, key: K, data: S, mut process_entry: F) -> R
    where
        F: FnMut(Entry<'_, K, V2<V>>, S) -> R,
        R: 'a,
        S: 'a,
    {
        let wc2 = &self.disk.write_cache[self.bin_index];
        let mut wc = wc2.write().unwrap();
        let entry = wc.entry(key);
        process_entry(entry, data)
    }

    /*
    pub fn insert(&mut self, key: K, value: V2<V>) {
        match self.entry(key) {
            Entry::Occupied(_occupied) => {
                panic!("");
            }
            Entry::Vacant(vacant) => vacant.insert(value),
        }
    }
    */

    pub fn get(&self, key: &K) -> Option<V2<V>> {
        // copy for now
        // could try read lock first here
        self.entry(*key, 0u8, |entry, _| {
            match entry {
                Entry::Occupied(occupied) => {
                    let get = occupied.get();
                    if get.confirmed_not_on_disk.load(Ordering::Relaxed) {
                        None
                    } else {
                        Some(get.clone())
                    }
                }
                Entry::Vacant(_vacant) => {
                    // not on disk, not in cache
                    None
                }
            }
        })
    }
    pub fn remove(&mut self, key: &K) {
        self.disk.delete_key(key);
    }
}
