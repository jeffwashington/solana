use crate::accounts_index::AccountMapEntry;
use crate::accounts_index::{IsCached, RefCount, SlotList, WriteAccountMapEntry, BINS_FOR_TESTING};
use crate::pubkey_bins::PubkeyBinCalculator16;
use solana_bucket_map::bucket_map::BucketMap;
use solana_measure::measure::Measure;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::collections::btree_map::BTreeMap;
use std::fmt::Debug;
use std::ops::Bound;
use std::ops::{Range, RangeBounds};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Instant;

type K = Pubkey;
use std::collections::{hash_map::Entry as HashMapEntry, HashMap};
use std::convert::TryInto;

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

pub const DEFAULT_AGE: u8 = 25;
pub const VERIFY_GET_ON_INSERT: bool = false;
pub const UPDATE_CACHING: bool = true;
pub const INSERT_CACHING: bool = true;
pub const GET_CACHING: bool = true;

pub type SlotT<T> = (Slot, T);

pub type WriteCache<V> = HashMap<Pubkey, V, MyBuildHasher>;
use crate::waitable_condvar::WaitableCondvar;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8};
use std::time::Duration;

#[derive(Debug)]
pub struct WriteCacheEntry<V> {
    pub data: V2<V>,
    pub age: u8,
    pub dirty: bool,
    pub insert: bool,
    pub must_do_lookup_from_disk: bool,
    pub confirmed_not_on_disk: bool,
}

#[derive(Debug)]
pub struct WriteCacheEntryArc<V> {
    pub instance: RwLock<WriteCacheEntry<V>>, //Arc<WriteCacheEntry<V>>,
}

#[derive(Debug)]
pub struct BucketMapWriteHolder<V> {
    pub current_age: AtomicU8,
    pub disk: BucketMap<SlotT<V>>,
    pub write_cache: Vec<RwLock<WriteCache<WriteCacheEntryArc<V>>>>,
    pub get_disk_us: AtomicU64,
    pub update_dist_us: AtomicU64,
    pub get_cache_us: AtomicU64,
    pub update_cache_us: AtomicU64,
    pub write_cache_flushes: AtomicU64,
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

impl<V: IsCached> BucketMapWriteHolder<V> {
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
        let binner = PubkeyBinCalculator16::new(bins);
        let unified_backing = false;
        assert_eq!(bins, bucket_map.num_buckets());

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
            let instance = v.instance.read().unwrap();
            let mut flush = instance.dirty;
            let keep_this_in_cache = Self::in_cache(&instance.data.slot_list); // this keeps 30M accts in mem on mnb || instance.data.slot_list.len() > 1;
            if bg && keep_this_in_cache {
                // for all account indexes that are in the write cache instead of storage, don't write them to disk yet - they will be updated soon
                flush = false;
            }
            if flush {
                flush_keys.push(*k);
                had_dirty = true;
            }
            if do_age {
                if instance.age == age_comp && !keep_this_in_cache {
                    // clear the cache of things that have aged out
                    delete_keys.push(*k);
                    get_purges += 1;

                    // if we should age this key out, then go ahead and flush it
                    if !flush && instance.dirty {
                        flush_keys.push(*k);
                    }
                }
            }
        }
        drop(read_lock);

        {
            let wc = &mut self.write_cache[ix].write().unwrap(); // maybe get lock for each item?
            for k in flush_keys.into_iter() {
                match wc.entry(k) {
                    HashMapEntry::Occupied(occupied) => {
                        //error!("mid flush: {}, {}", ix, line!());
                        let mut instance = occupied.get().instance.write().unwrap();
                        if instance.dirty {
                            //error!("mid flush: {}, {}", ix, line!());
                            self.update_no_cache(
                                occupied.key(),
                                |_current| {
                                    Some((instance.data.slot_list.clone(), instance.data.ref_count))
                                },
                                None,
                                true,
                            );
                            //error!("mid flush: {}, {}", ix, line!());
                            instance.age = next_age; // keep newly updated stuff around
                            instance.dirty = false;
                            flushed += 1;
                        }
                    }
                    HashMapEntry::Vacant(_vacant) => { // do nothing, item is no longer in cache somehow, so obviously not dirty
                    }
                }
            }
        }
        {
            let wc = &mut self.write_cache[ix].write().unwrap(); // maybe get lock for each item?
            for k in delete_keys.iter() {
                if let Some(item) = wc.remove(k) {
                    //error!("mid flush: {}, {}", ix, line!());
                    let instance = item.instance.write().unwrap();
                    //error!("mid flush: {}, {}", ix, line!());
                    // if someone else dirtied it or aged it newer, so re-insert it into the cache
                    if instance.dirty || (do_age && instance.age != age_comp) {
                        drop(instance);
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
        self.disk.keys(ix)
    }
    pub fn values<R: RangeBounds<Pubkey>>(
        &self,
        ix: usize,
        _range: Option<&R>,
    ) -> Option<Vec<Vec<SlotT<V>>>> {
        self.flush(ix, false, None);
        self.disk.values(ix)
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
        entry: &WriteCacheEntryArc<V>,
        mut new_value: AccountMapEntry<V>,
        reclaims: &mut SlotList<V>,
        reclaims_must_be_empty: bool,
        mut m1: Measure,
    ) {
        let mut instance = entry.instance.write().unwrap();

        if instance.must_do_lookup_from_disk {
            // todo
        }

        instance.age = self.set_age_to_future();
        instance.dirty = true;
        if instance.confirmed_not_on_disk {
            self.using_empty_get.fetch_add(1, Ordering::Relaxed);
            instance.data.ref_count = 0;
            assert!(instance.data.slot_list.is_empty());
            instance.confirmed_not_on_disk = false; // we are inserted now if we were 'confirmed_not_on_disk' before. update_static below handles this fine
        }
        self.updates_in_cache.fetch_add(1, Ordering::Relaxed);

        let mut current = &mut instance.data;
        let (slot, new_entry) = new_value.slot_list.remove(0);
        let addref = WriteAccountMapEntry::update_slot_list(
            &mut current.slot_list,
            slot,
            new_entry,
            reclaims,
            reclaims_must_be_empty,
        );
        if addref {
            current.ref_count += 1;
        }
        m1.stop();
        self.update_cache_us
            .fetch_add(m1.as_ns(), Ordering::Relaxed);
    }

    pub fn upsert(
        &self,
        key: &Pubkey,
        mut new_value: AccountMapEntry<V>,
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
            HashMapEntry::Occupied(occupied) => {
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
            HashMapEntry::Vacant(vacant) => {
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
                        &new_value.slot_list,
                        new_value.ref_count,
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
                    let (slot, new_entry) = new_value.slot_list.remove(0);
                    let addref = WriteAccountMapEntry::update_slot_list(
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
                    vacant.insert(self.allocate(
                        &new_value.slot_list,
                        new_value.ref_count,
                        true,
                        true,
                        must_do_lookup_from_disk,
                        confirmed_not_on_disk,
                    ));
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
        WriteCacheEntryArc {
            instance: RwLock::new(WriteCacheEntry {
                data: AccountMapEntry {
                    slot_list: slot_list.clone(),
                    ref_count,
                },
                dirty: dirty, //AtomicBool::new(dirty),
                age: self.set_age_to_future(),
                insert,
                must_do_lookup_from_disk,
                confirmed_not_on_disk,
            }),
        }
    }

    fn upsert_in_cache<F>(&self, key: &Pubkey, updatefn: F, current_value: Option<&V2<V>>)
    where
        F: Fn(Option<(&[SlotT<V>], u64)>) -> Option<(Vec<SlotT<V>>, u64)>,
    {
        let must_do_lookup_from_disk = false;
        let confirmed_not_on_disk = false;
        let current_value = current_value.map(|entry| (&entry.slot_list[..], entry.ref_count));
        // we are an update
        let result = updatefn(current_value);
        if let Some(result) = result {
            // stick this in the write cache and flush it later
            let ix = self.bucket_ix(key);
            let wc = &mut self.write_cache[ix].write().unwrap();

            match wc.entry(key.clone()) {
                HashMapEntry::Occupied(mut occupied) => {
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
                HashMapEntry::Vacant(vacant) => {
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
                assert!(self.get(key).is_some());
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

    pub fn get(&self, key: &Pubkey) -> Option<(u64, Vec<SlotT<V>>)> {
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
            if let Some(res) = res {
                if GET_CACHING {
                    let mut instance = res.instance.write().unwrap();
                    instance.age = self.set_age_to_future();
                    self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                    if instance.confirmed_not_on_disk {
                        return None; // does not really exist
                    }

                    let r = Some((instance.data.ref_count, instance.data.slot_list.clone()));
                    self.get_cache_us.fetch_add(m1.as_ns(), Ordering::Relaxed);
                    return r;
                } else {
                    let instance = res.instance.read().unwrap();
                    self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                    return Some((instance.data.ref_count, instance.data.slot_list.clone()));
                }
            }
            if !GET_CACHING {
                return self.get_no_cache(key);
            }
        }
        // get caching
        {
            let mut m1 = Measure::start("");
            let wc = &mut self.write_cache[ix].write().unwrap();
            let res = wc.entry(key.clone());
            match res {
                HashMapEntry::Occupied(occupied) => {
                    let mut instance = occupied.get().instance.write().unwrap();
                    instance.age = self.set_age_to_future();
                    if instance.confirmed_not_on_disk {
                        return None; // does not really exist
                    }
                    self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                    m1.stop();
                    self.update_cache_us
                        .fetch_add(m1.as_ns(), Ordering::Relaxed);
                    return Some((instance.data.ref_count, instance.data.slot_list.clone()));
                }
                HashMapEntry::Vacant(vacant) => {
                    let r = self.get_no_cache(key);
                    if let Some(loaded) = &r {
                        vacant.insert(self.allocate(
                            &loaded.1,
                            loaded.0,
                            false,
                            false,
                            must_do_lookup_from_disk,
                            confirmed_not_on_disk,
                        ));
                    } else {
                        // we looked this up. it does not exist. let's insert a marker in the cache saying it doesn't exist. otherwise, we have to look it up again on insert!
                        confirmed_not_on_disk = true;
                        vacant.insert(self.allocate(
                            &SlotList::default(),
                            RefCount::MAX,
                            false,
                            false,
                            must_do_lookup_from_disk,
                            confirmed_not_on_disk,
                        ));
                    }
                    m1.stop();
                    self.update_cache_us
                        .fetch_add(m1.as_ns(), Ordering::Relaxed);
                    r
                }
            }
        }
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
            HashMapEntry::Occupied(mut occupied) => {
                self.gets_from_cache.fetch_add(1, Ordering::Relaxed);
                let gm = occupied.get_mut();
                let mut instance = gm.instance.write().unwrap();
                if !instance.confirmed_not_on_disk {
                    if add {
                        instance.data.ref_count += 1;
                    } else {
                        instance.data.ref_count -= 1;
                    }
                }
            }
            HashMapEntry::Vacant(_vacant) => {
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
    in_memory: BTreeMap<K, V2<V>>,
    disk: Arc<BucketMapWriteHolder<V>>,
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

pub enum HybridEntry<'a, V: 'static + Clone + IsCached + Debug> {
    /// A vacant entry.
    Vacant(HybridVacantEntry<'a, V>),

    /// An occupied entry.
    Occupied(HybridOccupiedEntry<'a, V>),
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
            let r = Some(AccountMapEntry {
                slot_list: self.values[self.index].clone(),
                ref_count: RefCount::MAX, // todo: no clone
            });
            self.index += 1;
            r
        }
    }
}

pub struct HybridOccupiedEntry<'a, V: 'static + Clone + IsCached + Debug> {
    pubkey: Pubkey,
    entry: V2<V>,
    map: &'a HybridBTreeMap<V>,
}
pub struct HybridVacantEntry<'a, V: 'static + Clone + IsCached + Debug> {
    pubkey: Pubkey,
    map: &'a HybridBTreeMap<V>,
}

impl<'a, V: 'a + Clone + IsCached + Debug> HybridOccupiedEntry<'a, V> {
    pub fn get(&self) -> &V2<V> {
        &self.entry
    }
    pub fn update(&mut self, new_data: &SlotList<V>, new_rc: Option<RefCount>) {
        //error!("update: {}", self.pubkey);
        self.map.disk.update(
            &self.pubkey,
            |previous| {
                if previous.is_some() {
                    //error!("update {} to {:?}", self.pubkey, new_data);
                }
                Some((new_data.clone(), new_rc.unwrap_or(self.entry.ref_count)))
                // TODO no clone here
            },
            Some(&self.entry),
        );
        let g = self.map.disk.get(&self.pubkey).unwrap();
        assert_eq!(format!("{:?}", g.1), format!("{:?}", new_data));
    }
    pub fn addref(&mut self) {
        self.entry.ref_count += 1;

        self.map
            .disk
            .addref(&self.pubkey, self.entry.ref_count, &self.entry.slot_list);
        //error!("addref: {}, {}, {:?}", self.pubkey, self.entry.ref_count(), result);
    }
    pub fn unref(&mut self) {
        self.entry.ref_count -= 1;
        self.map
            .disk
            .unref(&self.pubkey, self.entry.ref_count, &self.entry.slot_list);
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

impl<'a, V: 'a + Clone + Debug + IsCached> HybridVacantEntry<'a, V> {
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
        self.map.disk.update(
            &self.pubkey,
            |_previous| {
                Some((value.slot_list.clone() /* todo bad */, value.ref_count))
            },
            None,
        );
    }
}

impl<V: IsCached> HybridBTreeMap<V> {
    /// Creates an empty `BTreeMap`.
    pub fn new2(bucket_map: &Arc<BucketMapWriteHolder<V>>, bin_index: usize, bins: usize) -> Self {
        Self {
            in_memory: BTreeMap::default(),
            disk: bucket_map.clone(),
            bin_index,
            bins: bins, //bucket_map.num_buckets(),
        }
    }

    pub fn new_for_testing() -> Self {
        let map = Self::new_bucket_map(BINS_FOR_TESTING);
        Self::new2(&map, 0, 1)
    }

    pub fn new_bucket_map(bins: usize) -> Arc<BucketMapWriteHolder<V>> {
        let buckets = PubkeyBinCalculator16::log_2(bins as u32) as u8; // make more buckets to try to spread things out
                                                                       // 15 hopefully avoids too many files open problem
                                                                       //buckets = std::cmp::min(buckets + 11, 15); // max # that works with open file handles and such
                                                                       //buckets =
                                                                       //error!("creating: {} for {}", buckets, BUCKET_BINS);
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
        R: RangeBounds<Pubkey>,
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

    pub fn insert(&mut self, key: K, value: V2<V>) {
        match self.entry(key) {
            HybridEntry::Occupied(_occupied) => {
                panic!("");
            }
            HybridEntry::Vacant(vacant) => vacant.insert(value),
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
    pub fn remove(&mut self, key: &K) {
        self.disk.delete_key(key); //.map(|x| x.entry)
    }
}
