use {
    crate::{
        accounts_index::{
            AccountMapEntry, AccountMapEntryInner, AccountMapEntryMeta, IndexValue,
            PreAllocatedAccountMapEntry, RefCount, SlotList, SlotSlice, ZeroLamport,
        },
        bucket_map_holder::{Age, BucketMapHolder},
        bucket_map_holder_stats::BucketMapHolderStats,
    },
    rand::{thread_rng, Rng},
    solana_bucket_map::bucket_api::BucketApi,
    solana_measure::measure::Measure,
    solana_sdk::{clock::Slot, pubkey::Pubkey},
    std::{
        collections::{
            hash_map::{Entry, VacantEntry},
            HashMap,
        },
        fmt::Debug,
        ops::{Bound, RangeBounds, RangeInclusive},
        sync::{
            atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering},
            Arc, RwLock,
        },
    },
};
type K = Pubkey;
type CacheRangesHeld = RwLock<Vec<RangeInclusive<Pubkey>>>;
pub type SlotT<T> = (Slot, T);

type InMemMap<T> = HashMap<Pubkey, AccountMapEntry<T>>;

#[allow(dead_code)] // temporary during staging
                    // one instance of this represents one bin of the accounts index.
pub struct InMemAccountsIndex<T: IndexValue> {
    last_age_flushed: AtomicU8,

    // backing store
    map_internal: RwLock<InMemMap<T>>,
    storage: Arc<BucketMapHolder<T>>,
    bin: usize,

    bucket: Option<Arc<BucketApi<SlotT<T>>>>,

    // pubkey ranges that this bin must hold in the cache while the range is present in this vec
    pub(crate) cache_ranges_held: CacheRangesHeld,
    // true while ranges are being manipulated. Used to keep an async flush from removing things while a range is being held.
    stop_flush: AtomicU64,
    // set to true when any entry in this bin is marked dirty
    bin_dirty: AtomicBool,
    // set to true while this bin is being actively flushed
    flushing_active: AtomicBool,
}

impl<T: IndexValue> Debug for InMemAccountsIndex<T> {
    fn fmt(&self, _f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

pub enum InsertNewEntryResults {
    DidNotExist,
    ExistedNewEntryZeroLamports,
    ExistedNewEntryNonZeroLamports,
}

#[allow(dead_code)] // temporary during staging
impl<T: IndexValue> InMemAccountsIndex<T> {
    pub fn new(storage: &Arc<BucketMapHolder<T>>, bin: usize) -> Self {
        Self {
            map_internal: RwLock::default(),
            storage: Arc::clone(storage),
            bin,
            bucket: storage
                .disk
                .as_ref()
                .map(|disk| disk.get_bucket_from_index(bin))
                .map(Arc::clone),
            cache_ranges_held: CacheRangesHeld::default(),
            stop_flush: AtomicU64::default(),
            bin_dirty: AtomicBool::default(),
            flushing_active: AtomicBool::default(),
            // initialize this to max, to make it clear we have not flushed at age 0, the starting age
            last_age_flushed: AtomicU8::new(Age::MAX),
        }
    }

    /// true if this bucket needs to call flush for the current age
    /// we need to scan each bucket once per value of age
    fn get_should_age(&self, age: Age) -> bool {
        let last_age_flushed = self.last_age_flushed();
        last_age_flushed != age
    }

    /// called after flush scans this bucket at the current age
    fn set_has_aged(&self, age: Age) {
        self.last_age_flushed.store(age, Ordering::Relaxed);
        self.storage.bucket_flushed_at_current_age();
    }

    fn last_age_flushed(&self) -> Age {
        self.last_age_flushed.load(Ordering::Relaxed)
    }

    fn map(&self) -> &RwLock<HashMap<Pubkey, AccountMapEntry<T>>> {
        &self.map_internal
    }

    /// Release entire in-mem hashmap to free all memory associated with it.
    /// Idea is that during startup we needed a larger map than we need during runtime.
    /// When using disk-buckets, in-mem index grows over time with dynamic use and then shrinks, in theory back to 0.
    pub fn shrink_to_fit(&self) {
        // shrink_to_fit could be quite expensive on large map sizes, which 'no disk buckets' could produce, so avoid shrinking in case we end up here
        if self.storage.is_disk_index_enabled() {
            self.map_internal.write().unwrap().shrink_to_fit();
        }
    }

    pub fn items<R>(&self, range: &R) -> Vec<K>
    where
        R: RangeBounds<Pubkey> + std::fmt::Debug,
    {
        let m = Measure::start("items");
        self.hold_range_in_memory(range, true);
        let map = self.map().read().unwrap();
        let mut result = Vec::with_capacity(map.len());
        map.iter().for_each(|(k, _v)| {
            if range.contains(k) {
                result.push(*k);
            }
        });
        self.hold_range_in_memory(range, false);
        Self::update_stat(&self.stats().items, 1);
        Self::update_time_stat(&self.stats().items_us, m);
        result
    }

    // only called in debug code paths
    pub fn keys(&self) -> Vec<Pubkey> {
        Self::update_stat(&self.stats().keys, 1);
        // easiest implementation is to load evrything from disk into cache and return the keys
        self.start_stop_flush(true);
        self.put_range_in_cache(&None::<&RangeInclusive<Pubkey>>);
        let keys = self.map().read().unwrap().keys().cloned().collect();
        self.start_stop_flush(false);
        keys
    }

    fn load_from_disk(&self, pubkey: &Pubkey) -> Option<(SlotList<T>, RefCount)> {
        self.bucket.as_ref().and_then(|disk| {
            let m = Measure::start("load_disk_found_count");
            let entry_disk = disk.read_value(pubkey);
            match &entry_disk {
                Some(_) => {
                    Self::update_time_stat(&self.stats().load_disk_found_us, m);
                    Self::update_stat(&self.stats().load_disk_found_count, 1);
                }
                None => {
                    Self::update_time_stat(&self.stats().load_disk_missing_us, m);
                    Self::update_stat(&self.stats().load_disk_missing_count, 1);
                }
            }
            entry_disk
        })
    }

    fn load_account_entry_from_disk(&self, pubkey: &Pubkey) -> Option<AccountMapEntry<T>> {
        let entry_disk = self.load_from_disk(pubkey)?; // returns None if not on disk

        Some(self.disk_to_cache_entry(entry_disk.0, entry_disk.1))
    }

    /// lookup 'pubkey' by only looking in memory. Does not look on disk.
    /// callback is called whether pubkey is found or not
    fn get_only_in_mem<RT>(
        &self,
        pubkey: &K,
        callback: impl for<'a> FnOnce(Option<&'a AccountMapEntry<T>>) -> RT,
    ) -> RT {
        let m = Measure::start("get");
        let map = self.map().read().unwrap();
        let result = map.get(pubkey);
        let stats = self.stats();
        let (count, time) = if result.is_some() {
            (&stats.gets_from_mem, &stats.get_mem_us)
        } else {
            (&stats.gets_missing, &stats.get_missing_us)
        };
        Self::update_time_stat(time, m);
        Self::update_stat(count, 1);

        callback(if let Some(entry) = result {
            entry.set_age(self.storage.future_age_to_flush());
            Some(entry)
        } else {
            drop(map);
            None
        })
    }

    /// lookup 'pubkey' in index (in_mem or disk).
    /// call 'callback' whether found or not
    pub(crate) fn get_internal<RT>(
        &self,
        pubkey: &K,
        // return true if item should be added to in_mem cache
        callback: impl for<'a> FnOnce(Option<&AccountMapEntry<T>>) -> (bool, RT),
    ) -> RT {
        self.get_only_in_mem(pubkey, |entry| {
            if let Some(entry) = entry {
                entry.set_age(self.storage.future_age_to_flush());
                callback(Some(entry)).1
            } else {
                // not in cache, look on disk
                let stats = &self.stats();
                let disk_entry = self.load_account_entry_from_disk(pubkey);
                if disk_entry.is_none() {
                    return callback(None).1;
                }
                let disk_entry = disk_entry.unwrap();
                let mut map = self.map().write().unwrap();
                let entry = map.entry(*pubkey);
                match entry {
                    Entry::Occupied(occupied) => callback(Some(occupied.get())).1,
                    Entry::Vacant(vacant) => {
                        let (add_to_cache, rt) = callback(Some(&disk_entry));

                        if add_to_cache {
                            stats.insert_or_delete_mem(true, self.bin);
                            vacant.insert(disk_entry);
                        }
                        rt
                    }
                }
            }
        })
    }

    /// lookup 'pubkey' in index (in_mem or disk).
    /// call 'callback' whether found or not
    pub(crate) fn get_mut_internal<RT>(
        &self,
        pubkey: &K,
        // return true if item should be added to in_mem cache
        callback: impl for<'a> FnOnce(Option<&mut AccountMapEntry<T>>) -> (bool, RT),
    ) -> RT {
        let mut map = self.map().write().unwrap();
        let entry = map.entry(*pubkey);
        match entry {
            Entry::Occupied(mut occupied) => {
                let result = callback(Some(occupied.get_mut())).1;
                occupied.get().set_age(self.storage.future_age_to_flush());
                result
            }
            Entry::Vacant(vacant) => {
                let stats = &self.stats();
                let mut disk_entry = self.load_account_entry_from_disk(pubkey);
                let (add_to_cache, rt) = callback(disk_entry.as_mut());

                if add_to_cache {
                    if let Some(disk_entry) = disk_entry {
                        stats.insert_or_delete_mem(true, self.bin);
                        vacant.insert(disk_entry);
                    }
                }
                rt
            }
        }
    }

    fn remove_if_slot_list_empty_value(&self, slot_list: SlotSlice<T>) -> bool {
        if slot_list.is_empty() {
            self.stats().insert_or_delete(false, self.bin);
            true
        } else {
            false
        }
    }

    fn delete_disk_key(&self, pubkey: &Pubkey) {
        if let Some(disk) = self.bucket.as_ref() {
            disk.delete_key(pubkey)
        }
    }

    fn remove_if_slot_list_empty_entry(&self, entry: Entry<K, AccountMapEntry<T>>) -> bool {
        match entry {
            Entry::Occupied(mut occupied) => {
                let result = self.remove_if_slot_list_empty_value(&occupied.get_mut().slot_list);
                if result {
                    // note there is a potential race here that has existed.
                    // if someone else holds the arc,
                    //  then they think the item is still in the index and can make modifications.
                    // We have to have a write lock to the map here, which means nobody else can get
                    //  the arc, but someone may already have retreived a clone of it.
                    // account index in_mem flushing is one such possibility
                    self.delete_disk_key(occupied.key());
                    self.stats().insert_or_delete_mem(false, self.bin);
                    occupied.remove();
                }
                result
            }
            Entry::Vacant(vacant) => {
                // not in cache, look on disk
                let entry_disk = self.load_from_disk(vacant.key());
                match entry_disk {
                    Some(entry_disk) => {
                        // on disk
                        if self.remove_if_slot_list_empty_value(&entry_disk.0) {
                            // not in cache, but on disk, so just delete from disk
                            self.delete_disk_key(vacant.key());
                            true
                        } else {
                            // could insert into cache here, but not required for correctness and value is unclear
                            false
                        }
                    }
                    None => false, // not in cache or on disk
                }
            }
        }
    }

    // If the slot list for pubkey exists in the index and is empty, remove the index entry for pubkey and return true.
    // Return false otherwise.
    pub fn remove_if_slot_list_empty(&self, pubkey: Pubkey) -> bool {
        let mut m = Measure::start("entry");
        let mut map = self.map().write().unwrap();
        let entry = map.entry(pubkey);
        m.stop();
        let found = matches!(entry, Entry::Occupied(_));
        let result = self.remove_if_slot_list_empty_entry(entry);
        drop(map);

        self.update_entry_stats(m, found);
        result
    }

    pub fn slot_list_mut<RT>(
        &self,
        pubkey: &Pubkey,
        user: impl for<'a> FnOnce(&mut SlotList<T>) -> RT,
    ) -> Option<RT> {
        self.get_mut_internal(pubkey, |entry| {
            (
                true,
                entry.map(|entry| {
                    let result = user(&mut entry.slot_list);
                    entry.set_dirty(true);
                    result
                }),
            )
        })
    }

    pub fn unref(&self, pubkey: &Pubkey) {
        self.get_mut_internal(pubkey, |entry| {
            if let Some(entry) = entry {
                entry.add_un_ref(false)
            }
            (true, ())
        })
    }

    pub fn upsert(
        &self,
        pubkey: &Pubkey,
        new_value: PreAllocatedAccountMapEntry<T>,
        reclaims: &mut SlotList<T>,
        previous_slot_entry_was_cached: bool,
    ) {
        let mut m = Measure::start("entry");
        let mut map = self.map().write().unwrap();
        let entry = map.entry(*pubkey);
        m.stop();
        let found = matches!(entry, Entry::Occupied(_));
        match entry {
            Entry::Occupied(mut occupied) => {
                let current = occupied.get_mut();
                Self::lock_and_update_slot_list(
                    current,
                    new_value.into(),
                    reclaims,
                    previous_slot_entry_was_cached,
                );
                current.set_age(self.storage.future_age_to_flush());
                Self::update_stat(&self.stats().updates_in_mem, 1);
            }
            Entry::Vacant(vacant) => {
                // not in cache, look on disk

                // desired to be this for filler accounts: self.storage.get_startup();
                // but, this has proven to be far too slow at high account counts
                let directly_to_disk = false;
                if directly_to_disk {
                    // We may like this to always run, but it is unclear.
                    // If disk bucket needs to resize, then this call can stall for a long time.
                    // Right now, we know it is safe during startup.
                    let already_existed = self.upsert_on_disk(
                        vacant,
                        new_value,
                        reclaims,
                        previous_slot_entry_was_cached,
                    );
                    if !already_existed {
                        self.stats().insert_or_delete(true, self.bin);
                    }
                } else {
                    // go to in-mem cache first
                    let disk_entry = self.load_account_entry_from_disk(vacant.key());
                    let new_value = if let Some(mut disk_entry) = disk_entry {
                        // on disk, so merge new_value with what was on disk
                        Self::lock_and_update_slot_list(
                            &mut disk_entry,
                            new_value.into(),
                            reclaims,
                            previous_slot_entry_was_cached,
                        );
                        disk_entry
                    } else {
                        // not on disk, so insert new thing
                        self.stats().insert_or_delete(true, self.bin);
                        new_value.into_account_map_entry(&self.storage)
                    };
                    assert!(new_value.dirty());
                    vacant.insert(new_value);
                    self.stats().insert_or_delete_mem(true, self.bin);
                }
            }
        }

        drop(map);
        self.update_entry_stats(m, found);
    }

    fn update_entry_stats(&self, stopped_measure: Measure, found: bool) {
        let stats = &self.stats();
        let (count, time) = if found {
            (&stats.entries_from_mem, &stats.entry_mem_us)
        } else {
            (&stats.entries_missing, &stats.entry_missing_us)
        };
        Self::update_stat(time, stopped_measure.as_us());
        Self::update_stat(count, 1);
    }

    // Try to update an item in the slot list the given `slot` If an item for the slot
    // already exists in the list, remove the older item, add it to `reclaims`, and insert
    // the new item.
    pub fn lock_and_update_slot_list(
        current: &mut AccountMapEntryInner<T>,
        new_value: (Slot, T),
        reclaims: &mut SlotList<T>,
        previous_slot_entry_was_cached: bool,
    ) {
        let slot_list = &mut current.slot_list;
        let (slot, new_entry) = new_value;
        let addref = Self::update_slot_list(
            slot_list,
            slot,
            new_entry,
            reclaims,
            previous_slot_entry_was_cached,
        );
        if addref {
            current.add_un_ref(true);
        }
        current.set_dirty(true);
    }

    // modifies slot_list
    // returns true if caller should addref
    fn update_slot_list(
        list: &mut SlotList<T>,
        slot: Slot,
        account_info: T,
        reclaims: &mut SlotList<T>,
        previous_slot_entry_was_cached: bool,
    ) -> bool {
        let mut addref = !account_info.is_cached();

        // find other dirty entries from the same slot
        for list_index in 0..list.len() {
            let (s, previous_update_value) = &list[list_index];
            if *s == slot {
                let previous_was_cached = previous_update_value.is_cached();
                addref = addref && previous_was_cached;

                let mut new_item = (slot, account_info);
                std::mem::swap(&mut new_item, &mut list[list_index]);
                if previous_slot_entry_was_cached {
                    assert!(previous_was_cached);
                } else {
                    reclaims.push(new_item);
                }
                list[(list_index + 1)..]
                    .iter()
                    .for_each(|item| assert!(item.0 != slot));
                return addref;
            }
        }

        // if we make it here, we did not find the slot in the list
        list.push((slot, account_info));
        addref
    }

    // convert from raw data on disk to AccountMapEntry, set to age in future
    fn disk_to_cache_entry(
        &self,
        slot_list: SlotList<T>,
        ref_count: RefCount,
    ) -> AccountMapEntry<T> {
        AccountMapEntry {
            slot_list,
            ref_count,
            meta: AccountMapEntryMeta::new_dirty(&self.storage),
        }
    }

    pub fn len_for_stats(&self) -> usize {
        self.stats().count_in_bucket(self.bin)
    }

    pub fn insert_new_entry_if_missing_with_lock(
        &self,
        pubkey: Pubkey,
        new_entry: PreAllocatedAccountMapEntry<T>,
    ) -> InsertNewEntryResults {
        let mut m = Measure::start("entry");
        let mut map = self.map().write().unwrap();
        let entry = map.entry(pubkey);
        m.stop();
        let new_entry_zero_lamports = new_entry.is_zero_lamport();
        let (found_in_mem, already_existed) = match entry {
            Entry::Occupied(mut occupied) => {
                // in cache, so merge into cache
                let (slot, account_info) = new_entry.into();
                InMemAccountsIndex::lock_and_update_slot_list(
                    occupied.get_mut(),
                    (slot, account_info),
                    &mut Vec::default(),
                    false,
                );
                (
                    true, /* found in mem */
                    true, /* already existed */
                )
            }
            Entry::Vacant(vacant) => {
                // not in cache, look on disk
                let initial_insert_directly_to_disk = false;
                if initial_insert_directly_to_disk {
                    // This is more direct, but becomes too slow with very large acct #.
                    // disk buckets will be improved to make them more performant. Tuning the disks may also help.
                    // This may become a config tuning option.
                    let already_existed =
                        self.upsert_on_disk(vacant, new_entry, &mut Vec::default(), false);
                    (false, already_existed)
                } else {
                    let disk_entry = self.load_account_entry_from_disk(vacant.key());
                    self.stats().insert_or_delete_mem(true, self.bin);
                    if let Some(mut disk_entry) = disk_entry {
                        let (slot, account_info) = new_entry.into();
                        InMemAccountsIndex::lock_and_update_slot_list(
                            &mut disk_entry,
                            (slot, account_info),
                            &mut Vec::default(),
                            false,
                        );
                        vacant.insert(disk_entry);
                        (
                            false, /* found in mem */
                            true,  /* already existed */
                        )
                    } else {
                        // not on disk, so insert new thing and we're done
                        let new_entry: AccountMapEntry<T> =
                            new_entry.into_account_map_entry(&self.storage);
                        assert!(new_entry.dirty());
                        vacant.insert(new_entry);
                        (false, false)
                    }
                }
            }
        };
        drop(map);
        self.update_entry_stats(m, found_in_mem);
        let stats = self.stats();
        if !already_existed {
            stats.insert_or_delete(true, self.bin);
        } else {
            Self::update_stat(&stats.updates_in_mem, 1);
        }
        if !already_existed {
            InsertNewEntryResults::DidNotExist
        } else if new_entry_zero_lamports {
            InsertNewEntryResults::ExistedNewEntryZeroLamports
        } else {
            InsertNewEntryResults::ExistedNewEntryNonZeroLamports
        }
    }

    /// return tuple:
    /// true if item already existed in the index
    fn upsert_on_disk(
        &self,
        vacant: VacantEntry<K, AccountMapEntry<T>>,
        new_entry: PreAllocatedAccountMapEntry<T>,
        reclaims: &mut SlotList<T>,
        previous_slot_entry_was_cached: bool,
    ) -> bool {
        if let Some(disk) = self.bucket.as_ref() {
            let mut existed = false;
            let (slot, account_info) = new_entry.into();
            disk.update(vacant.key(), |current| {
                if let Some((slot_list, mut ref_count)) = current {
                    // on disk, so merge and update disk
                    let mut slot_list = slot_list.to_vec();
                    let addref = Self::update_slot_list(
                        &mut slot_list,
                        slot,
                        account_info,
                        reclaims,
                        previous_slot_entry_was_cached,
                    );
                    if addref {
                        ref_count += 1
                    };
                    existed = true; // found on disk, so it did exist
                    Some((slot_list, ref_count))
                } else {
                    // doesn't exist on disk yet, so insert it
                    let ref_count = if account_info.is_cached() { 0 } else { 1 };
                    Some((vec![(slot, account_info)], ref_count))
                }
            });
            existed
        } else {
            // not using disk, so insert into mem
            self.stats().insert_or_delete_mem(true, self.bin);
            let new_entry: AccountMapEntry<T> = new_entry.into_account_map_entry(&self.storage);
            assert!(new_entry.dirty());
            vacant.insert(new_entry);
            false // not using disk, not in mem, so did not exist
        }
    }

    /// Look at the currently held ranges. If 'range' is already included in what is
    ///  being held, then add 'range' to the currently held list AND return true
    /// If 'range' is NOT already included in what is being held, then return false
    ///  withOUT adding 'range' to the list of what is currently held
    fn add_hold_range_in_memory_if_already_held<R>(&self, range: &R) -> bool
    where
        R: RangeBounds<Pubkey>,
    {
        let start_holding = true;
        let only_add_if_already_held = true;
        self.just_set_hold_range_in_memory_internal(range, start_holding, only_add_if_already_held)
    }

    fn just_set_hold_range_in_memory<R>(&self, range: &R, start_holding: bool)
    where
        R: RangeBounds<Pubkey>,
    {
        let only_add_if_already_held = false;
        let _ = self.just_set_hold_range_in_memory_internal(
            range,
            start_holding,
            only_add_if_already_held,
        );
    }

    /// if 'start_holding', then caller wants to add 'range' to the list of ranges being held
    /// if !'start_holding', then caller wants to remove 'range' to the list
    /// if 'only_add_if_already_held', caller intends to only add 'range' to the list if the range is already held
    /// returns true iff start_holding=true and the range we're asked to hold was already being held
    fn just_set_hold_range_in_memory_internal<R>(
        &self,
        range: &R,
        start_holding: bool,
        only_add_if_already_held: bool,
    ) -> bool
    where
        R: RangeBounds<Pubkey>,
    {
        assert!(!(only_add_if_already_held && !start_holding));
        let start = match range.start_bound() {
            Bound::Included(bound) | Bound::Excluded(bound) => *bound,
            Bound::Unbounded => Pubkey::new(&[0; 32]),
        };

        let end = match range.end_bound() {
            Bound::Included(bound) | Bound::Excluded(bound) => *bound,
            Bound::Unbounded => Pubkey::new(&[0xff; 32]),
        };
use log::*;
        // this becomes inclusive - that is ok - we are just roughly holding a range of items.
        // inclusive is bigger than exclusive so we may hold 1 extra item worst case
        let inclusive_range = start..=end;
        let mut ranges = self.cache_ranges_held.write().unwrap();
        let mut already_held = false;
        if start_holding {
            if only_add_if_already_held {
                for r in ranges.iter() {
                    if r.contains(&start) && r.contains(&end) {
                        already_held = true;
                        break;
                    }
                }
            }
            Self::update_stat(
                if already_held {
                    &self.stats().range_held_already
                } else {
                    &self.stats().new_range_held
                },
                1,
            );

            if already_held || !only_add_if_already_held {
                ranges.push(inclusive_range);
            }
        } else {
            // find the matching range and delete it since we don't want to hold it anymore
            // search backwards, assuming LIFO ordering
            let mut found = false;
            for (i, r) in ranges.iter().enumerate().rev() {
                if let (Bound::Included(start_found), Bound::Included(end_found)) =
                    (r.start_bound(), r.end_bound())
                {
                    if start_found == &start && end_found == &end {
                        // found a match. There may be dups, that's ok, we expect another call to remove the dup.
                        ranges.remove(i);
                        found = true;
                        break;
                    }
                }
            }
            assert!(found, "not found: {}..={} in {:?}", start, end, ranges);
        }
        already_held
    }

    fn start_stop_flush(&self, stop: bool) {
        if stop {
            self.stop_flush.fetch_add(1, Ordering::Release);
        } else if 1 == self.stop_flush.fetch_sub(1, Ordering::Release) {
            // stop_flush went to 0, so this bucket could now be ready to be aged
            self.storage.wait_dirty_or_aged.notify_one();
        }
    }

    pub fn hold_range_in_memory<R>(&self, range: &R, start_holding: bool)
    where
        R: RangeBounds<Pubkey> + Debug,
    {
        self.start_stop_flush(true);

        if !start_holding || !self.add_hold_range_in_memory_if_already_held(range) {
            if start_holding {
                // put everything in the cache and it will be held there
                self.put_range_in_cache(&Some(range));
            }
            // do this AFTER items have been put in cache - that way anyone who finds this range can know that the items are already in the cache
            self.just_set_hold_range_in_memory(range, start_holding);
        }

        self.start_stop_flush(false);
    }

    fn put_range_in_cache<R>(&self, range: &Option<&R>)
    where
        R: RangeBounds<Pubkey> + std::fmt::Debug,
    {
        use log::*;
        error!("put range in cache: {:?}", range);
        assert!(self.get_stop_flush()); // caller should be controlling the lifetime of how long this needs to be present
        let m = Measure::start("range");

        let mut added_to_mem = 0;
        // load from disk
        if let Some(disk) = self.bucket.as_ref() {
            let mut map = self.map().write().unwrap();
            let items = disk.items_in_range(range); // map's lock has to be held while we are getting items from disk
            let future_age = self.storage.future_age_to_flush();
            for item in items {
                let entry = map.entry(item.pubkey);
                match entry {
                    Entry::Occupied(occupied) => {
                        // item already in cache, bump age to future. This helps the current age flush to succeed.
                        occupied.get().set_age(future_age);
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(self.disk_to_cache_entry(item.slot_list, item.ref_count));
                        added_to_mem += 1;
                    }
                }
            }
        }
        self.stats()
            .insert_or_delete_mem_count(true, self.bin, added_to_mem);

        Self::update_time_stat(&self.stats().get_range_us, m);
    }

    fn get_stop_flush(&self) -> bool {
        self.stop_flush.load(Ordering::Relaxed) > 0
    }

    pub(crate) fn flush(&self) {
        let flushing = self.flushing_active.swap(true, Ordering::Acquire);
        if flushing {
            // already flushing in another thread
            return;
        }

        self.flush_internal();

        self.flushing_active.store(false, Ordering::Release);
    }

    pub fn set_bin_dirty(&self) {
        self.bin_dirty.store(true, Ordering::Release);
        // 1 bin dirty, so only need 1 thread to wake up if many could be waiting
        self.storage.wait_dirty_or_aged.notify_one();
    }

    fn random_chance_of_eviction() -> bool {
        // random eviction
        //const N: usize = 1000;
        // 1/N chance of eviction
        false//thread_rng().gen_range(0, N) == 0
    }

    /// assumes 1 entry in the slot list. Ignores overhead of the HashMap and such
    fn approx_size_of_one_entry() -> usize {
        std::mem::size_of::<T>()
            + std::mem::size_of::<Pubkey>()
            + std::mem::size_of::<AccountMapEntry<T>>()
    }

    /// return true if 'entry' should be removed from the in-mem index
    fn should_remove_from_mem(
        &self,
        current_age: Age,
        entry: &AccountMapEntry<T>,
        startup: bool,
        update_stats: bool,
        exceeds_budget: bool,
    ) -> bool {
        // this could be tunable dynamically based on memory pressure
        // we could look at more ages or we could throw out more items we are choosing to keep in the cache
        if startup || (current_age == entry.age()) {
            if exceeds_budget {
                // if we are already holding too many items in-mem, then we need to be more aggressive at kicking things out
                true
            } else {
                // only read the slot list if we are planning to throw the item out
                let slot_list = &entry.slot_list;
                if slot_list.len() != 1 {
                    if update_stats {
                        Self::update_stat(&self.stats().held_in_mem_slot_list_len, 1);
                    }
                    false // keep 0 and > 1 slot lists in mem. They will be cleaned or shrunk soon.
                } else {
                    // keep items with slot lists that contained cached items
                    let remove = !slot_list.iter().any(|(_, info)| info.is_cached());
                    if !remove && update_stats {
                        Self::update_stat(&self.stats().held_in_mem_slot_list_cached, 1);
                    }
                    remove
                }
            }
        } else {
            false
        }
    }

    fn flush_internal(&self) {
        let was_dirty = self.bin_dirty.swap(false, Ordering::Acquire);
        let current_age = self.storage.current_age();
        let mut iterate_for_age = self.get_should_age(current_age);
        let startup = self.storage.get_startup();
        if !was_dirty && !iterate_for_age && !startup {
            // wasn't dirty and no need to age, so no need to flush this bucket
            // but, at startup we want to remove from buckets as fast as possible if any items exist
            self.stats().empty_flush_calls.fetch_add(1, Ordering::Relaxed);
            return;
        }

        let in_mem_count = self.stats().count_in_mem.load(Ordering::Relaxed);
        let limit = self.storage.mem_budget_mb;
        let exceeds_budget = limit
            .map(|limit| in_mem_count * Self::approx_size_of_one_entry() >= limit * 1024 * 1024)
            .unwrap_or_default();

        // may have to loop if disk has to grow and we have to restart
        loop {
            let mut removes;
            let mut removes_random = Vec::default();
            let disk = self.bucket.as_ref().unwrap();

            let mut flush_entries_updated_on_disk = 0;
            let mut disk_resize = Ok(());
            // scan and update loop
            // holds read lock
            {
                let map = self.map().read().unwrap();
                removes = Vec::with_capacity(map.len());
                let m = Measure::start("flush_scan_and_update"); // we don't care about lock time in this metric - bg threads can wait
                for (k, v) in map.iter() {
                    if self.should_remove_from_mem(current_age, v, startup, true, exceeds_budget) {
                        removes.push(*k);
                    } else if Self::random_chance_of_eviction() {
                        removes_random.push(*k);
                    } else {
                        // not planning to remove this item from memory now, so don't write it to disk yet
                        continue;
                    }

                    // if we are removing it, then we need to update disk if we're dirty
                    if v.clear_dirty() {
                        // step 1: clear the dirty flag
                        // step 2: perform the update on disk based on the fields in the entry
                        // If a parallel operation dirties the item again - even while this flush is occurring,
                        //  the last thing the writer will do, after updating contents, is set_dirty(true)
                        //  That prevents dropping an item from cache before disk is updated to latest in mem.
                        // happens inside of lock on in-mem cache. This is because of deleting items
                        // it is possible that the item in the cache is marked as dirty while these updates are happening. That is ok.
                        disk_resize = disk.try_write(k, (&v.slot_list, v.ref_count()));
                        if disk_resize.is_ok() {
                            flush_entries_updated_on_disk += 1;
                        } else {
                            // disk needs to resize, so mark all unprocessed items as dirty again so we pick them up after the resize
                            v.set_dirty(true);
                            break;
                        }
                    }
                }
                drop(map);
                Self::update_time_stat(&self.stats().flush_scan_update_us, m);
            }
            Self::update_stat(
                &self.stats().flush_entries_updated_on_disk,
                flush_entries_updated_on_disk,
            );

            let m = Measure::start("flush_remove_or_grow");
            match disk_resize {
                Ok(_) => {
                    if !self.flush_remove_from_cache(
                        removes,
                        current_age,
                        startup,
                        false,
                        exceeds_budget,
                    ) || !self.flush_remove_from_cache(
                        removes_random,
                        current_age,
                        startup,
                        true,
                        exceeds_budget,
                    ) {
                        iterate_for_age = false; // did not make it all the way through this bucket, so didn't handle age completely
                    }
                    Self::update_time_stat(&self.stats().flush_remove_us, m);

                    if iterate_for_age {
                        // completed iteration of the buckets at the current age
                        assert_eq!(current_age, self.storage.current_age());
                        self.set_has_aged(current_age);
                    }
                    return;
                }
                Err(err) => {
                    // grow the bucket, outside of all in-mem locks.
                    // then, loop to try again
                    disk.grow(err);
                    Self::update_time_stat(&self.stats().flush_grow_us, m);
                }
            }
        }
    }

    // remove keys in 'removes' from in-mem cache due to age
    // return true if the removal was completed
    fn flush_remove_from_cache(
        &self,
        removes: Vec<Pubkey>,
        current_age: Age,
        startup: bool,
        randomly_evicted: bool,
        exceeds_budget: bool,
    ) -> bool {
        let mut completed_scan = true;
        if removes.is_empty() {
            return completed_scan; // completed, don't need to get lock or do other work
        }

        let ranges = self.cache_ranges_held.read().unwrap().clone();

        let mut removed = 0;
        // consider chunking these so we don't hold the write lock too long
        let mut map = self.map().write().unwrap();
        let mut stop = false;
        let next_age = self.storage.future_age(1);
        for k in removes {
            if let Entry::Occupied(occupied) = map.entry(k) {
                let v = occupied.get();
                if v.dirty()
                    || (!randomly_evicted
                        && !self.should_remove_from_mem(
                            current_age,
                            v,
                            startup,
                            false,
                            exceeds_budget,
                        ))
                {
                    // marked dirty or bumped in age after we looked above
                    // these will be handled in later passes
                    // but, at startup, everything is ready to age out if it isn't dirty
                    continue;
                }

                if !stop && ranges.iter().any(|range| range.contains(&k)) {
                    // this item is held in mem by range, so don't remove
                    //completed_scan = false;
                    // defer flushing this until next age
                    v.set_age(next_age);
                    continue;
                }

                stop = stop || self.get_stop_flush();
                if stop {
                    // defer flushing this until next age
                    v.set_age(next_age);
                    continue;
                }

                // all conditions for removing succeeded, so really remove item from in-mem cache
                removed += 1;
                occupied.remove();
            }
        }
        if map.is_empty() {
            map.shrink_to_fit();
        }
        drop(map);
        self.stats()
            .insert_or_delete_mem_count(false, self.bin, removed);
        Self::update_stat(&self.stats().flush_entries_removed_from_mem, removed as u64);

        completed_scan
    }

    pub fn stats(&self) -> &BucketMapHolderStats {
        &self.storage.stats
    }

    fn update_stat(stat: &AtomicU64, value: u64) {
        if value != 0 {
            stat.fetch_add(value, Ordering::Relaxed);
        }
    }

    pub fn update_time_stat(stat: &AtomicU64, mut m: Measure) {
        m.stop();
        let value = m.as_us();
        Self::update_stat(stat, value);
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::accounts_index::{AccountsIndexConfig, BINS_FOR_TESTING},
    };

    fn new_for_test<T: IndexValue>() -> InMemAccountsIndex<T> {
        let holder = Arc::new(BucketMapHolder::new(
            BINS_FOR_TESTING,
            &Some(AccountsIndexConfig::default()),
            1,
        ));
        let bin = 0;
        InMemAccountsIndex::new(&holder, bin)
    }

    fn new_disk_buckets_for_test<T: IndexValue>() -> InMemAccountsIndex<T> {
        let holder = Arc::new(BucketMapHolder::new(
            BINS_FOR_TESTING,
            &Some(AccountsIndexConfig {
                index_limit_mb: Some(1),
                ..AccountsIndexConfig::default()
            }),
            1,
        ));
        let bin = 0;
        let bucket = InMemAccountsIndex::new(&holder, bin);
        assert!(bucket.storage.is_disk_index_enabled());
        bucket
    }

    #[test]
    fn test_should_remove_from_mem() {
        solana_logger::setup();
        let bucket = new_for_test::<u64>();
        let mut startup = false;
        let mut current_age = 0;
        let ref_count = 0;
        let one_element_slot_list = vec![(0, 0)];
        let one_element_slot_list_entry = Arc::new(AccountMapEntryInner::new(
            one_element_slot_list,
            ref_count,
            AccountMapEntryMeta::default(),
        ));

        // exceeded budget
        assert!(bucket.should_remove_from_mem(
            current_age,
            &Arc::new(AccountMapEntryInner::new(
                vec![],
                ref_count,
                AccountMapEntryMeta::default()
            )),
            startup,
            false,
            true,
        ));
        // empty slot list
        assert!(!bucket.should_remove_from_mem(
            current_age,
            &Arc::new(AccountMapEntryInner::new(
                vec![],
                ref_count,
                AccountMapEntryMeta::default()
            )),
            startup,
            false,
            false,
        ));
        // 1 element slot list
        assert!(bucket.should_remove_from_mem(
            current_age,
            &one_element_slot_list_entry,
            startup,
            false,
            false,
        ));
        // 2 element slot list
        assert!(!bucket.should_remove_from_mem(
            current_age,
            &Arc::new(AccountMapEntryInner::new(
                vec![(0, 0), (1, 1)],
                ref_count,
                AccountMapEntryMeta::default()
            )),
            startup,
            false,
            false,
        ));

        {
            let bucket = new_for_test::<f64>();
            // 1 element slot list with a CACHED item - f64 acts like cached
            assert!(!bucket.should_remove_from_mem(
                current_age,
                &Arc::new(AccountMapEntryInner::new(
                    vec![(0, 0.0)],
                    ref_count,
                    AccountMapEntryMeta::default()
                )),
                startup,
                false,
                false,
            ));
        }

        // 1 element slot list, age is now
        assert!(bucket.should_remove_from_mem(
            current_age,
            &one_element_slot_list_entry,
            startup,
            false,
            false,
        ));

        // 1 element slot list, but not current age
        current_age = 1;
        assert!(!bucket.should_remove_from_mem(
            current_age,
            &one_element_slot_list_entry,
            startup,
            false,
            false,
        ));

        // 1 element slot list, but at startup and age not current
        startup = true;
        assert!(bucket.should_remove_from_mem(
            current_age,
            &one_element_slot_list_entry,
            startup,
            false,
            false,
        ));
    }

    #[test]
    fn test_hold_range_in_memory() {
        let bucket = new_disk_buckets_for_test::<u64>();
        // 0x81 is just some other range
        let all = Pubkey::new(&[0; 32])..=Pubkey::new(&[0xff; 32]);
        let ranges = [
            all.clone(),
            Pubkey::new(&[0x81; 32])..=Pubkey::new(&[0xff; 32]),
        ];
        for range in ranges.clone() {
            assert!(bucket.cache_ranges_held.read().unwrap().is_empty());
            bucket.hold_range_in_memory(&range, true);
            assert_eq!(
                bucket.cache_ranges_held.read().unwrap().to_vec(),
                vec![range.clone()]
            );
            {
                assert!(bucket.add_hold_range_in_memory_if_already_held(&range));
                bucket.hold_range_in_memory(&range, false);
            }
            bucket.hold_range_in_memory(&range, false);
            assert!(bucket.cache_ranges_held.read().unwrap().is_empty());
            bucket.hold_range_in_memory(&range, true);
            assert_eq!(
                bucket.cache_ranges_held.read().unwrap().to_vec(),
                vec![range.clone()]
            );
            bucket.hold_range_in_memory(&range, true);
            assert_eq!(
                bucket.cache_ranges_held.read().unwrap().to_vec(),
                vec![range.clone(), range.clone()]
            );
            bucket.hold_range_in_memory(&ranges[0], true);
            assert_eq!(
                bucket.cache_ranges_held.read().unwrap().to_vec(),
                vec![range.clone(), range.clone(), ranges[0].clone()]
            );
            bucket.hold_range_in_memory(&range, false);
            assert_eq!(
                bucket.cache_ranges_held.read().unwrap().to_vec(),
                vec![range.clone(), ranges[0].clone()]
            );
            bucket.hold_range_in_memory(&range, false);
            assert_eq!(
                bucket.cache_ranges_held.read().unwrap().to_vec(),
                vec![ranges[0].clone()]
            );
            bucket.hold_range_in_memory(&ranges[0].clone(), false);
            assert!(bucket.cache_ranges_held.read().unwrap().is_empty());

            // hold all in mem first
            assert!(bucket.cache_ranges_held.read().unwrap().is_empty());
            bucket.hold_range_in_memory(&all, true);
            assert!(bucket.add_hold_range_in_memory_if_already_held(&range));
            bucket.hold_range_in_memory(&range, false);
            bucket.hold_range_in_memory(&all, false);
        }
    }

    #[derive(Debug, Default, Clone)]
    struct testit {
        data: u64,
    }
    impl Drop for testit {
        fn drop(&mut self) {
            use log::*;
            //error!("dropping");
        }
    }

    #[test]
    fn test_hash() {
        use log::*;
        solana_logger::setup();
        use std::collections::hash_map::RandomState;
        let s = RandomState::new();
        let mut m = HashMap::with_hasher(s);
        // x=500k, size = 10k does not get freed
        let x = 500000u64;
        m.insert(1u32, 1u32);
        let mut m = Vec::with_capacity(x as usize);
        for i in 0..x {
            let s = 10_000;
            let mut v_inner = Vec::with_capacity(s);
            for j in 0..s {
                v_inner.push(1u32);
            }
            let v = Arc::new(vec![(i, (v_inner, 32u64, 45u64, testit::default()))]);
            if m.len() > 0 && false {
                m[0] = v;
            } else {
                m.push(v);
            }
            /*
            match m.entry(i) {
                Entry::Occupied(_occupied) => {
                    assert!(false);
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(v);
                }
            }
            */
        }
        error!("waiting after add");
        std::thread::sleep(std::time::Duration::from_millis(5_000));
        error!("removing");
        for i in 0..x {
            /*
            match m.entry(i) {
                Entry::Occupied(occupied) => {
                    occupied.remove();
                }
                Entry::Vacant(_vacant) => {
                }
            }
            */
            //let r = m.remove(&i).unwrap();
            let i = (x - i - 1) as usize;
            if i < m.len() {
                let r = m.remove(i);
                /*
                assert_eq!(1, Arc::strong_count(&r));
                let mut r = Arc::try_unwrap(r).unwrap();
                r = vec![];
                */
                drop(r);
            }
        }
        error!("done removing");
        //std::thread::sleep(std::time::Duration::from_millis(5_000));
        error!("done removing2");
        std::thread::sleep(std::time::Duration::from_millis(5_000));
        error!("drop map");
        drop(m);
        std::thread::sleep(std::time::Duration::from_millis(5_000));
        error!("allocate and drop large vector");
        let m: Vec<u8> = Vec::with_capacity(100_000_000_000);
        drop(m);
        std::thread::sleep(std::time::Duration::from_millis(5_000));
        error!("create new map");
        let s = RandomState::new();
        let mut m = HashMap::with_hasher(s);
        std::thread::sleep(std::time::Duration::from_millis(5_000));
        error!("populating new map");
        for i in 0..x {
            let v = vec![(i, ([1u64; 251], 32u64, 45u64))];
            match m.entry(i) {
                Entry::Occupied(_occupied) => {
                    assert!(false);
                }
                Entry::Vacant(vacant) => {
                    vacant.insert(v);
                }
            }
        }
        error!("waiting after populating");
        std::thread::sleep(std::time::Duration::from_millis(5_000));
    }

    #[test]
    fn test_age() {
        solana_logger::setup();
        let test = new_for_test::<u64>();
        assert!(test.get_should_age(test.storage.current_age()));
        assert_eq!(test.storage.count_ages_flushed(), 0);
        test.set_has_aged(0);
        assert!(!test.get_should_age(test.storage.current_age()));
        assert_eq!(test.storage.count_ages_flushed(), 1);
        // simulate rest of buckets aging
        for _ in 1..BINS_FOR_TESTING {
            assert!(!test.storage.all_buckets_flushed_at_current_age());
            test.storage.bucket_flushed_at_current_age();
        }
        assert!(test.storage.all_buckets_flushed_at_current_age());
        // advance age
        test.storage.increment_age();
        assert_eq!(test.storage.current_age(), 1);
        assert!(!test.storage.all_buckets_flushed_at_current_age());
        assert!(test.get_should_age(test.storage.current_age()));
        assert_eq!(test.storage.count_ages_flushed(), 0);
    }
}
