//! ReadOnlyAccountsCache used to store accounts, such as executable accounts,
//! which can be large, loaded many times, and rarely change.
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use {
    crate::index_list::{Index, IndexList},
    dashmap::{mapref::entry::Entry, DashMap, DashSet},
    solana_measure::measure_us,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        pubkey::Pubkey,
        timing::timestamp,
    },
    std::sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
};

const CACHE_ENTRY_SIZE: usize =
    std::mem::size_of::<ReadOnlyAccountCacheEntry>() + 2 * std::mem::size_of::<ReadOnlyCacheKey>();

type ReadOnlyCacheKey = (Pubkey, Slot);

#[derive(Default, Clone)]
pub struct Keep {
    pub account: AccountSharedData,
    pub pubkey: Pubkey,
    pub slot: Slot,
    pub time_from_start: u32,
    pub num_lru_updates: u32,
}

impl Debug for Keep {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "data,{}, pubkey,{}, slot,{}, time,{}, lru updates,{}", self.account.data().len(), self.pubkey, self.slot, self.time_from_start, self.num_lru_updates)
    }
}


#[derive(Debug, Default, Clone)]
struct Diffs {
    removed_ms_not_in_dashmap : HashSet<(Pubkey, Slot)>,
    removed_sampling_not_in_dashmap : HashSet<(Pubkey, Slot)>,
    removed : HashMap<(Pubkey, Slot), Keep>,
    //removed_ms_still_in_sampling : HashMap<(Pubkey, Slot), Keep>,
    removed_sampling_still_in_ms : HashMap<(Pubkey, Slot), Keep>,
}

#[derive(Debug, Default, Clone)]
pub struct DiffsVec {
    pub removed_ms_not_in_dashmap : Vec<(Pubkey, Slot)>,
    pub removed_sampling_not_in_dashmap : Vec<(Pubkey, Slot)>,
    pub removed : Vec<(Pubkey, Slot, Keep)>,
    pub missed_and_loaded : Vec<(Pubkey, Slot, Keep)>,
    pub removed_ms_still_in_sampling : Vec<(Pubkey, Slot, Keep)>,
    pub removed_sampling_still_in_ms : Vec<(Pubkey, Slot, Keep)>,
    pub would_have_missed_sampling : Vec<(Pubkey, Slot, Keep)>,
}

lazy_static! {
    static ref STARTUP_TIME: u32 = timestamp() as u32;
}

#[derive(Debug)]
struct ReadOnlyAccountCacheEntry {
    account: AccountSharedData,
    /// Index of the entry in the eviction queue.
    index_ms: AtomicU32, // Index of the entry in the eviction queue.
    index_sampling: AtomicU32, // Index of the entry in the eviction queue.
    /// lower bits of last timestamp when eviction queue was updated, in ms
    last_update_time: AtomicU32,
    removed_due_to_sampling: AtomicBool,
    removed_due_to_ms: AtomicBool,
    num_lru_updates: AtomicU32,

}
use std::fmt::Debug;
use std::fmt::Formatter;
impl Debug for ReadOnlyAccountsCache {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "")
    }
}


pub struct ReadOnlyAccountsCache {
    cache: DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>,
    pub has_been_in_cache: DashSet<ReadOnlyCacheKey>,
    pub would_have_been_evicted_sampling: DashSet<ReadOnlyCacheKey>,
    /// When an item is first entered into the cache, it is added to the end of
    /// the queue. Also each time an entry is looked up from the cache it is
    /// moved to the end of the queue. As a result, items in the queue are
    /// always sorted in the order that they have last been accessed. When doing
    /// LRU eviction, cache entries are evicted from the front of the queue.
    queue_ms: Mutex<IndexList<ReadOnlyCacheKey>>,
    queue_sampling: Mutex<IndexList<ReadOnlyCacheKey>>,
    max_data_size: usize,
    data_size: AtomicUsize,
    // read only cache does not update lru on read of an entry unless it has been at least this many ms since the last lru update
    ms_to_skip_lru_update: u32,

    /// stats
    hits: AtomicU64,
    time_based_sampling: AtomicU64,
    high_pass: AtomicU64,
    misses: AtomicU64,
    evicts: AtomicU64,
    load_us: AtomicU64,
    counter: AtomicU64,

    pub kept: Mutex<DiffsVec>,
}

impl ReadOnlyAccountsCache {
    pub(crate) fn new(max_data_size: usize, ms_to_skip_lru_update: u32) -> Self {
        Self {
            max_data_size,
            cache: DashMap::default(),
            queue_ms: Mutex::<IndexList<ReadOnlyCacheKey>>::default(),
            data_size: AtomicUsize::default(),
            ms_to_skip_lru_update,
            hits: AtomicU64::default(),
            misses: AtomicU64::default(),
            evicts: AtomicU64::default(),
            load_us: AtomicU64::default(),
            time_based_sampling: AtomicU64::default(),
            high_pass: AtomicU64::default(),
            counter: AtomicU64::default(),
            queue_sampling: Mutex::<IndexList<ReadOnlyCacheKey>>::default(),
            kept: Mutex::default(),
            has_been_in_cache: DashSet::default(),
            would_have_been_evicted_sampling: DashSet::default(),
        }
    }

    /// reset the read only accounts cache
    /// useful for benches/tests
    pub fn reset_for_tests(&self) {
        self.cache.clear();
        self.queue_ms.lock().unwrap().clear();
        self.queue_sampling.lock().unwrap().clear();
        self.data_size.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evicts.store(0, Ordering::Relaxed);
        self.load_us.store(0, Ordering::Relaxed);
    }

    /// true if pubkey is in cache at slot
    pub fn in_cache(&self, pubkey: &Pubkey, slot: Slot) -> bool {
        self.cache.contains_key(&(*pubkey, slot))
    }

    pub(crate) fn load(&self, pubkey: Pubkey, slot: Slot) -> Option<AccountSharedData> {
        let update_lru;
        let (account, load_us) = measure_us!({
            let key = (pubkey, slot);
            let pk = Pubkey::from_str("Gk64R86pcSaPXV1521vRxgVYb76kyjXpw1ih382XFo1E").unwrap();
            let Some(entry) = self.cache.get(&key) else {
                if pk == pubkey {
                    log::error!("missed: {}", pk);
                }
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            if self.would_have_been_evicted_sampling.remove(&key).is_some() {
                self.kept.lock().unwrap().would_have_missed_sampling.push((pubkey, slot, Keep {
                    account: entry.account.clone(),
                    pubkey,
                    slot,
                    time_from_start: entry
                    .last_update_time
                    .load(Ordering::Relaxed)
                    .wrapping_sub(*STARTUP_TIME),
                    num_lru_updates: entry.num_lru_updates.load(Ordering::Relaxed),
                }));
            }

            // Move the entry to the end of the queue.
            // self.queue is modified while holding a reference to the cache entry;
            // so that another thread cannot write to the same key.
            // If we updated the eviction queue within this much time, then leave it where it is. We're likely to hit it again.
            update_lru = true;//entry.ms_since_last_update() >= self.ms_to_skip_lru_update;
            entry.num_lru_updates.fetch_add(1, Ordering::Relaxed);
            //log::error!("{}", line!());
            if pk == pubkey {
                log::error!("loaded: {}", pk);
            }
            if update_lru {
                let mut queue = self.queue_ms.lock().unwrap();
                {
                    queue.move_to_last(entry.index_ms());
                    entry
                        .last_update_time
                        .store(ReadOnlyAccountCacheEntry::timestamp(), Ordering::Relaxed);
                }
            }
            if self.counter.fetch_add(1, Ordering::Relaxed) % 8 == 0 {
                if let Ok(mut queue) = self.queue_sampling.try_lock() {
                    queue.move_to_last(entry.index_sampling());
                    entry
                        .last_update_time
                        .store(ReadOnlyAccountCacheEntry::timestamp(), Ordering::Relaxed);
                }
            }
            //log::error!("{}", line!());
            let account = entry.account.clone();
            drop(entry);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(account)
        });
        self.load_us.fetch_add(load_us, Ordering::Relaxed);
        if update_lru {
            self.time_based_sampling.fetch_add(1, Ordering::Relaxed);
        }
        account
    }

    fn account_size(&self, account: &AccountSharedData) -> usize {
        CACHE_ENTRY_SIZE + account.data().len()
    }

    pub fn store(&self, pubkey: Pubkey, slot: Slot, account: AccountSharedData) {
        let key = (pubkey, slot);
        self.has_been_in_cache.insert(key);
        let account_size = self.account_size(&account);
        self.data_size.fetch_add(account_size, Ordering::Relaxed);
        let pk = Pubkey::from_str("Gk64R86pcSaPXV1521vRxgVYb76kyjXpw1ih382XFo1E").unwrap();
        // self.queue is modified while holding a reference to the cache entry;
        // so that another thread cannot write to the same key.
        //log::error!("{}", line!());
        match self.cache.entry(key) {
            Entry::Vacant(entry) => {
                // Insert the entry at the end of the queue.
                let index_ms;
                let index_sampling;
                {
                    let mut queue = self.queue_ms.lock().unwrap();
                    index_ms = queue.insert_last(key);
                    if pk == pubkey {
                        log::error!("vacant: {}", pk);
                    }
                }
                {
                    let mut queue = self.queue_sampling.lock().unwrap();
                    index_sampling = queue.insert_last(key);
                }
                entry.insert(ReadOnlyAccountCacheEntry::new(
                    account,
                    index_ms,
                    index_sampling,
                ));
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                let account_size = self.account_size(&entry.account);
                self.data_size.fetch_sub(account_size, Ordering::Relaxed);
                entry.account = account;
                // Move the entry to the end of the queue.
                {
                    if pk == pubkey {
                        log::error!("occupied: {}", pk);
                    }
                    let mut queue = self.queue_ms.lock().unwrap();
                    queue.remove(entry.index_ms());
                    entry.set_index_ms(queue.insert_last(key));
                }
                {
                    let mut queue: std::sync::MutexGuard<'_, IndexList<(Pubkey, u64)>> =
                        self.queue_sampling.lock().unwrap();
                    queue.remove(entry.index_sampling());
                    entry.set_index_sampling(queue.insert_last(key));
                }
            }
        };
        //log::error!("{}", line!());
        // Evict entries from the front of the queue.
        let mut diffs = Diffs::default();
        let mut num_evicts = 0;
        let mut startup_data_len = self.data_size.load(Ordering::Relaxed);
        let mut remove_from_sampling = Vec::default();
        let mut removed_ms_still_in_sampling = HashMap::<(Pubkey, Slot), (Index, Keep)>::default();
        while self.data_size.load(Ordering::Relaxed) > self.max_data_size {
            let mut queue = self.queue_ms.lock().unwrap();
            let Some((pubkey, slot)) = queue.get_first().cloned() else {
                break;
            };
            let first_index = queue.first_index();
            assert_eq!((pubkey, slot), queue.remove(first_index).unwrap());
            drop(queue);
            //removed.insert((pubkey, slot));
            num_evicts += 1;
            if let Some((_, entry)) = self.cache.remove(&(pubkey, slot)) {
                if pk == pubkey {
                    log::error!("evicted: {}", pk);
                }
                removed_ms_still_in_sampling.insert(
                    (pubkey, slot),
                    (entry.index_sampling(),
                    Keep {
                        account: entry.account.clone(),
                        pubkey,
                        slot,
                        time_from_start: entry
                            .last_update_time
                            .load(Ordering::Relaxed)
                            .wrapping_sub(*STARTUP_TIME),
                            num_lru_updates: entry.num_lru_updates.load(Ordering::Relaxed),

                    }),
                );
                // self.queue should be modified only after removing the entry from the
                // cache, so that this is still safe if another thread writes to the
                // same key.
                //log::error!("{}", line!());
                let account_size = self.account_size(&entry.account);
                self.data_size.fetch_sub(account_size, Ordering::Relaxed);
                remove_from_sampling.push((pubkey, slot, entry.index_sampling()));
            } else {
                if pk == pubkey {
                    log::error!("evicted but not found: {}", pk);
                }
                diffs.removed_ms_not_in_dashmap.insert((pubkey, slot));
            }
        }
        //log::error!("{}", line!());
        while startup_data_len > self.max_data_size {
            let mut queue = self.queue_sampling.lock().unwrap();
            let Some((pubkey, slot)) = queue.get_first().cloned() else {
                break;
            };
            let first_index = queue.first_index();
            assert_eq!((pubkey, slot), queue.remove(first_index).unwrap());
            drop(queue);
            if let Some(entry) = self.cache.get(&(pubkey, slot)) {
                diffs.removed_sampling_still_in_ms.insert(
                    (pubkey, slot),
                    Keep {
                        account: entry.account.clone(),
                        pubkey,
                        slot,
                        time_from_start: entry
                            .last_update_time
                            .load(Ordering::Relaxed)
                            .wrapping_sub(*STARTUP_TIME),
                            num_lru_updates: entry.num_lru_updates.load(Ordering::Relaxed),
                        },
                );
                self.would_have_been_evicted_sampling.insert((pubkey, slot));
                startup_data_len = startup_data_len.saturating_sub(self.account_size(&entry.account));
                drop(entry);
            } else {
                if let Some(removed) = removed_ms_still_in_sampling.remove(&(pubkey, slot)) {
                    startup_data_len = startup_data_len.saturating_sub(self.account_size(&removed.1.account));
                } else {
                    diffs.removed_ms_not_in_dashmap.insert((pubkey, slot));
                }
                continue;
            }
        }
        removed_ms_still_in_sampling.iter().for_each(|(k, (index, _))| {
            self.queue_sampling.lock().unwrap().remove(*index);
        });
        //log::error!("{}", line!());

        let mut kept = self.kept.lock().unwrap();
        kept.removed_ms_not_in_dashmap.extend(diffs.removed_ms_not_in_dashmap.iter().cloned());
        kept.removed_sampling_not_in_dashmap.extend(diffs.removed_sampling_not_in_dashmap.iter().cloned());
        //kept.removed.extend(diffs.removed.iter().cloned());
        kept.removed_ms_still_in_sampling.extend(removed_ms_still_in_sampling.iter().map(|(k, (_index, v))| (k.0, k.1, v.clone())));
        kept.removed_sampling_still_in_ms.extend(diffs.removed_sampling_still_in_ms.iter().map(|(k, v)| (k.0, k.1, v.clone())));

        self.evicts.fetch_add(num_evicts, Ordering::Relaxed);
    }

    pub fn remove(&self, pubkey: Pubkey, slot: Slot) -> Option<AccountSharedData> {
        let (_, entry) = self.cache.remove(&(pubkey, slot))?;
        // self.queue should be modified only after removing the entry from the
        // cache, so that this is still safe if another thread writes to the
        // same key.
        //log::error!("{}", line!());
        self.queue_ms.lock().unwrap().remove(entry.index_ms());
        self.queue_sampling
            .lock()
            .unwrap()
            .remove(entry.index_sampling());
        //log::error!("{}", line!());
        let account_size = self.account_size(&entry.account);
        self.data_size.fetch_sub(account_size, Ordering::Relaxed);
        Some(entry.account)
    }

    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub fn data_size(&self) -> usize {
        self.data_size.load(Ordering::Relaxed)
    }

    pub(crate) fn get_and_reset_stats(&self) -> (u64, u64, u64, u64, u64, u64) {
        let hits = self.hits.swap(0, Ordering::Relaxed);
        let misses = self.misses.swap(0, Ordering::Relaxed);
        let evicts = self.evicts.swap(0, Ordering::Relaxed);
        let load_us = self.load_us.swap(0, Ordering::Relaxed);
        let time_based_sampling = self.time_based_sampling.swap(0, Ordering::Relaxed);
        let high_pass = self.high_pass.swap(0, Ordering::Relaxed);
        (
            hits,
            misses,
            evicts,
            load_us,
            time_based_sampling,
            high_pass,
        )
    }
}

impl ReadOnlyAccountCacheEntry {
    fn new(account: AccountSharedData, index_ms: Index, index_sampling: Index) -> Self {
        let index_ms = unsafe { std::mem::transmute::<Index, u32>(index_ms) };
        let index_ms = AtomicU32::new(index_ms);
        let index_sampling = unsafe { std::mem::transmute::<Index, u32>(index_sampling) };
        let index_sampling = AtomicU32::new(index_sampling);
        Self {
            account,
            index_ms,
            last_update_time: AtomicU32::new(Self::timestamp()),
            index_sampling,
            removed_due_to_ms: AtomicBool::default(),
            removed_due_to_sampling: AtomicBool::default(),
            num_lru_updates: AtomicU32::default(),
        }
    }

    #[inline]
    fn index_sampling(&self) -> Index {
        let index = self.index_sampling.load(Ordering::Relaxed);
        unsafe { std::mem::transmute::<u32, Index>(index) }
    }

    #[inline]
    fn index_ms(&self) -> Index {
        let index = self.index_ms.load(Ordering::Relaxed);
        unsafe { std::mem::transmute::<u32, Index>(index) }
    }

    #[inline]
    fn set_index_sampling(&self, index: Index) {
        let index = unsafe { std::mem::transmute::<Index, u32>(index) };
        self.index_sampling.store(index, Ordering::Relaxed);
    }

    #[inline]
    fn set_index_ms(&self, index: Index) {
        let index = unsafe { std::mem::transmute::<Index, u32>(index) };
        self.index_ms.store(index, Ordering::Relaxed);
    }

    /// lower bits of current timestamp. We don't need higher bits and u32 packs with Index u32 in `ReadOnlyAccountCacheEntry`
    fn timestamp() -> u32 {
        timestamp() as u32
    }

    /// ms since `last_update_time` timestamp
    fn ms_since_last_update(&self) -> u32 {
        Self::timestamp().wrapping_sub(self.last_update_time.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::{
            seq::{IteratorRandom, SliceRandom},
            Rng, SeedableRng,
        },
        rand_chacha::ChaChaRng,
        solana_sdk::account::{accounts_equal, Account, WritableAccount},
        std::{collections::HashMap, iter::repeat_with, sync::Arc},
    };
    #[test]
    fn test_accountsdb_sizeof() {
        // size_of(arc(x)) does not return the size of x
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<u8>>());
        assert!(std::mem::size_of::<Arc<u64>>() == std::mem::size_of::<Arc<[u8; 32]>>());
    }

    #[test]
    fn test_read_only_accounts_cache() {
        solana_logger::setup();
        let per_account_size = CACHE_ENTRY_SIZE;
        let data_size = 100;
        let max = data_size + per_account_size;
        let cache =
            ReadOnlyAccountsCache::new(max, READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS);
        let slot = 0;
        assert!(cache.load(Pubkey::default(), slot).is_none());
        assert_eq!(0, cache.cache_len());
        assert_eq!(0, cache.data_size());
        cache.remove(Pubkey::default(), slot); // assert no panic
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let key3 = Pubkey::new_unique();
        let account1 = AccountSharedData::from(Account {
            data: vec![0; data_size],
            ..Account::default()
        });
        let mut account2 = account1.clone();
        account2.checked_add_lamports(1).unwrap(); // so they compare differently
        let mut account3 = account1.clone();
        account3.checked_add_lamports(4).unwrap(); // so they compare differently
        cache.store(key1, slot, account1.clone());
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.store(key2, slot, account2.clone());
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account2));
        assert_eq!(1, cache.cache_len());
        cache.store(key2, slot, account1.clone()); // overwrite key2 with account1
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.remove(key2, slot);
        assert_eq!(0, cache.data_size());
        assert_eq!(0, cache.cache_len());

        // can store 2 items, 3rd item kicks oldest item out
        let max = (data_size + per_account_size) * 2;
        let cache =
            ReadOnlyAccountsCache::new(max, READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS);
        cache.store(key1, slot, account1.clone());
        assert_eq!(100 + per_account_size, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert_eq!(1, cache.cache_len());
        cache.store(key2, slot, account2.clone());
        assert_eq!(max, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account2));
        assert_eq!(2, cache.cache_len());
        cache.store(key2, slot, account1.clone()); // overwrite key2 with account1
        assert_eq!(max, cache.data_size());
        assert!(accounts_equal(&cache.load(key1, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account1));
        assert_eq!(2, cache.cache_len());
        cache.store(key3, slot, account3.clone());
        assert_eq!(max, cache.data_size());
        assert!(cache.load(key1, slot).is_none()); // was lru purged
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(key3, slot).unwrap(), &account3));
        assert_eq!(2, cache.cache_len());
    }

    /// tests like to deterministically update lru always
    const READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS: u32 = 0;

    #[test]
    fn test_read_only_accounts_cache_random() {
        const SEED: [u8; 32] = [0xdb; 32];
        const DATA_SIZE: usize = 19;
        const MAX_CACHE_SIZE: usize = 17 * (CACHE_ENTRY_SIZE + DATA_SIZE);
        let mut rng = ChaChaRng::from_seed(SEED);
        let cache = ReadOnlyAccountsCache::new(
            MAX_CACHE_SIZE,
            READ_ONLY_CACHE_MS_TO_SKIP_LRU_UPDATE_FOR_TESTS,
        );
        let slots: Vec<Slot> = repeat_with(|| rng.gen_range(0, 1000)).take(5).collect();
        let pubkeys: Vec<Pubkey> = repeat_with(|| {
            let mut arr = [0u8; 32];
            rng.fill(&mut arr[..]);
            Pubkey::new_from_array(arr)
        })
        .take(7)
        .collect();
        let mut hash_map = HashMap::<ReadOnlyCacheKey, (AccountSharedData, usize)>::new();
        for ix in 0..1000 {
            if rng.gen_bool(0.1) {
                let key = *cache.cache.iter().choose(&mut rng).unwrap().key();
                let (pubkey, slot) = key;
                let account = cache.load(pubkey, slot).unwrap();
                let (other, index) = hash_map.get_mut(&key).unwrap();
                assert_eq!(account, *other);
                *index = ix;
            } else {
                let mut data = vec![0u8; DATA_SIZE];
                rng.fill(&mut data[..]);
                let account = AccountSharedData::from(Account {
                    lamports: rng.gen(),
                    data,
                    executable: rng.gen(),
                    rent_epoch: rng.gen(),
                    owner: Pubkey::default(),
                });
                let slot = *slots.choose(&mut rng).unwrap();
                let pubkey = *pubkeys.choose(&mut rng).unwrap();
                let key = (pubkey, slot);
                hash_map.insert(key, (account.clone(), ix));
                cache.store(pubkey, slot, account);
            }
        }
        assert_eq!(cache.cache_len(), 17);
        assert_eq!(hash_map.len(), 35);
        let index = hash_map
            .iter()
            .filter(|(k, _)| cache.cache.contains_key(k))
            .map(|(_, (_, ix))| *ix)
            .min()
            .unwrap();
        for (key, (account, ix)) in hash_map {
            let (pubkey, slot) = key;
            assert_eq!(
                cache.load(pubkey, slot),
                if ix < index { None } else { Some(account) }
            );
        }
    }
}
