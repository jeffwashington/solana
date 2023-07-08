//! ReadOnlyAccountsCache used to store accounts, such as executable accounts,
//! which can be large, loaded many times, and rarely change.
use {
    dashmap::{mapref::entry::Entry, DashMap},
    //index_list::{Index as CrateIndex, IndexList as CrateIndexList},
    solana_measure::measure_us,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::Slot,
        pubkey::Pubkey,
    },
    std::sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Mutex,
    },
};

const CACHE_ENTRY_SIZE: usize =
    std::mem::size_of::<ReadOnlyAccountCacheEntry>() + 2 * std::mem::size_of::<ReadOnlyCacheKey>();

type ReadOnlyCacheKey = (Pubkey, Slot);

#[derive(Debug)]
struct ReadOnlyAccountCacheEntry {
    account: AccountSharedData,
    index: Index, // Index of the entry in the eviction queue.
}

#[derive(Debug)]
pub(crate) struct ReadOnlyAccountsCache {
    cache: DashMap<ReadOnlyCacheKey, ReadOnlyAccountCacheEntry>,
    // When an item is first entered into the cache, it is added to the end of
    // the queue. Also each time an entry is looked up from the cache it is
    // moved to the end of the queue. As a result, items in the queue are
    // always sorted in the order that they have last been accessed. When doing
    // LRU eviction, cache entries are evicted from the front of the queue.
    queue: Mutex<IndexList<ReadOnlyCacheKey>>,
    max_data_size: usize,
    data_size: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    evicts: AtomicU64,
    eviction_us: AtomicU64,
    update_lru_us: AtomicU64,
    load_get_mut_us: AtomicU64,
    account_clone_us: AtomicU64,
    index_same: AtomicU64,
    selector: AtomicU64,
    
}

impl ReadOnlyAccountsCache {
    pub(crate) fn new(max_data_size: usize) -> Self {
        Self {
            max_data_size,
            cache: DashMap::default(),
            queue: Mutex::<IndexList<ReadOnlyCacheKey>>::default(),
            data_size: AtomicUsize::default(),
            hits: AtomicU64::default(),
            misses: AtomicU64::default(),
            evicts: AtomicU64::default(),
            eviction_us: AtomicU64::default(),
            update_lru_us: AtomicU64::default(),
            load_get_mut_us:  AtomicU64::default(),
            index_same: AtomicU64::default(),
            account_clone_us: AtomicU64::default(),
            selector:  AtomicU64::default(),
        }
    }

    /// reset the read only accounts cache
    /// useful for benches/tests
    pub fn reset_for_tests(&self) {
        self.cache.clear();
        self.queue.lock().unwrap().clear();
        self.data_size.store(0, Ordering::Relaxed);
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evicts.store(0, Ordering::Relaxed);
    }

    /// true if pubkey is in cache at slot
    pub fn in_cache(&self, pubkey: &Pubkey, slot: Slot) -> bool {
        self.cache.contains_key(&(*pubkey, slot))
    }

    pub(crate) fn load(&self, pubkey: Pubkey, slot: Slot) -> Option<AccountSharedData> {
        let timestamp = solana_sdk::timing::timestamp() / 100000;
        let selector = timestamp % 4;
        self.selector.store(selector, Ordering::Relaxed);
        if selector == 0 {
            let key = (pubkey, slot);
            use solana_measure::measure::Measure;
            let mut m = Measure::start("");
            let mut entry = match self.cache.get_mut(&key) {
                None => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Some(entry) => entry,
            };
            m.stop();
            // Move the entry to the end of the queue.
            // self.queue is modified while holding a reference to the cache entry;
            // so that another thread cannot write to the same key.
            let (_, update_lru_us) = measure_us!({
                let mut queue = self.queue.lock().unwrap();
                queue.remove(entry.index);
                entry.index = queue.insert_last(key);
            });
            let ( account, account_clone_us) = measure_us!(entry.account.clone());
            self.account_clone_us.fetch_add(account_clone_us, Ordering::Relaxed);
            drop(entry);
            self.load_get_mut_us.fetch_add(m.as_us(), Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.update_lru_us
                .fetch_add(update_lru_us, Ordering::Relaxed);
            Some(account)
        }
        else if selector == 3 {
            let key = (pubkey, slot);
            use solana_measure::measure::Measure;
            let mut m = Measure::start("");
            let mut entry = match self.cache.get_mut(&key) {
                None => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Some(entry) => entry,
            };
            m.stop();
            self.hits.fetch_add(1, Ordering::Relaxed);
            // Move the entry to the end of the queue.
            // self.queue is modified while holding a reference to the cache entry;
            // so that another thread cannot write to the same key.
            let (_, update_lru_us) = measure_us!({
                let mut queue = self.queue.lock().unwrap();
                queue.remove(entry.index);
                entry.index = queue.insert_last(key);
            });
            let ( account, account_clone_us) = measure_us!(entry.account.clone());
            drop(entry);
            self.account_clone_us.fetch_add(account_clone_us, Ordering::Relaxed);
            self.load_get_mut_us.fetch_add(m.as_us(), Ordering::Relaxed);
            self.update_lru_us
                .fetch_add(update_lru_us, Ordering::Relaxed);
            Some(account)
        }
        else if selector == 1 {
            // read lock
            let key = (pubkey, slot);
            use solana_measure::measure::Measure;
            let mut m = Measure::start("");
            let entry = match self.cache.get(&key) {
                None => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Some(entry) => entry,
            };
            m.stop();
            // Move the entry to the end of the queue.
            // self.queue is modified while holding a reference to the cache entry;
            // so that another thread cannot write to the same key.
            let (_, update_lru_us) = measure_us!({
                let mut queue = self.queue.lock().unwrap();
                queue.move_to_last(entry.index);
            });
            let ( account, account_clone_us) = measure_us!(entry.account.clone());
            self.account_clone_us.fetch_add(account_clone_us, Ordering::Relaxed);
            drop(entry);
            self.load_get_mut_us.fetch_add(m.as_us(), Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.update_lru_us
                .fetch_add(update_lru_us, Ordering::Relaxed);
            Some(account)
        }
        else {
            // downgrade write lock to read lock
            let key = (pubkey, slot);
            use solana_measure::measure::Measure;
            let mut m = Measure::start("");
            let mut entry = match self.cache.get_mut(&key) {
                None => {
                    self.misses.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
                Some(entry) => entry,
            };
            m.stop();
            // Move the entry to the end of the queue.
            // self.queue is modified while holding a reference to the cache entry;
            // so that another thread cannot write to the same key.
            let (_, update_lru_us) = measure_us!({
                let mut queue = self.queue.lock().unwrap();
                queue.remove(entry.index);
                entry.index = queue.insert_last(key);
            });
            let entry = entry.downgrade();
            self.load_get_mut_us.fetch_add(m.as_us(), Ordering::Relaxed);
            self.hits.fetch_add(1, Ordering::Relaxed);
            self.update_lru_us
                .fetch_add(update_lru_us, Ordering::Relaxed);
            let ( account, account_clone_us) = measure_us!(entry.account.clone());
            self.account_clone_us.fetch_add(account_clone_us, Ordering::Relaxed);
            Some(account)
        }
    }

    fn account_size(&self, account: &AccountSharedData) -> usize {
        CACHE_ENTRY_SIZE + account.data().len()
    }

    pub(crate) fn store(&self, pubkey: Pubkey, slot: Slot, account: AccountSharedData) {
        let key = (pubkey, slot);
        let account_size = self.account_size(&account);
        self.data_size.fetch_add(account_size, Ordering::Relaxed);
        // self.queue is modified while holding a reference to the cache entry;
        // so that another thread cannot write to the same key.
        match self.cache.entry(key) {
            Entry::Vacant(entry) => {
                // Insert the entry at the end of the queue.
                let mut queue = self.queue.lock().unwrap();
                let index = queue.insert_last(key);
                entry.insert(ReadOnlyAccountCacheEntry { account, index });
            }
            Entry::Occupied(mut entry) => {
                let entry = entry.get_mut();
                let account_size = self.account_size(&entry.account);
                self.data_size.fetch_sub(account_size, Ordering::Relaxed);
                entry.account = account;
                // Move the entry to the end of the queue.
                let mut queue = self.queue.lock().unwrap();
                queue.remove(entry.index);
                entry.index = queue.insert_last(key);
            }
        };
    }

    pub(crate) fn should_evict(&self) -> bool {
        self.data_size.load(Ordering::Relaxed) > self.max_data_size
    }

    pub(crate) fn evict_old(&self) {
        // Evict entries from the front of the queue.
        let mut num_evicts = 0;
        let (_, eviction_us) = measure_us!({
            while self.should_evict() {
                let (pubkey, slot) = match self.queue.lock().unwrap().get_first() {
                    None => break,
                    Some(key) => *key,
                };
                num_evicts += 1;
                self.remove(pubkey, slot);
            }
        });
        self.evicts.fetch_add(num_evicts, Ordering::Relaxed);
        self.eviction_us.fetch_add(eviction_us, Ordering::Relaxed);
    }

    pub(crate) fn remove(&self, pubkey: Pubkey, slot: Slot) -> Option<AccountSharedData> {
        let (_, entry) = self.cache.remove(&(pubkey, slot))?;
        // self.queue should be modified only after removing the entry from the
        // cache, so that this is still safe if another thread writes to the
        // same key.
        self.queue.lock().unwrap().remove(entry.index);
        let account_size = self.account_size(&entry.account);
        self.data_size.fetch_sub(account_size, Ordering::Relaxed);
        Some(entry.account)
    }

    pub(crate) fn cache_len(&self) -> usize {
        self.cache.len()
    }

    pub(crate) fn data_size(&self) -> usize {
        self.data_size.load(Ordering::Relaxed)
    }

    pub(crate) fn get_and_reset_stats(&self) -> (u64, u64, u64, u64, u64, u64, u64, u64, u64) {
        let hits = self.hits.swap(0, Ordering::Relaxed);
        let misses = self.misses.swap(0, Ordering::Relaxed);
        let evicts = self.evicts.swap(0, Ordering::Relaxed);
        let eviction_us = self.eviction_us.swap(0, Ordering::Relaxed);
        let update_lru_us = self.update_lru_us.swap(0, Ordering::Relaxed);
        let load_get_mut_us = self.load_get_mut_us.swap(0, Ordering::Relaxed);
        let index_same: u64 = self.index_same.swap(0, Ordering::Relaxed);
        let account_clone_us = self.account_clone_us.swap(0, Ordering::Relaxed);
        let selector = self.selector.load(Ordering::Relaxed);
        
        (hits, misses, evicts, eviction_us, update_lru_us, load_get_mut_us, index_same, account_clone_us, selector)
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
        let cache = ReadOnlyAccountsCache::new(max);
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
        cache.evict_old();
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
        let cache = ReadOnlyAccountsCache::new(max);
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
        cache.evict_old();
        assert_eq!(max, cache.data_size());
        assert!(cache.load(key1, slot).is_none()); // was lru purged
        assert!(accounts_equal(&cache.load(key2, slot).unwrap(), &account1));
        assert!(accounts_equal(&cache.load(key3, slot).unwrap(), &account3));
        assert_eq!(2, cache.cache_len());
    }

    #[test]
    fn test_read_only_accounts_cache_random() {
        const SEED: [u8; 32] = [0xdb; 32];
        const DATA_SIZE: usize = 19;
        const MAX_CACHE_SIZE: usize = 17 * (CACHE_ENTRY_SIZE + DATA_SIZE);
        let mut rng = ChaChaRng::from_seed(SEED);
        let cache = ReadOnlyAccountsCache::new(MAX_CACHE_SIZE);
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
                cache.evict_old();
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

use std::convert::TryFrom;
use std::fmt;
use std::iter::DoubleEndedIterator;
use std::iter::{Extend, FromIterator, FusedIterator};
use std::mem;
use std::num::NonZeroU32;

/// Vector index for the elements in the list. They are typically not
/// squential.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct Index(Option<NonZeroU32>);

impl Index {
    #[inline]
    pub fn new() -> Index {
        Index { 0: None }
    }
    #[inline]
    /// Returns `true` for a valid index.
    ///
    /// A valid index can be used in IndexList method calls.
    pub fn is_some(&self) -> bool {
        self.0.is_some()
    }
    #[inline]
    /// Returns `true` for an invalid index.
    ///
    /// An invalid index will always be ignored and have `None` returned from
    /// any IndexList method call that returns something.
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }
    #[inline]
    fn get(&self) -> Option<usize> {
        Some(self.0?.get() as usize - 1)
    }
    #[inline]
    fn set(mut self, index: Option<usize>) -> Self {
        if let Some(n) = index {
            if let Ok(num) = NonZeroU32::try_from(n as u32 + 1) {
                self.0 = Some(num);
            } else {
                self.0 = None;
            }
        } else {
            self.0 = None;
        }
        self
    }
}

impl From<u32> for Index {
    fn from(index: u32) -> Index {
        Index::new().set(Some(index as usize))
    }
}

impl From<u64> for Index {
    fn from(index: u64) -> Index {
        Index::new().set(Some(index as usize))
    }
}

impl From<usize> for Index {
    fn from(index: usize) -> Index {
        Index::new().set(Some(index))
    }
}

impl From<Option<usize>> for Index {
    fn from(index: Option<usize>) -> Index {
        Index::new().set(index)
    }
}

impl fmt::Display for Index {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ndx) = self.0 {
            write!(f, "{}", ndx)
        } else {
            write!(f, "|")
        }
    }
}

#[derive(Clone, Debug, Default)]
struct IndexNode {
    next: Index,
    prev: Index,
}

impl IndexNode {
    #[inline]
    fn new() -> IndexNode {
        IndexNode {
            next: Index::new(),
            prev: Index::new(),
        }
    }
    #[inline]
    fn new_next(&mut self, next: Index) -> Index {
        mem::replace(&mut self.next, next)
    }
    #[inline]
    fn new_prev(&mut self, prev: Index) -> Index {
        mem::replace(&mut self.prev, prev)
    }
}

impl fmt::Display for IndexNode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}<>{}", self.next, self.prev)
    }
}

#[derive(Clone, Debug, Default)]
struct IndexEnds {
    head: Index,
    tail: Index,
}

impl IndexEnds {
    #[inline]
    fn new() -> Self {
        IndexEnds {
            head: Index::new(),
            tail: Index::new(),
        }
    }
    #[inline]
    fn clear(&mut self) {
        self.new_both(Index::new());
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.head.is_none()
    }
    #[inline]
    fn new_head(&mut self, head: Index) -> Index {
        mem::replace(&mut self.head, head)
    }
    #[inline]
    fn new_tail(&mut self, tail: Index) -> Index {
        mem::replace(&mut self.tail, tail)
    }
    #[inline]
    fn new_both(&mut self, both: Index) {
        self.head = both;
        self.tail = both;
    }
}

impl fmt::Display for IndexEnds {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}>=<{}", self.head, self.tail)
    }
}

/// Doubly-linked list implemented in safe Rust.
#[derive(Debug)]
pub struct IndexList<T> {
    elems: Vec<Option<T>>,
    nodes: Vec<IndexNode>,
    used: IndexEnds,
    free: IndexEnds,
    size: usize,
}

impl<T> Default for IndexList<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> IndexList<T> {
    /// Creates a new empty index list.
    ///
    /// Example:
    /// ```rust
    /// use index_list::IndexList;
    ///
    /// let list = IndexList::<u64>::new();
    /// ```
    pub fn new() -> Self {
        IndexList {
            elems: Vec::new(),
            nodes: Vec::new(),
            used: IndexEnds::new(),
            free: IndexEnds::new(),
            size: 0,
        }
    }
    /// Returns the current capacity of the list.
    ///
    /// This value is always greater than or equal to the length.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let cap = list.capacity();
    /// assert!(cap >= list.len());
    /// ```
    #[inline]
    pub fn capacity(&self) -> usize {
        self.elems.len()
    }
    /// Returns the number of valid elements in the list.
    ///
    /// This value is always less than or equal to the capacity.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_first(42);
    /// let first = list.remove_first();
    /// assert!(list.len() < list.capacity());
    /// ```
    #[inline]
    pub fn len(&self) -> usize {
        self.size
    }
    /// Clears the list be removing all elements, making it empty.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// list.clear();
    /// assert!(list.is_empty());
    /// ```
    #[inline]
    pub fn clear(&mut self) {
        self.elems.clear();
        self.nodes.clear();
        self.used.clear();
        self.free.clear();
        self.size = 0;
    }
    /// Returns `true` when the list is empty.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// let list = IndexList::<u64>::new();
    /// assert!(list.is_empty());
    /// ```
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.used.is_empty()
    }
    /// Returns `true` if the index is valid.
    #[inline]
    pub fn is_index_used(&self, index: Index) -> bool {
        self.get(index).is_some()
    }
    /// Returns the index of the first element, or `None` if the list is empty.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let index = list.first_index();
    /// ```
    #[inline]
    pub fn first_index(&self) -> Index {
        self.used.head
    }
    /// Returns the index of the last element, or `None` if the list is empty.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let index = list.last_index();
    /// ```
    #[inline]
    pub fn last_index(&self) -> Index {
        self.used.tail
    }
    /// Returns the index of the next element, after index, or `None` when the
    /// end is reached.
    ///
    /// If index is `None` then the first index in the list is returned.
    ///
    /// *NOTE* that indexes are likely not sequential.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let mut index = list.first_index();
    /// while index.is_some() {
    ///     // Do something
    ///     index = list.next_index(index);
    /// }
    /// ```
    #[inline]
    pub fn next_index(&self, index: Index) -> Index {
        if let Some(ndx) = index.get() {
            if let Some(node) = self.nodes.get(ndx) {
                node.next
            } else {
                Index::new()
            }
        } else {
            self.first_index()
        }
    }
    /// Returns the index of the previous element, before index, or `None` when
    /// the beginning is reached.
    ///
    /// If index is `None` then the last index in the list is returned.
    ///
    /// *NOTE* that indexes are likely not sequential.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let mut index = list.last_index();
    /// while index.is_some() {
    ///     // Do something
    ///     index = list.prev_index(index);
    /// }
    /// ```
    #[inline]
    pub fn prev_index(&self, index: Index) -> Index {
        if let Some(ndx) = index.get() {
            if let Some(node) = self.nodes.get(ndx) {
                node.prev
            } else {
                Index::new()
            }
        } else {
            self.last_index()
        }
    }
    /// Move to an index `steps` number of elements away. Positive numbers will
    /// move in the next direction, while negative number in the prev direction.
    ///
    /// Returns the index `steps` elements away, or `None` when the end is
    /// reached.
    ///
    /// *NOTE* that indexes are likely not sequential.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::from(&mut vec!["A", "B", "C", "D", "E"]);
    /// let mut index = list.first_index();
    /// index = list.move_index(index, 3);
    /// // Do something with the 4:th element
    /// # assert_eq!(list.get(index), Some(&"D"));
    /// index = list.move_index(index, -2);
    /// // Do something with the 2:nd element
    /// # assert_eq!(list.get(index), Some(&"B"));
    /// index = list.move_index(index, -2);
    /// assert!(index.is_none());
    /// ```
    #[inline]
    pub fn move_index(&self, index: Index, steps: i32) -> Index {
        let mut index = index;
        match steps.cmp(&0) {
            core::cmp::Ordering::Greater => {
                (0..steps).for_each(|_| {
                    index = self.next_index(index);
                });
            }
            core::cmp::Ordering::Less => {
                (0..-steps).for_each(|_| {
                    index = self.prev_index(index);
                });
            }
            core::cmp::Ordering::Equal => (),
        }
        index
    }
    /// Get a reference to the first element data, or `None`.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let data = list.get_first();
    /// ```
    #[inline]
    pub fn get_first(&self) -> Option<&T> {
        self.get(self.first_index())
    }
    /// Get a reference to the last element data, or `None`.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// let data = list.get_last();
    /// ```
    #[inline]
    pub fn get_last(&self) -> Option<&T> {
        self.get(self.last_index())
    }
    /// Get an immutable reference to the element data at the index, or `None`.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let list = IndexList::<u64>::new();
    /// # let index = list.first_index();
    /// let data = list.get(index);
    /// ```
    #[inline]
    pub fn get(&self, index: Index) -> Option<&T> {
        let ndx = index.get().unwrap_or(usize::MAX);
        self.elems.get(ndx)?.as_ref()
    }
    /// Get a mutable reference to the first element data, or `None`.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_first(1);
    /// if let Some(data) = list.get_mut_first() {
    ///     // Update the data somehow
    ///     *data = 0;
    /// }
    /// # assert_eq!(list.get_first(), Some(&0u64));
    /// ```
    #[inline]
    pub fn get_mut_first(&mut self) -> Option<&mut T> {
        self.get_mut(self.first_index())
    }
    /// Get a mutable reference to the last element data, or `None`.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_first(2);
    /// if let Some(data) = list.get_mut_last() {
    ///     // Update the data somehow
    ///     *data *= 2;
    /// }
    /// # assert_eq!(list.get_last(), Some(&4u64));
    /// ```
    #[inline]
    pub fn get_mut_last(&mut self) -> Option<&mut T> {
        self.get_mut(self.last_index())
    }
    /// Get a mutable reference to the element data at the index, or `None`.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_first(0);
    /// # let index = list.first_index();
    /// if let Some(data) = list.get_mut(index) {
    ///     // Update the data somehow
    ///     *data += 1;
    /// }
    /// # assert_eq!(list.get_last(), Some(&1u64));
    /// ```
    #[inline]
    pub fn get_mut(&mut self, index: Index) -> Option<&mut T> {
        if let Some(ndx) = index.get() {
            if ndx < self.capacity() {
                return self.elems[ndx].as_mut();
            }
        }
        None
    }
    /// Swap the element data between two indexes.
    ///
    /// Both indexes must be valid.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_first(1);
    /// # list.insert_last(2);
    /// list.swap_index(list.first_index(), list.last_index());
    /// # assert_eq!(list.get_first(), Some(&2u64));
    /// # assert_eq!(list.get_last(), Some(&1u64));
    /// ```
    #[inline]
    pub fn swap_index(&mut self, this: Index, that: Index) {
        if let Some(here) = this.get() {
            if let Some(there) = that.get() {
                self.swap_data(here, there);
            }
        }
    }
    /// Peek at next element data, after the index, if any.
    ///
    /// Returns `None` if there is no next index in the list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![1, 2, 3]);
    /// # let index = list.first_index();
    /// if let Some(data) = list.peek_next(index) {
    ///     // Consider the next data
    /// #   assert_eq!(*data, 2);
    /// }
    /// ```
    #[inline]
    pub fn peek_next(&self, index: Index) -> Option<&T> {
        self.get(self.next_index(index))
    }
    /// Peek at previous element data, before the index, if any.
    ///
    /// Returns `None` if there is no previous index in the list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![1, 2, 3]);
    /// # let index = list.last_index();
    /// if let Some(data) = list.peek_prev(index) {
    ///     // Consider the previous data
    /// #   assert_eq!(*data, 2);
    /// }
    /// ```
    #[inline]
    pub fn peek_prev(&self, index: Index) -> Option<&T> {
        self.get(self.prev_index(index))
    }
    /// Returns `true` if the element is in the list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # let index = list.insert_first(42);
    /// if list.contains(42) {
    ///     // Find it?
    /// } else {
    ///     // Insert it?
    /// }
    /// ```
    #[inline]
    pub fn contains(&self, elem: T) -> bool
    where
        T: PartialEq,
    {
        self.elems.contains(&Some(elem))
    }
    /// Returns the index of the element containg the data.
    ///
    /// If there is more than one element with the same data, the one with the
    /// lowest index will always be returned.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::{Index, IndexList};
    /// # let mut list = IndexList::from(&mut vec![1, 2, 3]);
    /// let index = list.index_of(2);
    /// # assert_eq!(index, Index::from(1u32))
    /// ```
    #[inline]
    pub fn index_of(&self, elem: T) -> Index
    where
        T: PartialEq,
    {
        Index::from(self.elems.iter().position(|e| {
            if let Some(data) = e {
                data == &elem
            } else {
                false
            }
        }))
    }
    /// Insert a new element at the beginning.
    ///
    /// It is usually not necessary to keep the index, as the element data
    /// can always be found again by walking the list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// let index = list.insert_first(42);
    /// ```
    pub fn insert_first(&mut self, elem: T) -> Index {
        let this = self.new_node(Some(elem));
        self.linkin_first(this);
        this
    }
    /// Insert a new element at the end.
    ///
    /// It is typically not necessary to store the index, as the data will be
    /// there when walking the list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// let index = list.insert_last(42);
    /// ```
    pub fn insert_last(&mut self, elem: T) -> Index {
        let this = self.new_node(Some(elem));
        self.linkin_last(this);
        this
    }
    /// Insert a new element before the index.
    ///
    /// If the index is `None` then the new element will be inserted first.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # let mut index = list.last_index();
    /// index = list.insert_before(index, 42);
    /// ```
    pub fn insert_before(&mut self, index: Index, elem: T) -> Index {
        if index.is_none() {
            return self.insert_first(elem);
        }
        let this = self.new_node(Some(elem));
        self.linkin_this_before_that(this, index);
        this
    }
    /// Insert a new element after the index.
    ///
    /// If the index is `None` then the new element will be inserted last.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # let mut index = list.first_index();
    /// index = list.insert_after(index, 42);
    /// ```
    pub fn insert_after(&mut self, index: Index, elem: T) -> Index {
        if index.is_none() {
            return self.insert_last(elem);
        }
        let this = self.new_node(Some(elem));
        self.linkin_this_after_that(this, index);
        this
    }
    /// Remove the first element and return its data.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_first(42);
    /// let data = list.remove_first();
    /// # assert_eq!(data, Some(42));
    /// ```
    pub fn remove_first(&mut self) -> Option<T> {
        self.remove(self.first_index())
    }
    /// Remove the last element and return its data.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::<u64>::new();
    /// # list.insert_last(42);
    /// let data = list.remove_last();
    /// # assert_eq!(data, Some(42));
    /// ```
    pub fn remove_last(&mut self) -> Option<T> {
        self.remove(self.last_index())
    }
    /// Remove the element at the index and return its data.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec!["A", "B", "C"]);
    /// # let mut index = list.first_index();
    /// # index = list.next_index(index);
    /// let data = list.remove(index);
    /// # assert_eq!(data, Some("B"));
    /// ```
    pub fn remove(&mut self, index: Index) -> Option<T> {
        let elem_opt = self.remove_elem_at_index(index);
        if elem_opt.is_some() {
            self.linkout_used(index);
            self.linkin_free(index);
        }
        elem_opt
    }

    pub fn move_to_last(&mut self, index: Index) {
        // unlink where it is
        self.linkout_used(index);
        self.linkin_free(index);
        self.linkin_last(index);
    }

    /// Create a new iterator over all the elements.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![120, 240, 360]);
    /// let total: usize = list.iter().sum();
    /// assert_eq!(total, 720);
    /// ```
    #[inline]
    pub fn iter(&self) -> Iter<T> {
        Iter {
            list: &self,
            next: self.first_index(),
            prev: self.last_index(),
        }
    }
    /// Create a draining iterator over all the elements.
    ///
    /// This iterator will remove the elements as it is iterating over them.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec!["A", "B", "C"]);
    /// let items: Vec<&str> = list.drain_iter().collect();
    /// assert_eq!(list.len(), 0);
    /// assert_eq!(items, vec!["A", "B", "C"]);
    /// ```
    #[inline]
    pub fn drain_iter(&mut self) -> DrainIter<T> {
        DrainIter { 0: self }
    }
    /// Create a vector for all elements.
    ///
    /// Returns a new vector with immutable reference to the elements data.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![1, 2, 3]);
    /// let vector: Vec<&u64> = list.to_vec();
    /// # assert_eq!(format!("{:?}", vector), "[1, 2, 3]");
    /// ```
    pub fn to_vec(&self) -> Vec<&T> {
        self.iter().filter_map(Option::Some).collect()
    }
    /// Insert all the elements from the vector, which will be drained.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// let mut the_numbers = vec![4, 8, 15, 16, 23, 42];
    /// let list = IndexList::from(&mut the_numbers);
    /// assert_eq!(the_numbers.len(), 0);
    /// assert_eq!(list.len(), 6);
    /// ```
    pub fn from(vec: &mut Vec<T>) -> IndexList<T> {
        let mut list = IndexList::<T>::new();
        vec.drain(..).for_each(|elem| {
            list.insert_last(elem);
        });
        list
    }
    /// Remove any unused indexes at the end by truncating.
    ///
    /// If the unused indexes don't appear at the end, then nothing happens.
    ///
    /// No valid indexes are changed.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![4, 8, 15, 16, 23, 42]);
    /// list.remove_last();
    /// assert!(list.len() < list.capacity());
    /// list.trim_safe();
    /// assert_eq!(list.len(), list.capacity());
    /// ```
    pub fn trim_safe(&mut self) {
        let removed: Vec<usize> = (self.len()..self.capacity())
            .rev()
            .take_while(|&i| self.is_free(i))
            .collect();
        removed.iter().for_each(|&i| {
            self.linkout_free(Index::from(i));
        });
        if !removed.is_empty() {
            let left = self.capacity() - removed.len();
            self.nodes.truncate(left);
            self.elems.truncate(left);
        }
    }
    /// Remove all unused elements by swapping indexes and then truncating.
    ///
    /// This will reduce the capacity of the list, but only if there are any
    /// unused elements. Length and capacity will be equal after the call.
    ///
    /// *NOTE* that this call may invalidate some indexes.
    ///
    /// While it is possible to tell if an index has become invalid, because
    /// only indexes at or above the new capacity limit has been moved, it is
    /// not recommended to rely on that fact or test for it.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![4, 8, 15, 16, 23, 42]);
    /// list.remove_first();
    /// assert!(list.len() < list.capacity());
    /// list.trim_swap();
    /// assert_eq!(list.len(), list.capacity());
    /// ```
    pub fn trim_swap(&mut self) {
        let need = self.size;
        // destination is all free node indexes below the needed limit
        let dst: Vec<usize> = self.elems[..need]
            .iter()
            .enumerate()
            .filter(|(n, e)| e.is_none() && n < &need)
            .map(|(n, _e)| n)
            .collect();
        // source is all used node indexes above the needed limit
        let src: Vec<usize> = self.elems[need..]
            .iter()
            .enumerate()
            .filter(|(_n, e)| e.is_some())
            .map(|(n, _e)| n + need)
            .collect();
        debug_assert_eq!(dst.len(), src.len());
        src.iter()
            .zip(dst.iter())
            .for_each(|(s, d)| self.replace_dest_with_source(*s, *d));
        self.free.new_both(Index::new());
        self.elems.truncate(need);
        self.nodes.truncate(need);
    }
    /// Add the elements of the other list at the end.
    ///
    /// The other list will be empty after the call as all its elements have
    /// been moved to this list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![4, 8, 15]);
    /// # let mut other = IndexList::from(&mut vec![16, 23, 42]);
    /// let sum_both = list.len() + other.len();
    /// list.append(&mut other);
    /// assert!(other.is_empty());
    /// assert_eq!(list.len(), sum_both);
    /// # assert_eq!(list.to_string(), "[4 >< 8 >< 15 >< 16 >< 23 >< 42]");
    /// ```
    pub fn append(&mut self, other: &mut IndexList<T>) {
        while let Some(elem) = other.remove_first() {
            self.insert_last(elem);
        }
    }
    /// Add the elements of the other list at the beginning.
    ///
    /// The other list will be empty after the call as all its elements have
    /// been moved to this list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![16, 23, 42]);
    /// # let mut other = IndexList::from(&mut vec![4, 8, 15]);
    /// let sum_both = list.len() + other.len();
    /// list.prepend(&mut other);
    /// assert!(other.is_empty());
    /// assert_eq!(list.len(), sum_both);
    /// # assert_eq!(list.to_string(), "[4 >< 8 >< 15 >< 16 >< 23 >< 42]");
    /// ```
    pub fn prepend(&mut self, other: &mut IndexList<T>) {
        while let Some(elem) = other.remove_last() {
            self.insert_first(elem);
        }
    }
    /// Split the list by moving the elements from the index to a new list.
    ///
    /// The original list will no longer contain the elements data that was
    /// moved to the other list.
    ///
    /// Example:
    /// ```rust
    /// # use index_list::IndexList;
    /// # let mut list = IndexList::from(&mut vec![4, 8, 15, 16, 23, 42]);
    /// # let mut index = list.first_index();
    /// # index = list.next_index(index);
    /// # index = list.next_index(index);
    /// # index = list.next_index(index);
    /// let total = list.len();
    /// let other = list.split(index);
    /// assert!(list.len() < total);
    /// assert_eq!(list.len() + other.len(), total);
    /// # assert_eq!(list.to_string(), "[4 >< 8 >< 15]");
    /// # assert_eq!(other.to_string(), "[16 >< 23 >< 42]");
    /// ```
    pub fn split(&mut self, index: Index) -> IndexList<T> {
        let mut list = IndexList::<T>::new();
        while self.is_index_used(index) {
            list.insert_first(self.remove_last().unwrap());
        }
        list
    }

    #[inline]
    fn is_used(&self, at: usize) -> bool {
        self.elems[at].is_some()
    }
    fn is_free(&self, at: usize) -> bool {
        self.elems[at].is_none()
    }
    #[inline]
    fn get_mut_indexnode(&mut self, at: usize) -> &mut IndexNode {
        &mut self.nodes[at]
    }
    #[inline]
    fn get_indexnode(&self, at: usize) -> &IndexNode {
        &self.nodes[at]
    }
    #[inline]
    fn swap_data(&mut self, here: usize, there: usize) {
        self.elems.swap(here, there);
    }
    #[inline]
    fn set_prev(&mut self, index: Index, new_prev: Index) -> Index {
        if let Some(at) = index.get() {
            self.get_mut_indexnode(at).new_prev(new_prev)
        } else {
            index
        }
    }
    #[inline]
    fn set_next(&mut self, index: Index, new_next: Index) -> Index {
        if let Some(at) = index.get() {
            self.get_mut_indexnode(at).new_next(new_next)
        } else {
            index
        }
    }
    #[inline]
    fn linkin_tail(&mut self, prev: Index, this: Index, next: Index) {
        if next.is_none() {
            let old_tail = self.used.new_tail(this);
            debug_assert_eq!(old_tail, prev);
        }
    }
    #[inline]
    fn linkin_head(&mut self, prev: Index, this: Index, next: Index) {
        if prev.is_none() {
            let old_head = self.used.new_head(this);
            debug_assert_eq!(old_head, next);
        }
    }
    #[inline]
    fn insert_elem_at_index(&mut self, this: Index, elem: Option<T>) {
        if let Some(at) = this.get() {
            self.elems[at] = elem;
            self.size += 1;
        }
    }
    #[inline]
    fn remove_elem_at_index(&mut self, this: Index) -> Option<T> {
        this.get()
            .map(|at| {
                self.size -= 1;
                self.elems[at].take()
            })
            .flatten()
    }
    fn new_node(&mut self, elem: Option<T>) -> Index {
        let reuse = self.free.head;
        if reuse.is_some() {
            self.insert_elem_at_index(reuse, elem);
            self.linkout_free(reuse);
            return reuse;
        }
        let pos = self.nodes.len();
        self.nodes.push(IndexNode::new());
        self.elems.push(elem);
        self.size += 1;
        Index::from(pos)
    }
    fn linkin_free(&mut self, this: Index) {
        debug_assert_eq!(self.is_index_used(this), false);
        let prev = self.free.tail;
        self.set_next(prev, this);
        self.set_prev(this, prev);
        if self.free.is_empty() {
            self.free.new_both(this);
        } else {
            let old_tail = self.free.new_tail(this);
            debug_assert_eq!(old_tail, prev);
        }
    }
    fn linkin_first(&mut self, this: Index) {
        debug_assert!(self.is_index_used(this));
        let next = self.used.head;
        self.set_prev(next, this);
        self.set_next(this, next);
        if self.used.is_empty() {
            self.used.new_both(this);
        } else {
            let old_head = self.used.new_head(this);
            debug_assert_eq!(old_head, next);
        }
    }
    fn linkin_last(&mut self, this: Index) {
        debug_assert!(self.is_index_used(this));
        let prev = self.used.tail;
        self.set_next(prev, this);
        self.set_prev(this, prev);
        if self.used.is_empty() {
            self.used.new_both(this);
        } else {
            let old_tail = self.used.new_tail(this);
            debug_assert_eq!(old_tail, prev);
        }
    }
    // prev? >< that => prev? >< this >< that
    fn linkin_this_before_that(&mut self, this: Index, that: Index) {
        debug_assert!(self.is_index_used(this));
        debug_assert!(self.is_index_used(that));
        let prev = self.set_prev(that, this);
        let old_next = self.set_next(prev, this);
        if old_next.is_some() {
            debug_assert_eq!(old_next, that);
        }
        self.set_prev(this, prev);
        self.set_next(this, that);
        self.linkin_head(prev, this, that);
    }
    // that >< next? => that >< this >< next?
    fn linkin_this_after_that(&mut self, this: Index, that: Index) {
        debug_assert!(self.is_index_used(this));
        debug_assert!(self.is_index_used(that));
        let next = self.set_next(that, this);
        let old_prev = self.set_prev(next, this);
        if old_prev.is_some() {
            debug_assert_eq!(old_prev, that);
        }
        self.set_prev(this, that);
        self.set_next(this, next);
        self.linkin_tail(that, this, next);
    }
    // prev >< this >< next => prev >< next
    fn linkout_node(&mut self, this: Index) -> (Index, Index) {
        let next = self.set_next(this, Index::new());
        let prev = self.set_prev(this, Index::new());
        let old_prev = self.set_prev(next, prev);
        if old_prev.is_some() {
            debug_assert_eq!(old_prev, this);
        }
        let old_next = self.set_next(prev, next);
        if old_next.is_some() {
            debug_assert_eq!(old_next, this);
        }
        (prev, next)
    }
    fn linkout_used(&mut self, this: Index) {
        let (prev, next) = self.linkout_node(this);
        if next.is_none() {
            let old_tail = self.used.new_tail(prev);
            debug_assert_eq!(old_tail, this);
        }
        if prev.is_none() {
            let old_head = self.used.new_head(next);
            debug_assert_eq!(old_head, this);
        }
    }
    fn linkout_free(&mut self, this: Index) {
        let (prev, next) = self.linkout_node(this);
        if next.is_none() {
            let old_tail = self.free.new_tail(prev);
            debug_assert_eq!(old_tail, this);
        }
        if prev.is_none() {
            let old_head = self.free.new_head(next);
            debug_assert_eq!(old_head, this);
        }
    }
    fn replace_dest_with_source(&mut self, src: usize, dst: usize) {
        debug_assert!(self.is_free(dst));
        debug_assert!(self.is_used(src));
        self.linkout_free(Index::from(dst));
        let src_node = self.get_indexnode(src);
        let next = src_node.next;
        let prev = src_node.prev;
        self.linkout_used(Index::from(src));
        self.elems[dst] = self.elems[src].take();
        let this = Index::from(dst);
        if next.is_some() {
            self.linkin_this_before_that(this, next);
        } else if prev.is_some() {
            self.linkin_this_after_that(this, prev);
        } else {
            self.linkin_first(this);
        }
    }
}

impl<T> fmt::Display for IndexList<T>
where
    T: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let elems: Vec<String> = self.iter().map(|x| format!("{}", x)).collect();
        write!(f, "[{}]", elems.join(" >< "))
    }
}

impl<T> From<T> for IndexList<T> {
    fn from(elem: T) -> IndexList<T> {
        let mut list = IndexList::new();
        list.insert_last(elem);
        list
    }
}

impl<T> FromIterator<T> for IndexList<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let mut list = IndexList::new();
        for elem in iter {
            list.insert_last(elem);
        }
        list
    }
}

impl<T> Extend<T> for IndexList<T> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        for elem in iter {
            self.insert_last(elem);
        }
    }
}

/// A double-ended iterator over all the elements in the list. It is fused and
/// can be reversed.
pub struct Iter<'a, T> {
    list: &'a IndexList<T>,
    next: Index,
    prev: Index,
}

impl<'a, T> Iterator for Iter<'a, T> {
    type Item = &'a T;
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let item = self.list.get(self.next);
        self.next = self.list.next_index(self.next);
        item
    }
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let my_len = self.list.len();
        (my_len, Some(my_len))
    }
}
impl<T> FusedIterator for Iter<'_, T> {}

impl<'a, T> DoubleEndedIterator for Iter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let item = self.list.get(self.prev);
        self.prev = self.list.prev_index(self.prev);
        item
    }
}

/// A consuming interator that will remove elements from the list as it is
/// iterating over them. The iterator is fused and can also be reversed.
pub struct DrainIter<'a, T>(&'a mut IndexList<T>);

impl<'a, T> Iterator for DrainIter<'a, T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        self.0.remove_first()
    }
}

impl<'a, T> DoubleEndedIterator for DrainIter<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.remove_last()
    }
}

impl<T> FusedIterator for DrainIter<'_, T> {}

impl<'a, T> IntoIterator for &'a IndexList<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::size_of;

    #[test]
    fn test_struct_sizes() {
        assert_eq!(size_of::<Index>(), 4);
        assert_eq!(size_of::<IndexNode>(), 8);
        assert_eq!(size_of::<IndexEnds>(), 8);
        assert_eq!(size_of::<IndexList<u32>>(), 72);
    }
}
