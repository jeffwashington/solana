use crate::accounts_index::AccountMapEntry;
use crate::accounts_index::{IsCached, SlotList, ACCOUNTS_INDEX_CONFIG_FOR_TESTING};
use crate::bucket_map_holder::{BucketMapHolder, K};

use crate::pubkey_bins::PubkeyBinCalculator16;
use rayon::prelude::*;
use solana_bucket_map::bucket_map::BucketMap;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::fmt::Debug;
use std::ops::{Range, RangeBounds};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread::{Builder, JoinHandle};

pub type V2<T> = AccountMapEntry<T>;
pub type SlotT<T> = (Slot, T);

#[derive(Debug)]
pub struct InMemAccountsIndex<V: IsCached> {
    disk: Arc<BucketMapHolder<V>>,
    bin_index: usize,
}

#[derive(Debug)]
pub struct AccountsIndexBackground {
    exit: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for AccountsIndexBackground {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Relaxed);
        if let Some(x) = self.handle.take() {
            x.join().unwrap()
        }
    }
}

impl<V: IsCached> InMemAccountsIndex<V> {
    pub fn new(bucket_map: &Arc<BucketMapHolder<V>>, bin_index: usize) -> Self {
        Self {
            disk: bucket_map.clone(),
            bin_index,
        }
    }

    pub fn new_for_testing() -> Self {
        let bins = ACCOUNTS_INDEX_CONFIG_FOR_TESTING.bins.unwrap();
        let map = Self::new_bucket_map(bins);
        Self::new(&map, 0)
    }

    pub fn new_bucket_map(bins: usize) -> Arc<BucketMapHolder<V>> {
        let buckets = PubkeyBinCalculator16::log_2(bins as u32) as u8;
        Arc::new(BucketMapHolder::new(BucketMap::new_buckets(buckets)))
    }

    // create bg thread pool for flushing accounts index to disk
    pub fn create_bg_flusher(&self, mut threads: usize) -> AccountsIndexBackground {
        let bucket_map_ = self.disk.clone();
        threads = std::cmp::min(threads, self.disk.bins); // doesn't make sense to have more threads than bins
        let exit = Arc::new(AtomicBool::new(false));
        let exit_ = exit.clone();
        let handle = Some(
            Builder::new()
                .name("solana-index-flusher".to_string())
                .spawn(move || {
                    (0..threads).into_par_iter().for_each(|_| {
                        bucket_map_.bg_flusher(exit_.clone());
                    });
                })
                .unwrap(),
        );
        AccountsIndexBackground { exit, handle }
    }

    pub fn hold_range_in_memory<R>(&self, range: &R, hold: bool)
    where
        R: RangeBounds<Pubkey>,
    {
        self.disk.hold_range_in_memory(self.bin_index, range, hold);
    }

    pub fn iter<R>(&self, range: Option<&R>) -> Vec<(K, V2<V>)>
    where
        R: RangeBounds<Pubkey>,
    {
        self.disk.range(self.bin_index, range)
    }

    pub fn keys(&self) -> Vec<Pubkey> {
        self.disk
            .keys(self.bin_index, None::<&Range<Pubkey>>)
            .unwrap_or_default()
    }

    pub fn remove_if_slot_list_empty(&self, key: &Pubkey) -> bool {
        self.disk.remove_if_slot_list_empty(self.bin_index, key)
    }

    pub fn upsert(
        &self,
        pubkey: &Pubkey,
        new_value: AccountMapEntry<V>,
        reclaims: &mut SlotList<V>,
        previous_slot_entry_was_cached: bool,
    ) {
        self.disk.upsert(
            self.bin_index,
            pubkey,
            new_value,
            reclaims,
            previous_slot_entry_was_cached,
        );
    }

    pub fn get(&self, key: &K) -> Option<V2<V>> {
        self.disk.get(self.bin_index, key)
    }
    pub fn remove(&mut self, key: &K) {
        self.disk.delete_key(self.bin_index, key);
    }
    // This is expensive. We could use a counter per bucket to keep track of adds/deletes.
    // But, this is only used for metrics.
    pub fn len_expensive(&self) -> usize {
        self.disk.len_expensive(self.bin_index)
    }

    pub fn set_startup(&self, startup: bool) {
        self.disk.set_startup(startup);
    }

    pub fn update_or_insert_async(&self, pubkey: Pubkey, new_entry: AccountMapEntry<V>) {
        self.disk
            .update_or_insert_async(self.bin_index, pubkey, new_entry);
    }

    pub fn wait_for_flush_idle(&self) {
        self.disk.wait_for_flush_idle();
    }
}
