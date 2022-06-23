use {
    crate::{accounts_db::SnapshotStorages, ancestors::Ancestors, rent_collector::RentCollector},
    log::*,
    rayon::prelude::*,
    solana_measure::measure::Measure,
    solana_sdk::{
        hash::{Hash, Hasher},
        pubkey::Pubkey,
        sysvar::epoch_schedule::EpochSchedule,
    },
    std::{
        borrow::Borrow,
        convert::TryInto,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Mutex,
        },
    },
};
pub const ZERO_RAW_LAMPORTS_SENTINEL: u64 = std::u64::MAX;
pub const MERKLE_FANOUT: usize = 16;

#[derive(Default, Debug)]
pub struct PreviousPass {
    pub reduced_hashes: Vec<Vec<Hash>>,
    pub remaining_unhashed: Vec<Hash>,
    pub lamports: u64,
}

/// parameters to calculate accounts hash
#[derive(Debug)]
pub struct CalcAccountsHashConfig<'a> {
    /// true to use a thread pool dedicated to bg operations
    pub use_bg_thread_pool: bool,
    /// verify every hash in append vec/write cache with a recalculated hash
    /// this option will be removed
    pub check_hash: bool,
    /// 'ancestors' is used to get storages and also used if 'use_write_cache' is true to
    /// get account data from the write cache
    pub ancestors: Option<&'a Ancestors>,
    /// does hash calc need to consider account data that exists in the write cache?
    /// if so, 'ancestors' will be used for this purpose as well as storages.
    pub use_write_cache: bool,
    pub epoch_schedule: &'a EpochSchedule,
    pub rent_collector: &'a RentCollector,
}

impl<'a> CalcAccountsHashConfig<'a> {
    /// return true if we should cache accounts hash intermediate data between calls
    pub fn get_should_cache_hash_data() -> bool {
        // when we are skipping rewrites, we cannot rely on the cached data from old append vecs, so we have to disable caching for now
        false
    }
}

// smallest, 3 quartiles, largest, average
pub type StorageSizeQuartileStats = [usize; 6];

#[derive(Debug, Default)]
pub struct HashStats {
    pub scan_time_total_us: u64,
    pub zeros_time_total_us: u64,
    pub hash_time_total_us: u64,
    pub hash_time_pre_us: u64,
    pub sort_time_total_us: u64,
    pub hash_total: usize,
    pub unreduced_entries: usize,
    pub num_snapshot_storage: usize,
    pub num_slots: usize,
    pub collect_snapshots_us: u64,
    pub storage_sort_us: u64,
    pub min_bin_size: usize,
    pub max_bin_size: usize,
    pub storage_size_quartiles: StorageSizeQuartileStats,
    /// time spent hashing during rehash calls
    pub rehash_hash_us: AtomicU64,
    /// time spent determining whether to rehash during rehash calls
    pub rehash_calc_us: AtomicU64,
    /// # rehashes that took place and were necessary
    pub rehash_required: AtomicUsize,
    /// # rehashes that took place and were UNnecessary
    pub rehash_unnecessary: AtomicUsize,
    pub roots_older_than_epoch: AtomicUsize,
    pub accounts_in_roots_older_than_epoch: AtomicUsize,
    pub append_vec_sizes_older_than_epoch: AtomicUsize,
    /// # ancient append vecs encountered
    pub ancient_append_vecs: AtomicUsize,
}
impl HashStats {
    pub fn calc_storage_size_quartiles(&mut self, storages: &SnapshotStorages) {
        let mut sum = 0;
        let mut sizes = storages
            .iter()
            .flat_map(|storages| {
                let result = storages
                    .iter()
                    .map(|storage| {
                        let cap = storage.accounts.capacity() as usize;
                        sum += cap;
                        cap
                    })
                    .collect::<Vec<_>>();
                result
            })
            .collect::<Vec<_>>();
        sizes.sort_unstable();
        let len = sizes.len();
        self.storage_size_quartiles = if len == 0 {
            StorageSizeQuartileStats::default()
        } else {
            [
                *sizes.first().unwrap(),
                sizes[len / 4],
                sizes[len * 2 / 4],
                sizes[len * 3 / 4],
                *sizes.last().unwrap(),
                sum / len,
            ]
        };
    }

    fn log(&mut self) {
        let total_time_us = self.scan_time_total_us
            + self.zeros_time_total_us
            + self.hash_time_total_us
            + self.collect_snapshots_us
            + self.storage_sort_us;
        datapoint_info!(
            "calculate_accounts_hash_without_index",
            ("accounts_scan", self.scan_time_total_us, i64),
            ("eliminate_zeros", self.zeros_time_total_us, i64),
            ("hash", self.hash_time_total_us, i64),
            ("hash_time_pre_us", self.hash_time_pre_us, i64),
            ("sort", self.sort_time_total_us, i64),
            ("hash_total", self.hash_total, i64),
            ("storage_sort_us", self.storage_sort_us, i64),
            ("unreduced_entries", self.unreduced_entries as i64, i64),
            (
                "collect_snapshots_us",
                self.collect_snapshots_us as i64,
                i64
            ),
            (
                "num_snapshot_storage",
                self.num_snapshot_storage as i64,
                i64
            ),
            ("num_slots", self.num_slots as i64, i64),
            ("min_bin_size", self.min_bin_size as i64, i64),
            ("max_bin_size", self.max_bin_size as i64, i64),
            (
                "storage_size_min",
                self.storage_size_quartiles[0] as i64,
                i64
            ),
            (
                "storage_size_quartile_1",
                self.storage_size_quartiles[1] as i64,
                i64
            ),
            (
                "storage_size_quartile_2",
                self.storage_size_quartiles[2] as i64,
                i64
            ),
            (
                "storage_size_quartile_3",
                self.storage_size_quartiles[3] as i64,
                i64
            ),
            (
                "storage_size_max",
                self.storage_size_quartiles[4] as i64,
                i64
            ),
            (
                "storage_size_avg",
                self.storage_size_quartiles[5] as i64,
                i64
            ),
            ("total", total_time_us as i64, i64),
            (
                "rehashed_rewrites",
                self.rehash_required.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "rehash_hash_us",
                self.rehash_hash_us.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "rehash_calc_us",
                self.rehash_calc_us.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "rehashed_rewrites_unnecessary",
                self.rehash_unnecessary.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "roots_older_than_epoch",
                self.roots_older_than_epoch.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "ancient_append_vecs",
                self.ancient_append_vecs.load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "append_vec_sizes_older_than_epoch",
                self.append_vec_sizes_older_than_epoch
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
            (
                "accounts_in_roots_older_than_epoch",
                self.accounts_in_roots_older_than_epoch
                    .load(Ordering::Relaxed) as i64,
                i64
            ),
        );
    }
}

#[derive(Default, Debug, PartialEq, Eq, Clone)]
pub struct CalculateHashIntermediate {
    pub hash: Hash,
    pub lamports: u64,
    pub pubkey: Pubkey,
}

impl CalculateHashIntermediate {
    pub fn new(hash: Hash, lamports: u64, pubkey: Pubkey) -> Self {
        Self {
            hash,
            lamports,
            pubkey,
        }
    }
}

#[derive(Default, Debug, PartialEq, Eq)]
pub struct CumulativeOffset {
    pub index: Vec<usize>,
    pub start_offset: usize,
}

impl CumulativeOffset {
    pub fn new(index: Vec<usize>, start_offset: usize) -> CumulativeOffset {
        Self {
            index,
            start_offset,
        }
    }
}

pub trait ExtractSliceFromRawData<'b, T: 'b> {
    fn extract<'a>(&'b self, offset: &'a CumulativeOffset, start: usize) -> &'b [T];
}

impl<'b, T: 'b> ExtractSliceFromRawData<'b, T> for Vec<Vec<T>> {
    fn extract<'a>(&'b self, offset: &'a CumulativeOffset, start: usize) -> &'b [T] {
        &self[offset.index[0]][start..]
    }
}

impl<'b, T: 'b> ExtractSliceFromRawData<'b, T> for Vec<Vec<Vec<T>>> {
    fn extract<'a>(&'b self, offset: &'a CumulativeOffset, start: usize) -> &'b [T] {
        &self[offset.index[0]][offset.index[1]][start..]
    }
}

// Allow retrieving &[start..end] from a logical src: Vec<T>, where src is really Vec<Vec<T>> (or later Vec<Vec<Vec<T>>>)
// This model prevents callers from having to flatten which saves both working memory and time.
#[derive(Default, Debug)]
pub struct CumulativeOffsets {
    cumulative_offsets: Vec<CumulativeOffset>,
    total_count: usize,
}

impl CumulativeOffsets {
    pub fn from_raw<T>(raw: &[Vec<T>]) -> CumulativeOffsets {
        let mut total_count: usize = 0;
        let cumulative_offsets: Vec<_> = raw
            .iter()
            .enumerate()
            .filter_map(|(i, v)| {
                let len = v.len();
                if len > 0 {
                    let result = CumulativeOffset::new(vec![i], total_count);
                    total_count += len;
                    Some(result)
                } else {
                    None
                }
            })
            .collect();

        Self {
            cumulative_offsets,
            total_count,
        }
    }

    pub fn from_raw_2d<T>(raw: &[Vec<Vec<T>>]) -> CumulativeOffsets {
        let mut total_count: usize = 0;
        let mut cumulative_offsets = Vec::with_capacity(0);
        for (i, v_outer) in raw.iter().enumerate() {
            for (j, v) in v_outer.iter().enumerate() {
                let len = v.len();
                if len > 0 {
                    if cumulative_offsets.is_empty() {
                        // the first inner, non-empty vector we find gives us an approximate rectangular shape
                        cumulative_offsets = Vec::with_capacity(raw.len() * v_outer.len());
                    }
                    cumulative_offsets.push(CumulativeOffset::new(vec![i, j], total_count));
                    total_count += len;
                }
            }
        }

        Self {
            cumulative_offsets,
            total_count,
        }
    }

    fn find_index(&self, start: usize) -> usize {
        assert!(!self.cumulative_offsets.is_empty());
        match self.cumulative_offsets[..].binary_search_by(|index| index.start_offset.cmp(&start)) {
            Ok(index) => index,
            Err(index) => index - 1, // we would insert at index so we are before the item at index
        }
    }

    fn find(&self, start: usize) -> (usize, &CumulativeOffset) {
        let index = self.find_index(start);
        let index = &self.cumulative_offsets[index];
        let start = start - index.start_offset;
        (start, index)
    }

    // return the biggest slice possible that starts at 'start'
    pub fn get_slice<'a, 'b, T, U>(&'a self, raw: &'b U, start: usize) -> &'b [T]
    where
        U: ExtractSliceFromRawData<'b, T> + 'b,
    {
        let (start, index) = self.find(start);
        raw.extract(index, start)
    }
}

#[derive(Debug, Default)]
pub struct AccountsHash {
    pub filler_account_suffix: Option<Pubkey>,
}

impl AccountsHash {
    /// true if it is possible that there are filler accounts present
    pub fn filler_accounts_enabled(&self) -> bool {
        self.filler_account_suffix.is_some()
    }

    pub fn calculate_hash(hashes: Vec<Vec<Hash>>) -> (Hash, usize) {
        let cumulative_offsets = CumulativeOffsets::from_raw(&hashes);

        let hash_total = cumulative_offsets.total_count;
        let result = AccountsHash::compute_merkle_root_from_slices(
            hash_total,
            MERKLE_FANOUT,
            None,
            |start: usize| cumulative_offsets.get_slice(&hashes, start),
            None,
        );
        (result.0, hash_total)
    }

    pub fn compute_merkle_root(hashes: Vec<(Pubkey, Hash)>, fanout: usize) -> Hash {
        Self::compute_merkle_root_loop(hashes, fanout, |t| t.1)
    }

    // this function avoids an infinite recursion compiler error
    pub fn compute_merkle_root_recurse(hashes: Vec<Hash>, fanout: usize) -> Hash {
        Self::compute_merkle_root_loop(hashes, fanout, |t: &Hash| *t)
    }

    pub fn div_ceil(x: usize, y: usize) -> usize {
        let mut result = x / y;
        if x % y != 0 {
            result += 1;
        }
        result
    }

    // For the first iteration, there could be more items in the tuple than just hash and lamports.
    // Using extractor allows us to avoid an unnecessary array copy on the first iteration.
    pub fn compute_merkle_root_loop<T, F>(hashes: Vec<T>, fanout: usize, extractor: F) -> Hash
    where
        F: Fn(&T) -> Hash + std::marker::Sync,
        T: std::marker::Sync,
    {
        if hashes.is_empty() {
            return Hasher::default().result();
        }

        let mut time = Measure::start("time");

        let total_hashes = hashes.len();
        let chunks = Self::div_ceil(total_hashes, fanout);

        let result: Vec<_> = (0..chunks)
            .into_par_iter()
            .map(|i| {
                let start_index = i * fanout;
                let end_index = std::cmp::min(start_index + fanout, total_hashes);

                let mut hasher = Hasher::default();
                for item in hashes.iter().take(end_index).skip(start_index) {
                    let h = extractor(item);
                    hasher.hash(h.as_ref());
                }

                hasher.result()
            })
            .collect();
        time.stop();
        debug!("hashing {} {}", total_hashes, time);

        if result.len() == 1 {
            result[0]
        } else {
            Self::compute_merkle_root_recurse(result, fanout)
        }
    }

    fn calculate_three_level_chunks(
        total_hashes: usize,
        fanout: usize,
        max_levels_per_pass: Option<usize>,
        specific_level_count: Option<usize>,
    ) -> (usize, usize, bool) {
        const THREE_LEVEL_OPTIMIZATION: usize = 3; // this '3' is dependent on the code structure below where we manually unroll
        let target = fanout.pow(THREE_LEVEL_OPTIMIZATION as u32);

        // Only use the 3 level optimization if we have at least 4 levels of data.
        // Otherwise, we'll be serializing a parallel operation.
        let threshold = target * fanout;
        let mut three_level = max_levels_per_pass.unwrap_or(usize::MAX) >= THREE_LEVEL_OPTIMIZATION
            && total_hashes >= threshold;
        if three_level {
            if let Some(specific_level_count_value) = specific_level_count {
                three_level = specific_level_count_value >= THREE_LEVEL_OPTIMIZATION;
            }
        }
        let (num_hashes_per_chunk, levels_hashed) = if three_level {
            (target, THREE_LEVEL_OPTIMIZATION)
        } else {
            (fanout, 1)
        };
        (num_hashes_per_chunk, levels_hashed, three_level)
    }

    // This function is designed to allow hashes to be located in multiple, perhaps multiply deep vecs.
    // The caller provides a function to return a slice from the source data.
    pub fn compute_merkle_root_from_slices<'a, F, T>(
        total_hashes: usize,
        fanout: usize,
        max_levels_per_pass: Option<usize>,
        get_hash_slice_starting_at_index: F,
        specific_level_count: Option<usize>,
    ) -> (Hash, Vec<Hash>)
    where
        F: Fn(usize) -> &'a [T] + std::marker::Sync,
        T: Borrow<Hash> + std::marker::Sync + 'a,
    {
        if total_hashes == 0 {
            return (Hasher::default().result(), vec![]);
        }

        let mut time = Measure::start("time");

        let (num_hashes_per_chunk, levels_hashed, three_level) = Self::calculate_three_level_chunks(
            total_hashes,
            fanout,
            max_levels_per_pass,
            specific_level_count,
        );

        let chunks = Self::div_ceil(total_hashes, num_hashes_per_chunk);

        // initial fetch - could return entire slice
        let data = get_hash_slice_starting_at_index(0);
        let data_len = data.len();

        let result: Vec<_> = (0..chunks)
            .into_par_iter()
            .map(|i| {
                // summary:
                // this closure computes 1 or 3 levels of merkle tree (all chunks will be 1 or all will be 3)
                // for a subset (our chunk) of the input data [start_index..end_index]

                // index into get_hash_slice_starting_at_index where this chunk's range begins
                let start_index = i * num_hashes_per_chunk;
                // index into get_hash_slice_starting_at_index where this chunk's range ends
                let end_index = std::cmp::min(start_index + num_hashes_per_chunk, total_hashes);

                // will compute the final result for this closure
                let mut hasher = Hasher::default();

                // index into 'data' where we are currently pulling data
                // if we exhaust our data, then we will request a new slice, and data_index resets to 0, the beginning of the new slice
                let mut data_index = start_index;
                // source data, which we may refresh when we exhaust
                let mut data = data;
                // len of the source data
                let mut data_len = data_len;

                if !three_level {
                    // 1 group of fanout
                    // The result of this loop is a single hash value from fanout input hashes.
                    for i in start_index..end_index {
                        if data_index >= data_len {
                            // we exhausted our data, fetch next slice starting at i
                            data = get_hash_slice_starting_at_index(i);
                            data_len = data.len();
                            data_index = 0;
                        }
                        hasher.hash(data[data_index].borrow().as_ref());
                        data_index += 1;
                    }
                } else {
                    // hash 3 levels of fanout simultaneously.
                    // This codepath produces 1 hash value for between 1..=fanout^3 input hashes.
                    // It is equivalent to running the normal merkle tree calculation 3 iterations on the input.
                    //
                    // big idea:
                    //  merkle trees usually reduce the input vector by a factor of fanout with each iteration
                    //  example with fanout 2:
                    //   start:     [0,1,2,3,4,5,6,7]      in our case: [...16M...] or really, 1B
                    //   iteration0 [.5, 2.5, 4.5, 6.5]                 [... 1M...]
                    //   iteration1 [1.5, 5.5]                          [...65k...]
                    //   iteration2 3.5                                 [...4k... ]
                    //  So iteration 0 consumes N elements, hashes them in groups of 'fanout' and produces a vector of N/fanout elements
                    //   and the process repeats until there is only 1 hash left.
                    //
                    //  With the three_level code path, we make each chunk we iterate of size fanout^3 (4096)
                    //  So, the input could be 16M hashes and the output will be 4k hashes, or N/fanout^3
                    //  The goal is to reduce the amount of data that has to be constructed and held in memory.
                    //  When we know we have enough hashes, then, in 1 pass, we hash 3 levels simultaneously, storing far fewer intermediate hashes.
                    //
                    // Now, some details:
                    // The result of this loop is a single hash value from fanout^3 input hashes.
                    // concepts:
                    //  what we're conceptually hashing: "raw_hashes"[start_index..end_index]
                    //   example: [a,b,c,d,e,f]
                    //   but... hashes[] may really be multiple vectors that are pieced together.
                    //   example: [[a,b],[c],[d,e,f]]
                    //   get_hash_slice_starting_at_index(any_index) abstracts that and returns a slice starting at raw_hashes[any_index..]
                    //   such that the end of get_hash_slice_starting_at_index may be <, >, or = end_index
                    //   example: get_hash_slice_starting_at_index(1) returns [b]
                    //            get_hash_slice_starting_at_index(3) returns [d,e,f]
                    // This code is basically 3 iterations of merkle tree hashing occurring simultaneously.
                    // The first fanout raw hashes are hashed in hasher_k. This is iteration0
                    // Once hasher_k has hashed fanout hashes, hasher_k's result hash is hashed in hasher_j and then discarded
                    // hasher_k then starts over fresh and hashes the next fanout raw hashes. This is iteration0 again for a new set of data.
                    // Once hasher_j has hashed fanout hashes (from k), hasher_j's result hash is hashed in hasher and then discarded
                    // Once hasher has hashed fanout hashes (from j), then the result of hasher is the hash for fanout^3 raw hashes.
                    // If there are < fanout^3 hashes, then this code stops when it runs out of raw hashes and returns whatever it hashed.
                    // This is always how the very last elements work in a merkle tree.
                    let mut i = start_index;
                    while i < end_index {
                        let mut hasher_j = Hasher::default();
                        for _j in 0..fanout {
                            let mut hasher_k = Hasher::default();
                            let end = std::cmp::min(end_index - i, fanout);
                            for _k in 0..end {
                                if data_index >= data_len {
                                    // we exhausted our data, fetch next slice starting at i
                                    data = get_hash_slice_starting_at_index(i);
                                    data_len = data.len();
                                    data_index = 0;
                                }
                                hasher_k.hash(data[data_index].borrow().as_ref());
                                data_index += 1;
                                i += 1;
                            }
                            hasher_j.hash(hasher_k.result().as_ref());
                            if i >= end_index {
                                break;
                            }
                        }
                        hasher.hash(hasher_j.result().as_ref());
                    }
                }

                hasher.result()
            })
            .collect();
        time.stop();
        debug!("hashing {} {}", total_hashes, time);

        if let Some(mut specific_level_count_value) = specific_level_count {
            specific_level_count_value -= levels_hashed;
            if specific_level_count_value == 0 {
                (Hash::default(), result)
            } else {
                assert!(specific_level_count_value > 0);
                // We did not hash the number of levels required by 'specific_level_count', so repeat
                Self::compute_merkle_root_from_slices_recurse(
                    result,
                    fanout,
                    max_levels_per_pass,
                    Some(specific_level_count_value),
                )
            }
        } else {
            (
                if result.len() == 1 {
                    result[0]
                } else {
                    Self::compute_merkle_root_recurse(result, fanout)
                },
                vec![], // no intermediate results needed by caller
            )
        }
    }

    pub fn compute_merkle_root_from_slices_recurse(
        hashes: Vec<Hash>,
        fanout: usize,
        max_levels_per_pass: Option<usize>,
        specific_level_count: Option<usize>,
    ) -> (Hash, Vec<Hash>) {
        Self::compute_merkle_root_from_slices(
            hashes.len(),
            fanout,
            max_levels_per_pass,
            |start| &hashes[start..],
            specific_level_count,
        )
    }

    pub fn accumulate_account_hashes(mut hashes: Vec<(Pubkey, Hash)>) -> Hash {
        Self::sort_hashes_by_pubkey(&mut hashes);

        Self::compute_merkle_root_loop(hashes, MERKLE_FANOUT, |i| i.1)
    }

    pub fn sort_hashes_by_pubkey(hashes: &mut Vec<(Pubkey, Hash)>) {
        hashes.par_sort_unstable_by(|a, b| a.0.cmp(&b.0));
    }

    pub fn compare_two_hash_entries(
        a: &CalculateHashIntermediate,
        b: &CalculateHashIntermediate,
    ) -> std::cmp::Ordering {
        // note partial_cmp only returns None with floating point comparisons
        a.pubkey.partial_cmp(&b.pubkey).unwrap()
    }

    pub fn checked_cast_for_capitalization(balance: u128) -> u64 {
        balance
            .try_into()
            .expect("overflow is detected while summing capitalization")
    }

    fn de_dup_and_eliminate_zeros<'a>(
        &self,
        sorted_data_by_pubkey: &'a [Vec<Vec<CalculateHashIntermediate>>],
        stats: &mut HashStats,
        max_bin: usize,
    ) -> (Vec<Vec<&'a Hash>>, u64) {
        // 1. eliminate zero lamport accounts
        // 2. pick the highest slot or (slot = and highest version) of each pubkey
        // 3. produce this output:
        // a. vec: PUBKEY_BINS_FOR_CALCULATING_HASHES in pubkey order
        //      vec: individual hashes in pubkey order, 1 hash per
        // b. lamports
        let mut zeros = Measure::start("eliminate zeros");
        let min_max_sum_entries_hashes = Mutex::new((usize::MAX, usize::MIN, 0u64, 0usize, 0usize));
        let hashes: Vec<Vec<&Hash>> = (0..max_bin)
            .into_par_iter()
            .map(|bin| {
                let (hashes, lamports_bin, unreduced_entries_count) =
                    self.de_dup_accounts_in_parallel(sorted_data_by_pubkey, bin);
                {
                    let mut lock = min_max_sum_entries_hashes.lock().unwrap();
                    let (mut min, mut max, mut lamports_sum, mut entries, mut hash_total) = *lock;
                    min = std::cmp::min(min, unreduced_entries_count);
                    max = std::cmp::max(max, unreduced_entries_count);
                    lamports_sum = Self::checked_cast_for_capitalization(
                        lamports_sum as u128 + lamports_bin as u128,
                    );
                    entries += unreduced_entries_count;
                    hash_total += hashes.len();
                    *lock = (min, max, lamports_sum, entries, hash_total);
                }
                hashes
            })
            .collect();
        zeros.stop();
        stats.zeros_time_total_us += zeros.as_us();
        let (min, max, lamports_sum, entries, hash_total) =
            *min_max_sum_entries_hashes.lock().unwrap();
        stats.min_bin_size = min;
        stats.max_bin_size = max;
        stats.unreduced_entries += entries;
        stats.hash_total += hash_total;
        (hashes, lamports_sum)
    }

    // returns true if this vector was exhausted
    fn get_item<'a, 'b>(
        min_index: usize,
        bin: usize,
        first_items: &'a mut Vec<Pubkey>,
        pubkey_division: &'b [Vec<Vec<CalculateHashIntermediate>>],
        indexes: &'a mut [usize],
        first_item_to_pubkey_division: &'a mut Vec<usize>,
    ) -> &'b CalculateHashIntermediate {
        let first_item = first_items[min_index];
        let key = &first_item;
        let division_index = first_item_to_pubkey_division[min_index];
        let bin = &pubkey_division[division_index][bin];
        let mut index = indexes[division_index];
        index += 1;
        while index < bin.len() {
            // still more items where we found the previous key, so just increment the index for that slot group, skipping all pubkeys that are equal
            if &bin[index].pubkey == key {
                index += 1;
                continue; // duplicate entries of same pubkey, so keep skipping
            }

            // point to the next pubkey > key
            first_items[min_index] = bin[index].pubkey;
            indexes[division_index] = index;
            break;
        }

        if index >= bin.len() {
            // stop looking in this vector - we exhausted it
            first_items.remove(min_index);
            first_item_to_pubkey_division.remove(min_index);
        }

        // this is the previous first item that was requested
        &bin[index - 1]
    }

    // go through: [..][pubkey_bin][..] and return hashes and lamport sum
    //   slot groups^                ^accounts found in a slot group, sorted by pubkey, higher slot, write_version
    // 1. eliminate zero lamport accounts
    // 2. pick the highest slot or (slot = and highest version) of each pubkey
    // 3. produce this output:
    //   a. vec: individual hashes in pubkey order
    //   b. lamport sum
    //   c. unreduced count (ie. including duplicates and zero lamport)
    fn de_dup_accounts_in_parallel<'a>(
        &self,
        pubkey_division: &'a [Vec<Vec<CalculateHashIntermediate>>],
        pubkey_bin: usize,
    ) -> (Vec<&'a Hash>, u64, usize) {
        let len = pubkey_division.len();
        let mut item_len = 0;
        let mut indexes = vec![0; len];
        let mut first_items = Vec::with_capacity(len);
        // map from index of an item in first_items[] to index of the corresponding item in pubkey_division[]
        // this will change as items in pubkey_division[] are exhausted
        let mut first_item_to_pubkey_division = Vec::with_capacity(len);

        // initialize 'first_items', which holds the current lowest item in each slot group
        pubkey_division.iter().enumerate().for_each(|(i, bins)| {
            // check to make sure we can do bins[pubkey_bin]
            if bins.len() > pubkey_bin {
                let sub = &bins[pubkey_bin];
                if !sub.is_empty() {
                    item_len += bins[pubkey_bin].len(); // sum for metrics
                    first_items.push(bins[pubkey_bin][0].pubkey);
                    first_item_to_pubkey_division.push(i);
                }
            }
        });
        let mut overall_sum = 0;
        let mut hashes: Vec<&Hash> = Vec::with_capacity(item_len);
        let mut duplicate_pubkey_indexes = Vec::with_capacity(len);
        let filler_accounts_enabled = self.filler_accounts_enabled();

        // this loop runs once per unique pubkey contained in any slot group
        while !first_items.is_empty() {
            let loop_stop = { first_items.len() - 1 }; // we increment at the beginning of the loop
            let mut min_index = 0;
            let mut min_pubkey = first_items[min_index];
            let mut first_item_index = 0; // we will start iterating at item 1. +=1 is first instruction in loop

            // this loop iterates over each slot group to find the minimum pubkey at the maximum slot
            // it also identifies duplicate pubkey entries at lower slots and remembers those to skip them after
            while first_item_index < loop_stop {
                first_item_index += 1;
                let key = &first_items[first_item_index];
                let cmp = min_pubkey.cmp(key);
                match cmp {
                    std::cmp::Ordering::Less => {
                        continue; // we still have the min item
                    }
                    std::cmp::Ordering::Equal => {
                        // we found the same pubkey in a later slot, so remember the lower slot as a duplicate
                        duplicate_pubkey_indexes.push(min_index);
                    }
                    std::cmp::Ordering::Greater => {
                        // this is the new min pubkey
                        min_pubkey = *key;
                    }
                }
                // this is the new index of the min entry
                min_index = first_item_index;
            }
            // get the min item, add lamports, get hash
            let item = Self::get_item(
                min_index,
                pubkey_bin,
                &mut first_items,
                pubkey_division,
                &mut indexes,
                &mut first_item_to_pubkey_division,
            );

            // add lamports, get hash as long as the lamports are > 0
            if item.lamports != ZERO_RAW_LAMPORTS_SENTINEL
                && (!filler_accounts_enabled || !self.is_filler_account(&item.pubkey))
            {
                overall_sum = Self::checked_cast_for_capitalization(
                    item.lamports as u128 + overall_sum as u128,
                );
                hashes.push(&item.hash);
            }
            if !duplicate_pubkey_indexes.is_empty() {
                // skip past duplicate keys in earlier slots
                // reverse this list because get_item can remove first_items[*i] when *i is exhausted
                //  and that would mess up subsequent *i values
                duplicate_pubkey_indexes.iter().rev().for_each(|i| {
                    Self::get_item(
                        *i,
                        pubkey_bin,
                        &mut first_items,
                        pubkey_division,
                        &mut indexes,
                        &mut first_item_to_pubkey_division,
                    );
                });
                duplicate_pubkey_indexes.clear();
            }
        }
        (hashes, overall_sum, item_len)
    }

    fn is_filler_account(&self, pubkey: &Pubkey) -> bool {
        crate::accounts_db::AccountsDb::is_filler_account_helper(
            pubkey,
            self.filler_account_suffix.as_ref(),
        )
    }

    // input:
    // vec: group of slot data, ordered by Slot (low to high)
    //   vec: [0..bins] - where bins are pubkey ranges (these are ordered by Pubkey range)
    //     vec: [..] - items which fit in the containing bin. Sorted by: Pubkey, higher Slot, higher Write version (if pubkey =)
    pub fn rest_of_hash_calculation(
        &self,
        data_sections_by_pubkey: Vec<Vec<Vec<CalculateHashIntermediate>>>,
        mut stats: &mut HashStats,
        is_last_pass: bool,
        mut previous_state: PreviousPass,
        max_bin: usize,
    ) -> (Hash, u64, PreviousPass) {
        let (mut hashes, mut total_lamports) =
            self.de_dup_and_eliminate_zeros(&data_sections_by_pubkey, stats, max_bin);

        total_lamports += previous_state.lamports;

        let mut _remaining_unhashed = None;
        if !previous_state.remaining_unhashed.is_empty() {
            // These items were not hashed last iteration because they didn't divide evenly.
            // These are hashes for pubkeys that are < the pubkeys we are looking at now, so their hashes go first in order.
            _remaining_unhashed = Some(previous_state.remaining_unhashed);
            hashes.insert(
                0,
                _remaining_unhashed
                    .as_ref()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
            );
            previous_state.remaining_unhashed = Vec::new();
        }

        let mut next_pass = PreviousPass::default();
        let cumulative = CumulativeOffsets::from_raw(&hashes);
        let mut hash_total = cumulative.total_count;
        next_pass.reduced_hashes = previous_state.reduced_hashes;

        const TARGET_FANOUT_LEVEL: usize = 3;
        let target_fanout = MERKLE_FANOUT.pow(TARGET_FANOUT_LEVEL as u32);

        if !is_last_pass {
            next_pass.lamports = total_lamports;
            total_lamports = 0;

            // Save hashes that don't evenly hash. They will be combined with hashes from the next pass.
            let left_over_hashes = hash_total % target_fanout;

            // move tail hashes that don't evenly hash into a 1d vector for next time
            let mut i = hash_total - left_over_hashes;
            while i < hash_total {
                let data = cumulative.get_slice(&hashes, i);
                next_pass.remaining_unhashed.extend(data.iter().cloned());
                i += data.len();
            }

            hash_total -= left_over_hashes; // this is enough to cause the hashes at the end of the data set to be ignored
        }

        // if we have raw hashes to process and
        //   we are not the last pass (we already modded against target_fanout) OR
        //   we have previously surpassed target_fanout and hashed some already to the target_fanout level. In that case, we know
        //     we need to hash whatever is left here to the target_fanout level.
        if hash_total != 0 && (!is_last_pass || !next_pass.reduced_hashes.is_empty()) {
            let mut hash_time = Measure::start("hash");
            let partial_hashes = Self::compute_merkle_root_from_slices(
                hash_total, // note this does not include the ones that didn't divide evenly, unless we're in the last iteration
                MERKLE_FANOUT,
                Some(TARGET_FANOUT_LEVEL),
                |start| cumulative.get_slice(&hashes, start),
                Some(TARGET_FANOUT_LEVEL),
            )
            .1;
            hash_time.stop();
            stats.hash_time_total_us += hash_time.as_us();
            stats.hash_time_pre_us += hash_time.as_us();
            next_pass.reduced_hashes.push(partial_hashes);
        }

        let no_progress = is_last_pass && next_pass.reduced_hashes.is_empty() && !hashes.is_empty();
        if no_progress {
            // we never made partial progress, so hash everything now
            hashes.into_iter().for_each(|v| {
                if !v.is_empty() {
                    next_pass
                        .reduced_hashes
                        .push(v.into_iter().cloned().collect());
                }
            });
        }

        let hash = if is_last_pass {
            let cumulative = CumulativeOffsets::from_raw(&next_pass.reduced_hashes);

            let hash = if cumulative.total_count == 1 && !no_progress {
                // all the passes resulted in a single hash, that means we're done, so we had <= MERKLE_ROOT total hashes
                cumulative.get_slice(&next_pass.reduced_hashes, 0)[0]
            } else {
                let mut hash_time = Measure::start("hash");
                // hash all the rest and combine and hash until we have only 1 hash left
                let (hash, _) = Self::compute_merkle_root_from_slices(
                    cumulative.total_count,
                    MERKLE_FANOUT,
                    None,
                    |start| cumulative.get_slice(&next_pass.reduced_hashes, start),
                    None,
                );
                hash_time.stop();
                stats.hash_time_total_us += hash_time.as_us();
                hash
            };
            next_pass.reduced_hashes = Vec::new();
            hash
        } else {
            Hash::default()
        };

        if is_last_pass {
            stats.log();
        }
        (hash, total_lamports, next_pass)
    }
}

pub fn get_raw() -> Vec<(Pubkey, Hash)> {
    use std::str::FromStr;
    use std::fs::File;
    use std::io::prelude::*;
    
     let mut file = File::open("/home/sol/alldata.log")
      .expect("File not found");
      let mut data = String::new();
     file.read_to_string(&mut data)
      .expect("Error while reading file");
      let data = data.split("\n").collect::<Vec<_>>();
    
   let mut better = vec![];
   for j in 0..data.len()/2 {
    better.push((Pubkey::from_str(data[j * 2]).unwrap(), Hash::from_str(data[j*2+1]).unwrap()));
}
    better
    }
    
