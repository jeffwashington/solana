#![feature(test)]
use log::*;
use solana_measure::measure::Measure;
macro_rules! DEFINE_NxM_BENCH {
    ($i:ident, $n:literal, $m:literal, $s:literal) => {
        mod $i {
            use super::*;

            #[bench]
            fn bench_insert_baseline_hashmap(bencher: &mut Bencher) {
                do_bench_insert_baseline_hashmap(bencher, $n, $m);
            }

            #[bench]
            fn bench_insert_bucket_map(bencher: &mut Bencher) {
                do_bench_insert_bucket_map(bencher, $n, $m);
            }

            #[bench]
            fn bench_insert_get_bucket_map(bencher: &mut Bencher) {
                do_bench_insert_get_bucket_map(bencher, $n, $m, $s);
            }
        }
    };
}

extern crate test;
use rayon::prelude::*;
use solana_bucket_map::bucket_map::{BucketMap, BucketMapConfig};
use solana_sdk::pubkey::Pubkey;
use std::collections::hash_map::HashMap;
use std::sync::RwLock;
use test::Bencher;

// matches AccountInfo
type IndexValue = (usize, usize, usize, u64);

DEFINE_NxM_BENCH!(dim_01x02, 1, 100_000, 32);
DEFINE_NxM_BENCH!(dim_01x04, 1, 1000_000, 32);
DEFINE_NxM_BENCH!(dim_01x08, 1, 1000_000, 16);
DEFINE_NxM_BENCH!(dim_01x08, 1, 1000_000, 64);
/*
DEFINE_NxM_BENCH!(dim_02x04, 2, 4);
DEFINE_NxM_BENCH!(dim_04x08, 4, 8);
DEFINE_NxM_BENCH!(dim_08x16, 8, 16);
DEFINE_NxM_BENCH!(dim_16x32, 16, 32);
DEFINE_NxM_BENCH!(dim_32x64, 32, 64);
*/

/// Benchmark insert with Hashmap as baseline for N threads inserting M keys each
fn do_bench_insert_baseline_hashmap(bencher: &mut Bencher, n: usize, m: usize) {
    let index = RwLock::new(HashMap::new());
    (0..n).into_iter().into_par_iter().for_each(|i| {
        let key = Pubkey::new_unique();
        index
            .write()
            .unwrap()
            .insert(key, vec![(i, IndexValue::default())]);
    });
    bencher.iter(|| {
        (0..n).into_iter().into_par_iter().for_each(|_| {
            for j in 0..m {
                let key = Pubkey::new_unique();
                index
                    .write()
                    .unwrap()
                    .insert(key, vec![(j, IndexValue::default())]);
            }
        })
    });
}

/// Benchmark insert with BucketMap with N buckets for N threads inserting M keys each
fn do_bench_insert_bucket_map(bencher: &mut Bencher, n: usize, m: usize) {
    let index = BucketMap::new(BucketMapConfig::new(n));
    (0..n).into_iter().into_par_iter().for_each(|i| {
        let key = Pubkey::new_unique();
        index.update(&key, |_| Some((vec![(i, IndexValue::default())], 0)));
    });
    bencher.iter(|| {
        (0..n).into_iter().into_par_iter().for_each(|_| {
            for j in 0..m {
                let key = Pubkey::new_unique();
                index.update(&key, |_| Some((vec![(j, IndexValue::default())], 0)));
            }
        })
    });
}

/// Benchmark insert with BucketMap with N buckets for N threads inserting M keys each
fn do_bench_insert_get_bucket_map(_bencher: &mut Bencher, n: usize, m: usize, max_search: u8) {
    solana_logger::setup();
    error!("insert get get_missing m {}, max_search {}", m, max_search);

    for _ in 0..10 {
        let mut config = BucketMapConfig::new(n);
        config.max_search = Some(max_search);
        let index = BucketMap::new(config);
        (0..n).into_iter().into_iter().for_each(|i| {
            let key = Pubkey::new_unique();
            index.update(&key, |_| Some((vec![(i, IndexValue::default())], 0)));
        });
        let mut keys = vec![];
        let mut m0 = Measure::start("");
        (0..n).into_iter().for_each(|_| {
            let keys2 = (0..m)
                .into_iter()
                .map(|j| {
                    let key = Pubkey::new_unique();
                    index.update(&key, |_| Some((vec![(j, IndexValue::default())], 0)));
                    key
                })
                .collect::<Vec<_>>();
            keys.push(keys2);
        });
        m0.stop();
        let mut mg = Measure::start("");
        keys.iter().for_each(|keys| {
            keys.iter().for_each(|key| {
                index.read_value(key);
            })
        });
        mg.stop();
        let mut mm = Measure::start("");
        keys.iter().for_each(|keys| {
            keys.iter().for_each(|key| {
                index.read_value(key);
            })
        });
        mm.stop();
        error!("{} {} {}", m0.as_us(), mg.as_us(), mm.as_us());
    }
}
