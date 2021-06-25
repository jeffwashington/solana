#![feature(test)]

extern crate test;
use solana_runtime::accounts_db::AccountInfo;
use solana_runtime::bucket_map::BucketMap;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use test::Bencher;

#[bench]
#[ignore]
fn bench_bucket_map_insert(bencher: &mut Bencher) {
    let tmpdir = std::env::temp_dir();
    let drives = Arc::new(vec![tmpdir]);
    let index = BucketMap::new(8, drives);
    bencher.iter(|| {
        let key = Pubkey::new_unique();
        index.update(&key, |_| Some(vec![0, AccountsInfo::default()]));
    });
}
