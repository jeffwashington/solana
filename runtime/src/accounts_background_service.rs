// Service to clean up dead slots in accounts_db
//
// This can be expensive since we have to walk the append vecs being cleaned up.

use crate::{
    accounts_db::{AccountsDB, AccountStorageEntry},
    bank::{Bank, BankSlotDelta, DropCallback},
    bank_forks::{BankForks, SnapshotConfig},
    snapshot_package::AccountsPackageSender,
    snapshot_utils,
};
use crossbeam_channel::{Receiver, SendError, Sender};
use log::*;
use rand::{thread_rng, Rng};
use rayon::prelude::*;
use solana_measure::measure::Measure;
use solana_sdk::clock::Slot;
use std::{
    boxed::Box,
    fmt::{Debug, Formatter},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{self, sleep, Builder, JoinHandle},
    time::Duration,
};

const INTERVAL_MS: u64 = 100;
const SHRUNKEN_ACCOUNT_PER_SEC: usize = 250;
const SHRUNKEN_ACCOUNT_PER_INTERVAL: usize =
    SHRUNKEN_ACCOUNT_PER_SEC / (1000 / INTERVAL_MS as usize);
const CLEAN_INTERVAL_BLOCKS: u64 = 100;

pub type SnapshotRequestSender = Sender<SnapshotRequest>;
pub type SnapshotRequestReceiver = Receiver<SnapshotRequest>;
pub type DroppedSlotsSender = Sender<Slot>;
pub type DroppedSlotsReceiver = Receiver<Slot>;

#[derive(Clone)]
pub struct SendDroppedBankCallback {
    sender: DroppedSlotsSender,
}

impl DropCallback for SendDroppedBankCallback {
    fn callback(&self, bank: &Bank) {
        if let Err(e) = self.sender.send(bank.slot()) {
            warn!("Error sending dropped banks: {:?}", e);
        }
    }

    fn clone_box(&self) -> Box<dyn DropCallback + Send + Sync> {
        Box::new(self.clone())
    }
}

impl Debug for SendDroppedBankCallback {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "SendDroppedBankCallback({:p})", self)
    }
}

impl SendDroppedBankCallback {
    pub fn new(sender: DroppedSlotsSender) -> Self {
        Self { sender }
    }
}

pub struct SnapshotRequest {
    pub snapshot_root_bank: Arc<Bank>,
    pub status_cache_slot_deltas: Vec<BankSlotDelta>,
}

pub struct SnapshotRequestHandler {
    pub snapshot_config: SnapshotConfig,
    pub snapshot_request_receiver: SnapshotRequestReceiver,
    pub accounts_package_sender: AccountsPackageSender,
}

impl SnapshotRequestHandler {
    // Returns the latest requested snapshot slot, if one exists
    pub fn handle_snapshot_requests(&self, accounts_db_caching_enabled: bool) -> Option<u64> {
        self.snapshot_request_receiver
            .try_iter()
            .last()
            .map(|snapshot_request| {
                let mut total_time = Measure::start("total");
                let SnapshotRequest {
                    snapshot_root_bank,
                    status_cache_slot_deltas,
                } = snapshot_request;

                let a1 = snapshot_root_bank.get_sorted_accounts();

                warn!("extra time before");
                //snapshot_root_bank.update_accounts_hash_with_store_option(true, true,false);
                warn!("extra time before done");

                let mut shrink_time = Measure::start("shrink_time");
                if !accounts_db_caching_enabled {
                    snapshot_root_bank
                        .process_stale_slot_with_budget(0, SHRUNKEN_ACCOUNT_PER_INTERVAL);
                }
                shrink_time.stop();

                let mut flush_accounts_cache_time = Measure::start("flush_accounts_cache_time");
                if accounts_db_caching_enabled {
                    // Force flush all the roots from the cache so that the snapshot can be taken.
                    snapshot_root_bank.force_flush_accounts_cache();
                }
                flush_accounts_cache_time.stop();

                let times: Vec<_> = (0..2u32).into_par_iter().map(|x| {
                    if x == 0 {
                        let mut clean_time = Measure::start("clean_time");
                        // Don't clean the slot we're snapshotting because it may have zero-lamport
                        // accounts that were included in the bank delta hash when the bank was frozen,
                        // and if we clean them here, the newly created snapshot's hash may not match
                        // the frozen hash.
                        log(&snapshot_root_bank, line!());
                            
        
                        snapshot_root_bank.clean_accounts(true);
                        log(&snapshot_root_bank, line!());
                            
        
                        clean_time.stop();
                        clean_time
                    }
                    else {
                        let mut hash_time = Measure::start("hash_time");
                        snapshot_root_bank.update_accounts_hash_with_store_option(true, true, false);
                        hash_time.stop();
                        hash_time
                    }
                }).collect();
                let clean_time = times[0].as_us();
                let hash_time = times[1].as_us();

                {
                    let a2 = AccountsDB::get_sorted_accounts_from_stores(snapshot_root_bank.get_snapshot_storages(), snapshot_root_bank.simple_capitalization_enabled());
                    warn!("jwash:Comparing in accounts_bg_service");
                    assert!(!AccountsDB::compare2(a1.clone(), a2));
                }


                if accounts_db_caching_enabled {
                    shrink_time = Measure::start("shrink_time");
                    snapshot_root_bank.shrink_candidate_slots();
                    shrink_time.stop();
                }

                log(&snapshot_root_bank, line!());

                //warn!("extra time before");
                //snapshot_root_bank.update_accounts_hash_with_store_option(true, false, true);
                //warn!("extra time before done");

                // Generate an accounts package
                let mut snapshot_time = Measure::start("snapshot_time");
                let r = snapshot_utils::snapshot_bank(
                    &snapshot_root_bank,
                    status_cache_slot_deltas,
                    &self.accounts_package_sender,
                    &self.snapshot_config.snapshot_path,
                    &self.snapshot_config.snapshot_package_output_path,
                    self.snapshot_config.snapshot_version,
                    &self.snapshot_config.archive_format,
                );
                if r.is_err() {
                    warn!(
                        "Error generating snapshot for bank: {}, err: {:?}",
                        snapshot_root_bank.slot(),
                        r
                    );
                }

                let r = r.unwrap().clone();

                snapshot_time.stop();

                warn!("extra time before");
                //snapshot_root_bank.update_accounts_hash_with_store_option(true, false, true);
                warn!("extra time before done");

                log(&snapshot_root_bank, line!());

                {
                    //let a2 = AccountsDB::get_sorted_accounts_from_stores(snapshot_root_bank.get_snapshot_storages(), snapshot_root_bank.simple_capitalization_enabled());
                    warn!("jwash:Comparing in accounts_bg_service3");
                    assert!(!AccountsDB::compare2(r.clone(), a1.clone()));
                }

                // Cleanup outdated snapshots
                let mut purge_old_snapshots_time = Measure::start("purge_old_snapshots_time");
                snapshot_utils::purge_old_snapshots(&self.snapshot_config.snapshot_path);
                purge_old_snapshots_time.stop();
                total_time.stop();

                if true
                {
                    let a2 = AccountsDB::get_sorted_accounts_from_stores(snapshot_root_bank.get_snapshot_storages(), snapshot_root_bank.simple_capitalization_enabled());
                    warn!("jwash:Comparing in accounts_bg_service4");
                    assert!(!AccountsDB::compare2(a1.clone(), a2));
                    warn!("jwash:Comparing in accounts_bg_service5");
                    assert!(!AccountsDB::compare2(r.clone(), a1.clone()));
                }

                log(&snapshot_root_bank, line!());

                warn!("extra time before");
                //snapshot_root_bank.update_accounts_hash_with_store_option(true, true, true);

                //snapshot_root_bank.update_accounts_hash_with_store_option(true, false, true);
                warn!("extra time before done - account cleanup service");

                datapoint_info!(
                    "handle_snapshot_requests-timing",
                    ("hash_time", hash_time, i64),
                    (
                        "flush_accounts_cache_time",
                        flush_accounts_cache_time.as_us(),
                        i64
                    ),
                    ("shrink_time", shrink_time.as_us(), i64),
                    ("clean_time", clean_time, i64),
                    ("snapshot_time", snapshot_time.as_us(), i64),
                    (
                        "purge_old_snapshots_time",
                        purge_old_snapshots_time.as_us(),
                        i64
                    ),
                    ("total_time", total_time.as_us(), i64),
                );
                snapshot_root_bank.block_height()
            })
    }
}

fn log(bank: &Arc<Bank>, line: u32){
        let storage_maps: Vec<Arc<AccountStorageEntry>> = bank.get_snapshot_storages()
        .into_iter()
        .flatten()
        .into_iter()
        .map(|x| x.clone())
        .collect();
let mut sum = 0;
    let mut size = 0;
    let len = storage_maps.len();
    for s in storage_maps {
        sum += s.accounts.accounts(0).len();
        size += s.accounts.file_size;
    }
    warn!("scan_account_storage_no_bank_2 after cleanup storages: {}, line: {}, sum: {}, size: {}", len, line, sum, size);

}

#[derive(Default)]
pub struct ABSRequestSender {
    snapshot_request_sender: Option<SnapshotRequestSender>,
}

impl ABSRequestSender {
    pub fn new(snapshot_request_sender: Option<SnapshotRequestSender>) -> Self {
        ABSRequestSender {
            snapshot_request_sender,
        }
    }

    pub fn is_snapshot_creation_enabled(&self) -> bool {
        self.snapshot_request_sender.is_some()
    }

    pub fn send_snapshot_request(
        &self,
        snapshot_request: SnapshotRequest,
    ) -> Result<(), SendError<SnapshotRequest>> {
        if let Some(ref snapshot_request_sender) = self.snapshot_request_sender {
            snapshot_request_sender.send(snapshot_request)
        } else {
            Ok(())
        }
    }
}

pub struct ABSRequestHandler {
    pub snapshot_request_handler: Option<SnapshotRequestHandler>,
    pub pruned_banks_receiver: DroppedSlotsReceiver,
}

impl ABSRequestHandler {
    // Returns the latest requested snapshot block height, if one exists
    pub fn handle_snapshot_requests(&self, accounts_db_caching_enabled: bool) -> Option<u64> {
        self.snapshot_request_handler
            .as_ref()
            .and_then(|snapshot_request_handler| {
                snapshot_request_handler.handle_snapshot_requests(accounts_db_caching_enabled)
            })
    }

    pub fn handle_pruned_banks(&self, bank: &Bank) -> usize {
        let mut count = 0;
        for pruned_slot in self.pruned_banks_receiver.try_iter() {
            count += 1;
            bank.rc.accounts.purge_slot(pruned_slot);
        }

        count
    }
}

pub struct AccountsBackgroundService {
    t_background: JoinHandle<()>,
}

impl AccountsBackgroundService {
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        exit: &Arc<AtomicBool>,
        request_handler: ABSRequestHandler,
        accounts_db_caching_enabled: bool,
    ) -> Self {
        info!("AccountsBackgroundService active");
        let exit = exit.clone();
        let mut consumed_budget = 0;
        let mut last_cleaned_block_height = 0;
        let mut removed_slots_count = 0;
        let mut total_remove_slots_time = 0;
        let t_background = Builder::new()
            .name("solana-accounts-background".to_string())
            .spawn(move || loop {
                if exit.load(Ordering::Relaxed) {
                    break;
                }

                // Grab the current root bank
                let bank = bank_forks.read().unwrap().root_bank().clone();

                // Purge accounts of any dead slots
                Self::remove_dead_slots(
                    &bank,
                    &request_handler,
                    &mut removed_slots_count,
                    &mut total_remove_slots_time,
                );

                // Check to see if there were any requests for snapshotting banks
                // < the current root bank `bank` above.

                // Claim: Any snapshot request for slot `N` found here implies that the last cleanup
                // slot `M` satisfies `M < N`
                //
                // Proof: Assume for contradiction that we find a snapshot request for slot `N` here,
                // but cleanup has already happened on some slot `M >= N`. Because the call to
                // `bank.clean_accounts(true)` (in the code below) implies we only clean slots `<= bank - 1`,
                // then that means in some *previous* iteration of this loop, we must have gotten a root
                // bank for slot some slot `R` where `R > N`, but did not see the snapshot for `N` in the
                // snapshot request channel.
                //
                // However, this is impossible because BankForks.set_root() will always flush the snapshot
                // request for `N` to the snapshot request channel before setting a root `R > N`, and
                // snapshot_request_handler.handle_requests() will always look for the latest
                // available snapshot in the channel.
                let snapshot_block_height =
                    request_handler.handle_snapshot_requests(accounts_db_caching_enabled);
                if accounts_db_caching_enabled {
                    bank.flush_accounts_cache_if_needed();
                }

                if let Some(snapshot_block_height) = snapshot_block_height {
                    // Safe, see proof above
                    assert!(last_cleaned_block_height <= snapshot_block_height);
                    last_cleaned_block_height = snapshot_block_height;
                } else {
                    if accounts_db_caching_enabled {
                        bank.shrink_candidate_slots();
                    } else {
                        // under sustained writes, shrink can lag behind so cap to
                        // SHRUNKEN_ACCOUNT_PER_INTERVAL (which is based on INTERVAL_MS,
                        // which in turn roughly asscociated block time)
                        consumed_budget = bank
                            .process_stale_slot_with_budget(
                                consumed_budget,
                                SHRUNKEN_ACCOUNT_PER_INTERVAL,
                            )
                            .min(SHRUNKEN_ACCOUNT_PER_INTERVAL);
                    }
                    if bank.block_height() - last_cleaned_block_height
                        > (CLEAN_INTERVAL_BLOCKS + thread_rng().gen_range(0, 10))
                    {
                        if accounts_db_caching_enabled {
                            bank.force_flush_accounts_cache();
                        }
                        bank.clean_accounts(true);
                        last_cleaned_block_height = bank.block_height();
                    }
                }
                sleep(Duration::from_millis(INTERVAL_MS));
            })
            .unwrap();
        Self { t_background }
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_background.join()
    }

    fn remove_dead_slots(
        bank: &Bank,
        request_handler: &ABSRequestHandler,
        removed_slots_count: &mut usize,
        total_remove_slots_time: &mut u64,
    ) {
        let mut remove_slots_time = Measure::start("remove_slots_time");
        *removed_slots_count += request_handler.handle_pruned_banks(&bank);
        remove_slots_time.stop();
        *total_remove_slots_time += remove_slots_time.as_us();

        if *removed_slots_count >= 100 {
            datapoint_info!(
                "remove_slots_timing",
                ("remove_slots_time", *total_remove_slots_time, i64),
                ("removed_slots_count", *removed_slots_count, i64),
            );
            *total_remove_slots_time = 0;
            *removed_slots_count = 0;
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::genesis_utils::create_genesis_config;
    use crossbeam_channel::unbounded;
    use solana_sdk::{account::Account, pubkey::Pubkey};

    #[test]
    fn test_accounts_background_service_remove_dead_slots() {
        let genesis = create_genesis_config(10);
        let bank0 = Arc::new(Bank::new(&genesis.genesis_config));
        let (pruned_banks_sender, pruned_banks_receiver) = unbounded();
        let request_handler = ABSRequestHandler {
            snapshot_request_handler: None,
            pruned_banks_receiver,
        };

        // Store an account in slot 0
        let account_key = Pubkey::new_unique();
        bank0.store_account(&account_key, &Account::new(264, 0, &Pubkey::default()));
        assert!(bank0.get_account(&account_key).is_some());
        pruned_banks_sender.send(0).unwrap();
        AccountsBackgroundService::remove_dead_slots(&bank0, &request_handler, &mut 0, &mut 0);

        // Slot should be removed
        assert!(bank0.get_account(&account_key).is_none());
    }
}
