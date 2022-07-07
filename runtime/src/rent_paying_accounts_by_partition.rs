//! Quick lookup of all pubkeys which could be rent paying, grouped by rent collection partition
use {
    crate::bank::{Bank, PartitionIndex, PartitionsPerCycle},
    solana_sdk::{epoch_schedule::EpochSchedule, pubkey::Pubkey},
};

/// populated at startup with the accounts that were found that are rent paying.
/// These are the 'possible' rent paying accounts.
/// This set can never grow during runtime since it is not possible to create rent paying accounts now.
/// It can shrink during execution if a rent paying account is dropped to lamports=0 or is topped off.
/// The next time the validator restarts, it will remove the account from this list.
#[derive(Debug, Default)]
pub struct RentPayingAccountsByPartition {
    /// 1st index is partition, 0..=432_000
    /// 2nd dimension is list of pubkeys which were identified at startup to be rent paying
    pub accounts: Vec<Vec<Pubkey>>,
    partition_count: PartitionsPerCycle,
}

impl RentPayingAccountsByPartition {
    pub fn new(epoch_schedule: &EpochSchedule) -> Self {
        let partition_count = epoch_schedule.slots_per_epoch;
        Self {
            partition_count,
            accounts: (0..=partition_count).into_iter().map(|_| vec![]).collect(),
        }
    }
    pub fn add_account(&mut self, pubkey: &Pubkey) {
        let partition_index = Bank::partition_from_pubkey(pubkey, self.partition_count);
        let list = &mut self.accounts[partition_index as usize];

        if !list.contains(pubkey) {
            list.push(*pubkey);
        }
    }
    pub fn get_pubkeys_in_partition_index(&self, partition_index: PartitionIndex) -> &Vec<Pubkey> {
        &self.accounts[partition_index as usize]
    }
    pub fn is_initialized(&self) -> bool {
        self.partition_count != 0
    }
}
