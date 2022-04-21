//! mechanism for squashing append vecs into ancient append vecs

use {
    crate::{
        accounts_db::{
            AccountStorageEntry, AccountsDb, AppendVecId, FoundStoredAccount, SnapshotStorage,
        },
        accounts_index::AccountIndexGetResult,
        append_vec::{AppendVec, StoredAccountMeta},
    },
    solana_sdk::{clock::Slot, hash::Hash, pubkey::Pubkey},
    std::{collections::HashMap, sync::Arc},
};

pub enum StorageSelector {
    Primary,
    Overflow,
}

/// reference a set of accounts to store
pub struct AccountsToStore<'a> {
    hashes: Vec<&'a Hash>,
    accounts: Vec<(&'a Pubkey, &'a StoredAccountMeta<'a>, Slot)>,
    /// if 'accounts' contains more items than can be contained in the primary storage, then we have to split these accounts.
    /// 'index_first_item_overflow' specifies the index of the first item in 'accounts' that will go into the overflow storage
    index_first_item_overflow: usize,
}

impl<'a> AccountsToStore<'a> {
    /// available_bytes: how many bytes remain in the primary storage. Excess accounts will be directed to an overflow storage
    pub fn new(
        mut available_bytes: u64,
        stored_accounts: &'a HashMap<Pubkey, FoundStoredAccount>,
        slot: Slot,
    ) -> Self {
        let num_accounts = stored_accounts.len();
        let mut hashes = Vec::with_capacity(num_accounts);
        let mut accounts = Vec::with_capacity(num_accounts);
        // index of the first account that doesn't fit in the current append vec
        let mut index_first_item_overflow = num_accounts; // assume all fit
        stored_accounts.iter().for_each(|account| {
            let account_size = account.1.account_size as u64;
            if available_bytes >= account_size {
                available_bytes = available_bytes.saturating_sub(account_size);
            } else if index_first_item_overflow == num_accounts {
                available_bytes = 0;
                // the # of accounts we have so far seen is the most that will fit in the current ancient append vec
                index_first_item_overflow = hashes.len();
            }
            hashes.push(account.1.account.hash);
            // we have to specify 'slot' here because we are writing to an ancient append vec and squashing slots,
            // so we need to update the previous accounts index entry for this account from 'slot' to 'ancient_slot'
            accounts.push((&account.1.account.meta.pubkey, &account.1.account, slot));
        });
        Self {
            hashes,
            accounts,
            index_first_item_overflow,
        }
    }

    pub fn get(
        &self,
        storage: StorageSelector,
    ) -> (
        &[(&'a Pubkey, &'a StoredAccountMeta<'a>, Slot)],
        &[&'a Hash],
    ) {
        let range = match storage {
            StorageSelector::Overflow => self.index_first_item_overflow..self.accounts.len(),
            StorageSelector::Primary => 0..self.index_first_item_overflow,
        };
        (&self.accounts[range.clone()], &self.hashes[range])
    }
}

pub fn verify_contents<'a>(
    db: &AccountsDb,
    writer: &Arc<AccountStorageEntry>,
    append_vec_slot: Slot,
    recent: &[(&Pubkey, &StoredAccountMeta<'a>, u64)],
) {
    //if true {
    let store_id = writer.append_vec_id();
    for c in recent {
        if let AccountIndexGetResult::Found(g, _) =
            db.accounts_index.get(c.0, None, Some(append_vec_slot))
        {
            assert!(
                g.slot_list().iter().any(|(slot, info)| {
                    if slot == &append_vec_slot {
                        assert_eq!(info.store_id(), store_id);
                        true
                    } else {
                        false
                    }
                }),
                "{}, {:?}, id: {}",
                c.0,
                g.slot_list(),
                writer.append_vec_id()
            )
        } else {
            panic!("not found: {}", c.0);
        }
    }
    /* } else if true {
        let mut start = 0;
        let store_id = writer.append_vec_id();
        let mut current = 0;
        while let Some((account, next)) = writer.accounts.get_account(start) {
            let _account_size = next - start;
            let c = &recent[current];
            if c.0 == &account.meta.pubkey {
                match self
                    .accounts_index
                    .get(&account.meta.pubkey, None, Some(append_vec_slot))
                {
                    AccountIndexGetResult::Found(g, _) => assert!(
                        g.slot_list().iter().any(|(slot, info)| {
                            if slot == &append_vec_slot {
                                assert_eq!(info.store_id(), store_id);
                                true
                            } else {
                                false
                            }
                        }),
                        "{}, {:?}, id: {}",
                        account.meta.pubkey,
                        g.slot_list(),
                        writer.append_vec_id()
                    ),
                    _ => {}
                }
                current += 1;
            }
            start = next;
        }
        assert_eq!(current, recent.len());
    } else {
        let temp = Arc::clone(writer);
        let temp2 = [temp];
        let (stored_accounts, _num_stores, _original_bytes) =
            self.get_unique_accounts_from_storages(temp2.iter());
        for (pubkey, found) in stored_accounts.iter() {
            match self.accounts_index.get(pubkey, None, Some(append_vec_slot)) {
                AccountIndexGetResult::Found(g, _) => assert!(
                    g.slot_list().iter().any(|(slot, info)| {
                        slot == &append_vec_slot && info.store_id() == found.store_id
                    }),
                    "{}, {:?}, id: {}",
                    pubkey,
                    g.slot_list(),
                    writer.append_vec_id()
                ),
                _ => {}
            }
        }
    }*/
}

/// capacity of an ancient append vec
pub fn get_ancient_append_vec_capacity() -> u64 {
    use crate::append_vec::MAXIMUM_APPEND_VEC_FILE_SIZE;
    // smaller than max by a bit just in case
    MAXIMUM_APPEND_VEC_FILE_SIZE - 2048
}

/// true iff storage is ancient size and is almost completely full
pub fn is_full_ancient(storage: &AppendVec) -> bool {
    let threshold = 10_000; // just a guess here?
    is_ancient(storage) && storage.remaining_bytes() < threshold
}

/// is this a max-size append vec designed to be used as an ancient append vec?
pub fn is_ancient(storage: &AppendVec) -> bool {
    storage.capacity() == get_ancient_append_vec_capacity()
}

/// return true if there is a useful ancient append vec for this slot
/// otherwise, return false and the caller can skip this slot
pub fn get_ancient_append_vec(
    all_storages: &SnapshotStorage,
    current_ancient_storage: &mut Option<(Slot, Arc<AccountStorageEntry>)>,
    slot: Slot,
) -> bool {
    if current_ancient_storage.is_none() && all_storages.len() == 1 {
        let first_storage = all_storages.first().unwrap();
        if is_ancient(&first_storage.accounts) {
            if is_full_ancient(&first_storage.accounts) {
                return false; // skip this full ancient append vec completely
            }
            *current_ancient_storage = Some((slot, Arc::clone(first_storage)));
            return false; // we're done with this slot - this slot IS the ancient append vec
        }
    }
    true
}

pub fn purge_squashed_stores(
    db: &AccountsDb,
    drop_root: &mut bool,
    slot: Slot,
    ids: &[AppendVecId],
) -> Vec<Arc<AccountStorageEntry>> {
    let mut dead_storages = Vec::default();
    if let Some(slot_stores) = db.storage.get_slot_stores(slot) {
        let mut stores = slot_stores.write().unwrap();
        stores.retain(|_key, store| {
            if store.count() == 0 || !ids.contains(&store.append_vec_id()) {
                db.dirty_stores
                    .insert((slot, store.append_vec_id()), store.clone());
                dead_storages.push(store.clone());
                store.accounts.reset();
                false
            } else {
                true
            }
        });
        if stores.is_empty() {
            *drop_root = true;
        }
    }
    dead_storages
}
