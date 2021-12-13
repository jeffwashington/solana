//! AccountInfo represents a reference to AccountSharedData in either an AppendVec or the write cache.
//! AccountInfo is not persisted anywhere between program runs.
//! AccountInfo is purely runtime state.
//! Note that AccountInfo is saved to disk buckets during runtime, but disk buckets are recreated at startup.
use crate::{accounts_db::AppendVecId, accounts_index::ZeroLamport};

#[derive(Default, Debug, PartialEq, Clone, Copy)]
pub struct AccountInfo {
    /// index identifying the append storage
    pub store_id: AppendVecId,

    /// offset into the storage
    pub offset: usize,

    /// needed to track shrink candidacy in bytes. Used to update the number
    /// of alive bytes in an AppendVec as newer slots purge outdated entries
    /// Note that highest bit is used for ZERO_LAMPORT_BIT
    stored_size: usize,
}

/// presence of this bit in stored_size indicates this account info references an account with zero lamports
const ZERO_LAMPORT_BIT: usize = 1 << (usize::BITS - 1);

impl ZeroLamport for AccountInfo {
    fn is_zero_lamport(&self) -> bool {
        self.stored_size & ZERO_LAMPORT_BIT == ZERO_LAMPORT_BIT
    }
}

impl AccountInfo {
    pub fn new(
        store_id: AppendVecId,
        offset: usize,
        mut stored_size: usize,
        lamports: u64,
    ) -> Self {
        assert!(stored_size < ZERO_LAMPORT_BIT);
        if lamports == 0 {
            stored_size |= ZERO_LAMPORT_BIT;
        }
        Self {
            store_id,
            offset,
            stored_size,
        }
    }

    pub fn stored_size(&self) -> usize {
        // elminate the special bit that indicates the info references an account with zero lamports
        self.stored_size & !ZERO_LAMPORT_BIT
    }
}
