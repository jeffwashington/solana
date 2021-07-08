//! Persistent storage for accounts. For more information, see:
//! https://docs.solana.com/implemented-proposals/persistent-account-storage

use log::*;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    clock::{Slot},
    hash::Hash,
    pubkey::Pubkey,
};
use std::{
    fs::{remove_file, OpenOptions},
    io,
    io::{Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
};

// Data placement should be aligned at the next boundary. Without alignment accessing the memory may
// crash on some architectures.
const ALIGN_BOUNDARY_OFFSET: usize = mem::size_of::<u64>();
macro_rules! u64_align {
    ($addr: expr) => {
        ($addr + (ALIGN_BOUNDARY_OFFSET - 1)) & !(ALIGN_BOUNDARY_OFFSET - 1)
    };
}

pub type StoredMetaWriteVersion = u64;

/// Meta contains enough context to recover the index from storage itself
/// This struct will be backed by mmaped and snapshotted data files.
/// So the data layout must be stable and consistent across the entire cluster!
#[derive(Clone, PartialEq, Debug)]
pub struct StoredMeta {
    pub pubkey: Pubkey,
}

/// This struct will be backed by mmaped and snapshotted data files.
/// So the data layout must be stable and consistent across the entire cluster!
#[derive(Serialize, Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct AccountMeta {
    /// lamports in the account
    pub lamports: u64,
}

/// References to account data stored elsewhere. Getting an `Account` requires cloning
/// (see `StoredAccountMeta::clone_account()`).
#[derive(PartialEq, Debug)]
pub struct StoredAccountMeta<'a> {
    pub meta: &'a StoredMeta,
    /// account data
    pub account_meta: &'a AccountMeta,
    pub hash: &'a Hash,
}

/// A thread-safe, file-backed block of memory used to store `Account` instances. Append operations
/// are serialized such that only one thread updates the internal `append_lock` at a time. No
/// restrictions are placed on reading. That is, one may read items from one thread while another
/// is appending new items.
#[derive(Debug, AbiExample)]
pub struct StorageHashData {
    /// The file path where the data is stored.
    pub path: PathBuf,

    pub storage_path: PathBuf,
    pub expected_mod_date: u8,

    /// A file-backed block of memory that is used to store the data for each appended item.
    map: MmapMut,

    remove_on_drop: bool,
    file_size: usize,
}

impl Drop for StorageHashData {
    fn drop(&mut self) {
        if self.remove_on_drop {
            if let Err(_e) = remove_file(&self.path) {
                // promote this to panic soon.
                // disabled due to many false positive warnings while running tests.
                // blocked by rpc's upgrade to jsonrpc v17
                //error!("StorageHashData failed to remove {:?}: {:?}", &self.path, e);
            }
        }
    }
}

impl StorageHashData {
    pub fn new(file: &Path, create: bool, size: usize) -> Self {
        if create {
            let _ignored = remove_file(file);
        }

        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(file)
            .map_err(|e| {
                panic!(
                    "Unable to {} data file {} in current dir({:?}): {:?}",
                    if create { "create" } else { "open" },
                    file.display(),
                    std::env::current_dir(),
                    e
                );
            })
            .unwrap();

        // Theoretical performance optimization: write a zero to the end of
        // the file so that we won't have to resize it later, which may be
        // expensive.
        data.seek(SeekFrom::Start((size - 1) as u64)).unwrap();
        data.write_all(&[0]).unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        data.flush().unwrap();

        //UNSAFE: Required to create a Mmap
        let map = unsafe { MmapMut::map_mut(&data) };
        let map = map.unwrap_or_else(|e| {
            error!(
                "Failed to map the data file (size: {}): {}.\n
                    Please increase sysctl vm.max_map_count or equivalent for your platform.",
                size, e
            );
            std::process::exit(1);
        });

        let expected_mod_date = 0; // TODO
        let storage_path = file.to_path_buf(); // TODO
        let file_size = 0; // TODO
        StorageHashData {
            path: file.to_path_buf(),
            map,
            remove_on_drop: true,
            expected_mod_date,
            storage_path,
            file_size,
        }
    }

    pub fn file_name(slot: Slot, id: usize) -> String {
        format!("{}.{}", slot, id)
    }

    fn len(&self) -> usize {self.file_size}

    pub fn new_from_file<P: AsRef<Path>>(path: P, current_len: usize) -> io::Result<(Self, usize)> {
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)?;

        let file_size = std::fs::metadata(&path)?.len();

        let map = unsafe {
            let result = MmapMut::map_mut(&data);
            if result.is_err() {
                // for vm.max_map_count, error is: {code: 12, kind: Other, message: "Cannot allocate memory"}
                info!("memory map error: {:?}. This may be because vm.max_map_count is not set correctly.", result);
            }
            result?
        };

        let expected_mod_date = 0; // TODO
        let storage_path = path.as_ref().to_path_buf(); // TODO

        let new = StorageHashData {
            path: path.as_ref().to_path_buf(),
            map,
            remove_on_drop: true,
            expected_mod_date,
            storage_path,
            file_size: file_size as usize,
        };

        let num_accounts = file_size as usize / std::mem::size_of::<StoredAccountMeta>();

        Ok((new, num_accounts))
    }

    /// Get a reference to the data at `offset` of `size` bytes if that slice
    /// doesn't overrun the internal buffer. Otherwise return None.
    /// Also return the offset of the first byte after the requested data that
    /// falls on a 64-byte boundary.
    fn get_slice(&self, offset: usize, size: usize) -> Option<(&[u8], usize)> {
        let (next, overflow) = offset.overflowing_add(size);
        if overflow || next > self.len() {
            return None;
        }
        let data = &self.map[offset..next];
        let next = u64_align!(next);

        Some((
            //UNSAFE: This unsafe creates a slice that represents a chunk of self.map memory
            //The lifetime of this slice is tied to &self, since it points to self.map memory
            unsafe { std::slice::from_raw_parts(data.as_ptr() as *const u8, size) },
            next,
        ))
    }

    /// Copy `len` bytes from `src` to the first 64-byte boundary after position `offset` of
    /// the internal buffer. Then update `offset` to the first byte after the copied data.
    fn append_ptr(&self, offset: &mut usize, src: *const u8, len: usize) {
        let pos = u64_align!(*offset);
        let data = &self.map[pos..(pos + len)];
        //UNSAFE: This mut append is safe because only 1 thread can append at a time
        //Mutex<()> guarantees exclusive write access to the memory occupied in
        //the range.
        unsafe {
            let dst = data.as_ptr() as *mut u8;
            std::ptr::copy(src, dst, len);
        };
        *offset = pos + len;
    }

    /// Return a reference to the type at `offset` if its data doesn't overrun the internal buffer.
    /// Otherwise return None. Also return the offset of the first byte after the requested data
    /// that falls on a 64-byte boundary.
    fn get_type<'a, T>(&self, offset: usize) -> Option<(&'a T, usize)> {
        let (data, next) = self.get_slice(offset, mem::size_of::<T>())?;
        let ptr: *const T = data.as_ptr() as *const T;
        //UNSAFE: The cast is safe because the slice is aligned and fits into the memory
        //and the lifetime of the &T is tied to self, which holds the underlying memory map
        Some((unsafe { &*ptr }, next))
    }

    /// Return account metadata for the account at `offset` if its data doesn't overrun
    /// the internal buffer. Otherwise return None. Also return the offset of the first byte
    /// after the requested data that falls on a 64-byte boundary.
    pub fn get_account<'a>(&'a self, offset: usize) -> Option<(StoredAccountMeta<'a>, usize)> {
        let (meta, next): (&'a StoredMeta, _) = self.get_type(offset)?;
        let (account_meta, next): (&'a AccountMeta, _) = self.get_type(next)?;
        let (hash, next): (&'a Hash, _) = self.get_type(next)?;
        let stored_size = next - offset;
        Some((
            StoredAccountMeta {
                meta,
                account_meta,
                hash,
            },
            next,
        ))
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }
/*
    /// Copy each account metadata, account and hash to the internal buffer.
    /// Return the starting offset of each account metadata.
    /// After each account is appended, the internal `current_len` is updated
    /// and will be available to other threads.
    pub fn append_accounts(
        &self,
        accounts: &[(StoredMeta, Option<&impl ReadableAccount>)],
        hashes: &[impl Borrow<Hash>],
    ) -> Vec<usize> {
        let _lock = self.append_lock.lock().unwrap();
        let mut offset = self.len();
        let mut rv = Vec::with_capacity(accounts.len());
        for ((stored_meta, account), hash) in accounts.iter().zip(hashes) {
            let meta_ptr = stored_meta as *const StoredMeta;
            let account_meta = AccountMeta::from(*account);
            let account_meta_ptr = &account_meta as *const AccountMeta;
            let hash_ptr = hash.borrow().as_ref().as_ptr();
            let ptrs = [
                (meta_ptr as *const u8, mem::size_of::<StoredMeta>()),
                (account_meta_ptr as *const u8, mem::size_of::<AccountMeta>()),
                (hash_ptr as *const u8, mem::size_of::<Hash>()),
            ];
            if let Some(res) = self.append_ptrs_locked(&mut offset, &ptrs) {
                rv.push(res)
            } else {
                break;
            }
        }

        // The last entry in this offset needs to be the u64 aligned offset, because that's
        // where the *next* entry will begin to be stored.
        rv.push(u64_align!(offset));

        rv
    }

    /// Copy the account metadata, account and hash to the internal buffer.
    /// Return the starting offset of the account metadata.
    /// After the account is appended, the internal `current_len` is updated.
    pub fn append_account(
        &self,
        storage_meta: StoredMeta,
        account: &AccountSharedData,
        hash: Hash,
    ) -> Option<usize> {
        let res = self.append_accounts(&[(storage_meta, Some(account))], &[&hash]);
        if res.len() == 1 {
            None
        } else {
            res.first().cloned()
        }
    }
    */
}

#[cfg(test)]
pub mod tests {
    use super::test_utils::*;
    use super::*;
    use assert_matches::assert_matches;
    use rand::{thread_rng, Rng};
    use solana_sdk::{account::WritableAccount, timing::duration_as_ms};
    use std::time::Instant;

}
