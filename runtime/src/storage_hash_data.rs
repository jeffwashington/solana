//! Cached data for hashing accounts
use log::*;
use memmap2::MmapMut;
use rand::{thread_rng, Rng};
use std::fs::{remove_file, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use crate::accounts_hash::CalculateHashIntermediate;
use serde::{Deserialize, Serialize};
use solana_measure::measure::Measure;
use std::time::{UNIX_EPOCH};
use std::{
    io::{Read},
    ops::Range,
    path::{Path},
};

use crate::accounts_db::{PUBKEY_BINS_FOR_CALCULATING_HASHES, num_scan_passes, BINS_PER_PASS};


pub type SavedType = Vec<Vec<CalculateHashIntermediate>>;

#[repr(C)]
pub struct Header {
    lock: AtomicU64,
    bin_sizes: [u64; BINS_PER_PASS],
}

impl Header {
    fn try_lock(&self, uid: u64) -> bool {
        Ok(0)
            == self
                .lock
                .compare_exchange(0, uid, Ordering::Relaxed, Ordering::Relaxed)
    }
    fn unlock(&self, uid: u64) -> bool {
        Ok(uid)
            == self
                .lock
                .compare_exchange(uid, 0, Ordering::Relaxed, Ordering::Relaxed)
    }
    fn uid(&self) -> u64 {
        self.lock.load(Ordering::Relaxed)
    }
}

//#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CacheHashData {
    //pub data: SavedType,
    //pub storage_path: PathBuf,
    //pub expected_mod_date: u8,
    pub cell_size: u64,
    pub mmap: MmapMut,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CacheHashDataStats {
    pub storage_size: usize,
    pub cache_file_size: usize,
    pub entries: usize,
    pub loaded_from_cache: usize,
    pub entries_loaded_from_cache: usize,
    pub save_us: u64,
    pub load_us: u64,
    pub read_us: u64,
    pub decode_us: u64,
    pub calc_path_us: u64,
    pub merge_us: u64,
}

impl CacheHashDataStats {
    pub fn merge(&mut self, other: &CacheHashDataStats) {
        self.storage_size += other.storage_size;
        self.cache_file_size += other.cache_file_size;
        self.entries += other.entries;
        self.loaded_from_cache += other.loaded_from_cache;
        self.entries_loaded_from_cache += other.entries_loaded_from_cache;
        self.load_us += other.load_us;
        self.read_us += other.read_us;
        self.decode_us += other.decode_us;
        self.calc_path_us += other.calc_path_us;
        self.merge_us += other.merge_us;
        self.save_us += other.save_us;
    }
}

use bincode::serialize;
impl CacheHashData {
    fn calc_path<P: AsRef<Path>>(
        storage_file: &P,
        bin_range: &Range<usize>,
    ) -> Result<PathBuf, std::io::Error> {
        let storage_file = storage_file.as_ref();
        let parent = storage_file.parent().unwrap();
        let file_name = storage_file.file_name().unwrap();
        let parent_parent = parent.parent().unwrap();
        let parent_parent_parent = parent_parent.parent().unwrap();
        let amod = std::fs::metadata(storage_file)?.modified()?;
        let secs = amod.duration_since(UNIX_EPOCH).unwrap().as_secs();
        let cache = parent_parent_parent.join("calculate_cache_hash");
        let result = cache.join(format!(
            "{}.{}.{}",
            file_name.to_str().unwrap(),
            secs.to_string(),
            format!("{}.{}", bin_range.start, bin_range.end),
        ));
        Ok(result.to_path_buf())
    }

    fn new_map(file: &PathBuf, cell_size: usize, capacity: u64) -> MmapMut {
        let pos = format!("{}", thread_rng().gen_range(0, u128::MAX),);
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file.clone())
            .map_err(|e| {
                panic!(
                    "Unable to create data file {} in current dir({:?}): {:?}",
                    file.display(),
                    std::env::current_dir(),
                    e
                );
            })
            .unwrap();

        // Theoretical performance optimization: write a zero to the end of
        // the file so that we won't have to resize it later, which may be
        // expensive.
        //debug!("GROWING file {}", capacity * cell_size as u64);
        data.seek(SeekFrom::Start(capacity * cell_size as u64 - 1))
            .unwrap();
        data.write_all(&[0]).unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        data.flush().unwrap();
        unsafe { MmapMut::map_mut(&data).unwrap() }
    }
/*
    pub fn test() {
        let drives = Arc::new(vec![]);//    drives: Arc<Vec<PathBuf>>,
        let elements = 0;
        let index = Self::new_with_capacity(
            drives.clone(),
            1,
            std::mem::size_of::<CacheHashData>() as u64,
            elements,
        );
    }
    */
/*
    pub fn new_with_capacity(
        drives: Arc<Vec<PathBuf>>,
        num_elems: u64,
        elem_size: u64,
        capacity: u8,
    ) {
        // todo
        let cell_size = elem_size * num_elems + std::mem::size_of::<Header>() as u64;
        let (mmap, path) = Self::new_map(&drives, cell_size as usize, capacity);
        /*
        Self {
            path,
            mmap,
            drives,
            cell_size,
            used: AtomicU64::new(0),
            capacity,
        }*/
    }
    */
    pub fn load<P: AsRef<Path>>(
        storage_file: &P,
        bin_range: &Range<usize>,
    ) -> Result<(SavedType, CacheHashDataStats), std::io::Error> {
        let create = false;
        let mut timings = CacheHashDataStats::default();
        let mut m0 = Measure::start("");
        let path = Self::calc_path(storage_file, bin_range)?;
        m0.stop();
        timings.calc_path_us += m0.as_us();
        let mut m0 = Measure::start("");
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(create)
            .open(&path)?;
        panic!("loading: {:?}", path);
        /*
        let mut file_data = Vec::new();
        let bytes = file.read_to_end(&mut file_data)?;
        m0.stop();
        timings.load_us += m0.as_us();
        let mut m0 = Measure::start("");
        let decoded: Result<CacheHashData, _> = bincode::deserialize(&file_data[..]);
        m0.stop();
        timings.decode_us += m0.as_us();
        if decoded.is_err() {
            assert_eq!(bytes, file_data.len());
            panic!("failure to decode: {:?}, len: {}, error: {:?}", path, bytes, decoded);
        }
        let decoded = decoded.unwrap();
        drop(file);
        Ok((decoded.data, timings))
        */
    }
    pub fn get_mut<T: Sized>(&self, ix: u64) -> &mut T {
        let start = (ix * self.cell_size) as usize + std::mem::size_of::<Header>();
        let end = start + std::mem::size_of::<T>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut T;
            &mut *item
        }
    }

    pub fn get_header_mut(&self) -> &mut Header {
        let start = 0 as usize;
        let end = start + std::mem::size_of::<Header>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut Header;
            &mut *item
        }
    }


    pub fn save2<P: AsRef<Path> + std::fmt::Debug>(
        storage_file: &P,
        data: &mut SavedType,
        bin_range: &Range<usize>,
    ) -> Result<CacheHashDataStats, std::io::Error> {
        let mut m = Measure::start("save");
        let mut stats;
        {
        let cache_path = Self::calc_path(storage_file, bin_range)?;
        let parent = cache_path.parent().unwrap();
        std::fs::create_dir_all(parent);
        let create = true;
        if create {
            let _ignored = remove_file(&cache_path);
        }
        let elem_size = std::mem::size_of::<CacheHashData>() as u64;
        let entries = data.iter().map(|x: &Vec<CalculateHashIntermediate>| x.len()).collect::<Vec<_>>();
        let sum = entries.iter().sum::<usize>();
        let cell_size = elem_size * (sum as u64) + std::mem::size_of::<Header>() as u64;
        let capacity = cell_size;
        let mmap = Self::new_map(&cache_path, cell_size as usize, capacity);
        let mut chd = CacheHashData {
            //data: SavedType::default(),
            //storage_path
            mmap,
            cell_size,
        };
        stats = CacheHashDataStats {
            ..CacheHashDataStats::default()
        
        };

        let mut header = chd.get_header_mut();
        for i in 0..BINS_PER_PASS {
            header.bin_sizes[i] = if i <= entries.len() { entries[i] as u64} else {0};
        }

        //error!("writing {} bytes to: {:?}, lens: {:?}, storage_len: {}, storage: {:?}", encoded.len(), cache_path, file_data.data.iter().map(|x| x.len()).collect::<Vec<_>>(), file_len, storage_file);
        stats = CacheHashDataStats {
            //storage_size: file_len as usize,
            //cache_file_size: encoded.len(),
            entries: sum,
            ..CacheHashDataStats::default()
        
        };

        let mut i = 0;
        data.iter().for_each(|x| x.iter().for_each(|item| {
            let mut d = chd.get_mut(i as u64);
            i += 1;
            *d = item;
        }));
        error!("wrote: {:?}, {}, {:?}", cache_path, capacity, storage_file);
    }
        m.stop();
        stats.save_us += m.as_us();
        
        /*
        let expected_mod_date = 0; // TODO
        let file_size = 0; // TODO

        let mut data_bkup = SavedType::default();
        std::mem::swap(&mut data_bkup, data);
        let mut file_data = CacheHashData {
            expected_mod_date,
            storage_path: storage_file.as_ref().to_path_buf(),
            data: data_bkup,
        };

        let encoded: Vec<u8> = bincode::serialize(&file_data).unwrap();
        let file_len = std::fs::metadata(storage_file)?.len();
        let entries = file_data.data.iter().map(|x: &Vec<CalculateHashIntermediate>| x.len()).sum::<usize>();

        //error!("writing {} bytes to: {:?}, lens: {:?}, storage_len: {}, storage: {:?}", encoded.len(), cache_path, file_data.data.iter().map(|x| x.len()).collect::<Vec<_>>(), file_len, storage_file);
        let stats = CacheHashDataStats {
            storage_size: file_len as usize,
            cache_file_size: encoded.len(),
            entries,
            ..CacheHashDataStats::default()
        
        };
        std::mem::swap(&mut file_data.data, data);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(&cache_path)
            .map_err(|e| {
                panic!(
                    "Unable to {} data file {} in current dir({:?}): {:?}",
                    if create { "create" } else { "open" },
                    cache_path.display(),
                    std::env::current_dir(),
                    e
                );
            })
            .unwrap();
        file.write_all(&encoded)?;
        drop(file);
        */
        Ok(stats)
    }
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
