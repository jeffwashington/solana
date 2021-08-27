//! Cached data for hashing accounts
use crate::accounts_hash::CalculateHashIntermediate;
use crate::pubkey_bins::PubkeyBinCalculator16;
use log::*;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use solana_measure::measure::Measure;
use std::collections::HashSet;
use std::fs::{self};
use std::fs::{remove_file, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub type EntryType = CalculateHashIntermediate;
pub type SavedType = Vec<Vec<EntryType>>;
pub type SavedTypeSlice = [Vec<EntryType>];

#[repr(C)]
pub struct Header {
    count: usize,
}

struct CacheHashDataFile {
    cell_size: u64,
    mmap: MmapMut,
    capacity: u64,
}

impl CacheHashDataFile {
    fn get_mut<T: Sized>(&mut self, ix: u64) -> &mut T {
        let start = (ix * self.cell_size) as usize + std::mem::size_of::<Header>();
        let end = start + std::mem::size_of::<T>();
        assert!(
            end <= self.capacity as usize,
            "end: {}, capacity: {}, ix: {}, cell size: {}",
            end,
            self.capacity,
            ix,
            self.cell_size
        );
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut T;
            &mut *item
        }
    }

    fn get_header_mut(&mut self) -> &mut Header {
        let start = 0_usize;
        let end = start + std::mem::size_of::<Header>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut Header;
            &mut *item
        }
    }

    fn new_map(file: &Path, capacity: u64) -> MmapMut {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)
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
        data.seek(SeekFrom::Start(capacity - 1)).unwrap();
        data.write_all(&[0]).unwrap();
        data.seek(SeekFrom::Start(0)).unwrap();
        data.flush().unwrap();
        unsafe { MmapMut::map_mut(&data).unwrap() }
    }
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CacheHashDataStats {
    pub cache_file_size: usize,
    pub cache_file_count: usize,
    pub entries: usize,
    pub loaded_from_cache: usize,
    pub entries_loaded_from_cache: usize,
    pub save_us: u64,
    pub saved_to_cache: usize,
    pub write_to_mmap_us: u64,
    pub create_save_us: u64,
    pub load_us: u64,
    pub read_us: u64,
    pub decode_us: u64,
    pub merge_us: u64,
    pub failed_to_save_count: u64,
    pub failed_to_load_count: u64,
    pub unused_cache_files: usize,
    pub one_cache_miss_range_percent: usize,
}

impl CacheHashDataStats {
    pub fn merge(&mut self, other: &CacheHashDataStats) {
        self.cache_file_size += other.cache_file_size;
        self.entries += other.entries;
        self.loaded_from_cache += other.loaded_from_cache;
        self.entries_loaded_from_cache += other.entries_loaded_from_cache;
        self.load_us += other.load_us;
        self.read_us += other.read_us;
        self.decode_us += other.decode_us;
        self.merge_us += other.merge_us;
        self.save_us += other.save_us;
        self.saved_to_cache += other.saved_to_cache;
        self.create_save_us += other.create_save_us;
        self.cache_file_count += other.cache_file_count;
        self.write_to_mmap_us += other.write_to_mmap_us;
        self.failed_to_save_count += other.failed_to_save_count;
        self.failed_to_load_count += other.failed_to_load_count;
        self.unused_cache_files += other.unused_cache_files;
        self.one_cache_miss_range_percent = other.one_cache_miss_range_percent; // not an add - we just want one of them
    }

    pub fn report(&self) {
        datapoint_info!(
            "cache_hash_data_stats",
            ("cache_file_size", self.cache_file_size, i64),
            ("cache_file_count", self.cache_file_count, i64),
            ("entries", self.entries, i64),
            ("loaded_from_cache", self.loaded_from_cache, i64),
            ("saved_to_cache", self.saved_to_cache, i64),
            (
                "entries_loaded_from_cache",
                self.entries_loaded_from_cache,
                i64
            ),
            ("save_us", self.save_us, i64),
            ("write_to_mmap_us", self.write_to_mmap_us, i64),
            ("create_save_us", self.create_save_us, i64),
            ("load_us", self.load_us, i64),
            ("read_us", self.read_us, i64),
            ("decode_us", self.decode_us, i64),
            ("merge_us", self.merge_us, i64),
            ("failed_to_save_count", self.failed_to_save_count, i64),
            ("failed_to_load_count", self.failed_to_load_count, i64),
            ("unused_cache_files", self.unused_cache_files, i64),
            ("one_cache_miss_range_percent", self.one_cache_miss_range_percent, i64),
        );
    }
}

pub type PreExistingCacheFiles = HashSet<String>;
pub struct CacheHashData {
    cache_folder: PathBuf,
    pre_existing_cache_files: Arc<Mutex<PreExistingCacheFiles>>,
    pub stats: Arc<Mutex<CacheHashDataStats>>,
}

impl Drop for CacheHashData {
    fn drop(&mut self) {
        self.delete_old_cache_files();
        self.stats.lock().unwrap().report();
    }
}

impl CacheHashData {
    pub fn new<P: AsRef<Path> + std::fmt::Debug>(parent_folder: &P) -> CacheHashData {
        let cache_folder = Self::get_cache_root_path(parent_folder);

        let dir = std::fs::create_dir_all(cache_folder.clone());
        if dir.is_err() {
            info!("error creating cache dir: {:?}", cache_folder);
        }

        let mut result = CacheHashData {
            cache_folder,
            pre_existing_cache_files: Arc::new(Mutex::new(PreExistingCacheFiles::default())),
            stats: Arc::new(Mutex::new(CacheHashDataStats::default())),
        };

        result.get_cache_files();
        result
    }
    fn delete_old_cache_files(&self) {
        let pre_existing_cache_files = self.pre_existing_cache_files.lock().unwrap();
        if !pre_existing_cache_files.is_empty() {
            self.stats.lock().unwrap().unused_cache_files += pre_existing_cache_files.len();
            error!(
                "deleting unused cache files: {}",
                pre_existing_cache_files.len()
            );
            for file_name in pre_existing_cache_files.iter() {
                let result = self.cache_folder.join(file_name);
                let _ = fs::remove_file(result);
            }
        }
    }
    fn get_cache_files(&mut self) {
        if self.cache_folder.is_dir() {
            let dir = fs::read_dir(self.cache_folder.clone());
            if let Ok(dir) = dir {
                let mut pre_existing = self.pre_existing_cache_files.lock().unwrap();
                for entry in dir.flatten() {
                    if let Some(name) = entry.path().file_name() {
                        pre_existing.insert(name.to_str().unwrap().to_string());
                    }
                }
                self.stats.lock().unwrap().cache_file_count += pre_existing.len();
            }
        }
    }

    fn get_cache_root_path<P: AsRef<Path>>(parent_folder: &P) -> PathBuf {
        parent_folder.as_ref().join("calculate_cache_hash")
    }

    pub fn load<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        file_name: &P,
        accumulator: &mut SavedType,
        start_bin_index: usize,
        bin_calculator: &PubkeyBinCalculator16,
    ) -> Result<(), std::io::Error> {
        let mut stats = CacheHashDataStats::default();
        let result = self.load_internal(
            file_name,
            accumulator,
            start_bin_index,
            bin_calculator,
            &mut stats,
        );
        self.stats.lock().unwrap().merge(&stats);
        result
    }

    fn load_internal<P: AsRef<Path> + std::fmt::Debug>(
        &self,
        file_name: &P,
        accumulator: &mut SavedType,
        start_bin_index: usize,
        bin_calculator: &PubkeyBinCalculator16,
        stats: &mut CacheHashDataStats,
    ) -> Result<(), std::io::Error> {
        let mut m = Measure::start("overall");
        let create = false;
        let path = self.cache_folder.join(file_name);
        let file_len = std::fs::metadata(path.clone())?.len();
        let mut m1 = Measure::start("");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(&path)?;

        let elem_size = std::mem::size_of::<EntryType>() as u64;
        let cell_size = elem_size;
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        m1.stop();
        let mut cache_file = CacheHashDataFile {
            mmap,
            cell_size,
            capacity: 0,
        };
        let header = cache_file.get_header_mut();
        let entries = header.count;

        let capacity = elem_size * (entries as u64) + std::mem::size_of::<Header>() as u64;
        cache_file.capacity = capacity;
        assert_eq!(
            capacity, file_len,
            "expected: {}, len on disk: {} {:?}, entries: {}, elem_size: {}",
            capacity, file_len, path, entries, cell_size
        );

        stats.entries = entries;
        stats.read_us = m1.as_us();
        stats.cache_file_size += capacity as usize;

        let file_name_lookup = file_name.as_ref().to_str().unwrap().to_string();
        let found = self
            .pre_existing_cache_files
            .lock()
            .unwrap()
            .remove(&file_name_lookup);
        if !found {
            info!(
                "tried to mark {:?} as used, but it wasn't in the set, one example: {:?}",
                file_name_lookup,
                self.pre_existing_cache_files.lock().unwrap().iter().next()
            );
        }

        stats.loaded_from_cache += 1;
        stats.entries_loaded_from_cache += entries;
        let mut m2 = Measure::start("decode");
        for i in 0..entries {
            let d = cache_file.get_mut::<EntryType>(i as u64);
            let mut pubkey_to_bin_index = bin_calculator.bin_from_pubkey(&d.pubkey);
            pubkey_to_bin_index -= start_bin_index;
            accumulator[pubkey_to_bin_index].push(d.clone()); // may want to avoid clone here
        }

        m2.stop();
        stats.decode_us += m2.as_us();
        m.stop();
        stats.load_us += m.as_us();
        Ok(())
    }

    pub fn save(&self, file_name: &Path, data: &SavedTypeSlice) -> Result<(), std::io::Error> {
        let mut stats = CacheHashDataStats::default();
        let result = self.save_internal(file_name, data, &mut stats);
        self.stats.lock().unwrap().merge(&stats);
        result
    }

    pub fn save_internal(
        &self,
        file_name: &Path,
        data: &SavedTypeSlice,
        stats: &mut CacheHashDataStats,
    ) -> Result<(), std::io::Error> {
        let mut m = Measure::start("save");
        let cache_path = self.cache_folder.join(file_name);
        let create = true;
        if create {
            let _ignored = remove_file(&cache_path);
        }
        let elem_size = std::mem::size_of::<EntryType>() as u64;
        let mut m1 = Measure::start("create save");
        let entries = data
            .iter()
            .map(|x: &Vec<EntryType>| x.len())
            .collect::<Vec<_>>();
        let entries = entries.iter().sum::<usize>();
        let cell_size = elem_size;
        let capacity = elem_size * (entries as u64) + std::mem::size_of::<Header>() as u64;

        let mmap = CacheHashDataFile::new_map(&cache_path, capacity);
        m1.stop();
        let mut cache_file = CacheHashDataFile {
            mmap,
            cell_size,
            capacity,
        };

        let mut header = cache_file.get_header_mut();
        header.count = entries;

        stats.create_save_us += m1.as_us();
        stats.cache_file_size = capacity as usize;
        stats.entries = entries;

        let mut m2 = Measure::start("write_to_mmap");
        let mut i = 0;
        data.iter().for_each(|x| {
            x.iter().for_each(|item| {
                let d = cache_file.get_mut::<EntryType>(i as u64);
                i += 1;
                *d = item.clone();
            })
        });
        assert_eq!(i, entries);
        m2.stop();
        stats.write_to_mmap_us += m2.as_us();
        m.stop();
        stats.save_us += m.as_us();
        stats.saved_to_cache += 1;
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    /*
    use super::*;
    use rand::Rng;
    fn generate_test_data(count: usize, bins: usize) -> (SavedType, usize) {
        let mut rng = rand::thread_rng();
        let mut ct = 0;
        (
            (0..bins)
                .into_iter()
                .map(|x| {
                    let rnd = rng.gen::<u64>() % (bins as u64);
                    if rnd < count as u64 {
                        (0..std::cmp::max(1, count / bins))
                            .into_iter()
                            .map(|_| {
                                ct += 1;
                                CalculateHashIntermediate::new(
                                    solana_sdk::hash::new_rand(&mut rng),
                                    x as u64,
                                    solana_sdk::pubkey::new_rand(),
                                )
                            })
                            .collect::<Vec<_>>()
                    } else {
                        vec![]
                    }
                })
                .collect::<Vec<_>>(),
            ct,
        )
    }

    #[test]
    fn test_read_write_many() {
        solana_logger::setup();
        let max_slot: Slot = 200_000;
        let bin_ranges = 1_usize;
        let bins = 32768_usize;
        let sample_data_count = (80_000_usize / max_slot as usize / bin_ranges) as usize;
        let tmpdir = std::env::temp_dir().join("test_read_write_many/sub1/sub2");
        let mut generated_data = vec![];
        error!(
            "generating: {} slots, {} items, {} bins",
            max_slot, sample_data_count, bins
        );
        let mut save_time = 0;
        let mut timings = CacheHashDataStats::default();
        let mut a_storage_path = None;
        let mut big_ct = 0;
        for slot in 0..max_slot {
            for _bin in 0..bin_ranges {
                let (data, ct) = generate_test_data(sample_data_count, bins);
                big_ct += ct;
                generated_data.push(data);
            }
            let mut m0 = Measure::start("");
            for _bin in 0..bin_ranges {
                let storage_file = format!("{}/{}.{}", tmpdir.to_str().unwrap(), slot, slot);
                a_storage_path = Some(storage_file.clone());
                let bin_range = Range {
                    start: 0,
                    end: bins,
                };
                //error!("{} {} {:?}", file!(), line!(), storage_file);
                let mut data = generated_data.pop().unwrap();
                assert!(generated_data.is_empty());
                let timings2 =
                    CacheHashData::save2(slot, &storage_file, &mut data, &bin_range, true).unwrap();
                timings.merge(&timings2);
            }
            m0.stop();
            save_time += m0.as_ms();
        }

        error!(
            "data generated: {:?}, {} items, rnd: {}",
            a_storage_path,
            big_ct,
            {
                let mut rng = rand::thread_rng();
                rng.gen::<u64>()
            }
        );

        let bin_calculator = PubkeyBinCalculator16::new(bins);
        let pre_existing_cache_files = RwLock::new(
            a_storage_path
                .map(|s| CacheHashData::get_cache_files(&s))
                .unwrap_or_default(),
        );

        let mut m1 = Measure::start("");
        let mut entries_read = 0;
        for slot in 0..max_slot {
            for _bin in 0..bin_ranges {
                let storage_file = format!("{}/{}.{}", tmpdir.to_str().unwrap(), slot, slot);
                let start_bin_index = 0;
                let bin_range = Range {
                    start: 0,
                    end: bins,
                };
                let mut accum = vec![vec![]; bins];
                let (_, timings2) = CacheHashData::load(
                    slot,
                    &storage_file,
                    &bin_range,
                    &mut accum,
                    start_bin_index,
                    &bin_calculator,
                    &pre_existing_cache_files,
                    true,
                )
                .unwrap();
                for i in accum {
                    entries_read += i.len();
                }
                timings.merge(&timings2);
            }
        }
        m1.stop();
        error!(
            "save: {}, load: {}, entries read: {}",
            save_time,
            m1.as_ms(),
            entries_read
        );
        error!("{:?}", timings);
    }
    */
}
