//! Cached data for hashing accounts
use crate::accounts_hash::CalculateHashIntermediate;
use crate::pubkey_bins::PubkeyBinCalculator16;
use log::*;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use solana_measure::measure::Measure;
use solana_sdk::clock::Slot;
use std::collections::HashSet;
use std::fs::{self};
use std::fs::{remove_file, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::sync::RwLock;

pub type SavedType = Vec<Vec<CalculateHashIntermediate>>;
pub type SavedTypeSlice = [Vec<CalculateHashIntermediate>];

#[repr(C)]
pub struct Header {
    count: usize,
}

pub struct CacheHashData {
    cell_size: u64,
    mmap: MmapMut,
    capacity: u64,
}

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CacheHashDataStats {
    pub storage_size: usize,
    pub cache_file_size: usize,
    pub cache_file_count: usize,
    pub entries: usize,
    pub loaded_from_cache: usize,
    pub entries_loaded_from_cache: usize,
    pub save_us: u64,
    pub write_to_mmap_us: u64,
    pub create_save_us: u64,
    pub load_us: u64,
    pub read_us: u64,
    pub decode_us: u64,
    pub calc_path_us: u64,
    pub merge_us: u64,
    pub sum_entries_us: u64,
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
        self.create_save_us += other.create_save_us;
        self.cache_file_count += other.cache_file_count;
        self.write_to_mmap_us += other.write_to_mmap_us;
        self.sum_entries_us += other.sum_entries_us;
    }
}

pub type PreExistingCacheFiles = HashSet<String>;

impl CacheHashData {
    fn directory<P: AsRef<Path>>(parent_folder: &P) -> PathBuf {
        parent_folder.as_ref().join("calculate_cache_hash")
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

    pub fn delete_old_cache_files<P: AsRef<Path>>(
        cache_path: &P,
        file_names: &PreExistingCacheFiles,
    ) {
        error!("deleting unused cache files: {}", file_names.len());
        let cache_path = cache_path.as_ref();
        for file_name in file_names {
            let result = cache_path.join(file_name);
            let _ = fs::remove_file(result);
        }
    }
    pub fn get_cache_files<P: AsRef<Path>>(storage_path: &P) -> PreExistingCacheFiles {
        let mut items = PreExistingCacheFiles::new();
        let cache = Self::get_cache_root_path(storage_path);
        if cache.is_dir() {
            let dir = fs::read_dir(cache.clone());
            if let Ok(dir) = dir {
                for entry in dir.flatten() {
                    if let Some(name) = entry.path().file_name() {
                        items.insert(name.to_str().unwrap().to_string());
                    }
                }
            }
        }
        error!("{} items in hash calc cache: {:?}", items.len(), cache);
        items
    }

    pub fn get_cache_root_path<P: AsRef<Path>>(parent_folder: &P) -> PathBuf {
        Self::directory(parent_folder)
    }

    pub fn load<P: AsRef<Path> + std::fmt::Debug>(
        _slot: Slot,
        path: &P,
        accumulator: &mut Vec<Vec<CalculateHashIntermediate>>,
        start_bin_index: usize,
        bin_calculator: &PubkeyBinCalculator16,
        preexisting: &RwLock<PreExistingCacheFiles>,
        timings: CacheHashDataStats,
    ) -> Result<(SavedType, CacheHashDataStats), std::io::Error> {
        let mut m = Measure::start("overall");
        let create = false;
        let file_len = std::fs::metadata(path)?.len();
        let mut m1 = Measure::start("");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(create)
            .open(&path)?;

        let elem_size = std::mem::size_of::<CalculateHashIntermediate>() as u64;
        let cell_size = elem_size;
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        m1.stop();
        let mut chd = CacheHashData {
            mmap,
            cell_size,
            capacity: 0,
        };
        let header = chd.get_header_mut();
        let sum = header.count;

        let capacity = elem_size * (sum as u64) + std::mem::size_of::<Header>() as u64;
        chd.capacity = capacity;
        assert_eq!(
            capacity, file_len,
            "expected: {}, len on disk: {} {:?}, sum: {}, elem_size: {}",
            capacity, file_len, path, sum, cell_size
        );

        let mut stats = CacheHashDataStats {
            //storage_size: file_len as usize,
            entries: sum,
            ..timings
        };
        stats.read_us = m1.as_us();
        stats.cache_file_size += capacity as usize;

        let file_name: String = path
            .as_ref()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();

        let found = preexisting.write().unwrap().remove(&file_name);
        if !found {
            error!(
                "tried to mark {:?} as used, but it wasn't in the set: {:?}",
                file_name,
                preexisting.read().unwrap().iter().next()
            );
        }

        stats.entries_loaded_from_cache += sum;
        let mut m2 = Measure::start("");
        for i in 0..sum {
            let d = chd.get_mut::<CalculateHashIntermediate>(i as u64);
            let mut pubkey_to_bin_index = bin_calculator.bin_from_pubkey(&d.pubkey);
            pubkey_to_bin_index -= start_bin_index;
            accumulator[pubkey_to_bin_index].push(d.clone()); // may want to avoid clone here
        }

        m2.stop();
        stats.decode_us += m2.as_us();
        m.stop();
        stats.load_us += m.as_us();
        Ok((vec![], stats))
    }
    pub fn get_mut<T: Sized>(&mut self, ix: u64) -> &mut T {
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

    pub fn get_header_mut(&mut self) -> &mut Header {
        let start = 0_usize;
        let end = start + std::mem::size_of::<Header>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut Header;
            &mut *item
        }
    }

    pub fn save(
        _slot: Slot,
        cache_path: &Path,
        data: &SavedTypeSlice,
        mut stats: CacheHashDataStats,
    ) -> Result<CacheHashDataStats, std::io::Error> {
        let mut m = Measure::start("");
        let cache_path = cache_path;
        //let cache_path = cache_path.to_path_buf();
        let parent = cache_path.parent().unwrap();
        let dir = std::fs::create_dir_all(parent);
        if dir.is_err() {
            error!("error creating dir: {:?}", parent);
        }
        dir?;
        let create = true;
        if create {
            let _ignored = remove_file(&cache_path);
        }
        let elem_size = std::mem::size_of::<CalculateHashIntermediate>() as u64;
        let mut m0 = Measure::start("");
        let entries = data
            .iter()
            .map(|x: &Vec<CalculateHashIntermediate>| x.len())
            .collect::<Vec<_>>();
        let sum = entries.iter().sum::<usize>();
        m0.stop();
        stats.sum_entries_us += m0.as_us();
        let cell_size = elem_size;
        let capacity = elem_size * (sum as u64) + std::mem::size_of::<Header>() as u64;
        let mut m1 = Measure::start("");

        let mmap = Self::new_map(cache_path, capacity);
        m1.stop();
        let mut chd = CacheHashData {
            //data: SavedType::default(),
            //storage_path
            mmap,
            cell_size,
            capacity,
        };
        stats.create_save_us = m1.as_us();
        stats.cache_file_count = 1;

        let mut header = chd.get_header_mut();
        header.count = sum;

        stats = CacheHashDataStats {
            cache_file_size: capacity as usize,
            entries: sum,
            ..CacheHashDataStats::default()
        };

        let mut m2 = Measure::start("");
        let mut i = 0;
        data.iter().for_each(|x| {
            x.iter().for_each(|item| {
                let d = chd.get_mut::<CalculateHashIntermediate>(i as u64);
                i += 1;
                *d = item.clone();
            })
        });
        assert_eq!(i, sum);
        m2.stop();
        stats.write_to_mmap_us += m2.as_us();
        m.stop();
        stats.save_us += m.as_us();
        Ok(stats)
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
