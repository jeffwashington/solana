//! Cached data for hashing accounts
#[cfg(test)]
use crate::pubkey_bins::PubkeyBinCalculator24;
use {
    crate::{
        accounts_hash::{CalculateHashIntermediate},
        cache_hash_data_stats::CacheHashDataStats,
        pubkey_bins::PubkeyBinCalculator24,
    },
    log::*,
    memmap2::MmapMut,
    solana_measure::measure::Measure,
    std::{
        collections::HashSet,
        fs::{self, remove_file, OpenOptions},
        io::{Seek, SeekFrom, Write},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
    },
};

pub type EntryType = CalculateHashIntermediate;
pub type SavedType = Vec<Vec<EntryType>>;
pub type SavedTypeSlice = [Vec<EntryType>];

#[repr(C)]
pub struct Header {
    count: usize,
}

pub(crate) struct CacheHashDataFile {
    cell_size: u64,
    mmap: MmapMut,
    capacity: u64,
}

impl CacheHashDataFile {
    /// return a slice of a reference to all the cache hash data from the mmapped file
    pub(crate) fn get_cache_hash_data(&self) -> &[EntryType] {
        self.get_slice(0)
    }

    /// Populate 'accumulator' from entire contents of the cache file.
    pub(crate) fn load_all(
        &self,
        accumulator: &mut SavedType,
        start_bin_index: usize,
        bin_calculator: &PubkeyBinCalculator24,
        stats: &mut CacheHashDataStats,
    ) {
        let mut m2 = Measure::start("decode");
        let slices = self.get_cache_hash_data();
        for d in slices {
            let mut pubkey_to_bin_index = bin_calculator.bin_from_pubkey(&d.pubkey);
            assert!(
                pubkey_to_bin_index >= start_bin_index,
                "{pubkey_to_bin_index}, {start_bin_index}"
            ); // this would indicate we put a pubkey in too high of a bin
            pubkey_to_bin_index -= start_bin_index;
            accumulator[pubkey_to_bin_index].push(d.clone()); // may want to avoid clone here
        }

        m2.stop();
        stats.decode_us += m2.as_us();
    }

    /// get '&mut EntryType' from cache file [ix]
    fn get_mut(&mut self, ix: u64) -> &mut EntryType {
        let item_slice = self.get_slice_internal(ix);
        unsafe {
            let item = item_slice.as_ptr() as *mut EntryType;
            &mut *item
        }
    }

    /// get '&[EntryType]' from cache file [ix..]
    fn get_slice(&self, ix: u64) -> &[EntryType] {
        let start = self.get_element_offset_byte(ix);
        let item_slice: &[u8] = &self.mmap[start..];
        let remaining_elements = item_slice.len() / std::mem::size_of::<EntryType>();
        unsafe {
            let item = item_slice.as_ptr() as *const EntryType;
            std::slice::from_raw_parts(item, remaining_elements)
        }
    }

    /// return byte offset of entry 'ix' into a slice which contains a header and at least ix elements
    fn get_element_offset_byte(&self, ix: u64) -> usize {
        let start = (ix * self.cell_size) as usize + std::mem::size_of::<Header>();
        debug_assert_eq!(start % std::mem::align_of::<EntryType>(), 0);
        start
    }

    /// get the bytes representing cache file [ix]
    fn get_slice_internal(&self, ix: u64) -> &[u8] {
        let start = self.get_element_offset_byte(ix);
        let end = start + std::mem::size_of::<EntryType>();
        assert!(
            end <= self.capacity as usize,
            "end: {}, capacity: {}, ix: {}, cell size: {}",
            end,
            self.capacity,
            ix,
            self.cell_size
        );
        &self.mmap[start..end]
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

    fn new_map(file: impl AsRef<Path>, capacity: u64) -> Result<MmapMut, std::io::Error> {
        let mut data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file)?;

        // Theoretical performance optimization: write a zero to the end of
        // the file so that we won't have to resize it later, which may be
        // expensive.
        data.seek(SeekFrom::Start(capacity - 1)).unwrap();
        data.write_all(&[0]).unwrap();
        data.rewind().unwrap();
        data.flush().unwrap();
        Ok(unsafe { MmapMut::map_mut(&data).unwrap() })
    }

    fn load_map(file: impl AsRef<Path>) -> Result<MmapMut, std::io::Error> {
        let data = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(file)?;

        Ok(unsafe { MmapMut::map_mut(&data).unwrap() })
    }
}

pub type PreExistingCacheFiles = HashSet<PathBuf>;
pub struct CacheHashData {
    cache_dir: PathBuf,
    pre_existing_cache_files: Arc<Mutex<PreExistingCacheFiles>>,
    pub stats: Arc<Mutex<CacheHashDataStats>>,
}

impl Drop for CacheHashData {
    fn drop(&mut self) {
        //self.delete_old_cache_files();
        self.stats.lock().unwrap().report();
    }
}

type hashentry = (usize, solana_sdk::hash::Hash, u64);

impl CacheHashData {
    pub fn compare_two<P: AsRef<Path> + std::fmt::Debug>(files: &[&P; 2]) {
        let datas = files.into_iter().map(|p| Self::new(p)).collect::<Vec<_>>();
        use {dashmap::DashMap, std::collections::HashMap};
        let mut one = Mutex::new(HashMap::<Pubkey, Vec<hashentry>>::new());
        let mut two = Mutex::new(HashMap::<Pubkey, Vec<hashentry>>::new());
        let cache_one = &datas[0];
        use solana_sdk::pubkey::Pubkey;
        let mut files = cache_one
            .pre_existing_cache_files
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        let vec_size = 65536;

        use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
        let bin_calc = PubkeyBinCalculator24::new(65536);
        use std::str::FromStr;
        let interesting = Pubkey::from_str("JARQT5Jeb3nXH8vjE1pMiX5RADWshsRyLyDd7V72TjuZ").unwrap();
        let p1 = crate::accounts_partition::partition_from_pubkey(&interesting, 432_000);

        use log::*;
        error!("{}{}, p1: {}, {}", file!(), line!(), p1, interesting);
        files.sort();
        let cache_two = &datas[1];
        let mut files2 = cache_two
            .pre_existing_cache_files
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        use log::*;
        files2.sort();

        [0, 1].par_iter().for_each(|i| {

            if i == &0 {
                    let mut one: std::sync::MutexGuard<'_, HashMap<Pubkey, Vec<(usize, solana_sdk::hash::Hash, u64)>>> = one.lock().unwrap();
        files.iter().enumerate().for_each(|(i_file, file)| {
            //error!("file: {:?}", file);
            let mut accum = (0..vec_size).map(|_| Vec::default()).collect::<Vec<_>>();
            let x = cache_one.load_map(file);
            if x.is_err() {
                error!("failure to load file :{:?}", file);
            }
            x.unwrap().load_all(&mut accum, 0, &bin_calc, &mut CacheHashDataStats::default());
            accum.into_iter().flatten().for_each(|entry| {
                let pk = entry.pubkey;
                let new_one = (i_file, entry.hash, entry.lamports);
                if interesting == pk {
                    error!("found1: {:?}", new_one);
                }
                if let Some(mut current) = one.get_mut(&pk) {
                    current.push(new_one);
                } else {
                    one.insert(pk, vec![new_one]);
                }
            });
        });
    }
    else {
        error!("{}{}", file!(), line!());
        let mut two = two.lock().unwrap();
        files2.iter().enumerate().for_each(|(i_file, file)| {
            //error!("file2: {:?}", file);
            let mut accum = (0..vec_size).map(|_| Vec::default()).collect::<Vec<_>>();
            let x = cache_two.load_map(file);
            if x.is_err() {
                error!("failure to load file2 :{:?}", file);
            }
            x.unwrap().load_all(&mut accum, 0, &bin_calc, &mut CacheHashDataStats::default());
            accum.into_iter().flatten().for_each(|entry| {
                let pk = entry.pubkey;
                let new_one = (i_file, entry.hash, entry.lamports);
                if interesting == pk {
                    error!("found2: {:?}", new_one);
                }
                if let Some(mut current) = two.get_mut(&pk) {
                    current.push(new_one);
                } else {
                    two.insert(pk, vec![new_one]);
                }
            });
        });
    }});

    let mut one = one.into_inner().unwrap();
    let mut two = two.into_inner().unwrap();
    error!(
            "items in one: {}, two: {}, files in one, two: {}, {}",
            one.len(),
            two.len(),
            files.len(),
            files2.len()
        );

        let mut cap1 = 0;
        let mut cap2 = 0;
        let mut added1 = 0;
        let mut added2 = 0;
        let ZERO_RAW_LAMPORTS_SENTINEL = 0;
        error!("draining");
        for entry in one.iter() {
            let k = entry.0;
            let mut v = entry.1.clone();
            //v.sort_by(Self::sorter);
            let one = v.last().unwrap();
            if one.2 != ZERO_RAW_LAMPORTS_SENTINEL && one.2 != 0{
                cap1 += one.2;
                added1 += 1;
            }
            if let Some(mut entry) = two.remove(&k) {
                //entry.sort_by(Self::sorter);
                let two = entry.last().unwrap();
                if two.2 != ZERO_RAW_LAMPORTS_SENTINEL && two.2 != 0 {
                    cap2 += two.2;
                    added2 += 1;
                }
                if one.1 != two.1 && !((one.2 == ZERO_RAW_LAMPORTS_SENTINEL && two.2 == 0) || (one.2 == 0 && two.2 == ZERO_RAW_LAMPORTS_SENTINEL)) {
                    error!("values different: {} {:?}, {:?}", k, v, entry);
                } else {
                    assert_eq!(one.2, two.2);
                }
            } else {
                if one.2 != ZERO_RAW_LAMPORTS_SENTINEL && one.2 != 0 {
                    error!("in 1, not in 2: {:?}, {:?}", k, v);
                }
            }
        }
        for entry in two.iter() {
            let k = entry.0;
            let mut v = entry.1.clone();
            //v.sort_by(Self::sorter);
            let two = v.last().unwrap();
            if two.2 != ZERO_RAW_LAMPORTS_SENTINEL && two.2 != 0 {
                added2 += 1;
                cap2 += two.2;
                error!("in 2, not in 1: {:?}, {:?}", k, v);
            }
        }
        panic!(
            "done with compare, lamports: {}, {}, {}, added: {},{}",
            cap1,
            cap2,
            if cap1 > cap2 {
                cap1 - cap2
            } else {
                cap2 - cap1
            },
            added1,
            added2
        );
    }
/*
    fn sorter_unused(a: &hashentry, b: &hashentry) -> std::cmp::Ordering {
        a.0.cmp(&b.0)
        /*
        let slota = a.0.split('.').next().unwrap();
        let slotb = b.0.split('.').next().unwrap();
        slota.cmp(&slotb)*/
    }*/

    fn get_cache_root_path<P: AsRef<Path>>(parent_folder: &P) -> PathBuf {
        //parent_folder.as_ref().join("calculate_accounts_hash_cache")
        parent_folder.as_ref().to_path_buf()
    }

    pub fn new<P: AsRef<Path> + std::fmt::Debug>(parent_folder: &P) -> CacheHashData {
        let cache_folder = Self::get_cache_root_path(parent_folder);

        std::fs::create_dir_all(cache_folder.clone())
            .unwrap_or_else(|_| panic!("error creating cache dir: {:?}", cache_folder));

        info!("accounts hash folder: {:?}", cache_folder);

        let result = CacheHashData {
            cache_dir: cache_folder,
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
            for file_name in pre_existing_cache_files.iter() {
                let result = self.cache_dir.join(file_name);
                let _ = fs::remove_file(result);
            }
        }
    }
    fn get_cache_files(&self) {
        if self.cache_dir.is_dir() {
            let dir = fs::read_dir(&self.cache_dir);
            if let Ok(dir) = dir {
                let mut pre_existing = self.pre_existing_cache_files.lock().unwrap();
                for entry in dir.flatten() {
                    if let Some(name) = entry.path().file_name() {
                        pre_existing.insert(PathBuf::from(name));
                    }
                }
                self.stats.lock().unwrap().cache_file_count += pre_existing.len();
            }
        }
    }

    #[cfg(test)]
    /// load from 'file_name' into 'accumulator'
    pub(crate) fn load(
        &self,
        file_name: impl AsRef<Path>,
        accumulator: &mut SavedType,
        start_bin_index: usize,
        bin_calculator: &PubkeyBinCalculator24,
    ) -> Result<(), std::io::Error> {
        let mut m = Measure::start("overall");
        let cache_file = self.load_map(file_name)?;
        let mut stats = CacheHashDataStats::default();
        cache_file.load_all(accumulator, start_bin_index, bin_calculator, &mut stats);
        m.stop();
        self.stats.lock().unwrap().load_us += m.as_us();
        Ok(())
    }

    /// map 'file_name' into memory
    pub(crate) fn load_map(
        &self,
        file_name: impl AsRef<Path>,
    ) -> Result<CacheHashDataFile, std::io::Error> {
        let mut stats = CacheHashDataStats::default();
        let result = self.map(file_name, &mut stats);
        self.stats.lock().unwrap().accumulate(&stats);
        result
    }

    /// create and return a MappedCacheFile for a cache file path
    fn map(
        &self,
        file_name: impl AsRef<Path>,
        stats: &mut CacheHashDataStats,
    ) -> Result<CacheHashDataFile, std::io::Error> {
        let path = self.cache_dir.join(&file_name);
        let file_len = std::fs::metadata(&path)?.len();
        let mut m1 = Measure::start("read_file");
        let mmap = CacheHashDataFile::load_map(&path)?;
        m1.stop();
        stats.read_us = m1.as_us();
        let header_size = std::mem::size_of::<Header>() as u64;
        if file_len < header_size {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }

        let cell_size = std::mem::size_of::<EntryType>() as u64;
        unsafe {
            assert_eq!(
                mmap.align_to::<EntryType>().0.len(),
                0,
                "mmap is not aligned"
            );
        }
        assert_eq!((cell_size as usize) % std::mem::size_of::<u64>(), 0);
        let mut cache_file = CacheHashDataFile {
            mmap,
            cell_size,
            capacity: 0,
        };
        let header = cache_file.get_header_mut();
        let entries = header.count;

        let capacity = cell_size * (entries as u64) + header_size;
        if file_len < capacity {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        cache_file.capacity = capacity;
        assert_eq!(
            capacity, file_len,
            "expected: {capacity}, len on disk: {file_len} {}, entries: {entries}, cell_size: {cell_size}", path.display(),
        );

        stats.total_entries = entries;
        stats.cache_file_size += capacity as usize;

        self.pre_existing_cache_files
            .lock()
            .unwrap()
            .remove(file_name.as_ref());

        stats.loaded_from_cache += 1;
        stats.entries_loaded_from_cache += entries;

        Ok(cache_file)
    }

    /// save 'data' to 'file_name'
    pub fn save(
        &self,
        file_name: impl AsRef<Path>,
        data: &SavedTypeSlice,
    ) -> Result<(), std::io::Error> {
        let mut stats = CacheHashDataStats::default();
        let result = self.save_internal(file_name, data, &mut stats);
        self.stats.lock().unwrap().accumulate(&stats);
        result
    }

    fn save_internal(
        &self,
        file_name: impl AsRef<Path>,
        data: &SavedTypeSlice,
        stats: &mut CacheHashDataStats,
    ) -> Result<(), std::io::Error> {
        let mut m = Measure::start("save");
        let cache_path = self.cache_dir.join(file_name);
        // overwrite any existing file at this path
        let _ignored = remove_file(&cache_path);
        let cell_size = std::mem::size_of::<EntryType>() as u64;
        let mut m1 = Measure::start("create save");
        let entries = data
            .iter()
            .map(|x: &Vec<EntryType>| x.len())
            .collect::<Vec<_>>();
        let entries = entries.iter().sum::<usize>();
        let capacity = cell_size * (entries as u64) + std::mem::size_of::<Header>() as u64;

        let mmap = CacheHashDataFile::new_map(&cache_path, capacity)?;
        m1.stop();
        stats.create_save_us += m1.as_us();
        let mut cache_file = CacheHashDataFile {
            mmap,
            cell_size,
            capacity,
        };

        let mut header = cache_file.get_header_mut();
        header.count = entries;

        stats.cache_file_size = capacity as usize;
        stats.total_entries = entries;

        let mut m2 = Measure::start("write_to_mmap");
        let mut i = 0;
        data.iter().for_each(|x| {
            x.iter().for_each(|item| {
                let d = cache_file.get_mut(i as u64);
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
    use {super::*, rand::Rng};

    #[test]
    fn test_read_write() {
        // generate sample data
        // write to file
        // read
        // compare
        use tempfile::TempDir;
        let tmpdir = TempDir::new().unwrap();
        let cache_dir = tmpdir.path().to_path_buf();
        std::fs::create_dir_all(&cache_dir).unwrap();

        for bins in [1, 2, 4] {
            let bin_calculator = PubkeyBinCalculator24::new(bins);
            let num_points = 5;
            let (data, _total_points) = generate_test_data(num_points, bins, &bin_calculator);
            for passes in [1, 2] {
                let bins_per_pass = bins / passes;
                if bins_per_pass == 0 {
                    continue; // illegal test case
                }
                for pass in 0..passes {
                    for flatten_data in [true, false] {
                        let mut data_this_pass = if flatten_data {
                            vec![vec![], vec![]]
                        } else {
                            vec![]
                        };
                        let start_bin_this_pass = pass * bins_per_pass;
                        for bin in 0..bins_per_pass {
                            let mut this_bin_data = data[bin + start_bin_this_pass].clone();
                            if flatten_data {
                                data_this_pass[0].append(&mut this_bin_data);
                            } else {
                                data_this_pass.push(this_bin_data);
                            }
                        }
                        let cache = CacheHashData::new(cache_dir.clone());
                        let file_name = PathBuf::from("test");
                        cache.save(&file_name, &data_this_pass).unwrap();
                        cache.get_cache_files();
                        assert_eq!(
                            cache
                                .pre_existing_cache_files
                                .lock()
                                .unwrap()
                                .iter()
                                .collect::<Vec<_>>(),
                            vec![&file_name],
                        );
                        let mut accum = (0..bins_per_pass).map(|_| vec![]).collect();
                        cache
                            .load(&file_name, &mut accum, start_bin_this_pass, &bin_calculator)
                            .unwrap();
                        if flatten_data {
                            bin_data(
                                &mut data_this_pass,
                                &bin_calculator,
                                bins_per_pass,
                                start_bin_this_pass,
                            );
                        }
                        assert_eq!(
                            accum, data_this_pass,
                            "bins: {bins}, start_bin_this_pass: {start_bin_this_pass}, pass: {pass}, flatten: {flatten_data}, passes: {passes}"
                        );
                    }
                }
            }
        }
    }

    fn bin_data(
        data: &mut SavedType,
        bin_calculator: &PubkeyBinCalculator24,
        bins: usize,
        start_bin: usize,
    ) {
        let mut accum: SavedType = (0..bins).map(|_| vec![]).collect();
        data.drain(..).for_each(|mut x| {
            x.drain(..).for_each(|item| {
                let bin = bin_calculator.bin_from_pubkey(&item.pubkey);
                accum[bin - start_bin].push(item);
            })
        });
        *data = accum;
    }

    fn generate_test_data(
        count: usize,
        bins: usize,
        binner: &PubkeyBinCalculator24,
    ) -> (SavedType, usize) {
        let mut rng = rand::thread_rng();
        let mut ct = 0;
        (
            (0..bins)
                .map(|bin| {
                    let rnd = rng.gen::<u64>() % (bins as u64);
                    if rnd < count as u64 {
                        (0..std::cmp::max(1, count / bins))
                            .map(|_| {
                                ct += 1;
                                let mut pk;
                                loop {
                                    // expensive, but small numbers and for tests, so ok
                                    pk = solana_sdk::pubkey::new_rand();
                                    if binner.bin_from_pubkey(&pk) == bin {
                                        break;
                                    }
                                }

                                CalculateHashIntermediate::new(
                                    solana_sdk::hash::new_rand(&mut rng),
                                    ct as u64,
                                    pk,
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
}
