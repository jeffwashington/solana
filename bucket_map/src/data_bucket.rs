use log::*;
use memmap2::MmapMut;
use rand::{thread_rng, Rng};
use std::fs::{remove_file, OpenOptions};
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Mutex, atomic::{AtomicU64, Ordering}};
use std::sync::Arc;
use solana_measure::measure::Measure;

#[derive(Debug, Default)]
pub struct BucketStats {
    pub resizes: AtomicU64,
    pub max_size: Mutex<u64>,
    pub resize_us: AtomicU64,
    pub new_file_us: AtomicU64,
    pub flush_file_us: AtomicU64,
    pub mmap_us: AtomicU64,
}

#[derive(Debug, Default, Clone)]
pub struct BucketMapStats {
    pub index: Arc<BucketStats>,
    pub data: Arc<BucketStats>,
}

/*
1	2
2	4
3	8
4	16
5	32
6	64
7	128
8	256
9	512
10	1024
11	2048
12	4096
13	8192
14	16384
*/
// 23: 8,388,608
// 24; // 16,777,216
const DEFAULT_CAPACITY: u8 = 14;

#[repr(C)]
struct Header {
    lock: AtomicU64,
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

pub struct DataBucket {
    drives: Arc<Vec<PathBuf>>,
    path: PathBuf,
    mmap: MmapMut,
    pub cell_size: u64,
    //power of 2
    pub capacity: u8,
    pub bytes: u64,
    pub used: AtomicU64,
    pub stats: Arc<BucketStats>,
}

#[derive(Debug)]
pub enum DataBucketError {
    AlreadyAllocated,
    InvalidFree,
}

impl Drop for DataBucket {
    fn drop(&mut self) {
        if let Err(e) = remove_file(&self.path) {
            error!("failed to remove {:?}: {:?}", &self.path, e);
        }
    }
}

impl DataBucket {
    pub fn new_with_capacity(
        drives: Arc<Vec<PathBuf>>,
        num_elems: u64,
        elem_size: u64,
        capacity: u8,
        mut stats: Arc<BucketStats>,
    ) -> Self {
        let cell_size = elem_size * num_elems + std::mem::size_of::<Header>() as u64;
        let (mmap, path) = Self::new_map(&drives, cell_size as usize, capacity, &mut stats);
        Self {
            path,
            mmap,
            drives,
            cell_size,
            used: AtomicU64::new(0),
            capacity,
            stats,
            bytes: 1 << capacity,
        }
    }

    pub fn new(drives: Arc<Vec<PathBuf>>, num_elems: u64, elem_size: u64, stats: Arc<BucketStats>) -> Self {
        Self::new_with_capacity(drives, num_elems, elem_size, DEFAULT_CAPACITY, stats)
    }

    pub fn uid(&self, ix: u64) -> u64 {
        if ix >= self.num_cells() {
            panic!("bad index size");
        }
        let ix = (ix * self.cell_size) as usize;
        let hdr_slice: &[u8] = &self.mmap[ix..ix + std::mem::size_of::<Header>()];
        unsafe {
            let hdr = hdr_slice.as_ptr() as *const Header;
            return hdr.as_ref().unwrap().uid();
        }
    }

    pub fn allocate(&self, ix: u64, uid: u64) -> Result<(), DataBucketError> {
        if ix >= self.num_cells() {
            panic!("allocate: bad index size");
        }
        if 0 == uid {
            panic!("allocate: bad uid");
        }
        let mut e = Err(DataBucketError::AlreadyAllocated);
        let ix = (ix * self.cell_size) as usize;
        //debug!("ALLOC {} {}", ix, uid);
        let hdr_slice: &[u8] = &self.mmap[ix..ix + std::mem::size_of::<Header>()];
        unsafe {
            let hdr = hdr_slice.as_ptr() as *const Header;
            if hdr.as_ref().unwrap().try_lock(uid) {
                e = Ok(());
                self.used.fetch_add(1, Ordering::Relaxed);
            }
        };
        e
    }

    pub fn free(&self, ix: u64, uid: u64) -> Result<(), DataBucketError> {
        if ix >= self.num_cells() {
            panic!("free: bad index size");
        }
        if 0 == uid {
            panic!("free: bad uid");
        }
        let ix = (ix * self.cell_size) as usize;
        //debug!("FREE {} {}", ix, uid);
        let hdr_slice: &[u8] = &self.mmap[ix..ix + std::mem::size_of::<Header>()];
        let mut e = Err(DataBucketError::InvalidFree);
        unsafe {
            let hdr = hdr_slice.as_ptr() as *const Header;
            //debug!("FREE uid: {}", hdr.as_ref().unwrap().uid());
            if hdr.as_ref().unwrap().unlock(uid) {
                self.used.fetch_sub(1, Ordering::Relaxed);
                e = Ok(());
            }
        };
        e
    }

    pub fn get<T: Sized>(&self, ix: u64) -> &T {
        if ix >= self.num_cells() {
            panic!("bad index size");
        }
        let start = (ix * self.cell_size) as usize + std::mem::size_of::<Header>();
        let end = start + std::mem::size_of::<T>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *const T;
            &*item
        }
    }

    pub fn get_empty_cell_slice<T: Sized>(&self) -> &[T] {
        let len = 0;
        let item_slice: &[u8] = &self.mmap[0..0];
        unsafe {
            let item = item_slice.as_ptr() as *const T;
            std::slice::from_raw_parts(item, len as usize)
        }
    }

    pub fn get_cell_slice<T: Sized>(&self, ix: u64, len: u64) -> &[T] {
        if ix >= self.num_cells() {
            panic!("bad index size");
        }
        let ix = self.cell_size * ix;
        let start = ix as usize + std::mem::size_of::<Header>();
        let end = start + std::mem::size_of::<T>() * len as usize;
        //debug!("GET slice {} {}", start, end);
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *const T;
            std::slice::from_raw_parts(item, len as usize)
        }
    }

    pub fn get_mut<T: Sized>(&self, ix: u64) -> &mut T {
        if ix >= self.num_cells() {
            panic!("bad index size");
        }
        let start = (ix * self.cell_size) as usize + std::mem::size_of::<Header>();
        let end = start + std::mem::size_of::<T>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut T;
            &mut *item
        }
    }

    pub fn get_mut_cell_slice<T: Sized>(&self, ix: u64, len: u64) -> &mut [T] {
        if ix >= self.num_cells() {
            panic!("bad index size");
        }
        let ix = self.cell_size * ix;
        let start = ix as usize + std::mem::size_of::<Header>();
        let end = start + std::mem::size_of::<T>() * len as usize;
        //debug!("GET mut slice {} {}", start, end);
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut T;
            std::slice::from_raw_parts_mut(item, len as usize)
        }
    }

    fn new_map(drives: &[PathBuf], cell_size: usize, capacity: u8, stats: &mut Arc<BucketStats>) -> (MmapMut, PathBuf) {
        let mut m0 = Measure::start("");
        let capacity = 1u64 << capacity;
        let r = thread_rng().gen_range(0, drives.len());
        let drive = &drives[r];
        let pos = format!("{}", thread_rng().gen_range(0, u128::MAX),);
        let file = drive.join(pos);
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
        m0.stop();
        let mut m1 = Measure::start("");
        data.flush().unwrap(); // can we skip this?
        m1.stop();
        let mut m2 = Measure::start("");
        let res = (unsafe { MmapMut::map_mut(&data).unwrap() }, file);
        m2.stop();
        stats.new_file_us.fetch_add(m0.as_us(), Ordering::Relaxed);
        stats.flush_file_us.fetch_add(m0.as_us(), Ordering::Relaxed);
        stats.mmap_us.fetch_add(m0.as_us(), Ordering::Relaxed);
        res
    }

    pub fn grow(&mut self) {
        let mut m = Measure::start("grow");
        let old_cap = self.num_cells();
        let old_map = &self.mmap;
        let old_file = self.path.clone();
        let (new_map, new_file) =
            Self::new_map(&self.drives, self.cell_size as usize, self.capacity + 1, &mut self.stats);
        (0..old_cap as usize).into_iter().for_each(|i| {
            let old_ix = i * self.cell_size as usize;
            let new_ix = old_ix * 2;
            let dst_slice: &[u8] = &new_map[new_ix..new_ix + self.cell_size as usize];
            let src_slice: &[u8] = &old_map[old_ix..old_ix + self.cell_size as usize];

            unsafe {
                let dst = dst_slice.as_ptr() as *mut u8;
                let src = src_slice.as_ptr() as *const u8;
                std::ptr::copy_nonoverlapping(src, dst, self.cell_size as usize);
            };
        });
        self.mmap = new_map;
        self.path = new_file;
        self.capacity = self.capacity + 1;
        self.bytes = 1 << self.capacity;
        remove_file(old_file).unwrap();
        m.stop();
        let sz = 1 << self.capacity;
        {
            let mut max = self.stats.max_size.lock().unwrap();
            *max = std::cmp::max(*max, sz);
        }
        self.stats.resizes.fetch_add(1, Ordering::Relaxed);
        self.stats.resize_us.fetch_add(m.as_us(), Ordering::Relaxed);

    }
    pub fn num_cells(&self) -> u64 {
        self.bytes
    }
}
