use {
    crate::{bucket_map::BucketMapConfig, MaxSearch},
    memmap2::MmapMut,
    std::{
        fmt::{Debug, Formatter},
        fs::{remove_file, OpenOptions},
        io::{Seek, SeekFrom, Write},
        path::Path,
        sync::{Arc, Mutex},
    },
};

const HEADER_VERSION: u64 = 1;
#[derive(Debug)]
#[repr(C)]
pub struct Header {
    /// version of this file. Differences here indicate the file is not usable.
    version: u64,
    /// number of buckets these files represent.
    buckets: usize,
    /// u8 representing how many entries to search for during collisions.
    /// If this is different, then the contents of the index file's contents are likely not ideally helpful.
    max_search: usize,
}

#[derive(Debug)]
#[repr(C)]
pub struct OneIndexBucket {
    /// disk bucket file names are random u128s
    file_name: u128,
    /// each bucket stores a unique random value
    random: u64,
}

pub struct Restart {
    mmap: MmapMut,
}

#[derive(Clone)]
pub struct RestartableBucket {
    pub restart: Option<Arc<Mutex<Restart>>>,
    pub index: usize,
}

impl RestartableBucket {
    pub fn set_file(&self, file_name: u128, random: u64) {
        if let Some(mut restart) = self.restart.as_ref().map(|restart| restart.lock().unwrap()) {
            //log::error!("index: {}", self.index);
            let bucket = restart.get_bucket_mut(self.index);
            bucket.file_name = file_name;
            bucket.random = random;
        }
    }
    pub fn get(&self) -> Option<(u128, u64)> {
        self.restart.as_ref().map(|restart| {
            let restart = restart.lock().unwrap();
            let bucket = restart.get_bucket(self.index);
            (bucket.file_name, bucket.random)
        })
    }
}

impl Debug for RestartableBucket {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}",
            &self.restart.as_ref().map(|restart| restart.lock().unwrap())
        )?;
        Ok(())
    }
}

impl Debug for Restart {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let header = self.get_header();
        write!(f, "{:?}\n", header)?;
        write!(
            f,
            "{:?}\n",
            (0..header.buckets)
                .map(|index| self.get_bucket(index))
                .take(10)
                .collect::<Vec<_>>()
        )?;
        Ok(())
    }
}

impl Restart {
    pub fn get_restart_file(config: &BucketMapConfig, max_search: MaxSearch) -> Option<Restart> {
        log::error!("get_restart_file: {:?}", config.restart_config_file);
        let path = config.restart_config_file.as_ref()?;
        let metadata = std::fs::metadata(path).ok()?;
        let file_len = metadata.len();

        let expected_len = Self::expected_len(config.max_buckets);
        log::error!("found file: expected: {}, len: {}", expected_len, file_len);
        if expected_len as u64 != file_len {
            return None;
        }

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
            .ok()?;
        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };

        let mut restart = Restart { mmap };
        let header = restart.get_header_mut();
        if header.version != HEADER_VERSION
            || header.buckets != config.max_buckets
            || header.max_search != max_search as usize
        {
            return None;
        }

        log::error!("get_restart_file done: {:?}", config.restart_config_file);

        Some(restart)
    }

    fn expected_len(max_buckets: usize) -> usize {
        std::mem::size_of::<Header>() + max_buckets * std::mem::size_of::<OneIndexBucket>()
    }

    pub fn new_map(file: impl AsRef<Path>, capacity: u64) -> Result<MmapMut, std::io::Error> {
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

    pub fn new(config: &BucketMapConfig, max_search: MaxSearch) -> Option<Restart> {
        let expected_len = Self::expected_len(config.max_buckets);

        let path = config.restart_config_file.as_ref();
        if path.is_none() {
            log::error!("config path is none");
        }
        let path = path?;
        _ = remove_file(path);

        let mmap = Self::new_map(path, expected_len as u64);
        if mmap.is_err() {
            log::error!("error opening map: {:?}", mmap);
        }
        let mmap = mmap.ok()?;

        let mut restart = Restart { mmap };
        let header = restart.get_header_mut();
        header.version = HEADER_VERSION;
        header.buckets = config.max_buckets;
        header.max_search = max_search as usize;

        (0..config.max_buckets).for_each(|index| {
            let bucket = restart.get_bucket_mut(index);
            bucket.file_name = 0;
            bucket.random = 0;
        });
        log::error!("created file: {:?}", path);

        Some(restart)
    }

    fn get_header(&self) -> &Header {
        let start = 0_usize;
        let end = start + std::mem::size_of::<Header>();
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *const Header;
            &*item
        }
    }
    fn get_bucket(&self, index: usize) -> &OneIndexBucket {
        let record_len = std::mem::size_of::<OneIndexBucket>();
        let start = std::mem::size_of::<Header>() + record_len * index;
        let end = start + record_len;
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *const OneIndexBucket;
            &*item
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
    fn get_bucket_mut(&mut self, index: usize) -> &mut OneIndexBucket {
        let record_len = std::mem::size_of::<OneIndexBucket>();
        let start = std::mem::size_of::<Header>() + record_len * index;
        let end = start + record_len;
        let item_slice: &[u8] = &self.mmap[start..end];
        unsafe {
            let item = item_slice.as_ptr() as *mut OneIndexBucket;
            &mut *item
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    impl Default for RestartableBucket {
        fn default() -> Self {
            Self {
                index: 0,
                restart: None,
            }
        }
    }
}
