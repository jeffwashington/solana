//! Cached data for hashing accounts
use crate::accounts_hash::CalculateHashIntermediate;
use log::*;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    fs::{remove_file, OpenOptions},
    io::{Read, Write},
    ops::Range,
    path::{Path, PathBuf},
};

pub type SavedType = Vec<Vec<CalculateHashIntermediate>>;

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct CacheHashData {
    pub data: SavedType,
    pub storage_path: PathBuf,
    pub expected_mod_date: u8,
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
    pub fn load<P: AsRef<Path>>(
        storage_file: &P,
        bin_range: &Range<usize>,
    ) -> Result<SavedType, std::io::Error> {
        let create = false;
        let path = Self::calc_path(storage_file, bin_range)?;
        let mut file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(create)
            .open(&path)?;
        let mut file_data = Vec::new();
        let _bytes = file.read_to_end(&mut file_data)?;
        let decoded: Option<CacheHashData> = bincode::deserialize(&file_data[..]).unwrap();
        drop(file);
        Ok(decoded.unwrap().data)
    }
    pub fn save<P: AsRef<Path>>(
        storage_file: &P,
        data: &mut SavedType,
        bin_range: &Range<usize>,
    ) -> Result<(), std::io::Error> {
        let cache_path = Self::calc_path(storage_file, bin_range)?;
        error!("writing to: {:?}", cache_path);
        let parent = cache_path.parent().unwrap();
        std::fs::create_dir_all(parent);
        let create = true;
        if create {
            let _ignored = remove_file(&cache_path);
        }

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
        Ok(())
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
