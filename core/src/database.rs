use crate::memtable::MemTable;
use crate::utils::{timestamp_now, CommonBinaryFormatRef};
use crate::wal::WriteAheadLog;
use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::{fs, mem};
use crate::sstable::SstMetadata;
use crate::utils;

pub struct Database {
    /// write-ahead log for data loss prevention
    wal: WriteAheadLog,
    /// read-write memtable
    rw_memtable: MemTable,
    /// read-only memtable
    ro_memtable: MemTable,
    /// level num -> sorted vec of filepaths
    on_disk_levels: Vec<Vec<PathBuf>>,
    /// configuration
    options: DatabaseOptions,
}

#[derive(Default, Clone, Debug)]
pub struct DatabaseOptions {
    /// path where all the db files will be stored
    working_dir: PathBuf,
    /// size in bytes to store memtable on disk
    memtable_threshold: usize,
    /// limit of memtables count on level 0
    level_zero_memtables_limit: usize,
    /// number of levels
    level_num: usize,
    /// factor of count threshold between levels
    level_factor: usize,
}

impl DatabaseOptions {
    pub fn new() -> Self {
        Self {
            working_dir: PathBuf::from("."),
            memtable_threshold: 67_108_864, // 64 MB
            level_zero_memtables_limit: 8,
            level_num: 7,
            level_factor: 10,
        }
    }

    pub fn set_working_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.working_dir = dir.as_ref().to_path_buf();
        self
    }

    pub fn set_memtable_threshold(mut self, threshold: usize) -> Self {
        self.memtable_threshold = threshold;
        self
    }

    pub fn set_level_zero_memtables_limit(mut self, count: usize) -> Self {
        self.level_zero_memtables_limit = count;
        self
    }

    pub fn set_level_num(mut self, num: usize) -> Self {
        self.level_num = num;
        self
    }

    pub fn set_level_factor(mut self, factor: usize) -> Self {
        self.level_factor = factor;
        self
    }

    pub fn init(self) -> Result<Database> {
        Database::init(self)
    }
}

impl Database {
    pub fn options() -> DatabaseOptions {
        DatabaseOptions::new()
    }

    pub fn init(options: DatabaseOptions) -> Result<Self> {
        let (wal, rw_memtable) = WriteAheadLog::load_dir(&options.working_dir)?;
        let ro_memtable = MemTable::new(); // TODO: fill with latest sst?
        Ok(Self {
            wal,
            rw_memtable,
            ro_memtable,
            options,
            on_disk_levels: todo this,
        })
    }

    // TODO: async io, async swapping and compaction

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let timestamp = timestamp_now();
        self.wal.put(timestamp, &key, &value)?;
        self.rw_memtable.put(timestamp, key, value);

        if self.rw_memtable.data_size > self.options.memtable_threshold {
            self.swap_memtable()?;
        }

        Ok(())
    }

    pub fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let timestamp = timestamp_now();
        self.wal.delete(timestamp, &key)?;
        self.rw_memtable.delete(timestamp, key);

        if self.rw_memtable.data_size > self.options.memtable_threshold {
            self.swap_memtable()?;
        }

        Ok(())
    }

    pub fn query(&self, key: Vec<u8>) -> Result<Vec<u8>> {
        todo!()
    }

    /// Swapping logic:
    /// 1) rw memtable overflows
    /// 2) (async) ro memtable is sent to dumping queue, when dump is completed its wal file is deleted
    /// 3) ro memtable is replaced with current rw memtable, new wal is created for new rw memtable
    pub fn swap_memtable(&mut self) -> Result<()> {
        self.ro_memtable = MemTable::new();
        let old_wal_path = self.wal.path.clone();
        assert!(old_wal_path.exists());
        self.wal = WriteAheadLog::new(&self.options.working_dir)?;
        mem::swap(&mut self.rw_memtable, &mut self.ro_memtable);

        let timestamp = timestamp_now();
        let level = 0;
        let save_path = self.options.working_dir.join(format!("{timestamp}.sst"));
        assert!(!save_path.exists(), "trying to create sst file that already exists");
        let mut out_file = File::options().write(true).create(true).open(save_path)?;
        for entry in self.ro_memtable.entries.iter() {
            CommonBinaryFormatRef::new(
                entry.timestamp,
                &entry.key,
                entry.value.as_ref().map(|vec| vec.as_ref()),
            )
            .write(&mut out_file)?;
        }
        fs::remove_file(old_wal_path)?;
        Ok(())
    }

    fn find_existing_ssts(&mut self, working_dir: impl AsRef<Path>) -> Result<Vec<(PathBuf, SstMetadata)>> {
        let mut found = Vec::new();
        for file in utils::scan_dir(working_dir.as_ref(), &["sst"])? {
            let mut reader = BufReader::new(File::open(&file)?);
            let meta = SstMetadata::read(reader)?;
            found.push((file, meta));
        }
        Ok(found)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn swapping_memtable_works() {
        let test_dir = &PathBuf::from("./tests/swapping_memtable_works");
        if test_dir.exists() {
            fs::remove_dir_all(test_dir).unwrap();
        }

        let options = Database::options().set_working_dir(test_dir).set_memtable_threshold(256);
        let mut db = options.init().expect("failed to init db");

        db.put(b"key1".to_vec(), vec![1;150]).unwrap();
        db.put(b"key2".to_vec(), vec![2;150]).unwrap();
    }
}
