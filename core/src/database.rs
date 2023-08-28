use crate::memtable::MemTable;
use crate::utils::{timestamp_now, CommonBinaryFormatRef};
use crate::wal::WriteAheadLog;
use anyhow::Result;
use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::{fs, mem};

pub struct Database {
    /// write-ahead log for data loss prevention
    wal: WriteAheadLog,
    /// read-write memtable
    rw_memtable: MemTable,
    /// read-only memtable
    ro_memtable: MemTable,
    /// configuration
    options: DatabaseOptions,
}

#[derive(Default, Clone, Debug)]
pub struct DatabaseOptions {
    /// path where all the db files will be stored
    working_dir: PathBuf,
    /// size in bytes to store memtable on disk
    memtable_threshold: usize,
}

impl Database {
    pub fn init(options: DatabaseOptions) -> Result<Self> {
        let (wal, rw_memtable) = WriteAheadLog::load_dir(&options.working_dir)?;
        let ro_memtable = MemTable::new();
        Ok(Self {
            wal,
            rw_memtable,
            ro_memtable,
            options,
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
    /// 2) (async) ro memtable is sent to dumping queue, when dump is completed it's wal file is deleted
    /// 3) ro memtable is replaced with current rw memtable, new wal is created for new rw memtable
    pub fn swap_memtable(&mut self) -> Result<()> {
        self.ro_memtable = MemTable::new();
        let old_wal_path = self.wal.path.clone();
        self.wal = WriteAheadLog::new(&self.options.working_dir)?;
        mem::swap(&mut self.rw_memtable, &mut self.ro_memtable);

        let timestamp = timestamp_now();
        let save_path = self.options.working_dir.join(format!("{timestamp}.sst"));
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
}
