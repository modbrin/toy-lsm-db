use crate::memtable::MemTable;
use crate::utils;
use itertools::Itertools;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{fs, io};

pub struct WriteAheadLog {
    pub target: BufWriter<File>,
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(dir: impl AsRef<Path>) -> io::Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        fs::create_dir_all(&dir)?;
        let path = dir
            .as_ref()
            .to_path_buf()
            .join(timestamp.to_string())
            .with_extension("wal");
        let file = File::options().append(true).create(true).open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            target: writer,
            path,
        })
    }

    pub fn load(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::options().append(true).open(&path)?;
        let writer = BufWriter::new(file);
        Ok(Self {
            target: writer,
            path,
        })
    }

    pub fn load_dir(path: impl AsRef<Path>) -> io::Result<(Self, MemTable)> {
        let mut memtable = MemTable::new();
        let mut new_wal = WriteAheadLog::new(&path)?;

        let mut remove_files = Vec::new();

        for path in utils::scan_dir(path, &["wal"])?.into_iter().sorted() {
            for elem in Self::load(&path)?.into_iter()? {
                if let Some(value) = elem.value {
                    new_wal.put(elem.key.clone(), value.clone(), elem.timestamp)?;
                    memtable.put(elem.key, value, elem.timestamp)
                } else {
                    new_wal.delete(elem.key.clone(), elem.timestamp)?;
                    memtable.delete(elem.key, elem.timestamp);
                }
            }
            remove_files.push(path);
        }
        new_wal.flush()?;
        for path in remove_files {
            fs::remove_file(path)?;
        }

        Ok((new_wal, memtable))
    }

    pub fn put(
        &mut self,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        timestamp: u128,
    ) -> io::Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();
        self.target.write_all(&timestamp.to_le_bytes())?;
        self.target.write_all(&[0])?; // tombstone: false
        self.target.write_all(&key.len().to_le_bytes())?;
        self.target.write_all(&value.len().to_le_bytes())?;
        self.target.write_all(&key)?;
        self.target.write_all(&value)?;
        Ok(())
    }

    pub fn delete(&mut self, key: impl AsRef<[u8]>, timestamp: u128) -> io::Result<()> {
        let key = key.as_ref();
        self.target.write_all(&timestamp.to_le_bytes())?;
        self.target.write_all(&[1])?; // tombstone: true
        self.target.write_all(&key.len().to_le_bytes())?;
        self.target.write_all(&key)?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.target.flush()
    }

    pub fn into_iter(self) -> io::Result<impl Iterator<Item = WriteAheadLogEntry>> {
        drop(self.target);
        WriteAheadLogIterator::new(self.path)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteAheadLogEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
}

pub struct WriteAheadLogIterator {
    pub source: BufReader<File>,
}

impl WriteAheadLogIterator {
    pub fn new(path: impl AsRef<Path>) -> io::Result<Self> {
        let file = File::options().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(Self { source: reader })
    }
}

impl Iterator for WriteAheadLogIterator {
    type Item = WriteAheadLogEntry;

    fn next(&mut self) -> Option<WriteAheadLogEntry> {
        let mut timestamp = [0; 16];
        self.source.read_exact(&mut timestamp).ok()?;
        let timestamp = u128::from_le_bytes(timestamp);

        let mut tombstone = [0; 1];
        self.source.read_exact(&mut tombstone).ok()?;
        let is_delete = tombstone[0] != 0;

        let mut size_buffer = [0; 8];
        self.source.read_exact(&mut size_buffer).ok()?;
        let key_size = usize::from_le_bytes(size_buffer);

        let mut value_size = 0;
        if !is_delete {
            self.source.read_exact(&mut size_buffer).ok()?;
            value_size = usize::from_le_bytes(size_buffer);
        }

        let mut key = vec![0; key_size];
        self.source.read_exact(&mut key).ok()?;

        let mut value = None;
        if !is_delete {
            let mut value_data = vec![0; value_size];
            self.source.read_exact(&mut value_data).ok()?;
            value = Some(value_data);
        }
        let entry = WriteAheadLogEntry {
            key,
            value,
            timestamp,
        };
        Some(entry)
    }
}

#[cfg(test)]
mod tests {
    use crate::wal::{WriteAheadLog, WriteAheadLogEntry, WriteAheadLogIterator};
    use std::fs;

    #[test]
    fn load_cycle() {
        let test_dir = "./test_data";
        fs::remove_dir_all(test_dir).unwrap();
        let mut wal = WriteAheadLog::new(test_dir).unwrap();
        wal.put(vec![0, 0, 1], vec![2, 2], 1).unwrap();
        wal.put(vec![0, 1, 0], vec![3, 3, 3], 3).unwrap();
        wal.put(vec![0, 1, 1], vec![4, 4, 4, 4], 4).unwrap();
        wal.put(vec![1, 0, 0], vec![5, 5, 5, 5, 5], 10).unwrap();
        wal.delete(vec![0, 1, 1], 11).unwrap();
        wal.delete(vec![0, 1, 0], 25).unwrap();
        wal.put(vec![0, 1, 1], vec![2, 1, 2], 26).unwrap();
        wal.delete(vec![0, 1, 1], 30).unwrap();
        wal.flush().unwrap();
        let path = wal.path.clone();
        drop(wal);

        let wal = WriteAheadLog::load(path).unwrap();
        let elems: Vec<_> = wal.into_iter().unwrap().collect();
        assert_eq!(
            vec![
                WriteAheadLogEntry {
                    key: vec![0, 0, 1],
                    value: Some(vec![2, 2]),
                    timestamp: 1,
                },
                WriteAheadLogEntry {
                    key: vec![0, 1, 0],
                    value: Some(vec![3, 3, 3]),
                    timestamp: 3,
                },
                WriteAheadLogEntry {
                    key: vec![0, 1, 1],
                    value: Some(vec![4, 4, 4, 4]),
                    timestamp: 4,
                },
                WriteAheadLogEntry {
                    key: vec![1, 0, 0],
                    value: Some(vec![5, 5, 5, 5, 5]),
                    timestamp: 10,
                },
                WriteAheadLogEntry {
                    key: vec![0, 1, 1],
                    value: None,
                    timestamp: 11,
                },
                WriteAheadLogEntry {
                    key: vec![0, 1, 0],
                    value: None,
                    timestamp: 25,
                },
                WriteAheadLogEntry {
                    key: vec![0, 1, 1],
                    value: Some(vec![2, 1, 2]),
                    timestamp: 26,
                },
                WriteAheadLogEntry {
                    key: vec![0, 1, 1],
                    value: None,
                    timestamp: 30,
                },
            ],
            elems
        );
    }
}
