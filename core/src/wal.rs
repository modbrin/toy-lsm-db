use crate::memtable::MemTable;
use crate::utils::{timestamp_now, CommonBinaryFormat, CommonBinaryFormatRef};
use crate::{impl_cbf_conversion, utils};
use itertools::Itertools;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{fs, io};

pub struct WriteAheadLog {
    pub target: BufWriter<File>,
    pub path: PathBuf,
}

impl WriteAheadLog {
    pub fn new(dir: impl AsRef<Path>) -> io::Result<Self> {
        let timestamp = timestamp_now();
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

    pub fn load_dir(dir: impl AsRef<Path>) -> io::Result<(Self, MemTable)> {
        let mut memtable = MemTable::new();
        let mut new_wal = WriteAheadLog::new(&dir)?;

        let mut remove_files = Vec::new();

        for path in utils::scan_dir(dir, &["wal"])?.into_iter().sorted() {
            for elem in Self::load(&path)?.into_iter()? {
                if let Some(value) = elem.value {
                    new_wal.put(elem.timestamp, &elem.key, &value)?;
                    memtable.put(elem.timestamp, elem.key, value)
                } else {
                    new_wal.delete(elem.timestamp, &elem.key)?;
                    memtable.delete(elem.timestamp, elem.key);
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
        timestamp: u128,
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
    ) -> io::Result<()> {
        CommonBinaryFormatRef::new(timestamp, key.as_ref(), Some(value.as_ref()))
            .write(&mut self.target)?;
        Ok(())
    }

    pub fn delete(&mut self, timestamp: u128, key: &[u8]) -> io::Result<()> {
        CommonBinaryFormatRef::new(timestamp, key, None).write(&mut self.target)?;
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

impl_cbf_conversion!(WriteAheadLogEntry);

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
        let cbf = CommonBinaryFormat::read(&mut self.source).ok()?;
        let entry = WriteAheadLogEntry {
            key: cbf.key,
            value: cbf.value,
            timestamp: cbf.timestamp,
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
        wal.put(1, vec![0, 0, 1], vec![2, 2]).unwrap();
        wal.put(3, vec![0, 1, 0], vec![3, 3, 3]).unwrap();
        wal.put(4, vec![0, 1, 1], vec![4, 4, 4, 4]).unwrap();
        wal.put(10, vec![1, 0, 0], vec![5, 5, 5, 5, 5]).unwrap();
        wal.delete(11, &vec![0, 1, 1]).unwrap();
        wal.delete(25, &vec![0, 1, 0]).unwrap();
        wal.put(26, vec![0, 1, 1], vec![2, 1, 2]).unwrap();
        wal.delete(30, &vec![0, 1, 1]).unwrap();
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
