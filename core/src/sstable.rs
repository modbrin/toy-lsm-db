use crate::error::DBError;
use crate::memtable::MemTable;
use crate::utils::CommonBinaryFormat;
use itertools::Itertools;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;
use std::time::SystemTime;
use std::{io, mem};

static SSTABLE_FILE_EXT: &str = "sst";

struct SSTable {
    entries: Vec<SSTableEntry>,
    data_size: usize,
}

pub struct SSTableEntry {
    timestamp: u128,
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl From<MemTable> for SSTable {
    fn from(value: MemTable) -> Self {
        Self {
            entries: value
                .entries
                .into_iter()
                .map(|mte| SSTableEntry {
                    timestamp: mte.timestamp,
                    key: mte.key,
                    value: mte.value,
                })
                .collect(),
            data_size: value.data_size,
        }
    }
}

crate::impl_cbf_conversion!(SSTableEntry);

// impl SSTable {
//     pub fn write_memtable(self, dir: impl AsRef<Path>) -> io::Result<()> {
//         let timestamp = SystemTime::now()
//             .duration_since(SystemTime::UNIX_EPOCH)
//             .unwrap()
//             .as_micros();
//         let filepath = dir
//             .as_ref()
//             .to_path_buf()
//             .join(format!("{}.{}", timestamp, SSTABLE_FILE_EXT));
//         let file = File::options()
//             .create(true)
//             .write(true)
//             .truncate(true)
//             .open(dir)?;
//         let writer = BufWriter::new(file);
//
//         Ok(())
//     }
//
//     pub fn read_memtable(path: impl AsRef<Path>) -> io::Result<Self> {
//         todo!()
//     }
// }
