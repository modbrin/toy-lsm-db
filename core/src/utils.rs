use std::path::{Path, PathBuf};
use std::time::SystemTime;
use std::{fs, io};

pub fn scan_dir(path: impl AsRef<Path>, exts: &[&str]) -> io::Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for file in fs::read_dir(path)? {
        let path = file?.path();
        if path
            .extension()
            .and_then(|ext| ext.to_str().map(|s| exts.contains(&s)))
            .unwrap_or(false)
        {
            out.push(path);
        }
    }
    Ok(out)
}

/// Common binary (de)serialization format used by wal and sstable
/// > timestamp (16 bytes) | tombstone (1 byte) | key size (4 or 8 bytes) | value size (4 or 8 bytes) | key | value
pub struct CommonBinaryFormat {
    pub timestamp: u128,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

pub struct CommonBinaryFormatRef<'a> {
    pub timestamp: u128,
    pub key: &'a [u8],
    pub value: Option<&'a [u8]>,
}

#[macro_export]
macro_rules! impl_cbf_conversion {
    ($this:ty, $other:ty) => {
        impl From<$other> for $this {
            fn from(value: $other) -> Self {
                Self {
                    timestamp: value.timestamp,
                    key: value.key,
                    value: value.value,
                }
            }
        }
    };
    ($other:ty) => {
        crate::impl_cbf_conversion!(CommonBinaryFormat, $other);
        crate::impl_cbf_conversion!($other, CommonBinaryFormat);
    };
}

impl CommonBinaryFormat {
    pub fn new(timestamp: u128, key: Vec<u8>, value: Option<Vec<u8>>) -> Self {
        Self {
            timestamp,
            key,
            value,
        }
    }

    pub fn as_cbf_ref(&self) -> CommonBinaryFormatRef {
        CommonBinaryFormatRef {
            timestamp: self.timestamp,
            key: &self.key,
            value: self.value.as_ref().map(|vec| vec.as_ref()),
        }
    }

    pub fn read(reader: &mut impl io::Read) -> io::Result<Self> {
        let mut timestamp = [0; 16];
        reader.read_exact(&mut timestamp)?;
        let timestamp = u128::from_le_bytes(timestamp);

        let mut tombstone = [0; 1];
        reader.read_exact(&mut tombstone)?;
        let is_delete = tombstone[0] != 0;

        let mut size_buffer = [0; 8];
        reader.read_exact(&mut size_buffer)?;
        let key_size = usize::from_le_bytes(size_buffer);

        let mut value_size = 0;
        if !is_delete {
            reader.read_exact(&mut size_buffer)?;
            value_size = usize::from_le_bytes(size_buffer);
        }

        let mut key = vec![0; key_size];
        reader.read_exact(&mut key)?;

        let mut value = None;
        if !is_delete {
            let mut value_data = vec![0; value_size];
            reader.read_exact(&mut value_data)?;
            value = Some(value_data);
        }
        Ok(Self {
            timestamp,
            key,
            value,
        })
    }
}

impl<'a> CommonBinaryFormatRef<'a> {
    pub fn new(timestamp: u128, key: &'a [u8], value: Option<&'a [u8]>) -> Self {
        Self {
            timestamp,
            key,
            value,
        }
    }

    pub fn write(self, writer: &mut impl io::Write) -> io::Result<()> {
        writer.write_all(&self.timestamp.to_le_bytes())?;
        writer.write_all(&[if self.value.is_some() { 0 } else { 1 }])?;
        writer.write_all(&self.key.len().to_le_bytes())?;
        if let Some(value) = &self.value {
            writer.write_all(&value.len().to_le_bytes())?;
        }
        writer.write_all(self.key)?;
        if let Some(value) = self.value {
            writer.write_all(value)?;
        }
        Ok(())
    }
}

pub fn timestamp_now() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
}
