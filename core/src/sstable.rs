use std::{io, mem};

pub struct SstMetadata {
    /// level in sst hierarchy
    level: usize,
    /// offset from file start in bytes to lookup table
    lookup_table_offset: usize,
    /// offset from file start in bytes to values table
    values_table_offset: usize,
    // /// bloom filter to optimize redundant search in keys
    // bloom_filter: ???
    /// lowest key in table
    low_key: Vec<u8>,
    /// highest key in table
    high_key: Vec<u8>,
}

impl SstMetadata {
    pub fn write(&self, mut writer: impl io::Write) -> io::Result<()> {
        writer.write_all(&self.level.to_le_bytes())?;
        writer.write_all(&self.lookup_table_offset.to_le_bytes())?;
        writer.write_all(&self.values_table_offset.to_le_bytes())?;
        writer.write_all(&self.low_key.len().to_le_bytes())?;
        writer.write_all(&self.low_key)?;
        writer.write_all(&self.high_key.len().to_le_bytes())?;
        writer.write_all(&self.high_key)?;
        Ok(())
    }

    pub fn read(mut reader: impl io::Read) -> io::Result<Self> {
        let mut usize_buf = [0; mem::size_of::<usize>()];
        reader.read_exact(&mut usize_buf)?;
        let level = usize::from_le_bytes(usize_buf);

        reader.read_exact(&mut usize_buf)?;
        let lookup_table_offset = usize::from_le_bytes(usize_buf);

        reader.read_exact(&mut usize_buf)?;
        let values_table_offset = usize::from_le_bytes(usize_buf);

        reader.read_exact(&mut usize_buf)?;
        let low_key_size = usize::from_le_bytes(usize_buf);
        let mut low_key = vec![0; low_key_size];
        reader.read_exact(&mut low_key)?;

        reader.read_exact(&mut usize_buf)?;
        let high_key_size = usize::from_le_bytes(usize_buf);
        let mut high_key = vec![0; high_key_size];
        reader.read_exact(&mut high_key)?;

        let meta = Self {
            level,
            lookup_table_offset,
            values_table_offset,
            low_key,
            high_key,
        };
        Ok(meta)
    }
}

pub struct SstLookupTable {
    // sorted vec of entries (key -> value offset)
    entries: Vec<(Vec<u8>, usize)>,
}

pub struct SstValuesTable {
    entries: Vec<Vec<u8>>,
}
