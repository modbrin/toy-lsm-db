use std::mem;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTable {
    entries: Vec<MemTableEntry>, //TODO: replace with skip list
    data_size: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    /// None if corresponds to delete
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            data_size: 0,
        }
    }

    // returns Ok() with found index, Err() with index for insert
    pub fn get_index(&self, key: impl AsRef<[u8]>) -> Result<usize, usize> {
        self.entries
            .binary_search_by_key(&key.as_ref(), |e| e.key.as_slice())
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, timestamp: u128) {
        match self.get_index(&key) {
            Ok(idx) => {
                let elem = &mut self.entries[idx];
                if let Some(current_value) = elem.value.as_ref() {
                    if current_value.len() < value.len() {
                        self.data_size += value.len() - current_value.len();
                    } else {
                        self.data_size -= current_value.len() - value.len();
                    }
                }
                elem.value = Some(value);
                elem.timestamp = timestamp;
            }
            Err(idx) => {
                self.data_size += key.len() + value.len() + mem::size_of::<MemTableEntry>();
                let entry = MemTableEntry {
                    key,
                    value: Some(value),
                    timestamp,
                };
                self.entries.insert(idx, entry);
            }
        }
    }

    pub fn delete(&mut self, key: Vec<u8>, timestamp: u128) {
        match self.get_index(&key) {
            Ok(idx) => {
                let elem = &mut self.entries[idx];
                if let Some(value) = elem.value.as_ref() {
                    self.data_size -= value.len();
                }
                elem.value = None;
                elem.timestamp = timestamp;
            }
            Err(idx) => {
                self.data_size += key.len() + mem::size_of::<MemTableEntry>();
                let entry = MemTableEntry {
                    key,
                    value: None,
                    timestamp,
                };
                self.entries.insert(idx, entry);
            }
        }
    }

    pub fn get(&self, key: impl AsRef<[u8]>) -> Option<&MemTableEntry> {
        self.get_index(key.as_ref())
            .ok()
            .map(|idx| &self.entries[idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_get_remove_get() {
        let mut memtable = MemTable::new();
        assert_eq!(memtable.get(vec![1, 1, 1]), None);

        memtable.put(vec![1, 1, 1], vec![0, 0, 0], 1);
        assert_eq!(memtable.data_size, 70);
        assert_eq!(
            memtable.get(vec![1, 1, 1]),
            Some(&MemTableEntry {
                key: vec![1, 1, 1],
                value: Some(vec![0, 0, 0]),
                timestamp: 1,
            })
        );

        memtable.put(vec![3, 3, 3], vec![0, 1, 0, 1], 2);
        assert_eq!(memtable.data_size, 141);
        assert_eq!(
            memtable.get(vec![3, 3, 3]),
            Some(&MemTableEntry {
                key: vec![3, 3, 3],
                value: Some(vec![0, 1, 0, 1]),
                timestamp: 2,
            })
        );

        memtable.put(vec![2, 2, 2], vec![1, 0, 1, 0, 1], 3);
        assert_eq!(memtable.data_size, 213);
        assert_eq!(
            memtable.get(vec![2, 2, 2]),
            Some(&MemTableEntry {
                key: vec![2, 2, 2],
                value: Some(vec![1, 0, 1, 0, 1]),
                timestamp: 3,
            })
        );

        memtable.delete(vec![2, 2, 2], 4);
        assert_eq!(memtable.data_size, 208);
        assert_eq!(
            memtable.get(vec![2, 2, 2]),
            Some(&MemTableEntry {
                key: vec![2, 2, 2],
                value: None,
                timestamp: 4,
            })
        );

        memtable.delete(vec![1, 1, 1], 5);
        assert_eq!(memtable.data_size, 205);
        assert_eq!(
            memtable.get(vec![1, 1, 1]),
            Some(&MemTableEntry {
                key: vec![1, 1, 1],
                value: None,
                timestamp: 5,
            })
        );

        memtable.delete(vec![3, 3, 3], 6);
        assert_eq!(memtable.data_size, 201);
        assert_eq!(
            memtable.get(vec![3, 3, 3]),
            Some(&MemTableEntry {
                key: vec![3, 3, 3],
                value: None,
                timestamp: 6,
            })
        );
    }
}
