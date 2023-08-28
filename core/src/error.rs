use thiserror::Error;

#[derive(Error, Debug)]
pub enum DBError {
    #[error("sstable could not be loaded, data is corrupted")]
    MalformedSSTable,
}
