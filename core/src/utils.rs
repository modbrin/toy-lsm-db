use std::path::{Path, PathBuf};
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
