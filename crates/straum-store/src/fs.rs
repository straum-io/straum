//! Every file system call and clock read of the store goes through here.
//! Default: compio and `Instant`. With the `sim` feature: the in-memory
//! simulated disk and clock of `crate::sim`, selected at compile time, so
//! the default build has no indirection.

#[cfg(feature = "sim")]
pub use crate::sim::fs::*;
#[cfg(not(feature = "sim"))]
pub use real::*;

#[cfg(not(feature = "sim"))]
mod real {
    use std::ffi::OsString;
    use std::io;
    use std::path::Path;
    use std::time::Instant;

    use compio::BufResult;
    use compio::buf::{IntoInner, IoBuf};
    use compio::io::{AsyncReadAtExt, AsyncWriteAtExt};

    pub use compio::fs::{File, OpenOptions, create_dir_all, metadata, read, remove_file, rename};

    /// Reads exactly `len` bytes at `pos`.
    pub async fn read_exact_at(file: &File, len: usize, pos: u64) -> (io::Result<()>, Vec<u8>) {
        // `slice` caps the read at `len` even if the allocation is larger.
        let BufResult(res, buf) = file
            .read_exact_at(Vec::with_capacity(len).slice(..len), pos)
            .await;
        (res, buf.into_inner())
    }

    pub async fn write_all(file: &File, buf: Vec<u8>, pos: u64) -> (io::Result<()>, Vec<u8>) {
        let mut file = file;
        let BufResult(res, buf) = file.write_all_at(buf, pos).await;
        (res, buf)
    }

    /// Names in `dir`. Blocking: only at open, and compio 0.19 has no async
    /// read_dir without the extra fs-dir feature.
    pub fn list(dir: &Path) -> io::Result<Vec<OsString>> {
        std::fs::read_dir(dir)?
            .map(|e| e.map(|e| e.file_name()))
            .collect()
    }

    pub fn create_dir(path: &Path) -> io::Result<()> {
        std::fs::create_dir(path)
    }

    pub fn is_dir(path: &Path) -> bool {
        path.is_dir()
    }

    /// As `is_dir`, not following a symlink (as `DirEntry::file_type`).
    pub fn is_dir_entry(path: &Path) -> bool {
        std::fs::symlink_metadata(path).is_ok_and(|m| m.is_dir())
    }

    pub fn exists(path: &Path) -> bool {
        path.exists()
    }

    /// Whether a directory entry `path` exists (a symlink is not followed).
    pub fn name_taken(path: &Path) -> bool {
        std::fs::symlink_metadata(path).is_ok()
    }

    pub fn now() -> Instant {
        Instant::now()
    }
}
