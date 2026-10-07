//! The boundary between the log and the machine it runs on: files, the
//! directory they live in, and the clock the batching reads (D-28, stage 1).
//!
//! Two implementations, both real: [`OsDisk`] is `std::fs` and the operating
//! system's monotonic clock, and is what every production path uses;
//! [`crate::simulated::SimDisk`] is a disk in memory with a written layer, a
//! synced layer and a clock of its own, for deterministic simulation. The log
//! is generic over the boundary rather than holding a trait object: the write
//! path is the hot path, and static dispatch costs it nothing.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

/// How a file is opened. The four ways the log actually opens one, and no
/// others.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Read only. A missing file is `NotFound`.
    Read,
    /// Read and write an existing file. A missing file is `NotFound`.
    Write,
    /// Read and write, creating the file if it is missing and keeping what
    /// is there if it is not.
    Create,
    /// Read and write, creating the file or emptying an existing one.
    Replace,
}

/// One open file.
pub trait DiskFile: Read + Write + Seek {
    /// The file's length in bytes, as written — not as synced.
    fn size(&self) -> io::Result<u64>;
    fn set_len(&mut self, len: u64) -> io::Result<()>;
    /// Makes the file's contents durable.
    fn sync_data(&mut self) -> io::Result<()>;
    /// Makes the file's contents and metadata durable.
    fn sync_all(&mut self) -> io::Result<()>;
}

/// Files, their directory, and a monotonic clock.
pub trait Disk {
    type File: DiskFile;

    fn open(&self, path: &Path, access: Access) -> io::Result<Self::File>;
    /// Replaces `to` with `from`. Durable only once the directory is synced.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove(&self, path: &Path) -> io::Result<()>;
    /// The files in `directory`, sorted by name. A segmented log finds its
    /// segments this way when it recovers.
    fn list(&self, directory: &Path) -> io::Result<Vec<PathBuf>>;
    /// Makes the entries of `directory` durable, a rename among them.
    fn sync_directory(&self, directory: &Path) -> io::Result<()>;
    /// Monotonic time since an arbitrary origin. Storage latency is the
    /// signal the batching adapts to (docs/storage.md §95), so the log reads
    /// the clock through the same boundary as the device it is timing: the
    /// operating system's clock here, simulated time in a simulation.
    fn now(&self) -> Duration;
}

/// The operating system: `std::fs` and `Instant`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OsDisk;

impl DiskFile for File {
    fn size(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        File::set_len(self, len)
    }

    fn sync_data(&mut self) -> io::Result<()> {
        File::sync_data(self)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        File::sync_all(self)
    }
}

impl Disk for OsDisk {
    type File = File;

    fn open(&self, path: &Path, access: Access) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true);
        match access {
            Access::Read => {}
            Access::Write => {
                options.write(true);
            }
            Access::Create => {
                options.write(true).create(true).truncate(false);
            }
            Access::Replace => {
                options.write(true).create(true).truncate(true);
            }
        }
        options.open(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn list(&self, directory: &Path) -> io::Result<Vec<PathBuf>> {
        let mut paths = Vec::new();
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_file() {
                paths.push(entry.path());
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn sync_directory(&self, directory: &Path) -> io::Result<()> {
        File::open(directory)?.sync_all()
    }

    fn now(&self) -> Duration {
        // An origin fixed at first use: only differences are ever taken.
        static ORIGIN: OnceLock<Instant> = OnceLock::new();
        ORIGIN.get_or_init(Instant::now).elapsed()
    }
}
