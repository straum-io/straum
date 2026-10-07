//! A disk in memory, for deterministic simulation (D-28, stage 1).
//!
//! Each file has two layers: what has been written to the operating system,
//! and what the last sync made durable. A process crash keeps the first —
//! the bytes are in the page cache, not in the process. A power loss keeps
//! only the second, plus, if asked, a torn prefix of what was written after
//! it. Names behave the same way: a rename is visible at once and durable
//! only once the directory is synced.
//!
//! Names follow POSIX strictly (D-30): syncing a file says nothing about its
//! name. A file created, synced and never followed by a directory sync is
//! gone after a power loss, whatever ext4 or xfs would do in practice.
//!
//! Nothing here reads the operating system's clock. Time is a counter that
//! the caller advances, and that writes and syncs advance by a latency the
//! caller sets.

use std::collections::HashMap;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use crate::disk::{Access, Disk, DiskFile};

/// A disk in memory. Cloning shares it: every clone is the same disk.
#[derive(Debug, Clone, Default)]
pub struct SimDisk {
    state: Arc<Mutex<State>>,
}

#[derive(Debug, Default)]
struct State {
    inodes: Vec<Inode>,
    /// What a lookup sees now.
    names: HashMap<PathBuf, usize>,
    /// What a lookup sees after a power loss.
    durable_names: HashMap<PathBuf, usize>,
    now: Duration,
    write_latency: Duration,
    sync_latency: Duration,
    /// Operations left before the next one of that kind fails.
    fail_write_in: Option<u64>,
    fail_sync_in: Option<u64>,
    writes: u64,
    syncs: u64,
}

#[derive(Debug, Default, Clone)]
struct Inode {
    written: Vec<u8>,
    synced: Vec<u8>,
}

/// An open file on a [`SimDisk`]. Like a descriptor, it names the file it
/// was opened on, not the path: a rename over it leaves it writing to a file
/// no name reaches any more.
#[derive(Debug)]
pub struct SimFile {
    disk: SimDisk,
    inode: usize,
    position: u64,
    writable: bool,
}

fn injected(what: &str) -> io::Error {
    io::Error::other(format!("{what} failed: injected by the simulation"))
}

impl SimDisk {
    pub fn new() -> Self {
        Self::default()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        // Nothing in here can panic half way through a mutation, so a
        // poisoned lock still guards a consistent state.
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Moves the clock forward.
    pub fn advance(&self, by: Duration) {
        let mut state = self.state();
        state.now = state.now.saturating_add(by);
    }

    /// How long each write and each sync takes from now on.
    pub fn set_latency(&self, write: Duration, sync: Duration) {
        let mut state = self.state();
        state.write_latency = write;
        state.sync_latency = sync;
    }

    /// The write after the next `after` writes fails, once.
    pub fn fail_write_after(&self, after: u64) {
        self.state().fail_write_in = Some(after);
    }

    /// The sync after the next `after` syncs fails, once.
    pub fn fail_sync_after(&self, after: u64) {
        self.state().fail_sync_in = Some(after);
    }

    /// Writes and syncs performed so far, for choosing an injection point.
    pub fn operations(&self) -> (u64, u64) {
        let state = self.state();
        (state.writes, state.syncs)
    }

    /// The machine loses power. Every file goes back to what its last sync
    /// covered, keeping at most `torn` bytes of what was written after it —
    /// the start of a write that was in flight. Every name goes back to its
    /// last durable entry. Files opened before stay open but are the
    /// caller's to drop: a process does not survive this.
    pub fn power_loss(&self, torn: usize) {
        let mut state = self.state();
        for inode in &mut state.inodes {
            let kept = if inode.written.starts_with(&inode.synced) {
                let extra = inode.written.len().saturating_sub(inode.synced.len());
                inode.synced.len().saturating_add(extra.min(torn))
            } else {
                inode.synced.len()
            };
            let mut survived = inode.synced.clone();
            if let Some(tail) = inode.written.get(inode.synced.len()..kept) {
                survived.extend_from_slice(tail);
            }
            inode.written = survived.clone();
            inode.synced = survived;
        }
        state.names = state.durable_names.clone();
    }

    /// What a path holds now, if anything. For assertions.
    pub fn contents(&self, path: &Path) -> Option<Vec<u8>> {
        let state = self.state();
        let inode = *state.names.get(path)?;
        state.inodes.get(inode).map(|held| held.written.clone())
    }
}

impl State {
    fn tick_write(&mut self) -> io::Result<()> {
        self.writes = self.writes.saturating_add(1);
        self.now = self.now.saturating_add(self.write_latency);
        match self.fail_write_in {
            Some(0) => {
                self.fail_write_in = None;
                Err(injected("write"))
            }
            Some(left) => {
                self.fail_write_in = Some(left.saturating_sub(1));
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn tick_sync(&mut self) -> io::Result<()> {
        self.syncs = self.syncs.saturating_add(1);
        self.now = self.now.saturating_add(self.sync_latency);
        match self.fail_sync_in {
            Some(0) => {
                self.fail_sync_in = None;
                Err(injected("sync"))
            }
            Some(left) => {
                self.fail_sync_in = Some(left.saturating_sub(1));
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn inode(&mut self, index: usize) -> io::Result<&mut Inode> {
        self.inodes
            .get_mut(index)
            .ok_or_else(|| io::Error::other("a simulated file that does not exist"))
    }

    fn sync(&mut self, index: usize) -> io::Result<()> {
        self.tick_sync()?;
        let inode = self.inode(index)?;
        inode.synced = inode.written.clone();
        Ok(())
    }
}

impl Disk for SimDisk {
    type File = SimFile;

    fn open(&self, path: &Path, access: Access) -> io::Result<SimFile> {
        let mut state = self.state();
        let found = state.names.get(path).copied();
        let inode = match (found, access) {
            (Some(inode), Access::Replace) => {
                state.inode(inode)?.written.clear();
                inode
            }
            (Some(inode), _) => inode,
            (None, Access::Read | Access::Write) => {
                return Err(io::Error::from(io::ErrorKind::NotFound));
            }
            (None, Access::Create | Access::Replace) => {
                state.inodes.push(Inode::default());
                let inode = state.inodes.len().saturating_sub(1);
                state.names.insert(path.to_path_buf(), inode);
                inode
            }
        };
        Ok(SimFile {
            disk: self.clone(),
            inode,
            position: 0,
            writable: access != Access::Read,
        })
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut state = self.state();
        let inode = state
            .names
            .remove(from)
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        state.names.insert(to.to_path_buf(), inode);
        Ok(())
    }

    fn remove(&self, path: &Path) -> io::Result<()> {
        self.state()
            .names
            .remove(path)
            .map(|_| ())
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))
    }

    fn list(&self, directory: &Path) -> io::Result<Vec<PathBuf>> {
        let mut paths: Vec<PathBuf> = self
            .state()
            .names
            .keys()
            .filter(|path| path.parent() == Some(directory))
            .cloned()
            .collect();
        paths.sort();
        Ok(paths)
    }

    fn sync_directory(&self, _directory: &Path) -> io::Result<()> {
        // One directory: everything the log touches lives beside it.
        let mut state = self.state();
        state.tick_sync()?;
        state.durable_names = state.names.clone();
        Ok(())
    }

    fn now(&self) -> Duration {
        self.state().now
    }
}

impl Read for SimFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut state = self.disk.state();
        let inode = state.inode(self.inode)?;
        let start = usize::try_from(self.position).unwrap_or(usize::MAX);
        let available = inode.written.get(start..).unwrap_or_default();
        let taken = available.len().min(buf.len());
        if let (Some(into), Some(from)) = (buf.get_mut(..taken), available.get(..taken)) {
            into.copy_from_slice(from);
        }
        self.position = self
            .position
            .saturating_add(u64::try_from(taken).unwrap_or(u64::MAX));
        Ok(taken)
    }
}

impl Write for SimFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if !self.writable {
            return Err(io::Error::from(io::ErrorKind::PermissionDenied));
        }
        let mut state = self.disk.state();
        state.tick_write()?;
        let inode = state.inode(self.inode)?;
        let start = usize::try_from(self.position).unwrap_or(usize::MAX);
        let end = start.saturating_add(buf.len());
        if inode.written.len() < end {
            inode.written.resize(end, 0);
        }
        if let Some(into) = inode.written.get_mut(start..end) {
            into.copy_from_slice(buf);
        }
        self.position = u64::try_from(end).unwrap_or(u64::MAX);
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for SimFile {
    fn seek(&mut self, to: SeekFrom) -> io::Result<u64> {
        let size = self.size()?;
        let position = match to {
            SeekFrom::Start(at) => Some(at),
            SeekFrom::End(by) => size.checked_add_signed(by),
            SeekFrom::Current(by) => self.position.checked_add_signed(by),
        }
        .ok_or_else(|| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.position = position;
        Ok(position)
    }
}

impl DiskFile for SimFile {
    fn size(&self) -> io::Result<u64> {
        let mut state = self.disk.state();
        let inode = state.inode(self.inode)?;
        Ok(u64::try_from(inode.written.len()).unwrap_or(u64::MAX))
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let mut state = self.disk.state();
        let inode = state.inode(self.inode)?;
        inode
            .written
            .resize(usize::try_from(len).unwrap_or(usize::MAX), 0);
        Ok(())
    }

    fn sync_data(&mut self) -> io::Result<()> {
        self.disk.state().sync(self.inode)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.disk.state().sync(self.inode)
    }
}
