//! A log made of segments (unified-stream §3, ADR-0008 step 2).
//!
//! Each segment is one [`Wal`] file: framing, checksums, group commit and
//! recovery are the single-file log's. What this adds is the shape the
//! replicated log needs: appends go to the last segment, a new segment starts
//! at a batch boundary once the last one is past `segment_bytes`, and space
//! is given back by removing whole segments from the front — never by
//! rewriting a file somebody may be reading.
//!
//! A segment is named by a key the caller gives with the first batch written
//! to it (the replicated log uses the batch's first lsn), zero-padded so the
//! names sort in log order. The log does not read record bodies; which key a
//! record has is the caller's knowledge, as with [`Wal`].
//!
//! A reader that is working through a segment pins it, and removal stops at
//! the first pinned segment: compaction during catch-up never takes the
//! segment being read (§3: "compaction must not truncate a segment somebody
//! reads; a refcount is needed").

use std::path::{Path, PathBuf};

use crate::disk::Disk;
use crate::wal::{Committed, Retain, Wal, replay_on};
use crate::{StorageError, WalLimits};

const EXTENSION: &str = "seg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentLimits {
    pub wal: WalLimits,
    /// A new segment starts at the next batch once the last one holds this
    /// many bytes. Not a hard cap: a batch is never split.
    pub segment_bytes: u64,
}

/// A reader's hold on one segment, given back with [`Segments::unpin`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pin(u64);

struct Active<D: Disk> {
    key: u64,
    wal: Wal<D>,
    /// Complete records in the file: recovered plus committed since.
    records: u64,
    bytes: u64,
}

pub struct Segments<D: Disk + Clone> {
    disk: D,
    directory: PathBuf,
    limits: SegmentLimits,
    /// Keys and sizes in bytes of the segments before the active one,
    /// oldest first.
    sealed: Vec<(u64, u64)>,
    active: Option<Active<D>>,
    staged_records: u64,
    /// Pinned segment keys, one entry per pin: the refcount is the number of
    /// entries with that key.
    pins: Vec<(Pin, u64)>,
    next_pin: u64,
}

fn segment_path(directory: &Path, key: u64) -> PathBuf {
    directory.join(format!("{key:020}.{EXTENSION}"))
}

fn segment_key(path: &Path) -> Option<u64> {
    if path.extension()? != EXTENSION {
        return None;
    }
    path.file_stem()?.to_str()?.parse().ok()
}

impl<D: Disk + Clone> Segments<D> {
    /// Opens the log in `directory`, handing every recovered record to
    /// `visit` in log order with the key of the segment it is in. Each
    /// segment's torn tail is cut as [`Wal::open_on`] cuts it; a torn batch
    /// is the caller's to find and to cut with [`Segments::truncate_tail`].
    pub fn open(
        disk: D,
        directory: &Path,
        limits: SegmentLimits,
        visit: &mut impl FnMut(u64, &[u8]),
    ) -> Result<Self, StorageError> {
        let mut keys: Vec<u64> = disk
            .list(directory)?
            .iter()
            .filter_map(|path| segment_key(path))
            .collect();
        keys.sort_unstable();
        let active_key = keys.pop();
        let mut sealed = Vec::with_capacity(keys.len());
        for &key in &keys {
            let recovery = replay_on(
                &disk,
                &segment_path(directory, key),
                limits.wal,
                &mut |record| visit(key, record),
            )?;
            sealed.push((key, recovery.durable_offset));
        }
        let active = match active_key {
            Some(key) => {
                let path = segment_path(directory, key);
                let (wal, recovery) = Wal::open_on(disk.clone(), &path, limits.wal)?;
                replay_on(&disk, &path, limits.wal, &mut |record| visit(key, record))?;
                Some(Active {
                    key,
                    wal,
                    records: recovery.records,
                    bytes: recovery.durable_offset,
                })
            }
            None => None,
        };
        Ok(Self {
            disk,
            directory: directory.to_path_buf(),
            limits,
            sealed,
            active,
            staged_records: 0,
            pins: Vec::new(),
            next_pin: 0,
        })
    }

    /// Starts a batch whose first record has `key`. If the last segment is
    /// full, or there is none, the batch opens a new segment named `key`.
    pub fn begin(&mut self, key: u64) -> Result<(), StorageError> {
        if self.staged_records != 0 {
            return Err(StorageError::StagedDuringCompaction);
        }
        let full = self
            .active
            .as_ref()
            .is_none_or(|active| active.bytes >= self.limits.segment_bytes && active.records != 0);
        if !full {
            return Ok(());
        }
        // A segment is sealed durable: otherwise a power loss could take its
        // unsynced tail and keep the next segment, a hole in the log.
        if let Some(active) = self.active.as_mut() {
            active.wal.commit_with(true)?;
        }
        let path = segment_path(&self.directory, key);
        let (wal, recovery) = Wal::open_on(self.disk.clone(), &path, self.limits.wal)?;
        if let Some(previous) = self.active.replace(Active {
            key,
            wal,
            records: recovery.records,
            bytes: recovery.durable_offset,
        }) {
            self.sealed.push((previous.key, previous.bytes));
        }
        Ok(())
    }

    /// Adds a record to the batch [`Segments::begin`] started.
    pub fn stage(&mut self, record: &[u8]) -> Result<(), StorageError> {
        let active = self.active.as_mut().ok_or(StorageError::NotFramedRecords)?;
        active.wal.stage(record)?;
        self.staged_records = self.staged_records.saturating_add(1);
        Ok(())
    }

    /// Writes the staged batch; with `sync`, waits for the device too.
    pub fn commit(&mut self, sync: bool) -> Result<Committed, StorageError> {
        let active = self.active.as_mut().ok_or(StorageError::NotFramedRecords)?;
        let (committed, _, _) = active.wal.commit_with(sync)?;
        active.records = active.records.saturating_add(self.staged_records);
        active.bytes = committed.durable_offset;
        self.staged_records = 0;
        Ok(committed)
    }

    /// Cuts the last `records` records of the last segment: a batch a crash
    /// left half written. Rewrites only that segment, the way
    /// [`Wal::compact`] rewrites a file (durable copy, then rename).
    pub fn truncate_tail(&mut self, records: u64) -> Result<(), StorageError> {
        if records == 0 {
            return Ok(());
        }
        let active = self.active.as_mut().ok_or(StorageError::NotFramedRecords)?;
        let keep = active.records.saturating_sub(records);
        let mut seen = 0u64;
        let compaction = active.wal.compact(0, Retain::NONE, &mut |_| {
            seen = seen.saturating_add(1);
            seen <= keep
        })?;
        active.records = compaction.records_after;
        active.bytes = compaction.bytes_after;
        Ok(())
    }

    /// Keys of every segment, oldest first, the active one last.
    pub fn keys(&self) -> impl Iterator<Item = u64> + '_ {
        self.sealed
            .iter()
            .map(|&(key, _)| key)
            .chain(self.active.as_ref().map(|active| active.key))
    }

    /// Every segment's key and size in bytes, oldest first: what a retention
    /// bound in bytes is measured against.
    pub fn sizes(&self) -> impl Iterator<Item = (u64, u64)> + '_ {
        self.sealed.iter().copied().chain(
            self.active
                .as_ref()
                .map(|active| (active.key, active.bytes)),
        )
    }

    /// The segment that holds `key`: the last one whose own key is not
    /// above it. `None` if `key` is before the first segment, removed.
    pub fn segment_of(&self, key: u64) -> Option<u64> {
        self.keys().take_while(|&first| first <= key).last()
    }

    /// Hands every record of segment `key`, as written, to `visit`.
    pub fn read(&self, key: u64, visit: &mut impl FnMut(&[u8])) -> Result<(), StorageError> {
        replay_on(
            &self.disk,
            &segment_path(&self.directory, key),
            self.limits.wal,
            visit,
        )?;
        Ok(())
    }

    pub fn pin(&mut self, key: u64) -> Pin {
        let pin = Pin(self.next_pin);
        self.next_pin = self.next_pin.saturating_add(1);
        self.pins.push((pin, key));
        pin
    }

    pub fn unpin(&mut self, pin: Pin) {
        self.pins.retain(|(held, _)| *held != pin);
    }

    fn pinned(&self, key: u64) -> bool {
        self.pins.iter().any(|(_, held)| *held == key)
    }

    /// Removes sealed segments, oldest first, while every record in them is
    /// at or below `floor` — that is, while the next segment starts at or
    /// below `floor + 1`. Stops at the first pinned one, so the log stays a
    /// contiguous suffix. Never removes the active segment, nor the last sealed
    /// one: if a crash leaves the active segment with nothing but a torn batch
    /// to cut, the records before it are what says where the log stands — the
    /// last record's lsn, slot and hash, which the next record has to name.
    /// Returns how many went.
    pub fn remove_through(&mut self, floor: u64) -> Result<usize, StorageError> {
        let mut removed = 0;
        while self.sealed.len() > 1 {
            let Some(&(first, _)) = self.sealed.first() else {
                break;
            };
            let next = self
                .sealed
                .get(1)
                .map(|&(key, _)| key)
                .or(self.active.as_ref().map(|active| active.key));
            let covered = next.is_some_and(|next| next <= floor.saturating_add(1));
            if !covered || self.pinned(first) {
                break;
            }
            self.disk.remove(&segment_path(&self.directory, first))?;
            self.sealed.remove(0);
            removed += 1;
        }
        if removed != 0 {
            // A removed name that comes back after a power loss is an older
            // segment the log can still read past; one that stays gone is
            // the point. Syncing keeps the two from mixing on the next start.
            self.disk
                .sync_directory(&self.directory)
                .map_err(StorageError::DirectoryNotDurable)?;
        }
        Ok(removed)
    }
}
