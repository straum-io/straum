//! The write-ahead log: one sequential file, one durable offset.
//!
//! Lengths read back from the file are untrusted (a crash leaves whatever it
//! leaves), so this module refuses indexing and unchecked arithmetic the same
//! way the packet decoder does.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::record::{self, Frame, MAGIC};
use crate::{StorageError, WalLimits};

/// Bytes pulled from the file at a time while recovering. Recovery therefore
/// holds a window plus one record, never the whole log.
const SCAN_WINDOW: usize = 64 * 1024;

/// What one completed durability operation established.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    /// Records with a sequence at or below this one are durable.
    pub durable_sequence: u64,
    /// Bytes of the file that are durable.
    pub durable_offset: u64,
    pub records: usize,
    pub bytes: usize,
}

/// What recovery found in an existing log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovery {
    pub records: u64,
    pub durable_offset: u64,
    /// Bytes of incomplete or corrupt tail that were cut off.
    pub discarded_bytes: u64,
}

pub struct Wal {
    file: File,
    limits: WalLimits,
    write_offset: u64,
    durable_offset: u64,
    sequence: u64,
    durable_sequence: u64,
    /// Reused across batches: one buffer per WAL, not one per record.
    batch: Vec<u8>,
    batch_records: usize,
    /// A durability failure is terminal for this log: nothing may be
    /// acknowledged after it (docs/storage.md §99, invariant 7).
    failed: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

impl Wal {
    /// Opens a log, recovering its durable prefix and cutting off the tail.
    pub fn open(path: &Path, limits: WalLimits) -> Result<(Self, Recovery), StorageError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let length = file.metadata()?.len();
        let recovery = if length == 0 {
            file.write_all(&MAGIC)?;
            file.sync_data()?;
            Recovery {
                records: 0,
                durable_offset: header_len(),
                discarded_bytes: 0,
            }
        } else {
            scan(&mut file, &limits, length)?
        };

        // Appends continue at the end of the durable prefix: the tail is gone.
        file.set_len(recovery.durable_offset)?;
        file.seek(SeekFrom::Start(recovery.durable_offset))?;

        Ok((
            Self {
                file,
                limits,
                write_offset: recovery.durable_offset,
                durable_offset: recovery.durable_offset,
                sequence: recovery.records,
                durable_sequence: recovery.records,
                batch: Vec::new(),
                batch_records: 0,
                failed: false,
                #[cfg(test)]
                fail_next_sync: false,
            },
            recovery,
        ))
    }

    /// Adds a record to the batch being built. Nothing reaches the file until
    /// [`Wal::commit`].
    pub fn stage(&mut self, payload: &[u8]) -> Result<u64, StorageError> {
        if self.failed {
            return Err(StorageError::Poisoned);
        }
        if payload.is_empty() || payload.len() > self.limits.max_record_bytes {
            return Err(StorageError::RecordTooLarge);
        }

        record::encode(payload, &mut self.batch);
        self.batch_records = self.batch_records.saturating_add(1);
        self.sequence = self.sequence.saturating_add(1);
        Ok(self.sequence)
    }

    /// Writes the staged batch and makes it durable.
    ///
    /// Returns what became durable together with how long the two halves took:
    /// the storage device is the pacing signal, so its latency is not a
    /// diagnostic afterthought (docs/storage.md §95).
    pub fn commit(&mut self) -> Result<(Committed, Duration, Duration), StorageError> {
        if self.failed {
            return Err(StorageError::Poisoned);
        }
        let bytes = self.batch.len();
        let records = self.batch_records;

        let started = Instant::now();
        // The batch buffer is moved out and put back so the write borrows
        // it without copying it (docs/storage.md §96).
        let batch = std::mem::take(&mut self.batch);
        let written = self.write(&batch);
        self.batch = batch;
        written?;
        let write = started.elapsed();

        let started = Instant::now();
        self.sync()?;
        let sync = started.elapsed();

        self.batch.clear();
        self.batch_records = 0;
        self.durable_offset = self.write_offset;
        self.durable_sequence = self.sequence;

        Ok((
            Committed {
                durable_sequence: self.durable_sequence,
                durable_offset: self.durable_offset,
                records,
                bytes,
            },
            write,
            sync,
        ))
    }

    pub fn staged_bytes(&self) -> usize {
        self.batch.len()
    }

    pub fn staged_records(&self) -> usize {
        self.batch_records
    }

    pub fn durable_offset(&self) -> u64 {
        self.durable_offset
    }

    pub fn durable_sequence(&self) -> u64 {
        self.durable_sequence
    }

    fn write(&mut self, batch: &[u8]) -> Result<(), StorageError> {
        if let Err(error) = self.file.write_all(batch) {
            self.failed = true;
            return Err(error.into());
        }
        let written = u64::try_from(batch.len()).unwrap_or(u64::MAX);
        self.write_offset = self.write_offset.saturating_add(written);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        #[cfg(test)]
        if self.fail_next_sync {
            self.fail_next_sync = false;
            self.failed = true;
            return Err(StorageError::Poisoned);
        }
        if let Err(error) = self.file.sync_data() {
            self.failed = true;
            return Err(error.into());
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn fail_next_sync(&mut self) {
        self.fail_next_sync = true;
    }
}

fn header_len() -> u64 {
    u64::try_from(MAGIC.len()).unwrap_or(0)
}

/// Walks the log from the start and stops at the first thing that is not a
/// complete record. Everything before that point is the durable prefix.
fn scan(file: &mut File, limits: &WalLimits, length: u64) -> Result<Recovery, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0u8; MAGIC.len()];
    if file.read_exact(&mut magic).is_err() || magic != MAGIC {
        return Err(StorageError::NotAWal);
    }

    let mut buf: Vec<u8> = Vec::with_capacity(
        SCAN_WINDOW.saturating_add(limits.max_record_bytes.saturating_add(record::HEADER_BYTES)),
    );
    let mut start = 0usize;
    let mut offset = header_len();
    let mut records = 0u64;
    let mut exhausted = false;

    loop {
        let rest = buf.get(start..).unwrap_or_default();
        match record::read(rest, limits.max_record_bytes) {
            Frame::Record { total, .. } => {
                records = records.saturating_add(1);
                offset = offset.saturating_add(u64::try_from(total).unwrap_or(0));
                start = start.saturating_add(total);
            }
            Frame::Invalid => break,
            Frame::Incomplete => {
                if exhausted {
                    break;
                }
                // Drop what is already parsed, then pull the next window.
                buf.drain(..start);
                start = 0;
                let before = buf.len();
                buf.resize(before.saturating_add(SCAN_WINDOW), 0);
                let filled = read_some(file, &mut buf, before)?;
                buf.truncate(before.saturating_add(filled));
                if filled == 0 {
                    exhausted = true;
                }
            }
        }
    }

    Ok(Recovery {
        records,
        durable_offset: offset,
        discarded_bytes: length.saturating_sub(offset),
    })
}

/// Fills `buf[from..]` with as much as the file has left.
fn read_some(file: &mut File, buf: &mut [u8], from: usize) -> Result<usize, StorageError> {
    let target = buf.get_mut(from..).unwrap_or_default();
    let mut filled = 0usize;
    while let Some(slot) = target.get_mut(filled..) {
        if slot.is_empty() {
            break;
        }
        match file.read(slot) {
            Ok(0) => break,
            Ok(read) => filled = filled.saturating_add(read),
            Err(error) => return Err(error.into()),
        }
    }
    Ok(filled)
}
