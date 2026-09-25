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
    /// Kept so the log can be reopened after it is rewritten underneath.
    path: std::path::PathBuf,
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
            scan(&mut file, &limits, length, &mut |_| {})?
        };

        // Appends continue at the end of the durable prefix: the tail is gone.
        file.set_len(recovery.durable_offset)?;
        file.seek(SeekFrom::Start(recovery.durable_offset))?;

        Ok((
            Self {
                file,
                path: path.to_path_buf(),
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

    /// Adds a run of already-encoded records to the batch, as a follower does
    /// with a replication frame. Returns how many records it held.
    ///
    /// The bytes go in untouched: they were framed by the log that produced
    /// them, checksums and all, and re-staging them through [`Wal::stage`]
    /// would recompute every one of those checksums to arrive at the same
    /// bytes (docs/replication.md §114). The framing is walked to count them,
    /// because the log's own sequence is a record count and taking the
    /// caller's word for it would let a wrong number travel over the network
    /// into this node's idea of where it is.
    pub fn stage_framed(&mut self, records: &[u8]) -> Result<usize, StorageError> {
        if self.failed {
            return Err(StorageError::Poisoned);
        }
        let count = record::count(records, self.limits.max_record_bytes)
            .filter(|count| *count > 0)
            .ok_or(StorageError::NotFramedRecords)?;

        self.batch.extend_from_slice(records);
        self.batch_records = self.batch_records.saturating_add(count);
        self.sequence = self
            .sequence
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        Ok(count)
    }

    /// Writes the staged batch out.
    ///
    /// Returns what became durable together with how long the two halves
    /// took: the storage device is the pacing signal, so its latency is not
    /// a diagnostic afterthought (docs/storage.md §95).
    ///
    /// With `sync` false the batch reaches the operating system and no
    /// further: the caller is saying it wants the write ordered but not the
    /// storage device asked about it (docs/storage.md §80, `written`).
    /// Nothing else changes — the durable offset still advances, because
    /// what "durable" promises is the caller's choice to make.
    pub fn commit_with(
        &mut self,
        sync: bool,
    ) -> Result<(Committed, Duration, Duration), StorageError> {
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
        if sync {
            self.sync()?;
        }
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

    /// Rewrites the log in place, keeping only the records `keep` accepts,
    /// and continues writing to the result.
    ///
    /// Nothing may be staged: the batch would be written to a file that is
    /// about to be replaced. The sequence carries across — it numbers
    /// records that were submitted, not records that survived, and a
    /// sequence that went backwards would make an old number mean two
    /// different records (docs/storage.md §104).
    pub fn compact(
        &mut self,
        keep: &mut impl FnMut(&[u8]) -> bool,
    ) -> Result<Compaction, StorageError> {
        if self.failed {
            return Err(StorageError::Poisoned);
        }
        if !self.batch.is_empty() {
            return Err(StorageError::StagedDuringCompaction);
        }

        let outcome = compact(&self.path, self.limits, keep)?;

        // The file the handle refers to has been replaced; reopen or every
        // later write lands in an unlinked inode.
        let mut file = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let end = file.metadata()?.len();
        file.seek(SeekFrom::End(0))?;
        self.file = file;
        self.write_offset = end;
        self.durable_offset = end;
        Ok(outcome)
    }

    /// Writes the staged batch and waits for the storage device.
    pub fn commit(&mut self) -> Result<(Committed, Duration, Duration), StorageError> {
        self.commit_with(true)
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
fn scan(
    file: &mut File,
    limits: &WalLimits,
    length: u64,
    visit: &mut impl FnMut(&[u8]),
) -> Result<Recovery, StorageError> {
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
            Frame::Record {
                start: from,
                end,
                total,
            } => {
                // Only records inside the durable prefix are handed over: the
                // tail is not a record, it is whatever the crash left.
                if let Some(payload) = rest.get(from..end) {
                    visit(payload);
                }
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

/// What a compaction removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Compaction {
    pub records_before: u64,
    pub records_after: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Rewrites the log, keeping only the records `keep` accepts, in order.
///
/// The log is a byte log: which records still matter is a question only the
/// caller can answer, so this does the mechanical half and nothing else.
///
/// Nothing may be appending while this runs. The rewrite goes to a second
/// file that is made durable before it replaces the original, so a crash at
/// any point leaves either the whole old log or the whole new one — never a
/// half-rewritten log (docs/storage.md §104).
pub fn compact(
    path: &Path,
    limits: WalLimits,
    keep: &mut impl FnMut(&[u8]) -> bool,
) -> Result<Compaction, StorageError> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Compaction::default());
        }
        Err(error) => return Err(error.into()),
    };
    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(Compaction::default());
    }

    let mut target = path.as_os_str().to_owned();
    target.push(".compacting");
    let target = std::path::PathBuf::from(target);
    let mut out = std::io::BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&target)?,
    );
    out.write_all(&MAGIC)?;

    // `scan` hands records over with no way to report a write failure, so
    // the first one is kept here and returned once the walk is done.
    let mut failure: Option<std::io::Error> = None;
    let mut kept = 0u64;
    let mut buffer: Vec<u8> = Vec::new();
    let recovery = scan(&mut file, &limits, length, &mut |payload| {
        if failure.is_some() || !keep(payload) {
            return;
        }
        buffer.clear();
        record::encode(payload, &mut buffer);
        if let Err(error) = out.write_all(&buffer) {
            failure = Some(error);
            return;
        }
        kept = kept.saturating_add(1);
    })?;
    if let Some(error) = failure {
        let _ = std::fs::remove_file(&target);
        return Err(error.into());
    }

    let out = out.into_inner().map_err(|error| error.into_error())?;
    out.sync_all()?;
    let bytes_after = out.metadata()?.len();
    drop(out);

    // The rename is the commit point: until it returns, the old log is
    // still the log.
    std::fs::rename(&target, path)?;
    // The rename itself has to reach the disk, or a crash can leave the
    // directory pointing at a file that is gone.
    if let Some(parent) = path.parent() {
        if let Ok(directory) = File::open(parent) {
            let _ = directory.sync_all();
        }
    }

    Ok(Compaction {
        records_before: recovery.records,
        records_after: kept,
        bytes_before: length,
        bytes_after,
    })
}

/// Reads the durable prefix of a log without opening it for writing.
///
/// Every complete, checksum-clean record is handed to `visit` in the order it
/// was committed; whatever follows the first broken one is not a record and
/// is not offered (docs/storage.md §92).
pub fn replay(
    path: &Path,
    limits: WalLimits,
    visit: &mut impl FnMut(&[u8]),
) -> Result<Recovery, StorageError> {
    let mut file = match OpenOptions::new().read(true).open(path) {
        Ok(file) => file,
        // Nothing written yet is not a failure to recover.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Recovery {
                records: 0,
                durable_offset: 0,
                discarded_bytes: 0,
            });
        }
        Err(error) => return Err(error.into()),
    };

    let length = file.metadata()?.len();
    if length == 0 {
        return Ok(Recovery {
            records: 0,
            durable_offset: 0,
            discarded_bytes: 0,
        });
    }
    scan(&mut file, &limits, length, visit)
}
