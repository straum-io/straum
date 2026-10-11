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

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::Duration;

use crate::disk::{Access, Disk, DiskFile, OsDisk};

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
///
/// The physical and the logical are separate answers, and telling them apart
/// is the point: `records` is what the file holds, `sequence` and
/// `correlation` are what the log has ever handed out. Compaction changes the
/// first and never the other two.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Recovery {
    /// Complete records physically present in the file.
    pub records: u64,
    /// The highest sequence ever allocated in this log's history.
    ///
    /// Not the record count. Compaction removes records, so after one this is
    /// larger than `records` — which is exactly the case the log used to get
    /// wrong, handing out numbers it had already used
    /// (docs/replication.md §116).
    pub sequence: u64,
    /// The correlation high-water the header carried when the file was last
    /// written.
    ///
    /// A floor, not the answer. The log does not read record bodies, so it
    /// cannot see the correlations in them; the owner of those bodies takes
    /// the larger of this and whatever the surviving records carry.
    pub correlation: u64,
    pub durable_offset: u64,
    /// Where the addressable region starts: everything from here to
    /// `durable_offset` can be read by sequence (docs/replication.md §130).
    pub addressable_offset: u64,
    /// Bytes of incomplete or corrupt tail that were cut off.
    pub discarded_bytes: u64,
}

/// The logical state written into the file header.
///
/// ```text
/// [ magic 8 ][ sequence u64 BE ][ records u64 BE ][ correlation u64 BE ][ crc32 u32 BE ]
/// ```
///
/// `records` is how many records the file starts with that have no sequence
/// — the ones a rewrite filtered — and `sequence` is the number just below
/// the first record after them. A reader finding `n` records knows the live
/// sequence is `sequence + (n - records)`. A fresh log writes zeroes. A
/// rewrite that keeps no suffix writes the highest sequence allocated; one
/// that keeps a suffix writes the number below its first record, which is
/// what lets the suffix keep its numbers (docs/replication.md §130). Two fields rather than
/// one base, because a log that says what it started with can be checked.
///
/// `correlation` is opaque: the log never reads a record body, so the owner of
/// those bodies writes down its own high-water and gets it back.
///
/// The header is written when the file is created and when it is rewritten,
/// and at no other time. Nothing on the append path touches it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Header {
    sequence: u64,
    records: u64,
    correlation: u64,
}

/// Magic, three `u64`s and a checksum.
pub const HEADER_BYTES: usize = 36;

/// The header a log starts life with: nothing allocated yet.
pub(crate) fn empty_header() -> Vec<u8> {
    encode_header(Header::default())
}

fn encode_header(header: Header) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_BYTES);
    out.extend_from_slice(&MAGIC);
    out.extend_from_slice(&header.sequence.to_be_bytes());
    out.extend_from_slice(&header.records.to_be_bytes());
    out.extend_from_slice(&header.correlation.to_be_bytes());
    // Covers the numbers, not the magic: a damaged high-water reads as a
    // plausible number and would silently repeat identifiers, which is the
    // one failure this header exists to prevent.
    let sum = crate::crc::checksum(out.get(MAGIC.len()..).unwrap_or_default());
    out.extend_from_slice(&sum.to_be_bytes());
    out
}

fn decode_header(bytes: &[u8]) -> Result<Header, StorageError> {
    let checksummed = HEADER_BYTES.saturating_sub(4);
    if bytes.get(..MAGIC.len()) != Some(&MAGIC[..]) {
        return Err(StorageError::NotAWal);
    }
    let (Some(body), Some(expected)) = (
        bytes.get(MAGIC.len()..checksummed),
        bytes
            .get(checksummed..HEADER_BYTES)
            .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
            .map(u32::from_be_bytes),
    ) else {
        return Err(StorageError::CorruptHeader);
    };
    if crate::crc::checksum(body) != expected {
        return Err(StorageError::CorruptHeader);
    }

    let field = |at: usize| -> Option<u64> {
        let end = at.checked_add(8)?;
        body.get(at..end)
            .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
            .map(u64::from_be_bytes)
    };
    let (Some(sequence), Some(records), Some(correlation)) = (field(0), field(8), field(16)) else {
        return Err(StorageError::CorruptHeader);
    };
    Ok(Header {
        sequence,
        records,
        correlation,
    })
}

/// Generic over the [`Disk`] it lives on; [`OsDisk`] unless a simulation says
/// otherwise (D-28).
pub struct Wal<D: Disk = OsDisk> {
    disk: D,
    file: D::File,
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
    /// The batch the last commit wrote, kept in the buffer it was written
    /// from until the next one takes its place. A replica is handed these
    /// bytes rather than a copy of them (docs/replication.md §112).
    committed: Vec<u8>,
    /// A durability failure is terminal for this log: nothing may be
    /// acknowledged after it (docs/storage.md §99, invariant 7).
    failed: bool,
    #[cfg(test)]
    fail_next_sync: bool,
}

impl Wal<OsDisk> {
    /// Opens a log, recovering its durable prefix and cutting off the tail.
    pub fn open(path: &Path, limits: WalLimits) -> Result<(Self, Recovery), StorageError> {
        Self::open_on(OsDisk, path, limits)
    }
}

impl<D: Disk> Wal<D> {
    /// [`Wal::open`], on a disk of the caller's choosing.
    pub fn open_on(
        disk: D,
        path: &Path,
        limits: WalLimits,
    ) -> Result<(Self, Recovery), StorageError> {
        let mut file = disk.open(path, Access::Create)?;

        let length = file.size()?;
        let recovery = if length == 0 {
            file.write_all(&encode_header(Header::default()))?;
            file.sync_data()?;
            // A new file's name is durable only once its directory is: POSIX
            // promises nothing for the entry from a sync of the file (D-30).
            disk.sync_directory(directory_of(path))
                .map_err(StorageError::DirectoryNotDurable)?;
            Recovery {
                durable_offset: header_len(),
                addressable_offset: header_len(),
                ..Recovery::default()
            }
        } else {
            scan(&mut file, &limits, length, &mut |_| true, &mut |_, _, _| {})?
        };

        // Appends continue at the end of the durable prefix: the tail is gone.
        file.set_len(recovery.durable_offset)?;
        // What was recovered is what the operating system holds, and after a
        // process crash that can be a batch whose sync failed or never ran.
        // The log is about to call all of it durable — a follower reports it,
        // a leader ships it — so it is made durable first. One sync per open.
        // Without it a restart after a failed sync claimed records a power
        // loss then took (D-43, found by the simulation). A new file synced its
        // header above and holds nothing else.
        if length != 0 {
            file.sync_data()?;
        }
        file.seek(SeekFrom::Start(recovery.durable_offset))?;

        Ok((
            Self {
                disk,
                file,
                path: path.to_path_buf(),
                limits,
                write_offset: recovery.durable_offset,
                durable_offset: recovery.durable_offset,
                sequence: recovery.sequence,
                durable_sequence: recovery.sequence,
                batch: Vec::new(),
                committed: Vec::new(),
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

        let started = self.disk.now();
        // Two buffers, rotated: the one just written becomes the one a
        // replica can be handed without a copy, and the one it held becomes
        // the next batch with its capacity intact. The inner take-and-return
        // is what lets the write borrow the bytes rather than copy them
        // (docs/storage.md §96).
        std::mem::swap(&mut self.batch, &mut self.committed);
        let committed = std::mem::take(&mut self.committed);
        let written = self.write(&committed);
        self.committed = committed;
        written?;
        let write = self.disk.now().saturating_sub(started);

        let started = self.disk.now();
        if sync {
            self.sync()?;
        }
        let sync = self.disk.now().saturating_sub(started);

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

    /// Rewrites the log in place, keeping the suffix `retain` names verbatim
    /// and, ahead of it, only the records `keep` accepts; then continues
    /// writing to the result.
    ///
    /// Nothing may be staged: the batch would be written to a file that is
    /// about to be replaced. The sequence carries across — it numbers
    /// records that were submitted, not records that survived, and a
    /// sequence that went backwards would make an old number mean two
    /// different records (docs/storage.md §104).
    pub fn compact(
        &mut self,
        correlation: u64,
        retain: Retain,
        keep: &mut impl FnMut(&[u8]) -> bool,
    ) -> Result<Compaction, StorageError> {
        if self.failed {
            return Err(StorageError::Poisoned);
        }
        if !self.batch.is_empty() {
            return Err(StorageError::StagedDuringCompaction);
        }

        let outcome = match compact_on(
            &self.disk,
            &self.path,
            self.limits,
            correlation,
            retain,
            keep,
        ) {
            Ok(outcome) => outcome,
            // The name already points at the new file, and nothing says it
            // will after a power loss: whatever is committed from here could
            // vanish with it. Terminal, like a failed sync (D-9, D-31).
            Err(error @ StorageError::DirectoryNotDurable(_)) => {
                self.failed = true;
                return Err(error);
            }
            Err(error) => return Err(error),
        };

        // The file the handle refers to has been replaced; reopen or every
        // later write lands in an unlinked inode.
        let mut file = self.disk.open(&self.path, Access::Write)?;
        let end = file.size()?;
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

    /// The batch the last commit wrote, framed exactly as it went to the
    /// file. Empty before the first commit; replaced by the next one.
    ///
    /// Read after `commit_with` returns, so the fork to a replica happens
    /// after the local durability operation rather than beside it. Moving it
    /// earlier halves the latency a quorum acknowledgement waits for and is a
    /// change with a measurement behind it, not a refactor
    /// (docs/replication.md §112).
    pub fn committed_batch(&self) -> &[u8] {
        &self.committed
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

/// The directory a log lives in. A bare file name lives in the current one,
/// which `Path::parent` reports as empty rather than as `.`.
fn directory_of(path: &Path) -> &Path {
    match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent,
        _ => Path::new("."),
    }
}

fn header_len() -> u64 {
    u64::try_from(HEADER_BYTES).unwrap_or(0)
}

/// Walks the log from the start and stops at the first thing that is not a
/// complete record. Everything before that point is the durable prefix.
fn scan(
    file: &mut (impl Read + Seek),
    limits: &WalLimits,
    length: u64,
    // Called once with this file's retention floor as soon as the header is
    // decoded, before any record is handed over; `false` stops the walk.
    ready: &mut impl FnMut(u64) -> bool,
    // The record's body, the same record with its framing, and its WAL
    // sequence — `None` for one that survived a compaction.
    //
    // Compaction keeps a filtered set ahead of an optional verbatim
    // suffix. The filtered records have no recoverable sequence: nothing
    // maps a survivor back to its own number. The suffix, and anything
    // appended after it, does.
    // The distinction is made here, where the header has just been read,
    // and never reconstructed by a caller (docs/replication.md §130).
    visit: &mut impl FnMut(&[u8], &[u8], Option<u64>),
) -> Result<Recovery, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut head = [0u8; HEADER_BYTES];
    if file.read_exact(&mut head).is_err() {
        return Err(StorageError::NotAWal);
    }
    let header = decode_header(&head)?;
    if !ready(header.sequence.saturating_add(1)) {
        return Ok(Recovery::default());
    }
    // No further than the file ended when the walk began. A reader of a log
    // that is being appended to — catch-up — read on until `read` came back
    // empty, and under load chased the writer's tail without ever finishing
    // (D-76: the Linux container, `replication_matrix` test 41).
    let mut file = Read::by_ref(file).take(length.saturating_sub(header_len()));

    let mut buf: Vec<u8> = Vec::with_capacity(
        SCAN_WINDOW.saturating_add(limits.max_record_bytes.saturating_add(record::HEADER_BYTES)),
    );
    let mut start = 0usize;
    let mut offset = header_len();
    let mut addressable_offset = offset;
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
                records = records.saturating_add(1);
                // `past == 0` is the last record the rewrite kept, not the
                // first it appended: at or below `header.records` a record
                // belongs to the filtered set and has no sequence.
                let sequence = records
                    .checked_sub(header.records)
                    .filter(|past| *past > 0)
                    .map(|past| header.sequence.saturating_add(past));
                if let (Some(payload), Some(framed)) = (rest.get(from..end), rest.get(..total)) {
                    visit(payload, framed, sequence);
                }
                offset = offset.saturating_add(u64::try_from(total).unwrap_or(0));
                if records <= header.records {
                    addressable_offset = offset;
                }
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
                let filled = read_some(&mut file, &mut buf, before)?;
                buf.truncate(before.saturating_add(filled));
                if filled == 0 {
                    exhausted = true;
                }
            }
        }
    }

    Ok(Recovery {
        records,
        // What the file started with plus what it has gained since. A
        // compacted file holds fewer records than were ever allocated, and
        // the header is the only place that difference survives
        // (docs/replication.md §116).
        sequence: header
            .sequence
            .saturating_add(records.saturating_sub(header.records)),
        correlation: header.correlation,
        durable_offset: offset,
        addressable_offset,
        discarded_bytes: length.saturating_sub(offset),
    })
}

/// Fills `buf[from..]` with as much as the file has left.
fn read_some(file: &mut impl Read, buf: &mut [u8], from: usize) -> Result<usize, StorageError> {
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
    /// The retention floor the rewrite left: the lowest sequence the log can
    /// still be read from.
    pub floor: u64,
    /// Bytes of the addressable suffix the rewrite kept.
    pub retained_bytes: u64,
}

/// Which records a rewrite keeps verbatim, under the sequences they were
/// written with, instead of offering them to the caller's filter.
///
/// A record is retained if its sequence is at least `from`, or if it and
/// everything after it fit in `bytes`. Retention only extends the region
/// that is already addressable: it cannot give a number back to a record an
/// earlier rewrite filtered, so the floor never moves down
/// (docs/replication.md §130).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retain {
    pub from: u64,
    pub bytes: u64,
}

impl Retain {
    /// Every record goes through the filter; nothing stays addressable.
    pub const NONE: Self = Self {
        from: u64::MAX,
        bytes: 0,
    };
}

/// Rewrites the log, keeping the suffix `retain` names verbatim and, ahead
/// of it, only the records `keep` accepts, in order.
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
    correlation: u64,
    retain: Retain,
    keep: &mut impl FnMut(&[u8]) -> bool,
) -> Result<Compaction, StorageError> {
    compact_on(&OsDisk, path, limits, correlation, retain, keep)
}

/// [`compact`], on a disk of the caller's choosing.
pub fn compact_on<D: Disk>(
    disk: &D,
    path: &Path,
    limits: WalLimits,
    correlation: u64,
    retain: Retain,
    keep: &mut impl FnMut(&[u8]) -> bool,
) -> Result<Compaction, StorageError> {
    let mut file = match disk.open(path, Access::Read) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Compaction::default());
        }
        Err(error) => return Err(error.into()),
    };
    let length = file.size()?;
    if length == 0 {
        return Ok(Compaction::default());
    }

    let mut target = path.as_os_str().to_owned();
    target.push(".compacting");
    let target = std::path::PathBuf::from(target);
    // What the file about to be replaced says it has ever allocated. The
    // records themselves cannot say it: compaction is what removes them
    // (docs/replication.md §116).
    let mut head = [0u8; HEADER_BYTES];
    file.seek(SeekFrom::Start(0))?;
    if file.read_exact(&mut head).is_err() {
        return Err(StorageError::NotAWal);
    }
    let previous = decode_header(&head)?;

    let mut out = std::io::BufWriter::new(disk.open(&target, Access::Replace)?);
    // A placeholder: how many records survive is not known until the walk is
    // done, and the real header goes in before the file is made durable.
    out.write_all(&encode_header(Header::default()))?;

    // `scan` hands records over with no way to report a write failure, so
    // the first one is kept here and returned once the walk is done.
    let mut failure: Option<std::io::Error> = None;
    let mut kept = 0u64;
    // Records written ahead of the retained suffix: the filtered set.
    let mut filtered = 0u64;
    // The first sequence kept as part of the suffix, once one has been.
    let mut suffix: Option<u64> = None;
    let mut retained_bytes = 0u64;
    let mut at = header_len();
    let recovery = scan(
        &mut file,
        &limits,
        length,
        &mut |_| true,
        &mut |payload, framed, sequence| {
            let size = u64::try_from(framed.len()).unwrap_or(u64::MAX);
            // Measured against the file's length, which a crash may have left
            // a torn batch past: then the suffix comes out smaller than
            // `retain.bytes` by that batch, never larger.
            let from_end = length.saturating_sub(at);
            at = at.saturating_add(size);
            if failure.is_some() {
                return;
            }
            // Once the suffix starts it runs to the end: the addressable
            // region is contiguous, and a record dropped from the middle of
            // it would shift every number after it.
            let retained = suffix.is_some()
                || sequence
                    .is_some_and(|sequence| sequence >= retain.from || from_end <= retain.bytes);
            if retained {
                suffix = suffix.or(sequence);
                retained_bytes = retained_bytes.saturating_add(size);
            } else if keep(payload) {
                filtered = filtered.saturating_add(1);
            } else {
                return;
            }
            // The record goes across as it is. Re-encoding it would recompute a
            // checksum to produce the bytes already in hand.
            if let Err(error) = out.write_all(framed) {
                failure = Some(error);
                return;
            }
            kept = kept.saturating_add(1);
        },
    )?;
    if let Some(error) = failure {
        let _ = disk.remove(&target);
        return Err(error.into());
    }

    let mut out = out.into_inner().map_err(|error| error.into_error())?;
    // The allocation state, carried across the rewrite. `sequence` is what
    // the old file had ever handed out — its own header plus what it gained
    // since that was written — and it goes into the new file untouched,
    // together with how many records the new file starts with. A reader then
    // has both halves and never has to guess one from the other.
    out.seek(SeekFrom::Start(0))?;
    let allocated = previous
        .sequence
        .saturating_add(recovery.records.saturating_sub(previous.records));
    // With a suffix kept, the header names the sequence just below it and
    // counts only the filtered records ahead of it, so a reader labels the
    // suffix with exactly the numbers it was written under and the live
    // sequence still comes out as `allocated`.
    let sequence = suffix.map_or(allocated, |first| first.saturating_sub(1));
    out.write_all(&encode_header(Header {
        sequence,
        records: filtered,
        // `max`, so a caller that does not track correlations cannot lower a
        // high-water somebody else wrote down.
        correlation: previous.correlation.max(correlation),
    }))?;
    out.sync_all()?;
    let bytes_after = out.size()?;
    drop(out);

    // The rename is the commit point: until it returns, the old log is
    // still the log.
    disk.rename(&target, path)?;
    // The rename itself has to reach the disk, or a power loss puts the old
    // file back under the name — taking with it every record committed to
    // the new one since. A failure here is not swallowed: it was, and a
    // record acknowledged as fsynced after it was lost (D-31).
    disk.sync_directory(directory_of(path))
        .map_err(StorageError::DirectoryNotDurable)?;

    Ok(Compaction {
        records_before: recovery.records,
        records_after: kept,
        bytes_before: length,
        bytes_after,
        floor: sequence.saturating_add(1),
        retained_bytes,
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
    replay_on(&OsDisk, path, limits, visit)
}

/// [`replay`], on a disk of the caller's choosing.
pub fn replay_on<D: Disk>(
    disk: &D,
    path: &Path,
    limits: WalLimits,
    visit: &mut impl FnMut(&[u8]),
) -> Result<Recovery, StorageError> {
    replay_with(disk, path, limits, &mut |_| true, &mut |payload, _, _| {
        visit(payload)
    })
}

/// The records a follower can be caught up from, with their sequences.
///
/// Only the addressable region is offered: the suffix the last rewrite kept
/// verbatim and everything appended since. What a compaction filtered is
/// still in the file and still valid for
/// recovery, but it has no sequence any more, so it cannot be a source for
/// a sequence-addressed replication stream (docs/replication.md §130).
pub fn replay_addressable(
    path: &Path,
    limits: WalLimits,
    // Called once with this file's retention floor before any record is
    // handed over, and returning `false` stops the walk. That is what makes
    // the check and the read one representation: a caller cannot validate a
    // range against one state of the file and then read another
    // (docs/replication.md §130).
    ready: &mut impl FnMut(u64) -> bool,
    visit: &mut impl FnMut(u64, &[u8]),
) -> Result<Recovery, StorageError> {
    replay_addressable_on(&OsDisk, path, limits, ready, visit)
}

/// [`replay_addressable`], on a disk of the caller's choosing.
pub fn replay_addressable_on<D: Disk>(
    disk: &D,
    path: &Path,
    limits: WalLimits,
    ready: &mut impl FnMut(u64) -> bool,
    visit: &mut impl FnMut(u64, &[u8]),
) -> Result<Recovery, StorageError> {
    replay_with(disk, path, limits, ready, &mut |_, framed, sequence| {
        if let Some(sequence) = sequence {
            visit(sequence, framed);
        }
    })
}

fn replay_with<D: Disk>(
    disk: &D,
    path: &Path,
    limits: WalLimits,
    ready: &mut impl FnMut(u64) -> bool,
    visit: &mut impl FnMut(&[u8], &[u8], Option<u64>),
) -> Result<Recovery, StorageError> {
    let mut file = match disk.open(path, Access::Read) {
        Ok(file) => file,
        // Nothing written yet is not a failure to recover.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Recovery::default());
        }
        Err(error) => return Err(error.into()),
    };

    let length = file.size()?;
    if length == 0 {
        return Ok(Recovery::default());
    }
    scan(&mut file, &limits, length, ready, visit)
}

/// What a log says it has allocated, read from its header alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Allocated {
    /// The sequence just below the first addressable record.
    pub sequence: u64,
    /// How many filtered records precede the addressable region.
    pub records: u64,
    pub correlation: u64,
}

impl Allocated {
    /// The lowest sequence this log can still be read from by sequence.
    ///
    /// Not a claim that everything below it is gone. Records a compaction
    /// filtered are still in the file and still recover, but nothing maps
    /// them back to the numbers they were written under. Below this floor the log has bytes
    /// and no addresses (docs/replication.md §130).
    pub fn retention_floor(&self) -> u64 {
        self.sequence.saturating_add(1)
    }
}

/// Reads the header and nothing else.
///
/// One `read` of 36 bytes. A reader that needs to find a particular sequence
/// in the file needs the base the records start from, and walking the whole
/// log to learn it would be a second pass over everything.
pub fn allocated(path: &Path) -> Result<Allocated, StorageError> {
    allocated_on(&OsDisk, path)
}

/// [`allocated`], on a disk of the caller's choosing.
pub fn allocated_on<D: Disk>(disk: &D, path: &Path) -> Result<Allocated, StorageError> {
    let mut file = match disk.open(path, Access::Read) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(Allocated::default());
        }
        Err(error) => return Err(error.into()),
    };
    if file.size()? == 0 {
        return Ok(Allocated::default());
    }

    let mut head = [0u8; HEADER_BYTES];
    if file.read_exact(&mut head).is_err() {
        return Err(StorageError::NotAWal);
    }
    let header = decode_header(&head)?;
    Ok(Allocated {
        sequence: header.sequence,
        records: header.records,
        correlation: header.correlation,
    })
}
