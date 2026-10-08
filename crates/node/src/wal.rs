//! The log on disk, on a thread of its own: storage does blocking file I/O,
//! and `sync_data` never runs on the node loop (CLAUDE.md §3, storage).
//!
//! It also keeps the node's snapshot (D-67 B): one file, replaced
//! atomically, written before any segment below it is removed, so a start
//! always finds a snapshot and the log after it with no gap between them.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use journal::{BatchHeader, Chain, ChainError, Kind, Verifier, decode};
use storage::{OsDisk, SegmentLimits, Segments};
use tokio::sync::mpsc as tokio_mpsc;

use crate::NodeError;

pub enum Command {
    /// One batch, as the core handed it over.
    Append { first: u64, records: Vec<Vec<u8>> },
    /// The batch that starts at `from`, for catch-up.
    Read { to: u8, from: u64 },
    /// Records `from..=upto`, for applying what this process did not
    /// append itself; at most `MAX_RANGE` of them.
    Range { from: u64, upto: u64 },
    /// The applied state at `lsn`, encoded: made durable, then reported.
    Snapshot { lsn: u64, bytes: Vec<u8> },
    /// Removes the segments wholly at or below `floor` (never above the
    /// durable snapshot: the node loop computes it so).
    Compact { floor: u64 },
    /// Makes the promise of `slot` durable, then reports it (§2.2).
    Promise { slot: u64 },
    /// Cuts the log back to end at `after` and reports the chain there.
    Truncate { after: u64 },
}

pub enum Event {
    /// Durable through `lsn`. `bytes_floor` is the lowest lsn the retention
    /// bound in bytes lets the log keep (D-57 A), measured on the segments.
    Durable {
        lsn: u64,
        bytes_floor: u64,
    },
    Snapshot(u64),
    Promised(u64),
    Truncated(Chain),
    Read {
        to: u8,
        from: u64,
        batch: Option<Vec<Vec<u8>>>,
    },
    Range(Vec<Vec<u8>>),
    Failed(storage::StorageError),
}

/// Records handed back per range read: bounds what one read holds.
const MAX_RANGE: usize = 1024;

const SNAPSHOT: &str = "snapshot";
const SNAPSHOT_NEXT: &str = "snapshot.next";
const SNAPSHOT_MAGIC: &[u8; 8] = b"STRMSNAP";
const PROMISE: &str = "promise";
const PROMISE_NEXT: &str = "promise.next";

/// What a start finds on disk.
pub struct Recovered {
    /// The chain after the last complete batch.
    pub chain: Chain,
    /// The lowest lsn still in the log; `None` if it is empty.
    pub first: Option<u64>,
    /// The snapshot's body, checksum verified; decoding it is `apply`'s.
    pub snapshot: Option<Vec<u8>>,
    /// The log's slot table: (slot, first lsn), ascending.
    pub slots: Vec<(u64, u64)>,
    /// The highest slot promised, as the disk holds it.
    pub promised: u64,
}

fn read_promise(directory: &Path) -> Result<u64, NodeError> {
    let mut bytes = [0u8; 8];
    match std::fs::File::open(directory.join(PROMISE)) {
        Ok(mut file) => file.read_exact(&mut bytes)?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    Ok(u64::from_be_bytes(bytes))
}

/// Written aside, synced, renamed, the directory synced: the promise is on
/// the disk before anything that depends on it is sent.
fn write_promise(directory: &Path, slot: u64) -> std::io::Result<()> {
    let next = directory.join(PROMISE_NEXT);
    let mut file = std::fs::File::create(&next)?;
    file.write_all(&slot.to_be_bytes())?;
    file.sync_all()?;
    std::fs::rename(&next, directory.join(PROMISE))?;
    std::fs::File::open(directory)?.sync_all()
}

/// Cuts the log back to `after` and returns the chain there: the record
/// after it names the hash, the record at it the slot.
fn truncate(log: &mut Segments<OsDisk>, after: u64) -> Result<Chain, storage::StorageError> {
    let chain = if after == 0 {
        Chain::genesis()
    } else {
        let around = read_range(log, after, after.saturating_add(1));
        let decoded: Vec<_> = around.iter().filter_map(|r| decode(r).ok()).collect();
        match (decoded.first(), decoded.get(1)) {
            (Some(at), Some(next)) => Chain::resume(next.prev_hash, after, at.slot),
            // Nothing after it: there is nothing to cut.
            _ => return Err(storage::StorageError::NotFramedRecords),
        }
    };
    log.truncate_after(after)?;
    Ok(chain)
}

/// `[magic][crc32 of body u32][body]`: a torn or damaged snapshot is
/// refused, never decoded.
fn read_snapshot(directory: &Path) -> Result<Option<Vec<u8>>, NodeError> {
    let mut file = match std::fs::File::open(directory.join(SNAPSHOT)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let (Some(magic), Some(crc), Some(body)) = (bytes.get(..8), bytes.get(8..12), bytes.get(12..))
    else {
        return Err(NodeError::SnapshotDamaged);
    };
    let crc = u32::from_be_bytes(crc.try_into().map_err(|_| NodeError::SnapshotDamaged)?);
    if magic != SNAPSHOT_MAGIC || crc32fast::hash(body) != crc {
        return Err(NodeError::SnapshotDamaged);
    }
    Ok(Some(body.to_vec()))
}

fn write_snapshot(directory: &Path, body: &[u8]) -> std::io::Result<()> {
    let next = directory.join(SNAPSHOT_NEXT);
    let mut file = std::fs::File::create(&next)?;
    file.write_all(SNAPSHOT_MAGIC)?;
    file.write_all(&crc32fast::hash(body).to_be_bytes())?;
    file.write_all(body)?;
    file.sync_all()?;
    std::fs::rename(&next, directory.join(SNAPSHOT))?;
    std::fs::File::open(directory)?.sync_all()
}

/// The lowest lsn a retention bound of `retain` bytes keeps: the first key
/// of the newest segments that together hold no more than it.
fn bytes_floor(log: &Segments<OsDisk>, retain: u64) -> u64 {
    let mut kept = 0u64;
    let mut floor = 0;
    for (key, bytes) in log.sizes().rev() {
        kept = kept.saturating_add(bytes);
        if kept > retain {
            floor = key;
            break;
        }
    }
    floor
}

/// Opens the log, checks it is one chain, cuts a torn batch, reads the
/// snapshot, and starts the thread.
pub fn start(
    directory: PathBuf,
    limits: SegmentLimits,
    retain_bytes: u64,
    events: tokio_mpsc::Sender<Event>,
) -> Result<
    (
        Recovered,
        mpsc::Sender<Command>,
        std::thread::JoinHandle<()>,
    ),
    NodeError,
> {
    std::fs::create_dir_all(&directory)?;
    // Verified as it is read: recovery holds one record at a time, not the
    // log (CLAUDE.md §7).
    let mut verifier: Option<Verifier> = None;
    let mut broken: Option<ChainError> = None;
    let mut first_lsn = None;
    let mut slots: Vec<(u64, u64)> = Vec::new();
    let mut log = Segments::open(OsDisk, &directory, limits, &mut |_, record| {
        if broken.is_some() {
            return;
        }
        let verifier = verifier.get_or_insert_with(|| match decode(record) {
            Ok(first) => Verifier::new(Chain::resume(
                first.prev_hash,
                first.lsn.saturating_sub(1),
                first.slot,
            )),
            // `feed` below reports the same record as malformed.
            Err(_) => Verifier::new(Chain::genesis()),
        });
        if let Ok(r) = decode(record) {
            if first_lsn.is_none() {
                first_lsn = Some(r.lsn);
            }
            if slots.last().is_none_or(|&(slot, _)| slot < r.slot) {
                slots.push((r.slot, r.lsn));
            }
        }
        if let Err(error) = verifier.feed(record) {
            broken = Some(error);
        }
    })?;
    if let Some(error) = broken {
        return Err(NodeError::Recovery(error));
    }
    let tail = verifier.map_or(
        journal::Tail {
            chain: Chain::genesis(),
            partial: 0,
        },
        Verifier::finish,
    );
    if tail.partial > 0 {
        log.truncate_tail(tail.partial)?;
    }
    // A log cut back to nothing but a torn batch is empty.
    let first = first_lsn.filter(|&f| f < tail.chain.next_lsn());
    let snapshot = read_snapshot(&directory)?;
    let promised = read_promise(&directory)?;
    slots.retain(|&(_, first)| first < tail.chain.next_lsn());
    let (commands, inbox) = mpsc::channel::<Command>();
    let thread = std::thread::Builder::new()
        .name("wal".into())
        .spawn(move || run(log, directory, retain_bytes, inbox, events))?;
    Ok((
        Recovered {
            chain: tail.chain,
            first,
            snapshot,
            slots,
            promised,
        },
        commands,
        thread,
    ))
}

fn run(
    mut log: Segments<OsDisk>,
    directory: PathBuf,
    retain_bytes: u64,
    inbox: mpsc::Receiver<Command>,
    events: tokio_mpsc::Sender<Event>,
) {
    while let Ok(first) = inbox.recv() {
        // Group commit: every append already waiting goes in this sync.
        let mut pending = vec![first];
        while let Ok(next) = inbox.try_recv() {
            pending.push(next);
        }
        let mut durable = None;
        let mut deferred = Vec::new();
        for command in pending {
            match command {
                Command::Append { first, records } => {
                    let staged = log
                        .begin(first)
                        .and_then(|()| records.iter().try_for_each(|r| log.stage(r)));
                    if let Err(e) = staged.and_then(|()| log.commit(false).map(|_| ())) {
                        let _ = events.blocking_send(Event::Failed(e));
                        return;
                    }
                    durable = Some(first.saturating_add(records.len() as u64).saturating_sub(1));
                }
                other => deferred.push(other),
            }
        }
        if let Some(last) = durable {
            if let Err(e) = log.commit(true) {
                // The log can no longer say what is durable (storage §99).
                let _ = events.blocking_send(Event::Failed(e));
                return;
            }
            let durable = Event::Durable {
                lsn: last,
                bytes_floor: bytes_floor(&log, retain_bytes),
            };
            if events.blocking_send(durable).is_err() {
                return;
            }
        }
        // Reads after the appends that came before them.
        for command in deferred {
            let event = match command {
                Command::Read { to, from } => Event::Read {
                    to,
                    from,
                    batch: read_batch(&log, from),
                },
                Command::Range { from, upto } => Event::Range(read_range(&log, from, upto)),
                Command::Snapshot { lsn, bytes } => match write_snapshot(&directory, &bytes) {
                    Ok(()) => Event::Snapshot(lsn),
                    Err(e) => Event::Failed(storage::StorageError::Io(e)),
                },
                Command::Compact { floor } => match log.remove_through(floor) {
                    Ok(_) => continue,
                    Err(e) => Event::Failed(e),
                },
                Command::Promise { slot } => match write_promise(&directory, slot) {
                    Ok(()) => Event::Promised(slot),
                    Err(e) => Event::Failed(storage::StorageError::Io(e)),
                },
                Command::Truncate { after } => match truncate(&mut log, after) {
                    Ok(chain) => Event::Truncated(chain),
                    Err(e) => Event::Failed(e),
                },
                Command::Append { .. } => continue,
            };
            if events.blocking_send(event).is_err() {
                return;
            }
        }
    }
}

fn read_batch(log: &Segments<OsDisk>, from: u64) -> Option<Vec<Vec<u8>>> {
    let segment = log.segment_of(from)?;
    let mut batch = Vec::new();
    let mut remaining: Option<u32> = None;
    log.read(segment, &mut |record| {
        let Ok(r) = decode(record) else { return };
        match remaining {
            None if r.lsn == from && r.kind == Kind::BatchHeader => {
                remaining = BatchHeader::decode(r.body).ok().map(|h| h.records);
                batch.push(record.to_vec());
            }
            Some(n) if n > 0 && r.lsn == from.saturating_add(batch.len() as u64) => {
                remaining = Some(n.saturating_sub(1));
                batch.push(record.to_vec());
            }
            _ => {}
        }
    })
    .ok()?;
    (remaining == Some(0)).then_some(batch)
}

fn read_range(log: &Segments<OsDisk>, from: u64, upto: u64) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    // From the segment that holds `from`: a restart reads the log back in
    // ranges, and starting each at the first segment made it quadratic.
    let start = log.segment_of(from).unwrap_or(0);
    let keys: Vec<u64> = log.keys().filter(|&k| k >= start).collect();
    for key in keys {
        if out.len() >= MAX_RANGE {
            break;
        }
        let _ = log.read(key, &mut |record| {
            if out.len() >= MAX_RANGE {
                return;
            }
            if let Ok(r) = decode(record) {
                // Contiguous from `from`: the next one wanted, and no further.
                if r.lsn == from.saturating_add(out.len() as u64) && r.lsn <= upto {
                    out.push(record.to_vec());
                }
            }
        });
    }
    out
}
