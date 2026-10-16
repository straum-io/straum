//! The log on disk, on a thread of its own: storage does blocking file I/O,
//! and `sync_data` never runs on the node loop (CLAUDE.md §3, storage).
//!
//! It also keeps the node's snapshot (D-67 B): one file, replaced
//! atomically, written before any segment below it is removed, so a start
//! always finds a snapshot and the log after it with no gap between them.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use journal::{Chain, ChainError, Kind, Verifier, decode};
use storage::{OsDisk, SegmentLimits, Segments};
use tokio::sync::mpsc as tokio_mpsc;
use tokio::time::Instant;

use crate::NodeError;

pub enum Command {
    /// One batch, as the core handed it over.
    Append { first: u64, records: Vec<Vec<u8>> },
    /// Whole batches from the one at `from`, none past `upto`, at most
    /// `bytes` past the first: for catch-up (D-81 A).
    Read {
        to: u8,
        from: u64,
        upto: u64,
        bytes: usize,
    },
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
        /// The group's start, its writes done, its sync done (D-136 A):
        /// two clock reads a sync, against the sync itself.
        started: Instant,
        written: Instant,
        synced: Instant,
    },
    Snapshot(u64),
    /// Compacted: the log now holds nothing at or below this lsn — the
    /// floor reached, which whole segments can leave below the one asked.
    Compacted(u64),
    Promised(u64),
    Truncated(Chain),
    Read {
        to: u8,
        from: u64,
        batch: Option<Vec<Vec<u8>>>,
        /// When this thread took the read up and finished it (D-136 A).
        started: Instant,
        done: Instant,
        scan: Scan,
    },
    /// Committed records read back for apply; when this thread took the
    /// read up and finished it, and the records and bytes it visited to
    /// get them (D-139 A).
    Range {
        records: Vec<Vec<u8>>,
        started: Instant,
        done: Instant,
        visited: u64,
        visited_bytes: u64,
    },
    Failed(storage::StorageError),
}

/// Where a read's time went (D-139 A): records and bytes scanned before it
/// reached its start and after it was complete — the replay goes on to the
/// segment's end — and when it reached and completed it.
#[derive(Debug, Clone, Copy, Default)]
pub struct Scan {
    pub before: u64,
    pub before_bytes: u64,
    pub reached: Option<Instant>,
    pub finished: Option<Instant>,
    pub after: u64,
    pub after_bytes: u64,
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

/// Cuts the log back to the last batch boundary at or below `after` and
/// returns the chain there: the batch header after the boundary names the
/// hash, the record at the boundary the slot. `after` is a boundary in the
/// log the voter compared with; here it can fall inside a batch.
fn truncate(log: &mut Segments<OsDisk>, after: u64) -> Result<Chain, storage::StorageError> {
    let next = after.saturating_add(1);
    // Segments start at batch boundaries: the batch holding `next` starts
    // in the segment that holds it.
    let segment = log
        .segment_of(next)
        .ok_or(storage::StorageError::NotFramedRecords)?;
    let mut header: Option<(u64, u128)> = None;
    log.read(segment, &mut |record| {
        if let Ok(r) = decode(record) {
            if r.kind == Kind::BatchHeader && r.lsn <= next {
                header = Some((r.lsn, r.prev_hash));
            }
        }
    })?;
    let (start, prev_hash) = header.ok_or(storage::StorageError::NotFramedRecords)?;
    let boundary = start.saturating_sub(1);
    let chain = if boundary == 0 {
        Chain::genesis()
    } else {
        let (at, _, _) = read_range(log, boundary, boundary);
        let slot = at
            .first()
            .and_then(|r| decode(r).ok())
            .map(|r| r.slot)
            .ok_or(storage::StorageError::NotFramedRecords)?;
        Chain::resume(prev_hash, boundary, slot)
    };
    log.truncate_after(boundary)?;
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
        // Slot 0, not the first record's: `prev_hash` already binds the
        // first batch to what came before, and complete records set the slot
        // as they are fed. Taken from a first batch that turns out torn, an
        // empty log claimed that batch's slot and refused for ever the
        // committed log of an older one (found by the cluster simulation,
        // seed 4045, D-82).
        let verifier = verifier.get_or_insert_with(|| match decode(record) {
            Ok(first) => Verifier::new(Chain::resume(
                first.prev_hash,
                first.lsn.saturating_sub(1),
                0,
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
        let started = Instant::now();
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
            let written = Instant::now();
            if let Err(e) = log.commit(true) {
                // The log can no longer say what is durable (storage §99).
                let _ = events.blocking_send(Event::Failed(e));
                return;
            }
            let durable = Event::Durable {
                lsn: last,
                bytes_floor: bytes_floor(&log, retain_bytes),
                started,
                written,
                synced: Instant::now(),
            };
            if events.blocking_send(durable).is_err() {
                return;
            }
        }
        // Reads after the appends that came before them.
        for command in deferred {
            let event = match command {
                Command::Read {
                    to,
                    from,
                    upto,
                    bytes,
                } => {
                    let started = Instant::now();
                    let (batch, scan) = read_batches(&log, from, upto, bytes);
                    Event::Read {
                        to,
                        from,
                        batch,
                        started,
                        done: Instant::now(),
                        scan,
                    }
                }
                Command::Range { from, upto } => {
                    let started = Instant::now();
                    let (records, visited, visited_bytes) = read_range(&log, from, upto);
                    Event::Range {
                        records,
                        started,
                        done: Instant::now(),
                        visited,
                        visited_bytes,
                    }
                }
                Command::Snapshot { lsn, bytes } => match write_snapshot(&directory, &bytes) {
                    Ok(()) => Event::Snapshot(lsn),
                    Err(e) => Event::Failed(storage::StorageError::Io(e)),
                },
                Command::Compact { floor } => match log.remove_through(floor) {
                    Ok(_) => Event::Compacted(
                        log.keys().next().map_or(0, |first| first.saturating_sub(1)),
                    ),
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

/// Whole batches from the one at `from`, none past `upto`, under `bytes`
/// past the first (`cluster::ReadBatches`, D-81 A); within the segment
/// that holds `from`.
fn read_batches(
    log: &Segments<OsDisk>,
    from: u64,
    upto: u64,
    bytes: usize,
) -> (Option<Vec<Vec<u8>>>, Scan) {
    let mut scan = Scan::default();
    let Some(segment) = log.segment_of(from) else {
        return (None, scan);
    };
    let mut batches = cluster::ReadBatches::new(from, upto, bytes);
    // Counted on every read, not only traced ones: two adds and a branch
    // a record, against the record's checksum.
    let read = log.read(segment, &mut |record| {
        let len = record.len() as u64;
        if batches.done() {
            scan.after += 1;
            scan.after_bytes += len;
            return;
        }
        batches.feed(record);
        if !batches.started() {
            scan.before += 1;
            scan.before_bytes += len;
        } else if scan.reached.is_none() {
            scan.reached = Some(Instant::now());
        }
        if batches.done() {
            scan.finished = Some(Instant::now());
        }
    });
    if read.is_err() {
        return (None, scan);
    }
    (batches.finish(), scan)
}

fn read_range(log: &Segments<OsDisk>, from: u64, upto: u64) -> (Vec<Vec<u8>>, u64, u64) {
    let mut out = Vec::new();
    let (mut visited, mut visited_bytes) = (0u64, 0u64);
    // From the segment that holds `from`: a restart reads the log back in
    // ranges, and starting each at the first segment made it quadratic.
    let start = log.segment_of(from).unwrap_or(0);
    let keys: Vec<u64> = log.keys().filter(|&k| k >= start).collect();
    for key in keys {
        if out.len() >= MAX_RANGE {
            break;
        }
        let _ = log.read(key, &mut |record| {
            visited += 1;
            visited_bytes += record.len() as u64;
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
    (out, visited, visited_bytes)
}

#[cfg(test)]
mod tests {
    use journal::{Entry, Kind};

    use super::*;

    /// A cut inside a batch goes back to that batch's start, and the chain
    /// reported is the one after the batch before it.
    #[test]
    fn a_cut_inside_a_batch_rounds_down_to_its_start() {
        let dir = std::env::temp_dir().join(format!("straum-node-wal-cut-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let limits = SegmentLimits {
            wal: storage::WalLimits::default(),
            segment_bytes: 1 << 20,
        };
        let mut log = Segments::open(OsDisk, &dir, limits, &mut |_, _| {}).unwrap();
        let mut chain = Chain::genesis();
        let mut after_first = Chain::genesis();
        for batch in 0..3u8 {
            let entries = [
                Entry {
                    kind: Kind::Publish,
                    properties: &[],
                    body: &[batch, 1],
                },
                Entry {
                    kind: Kind::Publish,
                    properties: &[],
                    body: &[batch, 2],
                },
            ];
            let first = chain.next_lsn();
            let mut records = Vec::new();
            chain.batch(1, &entries, &mut records).unwrap();
            log.begin(first).unwrap();
            for record in &records {
                log.stage(record).unwrap();
            }
            log.commit(true).unwrap();
            if batch == 0 {
                after_first = chain;
            }
        }
        // Batches are lsns 1–3, 4–6, 7–9: 5 is inside the second.
        let cut = truncate(&mut log, 5).unwrap();
        assert_eq!(cut.next_lsn(), 4);
        assert_eq!(cut.hash(), after_first.hash());
        drop(log);
        let mut seen = Vec::new();
        Segments::open(OsDisk, &dir, limits, &mut |_, r| {
            seen.push(decode(r).unwrap().lsn)
        })
        .unwrap();
        assert_eq!(seen, vec![1, 2, 3]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A log holding only a torn batch of a newer slot recovers as an
    /// empty log of no slot: the committed log of an older slot must still
    /// chain onto it (D-82, cluster simulation seed 4045).
    #[test]
    fn a_torn_first_batch_leaves_a_log_of_no_slot() {
        let dir = std::env::temp_dir().join(format!("straum-node-wal-torn-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let limits = SegmentLimits {
            wal: storage::WalLimits::default(),
            segment_bytes: 1 << 20,
        };
        let mut log = Segments::open(OsDisk, &dir, limits, &mut |_, _| {}).unwrap();
        let mut chain = Chain::genesis();
        let entries = [
            Entry {
                kind: Kind::SlotStart,
                properties: &[],
                body: &[0; 8],
            },
            Entry {
                kind: Kind::Publish,
                properties: &[],
                body: &[1],
            },
        ];
        let mut records = Vec::new();
        chain.batch(5, &entries, &mut records).unwrap();
        log.begin(1).unwrap();
        // The header and one of its two records: torn.
        for record in &records[..2] {
            log.stage(record).unwrap();
        }
        log.commit(true).unwrap();
        drop(log);
        let (events, _rx) = tokio_mpsc::channel(8);
        let (recovered, wal, thread) = start(dir.clone(), limits, 1 << 20, events).unwrap();
        assert_eq!(recovered.chain, Chain::genesis());
        drop(wal);
        thread.join().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
