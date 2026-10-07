//! The log on disk, on a thread of its own: storage does blocking file I/O,
//! and `sync_data` never runs on the node loop (CLAUDE.md §3, storage).

use std::path::PathBuf;
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
}

pub enum Event {
    Durable(u64),
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

/// Opens the log, checks it is one chain, cuts a torn batch, and starts the
/// thread. Returns where the log ends.
pub fn start(
    directory: PathBuf,
    limits: SegmentLimits,
    events: tokio_mpsc::Sender<Event>,
) -> Result<(Chain, mpsc::Sender<Command>, std::thread::JoinHandle<()>), NodeError> {
    std::fs::create_dir_all(&directory)?;
    // Verified as it is read: recovery holds one record at a time, not the
    // log (CLAUDE.md §7).
    let mut verifier: Option<Verifier> = None;
    let mut broken: Option<ChainError> = None;
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
    let (commands, inbox) = mpsc::channel::<Command>();
    let thread = std::thread::Builder::new()
        .name("wal".into())
        .spawn(move || run(log, inbox, events))?;
    Ok((tail.chain, commands, thread))
}

fn run(
    mut log: Segments<OsDisk>,
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
            if events.blocking_send(Event::Durable(last)).is_err() {
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
    let keys: Vec<u64> = log.keys().collect();
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
