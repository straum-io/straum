//! The sequencer and the replica, sans-IO (ADR-0008 step 2; unified-stream
//! §2.1–§2.3, §2.6).
//!
//! One node owns the slot and orders the log; every other voting node
//! replicates it. Both are state machines: they take events (a proposal, a
//! message, "the log is durable through n", a timer) and leave effects (write
//! this batch, send this message, read this batch back) for a driver to carry
//! out. Nothing here touches a socket, a file or a clock, so the simulation
//! drives exactly the code the runtime will.
//!
//! What this step builds, and what it leaves for later steps:
//!
//! * The slot owner is fixed by configuration. Taking over a slot (promises,
//!   truncation by the slot table) is step 5, handoff step 6.
//! * Star topology: the sequencer sends to every replica directly (§2.6).
//! * Commit is a majority of voters durable (§2.3), as a prefix: the highest
//!   lsn that a majority holds durably. It only grows.
//! * The sequencer ships only what is durable in its own log. A replica's log
//!   is therefore always a prefix of the sequencer's durable log, and a
//!   sequencer that restarts after a power loss has lost nothing a replica
//!   holds — no divergent suffix can exist with one fixed owner. Shipping
//!   before the local sync, in parallel with it, needs the takeover of step 5
//!   on every sequencer restart (D-58).
//! * A replica accepts a batch only from the slot's owner, only in the
//!   current slot, only as the continuation of its own log, and only if the
//!   `prev_hash` chain holds (§2.2, invariant 5).
//!
//! Memory is bounded: proposals waiting for a batch (`max_queue`), batches
//! kept for shipping from memory (`recent_batches`), and what one replica may
//! have been sent beyond what it has made durable (`window`). A replica
//! further behind is caught up from the log, one batch at a time.

pub mod wire;

use std::collections::VecDeque;

use journal::{Chain, ChainError, Entry, Kind, Verifier, decode};

pub type NodeId = u8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// The voting nodes, this one included.
    pub nodes: Vec<NodeId>,
    /// The sequencer of `slot`.
    pub owner: NodeId,
    pub slot: u64,
    /// Lsns a replica may have been sent beyond what it reported durable.
    pub window: u64,
    /// Proposals waiting for the next batch.
    pub max_queue: usize,
    /// Proposals in one batch.
    pub max_batch: usize,
    /// Batches the sequencer keeps to ship from memory.
    pub recent_batches: usize,
    /// Lsns appended beyond the commit point. At the bound the sequencer
    /// makes no new batch: proposals wait in the queue, which refuses when
    /// full, so the uncommitted tail a driver holds is bounded.
    pub max_uncommitted: u64,
}

impl Config {
    /// The smallest number of voters that is a majority.
    pub fn majority(&self) -> usize {
        self.nodes.len() / 2 + 1
    }
}

/// What travels between nodes. The wire encoding comes with the runtime
/// driver (step 4); here a message is a value.
///
/// The one thing required of the transport: nothing addressed to a process
/// reaches its successor after a restart — a connection does not outlive
/// the process, as with TCP. A position report from before a sequencer
/// restarted could otherwise send it to the log for records the replica
/// holds and the previous process compacted, losing the replica for nothing
/// (found by the simulation, seed 72). What a dead process sent may still
/// arrive at the others; loss and reordering are tolerated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// Sequencer → replica: one complete batch, record by record as the
    /// sequencer's log holds it, and the commit point. No records: a
    /// heartbeat that carries the commit point and asks for an `Ack`.
    Append {
        slot: u64,
        records: Vec<Vec<u8>>,
        commit: u64,
    },
    /// Replica → sequencer: its log ends at `end`, durable through `durable`.
    Ack { slot: u64, end: u64, durable: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// Write these records to the local log as one batch. `first` is the lsn
    /// of the first; report durability with `on_durable`.
    Append {
        first: u64,
        records: Vec<Vec<u8>>,
    },
    Send {
        to: NodeId,
        message: Message,
    },
    /// Sequencer: read the batch that starts at `from` back from the local
    /// log and hand it to `on_read`, or call `on_read_missing` if the log no
    /// longer has it.
    Read {
        to: NodeId,
        from: u64,
    },
    /// Sequencer: `to` is shipped from memory again; a reader held for it can
    /// be released.
    ReadDone {
        to: NodeId,
    },
    /// Everything through this lsn is committed.
    Committed(u64),
    /// Sequencer: `to` needs records that were compacted away. It is no longer
    /// shipped to, and a reader held for it can go; bringing it back needs a
    /// copy of the state (D-57).
    Lost {
        to: NodeId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClusterError {
    #[error("the proposal queue is full")]
    Full,
    #[error("{0}")]
    Chain(#[from] ChainError),
}

/// A record waiting for its batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    pub kind: Kind,
    pub properties: Vec<u8>,
    pub body: Vec<u8>,
}

/// The longest wait between resends to a stalled replica, in heartbeats.
const MAX_BACKOFF: u32 = 8;

#[derive(Debug, Clone)]
struct Peer {
    id: NodeId,
    /// Heard from since this sequencer started: until then its position is
    /// unknown and nothing is shipped to it.
    known: bool,
    /// The last lsn sent, or that it reported having if that is more.
    end: u64,
    /// The end it last reported. Below `end` at a heartbeat, what was sent
    /// since is presumed lost and sent again.
    reported: u64,
    /// `reported` at the last heartbeat: a catch-up that has not moved over a
    /// whole period gives up its hold on the log.
    reported_at_tick: u64,
    /// Heartbeats without progress since the last resend, and how many to
    /// wait before the next: doubled after each resend, back to one on
    /// progress (exponential backoff).
    stalled_ticks: u32,
    backoff: u32,
    durable: u64,
    /// A read from the log is outstanding for it, starting here.
    reading: Option<u64>,
    /// Shipped from the log rather than from memory since the last
    /// `ReadDone`.
    from_log: bool,
    lost: bool,
}

#[derive(Debug, Clone)]
struct Batch {
    first: u64,
    last: u64,
    records: Vec<Vec<u8>>,
}

fn last_lsn(records: &[Vec<u8>]) -> Option<u64> {
    records
        .last()
        .and_then(|record| decode(record).ok())
        .map(|record| record.lsn)
}

#[derive(Debug)]
pub struct Sequencer {
    config: Config,
    /// After the last batch handed to the log.
    chain: Chain,
    durable: u64,
    commit: u64,
    peers: Vec<Peer>,
    queue: VecDeque<Proposal>,
    recent: VecDeque<Batch>,
    effects: Vec<Effect>,
}

impl Sequencer {
    /// A sequencer over a log that ends with `chain` — recovered, its torn
    /// batch cut and, being reopened, durable (D-43).
    pub fn new(config: Config, chain: Chain) -> Self {
        let peers = config
            .nodes
            .iter()
            .filter(|&&id| id != config.id)
            .map(|&id| Peer {
                id,
                known: false,
                end: 0,
                reported: 0,
                reported_at_tick: 0,
                stalled_ticks: 0,
                backoff: 1,
                durable: 0,
                reading: None,
                from_log: false,
                lost: false,
            })
            .collect();
        Self {
            durable: chain.next_lsn().saturating_sub(1),
            config,
            chain,
            commit: 0,
            peers,
            queue: VecDeque::new(),
            recent: VecDeque::new(),
            effects: Vec::new(),
        }
    }

    pub fn commit(&self) -> u64 {
        self.commit
    }

    pub fn durable(&self) -> u64 {
        self.durable
    }

    /// The last lsn handed to the log.
    pub fn end(&self) -> u64 {
        self.chain.next_lsn().saturating_sub(1)
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    /// How far the local log may be compacted (D-57 A): through the commit
    /// point, but not past the slowest replica still being served, so a
    /// replica that is only behind is caught up from the log — unless that
    /// would keep more than the byte bound. `bytes_floor` is the lowest lsn
    /// the bound lets the log keep, which only the driver can know (it sees
    /// the segment sizes); a replica below a floor forced by it is lost
    /// (`Effect::Lost` at its next read) and needs a copy of the state.
    ///
    /// A replica this sequencer has not heard from since it started counts
    /// as being at 0: only the byte bound compacts past it.
    pub fn compaction_floor(&self, bytes_floor: u64) -> u64 {
        let slowest = self
            .peers
            .iter()
            .filter(|peer| !peer.lost)
            .map(|peer| if peer.known { peer.durable } else { 0 })
            .min()
            .unwrap_or(u64::MAX);
        self.commit.min(slowest.max(bytes_floor))
    }

    /// The durable position this sequencer knows for replica `id`, unless it
    /// has been lost.
    pub fn replica_durable(&self, id: NodeId) -> Option<u64> {
        self.peers
            .iter()
            .find(|peer| peer.id == id && !peer.lost)
            .map(|peer| if peer.known { peer.durable } else { 0 })
    }

    /// Queues a record for the next batch. Refused when the queue is full;
    /// the refused proposal is the caller's to hold or to give up on.
    pub fn propose(&mut self, proposal: Proposal) -> Result<(), ClusterError> {
        if self.queue.len() >= self.config.max_queue {
            return Err(ClusterError::Full);
        }
        self.queue.push_back(proposal);
        Ok(())
    }

    /// Makes one batch of what is queued and hands it to the log.
    pub fn flush(&mut self) -> Result<(), ClusterError> {
        if self.queue.is_empty() {
            return Ok(());
        }
        if self.end().saturating_sub(self.commit) >= self.config.max_uncommitted {
            return Ok(());
        }
        let take = self.queue.len().min(self.config.max_batch);
        let first = self.chain.next_lsn();
        let mut records = Vec::with_capacity(take.saturating_add(1));
        {
            let entries: Vec<Entry<'_>> = self
                .queue
                .iter()
                .take(take)
                .map(|proposal| Entry {
                    kind: proposal.kind,
                    properties: &proposal.properties,
                    body: &proposal.body,
                })
                .collect();
            self.chain.batch(self.config.slot, &entries, &mut records)?;
        }
        self.queue.drain(..take);
        let last = self.end();
        self.effects.push(Effect::Append {
            first,
            records: records.clone(),
        });
        self.recent.push_back(Batch {
            first,
            last,
            records,
        });
        while self.recent.len() > self.config.recent_batches {
            self.recent.pop_front();
        }
        Ok(())
    }

    /// The local log is durable through `lsn`.
    pub fn on_durable(&mut self, lsn: u64) {
        self.durable = self.durable.max(lsn.min(self.end()));
        self.advance_commit();
        self.ship_all();
    }

    pub fn on_message(&mut self, from: NodeId, message: Message) {
        let Message::Ack { slot, end, durable } = message else {
            return;
        };
        if slot != self.config.slot {
            return;
        }
        let Some(peer) = self.peers.iter_mut().find(|peer| peer.id == from) else {
            return;
        };
        // Durable only grows: what a replica synced survives its restarts,
        // and an older report overtaken by a newer one says less, not other.
        peer.durable = peer.durable.max(durable.min(end));
        peer.reported = end;
        // A report never makes this resend: with messages reordered, every
        // stale batch would draw a report and every report a window of
        // resends, a storm the simulation found. Going back is the
        // heartbeat's job (`tick`); a report only moves the position
        // forward, or sets it the first time.
        if !peer.known || end > peer.end {
            peer.end = end;
            // A read in flight no longer continues it; its result is ignored.
            peer.reading = None;
        }
        peer.known = true;
        self.advance_commit();
        if let Some(index) = self.peers.iter().position(|peer| peer.id == from) {
            self.ship(index);
        }
    }

    /// The batch that starts where the read asked, from the local log.
    pub fn on_read(&mut self, to: NodeId, records: Vec<Vec<u8>>) {
        let Some(index) = self.peers.iter().position(|peer| peer.id == to) else {
            return;
        };
        let commit = self.commit;
        let slot = self.config.slot;
        let peer = &mut self.peers[index];
        let first = records
            .first()
            .and_then(|record| decode(record).ok())
            .map(|record| record.lsn);
        if peer.reading.take().is_none() || first != Some(peer.end.saturating_add(1)) {
            // Stale: the replica's position moved while the read was out.
            self.ship(index);
            return;
        }
        let Some(last) = last_lsn(&records) else {
            return;
        };
        peer.end = last;
        peer.from_log = true;
        self.effects.push(Effect::Send {
            to,
            message: Message::Append {
                slot,
                records,
                commit,
            },
        });
        self.ship(index);
    }

    /// The log no longer holds the batch at `from` that a read for `to` asked
    /// for. Only the read still outstanding counts: one the replica's position
    /// overtook while it waited is stale, and losing a replica on it would
    /// lose one that is fine (found by the simulation, seed 7).
    pub fn on_read_missing(&mut self, to: NodeId, from: u64) {
        let Some(index) = self.peers.iter().position(|peer| peer.id == to) else {
            return;
        };
        if self.peers[index].reading != Some(from) {
            self.ship(index);
            return;
        }
        if let Some(peer) = self.peers.get_mut(index) {
            peer.reading = None;
            peer.from_log = false;
            peer.lost = true;
            self.effects.push(Effect::Lost { to });
        }
    }

    /// The heartbeat timer: every replica hears the commit point and answers
    /// with its position. A replica that has not reported what it was sent
    /// is sent it again from where it reported (go-back-N). This is the only
    /// place a resend starts, so a lost message costs one heartbeat period
    /// and reordered ones cost nothing.
    ///
    /// A replica caught up from the log that reported no progress over a
    /// whole period releases the segment it was being read from
    /// (`ReadDone`): a replica that died half way must not hold the log's
    /// disk space. If compaction takes that segment meanwhile, the next read
    /// finds it gone and the replica is lost (D-57).
    pub fn tick(&mut self) {
        for peer in &mut self.peers {
            // Never below what it reported durable: a late report from before
            // can say less than the replica holds, and going back that far
            // asks the log for records that may have been compacted, losing
            // a replica that only had a stale report (found by the
            // simulation, seed 28).
            let resume = peer.reported.max(peer.durable);
            // Only a replica that reported no progress is sent again what it
            // has not reported, and each resend without progress doubles the
            // wait for the next (capped). Resending a window at every
            // heartbeat to a replica that was only slow piled duplicates onto
            // a slow network until it carried little else (found by the
            // gateway simulation). A replica stuck on a lost message stops
            // advancing, so it is still repaired, within a bounded wait.
            if peer.reported == peer.reported_at_tick {
                peer.stalled_ticks = peer.stalled_ticks.saturating_add(1);
            } else {
                peer.stalled_ticks = 0;
                peer.backoff = 1;
            }
            if peer.known && !peer.lost && resume < peer.end && peer.stalled_ticks >= peer.backoff {
                peer.end = resume;
                peer.reading = None;
                peer.stalled_ticks = 0;
                peer.backoff = peer.backoff.saturating_mul(2).min(MAX_BACKOFF);
            }
            if peer.from_log && peer.reported == peer.reported_at_tick {
                peer.from_log = false;
                self.effects.push(Effect::ReadDone { to: peer.id });
            }
            peer.reported_at_tick = peer.reported;
        }
        for peer in &self.peers {
            if peer.lost {
                continue;
            }
            self.effects.push(Effect::Send {
                to: peer.id,
                message: Message::Append {
                    slot: self.config.slot,
                    records: Vec::new(),
                    commit: self.commit,
                },
            });
        }
        self.ship_all();
    }

    fn advance_commit(&mut self) {
        let mut durable: Vec<u64> = self.peers.iter().map(|peer| peer.durable).collect();
        durable.push(self.durable);
        durable.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = durable
            .get(self.config.majority().saturating_sub(1))
            .copied()
            .unwrap_or(0);
        if candidate > self.commit {
            self.commit = candidate;
            self.effects.push(Effect::Committed(candidate));
        }
    }

    fn ship_all(&mut self) {
        for index in 0..self.peers.len() {
            self.ship(index);
        }
    }

    /// Sends `peer` what comes next for it, from memory where possible, as
    /// far as its window allows.
    fn ship(&mut self, index: usize) {
        loop {
            let Some(peer) = self.peers.get_mut(index) else {
                return;
            };
            if !peer.known
                || peer.lost
                || peer.reading.is_some()
                || peer.end >= self.durable
                || peer.end.saturating_sub(peer.durable) >= self.config.window
            {
                return;
            }
            let from = peer.end.saturating_add(1);
            match self.recent.iter().find(|batch| batch.first == from) {
                Some(batch) if batch.last <= self.durable => {
                    peer.end = batch.last;
                    if std::mem::take(&mut peer.from_log) {
                        self.effects.push(Effect::ReadDone { to: peer.id });
                    }
                    self.effects.push(Effect::Send {
                        to: peer.id,
                        message: Message::Append {
                            slot: self.config.slot,
                            records: batch.records.clone(),
                            commit: self.commit,
                        },
                    });
                }
                Some(_) => return,
                None => {
                    peer.reading = Some(from);
                    self.effects.push(Effect::Read { to: peer.id, from });
                    return;
                }
            }
        }
    }
}

#[derive(Debug)]
pub struct Replica {
    config: Config,
    /// At the end of the log, after its last complete batch.
    verifier: Verifier,
    end: u64,
    durable: u64,
    commit: u64,
    refused: u64,
    effects: Vec<Effect>,
}

impl Replica {
    /// A replica over a log that ends with `chain`, recovered and durable.
    pub fn new(config: Config, chain: Chain) -> Self {
        let end = chain.next_lsn().saturating_sub(1);
        Self {
            config,
            verifier: Verifier::new(chain),
            end,
            durable: end,
            commit: 0,
            refused: 0,
            effects: Vec::new(),
        }
    }

    pub fn end(&self) -> u64 {
        self.end
    }

    pub fn durable(&self) -> u64 {
        self.durable
    }

    /// The commit point as far as this replica knows it, never beyond its
    /// own log.
    pub fn commit(&self) -> u64 {
        self.commit
    }

    /// Batches refused: from a node that does not own the slot, from another
    /// slot, or breaking the chain.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    pub fn on_message(&mut self, from: NodeId, message: Message) {
        let Message::Append {
            slot,
            records,
            commit,
        } = message
        else {
            return;
        };
        // Only the owner of the current slot writes to this log (§2.2).
        if from != self.config.owner || slot != self.config.slot {
            self.refused = self.refused.saturating_add(1);
            return;
        }
        if let Some(first) = records.first() {
            let continues =
                decode(first).is_ok_and(|record| record.lsn == self.end.saturating_add(1));
            // Anything else is a duplicate or arrived out of order: the Ack
            // below tells the sequencer where this log really ends.
            if continues {
                self.append(records);
            }
        }
        let known = commit.min(self.end);
        if known > self.commit {
            self.commit = known;
            self.effects.push(Effect::Committed(known));
        }
        self.ack();
    }

    fn append(&mut self, records: Vec<Vec<u8>>) {
        let mut verifier = self.verifier.clone();
        let chained = records.iter().all(|record| verifier.feed(record).is_ok());
        // A batch is taken whole or not at all: half of one is never a
        // position this log can stand at.
        if !chained || verifier.clone().finish().partial != 0 {
            self.refused = self.refused.saturating_add(1);
            return;
        }
        let Some(last) = last_lsn(&records) else {
            return;
        };
        let first = self.end.saturating_add(1);
        self.verifier = verifier;
        self.end = last;
        self.effects.push(Effect::Append { first, records });
    }

    /// The local log is durable through `lsn`.
    pub fn on_durable(&mut self, lsn: u64) {
        self.durable = self.durable.max(lsn.min(self.end));
        self.ack();
    }

    fn ack(&mut self) {
        self.effects.push(Effect::Send {
            to: self.config.owner,
            message: Message::Ack {
                slot: self.config.slot,
                end: self.end,
                durable: self.durable,
            },
        });
    }
}
