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
//! * [`Sequencer`] and [`Replica`] serve one slot whose owner is fixed;
//!   [`Voter`] (step 5) runs a node across slots: it follows the owner,
//!   takes the next slot over when that owner falls silent (durable
//!   promises from a majority, the longest of their logs, `SlotStart`), and
//!   cuts a tail from an older slot where the slot table proves it differs.
//!   Planned handoff (`SlotEnd`) is step 6.
//! * Star topology: the sequencer sends to every replica directly (§2.6).
//! * Commit is a majority of voters durable (§2.3), as a prefix: the highest
//!   lsn that a majority holds durably. It only grows.
//! * The sequencer ships only what is durable in its own log. A replica's log
//!   is therefore always a prefix of the sequencer's durable log within a
//!   slot. Across slots a replica may hold a tail of an older slot the new
//!   owner's log does not have; its durability counts toward commit only
//!   once its log ends in the sequencer's slot. A restarted [`Voter`] never
//!   leads its old slot again, which is what would let a sequencer ship
//!   before its own sync (D-58); it still syncs first.
//! * A replica accepts a batch only from the slot's owner, only in the
//!   current slot, only as the continuation of its own log, and only if the
//!   `prev_hash` chain holds (§2.2, invariant 5).
//!
//! Memory is bounded: proposals waiting for a batch (`max_queue`), batches
//! kept for shipping from memory (`recent_batches`), and what one replica may
//! have been sent beyond what it has made durable (`window`). A replica
//! further behind is caught up from the log, one batch at a time.

mod voter;
pub mod wire;

pub use voter::Voter;

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
    /// Heartbeats without hearing from the slot's owner before the next
    /// node in the schedule takes the slot over ([`Voter`], step 5); each
    /// further period of silence passes the turn to the node after it.
    /// Time only starts a takeover; the promises decide it (§1).
    pub takeover_ticks: u32,
}

impl Config {
    /// The smallest number of voters that is a majority.
    pub fn majority(&self) -> usize {
        self.nodes.len() / 2 + 1
    }

    /// The node that may write `slot` (§2.2): `owner` for `self.slot`, then
    /// the voters in turn. No one owns a slot before the first.
    pub fn owner_of(&self, slot: u64) -> Option<NodeId> {
        let count = u64::try_from(self.nodes.len()).ok().filter(|&n| n > 0)?;
        let first = self.nodes.iter().position(|&id| id == self.owner)?;
        let steps = slot.checked_sub(self.slot)?;
        let index = (u64::try_from(first).ok()? + steps % count) % count;
        self.nodes.get(usize::try_from(index).ok()?).copied()
    }
}

/// The last lsn of the latest slot at or below `slot` in a log whose slot
/// table is `slots` (slot, first lsn; ascending) and which ends at `end`:
/// where a log whose last record is in `slot` stops agreeing with this one,
/// at the latest (Kafka's leader-epoch rule).
pub(crate) fn end_of_slot(slots: &[(u64, u64)], slot: u64, end: u64) -> u64 {
    slots
        .iter()
        .find(|&&(s, _)| s > slot)
        .map_or(end, |&(_, first)| first.saturating_sub(1))
}

/// The slot of the record at `lsn` by the slot table; 0 for no record.
pub(crate) fn slot_at(slots: &[(u64, u64)], lsn: u64) -> u64 {
    slots
        .iter()
        .take_while(|&&(_, first)| first <= lsn)
        .last()
        .map_or(0, |&(slot, _)| slot)
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
    /// sequencer's log holds it, following lsn `prev`, whose record is in
    /// `prev_slot`, and the commit point. No records: a heartbeat that
    /// carries the commit point and asks for an `Ack`.
    Append {
        slot: u64,
        prev: u64,
        prev_slot: u64,
        records: Vec<Vec<u8>>,
        commit: u64,
    },
    /// Replica → sequencer: its log ends at `end`, whose record is in
    /// `last_slot`, durable through `durable`. `cut`: it cut its log back
    /// and has taken nothing since — its position went back, which no other
    /// report may move it to: a stale one could, and lose it.
    Ack {
        slot: u64,
        last_slot: u64,
        end: u64,
        durable: u64,
        cut: bool,
    },
    /// Candidate → every voter: promise to accept nothing of a slot below
    /// `slot`, and say where your log ends (§2.4, ungraceful).
    Promise { slot: u64 },
    /// The promise, durable before it is sent, and the log's end.
    Promised { slot: u64, last_slot: u64, end: u64 },
    /// Candidate → the voter with the longest log: what follows mine?
    Fetch { slot: u64, last_slot: u64, end: u64 },
    /// One batch following lsn `after`, whose record is in `after_slot`.
    /// Where the asker's log parts from that, it cuts back first. No records
    /// at the asker's end: nothing more.
    Fetched {
        slot: u64,
        /// The asker's end this answers: an answer to an older question is
        /// ignored.
        asked: u64,
        after: u64,
        after_slot: u64,
        records: Vec<Vec<u8>>,
    },
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
    /// Make "this node accepts nothing below `slot`" durable, then call
    /// [`Voter::on_promised`]. Nothing that depends on it is sent before
    /// (§2.2).
    Promise {
        slot: u64,
    },
    /// Cut the local log back to end at `after` — a tail from an older slot
    /// that the new owner's log does not have, never below the commit point —
    /// then call [`Voter::on_truncated`] with the chain there.
    Truncate {
        after: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ClusterError {
    #[error("the proposal queue is full")]
    Full,
    #[error("this node is not the slot's sequencer")]
    NotLeader,
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
    /// Its last report ended in this sequencer's slot: its log is a prefix
    /// of this one, and only then does its durability count toward commit.
    /// A log ending in an older slot may hold records this one does not.
    in_slot: bool,
    /// Its log ends in a slot this one does not have: where the two agree
    /// is not known yet, and no point is known to be a batch boundary in
    /// both. It gets only heartbeats, which carry the slot at its position,
    /// until it cuts back into a slot both logs share.
    probing: bool,
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
    /// The local log's slot table: (slot, first lsn), ascending.
    slots: Vec<(u64, u64)>,
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
        let durable = chain.next_lsn().saturating_sub(1);
        let slots = if durable > 0 {
            vec![(chain.slot(), 1)]
        } else {
            Vec::new()
        };
        Self::with(config, chain, durable, 0, slots)
    }

    /// A sequencer taking over a log that ends with `chain`, durable through
    /// `durable`, known committed through `commit`, with slot table `slots`.
    pub fn with(
        config: Config,
        chain: Chain,
        durable: u64,
        commit: u64,
        slots: Vec<(u64, u64)>,
    ) -> Self {
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
                in_slot: false,
                probing: false,
                reading: None,
                from_log: false,
                lost: false,
            })
            .collect();
        Self {
            durable,
            config,
            slots,
            chain,
            commit,
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

    pub fn slot(&self) -> u64 {
        self.config.slot
    }

    /// After the last batch handed to the log.
    pub fn chain(&self) -> Chain {
        self.chain
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
        if self.end().saturating_sub(self.commit) >= self.config.max_uncommitted {
            return Ok(());
        }
        self.batch()
    }

    /// The first batch of a slot taken over: alone, and past the
    /// uncommitted bound. A new owner may inherit more uncommitted records
    /// than the bound from the slots before, and only this batch's commit
    /// can commit them; held back by the bound, it would never be made
    /// (found by the takeover simulation, seed 1).
    pub(crate) fn begin_slot(&mut self, start: Proposal) -> Result<(), ClusterError> {
        if !self.queue.is_empty() {
            return Err(ClusterError::Full);
        }
        self.queue.push_back(start);
        self.batch()
    }

    fn batch(&mut self) -> Result<(), ClusterError> {
        if self.queue.is_empty() {
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
        if self.slots.last().map(|&(slot, _)| slot) != Some(self.config.slot) {
            self.slots.push((self.config.slot, first));
        }
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
        let Message::Ack {
            slot,
            last_slot,
            end,
            durable,
            cut,
        } = message
        else {
            return;
        };
        if slot != self.config.slot {
            return;
        }
        let own_end = self.end();
        let meets = end_of_slot(&self.slots, last_slot, own_end);
        let Some(peer) = self.peers.iter_mut().find(|peer| peer.id == from) else {
            return;
        };
        // An empty log holds nothing to repair: a stale report of one, taken
        // for a repair, moved a replica back below what was compacted and
        // lost it (the apply simulation, seed 19). A cut says so itself.
        if cut || (last_slot != self.config.slot && end > 0) {
            // A log ending in an older slot: it may hold records this one
            // does not, and it may cut them. Ship from where the two meet
            // as it reports itself now — its position and durability can go
            // back, unlike a replica's in this slot, which only grows (found
            // by the takeover simulation, seed 2: a replica that cut its tail
            // was never shipped to again). Its durability does not count
            // toward commit until it is in this slot.
            let at = meets.min(end);
            // Shipping needs `at` to be a batch boundary in this log. The end
            // of one of its slots is; the replica's end is when the slot it
            // ends in is one both logs share — the records are the same
            // ones there. A slot only the replica has: a probe first, which
            // carries the slot here at `at` (found by the takeover
            // simulation, seed 1010: shipping from such an end read from
            // inside a batch, and lost the replica).
            let shared = self
                .slots
                .iter()
                .take_while(|&&(s, _)| s <= last_slot)
                .last()
                .map(|&(s, _)| s);
            // `at` is a batch boundary here if it ends a slot of this log;
            // the replica's end only if that slot is one both logs share.
            peer.probing = at < meets && shared != Some(last_slot);
            peer.in_slot = false;
            peer.known = true;
            peer.durable = durable.min(at);
            peer.end = at;
            peer.reported = at;
            peer.reading = None;
            if let Some(index) = self.peers.iter().position(|peer| peer.id == from) {
                self.ship(index);
            }
            return;
        }
        // An empty log is a prefix of every log.
        peer.in_slot = last_slot == self.config.slot || end == 0;
        peer.probing = false;
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
        let prev = peer.end;
        peer.end = last;
        peer.from_log = true;
        self.effects.push(Effect::Send {
            to,
            message: Message::Append {
                slot,
                prev,
                prev_slot: slot_at(&self.slots, prev),
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
            if !peer.in_slot {
                // A replica in repair is placed by each report, and a stale
                // one can place it below what was compacted since: forget
                // where it is and wait for the next, rather than lose it
                // (found by the apply simulation, seed 19). One that really
                // needs what is gone waits here, unshipped, as a lost one
                // would, until state copy (step 7b).
                peer.known = false;
                return;
            }
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
                    prev: peer.end,
                    prev_slot: slot_at(&self.slots, peer.end),
                    records: Vec::new(),
                    commit: self.commit,
                },
            });
        }
        self.ship_all();
    }

    fn advance_commit(&mut self) {
        // Only a replica whose log is a prefix of this one counts: one
        // ending in an older slot may hold other records at those lsns.
        let mut durable: Vec<u64> = self
            .peers
            .iter()
            .map(|peer| if peer.in_slot { peer.durable } else { 0 })
            .collect();
        durable.push(self.durable);
        durable.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = durable
            .get(self.config.majority().saturating_sub(1))
            .copied()
            .unwrap_or(0);
        // A slot commits only through a record of its own: the commit point
        // reaches this slot's first lsn or does not move. Records of older
        // slots commit with it, never by counting their copies — copies a
        // later takeover may not see (found by the takeover simulation, seed
        // 5985: a slot 31 sequencer committed lsn 2, a slot 1 record, and a
        // log without it later won).
        let start = self
            .slots
            .iter()
            .find(|&&(slot, _)| slot == self.config.slot)
            .map(|&(_, first)| first);
        if candidate > self.commit && start.is_some_and(|start| candidate >= start) {
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
                || peer.probing
                || peer.reading.is_some()
                || peer.end >= self.durable
                || peer.end.saturating_sub(peer.durable) >= self.config.window
            {
                return;
            }
            let from = peer.end.saturating_add(1);
            match self.recent.iter().find(|batch| batch.first == from) {
                Some(batch) if batch.last <= self.durable => {
                    let prev = peer.end;
                    peer.end = batch.last;
                    if std::mem::take(&mut peer.from_log) {
                        self.effects.push(Effect::ReadDone { to: peer.id });
                    }
                    self.effects.push(Effect::Send {
                        to: peer.id,
                        message: Message::Append {
                            slot: self.config.slot,
                            prev,
                            prev_slot: slot_at(&self.slots, prev),
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
    /// The slot of the record at `end`.
    last_slot: u64,
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
            last_slot: chain.slot(),
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
            ..
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
        if let Some(record) = records.last().and_then(|r| decode(r).ok()) {
            self.last_slot = record.slot;
        }
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
                last_slot: self.last_slot,
                end: self.end,
                durable: self.durable,
                cut: false,
            },
        });
    }
}
