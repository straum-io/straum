//! One voting node across slots (ADR-0008 step 5; unified-stream §2.2,
//! §2.4 ungraceful): it follows the owner of its slot, takes the next slot
//! over when that owner falls silent and the schedule names it, and
//! sequences its own slot through [`Sequencer`].
//!
//! The rules that make a takeover safe, and where each lives:
//!
//! * **Promises are durable before they count.** A node promises a slot
//!   with [`Effect::Promise`] and answers only from
//!   [`Voter::on_promised`]; from then on it takes no record of a lower
//!   slot, acknowledges none, and leads none (invariant 6).
//! * **The candidate takes the longest log of a majority.** It asks every
//!   voter for a promise and its log's end, and once a majority (itself
//!   included) has answered, catches up from the one whose log ends latest
//!   by `(slot, lsn)`. Every committed record is durable on a majority, so
//!   one of them holds it, and a log that ends later in the same slot holds
//!   it too: within a slot one owner writes one sequence (invariant 2).
//! * **A new slot starts with a record.** The candidate writes `SlotStart`
//!   as the first batch of its slot; a replica's durability counts toward
//!   commit only once its log ends in that slot, so nothing from an older
//!   slot is committed by counting copies that differ (§2.3).
//! * **Tails are cut only where logs part.** A follower whose log ends in an
//!   older slot is shipped from where it meets the sequencer's, by the slot
//!   table (Kafka's leader-epoch rule), and cuts its tail there
//!   ([`Effect::Truncate`]) — never below what it knows committed.
//!
//! A planned handoff (step 6, §2.4 graceful) needs no promise round:
//! [`Voter::hand_off`] seals the slot and writes `SlotEnd` as its last
//! batch; the sequencer steps down at the first heartbeat after it
//! commits. The next owner, seeing it committed in its own log — which then
//! holds the whole slot, since nothing follows `SlotEnd` in it — promises
//! the next slot durably and writes `SlotStart` at once. A log holding
//! every committed record is all a takeover's promise round finds out; a
//! majority that promised past the slot meanwhile refuses the new one as
//! it would any older slot. If the successor does not act, the silence
//! starts the usual takeover.
//!
//! Time only starts a takeover: `takeover_ticks` heartbeats of silence, then
//! as many again for each node after it in the schedule (a dead successor).
//! A restarted node never leads the slot it led before; it follows, and the
//! schedule moves on, which is what lets a sequencer ship before its own
//! sync later (D-58).

use journal::{Chain, Kind, Verifier, decode};

use crate::{
    ClusterError, Config, Effect, Message, NodeId, Proposal, Sequencer, end_of_slot, slot_at,
};

/// The most `patience` grows to: a dead sequencer is passed over within
/// `takeover_ticks × MAX_PATIENCE` heartbeats per turn at worst.
const MAX_PATIENCE: u32 = 64;

/// The local log while this node is not sequencing.
#[derive(Debug)]
struct Log {
    verifier: Verifier,
    end: u64,
    last_slot: u64,
    durable: u64,
    commit: u64,
}

impl Log {
    fn new(chain: Chain, durable: u64, commit: u64) -> Self {
        let end = chain.next_lsn().saturating_sub(1);
        Self {
            verifier: Verifier::new(chain),
            end,
            last_slot: chain.slot(),
            durable: durable.min(end),
            commit,
        }
    }
}

#[derive(Debug)]
struct Take {
    slot: u64,
    /// Voters that promised, with where their logs end: (node, slot, lsn).
    promised: Vec<(NodeId, u64, u64)>,
    /// Decided once a majority promised: the voter to catch up from and
    /// where its log ends.
    source: Option<(NodeId, u64, u64)>,
    /// Handed over by a committed `SlotEnd`: leads once its own promise is
    /// durable, asking no one.
    handoff: bool,
}

#[derive(Debug)]
enum Role {
    Follow,
    Take(Take),
    Lead(Box<Sequencer>),
}

/// A voter's counts and its outbox, for metrics (D-112).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VoterStats {
    /// Takeovers started after silence, and slots taken over by a
    /// committed `SlotEnd` (a handoff's successor).
    pub takeovers: u64,
    pub successions: u64,
    /// Slots this node began sequencing, and times it stopped (handed
    /// over, or fenced by a newer slot).
    pub slots_led: u64,
    pub stepped_down: u64,
    /// Other nodes' forwards its full or sealed queue refused.
    pub forwards_refused: u64,
    /// This node's own proposals accepted and awaiting commit, of them sent,
    /// their bytes, the bound, and entries sent again after a slot closed.
    pub outbox_entries: usize,
    pub outbox_sent: usize,
    pub outbox_bytes: usize,
    pub outbox_capacity: usize,
    pub outbox_resent: u64,
}

#[derive(Debug)]
pub struct Voter {
    config: Config,
    /// The highest slot promised, durable: nothing of a lower slot is taken.
    slot: u64,
    /// A promise handed to the driver and not yet durable, and who waits for
    /// the answer.
    promising: Option<(u64, Vec<NodeId>)>,
    role: Role,
    /// Valid while not leading; rebuilt from the sequencer when it steps
    /// down.
    log: Log,
    /// The local log's slot table: (slot, first lsn), ascending.
    slots: Vec<(u64, u64)>,
    /// The latest `SlotEnd` in the local log: (slot, lsn).
    slot_end: Option<(u64, u64)>,
    /// Leading, hand the slot over every this many heartbeats (rotation,
    /// off unless set).
    rotate: Option<u32>,
    /// The silence a takeover waits for, in multiples of `takeover_ticks`:
    /// doubled by each takeover started by silence that a majority promised
    /// (D-88 A), back to 1 once the commit point moves (D-85). A fixed wait shorter than a takeover's
    /// round trips has each candidate passed over before it finishes, and
    /// the next one too, for ever (cluster simulation, seed 4); the doubling
    /// outgrows round trips up to `takeover_ticks × MAX_PATIENCE` heartbeats
    /// without knowing them (the liveness contract, ADR-0008). Time still only
    /// starts a takeover.
    patience: u32,
    /// The commit point when `patience` last went back to 1.
    patient_since: u64,
    /// Leading, heartbeats since `SlotEnd` committed while the successor
    /// does not hold it yet.
    handing: u32,
    /// Heartbeats since the slot's owner was last heard from.
    silent: u32,
    /// A cut handed to the driver and not yet done.
    truncating: bool,
    /// Cut back, and nothing taken since: every report says so.
    cut: bool,
    /// The next report's number: the incarnation in the high bits.
    report: u64,
    /// Fetches waiting for a read of the local log: (asker, slot, the
    /// asker's last slot and end).
    serving: Vec<(NodeId, u64, (u64, u64))>,
    refused: u64,
    effects: Vec<Effect>,
    /// This node's own proposals until they commit (D-109 B).
    outbox: crate::outbox::Outbox,
    /// The slot of the last committed record applied: every older slot is
    /// closed.
    decided: Option<u64>,
    /// Other nodes' forwards this sequencer's full or sealed queue refused:
    /// lost (D-115 A counts them).
    forwards_refused: u64,
    /// Role changes, counted for metrics (D-112); the outbox fields are
    /// filled in by `stats`.
    roles: VoterStats,
    /// The highest commit point heard from a slot's owner, not capped at
    /// this log's end: how far behind this replica is (D-132). Reported,
    /// never acted on.
    heard_commit: u64,
}

impl Voter {
    /// A voter over a recovered log that ends with `chain` and whose slot
    /// table is `slots`, having promised `promised` durably. It starts as a
    /// follower, whatever it was before.
    pub fn new(config: Config, chain: Chain, slots: Vec<(u64, u64)>, promised: u64) -> Self {
        let max_queue = config.max_queue;
        let voters = config.nodes.len();
        let end = chain.next_lsn().saturating_sub(1);
        Self {
            slot: promised.max(chain.slot()),
            config,
            promising: None,
            role: Role::Follow,
            log: Log::new(chain, end, 0),
            slots,
            slot_end: None,
            rotate: None,
            silent: 0,
            handing: 0,
            patience: 1,
            patient_since: 0,
            truncating: false,
            cut: false,
            report: 1,
            serving: Vec::new(),
            refused: 0,
            effects: Vec::new(),
            outbox: crate::outbox::Outbox::new(crate::outbox::capacity(max_queue, voters)),
            decided: None,
            forwards_refused: 0,
            roles: VoterStats::default(),
            heard_commit: 0,
        }
    }

    /// What the driver knows is committed — after a restart, what it has
    /// applied, from its snapshot or its log. The guard that never cuts
    /// below the commit point would otherwise be blind until the first
    /// append tells it.
    pub fn knowing_commit(mut self, commit: u64) -> Self {
        self.log.commit = self.log.commit.max(commit.min(self.log.end));
        self
    }

    /// Which life of this node this is: a number that grows across
    /// restarts, durable before use (`node` keeps one). It orders this
    /// node's reports past its restarts; without it a restarted node's
    /// reports would look older than its previous life's.
    pub fn incarnation(mut self, incarnation: u64) -> Self {
        self.report = (incarnation << 40) | 1;
        self
    }

    /// Leading, hand the slot over every `ticks` heartbeats (§2.4: planned
    /// rotation, "continuous failover drill" with long windows). Off by
    /// default: a sequencer is stable.
    pub fn rotate_every(mut self, ticks: u32) -> Self {
        self.rotate = Some(ticks.max(1));
        self
    }

    pub fn id(&self) -> NodeId {
        self.config.id
    }

    /// How many times `takeover_ticks` this node now waits for silence
    /// before a takeover: 1, or more after takeovers that did not commit
    /// (D-85, D-86).
    pub fn patience(&self) -> u32 {
        self.patience
    }

    /// Leading, whether flow control holds new batches now, and how many
    /// replicas it counts (D-89).
    pub fn flow(&self) -> Option<(bool, usize)> {
        match &self.role {
            Role::Lead(s) => Some((s.holding_back(), s.attached())),
            _ => None,
        }
    }

    /// The slot this node is taking over by silence — not a planned
    /// handoff — if it is (D-98).
    pub fn taking(&self) -> Option<u64> {
        match &self.role {
            Role::Take(take) if !take.handoff => Some(take.slot),
            _ => None,
        }
    }

    /// Leading, whether flow control counts replica `id` (D-89).
    pub fn counts(&self, id: NodeId) -> Option<bool> {
        match &self.role {
            Role::Lead(s) => Some(s.counts(id)),
            _ => None,
        }
    }

    /// The highest slot this node has promised.
    pub fn slot(&self) -> u64 {
        self.slot
    }

    /// Whom proposals go to: the owner of the promised slot.
    pub fn owner(&self) -> Option<NodeId> {
        self.config.owner_of(self.slot)
    }

    /// Sequencing its own slot now.
    pub fn leading(&self) -> bool {
        matches!(self.role, Role::Lead(_))
    }

    pub fn commit(&self) -> u64 {
        match &self.role {
            Role::Lead(s) => s.commit(),
            _ => self.log.commit,
        }
    }

    pub fn durable(&self) -> u64 {
        match &self.role {
            Role::Lead(s) => s.durable(),
            _ => self.log.durable,
        }
    }

    pub fn end(&self) -> u64 {
        match &self.role {
            Role::Lead(s) => s.end(),
            _ => self.log.end,
        }
    }

    /// Messages refused: from an older slot or not its owner, or breaking
    /// the chain.
    pub fn refused(&self) -> u64 {
        self.refused
    }

    /// How far the local log may be compacted: the sequencer's floor
    /// (D-57 A), or, following, the commit point less what the byte bound
    /// keeps — a follower may lead next and serve catch-up.
    ///
    /// Never the commit point itself: a cut can go back to it, and the log
    /// must still hold a record at or below the cut to say where it stands
    /// (found by the cluster simulation, seed 229: compacted to 16, cut to
    /// 16, a restart found no log at all).
    pub fn compaction_floor(&self, bytes_floor: u64) -> u64 {
        let floor = match &self.role {
            Role::Lead(s) => s.compaction_floor(bytes_floor),
            _ => self.log.commit.min(bytes_floor),
        };
        floor.min(self.commit().saturating_sub(1))
    }

    /// Leading: the durable position known for replica `id`, unless lost
    /// (`Sequencer::replica_durable`).
    pub fn replica_durable(&self, id: NodeId) -> Option<u64> {
        match &self.role {
            Role::Lead(s) => s.replica_durable(id),
            _ => None,
        }
    }

    /// The local log was compacted through `floor`: the slot tables forget
    /// the slots that ended there (`prune_slots`, D-79 A), so they are
    /// bounded by the slot changes in the retained log.
    pub fn on_compacted(&mut self, floor: u64) {
        crate::prune_slots(&mut self.slots, floor);
        if let Role::Lead(s) = &mut self.role {
            s.compacted(floor);
        }
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        if let Role::Lead(s) = &mut self.role {
            let effects = s.take_effects();
            for effect in &effects {
                if let Effect::Append { records, .. } = effect {
                    note_slots(&mut self.slots, &mut self.slot_end, records);
                }
            }
            self.effects.extend(effects);
        }
        std::mem::take(&mut self.effects)
    }

    pub fn propose(&mut self, proposal: Proposal) -> Result<(), ClusterError> {
        match &mut self.role {
            Role::Lead(s) => s.propose(proposal),
            _ => Err(ClusterError::NotLeader),
        }
    }

    /// Takes one of this node's own proposals (a gateway's): held until its
    /// record is applied from the committed log, sent to the sequencer of
    /// the promised slot, and sent again only once that slot is closed in
    /// the committed log (`outbox`, D-109 B). `Full` at `max_queue` held:
    /// refused, never acknowledged.
    pub fn submit(&mut self, proposal: Proposal) -> Result<(), ClusterError> {
        if !self.outbox.push(proposal) {
            return Err(ClusterError::Full);
        }
        self.pump();
        Ok(())
    }

    /// The driver applied this committed record, in log order. Returns this
    /// node's proposals it shows lost — sent before it to the same slot and
    /// never committed; sending them again would reorder them, so they are
    /// given up and handed back for the driver to account for (D-115 A).
    pub fn applied(&mut self, record: &journal::Record<'_>) -> Vec<Proposal> {
        self.decided = Some(record.slot);
        let lost = self.outbox.committed(record);
        self.pump();
        lost
    }

    /// This node's own proposals held uncommitted, and how many of them are
    /// sent and wait for their slot to be decided (D-109 B).
    pub fn held(&self) -> (usize, usize) {
        self.outbox.held()
    }

    /// The outbox is at its share of the sequencer's queue (D-120 A): a
    /// driver stops reading its clients until it is not, and `submit`
    /// refuses meanwhile.
    pub fn outbox_full(&self) -> bool {
        self.outbox.full()
    }

    /// Forwards from other nodes this node, sequencing, refused at a full or
    /// sealed queue: lost; their nodes give them up once a later one
    /// commits, or send them again once the slot closes.
    pub fn forwards_refused(&self) -> u64 {
        self.forwards_refused
    }

    /// What this voter has done and holds, for metrics (D-112): counts since
    /// it was built, and its outbox now. Read on demand, never on a path
    /// that decides anything.
    pub fn stats(&self) -> VoterStats {
        let (entries, sent) = self.outbox.held();
        let (bytes, resent) = self.outbox.bytes_resent();
        VoterStats {
            forwards_refused: self.forwards_refused,
            outbox_entries: entries,
            outbox_sent: sent,
            outbox_bytes: bytes,
            outbox_capacity: self.outbox.capacity(),
            outbox_resent: resent,
            ..self.roles
        }
    }

    /// The commit point this node has heard of — its own when sequencing;
    /// as a replica, the highest an owner sent, not capped at this log's
    /// end (D-132). With `end`, how far behind it is: a report only.
    pub fn heard_commit(&self) -> u64 {
        match &self.role {
            Role::Lead(s) => s.commit(),
            _ => self.heard_commit.max(self.log.commit),
        }
    }

    /// Sequencing: each replica's durable position as last reported, for
    /// metrics (D-112); `None` for one not heard from in this slot.
    pub fn replicas(&self) -> Vec<(NodeId, Option<u64>)> {
        match &self.role {
            Role::Lead(s) => s.replicas(),
            _ => Vec::new(),
        }
    }

    /// After a restart: the slot of the last committed record the driver
    /// has applied — from its snapshot when nothing after it is replayed.
    /// Nothing is forwarded before the promised slot has a committed record.
    pub fn knowing_applied_slot(mut self, slot: Option<u64>) -> Self {
        self.decided = slot;
        self
    }

    /// Sends what the outbox holds to the sequencer of the promised slot:
    /// this node's own, or a forward to an owner whose slot has a committed
    /// record — before that, the owner may not sequence yet and would drop
    /// it. Stops at the first its own sequencer refuses.
    fn pump(&mut self) {
        let slot = self.slot;
        let leading = matches!(self.role, Role::Lead(_));
        let to = if leading {
            // Not into a slot the committed log has already passed.
            if self.decided > Some(slot) {
                return;
            }
            None
        } else {
            match self.owner() {
                Some(owner) if owner != self.config.id && self.decided == Some(slot) => Some(owner),
                _ => return,
            }
        };
        while let Some(proposal) = self.outbox.next(slot) {
            // A copy per send, the outbox keeps the original until
            // it commits; unmeasured (D-112).
            let proposal = proposal.clone();
            match (to, &mut self.role) {
                (None, Role::Lead(s)) => {
                    if s.propose(proposal).is_err() {
                        return;
                    }
                }
                (Some(owner), _) => self.send(owner, Message::Forward { slot, proposal }),
                _ => return,
            }
            self.outbox.sent(slot);
        }
    }

    pub fn flush(&mut self) -> Result<(), ClusterError> {
        self.pump();
        match &mut self.role {
            Role::Lead(s) => s.flush(),
            _ => Ok(()),
        }
    }

    /// Hands the slot over to the next owner in the schedule (§2.4
    /// graceful): refuses proposals from now on, writes what is queued and
    /// then `SlotEnd`, and steps down at the first heartbeat after
    /// `SlotEnd` commits. Repeating it changes nothing.
    pub fn hand_off(&mut self) -> Result<(), ClusterError> {
        let Role::Lead(s) = &mut self.role else {
            return Err(ClusterError::NotLeader);
        };
        if self.config.owner_of(s.slot().saturating_add(1)) == Some(self.config.id) {
            return Err(ClusterError::NoSuccessor);
        }
        s.end_slot();
        s.flush()
    }

    /// The heartbeat. A sequencer sends its own; anyone else counts the
    /// silence and, at its turn in the schedule, takes the next slot over.
    pub fn tick(&mut self) {
        // What waited for a slot to be decided or a sequencer to start goes
        // out at the latest with the heartbeat.
        self.pump();
        if let Role::Lead(s) = &mut self.role {
            s.tick();
            // This heartbeat carried the commit point past `SlotEnd`. The
            // slot is handed over once the successor holds `SlotEnd`
            // durably — then this heartbeat lets it take over, and it is the
            // last one this slot sends. A successor still catching up keeps
            // the heartbeats coming for up to `takeover_ticks` of them:
            // stepping down at once left one that had just cut its log with
            // no one to catch up from, and the slot went by silence instead
            // (takeover simulation, seed 4905; D-85). Lost, or past that,
            // the successor or the next node takes over by silence.
            if let Some((slot, lsn)) = self
                .slot_end
                .filter(|&(slot, lsn)| slot == s.slot() && s.commit() >= lsn)
            {
                let holds = self
                    .config
                    .owner_of(slot.saturating_add(1))
                    .and_then(|next| s.replica_durable(next))
                    .is_some_and(|durable| durable >= lsn);
                self.handing = self.handing.saturating_add(1);
                if holds || self.handing > self.config.takeover_ticks {
                    self.handing = 0;
                    self.step_down();
                }
                return;
            }
            self.silent = self.silent.saturating_add(1);
            if self.rotate.is_some_and(|every| self.silent % every == 0) {
                let _ = self.hand_off();
            }
            return;
        }
        self.silent = self.silent.saturating_add(1);
        let commit = self.commit();
        if commit > self.patient_since {
            self.patient_since = commit;
            self.patience = 1;
        }
        if let Role::Take(take) = &self.role {
            let slot = take.slot;
            // Lost requests are asked again, every heartbeat.
            match take.source {
                None if self.slot == slot => {
                    let asked: Vec<NodeId> = take.promised.iter().map(|p| p.0).collect();
                    for peer in self.config.nodes.clone() {
                        if peer != self.config.id && !asked.contains(&peer) {
                            self.send(peer, Message::Promise { slot });
                        }
                    }
                }
                Some((source, ..)) if !self.truncating => self.fetch(source, slot),
                _ => {}
            }
        }
        let target = if self.slot < self.config.slot {
            // A fresh cluster: the first slot's owner takes it at once.
            self.config.slot
        } else {
            let wait = self
                .config
                .takeover_ticks
                .max(1)
                .saturating_mul(self.patience);
            let turns = self.silent / wait;
            self.slot.saturating_add(u64::from(turns))
        };
        let taking = match &self.role {
            Role::Take(take) => take.slot >= target,
            _ => false,
        };
        // Not while a cut is pending: the log it would lead from is not the
        // one the disk will hold (found by the takeover simulation, seed
        // 1066).
        if target > self.slot
            && !taking
            && !self.truncating
            && self.config.owner_of(target) == Some(self.config.id)
        {
            self.roles.takeovers = self.roles.takeovers.saturating_add(1);
            self.role = Role::Take(Take {
                slot: target,
                promised: Vec::new(),
                source: None,
                handoff: false,
            });
            self.promise(target, None);
        }
    }

    pub fn on_message(&mut self, from: NodeId, message: Message) {
        match message {
            Message::Append {
                slot,
                prev,
                prev_slot,
                records,
                commit,
            } => self.on_append(from, slot, (prev, prev_slot), records, commit),
            Message::Ack { .. } => {
                if let Role::Lead(s) = &mut self.role {
                    s.on_message(from, message);
                }
            }
            Message::Promise { slot } => {
                self.promise(slot, Some(from));
            }
            Message::Forward { slot, proposal } => {
                // Only into the very slot it was sent for; dropped otherwise,
                // and its node sends it again once that slot is closed.
                if let Role::Lead(s) = &mut self.role {
                    if s.slot() == slot && s.propose(proposal).is_err() {
                        self.forwards_refused = self.forwards_refused.saturating_add(1);
                    }
                }
            }
            Message::Promised {
                slot,
                last_slot,
                end,
            } => self.on_promised_by(from, slot, last_slot, end),
            Message::Fetch {
                slot,
                last_slot,
                end,
            } => self.serve(from, slot, last_slot, end),
            Message::Fetched {
                slot,
                asked_slot,
                asked,
                after,
                after_slot,
                records,
            } => self.on_fetched(
                from,
                slot,
                (asked_slot, asked),
                (after, after_slot),
                records,
            ),
        }
    }

    /// The local log is durable through `lsn`.
    pub fn on_durable(&mut self, lsn: u64) {
        if let Role::Lead(s) = &mut self.role {
            s.on_durable(lsn);
            return;
        }
        self.log.durable = self.log.durable.max(lsn.min(self.log.end));
        if matches!(self.role, Role::Follow) && !self.truncating {
            self.ack();
        }
    }

    /// The promise of `slot` is durable.
    pub fn on_promised(&mut self, slot: u64) {
        let Some((promising, waiting)) = self.promising.take() else {
            return;
        };
        if promising != slot {
            self.promising = Some((promising, waiting));
            return;
        }
        if slot > self.slot {
            self.slot = slot;
            self.silent = 0;
        }
        // A sequencer of an older slot steps down: its slot can commit
        // nothing once a majority has promised past it. What it handed over
        // and the driver has not taken yet — batches to write among them,
        // which its chain already counts — goes out first.
        if let Role::Lead(s) = &self.role {
            if s.slot() < self.slot {
                self.step_down();
            }
        }
        if let Role::Take(take) = &mut self.role {
            if take.slot < self.slot {
                self.role = Role::Follow;
            } else if take.slot == slot && take.handoff {
                self.lead(slot);
            } else if take.slot == slot {
                let own = (self.config.id, self.log.last_slot, self.log.end);
                if !take.promised.iter().any(|p| p.0 == own.0) {
                    take.promised.push(own);
                }
                for &peer in &self.config.nodes {
                    if peer != self.config.id {
                        self.effects.push(Effect::Send {
                            to: peer,
                            message: Message::Promise { slot },
                        });
                    }
                }
                self.decide();
            }
        }
        let (last_slot, end) = self.position();
        for to in waiting {
            self.send(
                to,
                Message::Promised {
                    slot,
                    last_slot,
                    end,
                },
            );
        }
    }

    /// The local log now ends at `chain`, cut as [`Effect::Truncate`] asked.
    pub fn on_truncated(&mut self, chain: Chain) {
        self.truncating = false;
        self.cut = true;
        let end = chain.next_lsn().saturating_sub(1);
        self.log = Log::new(chain, self.log.durable.min(end), self.log.commit);
        self.slots.retain(|&(_, first)| first <= end);
        if self.slot_end.is_some_and(|(_, lsn)| lsn > end) {
            self.slot_end = None;
        }
        match &self.role {
            Role::Take(Take {
                slot,
                source: Some((source, ..)),
                ..
            }) => {
                let (slot, source) = (*slot, *source);
                self.fetch(source, slot);
            }
            Role::Take(Take { source: None, .. }) => self.decide(),
            Role::Follow => self.ack(),
            _ => {}
        }
    }

    /// The batches a read asked for: a fetch being served, the
    /// sequencer's catch-up, or both. A read is matched by whom it is for
    /// and where it starts: matched by whom alone, the answer to a
    /// sequencer's catch-up read went to a fetch from the same replica
    /// still waiting from before this node led, and the sequencer waited
    /// for it for ever, shipping that replica nothing (found by the cluster
    /// simulation, seed 160; D-85).
    pub fn on_read(&mut self, to: NodeId, records: Vec<Vec<u8>>) {
        let from = records
            .first()
            .and_then(|r| decode(r).ok())
            .map_or(0, |r| r.lsn);
        if let Some(index) = self
            .serving
            .iter()
            .position(|&(asker, _, (_, asked))| asker == to && asked.saturating_add(1) == from)
        {
            let (_, slot, (asked_slot, asked)) = self.serving.remove(index);
            let after = from.saturating_sub(1);
            let after_slot = slot_at(&self.slots, after);
            self.send(
                to,
                Message::Fetched {
                    slot,
                    asked_slot,
                    asked,
                    after,
                    after_slot,
                    records: records.clone(),
                },
            );
        }
        if let Role::Lead(s) = &mut self.role {
            s.on_read(to, records);
        }
    }

    pub fn on_read_missing(&mut self, to: NodeId, from: u64) {
        if let Some(index) = self
            .serving
            .iter()
            .position(|&(asker, _, (_, asked))| asker == to && asked.saturating_add(1) == from)
        {
            // Compacted away: the candidate cannot catch up from here, and
            // waits for a later slot's owner (state copy is step 7b).
            self.serving.remove(index);
        }
        if let Role::Lead(s) = &mut self.role {
            s.on_read_missing(to, from);
        }
    }

    fn position(&self) -> (u64, u64) {
        match &self.role {
            Role::Lead(s) => (s.chain().slot(), s.end()),
            _ => (self.log.last_slot, self.log.end),
        }
    }

    fn send(&mut self, to: NodeId, message: Message) {
        self.effects.push(Effect::Send { to, message });
    }

    /// Stops sequencing and follows. What the sequencer handed over and
    /// the driver has not taken yet — batches to write among them, which
    /// its chain already counts — goes out first.
    fn step_down(&mut self) {
        let Role::Lead(s) = &mut self.role else {
            return;
        };
        let pending = s.take_effects();
        for effect in &pending {
            if let Effect::Append { records, .. } = effect {
                note_slots(&mut self.slots, &mut self.slot_end, records);
            }
        }
        self.effects.extend(pending);
        self.log = Log::new(s.chain(), s.durable(), s.commit());
        self.role = Role::Follow;
        self.roles.stepped_down = self.roles.stepped_down.saturating_add(1);
        self.silent = 0;
    }

    /// Following a slot whose `SlotEnd` this log holds committed, and named
    /// next in the schedule: take the next slot over without a promise
    /// round (§2.4 graceful).
    fn succeed(&mut self) {
        let next = self.slot.saturating_add(1);
        let handed = self
            .slot_end
            .is_some_and(|(slot, lsn)| slot == self.slot && lsn <= self.log.commit);
        if handed
            && matches!(self.role, Role::Follow)
            && !self.truncating
            && self.promising.is_none()
            && self.config.owner_of(next) == Some(self.config.id)
        {
            self.roles.successions = self.roles.successions.saturating_add(1);
            self.role = Role::Take(Take {
                slot: next,
                promised: Vec::new(),
                source: None,
                handoff: true,
            });
            self.promise(next, None);
        }
    }

    /// Promises `slot`, durably, and answers `to` once it is.
    fn promise(&mut self, slot: u64, to: Option<NodeId>) {
        if slot < self.slot {
            return;
        }
        if slot == self.slot && self.promising.is_none() {
            // Already durable: answer now.
            if let Some(to) = to {
                let (last_slot, end) = self.position();
                self.send(
                    to,
                    Message::Promised {
                        slot,
                        last_slot,
                        end,
                    },
                );
            }
            return;
        }
        match &mut self.promising {
            Some((pending, waiting)) if *pending == slot => {
                if let Some(to) = to {
                    if !waiting.contains(&to) {
                        waiting.push(to);
                    }
                }
            }
            Some((pending, _)) if *pending > slot => {}
            _ => {
                self.promising = Some((slot, to.into_iter().collect()));
                self.effects.push(Effect::Promise { slot });
            }
        }
    }

    /// Where this log provably parts from another whose record at `at.0`
    /// is in slot `at.1` and whose next record is in slot `next` (`None`:
    /// it holds nothing more of this log's slots). `None` when nothing here
    /// is proven different. Only a difference is cut, never a guess: two
    /// records in one slot at one lsn are the same record — one owner wrote
    /// one sequence — and records in different slots are different. Agreeing
    /// at `at.0` says nothing of what follows, and cutting there anyway lost
    /// copies of committed records to a stale report (found by the takeover
    /// simulation, seed 4).
    fn parts(&self, at: (u64, u64), next: Option<u64>) -> Option<u64> {
        let (lsn, slot) = at;
        if lsn > self.log.end {
            return None;
        }
        // Slot 0 at a record: the other side's slot table no longer covers
        // it — compacted. Unknown proves nothing (found by the cluster
        // simulation, seed 197: read as a difference, it cut a committed
        // copy).
        if lsn > 0 && slot == 0 {
            return None;
        }
        // The committed prefix is the same in every log, whatever this
        // log's slot table still says of it after compaction (found by the
        // cluster simulation, seed 64).
        let committed = |at: u64| at <= self.log.commit;
        if !committed(lsn) && slot_at(&self.slots, lsn) != slot {
            // This record differs, and so does all that follows it: cut
            // before it, and back to the end of this log's latest slot not
            // after the other's. `lsn` is a batch boundary only in the other
            // log; the driver rounds the cut down to one in this log (found
            // by the takeover simulation, seed 551, and the cluster
            // simulation, seed 33). Safe: within a slot a batch is the same
            // batch in every log, so the whole batch around a differing
            // record differs.
            let cut = end_of_slot(&self.slots, slot, self.log.end).min(lsn.saturating_sub(1));
            return (cut < self.log.end).then_some(cut);
        }
        if lsn == self.log.end {
            return None;
        }
        let following = lsn.saturating_add(1);
        match next {
            _ if committed(following) => None,
            Some(next) if slot_at(&self.slots, following) == next => None,
            _ => Some(lsn),
        }
    }

    /// Cuts the log back to `after`, never below what is known committed:
    /// every committed record is in the log being followed.
    fn cut(&mut self, after: u64) {
        if after < self.log.commit {
            self.refused = self.refused.saturating_add(1);
            return;
        }
        self.truncating = true;
        self.effects.push(Effect::Truncate { after });
    }

    fn on_append(
        &mut self,
        from: NodeId,
        slot: u64,
        prev: (u64, u64),
        records: Vec<Vec<u8>>,
        commit: u64,
    ) {
        if slot < self.slot || self.config.owner_of(slot) != Some(from) || from == self.config.id {
            self.refused = self.refused.saturating_add(1);
            return;
        }
        if slot > self.slot {
            // A newer slot's owner: promise it first; its next heartbeat
            // finds this node following. Meanwhile it is alive, and heard:
            // counting silence while the promise is made durable — long, on
            // a slow disk — started takeovers this node could not finish and
            // deposed a healthy sequencer (D-98 A). Only silence: nothing is
            // appended or acknowledged before the promise is durable.
            self.silent = 0;
            self.promise(slot, None);
            return;
        }
        if !matches!(self.role, Role::Follow) {
            return;
        }
        self.silent = 0;
        // A cut in flight: this log's end is about to change, and a report
        // of the old one would place it wrong (found by the cluster
        // simulation, seed 12). The report after the cut tells.
        if self.truncating {
            return;
        }
        // A log ending in the owner's slot is a prefix of the owner's. One
        // ending in an older slot may hold records the owner's does not: cut
        // back where they provably differ, then take what follows. A
        // heartbeat proves a difference only by the slot at `prev`.
        if !self.truncating && self.log.last_slot < slot {
            let cut = if records.is_empty() {
                (prev.0 <= self.log.end && slot_at(&self.slots, prev.0) != prev.1)
                    .then(|| self.parts(prev, None))
                    .flatten()
            } else {
                let next = records.first().and_then(|r| decode(r).ok()).map(|r| r.slot);
                self.parts(prev, next)
            };
            if let Some(cut) = cut {
                self.cut(cut);
                return;
            }
        }
        // From the owner of the promised slot: its commit point, as heard.
        self.heard_commit = self.heard_commit.max(commit);
        if !records.is_empty() && !self.truncating && prev.0 == self.log.end {
            self.append(records);
        }
        // The commit point applies to this log only once it ends in the
        // owner's slot: then it is a prefix of the owner's log.
        if self.log.last_slot == slot {
            let known = commit.min(self.log.end);
            if known > self.log.commit {
                self.log.commit = known;
                self.effects.push(Effect::Committed(known));
            }
        }
        self.ack();
        self.succeed();
    }

    /// Takes one whole batch that continues the chain, or nothing.
    fn append(&mut self, records: Vec<Vec<u8>>) -> bool {
        let mut verifier = self.log.verifier.clone();
        let chained = records.iter().all(|record| verifier.feed(record).is_ok());
        if !chained || verifier.clone().finish().partial != 0 {
            self.refused = self.refused.saturating_add(1);
            return false;
        }
        let Some(last) = records.last().and_then(|r| decode(r).ok()) else {
            return false;
        };
        let first = self.log.end.saturating_add(1);
        self.log.verifier = verifier;
        self.log.end = last.lsn;
        self.log.last_slot = last.slot;
        self.cut = false;
        note_slots(&mut self.slots, &mut self.slot_end, &records);
        self.effects.push(Effect::Append { first, records });
        true
    }

    fn ack(&mut self) {
        let Some(owner) = self.owner() else {
            return;
        };
        if owner == self.config.id {
            return;
        }
        self.report = self.report.saturating_add(1);
        self.send(
            owner,
            Message::Ack {
                slot: self.slot,
                last_slot: self.log.last_slot,
                end: self.log.end,
                durable: self.log.durable,
                cut: self.cut,
                report: self.report,
            },
        );
    }

    fn on_promised_by(&mut self, from: NodeId, slot: u64, last_slot: u64, end: u64) {
        let Role::Take(take) = &mut self.role else {
            return;
        };
        if take.slot != slot || take.source.is_some() || self.slot != slot {
            return;
        }
        if !take.promised.iter().any(|p| p.0 == from) {
            take.promised.push((from, last_slot, end));
        }
        self.decide();
    }

    /// With a majority promised, catch up from the longest log among them.
    fn decide(&mut self) {
        if self.truncating {
            return;
        }
        let Role::Take(take) = &mut self.role else {
            return;
        };
        if take.source.is_some() || take.promised.len() < self.config.majority() {
            return;
        }
        let id = self.config.id;
        let Some(&(source, last_slot, end)) = take
            .promised
            .iter()
            // Ties go to this node: nothing to fetch.
            .max_by_key(|&&(node, slot, end)| (slot, end, node == id))
        else {
            return;
        };
        take.source = Some((source, last_slot, end));
        let slot = take.slot;
        // A takeover a majority answered: if it does not commit, the round
        // trips outgrew the wait, and the next one waits longer, until a
        // commit shows takeovers finishing again. One no majority answered
        // says nothing about round trips — a partition — and leaves the
        // wait as it was, so a partition does not lengthen the recovery
        // after it (D-88 A). The simulation showed patience was not what
        // made recovery long: under the old rule 64 runs of the bounded
        // seed groups healed at 64× within 41 rounds.
        if !take.handoff {
            self.patience = self.patience.saturating_mul(2).min(MAX_PATIENCE);
        }
        if source == id {
            self.lead(slot);
        } else {
            self.fetch(source, slot);
        }
    }

    fn fetch(&mut self, source: NodeId, slot: u64) {
        let message = Message::Fetch {
            slot,
            last_slot: self.log.last_slot,
            end: self.log.end,
        };
        self.send(source, message);
    }

    /// Answers a candidate's fetch from the local log.
    fn serve(&mut self, to: NodeId, slot: u64, last_slot: u64, end: u64) {
        // A fetch for a slot older than the one promised is from a candidate
        // passed over since: serving it handed a sequencer's records not yet
        // durable to a replica, which then held them durably first, and the
        // sequencer's own later sync committed a record of its slot after a
        // majority had promised past it — invariant 6 (found by the cluster
        // simulation, seed 4629; D-85).
        if slot < self.slot {
            return;
        }
        let (_, own_end) = self.position();
        let meets = end_of_slot(&self.slots, last_slot, own_end).min(end);
        if meets < end || end >= own_end {
            let after_slot = slot_at(&self.slots, meets);
            self.send(
                to,
                Message::Fetched {
                    slot,
                    asked_slot: last_slot,
                    asked: end,
                    after: meets,
                    after_slot,
                    records: Vec::new(),
                },
            );
            return;
        }
        if !self.serving.iter().any(|&(asker, ..)| asker == to) {
            self.serving.push((to, slot, (last_slot, end)));
            self.effects.push(Effect::Read {
                to,
                from: end.saturating_add(1),
                upto: own_end,
            });
        }
    }

    fn on_fetched(
        &mut self,
        from: NodeId,
        slot: u64,
        asked: (u64, u64),
        at: (u64, u64),
        records: Vec<Vec<u8>>,
    ) {
        let Role::Take(Take {
            slot: taking,
            source: Some((source, ..)),
            ..
        }) = &self.role
        else {
            return;
        };
        let (taking, source) = (*taking, *source);
        // An answer to an older question is about a log this one no longer
        // is.
        if taking != slot
            || source != from
            || self.truncating
            || asked != (self.log.last_slot, self.log.end)
        {
            return;
        }
        let next = records.first().and_then(|r| decode(r).ok()).map(|r| r.slot);
        if let Some(cut) = self.parts(at, next) {
            self.cut(cut);
            return;
        }
        if at.0 != self.log.end {
            // Agrees further than the answer showed: ask from here.
            self.fetch(source, slot);
            return;
        }
        if records.is_empty() {
            // The source holds nothing more: this log is the longest.
            self.lead(slot);
        } else if self.append(records) {
            self.fetch(source, slot);
        }
    }

    /// Writes `SlotStart` as the first batch of `slot` and sequences it.
    fn lead(&mut self, slot: u64) {
        let chain = self.log.verifier.clone().finish().chain;
        let config = Config {
            slot,
            owner: self.config.id,
            ..self.config.clone()
        };
        let mut sequencer = Sequencer::with(
            config,
            chain,
            self.log.durable,
            self.log.commit,
            self.slots.clone(),
        );
        let start = Proposal {
            kind: Kind::SlotStart,
            properties: Vec::new(),
            body: self.log.end.to_be_bytes().to_vec(),
        };
        if sequencer.begin_slot(start).is_err() {
            self.role = Role::Follow;
            return;
        }
        self.silent = 0;
        self.role = Role::Lead(Box::new(sequencer));
        self.roles.slots_led = self.roles.slots_led.saturating_add(1);
    }
}

/// Adds the slots `records` start to `slots`, and notes a `SlotEnd`.
fn note_slots(slots: &mut Vec<(u64, u64)>, slot_end: &mut Option<(u64, u64)>, records: &[Vec<u8>]) {
    for record in records {
        let Ok(record) = decode(record) else { continue };
        if slots.last().is_none_or(|&(slot, _)| slot < record.slot) {
            slots.push((record.slot, record.lsn));
        }
        if record.kind == Kind::SlotEnd {
            *slot_end = Some((record.slot, record.lsn));
        }
    }
}
