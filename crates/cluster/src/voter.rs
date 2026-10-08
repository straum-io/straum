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
//! Time only starts a takeover: `takeover_ticks` heartbeats of silence, then
//! as many again for each node after it in the schedule (a dead successor).
//! A restarted node never leads the slot it led before; it follows, and the
//! schedule moves on, which is what lets a sequencer ship before its own
//! sync later (D-58).

use journal::{Chain, Kind, Verifier, decode};

use crate::{
    ClusterError, Config, Effect, Message, NodeId, Proposal, Sequencer, end_of_slot, slot_at,
};

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
}

#[derive(Debug)]
enum Role {
    Follow,
    Take(Take),
    Lead(Box<Sequencer>),
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
    /// Heartbeats since the slot's owner was last heard from.
    silent: u32,
    /// A cut handed to the driver and not yet done.
    truncating: bool,
    /// Cut back, and nothing taken since: every report says so.
    cut: bool,
    /// Fetches waiting for a read of the local log: (asker, slot, the
    /// asker's end).
    serving: Vec<(NodeId, u64, u64)>,
    refused: u64,
    effects: Vec<Effect>,
}

impl Voter {
    /// A voter over a recovered log that ends with `chain` and whose slot
    /// table is `slots`, having promised `promised` durably. It starts as a
    /// follower, whatever it was before.
    pub fn new(config: Config, chain: Chain, slots: Vec<(u64, u64)>, promised: u64) -> Self {
        let end = chain.next_lsn().saturating_sub(1);
        Self {
            slot: promised.max(chain.slot()),
            config,
            promising: None,
            role: Role::Follow,
            log: Log::new(chain, end, 0),
            slots,
            silent: 0,
            truncating: false,
            cut: false,
            serving: Vec::new(),
            refused: 0,
            effects: Vec::new(),
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

    pub fn id(&self) -> NodeId {
        self.config.id
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
    pub fn compaction_floor(&self, bytes_floor: u64) -> u64 {
        match &self.role {
            Role::Lead(s) => s.compaction_floor(bytes_floor),
            _ => self.log.commit.min(bytes_floor),
        }
    }

    pub fn take_effects(&mut self) -> Vec<Effect> {
        if let Role::Lead(s) = &mut self.role {
            let effects = s.take_effects();
            for effect in &effects {
                if let Effect::Append { records, .. } = effect {
                    note_slots(&mut self.slots, records);
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

    pub fn flush(&mut self) -> Result<(), ClusterError> {
        match &mut self.role {
            Role::Lead(s) => s.flush(),
            _ => Ok(()),
        }
    }

    /// The heartbeat. A sequencer sends its own; anyone else counts the
    /// silence and, at its turn in the schedule, takes the next slot over.
    pub fn tick(&mut self) {
        if let Role::Lead(s) = &mut self.role {
            s.tick();
            return;
        }
        self.silent = self.silent.saturating_add(1);
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
            let turns = self.silent / self.config.takeover_ticks.max(1);
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
            self.role = Role::Take(Take {
                slot: target,
                promised: Vec::new(),
                source: None,
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
            Message::Promise { slot } => self.promise(slot, Some(from)),
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
                asked,
                after,
                after_slot,
                records,
            } => self.on_fetched(from, slot, asked, (after, after_slot), records),
        }
    }

    /// The local log is durable through `lsn`.
    pub fn on_durable(&mut self, lsn: u64) {
        if let Role::Lead(s) = &mut self.role {
            s.on_durable(lsn);
            return;
        }
        self.log.durable = self.log.durable.max(lsn.min(self.log.end));
        if matches!(self.role, Role::Follow) {
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
        // nothing once a majority has promised past it.
        if let Role::Lead(s) = &self.role {
            if s.slot() < self.slot {
                self.log = Log::new(s.chain(), s.durable(), s.commit());
                self.role = Role::Follow;
            }
        }
        if let Role::Take(take) = &mut self.role {
            if take.slot < self.slot {
                self.role = Role::Follow;
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

    /// The batch a read asked for: a fetch being served, or the sequencer's
    /// catch-up.
    pub fn on_read(&mut self, to: NodeId, records: Vec<Vec<u8>>) {
        if let Some(index) = self.serving.iter().position(|&(asker, ..)| asker == to) {
            let (_, slot, asked) = self.serving.remove(index);
            let after = records
                .first()
                .and_then(|r| decode(r).ok())
                .map_or(0, |r| r.lsn.saturating_sub(1));
            let after_slot = slot_at(&self.slots, after);
            self.send(
                to,
                Message::Fetched {
                    slot,
                    asked,
                    after,
                    after_slot,
                    records,
                },
            );
            return;
        }
        if let Role::Lead(s) = &mut self.role {
            s.on_read(to, records);
        }
    }

    pub fn on_read_missing(&mut self, to: NodeId, from: u64) {
        if let Some(index) = self.serving.iter().position(|&(asker, ..)| asker == to) {
            // Compacted away: the candidate cannot catch up from here, and
            // waits for a later slot's owner (state copy is step 7b).
            self.serving.remove(index);
            return;
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
        if slot_at(&self.slots, lsn) != slot {
            // This record differs, and so does all that follows it: cut back
            // to the end of this log's latest slot not after the other's, or
            // to `lsn` — both batch boundaries; a cut inside a batch would
            // leave half of one (found by the takeover simulation, seed 551).
            // Cut at `lsn`, the record there still differs: the next append
            // fails the chain, and the report that follows moves the other
            // side back to where the slots say the logs meet.
            let cut = end_of_slot(&self.slots, slot, self.log.end).min(lsn);
            return (cut < self.log.end).then_some(cut);
        }
        if lsn == self.log.end {
            return None;
        }
        match next {
            Some(next) if slot_at(&self.slots, lsn.saturating_add(1)) == next => None,
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
            // finds this node following.
            self.promise(slot, None);
            return;
        }
        if !matches!(self.role, Role::Follow) {
            return;
        }
        self.silent = 0;
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
        note_slots(&mut self.slots, &records);
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
        self.send(
            owner,
            Message::Ack {
                slot: self.slot,
                last_slot: self.log.last_slot,
                end: self.log.end,
                durable: self.log.durable,
                cut: self.cut,
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
        let (_, own_end) = self.position();
        let meets = end_of_slot(&self.slots, last_slot, own_end).min(end);
        if meets < end || end >= own_end {
            let after_slot = slot_at(&self.slots, meets);
            self.send(
                to,
                Message::Fetched {
                    slot,
                    asked: end,
                    after: meets,
                    after_slot,
                    records: Vec::new(),
                },
            );
            return;
        }
        if !self.serving.iter().any(|&(asker, ..)| asker == to) {
            self.serving.push((to, slot, end));
            self.effects.push(Effect::Read {
                to,
                from: end.saturating_add(1),
            });
        }
    }

    fn on_fetched(
        &mut self,
        from: NodeId,
        slot: u64,
        asked: u64,
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
        if taking != slot || source != from || self.truncating || asked != self.log.end {
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
    }
}

/// Adds the slots `records` start to `slots`.
fn note_slots(slots: &mut Vec<(u64, u64)>, records: &[Vec<u8>]) {
    for record in records {
        let Ok(record) = decode(record) else { continue };
        if slots.last().is_none_or(|&(slot, _)| slot < record.slot) {
            slots.push((record.slot, record.lsn));
        }
    }
}
