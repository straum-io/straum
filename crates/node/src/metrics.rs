//! What a node measures about itself (D-112), and its Prometheus text.
//!
//! Owned by the node loop, one task: plain integers, no atomics, nothing
//! allocated per event — the queues of start times grow to a bound and are
//! reused. The bounds are the cores': accept times, the outbox's share
//! (`max_queue / voters`); batches awaiting durable or commit, what the
//! core lets past those points — `max_uncommitted` past the commit at a
//! sequencer, the replication `window` past what a replica reported
//! durable. Time comes
//! from the monotonic clock. Nothing here feeds a decision: the loop reads
//! the cores after they acted, and a snapshot is rendered only when
//! someone asks for one.
//!
//! A proposal of this node is in one of four states, never mixed:
//! refused (at a full outbox: never accepted), accepted and awaiting
//! commit (in the outbox), committed (its record applied here), or given
//! up (shown lost by a later one of this node committing first).

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::time::Duration;

use tokio::time::Instant;

/// Upper bounds of the buckets: 1 µs × 2^i, i in 0..BUCKETS — 1 µs to
/// about 67 s; slower lands in the last, counted in `count` as all are.
const BUCKETS: usize = 27;

/// A latency histogram with fixed power-of-two buckets.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Histogram {
    buckets: [u64; BUCKETS],
    count: u64,
    sum_micros: u64,
}

impl Histogram {
    pub fn observe(&mut self, took: Duration) {
        let micros = u64::try_from(took.as_micros()).unwrap_or(u64::MAX);
        // The smallest i with micros <= 2^i.
        let i = (u64::BITS - micros.saturating_sub(1).leading_zeros()) as usize;
        if let Some(bucket) = self.buckets.get_mut(i.min(BUCKETS - 1)) {
            *bucket = bucket.saturating_add(1);
        }
        self.count = self.count.saturating_add(1);
        self.sum_micros = self.sum_micros.saturating_add(micros);
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    /// The upper bound of the bucket holding quantile `q`, for a bench to
    /// report (D-133 B); `None` with no observations.
    pub fn quantile(&self, q: f64) -> Option<Duration> {
        let target = (self.count as f64 * q).ceil() as u64;
        let mut seen = 0u64;
        for (i, n) in self.buckets.iter().enumerate() {
            seen = seen.saturating_add(*n);
            if seen >= target.max(1) {
                return Some(Duration::from_micros(1u64 << i));
            }
        }
        None
    }

    fn render(&self, out: &mut String, name: &str, help: &str) {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} histogram");
        let mut cumulative = 0u64;
        for (i, n) in self.buckets.iter().enumerate() {
            cumulative = cumulative.saturating_add(*n);
            let le = (1u64 << i) as f64 / 1e6;
            let _ = writeln!(out, "{name}_bucket{{le=\"{le}\"}} {cumulative}");
        }
        let _ = writeln!(out, "{name}_bucket{{le=\"+Inf\"}} {}", self.count);
        let _ = writeln!(out, "{name}_sum {}", self.sum_micros as f64 / 1e6);
        let _ = writeln!(out, "{name}_count {}", self.count);
    }
}

/// A position's progress (D-132): where it is, when it last moved, and how
/// fast its lag changes — sampled at most once a second, so the rate spans
/// at least that. Reported only; nothing acts on it.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Progress {
    pub position: u64,
    pub lag: u64,
    moved_at: Option<Instant>,
    sample: Option<(u64, Instant)>,
    /// Positions a second: positive while the lag grows.
    pub lag_rate: f64,
    /// Heartbeats sampled, those in which the position did not move while
    /// there was a lag, and the positions it moved in all (D-135 A).
    pub ticks: u64,
    pub still_ticks: u64,
    pub advanced: u64,
}

impl Progress {
    pub fn update(&mut self, position: u64, lag: u64, now: Instant) {
        if self.moved_at.is_some() {
            self.ticks = self.ticks.saturating_add(1);
            if position > self.position {
                self.advanced = self.advanced.saturating_add(position - self.position);
            } else if lag > 0 {
                self.still_ticks = self.still_ticks.saturating_add(1);
            }
        }
        if self.moved_at.is_none() || position != self.position {
            self.moved_at = Some(now);
            self.position = position;
        }
        self.lag = lag;
        match self.sample {
            Some((before, at)) if now.saturating_duration_since(at) >= Duration::from_secs(1) => {
                let seconds = now.saturating_duration_since(at).as_secs_f64();
                self.lag_rate = (lag as f64 - before as f64) / seconds;
                self.sample = Some((lag, now));
            }
            None => self.sample = Some((lag, now)),
            _ => {}
        }
    }

    /// Time since the position last moved. With the lag: a lag above 0
    /// that does not move is a stalled catch-up; a lag of 0 that does not
    /// move, an idle cluster.
    pub fn since_moved(&self, now: Instant) -> Duration {
        self.moved_at
            .map_or(Duration::ZERO, |at| now.saturating_duration_since(at))
    }
}

/// What a sequencer has sent one replica and the replica has not yet
/// reported durable (D-135 A): appends recorded as (last position, bytes
/// sent so far), dropped once its durable position passes them. Resends
/// count again — they are on the wire again — but merge into the newest
/// entry, so the entries' positions only grow, and a full queue merges
/// too: a stalled replica resent to forever cannot grow it past
/// `FLIGHT_ENTRIES`.
/// At most this many sends tracked per replica; past it the newest entry
/// absorbs the rest, which only coarsens when bytes count as acknowledged.
const FLIGHT_ENTRIES: usize = 4_096;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Flight {
    sent: VecDeque<(u64, u64)>,
    total: u64,
    acked: u64,
    /// Now, and the most seen, in bytes and positions.
    pub bytes: u64,
    pub max_bytes: u64,
    pub positions: u64,
    /// Frames queued for the link to this peer at the last heartbeat.
    pub queued_frames: usize,
}

impl Flight {
    fn sent(&mut self, last: u64, bytes: u64) {
        self.total = self.total.saturating_add(bytes);
        let full = self.sent.len() >= FLIGHT_ENTRIES;
        match self.sent.back_mut() {
            Some(back) if last <= back.0 || full => {
                *back = (back.0.max(last), self.total);
            }
            _ => self.sent.push_back((last, self.total)),
        }
    }

    fn durable(&mut self, durable: u64) {
        while let Some(&(last, total)) = self.sent.front() {
            if last > durable {
                break;
            }
            self.acked = total;
            self.sent.pop_front();
        }
        self.bytes = self.total.saturating_sub(self.acked);
        self.max_bytes = self.max_bytes.max(self.bytes);
        self.positions = self
            .sent
            .back()
            .map_or(0, |&(last, _)| last.saturating_sub(durable));
    }
}

/// Spells of withheld reads: the node read none of its MQTT clients while
/// its outbox was full (D-120 A, D-123).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Spells {
    pub count: u64,
    pub total: Duration,
    pub longest: Duration,
}

/// A boundary a batch crosses on its way to a replica and back (D-136 A).
/// `first..=last` names the positions at that boundary; the bench joins
/// the nodes' stamps by them, over the one clock of its process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// Sequencer: a read from the log asked for `peer`, from `first`
    /// (`last`: as far as the window lets).
    ReadAsked,
    /// The log thread took the read up; `last` the batch's end, 0 none.
    ReadStarted,
    ReadDone,
    /// The node loop took the read's answer.
    ReadHandled,
    /// An append handed to the link to `peer`.
    Shipped,
    /// Replica: an append read from the link, from `peer`.
    Received,
    /// Handed to the log thread.
    Appended,
    /// The log thread took up the group that holds `last`.
    WalStarted,
    /// Written, before the sync.
    WalWritten,
    /// Synced: durable through `last`.
    Synced,
    /// The node loop took the durable position.
    DurableHandled,
    /// A durable report handed to the link to `peer` (`last`: durable).
    ReportSent,
    /// Sequencer: a durable report read from the link, from `peer`.
    ReportReceived,
    /// A read-back of committed records for apply: taken up, and done
    /// (`first` the records it brought).
    RangeStarted,
    RangeDone,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub stage: Stage,
    pub peer: u8,
    pub first: u64,
    pub last: u64,
    pub at: Instant,
}

/// Stamps of the stages, while a run asks for them (`Node::trace`); off,
/// it records nothing. Bounded: past `capacity` the newest are dropped and
/// counted.
#[derive(Debug, Clone, Default)]
pub struct Trace {
    pub stamps: Vec<Stamp>,
    pub dropped: u64,
    capacity: usize,
}

impl Trace {
    pub fn start(&mut self, capacity: usize) {
        self.stamps = Vec::with_capacity(capacity);
        self.dropped = 0;
        self.capacity = capacity;
    }

    /// What was recorded; recording stops.
    pub fn take(&mut self) -> Trace {
        std::mem::take(self)
    }

    /// As `stamp`, at now — the clock read only while recording: the
    /// node loop pays no clock read for an idle trace.
    pub fn stamp_now(&mut self, stage: Stage, peer: u8, first: u64, last: u64) {
        if self.capacity != 0 {
            self.stamp(stage, peer, first, last, Instant::now());
        }
    }

    pub fn on(&self) -> bool {
        self.capacity != 0
    }

    pub fn stamp(&mut self, stage: Stage, peer: u8, first: u64, last: u64, at: Instant) {
        if self.capacity == 0 {
            return;
        }
        if self.stamps.len() < self.capacity {
            self.stamps.push(Stamp {
                stage,
                peer,
                first,
                last,
                at,
            });
        } else {
            self.dropped = self.dropped.saturating_add(1);
        }
    }
}

/// What the loop measures (D-112). The cores' own counts are read at
/// snapshot time and joined in `Snapshot`.
#[derive(Debug, Default)]
pub struct Measured {
    /// Submitted to the outbox and taken; refused at a full one.
    pub accepted: u64,
    pub refused: u64,
    /// Of the accepted: committed (record applied here), and given up.
    pub committed: u64,
    pub given_up: u64,
    /// Frames dropped at a full link queue, and forwards among them: a named
    /// loss reason (D-115 A).
    pub link_drops: u64,
    pub forwards_dropped: u64,
    /// When each accepted proposal still in the outbox was accepted, in
    /// outbox order (the outbox removes only from its front).
    accepted_at: VecDeque<Instant>,
    /// Accepted → its record applied here.
    pub proposal_commit: Histogram,
    /// Batches appended to the local log, (last lsn, when), until durable
    /// and until committed.
    appended: VecDeque<(u64, Instant)>,
    awaiting_commit: VecDeque<(u64, Instant)>,
    pub append_durable: Histogram,
    pub append_commit: Histogram,
    /// Takeovers by silence this node started: completed (it leads the
    /// slot) or abandoned; how long the completed ones took.
    taking: Option<(u64, Instant)>,
    pub takeovers_completed: u64,
    pub takeovers_abandoned: u64,
    pub takeover: Histogram,
    /// Withheld reads (D-123): spells closed so far, and the open one.
    pub spells: Spells,
    blocked_since: Option<Instant>,
    /// D-132: this node's own log against the commit point it heard, and,
    /// sequencing, each replica's durable position against the commit.
    pub own: Progress,
    pub replicas: Vec<(u8, Progress)>,
    /// D-135 A: what is in flight to each peer.
    pub flights: Vec<(u8, Flight)>,
}

impl Measured {
    pub fn accepted(&mut self, now: Instant) {
        self.accepted = self.accepted.saturating_add(1);
        self.accepted_at.push_back(now);
    }

    pub fn refused(&mut self) {
        self.refused = self.refused.saturating_add(1);
    }

    /// A frame dropped at a full link queue; `forward` if it carried one of
    /// this node's proposals, which is then lost (the protocol resends the
    /// rest).
    pub fn link_dropped(&mut self, forward: bool) {
        self.link_drops = self.link_drops.saturating_add(1);
        if forward {
            self.forwards_dropped = self.forwards_dropped.saturating_add(1);
        }
    }

    /// The outbox lost `given_up` entries from its front and, if
    /// `confirmed`, one more, committed.
    pub fn left_outbox(&mut self, given_up: usize, confirmed: bool, now: Instant) {
        for _ in 0..given_up {
            self.accepted_at.pop_front();
        }
        self.given_up = self.given_up.saturating_add(given_up as u64);
        if confirmed {
            if let Some(at) = self.accepted_at.pop_front() {
                self.proposal_commit
                    .observe(now.saturating_duration_since(at));
            }
            self.committed = self.committed.saturating_add(1);
        }
    }

    pub fn appended(&mut self, last: u64, now: Instant) {
        self.appended.push_back((last, now));
        self.awaiting_commit.push_back((last, now));
    }

    /// The local log was cut after `end`: what was appended past it never
    /// becomes durable or committed here.
    pub fn truncated(&mut self, end: u64) {
        self.appended.retain(|&(last, _)| last <= end);
        self.awaiting_commit.retain(|&(last, _)| last <= end);
    }

    pub fn durable(&mut self, lsn: u64, now: Instant) {
        while let Some(&(last, at)) = self.appended.front() {
            if last > lsn {
                break;
            }
            self.appended.pop_front();
            self.append_durable
                .observe(now.saturating_duration_since(at));
        }
    }

    pub fn committed_through(&mut self, commit: u64, now: Instant) {
        while let Some(&(last, at)) = self.awaiting_commit.front() {
            if last > commit {
                break;
            }
            self.awaiting_commit.pop_front();
            self.append_commit
                .observe(now.saturating_duration_since(at));
        }
    }

    /// Each pass of the loop: a takeover by silence in progress (its slot),
    /// whether this node now leads, and in which slot.
    pub fn roles(&mut self, taking: Option<u64>, leading: bool, slot: u64, now: Instant) {
        match (self.taking, taking) {
            (None, Some(target)) => self.taking = Some((target, now)),
            (Some((target, since)), None) => {
                if leading && slot == target {
                    self.takeovers_completed = self.takeovers_completed.saturating_add(1);
                    self.takeover.observe(now.saturating_duration_since(since));
                } else {
                    self.takeovers_abandoned = self.takeovers_abandoned.saturating_add(1);
                }
                self.taking = None;
            }
            (Some((target, _)), Some(next)) if next != target => {
                self.takeovers_abandoned = self.takeovers_abandoned.saturating_add(1);
                self.taking = Some((next, now));
            }
            _ => {}
        }
    }

    /// An append sent to `peer`, ending at `last`, of `bytes` record bytes
    /// (D-135 A).
    pub fn shipped(&mut self, peer: u8, last: u64, bytes: u64) {
        match self.flights.iter_mut().find(|(p, _)| *p == peer) {
            Some((_, f)) => f.sent(last, bytes),
            None => {
                let mut f = Flight::default();
                f.sent(last, bytes);
                self.flights.push((peer, f));
            }
        }
    }

    /// Each heartbeat (D-135 A): the link queue's depth per peer.
    pub fn queued(&mut self, peer: u8, frames: usize) {
        if let Some((_, f)) = self.flights.iter_mut().find(|(p, _)| *p == peer) {
            f.queued_frames = frames;
        }
    }

    /// Each heartbeat (D-132): this node's log end against the commit point
    /// it heard, and each replica's durable position against this
    /// sequencer's commit. A replica not sequenced for is forgotten.
    pub fn progress(
        &mut self,
        end: u64,
        heard: u64,
        replicas: &[(u8, Option<u64>)],
        commit: u64,
        now: Instant,
    ) {
        self.own.update(end, heard.saturating_sub(end), now);
        self.replicas
            .retain(|(id, _)| replicas.iter().any(|(r, d)| r == id && d.is_some()));
        for &(id, durable) in replicas {
            let Some(durable) = durable else { continue };
            if let Some((_, f)) = self.flights.iter_mut().find(|(p, _)| *p == id) {
                f.durable(durable);
            }
            let lag = commit.saturating_sub(durable);
            match self.replicas.iter_mut().find(|(r, _)| *r == id) {
                Some((_, p)) => p.update(durable, lag, now),
                None => {
                    let mut p = Progress::default();
                    p.update(durable, lag, now);
                    self.replicas.push((id, p));
                }
            }
        }
    }

    /// Whether reads are withheld now (the outbox is full).
    pub fn blocked(&mut self, blocked: bool, now: Instant) {
        match (self.blocked_since, blocked) {
            (None, true) => self.blocked_since = Some(now),
            (Some(since), false) => {
                let took = now.saturating_duration_since(since);
                self.spells.count = self.spells.count.saturating_add(1);
                self.spells.total = self.spells.total.saturating_add(took);
                self.spells.longest = self.spells.longest.max(took);
                self.blocked_since = None;
            }
            _ => {}
        }
    }

    /// The spells, the open one included as far as it has gone.
    pub fn spells(&self, now: Instant) -> Spells {
        let mut spells = self.spells;
        if let Some(since) = self.blocked_since {
            let took = now.saturating_duration_since(since);
            spells.total = spells.total.saturating_add(took);
            spells.longest = spells.longest.max(took);
        }
        spells
    }
}

/// Connections refused before the gateway saw them, since the node started
/// (D-167 A): no handshake permit, a source at its cap (D-165 A), a
/// handshake that failed or did not finish in time (D-164 A).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Refused {
    pub no_permit: u64,
    pub source_full: u64,
    pub handshake_failed: u64,
}

/// Everything rendered at one scrape: the loop's measurements, the cores'
/// counts and the node's status, as they were together.
#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub node: u8,
    pub status: crate::Status,
    pub durable: u64,
    pub voter: cluster::VoterStats,
    /// Sequencing: each replica's durable position, if known.
    pub replicas: Vec<(u8, Option<u64>)>,
    pub gateway: Option<gateway::Load>,
    pub accepted: u64,
    pub refused: u64,
    pub committed: u64,
    pub given_up: u64,
    pub link_drops: u64,
    pub forwards_dropped: u64,
    pub proposal_commit: Histogram,
    pub append_durable: Histogram,
    pub append_commit: Histogram,
    pub takeovers_completed: u64,
    pub takeovers_abandoned: u64,
    pub takeover: Histogram,
    pub spells: Spells,
    /// D-132: the commit point heard, this node's own progress against
    /// it, each replica's (sequencing), and whether the gateway serves.
    pub heard_commit: u64,
    pub own: Progress,
    pub replica_progress: Vec<(u8, Progress)>,
    pub serving: bool,
    /// D-135 A: what is in flight to each peer.
    pub flights: Vec<(u8, Flight)>,
    pub refused_at_accept: Refused,
}

fn gauge(out: &mut String, name: &str, help: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} gauge");
    let _ = writeln!(out, "{name} {value}");
}

fn counter(out: &mut String, name: &str, help: &str, value: u64) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} counter");
    let _ = writeln!(out, "{name} {value}");
}

impl Snapshot {
    /// The Prometheus text exposition format, written by hand:
    /// every name starts `straum_`.
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(8 * 1024);
        let s = &self.status;
        let o = &mut out;
        // 1. Commit and durability.
        gauge(o, "straum_node", "This node's id.", self.node);
        gauge(o, "straum_log_end", "The local log's last lsn.", s.end);
        gauge(
            o,
            "straum_log_durable",
            "The local log durable through this lsn.",
            self.durable,
        );
        gauge(
            o,
            "straum_commit",
            "The commit point this node knows.",
            s.commit,
        );
        gauge(o, "straum_applied", "The last lsn applied here.", s.applied);
        gauge(
            o,
            "straum_slot",
            "The highest slot this node promised.",
            s.slot,
        );
        gauge(
            o,
            "straum_leading",
            "1 while this node sequences its slot.",
            u8::from(s.leading),
        );
        gauge(
            o,
            "straum_uncommitted",
            "Lsns in the local log past the commit point.",
            s.end.saturating_sub(s.commit),
        );
        self.proposal_commit.render(
            o,
            "straum_proposal_commit_seconds",
            "This node's own proposals: accepted into the outbox until its record is applied here.",
        );
        self.append_durable.render(
            o,
            "straum_append_durable_seconds",
            "A batch handed to the local log until it is durable there.",
        );
        self.append_commit.render(
            o,
            "straum_append_commit_seconds",
            "A batch handed to the local log until this node knows it committed.",
        );
        // 2. The outbox: this node's own proposals, by state.
        counter(
            o,
            "straum_proposals_accepted_total",
            "Own proposals accepted into the outbox.",
            self.accepted,
        );
        counter(
            o,
            "straum_proposals_refused_total",
            "Own proposals refused at a full outbox: never accepted.",
            self.refused,
        );
        counter(
            o,
            "straum_proposals_committed_total",
            "Own proposals whose record was applied here.",
            self.committed,
        );
        counter(
            o,
            "straum_proposals_given_up_total",
            "Own proposals shown lost by a later one committing first.",
            self.given_up,
        );
        let v = &self.voter;
        gauge(
            o,
            "straum_outbox_entries",
            "Own proposals accepted and awaiting commit.",
            v.outbox_entries,
        );
        gauge(
            o,
            "straum_outbox_sent",
            "Of those, sent and awaiting their slot's decision.",
            v.outbox_sent,
        );
        gauge(
            o,
            "straum_outbox_bytes",
            "Bytes the outbox holds (bodies and properties).",
            v.outbox_bytes,
        );
        gauge(
            o,
            "straum_outbox_capacity",
            "The outbox's bound: max_queue / voters.",
            v.outbox_capacity,
        );
        counter(
            o,
            "straum_outbox_resent_total",
            "Own proposals sent again after their slot closed.",
            v.outbox_resent,
        );
        counter(
            o,
            "straum_forwards_refused_total",
            "Other nodes' forwards this sequencer's full or sealed queue refused.",
            v.forwards_refused,
        );
        counter(
            o,
            "straum_forwards_dropped_total",
            "Own proposals' forwards dropped at a full link queue: lost, given up once a later one commits.",
            self.forwards_dropped,
        );
        counter(
            o,
            "straum_link_drops_total",
            "Frames dropped at a full link queue, forwards included; the protocol resends the rest.",
            self.link_drops,
        );
        // D-123: reads withheld while the outbox is full.
        let spells = self.spells;
        counter(
            o,
            "straum_read_block_spells_total",
            "Spells in which no MQTT client was read: the outbox was full.",
            spells.count,
        );
        gauge(
            o,
            "straum_read_block_seconds_total",
            "Time spent in such spells.",
            spells.total.as_secs_f64(),
        );
        gauge(
            o,
            "straum_read_block_longest_seconds",
            "The longest such spell, the open one included.",
            spells.longest.as_secs_f64(),
        );
        // 3. Replication, as the sequencer sees it.
        let _ = writeln!(
            o,
            "# HELP straum_replica_durable A replica's durable lsn, as it last reported to this sequencer."
        );
        let _ = writeln!(o, "# TYPE straum_replica_durable gauge");
        for (id, durable) in &self.replicas {
            if let Some(durable) = durable {
                let _ = writeln!(o, "straum_replica_durable{{replica=\"{id}\"}} {durable}");
            }
        }
        let _ = writeln!(
            o,
            "# HELP straum_replica_lag Lsns a replica's durable position is behind the commit point."
        );
        let _ = writeln!(o, "# TYPE straum_replica_lag gauge");
        for (id, durable) in &self.replicas {
            if let Some(durable) = durable {
                let _ = writeln!(
                    o,
                    "straum_replica_lag{{replica=\"{id}\"}} {}",
                    s.commit.saturating_sub(*durable)
                );
            }
        }
        // D-132: catching up. A lag above 0 whose position has not moved
        // for long is a stalled catch-up; one that shrinks, a slow one; one
        // that grows, a replica losing ground under load. Nothing acts on
        // these: they are for an operator and the Linux run.
        let now = Instant::now();
        let own = &self.own;
        gauge(
            o,
            "straum_heard_commit",
            "The commit point this node has heard of, not capped at its log's end.",
            self.heard_commit,
        );
        gauge(
            o,
            "straum_lag",
            "Positions this node's log is behind the commit point it heard.",
            own.lag,
        );
        gauge(
            o,
            "straum_lag_rate",
            "How fast that lag changes, positions a second; above 0 while it grows.",
            own.lag_rate,
        );
        gauge(
            o,
            "straum_progress_age_seconds",
            "Time since this node's log end last moved.",
            own.since_moved(now).as_secs_f64(),
        );
        gauge(
            o,
            "straum_catching_up",
            "1 while lag is above 0. An indicator only: a healthy replica under load is a few positions behind too; telling catch-up from normal replication takes lag, rate and age together.",
            u8::from(own.lag > 0),
        );
        gauge(
            o,
            "straum_serving",
            "1 once this node's gateway takes clients: its own reset is applied.",
            u8::from(self.serving),
        );
        let _ = writeln!(
            o,
            "# HELP straum_replica_progress_age_seconds Time since a replica's durable position last moved, as this sequencer saw it."
        );
        let _ = writeln!(o, "# TYPE straum_replica_progress_age_seconds gauge");
        for (id, p) in &self.replica_progress {
            let _ = writeln!(
                o,
                "straum_replica_progress_age_seconds{{replica=\"{id}\"}} {}",
                p.since_moved(now).as_secs_f64()
            );
        }
        let _ = writeln!(
            o,
            "# HELP straum_replica_lag_rate How fast a replica's lag changes, positions a second; above 0 while it grows."
        );
        let _ = writeln!(o, "# TYPE straum_replica_lag_rate gauge");
        for (id, p) in &self.replica_progress {
            let _ = writeln!(
                o,
                "straum_replica_lag_rate{{replica=\"{id}\"}} {}",
                p.lag_rate
            );
        }
        // D-135 A: progress per heartbeat, this node's and each replica's,
        // and what is in flight to each peer.
        counter(
            o,
            "straum_progress_ticks_total",
            "Heartbeats at which this node's progress was sampled.",
            own.ticks,
        );
        counter(
            o,
            "straum_progress_still_ticks_total",
            "Of those, heartbeats with a lag and no movement.",
            own.still_ticks,
        );
        counter(
            o,
            "straum_progress_advanced_total",
            "Positions this node's log end moved over those heartbeats.",
            own.advanced,
        );
        let _ = writeln!(
            o,
            "# HELP straum_replica_still_ticks_total Heartbeats with a replica behind and its durable position not moved."
        );
        let _ = writeln!(o, "# TYPE straum_replica_still_ticks_total counter");
        for (id, p) in &self.replica_progress {
            let _ = writeln!(
                o,
                "straum_replica_still_ticks_total{{replica=\"{id}\"}} {}",
                p.still_ticks
            );
        }
        for (name, help, read) in [
            (
                "straum_replica_bytes_in_flight",
                "Record bytes sent to a replica and not yet reported durable, resends included.",
                (|f: &Flight| f.bytes as f64) as fn(&Flight) -> f64,
            ),
            (
                "straum_replica_bytes_in_flight_max",
                "The most seen.",
                |f: &Flight| f.max_bytes as f64,
            ),
            (
                "straum_replica_positions_in_flight",
                "Positions sent past a replica's durable position.",
                |f: &Flight| f.positions as f64,
            ),
            (
                "straum_link_queued_frames",
                "Frames queued for the link to a peer at the last heartbeat.",
                |f: &Flight| f.queued_frames as f64,
            ),
        ] {
            let _ = writeln!(o, "# HELP {name} {help}");
            let _ = writeln!(o, "# TYPE {name} gauge");
            for (id, f) in &self.flights {
                let _ = writeln!(o, "{name}{{peer=\"{id}\"}} {}", read(f));
            }
        }
        // 4. Takeover.
        counter(
            o,
            "straum_takeovers_started_total",
            "Takeovers this node started after its owner's silence.",
            v.takeovers,
        );
        counter(
            o,
            "straum_takeovers_completed_total",
            "Of those, ending with this node sequencing the slot.",
            self.takeovers_completed,
        );
        counter(
            o,
            "straum_takeovers_abandoned_total",
            "Of those, abandoned: another slot won, or the owner was heard.",
            self.takeovers_abandoned,
        );
        self.takeover.render(
            o,
            "straum_takeover_seconds",
            "A takeover by silence, started until this node sequences the slot.",
        );
        counter(
            o,
            "straum_successions_total",
            "Slots taken over by a committed SlotEnd (handoff).",
            v.successions,
        );
        counter(
            o,
            "straum_slots_led_total",
            "Slots this node began sequencing.",
            v.slots_led,
        );
        counter(
            o,
            "straum_stepped_down_total",
            "Times this node stopped sequencing: handed over, or fenced by a newer slot.",
            v.stepped_down,
        );
        // 5. MQTT.
        if let Some(g) = &self.gateway {
            let c = &g.counted;
            gauge(
                o,
                "straum_mqtt_connections",
                "Open MQTT connections.",
                g.connections,
            );
            gauge(
                o,
                "straum_mqtt_pending",
                "Client requests proposed and awaiting commit.",
                g.pending,
            );
            gauge(
                o,
                "straum_mqtt_pending_bytes",
                "Their bytes.",
                g.pending_bytes,
            );
            gauge(
                o,
                "straum_mqtt_waiting_bytes",
                "Deliveries held for clients' receive maximum.",
                g.waiting_bytes,
            );
            counter(
                o,
                "straum_mqtt_publishes_total",
                "PUBLISH packets proposed to the log.",
                c.publishes,
            );
            counter(
                o,
                "straum_mqtt_pubacks_total",
                "PUBACKs sent, each after its commit.",
                c.pubacks,
            );
            counter(
                o,
                "straum_mqtt_deliveries_total",
                "PUBLISH packets sent to subscribers.",
                c.deliveries,
            );
            counter(
                o,
                "straum_mqtt_dropped_total",
                "At-most-once deliveries dropped at the waiting bounds.",
                g.dropped,
            );
            counter(
                o,
                "straum_mqtt_closed_total",
                "Connections closed.",
                c.closed,
            );
            counter(
                o,
                "straum_mqtt_closed_keep_alive_total",
                "Of those, for keep-alive silence.",
                c.closed_keep_alive,
            );
            counter(
                o,
                "straum_mqtt_closed_commit_timeout_total",
                "Of those, for a proposal not committed in time.",
                c.closed_commit_timeout,
            );
            // D-167 A: never seen by the gateway, so not in its counts.
            let r = &self.refused_at_accept;
            counter(
                o,
                "straum_mqtt_refused_no_permit_total",
                "Connections closed at accept: every handshake slot was taken.",
                r.no_permit,
            );
            counter(
                o,
                "straum_mqtt_refused_source_full_total",
                "Connections closed at accept: their source had as many waiting as it may.",
                r.source_full,
            );
            counter(
                o,
                "straum_mqtt_handshake_failed_total",
                "Connections whose setup (read buffers, or the TLS handshake) failed or did not finish within the connect timeout.",
                r.handshake_failed,
            );
        }
        out
    }
}

/// Connections served at once; one more waits in the kernel's backlog until
/// one ends. Each holds at most one snapshot's text and ends within
/// `IO_TIMEOUT` on its read and on its write: a client that connects and
/// stays silent, or never reads, cannot hold memory or a task for longer.
const MAX_SCRAPES: usize = 4;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

/// Serves the Prometheus text over HTTP: every request, whatever it asks,
/// gets the current snapshot (D-112). Plain HTTP/1.1, one response per
/// connection, written by hand. At most `MAX_SCRAPES` at
/// once: memory stays bounded however many connect. Ends
/// with the node.
pub async fn serve(
    listener: tokio::net::TcpListener,
    requests: tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<Snapshot>>,
) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let permits = std::sync::Arc::new(tokio::sync::Semaphore::new(MAX_SCRAPES));
    loop {
        // A permit before the accept: past the bound, connections wait
        // unaccepted rather than as tasks.
        let Ok(permit) = std::sync::Arc::clone(&permits).acquire_owned().await else {
            return;
        };
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let requests = requests.clone();
        tokio::spawn(async move {
            let _permit = permit;
            // The request is read and ignored: one bounded read, then answer.
            let mut request = [0u8; 1024];
            if !matches!(
                tokio::time::timeout(IO_TIMEOUT, stream.read(&mut request)).await,
                Ok(Ok(n)) if n > 0
            ) {
                return;
            }
            let (reply, answer) = tokio::sync::oneshot::channel();
            if requests.send(reply).await.is_err() {
                return;
            }
            let Ok(snapshot) = answer.await else { return };
            let body = snapshot.render();
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            let _ = tokio::time::timeout(IO_TIMEOUT, async {
                stream.write_all(head.as_bytes()).await?;
                stream.write_all(body.as_bytes()).await
            })
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_observation_lands_in_the_smallest_bucket_that_holds_it() {
        let mut h = Histogram::default();
        h.observe(Duration::from_micros(1));
        h.observe(Duration::from_micros(3));
        h.observe(Duration::from_micros(4));
        h.observe(Duration::from_secs(1000));
        assert_eq!(h.buckets[0], 1);
        assert_eq!(h.buckets[2], 2, "3 µs and 4 µs both fit 4 µs");
        assert_eq!(
            h.buckets[BUCKETS - 1],
            1,
            "past the last bound, in the last"
        );
        assert_eq!(h.count(), 4);
    }

    #[test]
    fn proposals_leave_the_outbox_in_order_and_are_counted_by_state() {
        let mut m = Measured::default();
        let t0 = Instant::now();
        for _ in 0..3 {
            m.accepted(t0);
        }
        m.refused();
        m.link_dropped(true);
        m.link_dropped(false);
        assert_eq!((m.link_drops, m.forwards_dropped), (2, 1));
        // One given up, one committed; one still awaiting.
        m.left_outbox(1, true, t0 + Duration::from_millis(5));
        assert_eq!(
            (m.accepted, m.refused, m.committed, m.given_up),
            (3, 1, 1, 1)
        );
        assert_eq!(m.accepted_at.len(), 1);
        assert_eq!(m.proposal_commit.count(), 1);
    }

    #[test]
    fn a_batch_is_timed_to_durable_and_to_commit_and_a_cut_forgets_it() {
        let mut m = Measured::default();
        let t0 = Instant::now();
        m.appended(4, t0);
        m.appended(8, t0);
        m.durable(4, t0);
        assert_eq!(m.append_durable.count(), 1);
        m.truncated(4);
        m.durable(8, t0);
        m.committed_through(8, t0);
        assert_eq!((m.append_durable.count(), m.append_commit.count()), (1, 1));
    }

    #[test]
    fn a_takeover_is_timed_when_it_ends_with_the_slot_and_counted_when_abandoned() {
        let mut m = Measured::default();
        let t0 = Instant::now();
        m.roles(Some(2), false, 1, t0);
        m.roles(None, true, 2, t0 + Duration::from_millis(30));
        m.roles(Some(5), false, 2, t0);
        m.roles(None, false, 3, t0);
        assert_eq!((m.takeovers_completed, m.takeovers_abandoned), (1, 1));
        assert_eq!(m.takeover.count(), 1);
    }

    #[test]
    fn a_spell_of_withheld_reads_counts_once_it_ends_and_shows_while_open() {
        let mut m = Measured::default();
        let t0 = Instant::now();
        m.blocked(true, t0);
        assert_eq!(
            m.spells(t0 + Duration::from_secs(2)).longest,
            Duration::from_secs(2)
        );
        m.blocked(false, t0 + Duration::from_secs(3));
        let spells = m.spells(t0 + Duration::from_secs(9));
        assert_eq!((spells.count, spells.longest), (1, Duration::from_secs(3)));
    }
}

#[cfg(test)]
mod serving {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// A burst of silent connections takes at most `MAX_SCRAPES` at a time,
    /// each closed after `IO_TIMEOUT`; a real scrape behind them is still
    /// answered (D-112: bounded under any input).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn silent_connections_are_bounded_and_closed_and_a_scrape_still_answers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (requests, mut asked) =
            tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<Snapshot>>(4);
        tokio::spawn(serve(listener, requests));
        tokio::spawn(async move {
            while let Some(reply) = asked.recv().await {
                let _ = reply.send(Snapshot::default());
            }
        });
        let mut silent = Vec::new();
        for _ in 0..2 * MAX_SCRAPES {
            silent.push(tokio::net::TcpStream::connect(address).await.unwrap());
        }
        let mut scrape = tokio::net::TcpStream::connect(address).await.unwrap();
        scrape.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
        let mut text = String::new();
        // Two rounds of silent connections timing out, then this one.
        tokio::time::timeout(IO_TIMEOUT * 4, scrape.read_to_string(&mut text))
            .await
            .expect("the scrape waited past the silent connections' timeouts")
            .unwrap();
        assert!(text.starts_with("HTTP/1.1 200 OK"), "{text}");
        // Every silent connection was closed by the server, unanswered.
        for mut stream in silent {
            let mut byte = [0u8; 1];
            let read = tokio::time::timeout(IO_TIMEOUT * 2, stream.read(&mut byte)).await;
            assert!(matches!(read, Ok(Ok(0) | Err(_))), "{read:?}");
        }
    }
}

#[cfg(test)]
mod progress {
    use super::*;

    /// D-132: a position that stops moving ages; one that moves resets its
    /// age; the lag's rate is taken over at least a second.
    #[test]
    fn progress_ages_when_still_and_rates_the_lag() {
        let t0 = Instant::now();
        let mut p = Progress::default();
        p.update(10, 100, t0);
        p.update(10, 100, t0 + Duration::from_millis(500));
        assert_eq!(
            p.since_moved(t0 + Duration::from_secs(3)),
            Duration::from_secs(3)
        );
        assert_eq!(p.lag_rate, 0.0, "no rate before a second has passed");
        p.update(60, 40, t0 + Duration::from_secs(2));
        assert_eq!(p.since_moved(t0 + Duration::from_secs(2)), Duration::ZERO);
        assert!((p.lag_rate - -30.0).abs() < 1e-9, "{}", p.lag_rate);
    }
}

#[cfg(test)]
mod flight {
    use super::*;

    /// D-136 A: off, a trace records nothing; on, at most its capacity,
    /// the rest counted; taking it stops it.
    #[test]
    fn a_trace_records_only_while_on_and_to_its_bound() {
        let now = Instant::now();
        let mut t = Trace::default();
        t.stamp(Stage::Shipped, 1, 1, 1, now);
        assert!(t.stamps.is_empty());
        t.start(2);
        for i in 0..5 {
            t.stamp(Stage::Shipped, 1, i, i, now);
        }
        let taken = t.take();
        assert_eq!((taken.stamps.len(), taken.dropped), (2, 3));
        t.stamp(Stage::Shipped, 1, 9, 9, now);
        assert!(t.stamps.is_empty());
    }

    /// D-135 A: bytes in flight are those sent past the durable position;
    /// a still heartbeat is one with a lag and no movement.
    #[test]
    fn bytes_in_flight_follow_the_durable_position_and_still_ticks_count() {
        let mut f = Flight::default();
        f.sent(10, 100);
        f.sent(20, 50);
        f.durable(5);
        assert_eq!((f.bytes, f.positions), (150, 15));
        f.durable(10);
        assert_eq!((f.bytes, f.max_bytes, f.positions), (50, 150, 10));
        f.durable(20);
        assert_eq!((f.bytes, f.positions), (0, 0));

        // A stalled replica resent to: one entry, the bytes counted again.
        let mut s = Flight::default();
        for _ in 0..10_000 {
            s.sent(30, 10);
        }
        s.durable(20);
        assert_eq!((s.sent.len(), s.bytes, s.positions), (1, 100_000, 10));
        // Past the cap, new positions merge into the newest entry.
        let mut c = Flight::default();
        for last in 1..=(FLIGHT_ENTRIES as u64 + 100) {
            c.sent(last, 1);
        }
        assert_eq!(c.sent.len(), FLIGHT_ENTRIES);
        c.durable(FLIGHT_ENTRIES as u64 + 100);
        assert_eq!((c.bytes, c.positions), (0, 0));

        let t0 = Instant::now();
        let mut p = Progress::default();
        p.update(0, 10, t0);
        p.update(0, 10, t0);
        p.update(4, 6, t0);
        assert_eq!((p.ticks, p.still_ticks, p.advanced), (2, 1, 4));
    }
}

#[cfg(test)]
mod stable {
    use super::*;

    /// D-141 A: the stable set in docs/configuration.md is what clients
    /// outside the cluster may rely on; each is rendered once, as a gauge
    /// without labels. The list is read from the document, so the two
    /// cannot drift apart.
    #[test]
    fn the_stable_metrics_are_rendered_as_gauges() {
        let doc = include_str!("../../../docs/configuration.md");
        let block = doc
            .split("<!-- stable-metrics:begin -->")
            .nth(1)
            .and_then(|rest| rest.split("<!-- stable-metrics:end -->").next())
            .expect("the stable-metrics block in docs/configuration.md");
        let names: Vec<&str> = block
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("straum_"))
            .collect();
        assert_eq!(names.len(), 11, "the stable set changed: {names:?}");
        let text = Snapshot::default().render();
        for name in names {
            assert_eq!(
                text.matches(&format!("# TYPE {name} gauge\n")).count(),
                1,
                "{name} is not one gauge"
            );
            let samples: Vec<&str> = text
                .lines()
                .filter(|l| l.split([' ', '{']).next() == Some(name))
                .collect();
            assert_eq!(samples.len(), 1, "{name}: {samples:?}");
            assert!(
                samples[0].starts_with(&format!("{name} ")),
                "{name} has labels"
            );
        }
    }
}
