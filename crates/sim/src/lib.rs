//! Deterministic simulation of the leader/follower replication (phase 0,
//! ADR-0007; docs/architecture/unified-stream.md §5).
//!
//! Two nodes, each on its own simulated disk, run the production code that
//! decides: the log (`storage::Wal` on `SimDisk`), the leader's link
//! (`replication::Link`, `Redial`), the follower (`replication::Follower`)
//! and the frame codec. What this crate supplies is what the runtime
//! supplies in production — moving bytes, waiting, restarting — and every
//! choice it makes comes from one seed: which event happens next, how many
//! bytes a read delivers, when a node crashes or loses power, how much of an
//! unsynced write survives. No thread, no task, no clock, no socket.
//!
//! After every step the oracle checks these. The names are the harness's own;
//! the right-hand column says which invariant of unified-stream §5 or of D-28
//! each one covers, and how much of it.
//!
//! ```text
//! U1             a committed record never disappears or changes its sequence,
//!                over the addressable region of the leader's log  — unified §5.1,
//!                narrowed: compaction drops consumed records by design (ADR-0007);
//!                D-28 I3 for `fsynced` on the leader
//! U3             where both nodes hold a sequence, they hold the same record
//!                                                       — unified §5.3; D-28 I6 (prefix)
//! U5-lsn         sequences are monotonic; a restart never hands one out again
//!                                — unified §5.5 without prev_hash; D-28 I7 without correlation
//! H-quorum-copy  under `quorum` a committed record is on the follower too
//!                                                   — D-28 I4, the "copies" half
//! H-watermark    the quorum watermark never moves back within a leader process,
//!                and a report never claims more than was sent      — D-28 I5, in part
//! H-ship-durable a follower is never sent a record before the leader made it
//!                durable                                 — D-28 I6, first half
//! H-ahead        a follower is never ahead of a leader that lost nothing synced
//!                                                                   — harness's own
//! H-reported     a follower's reported durable position survives its own power
//!                loss                                 — harness's own; serves D-28 I4
//! H-floor        the floor never moves back without a power loss, and under
//!                `quorum` a rewrite never moves it past the follower
//!                                    — D-28 I10, first two parts; "within R served" not
//! ```
//!
//! Not checked: D-28 I1, I2 and I9 and the PUBACK side of I3 (they need the
//! broker; D-44), I8 as D-28 defines it (no PUBACK exists here to withhold),
//! correlations (no broker records), and "within R is served".
//!
//! "Committed" is what a PUBACK would have been sent for: the leader's own
//! durable sequence under `fsynced`, the quorum watermark under `quorum`.
//!
//! A run is reproduced by its seed: `run(Config { seed, .. })` makes the same
//! choices and produces the same trace hash every time.

use std::collections::BTreeMap;
use std::fmt;
use std::path::{Path, PathBuf};

use replication::{Accepted, Follower, Limits, Link, Live, Message, Records, Redial};
use storage::simulated::SimDisk;
use storage::{Retain, Wal, WalLimits, allocated_on, replay_addressable_on};

/// The durability level the leader acknowledges at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Acknowledged once durable on the leader; the follower is a copy.
    Fsynced,
    /// Acknowledged once durable on the leader and the follower.
    Quorum,
}

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub seed: u64,
    pub steps: u32,
    pub mode: Mode,
}

/// How often each kind of event happened in one run.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counts {
    pub commits: u64,
    pub chunks: u64,
    pub frames: u64,
    pub reports: u64,
    pub leader_crashes: u64,
    pub leader_power_losses: u64,
    pub follower_crashes: u64,
    pub follower_power_losses: u64,
    pub sync_failures: u64,
    pub cuts: u64,
    pub connections: u64,
    pub stalls: u64,
    pub refusals: u64,
    pub rewrites: u64,
    pub fresh_copies: u64,
}

/// What a run ended with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// Hash of every choice made and every outcome seen, in order.
    pub trace: u64,
    /// The highest sequence ever committed.
    pub committed: u64,
    pub counts: Counts,
}

/// An invariant that did not hold.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub seed: u64,
    pub step: u32,
    pub invariant: &'static str,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "seed {} step {}: {} — {}",
            self.seed, self.step, self.invariant, self.detail
        )
    }
}

/// Bytes a direction of the connection holds before its writer has to wait:
/// small, so a follower that stops reading fills it within a few frames.
const WINDOW: usize = 1024;
/// The write-stall limit, in simulated milliseconds (D-27).
const STALL_MS: u64 = 300;
/// Largest batch a catch-up frame carries.
const CATCH_UP_BYTES: usize = 512;

fn path() -> PathBuf {
    PathBuf::from("/sim/node.wal")
}

/// xorshift64*: the one source of every choice.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // A zero state would stay zero.
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform in `0..bound`, `bound > 0`.
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

/// FNV-1a over everything that happened.
struct Trace(u64);

impl Trace {
    fn note(&mut self, values: &[u64]) {
        for value in values {
            for byte in value.to_le_bytes() {
                self.0 ^= u64::from(byte);
                self.0 = self.0.wrapping_mul(0x0000_0100_0000_01B3);
            }
        }
    }
}

/// One direction of a connection: bytes on the wire, and bytes the receiver
/// has read but not yet consumed as whole messages.
#[derive(Default)]
struct Pipe {
    wire: Vec<u8>,
    read: Vec<u8>,
}

struct Connection {
    to_follower: Pipe,
    to_leader: Pipe,
    /// The leader's side, once the follower has spoken.
    link: Option<Link>,
    /// Since when the leader has had a frame it could not write.
    stalled_since: Option<u64>,
}

struct Leader {
    disk: SimDisk,
    wal: Wal<SimDisk>,
    /// Bodies by sequence, as this leader wrote or recovered them.
    log: BTreeMap<u64, Vec<u8>>,
    /// Bodies are unique across the run, so a record can never be mistaken
    /// for another one that happened to be written under the same number.
    next_body: u64,
    /// The highest watermark announced in this process.
    watermark: u64,
    /// What compaction keeps under `quorum`: the follower's reported
    /// position. Zero until it has spoken, as after a restart (D-20, D-25).
    replicated: u64,
    redial: Redial,
    redial_at: u64,
    /// The highest retention floor seen since the last power loss.
    floor_seen: u64,
}

struct FollowerNode {
    disk: SimDisk,
    wal: Wal<SimDisk>,
    state: Follower,
    /// A batch handed to the log and not yet synced.
    staged: bool,
    reading: bool,
    /// The highest durable position this follower has reported.
    reported: u64,
    /// Refused by the leader, waiting for an operator's fresh copy.
    refused: bool,
    /// Restarted after a failed sync and not synced since: what it holds may
    /// be only in the operating system (the D-43 signature).
    recovered_unsynced: bool,
}

/// The whole world of one run.
struct World {
    config: Config,
    rng: Rng,
    trace: Trace,
    step: u32,
    clock: u64,
    leader: Leader,
    follower: FollowerNode,
    connection: Option<Connection>,
    /// The follower's log failed while no link was up; it restarts at the
    /// next opportunity.
    follower_poisoned: bool,
    /// Every committed record, by sequence. The oracle's memory.
    committed: BTreeMap<u64, Vec<u8>>,
    committed_through: u64,
    counts: Counts,
}

/// Opens a node's log. A failed open is a process that did not come up: it
/// is started again, as an operator or a supervisor would, and an injected
/// failure is spent on the first attempt.
fn open(disk: &SimDisk) -> Result<(Wal<SimDisk>, u64), String> {
    let attempt = || Wal::open_on(disk.clone(), &path(), WalLimits::default());
    let (wal, recovery) = attempt()
        .or_else(|_| attempt())
        .map_err(|e| e.to_string())?;
    Ok((wal, recovery.sequence))
}

/// The addressable region of a node's log, by sequence.
fn addressable(disk: &SimDisk) -> Result<BTreeMap<u64, Vec<u8>>, String> {
    let mut seen = BTreeMap::new();
    replay_addressable_on(
        disk,
        &path(),
        WalLimits::default(),
        &mut |_| true,
        &mut |sequence, framed| {
            // The framing is eight bytes of length and checksum.
            seen.insert(sequence, framed.get(8..).unwrap_or_default().to_vec());
        },
    )
    .map_err(|e| e.to_string())?;
    Ok(seen)
}

fn floor(disk: &SimDisk) -> Result<u64, String> {
    allocated_on(disk, &path())
        .map(|held| held.retention_floor())
        .map_err(|e| e.to_string())
}

impl World {
    fn new(config: Config) -> Result<Self, String> {
        let leader_disk = SimDisk::new();
        let follower_disk = SimDisk::new();
        let (leader_wal, _) = open(&leader_disk)?;
        let (follower_wal, _) = open(&follower_disk)?;
        Ok(Self {
            config,
            rng: Rng::new(config.seed),
            trace: Trace(0xCBF2_9CE4_8422_2325),
            step: 0,
            clock: 0,
            leader: Leader {
                disk: leader_disk,
                wal: leader_wal,
                log: BTreeMap::new(),
                next_body: 0,
                watermark: 0,
                replicated: 0,
                redial: Redial::new(),
                redial_at: 0,
                floor_seen: 1,
            },
            follower: FollowerNode {
                disk: follower_disk,
                wal: follower_wal,
                state: Follower::resuming_from(0),
                staged: false,
                reading: true,
                reported: 0,
                refused: false,
                recovered_unsynced: false,
            },
            connection: None,
            follower_poisoned: false,
            committed: BTreeMap::new(),
            committed_through: 0,
            counts: Counts::default(),
        })
    }

    fn violation(&self, invariant: &'static str, detail: String) -> Violation {
        Violation {
            seed: self.config.seed,
            step: self.step,
            invariant,
            detail,
        }
    }

    fn local(&self) -> u64 {
        self.leader.wal.durable_sequence()
    }

    /// Records everything through `through` as committed: what a PUBACK
    /// would have been sent for.
    fn commit_through(&mut self, through: u64) -> Result<(), Violation> {
        for sequence in self.committed_through + 1..=through {
            let Some(body) = self.leader.log.get(&sequence).cloned() else {
                return Err(self.violation(
                    "U1",
                    format!("committed {sequence}, which the leader does not hold"),
                ));
            };
            self.committed.insert(sequence, body);
        }
        self.committed_through = self.committed_through.max(through);
        Ok(())
    }

    fn step(&mut self) -> Result<(), Violation> {
        self.step += 1;
        self.clock += 1 + self.rng.below(20);
        let event = self.rng.below(1000);
        self.trace.note(&[u64::from(self.step), event]);
        // Ordinary work most of the time, faults rarely: a fault every few
        // steps leaves connections no time to replicate anything, and the
        // invariants would hold over nothing. Per thousand steps: about two
        // hundred commits, five hundred deliveries, a dozen faults.
        match event {
            0..=219 => self.commit()?,
            220..=499 => self.deliver_to_follower()?,
            500..=689 => self.deliver_to_leader()?,
            690..=819 => self.follower_sync()?,
            820..=939 => self.leader_ship()?,
            940..=969 => self.connect()?,
            970..=979 => self.rewrite()?,
            980..=981 => self.cut("cut"),
            982..=984 => self.stop_reading(),
            985..=986 => self.leader_restart(false)?,
            987 => self.leader_restart(true)?,
            988..=989 => self.follower_restart(false)?,
            990 => self.follower_restart(true)?,
            991..=994 => self.fresh_copy()?,
            995..=996 => self.inject_sync_failure(),
            _ => {}
        }
        self.check_stall();
        self.check()
    }

    fn commit(&mut self) -> Result<(), Violation> {
        let count = 1 + self.rng.below(4);
        let first = self.local() + 1;
        let mut bodies = Vec::new();
        for _ in 0..count {
            let body = format!("r{}", self.leader.next_body).into_bytes();
            self.leader.next_body += 1;
            if self.leader.wal.stage(&body).is_err() {
                // Poisoned by an earlier failure: the process has to restart.
                return self.leader_restart(false);
            }
            bodies.push(body);
        }
        match self.leader.wal.commit_with(true) {
            Ok((done, _, _)) => {
                let durable = done.durable_sequence;
                if durable != first + count - 1 {
                    return Err(self.violation(
                        "U5-lsn",
                        format!(
                            "committed through {durable}, expected {}",
                            first + count - 1
                        ),
                    ));
                }
                for (offset, body) in bodies.into_iter().enumerate() {
                    self.leader.log.insert(first + offset as u64, body);
                }
                self.counts.commits += 1;
                self.trace.note(&[durable]);
                if self.config.mode == Mode::Fsynced {
                    self.commit_through(durable)?;
                }
                self.ship_live(first - 1, durable)
            }
            Err(_) => {
                // A failed write or sync: nothing is acknowledged, and the
                // process restarts on what the operating system holds.
                self.counts.sync_failures += 1;
                self.leader_restart(false)
            }
        }
    }

    /// The live queue: the batch just committed, if it is the next thing the
    /// follower needs and the window has room. Otherwise it is dropped, as a
    /// full queue drops it, and the log fills the gap.
    fn ship_live(&mut self, previous: u64, last: u64) -> Result<(), Violation> {
        let batch = self.leader.wal.committed_batch().to_vec();
        let Some(connection) = self.connection.as_mut() else {
            return Ok(());
        };
        let Some(link) = connection.link.as_mut() else {
            return Ok(());
        };
        if link.live(previous, last) == Live::Skip || connection.to_follower.wire.len() >= WINDOW {
            return Ok(());
        }
        replication::encode_records_header(
            previous,
            last,
            &batch,
            &mut connection.to_follower.wire,
        );
        connection.to_follower.wire.extend_from_slice(&batch);
        link.on_sent(last);
        self.counts.frames += 1;
        Ok(())
    }

    /// The catch-up path: what `Link::behind` names, read from the log.
    fn leader_ship(&mut self) -> Result<(), Violation> {
        let local = self.local();
        let Some(connection) = self.connection.as_ref() else {
            return Ok(());
        };
        let Some(link) = connection.link.as_ref() else {
            return Ok(());
        };
        if connection.to_follower.wire.len() >= WINDOW {
            return Ok(());
        }
        let Some((from, until)) = link.behind(local) else {
            return Ok(());
        };
        // I6: nothing past what this node has made durable.
        if until > local {
            return Err(self.violation(
                "H-ship-durable",
                format!("catch-up to {until} with only {local} durable"),
            ));
        }
        let mut batch = Vec::new();
        let mut last = from - 1;
        let mut refused = None;
        replay_addressable_on(
            &self.leader.disk,
            &path(),
            WalLimits::default(),
            &mut |floor| {
                let served = Link::serves(from, floor);
                if !served {
                    refused = Some(floor);
                }
                served
            },
            &mut |sequence, framed| {
                if sequence == last + 1 && sequence <= until && batch.len() < CATCH_UP_BYTES {
                    batch.extend_from_slice(framed);
                    last = sequence;
                }
            },
        )
        .map_err(|e| self.violation("storage", e.to_string()))?;
        if let Some(floor) = refused {
            // Below the floor: refused, and dialled again later (D-23).
            self.counts.refusals += 1;
            self.trace.note(&[floor]);
            self.follower.refused = true;
            self.leader.replicated = u64::MAX;
            self.cut("refused");
            self.leader.redial.refused(from);
            return Ok(());
        }
        if last < from {
            return Ok(());
        }
        let Some(connection) = self.connection.as_mut() else {
            return Ok(());
        };
        let Some(link) = connection.link.as_mut() else {
            return Ok(());
        };
        replication::encode_records_header(
            from - 1,
            last,
            &batch,
            &mut connection.to_follower.wire,
        );
        connection.to_follower.wire.extend_from_slice(&batch);
        link.on_sent(last);
        self.counts.frames += 1;
        self.trace.note(&[from, last]);
        Ok(())
    }

    /// A read on the follower's side: any number of bytes, so a frame
    /// arrives in pieces cut at any place (S4).
    fn deliver_to_follower(&mut self) -> Result<(), Violation> {
        if !self.follower.reading {
            // It reads again, sometimes before the stall limit and
            // sometimes after.
            if self.rng.below(40) == 0 {
                self.follower.reading = true;
            }
            return Ok(());
        }
        let Some(connection) = self.connection.as_mut() else {
            return Ok(());
        };
        let available = connection.to_follower.wire.len();
        if available == 0 {
            return Ok(());
        }
        let take = 1 + self.rng.below(available as u64) as usize;
        let piece: Vec<u8> = connection.to_follower.wire.drain(..take).collect();
        connection.to_follower.read.extend_from_slice(&piece);
        connection.stalled_since = None;
        self.counts.chunks += 1;
        self.trace.note(&[take as u64]);
        self.follower_take()
    }

    /// The follower consumes every whole frame it has read.
    fn follower_take(&mut self) -> Result<(), Violation> {
        loop {
            let Some(connection) = self.connection.as_mut() else {
                return Ok(());
            };
            let decoded = replication::decode(&connection.to_follower.read, Limits::default())
                .map_err(|e| e.to_string());
            let (frame, consumed) = match decoded {
                Ok(Some((Message::Records(frame), consumed))) => {
                    ((frame.previous, frame.last, frame.batch.to_vec()), consumed)
                }
                Ok(Some((Message::Durable(_), _))) => {
                    return Err(self.violation("protocol", "a leader sent a report".into()));
                }
                Ok(None) => return Ok(()),
                Err(error) => return Err(self.violation("protocol", error)),
            };
            connection.to_follower.read.drain(..consumed);
            let (previous, last, batch) = frame;
            let records = Records {
                previous,
                last,
                batch: &batch,
            };
            match self.follower.state.accept(records) {
                Ok(Accepted::Append { .. }) => {
                    if self.follower.wal.stage_framed(&batch).is_err() {
                        // Poisoned by an earlier failed sync.
                        self.follower.recovered_unsynced = true;
                        return self.follower_restart(false);
                    }
                    self.follower.staged = true;
                }
                Ok(Accepted::Acknowledge { durable }) => self.report(durable),
                Err(_) => {
                    // A gap or a straddle ends the connection, as in the
                    // runtime.
                    self.cut("refused frame");
                    return Ok(());
                }
            }
        }
    }

    fn report(&mut self, durable: u64) {
        self.follower.reported = self.follower.reported.max(durable);
        if let Some(connection) = self.connection.as_mut() {
            replication::encode_durable(durable, &mut connection.to_leader.wire);
            self.counts.reports += 1;
        }
    }

    /// The follower's log commits and syncs what was handed to it; only then
    /// is a position reported.
    fn follower_sync(&mut self) -> Result<(), Violation> {
        if !self.follower.staged {
            return Ok(());
        }
        match self.follower.wal.commit_with(true) {
            Ok((done, _, _)) => {
                self.follower.staged = false;
                // A sync covers the whole file, what was unsynced included.
                self.follower.recovered_unsynced = false;
                if let Some(durable) = self.follower.state.on_durable(done.durable_sequence) {
                    self.report(durable);
                }
                Ok(())
            }
            Err(_) => {
                self.counts.sync_failures += 1;
                self.follower.recovered_unsynced = true;
                self.follower_restart(false)
            }
        }
    }

    fn deliver_to_leader(&mut self) -> Result<(), Violation> {
        let local = self.local();
        let required = 2;
        let Some(connection) = self.connection.as_mut() else {
            return Ok(());
        };
        let available = connection.to_leader.wire.len();
        if available == 0 {
            return Ok(());
        }
        let take = 1 + self.rng.below(available as u64) as usize;
        let piece: Vec<u8> = connection.to_leader.wire.drain(..take).collect();
        connection.to_leader.read.extend_from_slice(&piece);
        self.trace.note(&[take as u64]);
        loop {
            let Some(connection) = self.connection.as_mut() else {
                return Ok(());
            };
            let decoded = replication::decode(&connection.to_leader.read, Limits::default())
                .map_err(|e| e.to_string());
            let (sequence, consumed) = match decoded {
                Ok(Some((Message::Durable(sequence), consumed))) => (sequence, consumed),
                Ok(Some((Message::Records(_), _))) => {
                    return Err(self.violation("protocol", "a follower sent records".into()));
                }
                Ok(None) => return Ok(()),
                Err(error) => return Err(self.violation("protocol", error)),
            };
            connection.to_leader.read.drain(..consumed);
            match connection.link.as_mut() {
                None => {
                    // The handshake: the follower's own word about itself.
                    match Link::connect(sequence, local, required) {
                        Ok((mut link, _)) => {
                            let reached = link.begin(local);
                            connection.link = Some(link);
                            self.leader.replicated = sequence;
                            self.trace.note(&[sequence, local]);
                            if let Some(reached) = reached {
                                self.announce(reached)?;
                            }
                        }
                        Err(_) => {
                            return Err(self.violation(
                                "H-ahead",
                                format!("follower at {sequence} is ahead of a leader at {local}"),
                            ));
                        }
                    }
                }
                Some(link) => match link.on_report(sequence, local) {
                    Ok(Some(advance)) => {
                        self.leader.replicated = advance.durable;
                        if let Some(reached) = advance.watermark {
                            self.announce(reached)?;
                        }
                    }
                    Ok(None) => {}
                    Err(error) => {
                        return Err(self.violation("H-watermark", error.to_string()));
                    }
                },
            }
        }
    }

    fn announce(&mut self, reached: u64) -> Result<(), Violation> {
        if reached < self.leader.watermark {
            return Err(self.violation(
                "H-watermark",
                format!("watermark {reached} after {}", self.leader.watermark),
            ));
        }
        self.leader.watermark = reached;
        if self.config.mode == Mode::Quorum {
            self.commit_through(reached)?;
        }
        Ok(())
    }

    fn connect(&mut self) -> Result<(), Violation> {
        if self.follower_poisoned {
            self.follower_poisoned = false;
            self.follower_restart(false)?;
        }
        if self.connection.is_some() || self.clock < self.leader.redial_at || self.follower.refused
        {
            return Ok(());
        }
        self.leader.redial.connected();
        // The follower speaks first, and says what it holds.
        let durable = self.follower.wal.durable_sequence();
        self.follower.state = Follower::resuming_from(durable);
        let mut connection = Connection {
            to_follower: Pipe::default(),
            to_leader: Pipe::default(),
            link: None,
            stalled_since: None,
        };
        replication::encode_durable(durable, &mut connection.to_leader.wire);
        self.connection = Some(connection);
        self.counts.connections += 1;
        self.trace.note(&[durable]);
        Ok(())
    }

    fn cut(&mut self, _why: &str) {
        // What the follower handed to its log is committed when the link
        // ends, as the runtime's writer thread does before it is joined;
        // left staged, it would be staged a second time on the next link.
        if self.follower.staged {
            self.follower.staged = false;
            if self.follower.wal.commit_with(true).is_err() {
                self.counts.sync_failures += 1;
                self.follower_poisoned = true;
                self.follower.recovered_unsynced = true;
            }
        }
        if self.connection.take().is_some() {
            self.counts.cuts += 1;
            self.leader.redial.ended();
            self.leader.redial_at = self.clock + self.leader.redial.wait().as_millis() as u64;
        }
    }

    fn stop_reading(&mut self) {
        self.follower.reading = false;
    }

    /// S9: a follower that stops reading fills the window, and the leader
    /// gives up on a frame that cannot leave within the stall limit (D-27).
    fn check_stall(&mut self) {
        let clock = self.clock;
        let Some(connection) = self.connection.as_mut() else {
            return;
        };
        if connection.to_follower.wire.len() < WINDOW {
            connection.stalled_since = None;
            return;
        }
        let since = *connection.stalled_since.get_or_insert(clock);
        if clock.saturating_sub(since) >= STALL_MS {
            self.counts.stalls += 1;
            self.cut("stalled");
        }
    }

    fn leader_restart(&mut self, power: bool) -> Result<(), Violation> {
        self.cut("leader restart");
        if power {
            let torn = self.rng.below(64) as usize;
            self.leader.disk.power_loss(torn);
            self.counts.leader_power_losses += 1;
            // A rewrite whose directory sync failed may come back as the old
            // file, with the old floor (D-31): only a power loss may do that.
            self.leader.floor_seen = 1;
            self.trace.note(&[1, torn as u64]);
        } else {
            self.counts.leader_crashes += 1;
            self.trace.note(&[2]);
        }
        let (wal, recovered) = open(&self.leader.disk).map_err(|e| self.violation("storage", e))?;
        self.leader.wal = wal;
        // U5: a restart never hands out a committed number again.
        if recovered < self.committed_through {
            return Err(self.violation(
                "U5-lsn",
                format!(
                    "restarted at {recovered} after committing {}",
                    self.committed_through
                ),
            ));
        }
        self.leader.log =
            addressable(&self.leader.disk).map_err(|e| self.violation("storage", e))?;
        self.leader.watermark = 0;
        self.leader.replicated = 0;
        Ok(())
    }

    fn follower_restart(&mut self, power: bool) -> Result<(), Violation> {
        self.cut("follower restart");
        if power {
            let torn = self.rng.below(64) as usize;
            self.follower.disk.power_loss(torn);
            self.counts.follower_power_losses += 1;
            self.trace.note(&[3, torn as u64]);
        } else {
            self.counts.follower_crashes += 1;
            self.trace.note(&[4]);
        }
        let (wal, recovered) =
            open(&self.follower.disk).map_err(|e| self.violation("storage", e))?;
        // H-reported: what it said was durable is still there after a power
        // loss. D-43 is the one known way to break it — a restart after a
        // failed sync takes what the operating system held as durable — and
        // it is named apart, so that no other way of breaking it is mistaken
        // for the known one.
        if power && recovered < self.follower.reported {
            let invariant = if self.follower.recovered_unsynced {
                "H-reported (D-43 signature)"
            } else {
                "H-reported"
            };
            return Err(self.violation(
                invariant,
                format!(
                    "follower reported {} durable and recovered {recovered}",
                    self.follower.reported
                ),
            ));
        }
        self.follower.wal = wal;
        self.follower.staged = false;
        if power {
            // What survived a power loss was synced.
            self.follower.recovered_unsynced = false;
        }
        self.follower.reported = recovered;
        self.follower.state = Follower::resuming_from(recovered);
        Ok(())
    }

    /// A rewrite of the leader's log with the runtime's retention rule for
    /// the mode: under `quorum` everything past the follower's position
    /// (D-20), under `fsynced` a small byte budget (D-18). Records below the
    /// suffix are dropped, as consumed publishes are.
    fn rewrite(&mut self) -> Result<(), Violation> {
        let retain = match self.config.mode {
            Mode::Quorum => Retain {
                from: self.leader.replicated.saturating_add(1),
                bytes: 0,
            },
            Mode::Fsynced => Retain {
                from: u64::MAX,
                bytes: 256 + self.rng.below(1024),
            },
        };
        let before = floor(&self.leader.disk).map_err(|e| self.violation("storage", e))?;
        match self.leader.wal.compact(0, retain, &mut |_| false) {
            Ok(outcome) => {
                self.counts.rewrites += 1;
                self.trace.note(&[outcome.floor]);
                // I10: under `quorum` a rewrite never moves the floor past the
                // follower. A floor that did not move says nothing: after a
                // restart the leader knows nothing of the follower until it
                // reports, and the floor stays where an earlier rewrite left it.
                if self.config.mode == Mode::Quorum
                    && self.leader.replicated != u64::MAX
                    && outcome.floor > before
                    && outcome.floor > self.leader.replicated.saturating_add(1)
                {
                    return Err(self.violation(
                        "H-floor",
                        format!(
                            "floor {} past the follower at {}",
                            outcome.floor, self.leader.replicated
                        ),
                    ));
                }
                let floor = outcome.floor;
                self.leader.log.retain(|sequence, _| *sequence >= floor);
                Ok(())
            }
            Err(_) => self.leader_restart(false),
        }
    }

    /// The operator's procedure for a refused follower: a copy of the
    /// leader's log, then a restart (D-3, D-23).
    fn fresh_copy(&mut self) -> Result<(), Violation> {
        if !self.follower.refused {
            return Ok(());
        }
        self.cut("fresh copy");
        let Some(copy) = self.leader.disk.contents(&path()) else {
            return Ok(());
        };
        let disk = SimDisk::new();
        {
            use std::io::Write;
            use storage::{Access, Disk, DiskFile};
            let mut file = disk
                .open(&path(), Access::Replace)
                .map_err(|e| self.violation("storage", e.to_string()))?;
            file.write_all(&copy)
                .and_then(|()| file.sync_all())
                .map_err(|e| self.violation("storage", e.to_string()))?;
            disk.sync_directory(Path::new("/sim"))
                .map_err(|e| self.violation("storage", e.to_string()))?;
        }
        self.follower.disk = disk;
        self.follower.refused = false;
        self.follower.recovered_unsynced = false;
        self.counts.fresh_copies += 1;
        self.follower_restart(false)
    }

    fn inject_sync_failure(&mut self) {
        let which = self.rng.below(2);
        self.trace.note(&[5, which]);
        if which == 0 {
            self.leader.disk.fail_sync_after(0);
        } else {
            self.follower.disk.fail_sync_after(0);
        }
    }

    /// The oracle's checks that read the logs.
    fn check(&mut self) -> Result<(), Violation> {
        let leader = addressable(&self.leader.disk).map_err(|e| self.violation("storage", e))?;
        let leader_floor = floor(&self.leader.disk).map_err(|e| self.violation("storage", e))?;
        // H-floor: the floor never moves back except through a power loss.
        if leader_floor < self.leader.floor_seen {
            return Err(self.violation(
                "H-floor",
                format!("floor {leader_floor} after {}", self.leader.floor_seen),
            ));
        }
        self.leader.floor_seen = leader_floor;
        // U1: every committed record at or above the floor is where it was
        // committed, unchanged.
        for (sequence, body) in self.committed.range(leader_floor..) {
            if leader.get(sequence) != Some(body) {
                return Err(self.violation(
                    "U1",
                    format!(
                        "committed {sequence} is {:?} on the leader",
                        leader
                            .get(sequence)
                            .map(|b| String::from_utf8_lossy(b).into_owned())
                    ),
                ));
            }
        }
        // U3: where both hold a sequence, they hold the same record.
        let follower =
            addressable(&self.follower.disk).map_err(|e| self.violation("storage", e))?;
        // I4: under `quorum` a committed record is on the follower as well —
        // it counted towards the quorum only because the follower said it
        // had synced it.
        if self.config.mode == Mode::Quorum {
            let follower_floor =
                floor(&self.follower.disk).map_err(|e| self.violation("storage", e))?;
            for (sequence, body) in self.committed.range(follower_floor..) {
                if follower.get(sequence) != Some(body) {
                    return Err(self.violation(
                        "H-quorum-copy",
                        format!("committed {sequence} is not on the follower"),
                    ));
                }
            }
        }
        for (sequence, body) in &follower {
            if let Some(held) = leader.get(sequence) {
                if held != body {
                    return Err(self.violation(
                        "U3",
                        format!(
                            "sequence {sequence}: leader {:?}, follower {:?}",
                            String::from_utf8_lossy(held),
                            String::from_utf8_lossy(body)
                        ),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Runs one seed. `Err` is the first invariant that failed, with the step it
/// failed at; the same seed fails the same way every time.
pub fn run(config: Config) -> Result<Report, Violation> {
    let mut world = World::new(config).map_err(|detail| Violation {
        seed: config.seed,
        step: 0,
        invariant: "storage",
        detail,
    })?;
    for _ in 0..config.steps {
        world.step()?;
    }
    Ok(Report {
        trace: world.trace.0,
        committed: world.committed_through,
        counts: world.counts,
    })
}
