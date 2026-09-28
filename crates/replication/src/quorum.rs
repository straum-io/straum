//! Turning peer reports into one durability position.
//!
//! Storage declares local durability. This observes it, and what peers say
//! about themselves, and is the only thing allowed to turn the two into a
//! quorum position (docs/replication.md §126).
//!
//! It is deliberately not in `report()` and not in storage. A report means
//! one thing — "this peer has written and synced everything through X" —
//! and whether X is enough for a quorum is a different question with a
//! different owner. Letting the peer protocol answer it would make the
//! wire format decide the runtime's durability semantics.
//!
//! Nothing here polls. The watermark is recomputed when the local position
//! moves or a peer reports, so there is no timer between a commit becoming
//! durable and an acknowledgement going out.
#![deny(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

/// What is known about one peer.
///
/// The two fields are independent on purpose. A peer that is not connected
/// has not lost what it already wrote: its disk still holds everything
/// through `durable`, and that copy still counts. What it cannot do is
/// acknowledge anything new, so it stops counting towards whether a quorum
/// is *reachable*.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Position {
    durable: u64,
    connected: bool,
}

/// The one owner of the quorum watermark.
#[derive(Debug, Clone)]
pub struct Quorum {
    /// What this node's own log has made durable. Counts as one copy.
    local: u64,
    /// Static membership: one entry per peer, allocated once.
    peers: Vec<Position>,
    /// Copies required, this node included.
    required: usize,
    /// The highest watermark ever handed out. Monotonic, and that is not a
    /// convenience: an acknowledgement cannot be taken back, so a position
    /// once declared durable stays declared even when the replica that made
    /// it durable goes away.
    reported: u64,
}

impl Quorum {
    /// `peers` is how many other nodes there are; `required` counts copies
    /// including this one. `required` of 1 or 0 means this node alone is a
    /// quorum, which is what a single-node broker is.
    pub fn new(peers: usize, required: usize) -> Self {
        Self {
            local: 0,
            peers: vec![Position::default(); peers],
            required,
            reported: 0,
        }
    }

    /// The local log made everything through `sequence` durable.
    ///
    /// Returns the new quorum watermark when it advanced, and nothing when
    /// it did not — so the caller emits one event per real advance rather
    /// than one per commit.
    pub fn on_local(&mut self, sequence: u64) -> Option<u64> {
        self.local = self.local.max(sequence);
        self.settle()
    }

    /// A peer reported everything through `sequence` durable on its side.
    ///
    /// Out of range or going backwards is ignored rather than trusted: a
    /// peer's own state machine already refuses a regression, and this is
    /// the coordinator, not a second place to decide protocol violations.
    pub fn on_peer(&mut self, peer: usize, sequence: u64) -> Option<u64> {
        let held = self.peers.get_mut(peer)?;
        held.durable = held.durable.max(sequence);
        held.connected = true;
        self.settle()
    }

    /// The link to a peer went away. What it had already written is still
    /// written, so its position stays; what it can no longer do is take
    /// part in reaching a quorum.
    pub fn on_disconnected(&mut self, peer: usize) {
        if let Some(held) = self.peers.get_mut(peer) {
            held.connected = false;
        }
    }

    /// Everything through here is durable on enough copies.
    pub fn watermark(&self) -> u64 {
        self.reported
    }

    /// Whether a quorum can still be reached for something published now.
    ///
    /// Distinct from [`Quorum::watermark`], which is about what already
    /// happened, and distinct from a poisoned log, which is about this
    /// node's storage being unusable. A partition, a peer restarting or a
    /// follower that is merely slow all make this false and none of them is
    /// terminal (docs/replication.md §126).
    pub fn reachable(&self) -> bool {
        if self.required <= 1 {
            return true;
        }
        let live = self
            .peers
            .iter()
            .filter(|held| held.connected)
            .count()
            .saturating_add(1);
        live >= self.required
    }

    /// Recomputes the watermark and reports it if it moved.
    ///
    /// Recomputed on each event rather than maintained incrementally: the
    /// membership is a handful of entries and this runs on a commit or a
    /// report, never per message. An order-statistics structure would be a
    /// change to make when a benchmark asks for it (docs/algorithms.md).
    fn settle(&mut self) -> Option<u64> {
        let now = self.compute();
        if now <= self.reported {
            return None;
        }
        self.reported = now;
        Some(now)
    }

    /// The `required`-th largest position, counting this node.
    ///
    /// Never above `local`: a leader cannot call a position durable
    /// anywhere when it does not hold it itself, because it never sent it.
    /// A disconnected peer's last known position still counts — the bytes
    /// are on its disk whatever the link is doing.
    fn compute(&self) -> u64 {
        if self.required <= 1 {
            return self.local;
        }
        if self.required > self.peers.len().saturating_add(1) {
            // More copies required than exist. Nothing is ever quorum
            // durable, which is a configuration to refuse at startup rather
            // than a number to invent here.
            return 0;
        }

        // Counting rather than sorting: the candidate set is the cluster,
        // so it is a handful of values and this allocates nothing.
        let mut best = 0;
        for candidate in std::iter::once(self.local).chain(self.peers.iter().map(|at| at.durable)) {
            let candidate = candidate.min(self.local);
            if candidate <= best {
                continue;
            }
            let holding = std::iter::once(self.local)
                .chain(self.peers.iter().map(|at| at.durable))
                .filter(|held| *held >= candidate)
                .count();
            if holding >= self.required {
                best = candidate;
            }
        }
        best
    }
}
