//! Who holds what, as ranges.
//!
//! Nothing here tracks an individual record. The unit is a half-open range of
//! WAL sequences, `(previous, last]`, and every position is a **contiguous
//! prefix**: "everything through N". That is the same rule the local log
//! follows for its own durable boundary (docs/replication.md §111), applied
//! on the other side of the wire, and it is what makes a quorum position
//! meaningful at all.
//!
//! The two halves never share state. A follower decides what it may append
//! and what it may acknowledge; a leader records what it sent to each peer
//! and what that peer confirmed. Neither knows about MQTT, files, sockets,
//! tasks or the broker: they take events and hand back what the runtime
//! should do.
#![deny(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

use crate::frame::Records;

/// What the runtime should do with a frame the follower accepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Accepted {
    /// Append the batch to the log. Nothing is acknowledged yet: the
    /// acknowledgement comes from [`Follower::on_durable`], once the
    /// durability operation covering it has completed. Received is not
    /// durable and written is not durable.
    Append { through: u64 },
    /// The log already holds this range. Send the current position again —
    /// the leader asking a second time means it did not hear the first
    /// answer — and write nothing.
    Acknowledge { durable: u64 },
}

/// Why a follower would not take a frame.
///
/// Both leave the follower's position exactly where it was. Neither is
/// recoverable by skipping the frame: the leader has to continue from
/// `expected`, or the connection ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Refused {
    /// The frame starts past what this node holds. Taking it would leave a
    /// hole, and a prefix with a hole is not a prefix.
    #[error("frame starts at {found}, log holds through {expected}")]
    Gap { expected: u64, found: u64 },
    /// The frame starts below what this node holds and ends above it: the
    /// leader batched these records differently from the way this follower
    /// wrote them. Whole frames only, so it has to re-frame from `expected`.
    #[error("frame {found}..{through} straddles the log's position {expected}")]
    Straddles {
        expected: u64,
        found: u64,
        through: u64,
    },
}

/// One node receiving a stream of frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Follower {
    /// Handed to the log. Not a durability statement.
    written: u64,
    /// Covered by a completed durability operation. The only number that may
    /// ever leave this node as an acknowledgement.
    durable: u64,
}

impl Follower {
    /// Starts from what the local log recovered: everything through
    /// `durable` is on this node's storage, and nothing above it is.
    pub fn resuming_from(durable: u64) -> Self {
        Self {
            written: durable,
            durable,
        }
    }

    /// Everything through here is durable on this node.
    pub fn durable(&self) -> u64 {
        self.durable
    }

    /// Everything through here has been handed to the log. The gap up from
    /// [`Follower::durable`] is what a crash here would lose, which is
    /// exactly why it is never acknowledged.
    pub fn written(&self) -> u64 {
        self.written
    }

    /// Decides what to do with one frame.
    ///
    /// A frame is taken only when it continues the log exactly, or when the
    /// log already holds all of it. Anything else is refused with its reason
    /// and changes nothing.
    ///
    /// On `Append` the position moves before the append happens: from here on
    /// this frame is what the follower expects to have written. An append that
    /// then fails takes the connection, and this state, with it.
    pub fn accept(&mut self, frame: Records<'_>) -> Result<Accepted, Refused> {
        if frame.last <= self.written {
            // Already held, at or below what went to the log. Idempotent by
            // construction: a repeated frame writes nothing twice.
            return Ok(Accepted::Acknowledge {
                durable: self.durable,
            });
        }
        if frame.previous > self.written {
            return Err(Refused::Gap {
                expected: self.written,
                found: frame.previous,
            });
        }
        if frame.previous < self.written {
            return Err(Refused::Straddles {
                expected: self.written,
                found: frame.previous,
                through: frame.last,
            });
        }

        self.written = frame.last;
        Ok(Accepted::Append {
            through: frame.last,
        })
    }

    /// The local log reports everything through `sequence` durable.
    ///
    /// Returns the acknowledgement to send, or `None` when the position did
    /// not move. Clamped to what was actually handed to the log: a log that
    /// reports more than this follower wrote is talking about somebody else's
    /// records.
    pub fn on_durable(&mut self, sequence: u64) -> Option<u64> {
        let reached = sequence.min(self.written);
        if reached <= self.durable {
            return None;
        }
        self.durable = reached;
        Some(reached)
    }
}

/// What a freshly connected follower turned out to need.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciled {
    /// The positions agree. Ordinary replication continues from here.
    InSync,
    /// The follower is behind. Everything from this sequence on goes again
    /// before anything new does.
    Resend { from: u64 },
}

/// The follower holds records this leader never wrote.
///
/// Not automatically corruption, and deliberately not automatically
/// recoverable either. It is what two logs that are not the same history
/// look like — a leader restarted onto an empty log, a follower pointed at
/// the wrong node — and this implementation has no way to tell those apart
/// from a log that legitimately went backwards, because a log carries no
/// identity of its own yet (docs/replication.md §117). Refusing is the
/// answer that cannot lose data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("follower holds through {follower}, this log has written {leader}")]
pub struct Incompatible {
    pub follower: u64,
    pub leader: u64,
}

/// What a peer said that it cannot have meant.
///
/// Both end the connection. Neither can be absorbed: the leader may already
/// have told the broker that a position is quorum-durable, and there is no
/// way to take that back.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerError {
    /// Claimed more than it was ever sent.
    #[error("peer claims {claimed} durable, was sent through {sent}")]
    AheadOfWhatWasSent { claimed: u64, sent: u64 },
    /// Went backwards. Durable does not become undurable, so either this is a
    /// different node or one of the two answers was wrong.
    #[error("peer claims {claimed} durable, had already claimed {held}")]
    Regressed { claimed: u64, held: u64 },
}

/// One peer, as the leader sees it.
///
/// Two monotonic watermarks, not a record of frames. In flight is the range
/// between them.
///
/// There is deliberately no third watermark for "written but not synced on
/// the peer": this protocol has one acknowledgement and it means durable
/// (docs/replication.md §114). A field for it would be a guess with a name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    sent: u64,
    durable: u64,
}

impl Peer {
    /// Starts from the position the peer reported when it connected.
    pub fn starting_at(durable: u64) -> Self {
        Self {
            sent: durable,
            durable,
        }
    }

    /// Settles a freshly connected follower against this leader's own durable
    /// sequence, before any record is sent.
    ///
    /// The follower's own answer is taken as the truth about the follower,
    /// rather than anything the leader remembers about it. That is what makes
    /// the awkward case cheap: a follower that synced a batch and lost the
    /// connection before the acknowledgement left is durable, says so on
    /// reconnect, and is not sent those records a second time.
    pub fn connected(reported: u64, local: u64) -> Result<(Self, Reconciled), Incompatible> {
        if reported > local {
            return Err(Incompatible {
                follower: reported,
                leader: local,
            });
        }
        let peer = Self::starting_at(reported);
        let outcome = if reported == local {
            Reconciled::InSync
        } else {
            Reconciled::Resend {
                from: reported.saturating_add(1),
            }
        };
        Ok((peer, outcome))
    }

    /// Everything through here has gone out to this peer.
    pub fn sent(&self) -> u64 {
        self.sent
    }

    /// Everything through here is durable on this peer, and counts towards a
    /// quorum.
    pub fn durable(&self) -> u64 {
        self.durable
    }

    /// Sequences handed to the network and not yet confirmed.
    pub fn in_flight(&self) -> u64 {
        self.sent.saturating_sub(self.durable)
    }

    /// A frame through `last` went out.
    pub fn on_sent(&mut self, last: u64) {
        self.sent = self.sent.max(last);
    }

    /// The peer reports everything through `sequence` durable.
    ///
    /// `Ok(true)` when the position moved, so the quorum watermark is worth
    /// recomputing; `Ok(false)` for a repeat, which is what a duplicate frame
    /// produces and has to be harmless.
    pub fn on_durable(&mut self, sequence: u64) -> Result<bool, PeerError> {
        if sequence > self.sent {
            return Err(PeerError::AheadOfWhatWasSent {
                claimed: sequence,
                sent: self.sent,
            });
        }
        if sequence < self.durable {
            return Err(PeerError::Regressed {
                claimed: sequence,
                held: self.durable,
            });
        }
        if sequence == self.durable {
            return Ok(false);
        }
        self.durable = sequence;
        Ok(true)
    }
}

/// The highest position a quorum holds durably.
///
/// Not a maximum and not an average: the highest `p` such that `required`
/// replicas — this node counts as one — each hold a contiguous prefix
/// covering `p`. With replicas at 200, 140 and 60 and `required` 2, that is
/// 140, because two of them hold everything through it and nobody holds a
/// gap.
///
/// Never above `local`: the leader cannot truthfully call a position durable
/// anywhere if it does not hold it here, since it never sent it. `required`
/// larger than the replicas that exist yields 0 — a quorum that cannot be
/// formed holds nothing.
pub fn quorum_watermark(local: u64, peers: &[u64], required: usize) -> u64 {
    if required <= 1 {
        return local;
    }
    if required > peers.len().saturating_add(1) {
        return 0;
    }

    // Counting rather than sorting: the candidate set is the cluster, so it is
    // three or five values, and this allocates nothing.
    let mut best = 0;
    for &candidate in std::iter::once(&local).chain(peers) {
        let candidate = candidate.min(local);
        if candidate <= best {
            continue;
        }
        let holding = std::iter::once(&local)
            .chain(peers)
            .filter(|&&held| held >= candidate)
            .count();
        if holding >= required {
            best = candidate;
        }
    }
    best
}
