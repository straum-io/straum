//! The leader's side of one replication link, as decisions (D-28, stage 2).
//!
//! The runtime moves bytes and sleeps; everything it has to decide on the way
//! is decided here, without a socket, a task or a clock:
//!
//! * where a freshly connected follower stands, and what it is missing;
//! * how far a catch-up may read — never past what this node has made
//!   durable, because a record shipped before it is durable here lets the
//!   follower report a position the leader does not hold yet (D-22);
//! * whether a live batch is the next one to send or a leftover;
//! * what a report from the follower moves, the quorum watermark included;
//! * how long to wait before dialling again, and whether a refusal is news.
//!
//! Time appears only as values handed back for the caller to wait. A
//! simulation drives the same code and decides what waiting means.
#![deny(clippy::arithmetic_side_effects, clippy::indexing_slicing)]

use std::time::Duration;

use crate::quorum::Quorum;
use crate::state::{Incompatible, Peer, PeerError, Reconciled};

/// One connected follower, as the leader sees it.
#[derive(Debug, Clone)]
pub struct Link {
    peer: Peer,
    /// One follower, static membership. It lives for the connection; the
    /// watermark it hands out does not, because the runtime keeps the
    /// highest one ever announced and an announced position is never
    /// withdrawn.
    quorum: Quorum,
}

/// What to do with a batch from the live queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Live {
    /// It continues exactly what the follower was sent: send it.
    Send,
    /// Already sent, or it starts past a gap the queue left. The gap is
    /// closed from the log by [`Link::behind`], not by this batch.
    Skip,
}

/// What a report from the follower moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Advance {
    /// The follower's new durable position.
    pub durable: u64,
    /// The quorum watermark, if it moved.
    pub watermark: Option<u64>,
}

impl Link {
    /// The follower spoke first and reported `reported`; this node holds
    /// `local`. The follower's own answer is the truth about the follower
    /// (docs/replication.md §117). `required` counts copies, this node
    /// included.
    pub fn connect(
        reported: u64,
        local: u64,
        required: usize,
    ) -> Result<(Self, Reconciled), Incompatible> {
        let (peer, outcome) = Peer::connected(reported, local)?;
        Ok((
            Self {
                peer,
                quorum: Quorum::new(1, required),
            },
            outcome,
        ))
    }

    /// The follower is caught up to what the handshake asked for and the
    /// stream begins: the coordinator learns both positions. Returns the
    /// watermark if that moved it.
    pub fn begin(&mut self, local: u64) -> Option<u64> {
        let peer = self.quorum.on_peer(0, self.peer.durable());
        let local = self.quorum.on_local(local);
        local.or(peer)
    }

    /// The range the follower is missing and the log has to supply, as
    /// `(from, until)`, both inclusive — or nothing.
    ///
    /// `until` is `local`, this node's own durable sequence: the file holds
    /// more than that from the moment a batch is written until it is synced,
    /// and those records are not this node's to ship yet (D-22). `local` is a
    /// commit boundary, so the range never splits a batch.
    pub fn behind(&self, local: u64) -> Option<(u64, u64)> {
        let sent = self.peer.sent();
        (sent < local).then(|| (sent.saturating_add(1), local))
    }

    /// Whether a follower that needs `from` can be served from a log whose
    /// retention floor is `floor`.
    ///
    /// Below the floor the log has bytes and no names: a rewrite kept them as
    /// a filtered set, and nothing maps them back to their sequences. There
    /// is no answer to give such a follower, only a refusal; getting it back
    /// needs a fresh copy of the log, which is an operator's decision
    /// (docs/replication.md §130).
    pub fn serves(from: u64, floor: u64) -> bool {
        from >= floor
    }

    /// Whether a live batch covering `(previous, last]` is the next one.
    pub fn live(&self, previous: u64, last: u64) -> Live {
        let sent = self.peer.sent();
        if last <= sent || previous != sent {
            Live::Skip
        } else {
            Live::Send
        }
    }

    /// A frame through `last` went out. Sent is not durable.
    pub fn on_sent(&mut self, last: u64) {
        self.peer.on_sent(last);
    }

    /// The follower reports everything through `sequence` durable, while this
    /// node holds `local`.
    ///
    /// The only thing that advances remote durability: a write that succeeded
    /// and a frame that was delivered are not this. `None` for a repeat; an
    /// error for a report that cannot be true, which ends the link.
    pub fn on_report(&mut self, sequence: u64, local: u64) -> Result<Option<Advance>, PeerError> {
        if !self.peer.on_durable(sequence)? {
            return Ok(None);
        }
        // The peer said where its own durable prefix ends and nothing more.
        // Whether that is enough for a quorum is the coordinator's question,
        // and its answer alone.
        let local = self.quorum.on_local(local);
        let peer = self.quorum.on_peer(0, sequence);
        Ok(Some(Advance {
            durable: sequence,
            watermark: peer.or(local),
        }))
    }

    pub fn sent(&self) -> u64 {
        self.peer.sent()
    }

    pub fn durable(&self) -> u64 {
        self.peer.durable()
    }

    pub fn in_flight(&self) -> u64 {
        self.peer.in_flight()
    }
}

/// When to dial the follower again.
///
/// Bounded and growing: a loop that redials a refusing peer at once is the
/// same mistake as an accept loop that spins on `EMFILE`. A refusal has its
/// own backoff, because the connection that carried it succeeded and resets
/// the ordinary one (D-23).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Redial {
    backoff: Duration,
    refusal_backoff: Duration,
    /// The position last refused, so one refusal is reported once.
    refused_from: Option<u64>,
}

impl Default for Redial {
    fn default() -> Self {
        Self::new()
    }
}

impl Redial {
    pub const FIRST: Duration = Duration::from_millis(50);
    pub const LONGEST: Duration = Duration::from_secs(5);

    pub fn new() -> Self {
        Self {
            backoff: Self::FIRST,
            refusal_backoff: Self::FIRST,
            refused_from: None,
        }
    }

    /// A connection was established.
    pub fn connected(&mut self) {
        self.backoff = Self::FIRST;
    }

    /// The follower was refused for needing `from`, below what the log can
    /// serve. Returns whether this is news: `false` when the same position
    /// was refused last time too.
    pub fn refused(&mut self, from: u64) -> bool {
        self.backoff = self.refusal_backoff;
        self.refusal_backoff = double(self.refusal_backoff);
        let news = self.refused_from != Some(from);
        self.refused_from = Some(from);
        news
    }

    /// The link ended for any other reason, or cleanly.
    pub fn ended(&mut self) {
        self.refused_from = None;
        self.refusal_backoff = Self::FIRST;
    }

    /// How long to wait before the next attempt. Each call doubles the next
    /// answer, up to [`Redial::LONGEST`].
    pub fn wait(&mut self) -> Duration {
        let now = self.backoff;
        self.backoff = double(self.backoff);
        now
    }
}

fn double(wait: Duration) -> Duration {
    wait.saturating_mul(2).min(Redial::LONGEST)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_follower_behind_is_missing_up_to_local_and_no_further() {
        let (link, outcome) = Link::connect(3, 10, 2).unwrap();
        assert_eq!(outcome, Reconciled::Resend { from: 4 });
        assert_eq!(link.behind(10), Some((4, 10)));
        // A batch written and not yet durable here is not the follower's.
        assert_eq!(link.behind(10), link.behind(10));
    }

    #[test]
    fn a_level_follower_is_missing_nothing() {
        let (link, outcome) = Link::connect(10, 10, 2).unwrap();
        assert_eq!(outcome, Reconciled::InSync);
        assert_eq!(link.behind(10), None);
    }

    #[test]
    fn a_follower_ahead_is_refused() {
        assert!(Link::connect(11, 10, 2).is_err());
    }

    #[test]
    fn a_live_batch_is_sent_only_when_it_continues_what_was_sent() {
        let (mut link, _) = Link::connect(5, 5, 2).unwrap();
        assert_eq!(link.live(5, 8), Live::Send);
        link.on_sent(8);
        // Left over from before a catch-up.
        assert_eq!(link.live(5, 8), Live::Skip);
        // After a gap the queue dropped.
        assert_eq!(link.live(9, 12), Live::Skip);
        assert_eq!(link.behind(12), Some((9, 12)));
    }

    #[test]
    fn a_report_moves_the_watermark_to_the_smaller_position() {
        let (mut link, _) = Link::connect(0, 0, 2).unwrap();
        assert_eq!(link.begin(0), None);
        link.on_sent(10);
        let advance = link.on_report(10, 10).unwrap().unwrap();
        assert_eq!(advance.durable, 10);
        assert_eq!(advance.watermark, Some(10));
        // A repeat moves nothing.
        assert_eq!(link.on_report(10, 10).unwrap(), None);
    }

    #[test]
    fn a_report_past_what_was_sent_ends_the_link() {
        let (mut link, _) = Link::connect(0, 10, 2).unwrap();
        link.on_sent(5);
        assert!(link.on_report(6, 10).is_err());
    }

    #[test]
    fn a_refusal_is_news_once_per_position_and_backs_off() {
        let mut redial = Redial::new();
        redial.connected();
        assert!(redial.refused(1));
        assert_eq!(redial.wait(), Duration::from_millis(50));
        redial.connected();
        assert!(!redial.refused(1), "the same refusal reported twice");
        assert_eq!(redial.wait(), Duration::from_millis(100));
        redial.connected();
        assert!(redial.refused(4), "a new position is news");
        assert_eq!(redial.wait(), Duration::from_millis(200));
        redial.ended();
        assert!(
            redial.refused(4),
            "after a link that ended, it is news again"
        );
    }

    #[test]
    fn waiting_doubles_to_a_ceiling() {
        let mut redial = Redial::new();
        let waits: Vec<Duration> = (0..10).map(|_| redial.wait()).collect();
        assert_eq!(waits.first(), Some(&Duration::from_millis(50)));
        assert_eq!(waits.last(), Some(&Redial::LONGEST));
        assert!(waits.windows(2).all(|pair| pair.first() <= pair.get(1)));
    }
}
