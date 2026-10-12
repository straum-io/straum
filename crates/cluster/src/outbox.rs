//! The proposals this node made and has not yet seen committed (D-109 B,
//! D-117 B): kept in `cluster` so the simulation drives the code the node
//! runs.
//!
//! A proposal leaves only when its record is applied from the committed log
//! — the confirmation — or when it is shown lost. It is sent again only
//! once the slot it went to is closed: a record of a later slot committed,
//! so the committed prefix can never gain a record of that slot (fencing).
//! A proposal therefore reaches the committed log at most once from here,
//! whatever the sequencer did with it — given that a sequencer takes a
//! forward only into the slot it was sent for (`Message::Forward`), and
//! that one link delivers in order, losing at most whole forwards.
//! Sends go strictly in order, and none while an earlier entry waits for its slot to be decided: one session's
//! proposals reach the log in the order they were made (CLAUDE.md §7).

use std::collections::VecDeque;

use journal::Record;

use crate::Proposal;

/// Each node's share of the sequencer's queue (D-120 A): after a slot
/// change every node sends its whole outbox at once, and all of them
/// together must fit the queue of `max_queue`, or the overflow is lost.
/// Rounded down, and never 0: a node with no room could propose nothing.
pub(crate) fn capacity(max_queue: usize, voters: usize) -> usize {
    (max_queue / voters.max(1)).max(1)
}

#[derive(Debug)]
pub(crate) struct Outbox {
    entries: VecDeque<Proposal>,
    /// The first `sent` entries went, in order, to the sequencer of `slot`.
    sent: usize,
    slot: u64,
    /// The sequencer's own queue bound (`cluster::Config::max_queue`): a
    /// node holds no more for a sequencer than a sequencer would.
    max: usize,
}

impl Outbox {
    pub(crate) fn new(max: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            sent: 0,
            slot: 0,
            max,
        }
    }

    /// Whether a proposal would be refused now: the driver stops reading
    /// its clients while it is (D-120 A, backpressure).
    pub(crate) fn full(&self) -> bool {
        self.entries.len() >= self.max
    }

    /// Takes a proposal; refused when full, as a full sequencer queue
    /// refuses: the gateway's `commit_timeout` closes its publisher.
    pub(crate) fn push(&mut self, proposal: Proposal) -> bool {
        if self.entries.len() >= self.max {
            return false;
        }
        self.entries.push_back(proposal);
        true
    }

    /// The next entry to send to the sequencer of `slot`; none while
    /// earlier entries wait for an older slot to be decided.
    pub(crate) fn next(&self, slot: u64) -> Option<&Proposal> {
        if self.sent > 0 && self.slot != slot {
            return None;
        }
        self.entries.get(self.sent)
    }

    /// The entry `next` returned went to the sequencer of `slot`.
    pub(crate) fn sent(&mut self, slot: u64) {
        self.slot = slot;
        self.sent = self.sent.saturating_add(1).min(self.entries.len());
    }

    /// A committed record, in log order. Returns the entries it shows lost,
    /// in order: sent before it to the same slot and never committed. A
    /// resend would put them behind it, so they are given up — handed back,
    /// never dropped unseen (D-115 A): the caller accounts for each.
    pub(crate) fn committed(&mut self, record: &Record<'_>) -> Vec<Proposal> {
        if self.sent == 0 || record.slot < self.slot {
            return Vec::new();
        }
        if record.slot > self.slot {
            // The slot closed without them: to the next sequencer, in order.
            self.sent = 0;
            return Vec::new();
        }
        // ponytail: a scan of the sent prefix per committed record; it is
        // the head almost always for this node's own records, and the
        // prefix is bounded by `max`.
        let Some(at) = self
            .entries
            .iter()
            .take(self.sent)
            .position(|p| same(p, record))
        else {
            return Vec::new();
        };
        let lost: Vec<Proposal> = self.entries.drain(..at).collect();
        self.entries.pop_front();
        self.sent -= at + 1;
        lost
    }

    /// Entries held, and of them sent and awaiting their slot's decision.
    pub(crate) fn held(&self) -> (usize, usize) {
        (self.entries.len(), self.sent)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }
}

/// The record this proposal became. Every gateway proposal names its node
/// and session, and a publish or a filter its request, so another node's
/// record never equals one of these.
fn same(proposal: &Proposal, record: &Record<'_>) -> bool {
    proposal.kind == record.kind
        && proposal.body == record.body
        && proposal.properties == record.properties.bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use journal::{Kind, Properties};

    fn proposal(n: u8) -> Proposal {
        Proposal {
            kind: Kind::Publish,
            properties: Vec::new(),
            body: vec![n],
        }
    }

    fn record(slot: u64, body: &[u8]) -> Record<'_> {
        Record {
            kind: Kind::Publish,
            slot,
            lsn: 0,
            prev_hash: 0,
            properties: Properties::new(&[]),
            body,
        }
    }

    #[test]
    fn an_entry_leaves_only_when_its_record_commits() {
        let mut outbox = Outbox::new(8);
        assert!(outbox.push(proposal(1)));
        assert_eq!(outbox.next(3).map(|p| p.body.clone()), Some(vec![1]));
        outbox.sent(3);
        assert!(outbox.next(3).is_none());
        // Another node's record, and an older slot's: nothing changes.
        assert!(outbox.committed(&record(3, &[9])).is_empty());
        assert!(outbox.committed(&record(2, &[1])).is_empty());
        assert_eq!(outbox.len(), 1);
        assert!(outbox.committed(&record(3, &[1])).is_empty());
        assert_eq!(outbox.len(), 0);
    }

    #[test]
    fn nothing_is_sent_again_until_its_slot_is_closed_and_then_in_order() {
        let mut outbox = Outbox::new(8);
        for n in 1..=3 {
            outbox.push(proposal(n));
        }
        outbox.next(3).unwrap();
        outbox.sent(3);
        outbox.next(3).unwrap();
        outbox.sent(3);
        // A later slot is known but not decided: neither the waiting
        // entries nor the one behind them go to it.
        assert!(outbox.next(4).is_none());
        // Entry 1 committed in slot 3, then slot 4 began: entry 2 was lost
        // with slot 3 and goes again, before entry 3.
        outbox.committed(&record(3, &[1]));
        assert!(outbox.next(4).is_none());
        outbox.committed(&record(4, &[7]));
        assert_eq!(outbox.next(4).map(|p| p.body.clone()), Some(vec![2]));
        outbox.sent(4);
        assert_eq!(outbox.next(4).map(|p| p.body.clone()), Some(vec![3]));
    }

    #[test]
    fn a_later_entry_committing_shows_the_earlier_ones_lost() {
        let mut outbox = Outbox::new(8);
        for n in 1..=3 {
            outbox.push(proposal(n));
            outbox.next(3).unwrap();
            outbox.sent(3);
        }
        // 1 never reached the log; 2 did. Sending 1 again would put it
        // behind 2: it is given up, never reordered.
        assert_eq!(outbox.committed(&record(3, &[2])), vec![proposal(1)]);
        assert_eq!(outbox.len(), 1);
        assert!(outbox.committed(&record(3, &[3])).is_empty());
        assert_eq!(outbox.len(), 0);
    }

    #[test]
    fn each_node_holds_its_share_of_the_queue_and_never_none() {
        assert_eq!(capacity(1024, 3), 341);
        assert_eq!(capacity(32, 3), 10);
        assert_eq!(capacity(2, 3), 1);
        assert_eq!(capacity(0, 3), 1);
        assert_eq!(capacity(5, 0), 5);
    }

    #[test]
    fn it_holds_no_more_than_its_bound() {
        let mut outbox = Outbox::new(2);
        assert!(outbox.push(proposal(1)));
        assert!(outbox.push(proposal(2)));
        assert!(outbox.full());
        assert!(!outbox.push(proposal(3)));
    }
}
