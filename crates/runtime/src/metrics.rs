//! Plain atomics, `Relaxed`: they order nothing but themselves (CLAUDE.md §14).

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    pub connections_accepted: AtomicU64,
    pub protocol_errors: AtomicU64,
    pub transport_errors: AtomicU64,
    pub bytes_in: AtomicU64,
    pub bytes_out: AtomicU64,
    pub keepalive_closes: AtomicU64,
    pub authentication_failures: AtomicU64,
    pub active_sessions: AtomicU64,
    pub queued_messages: AtomicUsize,
    pub queued_bytes: AtomicUsize,
    pub wal_commits: AtomicU64,
    pub wal_records_committed: AtomicU64,
    pub wal_bytes_committed: AtomicU64,
    pub wal_commit_failures: AtomicU64,
    /// Times the log was rewritten beside the writer.
    pub wal_compactions: AtomicU64,
    /// Session records the log queue could not take. Each one is a session
    /// that will not come back after a restart.
    pub session_records_dropped: AtomicU64,
    /// At-least-once messages a session backlog had no room for.
    pub backlog_refused: AtomicU64,
    /// Payload bytes the broker is holding for sessions that are behind.
    pub backlog_bytes: AtomicU64,
    /// Subscribers the broker gave up on. Each one is a client that saw a
    /// closed socket and no reason for it.
    pub slow_consumer_closes: AtomicU64,
    /// Latest sync, not an average: the average of a bimodal latency is
    /// a number that never happened.
    pub wal_sync_nanos: AtomicU64,
    pub wal_durable_sequence: AtomicU64,
    /// Replication links established, either direction.
    pub replication_connections: AtomicU64,
    /// Links that ended in an error rather than a shutdown.
    pub replication_errors: AtomicU64,
    /// 1 while a replication link is up. A gauge: an operator watching a
    /// follower wants to know it is there, not how often it has been.
    pub replication_connected: AtomicU64,
    /// How far the follower is durable. On the follower itself, how far it
    /// has told the leader it is.
    pub replication_follower_sequence: AtomicU64,
    /// Committed batches the live queue had no room for. Not lost — the
    /// catch-up path reads them back out of the log — but a steady count
    /// means the follower is not keeping up (docs/replication.md §119).
    pub replication_shipments_dropped: AtomicU64,
    /// Times the leader had to read its own log to fill a gap.
    pub replication_catchups: AtomicU64,
    /// Sequences sent to the follower and not yet confirmed. A gauge, stored
    /// once per report and once per frame — not on any per-record path.
    pub replication_in_flight: AtomicU64,
    /// Committed batches waiting in the live queue. Bounded by its capacity
    /// by construction; this is what lets an operator see it sitting at the
    /// bound rather than infer it (docs/replication.md §123).
    pub replication_queued: AtomicU64,
}

impl Metrics {
    pub fn increment(counter: &AtomicU64, by: u64) {
        counter.fetch_add(by, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            connections_accepted: self.connections_accepted.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            transport_errors: self.transport_errors.load(Ordering::Relaxed),
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            keepalive_closes: self.keepalive_closes.load(Ordering::Relaxed),
            authentication_failures: self.authentication_failures.load(Ordering::Relaxed),
            active_sessions: self.active_sessions.load(Ordering::Relaxed),
            queued_messages: self.queued_messages.load(Ordering::Relaxed),
            queued_bytes: self.queued_bytes.load(Ordering::Relaxed),
            wal_commits: self.wal_commits.load(Ordering::Relaxed),
            wal_records_committed: self.wal_records_committed.load(Ordering::Relaxed),
            wal_bytes_committed: self.wal_bytes_committed.load(Ordering::Relaxed),
            wal_commit_failures: self.wal_commit_failures.load(Ordering::Relaxed),
            wal_compactions: self.wal_compactions.load(Ordering::Relaxed),
            session_records_dropped: self.session_records_dropped.load(Ordering::Relaxed),
            backlog_refused: self.backlog_refused.load(Ordering::Relaxed),
            backlog_bytes: self.backlog_bytes.load(Ordering::Relaxed),
            slow_consumer_closes: self.slow_consumer_closes.load(Ordering::Relaxed),
            wal_sync_nanos: self.wal_sync_nanos.load(Ordering::Relaxed),
            wal_durable_sequence: self.wal_durable_sequence.load(Ordering::Relaxed),
            replication_connections: self.replication_connections.load(Ordering::Relaxed),
            replication_errors: self.replication_errors.load(Ordering::Relaxed),
            replication_connected: self.replication_connected.load(Ordering::Relaxed),
            replication_follower_sequence: self
                .replication_follower_sequence
                .load(Ordering::Relaxed),
            replication_shipments_dropped: self
                .replication_shipments_dropped
                .load(Ordering::Relaxed),
            replication_catchups: self.replication_catchups.load(Ordering::Relaxed),
            replication_in_flight: self.replication_in_flight.load(Ordering::Relaxed),
            replication_queued: self.replication_queued.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Snapshot {
    pub connections_accepted: u64,
    pub protocol_errors: u64,
    pub transport_errors: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub keepalive_closes: u64,
    pub authentication_failures: u64,
    pub active_sessions: u64,
    pub queued_messages: usize,
    pub queued_bytes: usize,
    pub wal_commits: u64,
    pub wal_records_committed: u64,
    pub wal_bytes_committed: u64,
    pub wal_commit_failures: u64,
    pub wal_compactions: u64,
    pub session_records_dropped: u64,
    pub backlog_refused: u64,
    pub backlog_bytes: u64,
    pub slow_consumer_closes: u64,
    pub wal_sync_nanos: u64,
    pub wal_durable_sequence: u64,
    pub replication_connections: u64,
    pub replication_errors: u64,
    /// 1 while a replication link is up.
    pub replication_connected: u64,
    /// How far the follower is durable, as the leader last heard it.
    pub replication_follower_sequence: u64,
    pub replication_shipments_dropped: u64,
    pub replication_catchups: u64,
    /// Sequences sent and not yet confirmed.
    pub replication_in_flight: u64,
    /// Committed batches waiting in the live queue.
    pub replication_queued: u64,
}
