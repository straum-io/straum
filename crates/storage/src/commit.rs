//! Adaptive group commit.
//!
//! The completion of the previous durability operation is the batching
//! boundary (docs/storage.md §73). Nothing here waits for a timer or for a
//! record count: a batch is "everything that arrived while the disk was
//! busy", which is one record when the broker is idle and thousands when it
//! is not. The batching factor is `incoming rate × storage latency`, and
//! neither term is ours to choose.

use std::collections::BTreeSet;
use std::time::Duration;

/// Where the writer takes records from.
///
/// A trait because there are two real sources with no common type: a
/// `std` channel in the tests, and the runtime's async channel drained from
/// the WAL thread. Both are in this repository; neither is speculative.
pub trait RecordSource {
    /// Blocks until a record arrives, or the source is closed.
    fn recv(&mut self) -> Option<WalRecord>;
    /// Takes what is already there, if anything.
    fn try_recv(&mut self) -> Option<WalRecord>;
}

impl RecordSource for std::sync::mpsc::Receiver<WalRecord> {
    fn recv(&mut self) -> Option<WalRecord> {
        std::sync::mpsc::Receiver::recv(self).ok()
    }

    fn try_recv(&mut self) -> Option<WalRecord> {
        std::sync::mpsc::Receiver::try_recv(self).ok()
    }
}

use bytes::Bytes;

use crate::wal::Wal;
use crate::{StorageError, WalLimits};

/// Commits between recomputations of the batch bound. Adaptation is stable
/// and bounded on purpose: reacting to every single sync oscillates (§78).
const ADAPT_EVERY: u64 = 32;

/// Weight of the history in the latency estimates: new = (7·old + sample)/8.
const EWMA_SHIFT: u32 = 3;

/// One record on its way to the log.
#[derive(Debug, Clone)]
pub struct WalRecord {
    /// Chosen by the submitter and echoed back once durable. The log does not
    /// interpret it; it is how the broker finds out which publishes it may
    /// acknowledge.
    pub correlation: u64,
    pub payload: Bytes,
}

/// One completed commit cycle: one write, one durability operation, many
/// acknowledgements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
    pub durable_sequence: u64,
    pub durable_offset: u64,
    /// The contiguous durable prefix: every correlation up to and including
    /// this one has been committed.
    ///
    /// Deliberately not the highest correlation seen. A submitter takes its
    /// correlation and then queues the record, so a higher correlation can
    /// reach the log first; a maximum would then report a record that is
    /// still sitting in the channel as durable (docs/replication.md §110).
    /// Monotonic, and never ahead of what `sync_data` covered.
    pub durable_correlation: u64,
    pub records: usize,
    pub bytes: usize,
    pub write: Duration,
    pub sync: Duration,
}

/// Drives one WAL through commit cycles.
pub struct Committer {
    /// Whether a commit waits for the storage device.
    sync: bool,
    wal: Wal,
    limits: WalLimits,
    /// Upper bound for the batch currently being built. Not a target to wait
    /// for: a batch is closed as soon as nothing more is available.
    batch_bound: usize,
    bytes_ewma: u64,
    write_nanos_ewma: u64,
    sync_nanos_ewma: u64,
    commits: u64,
    /// The contiguous durable prefix (see [`Commit::durable_correlation`]).
    durable_correlation: u64,
    /// Committed correlations that sit above a gap, waiting for it to close.
    /// Empty whenever submission order matched correlation order, which is
    /// the ordinary case: it does not allocate until one record overtakes
    /// another.
    ahead: BTreeSet<u64>,
}

impl Committer {
    /// `sync` false commits without asking the storage device, which is
    /// what `Durability::Written` means (docs/storage.md §80). The
    /// adaptation still runs: without a sync to measure, the write latency
    /// is the signal it paces against.
    ///
    /// `durable_correlation` is what the recovered log already covers: the
    /// next record submitted is expected to carry the correlation after it.
    /// A value that is too low stalls acknowledgement until the gap closes,
    /// which is the safe direction to be wrong in.
    pub fn new(wal: Wal, limits: WalLimits, sync: bool, durable_correlation: u64) -> Self {
        Self {
            sync,
            wal,
            batch_bound: limits.initial_batch_bytes,
            limits,
            bytes_ewma: 0,
            write_nanos_ewma: 0,
            sync_nanos_ewma: 0,
            commits: 0,
            durable_correlation,
            ahead: BTreeSet::new(),
        }
    }

    /// Folds one committed correlation into the contiguous prefix.
    ///
    /// Correlations form a dense sequence — one is taken only once its record
    /// is certain to be queued — so a gap means "not committed yet", never
    /// "will never arrive" (docs/replication.md §110).
    fn cover(&mut self, correlation: u64) {
        if correlation != self.durable_correlation.saturating_add(1) {
            if correlation > self.durable_correlation {
                self.ahead.insert(correlation);
            }
            return;
        }
        self.durable_correlation = correlation;
        // This record may have been the gap a whole run was waiting behind.
        while let Some(next) = self.ahead.first().copied() {
            if next != self.durable_correlation.saturating_add(1) {
                break;
            }
            self.ahead.pop_first();
            self.durable_correlation = next;
        }
    }

    /// Runs until the channel is closed and drained.
    pub fn run(
        &mut self,
        records: &mut impl RecordSource,
        mut on_commit: impl FnMut(&Commit),
    ) -> Result<(), StorageError> {
        while let Some(commit) = self.commit_available(records)? {
            on_commit(&commit);
        }
        Ok(())
    }

    /// One cycle: block for the first record, take whatever else is already
    /// there, write it, make it durable.
    ///
    /// `Ok(None)` means the channel is closed and empty.
    pub fn commit_available(
        &mut self,
        records: &mut impl RecordSource,
    ) -> Result<Option<Commit>, StorageError> {
        let Some(first) = records.recv() else {
            return Ok(None);
        };

        // Staged first, covered second: a record the log refused is not part
        // of any prefix.
        self.wal.stage(&first.payload)?;
        self.cover(first.correlation);

        // Everything currently available, not everything that might arrive:
        // an idle broker commits a batch of one without waiting for anybody.
        while self.wal.staged_bytes() < self.batch_bound {
            let Some(record) = records.try_recv() else {
                break;
            };
            self.wal.stage(&record.payload)?;
            self.cover(record.correlation);
        }

        let (committed, write, sync) = self.wal.commit_with(self.sync)?;
        self.observe(committed.bytes, write, sync);

        Ok(Some(Commit {
            durable_sequence: committed.durable_sequence,
            durable_offset: committed.durable_offset,
            durable_correlation: self.durable_correlation,
            records: committed.records,
            bytes: committed.bytes,
            write,
            sync,
        }))
    }

    /// The bound a batch is currently allowed to reach.
    pub fn batch_bound(&self) -> usize {
        self.batch_bound
    }

    pub fn durable_sequence(&self) -> u64 {
        self.wal.durable_sequence()
    }

    /// Bytes the log occupies right now.
    pub fn log_bytes(&self) -> u64 {
        self.wal.durable_offset()
    }

    /// Rewrites the log, keeping only the records `keep` accepts. Safe to
    /// call between commits and nowhere else (docs/storage.md §104).
    pub fn compact(
        &mut self,
        keep: &mut impl FnMut(&[u8]) -> bool,
    ) -> Result<crate::Compaction, StorageError> {
        self.wal.compact(keep)
    }

    /// Folds one cycle into the estimates and, occasionally, into the bound.
    fn observe(&mut self, bytes: usize, write: Duration, sync: Duration) {
        self.bytes_ewma = ewma(self.bytes_ewma, u64::try_from(bytes).unwrap_or(u64::MAX));
        self.write_nanos_ewma = ewma(self.write_nanos_ewma, nanos(write));
        self.sync_nanos_ewma = ewma(self.sync_nanos_ewma, nanos(sync));
        self.commits = self.commits.saturating_add(1);

        if self.commits % ADAPT_EVERY == 0 {
            self.adapt();
        }
    }

    /// `target ≈ throughput × sync latency`, which with throughput measured as
    /// `bytes / write time` is `bytes × sync / write` — no floating point and
    /// no assumed device speed (§78).
    fn adapt(&mut self) {
        if self.write_nanos_ewma == 0 || self.bytes_ewma == 0 {
            return;
        }
        let estimate = self
            .bytes_ewma
            .saturating_mul(self.sync_nanos_ewma)
            .checked_div(self.write_nanos_ewma)
            .unwrap_or(0);

        let estimate = usize::try_from(estimate).unwrap_or(self.limits.max_batch_bytes);
        self.batch_bound = estimate.clamp(self.limits.min_batch_bytes, self.limits.max_batch_bytes);
    }
}

fn ewma(current: u64, sample: u64) -> u64 {
    if current == 0 {
        return sample;
    }
    let history = current.saturating_mul((1 << EWMA_SHIFT) - 1);
    history.saturating_add(sample) >> EWMA_SHIFT
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::Wal;
    use std::sync::mpsc::{Receiver, Sender, channel};

    fn scratch(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "straum-commit-{name}-{}-{:?}.wal",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        path
    }

    fn committer(name: &str) -> (Committer, Sender<WalRecord>, Receiver<WalRecord>) {
        let path = scratch(name);
        let (wal, _) = Wal::open(&path, WalLimits::default()).unwrap();
        let (tx, rx) = channel();
        (Committer::new(wal, WalLimits::default(), true, 0), tx, rx)
    }

    fn record(correlation: u64, size: usize) -> WalRecord {
        WalRecord {
            correlation,
            payload: Bytes::from(vec![b'x'; size]),
        }
    }

    #[test]
    fn an_idle_writer_commits_a_batch_of_one() {
        let (mut committer, tx, mut rx) = committer("idle");
        tx.send(record(1, 64)).unwrap();

        let commit = committer.commit_available(&mut rx).unwrap().unwrap();

        // No timer, no threshold, no second producer: one record, one sync.
        assert_eq!(commit.records, 1);
        assert_eq!(commit.durable_correlation, 1);
        assert_eq!(commit.durable_sequence, 1);
    }

    #[test]
    fn everything_available_goes_into_one_batch() {
        let (mut committer, tx, mut rx) = committer("batch");
        for correlation in 1..=64 {
            tx.send(record(correlation, 64)).unwrap();
        }

        let commit = committer.commit_available(&mut rx).unwrap().unwrap();

        // One durability operation for all of them, not 64 of them.
        assert_eq!(commit.records, 64);
        assert_eq!(commit.durable_correlation, 64);
    }

    #[test]
    fn records_accumulated_during_a_commit_form_the_next_batch() {
        let (mut committer, tx, mut rx) = committer("accumulate");
        tx.send(record(1, 64)).unwrap();
        let first = committer.commit_available(&mut rx).unwrap().unwrap();
        assert_eq!(first.records, 1);

        // Everything that piled up while the previous sync was running.
        for correlation in 2..=9 {
            tx.send(record(correlation, 64)).unwrap();
        }
        let second = committer.commit_available(&mut rx).unwrap().unwrap();

        assert_eq!(second.records, 8);
        assert_eq!(second.durable_correlation, 9);
        assert!(second.durable_sequence > first.durable_sequence);
    }

    #[test]
    fn a_correlation_that_overtook_another_acknowledges_neither_early() {
        let (mut committer, tx, mut rx) = committer("overtake");
        // 99 was numbered first but blocked on a full queue; 100 and 101 went
        // past it. Nothing below 99 may be called durable, because 99 is not
        // in the log yet (docs/replication.md §110).
        for correlation in [100, 101] {
            tx.send(record(correlation, 64)).unwrap();
        }

        let commit = committer.commit_available(&mut rx).unwrap().unwrap();

        assert_eq!(commit.records, 2);
        assert_eq!(
            commit.durable_correlation, 0,
            "a maximum would have reported 101 and acknowledged 99 with it"
        );
    }

    #[test]
    fn the_prefix_jumps_the_whole_run_when_the_gap_closes() {
        let (mut committer, tx, mut rx) = committer("gapclose");
        for correlation in [2, 3, 5] {
            tx.send(record(correlation, 64)).unwrap();
        }
        assert_eq!(
            committer
                .commit_available(&mut rx)
                .unwrap()
                .unwrap()
                .durable_correlation,
            0
        );

        // 1 arrives: 2 and 3 were only ever waiting for it. 5 still is not,
        // because 4 has not been committed.
        tx.send(record(1, 64)).unwrap();
        let commit = committer.commit_available(&mut rx).unwrap().unwrap();
        assert_eq!(commit.durable_correlation, 3);

        tx.send(record(4, 64)).unwrap();
        let commit = committer.commit_available(&mut rx).unwrap().unwrap();
        assert_eq!(commit.durable_correlation, 5);
    }

    #[test]
    fn a_recovered_log_continues_its_prefix_rather_than_restarting_it() {
        let path = scratch("recovered");
        let (wal, _) = Wal::open(&path, WalLimits::default()).unwrap();
        // 500 records were already durable before this process started.
        let mut committer = Committer::new(wal, WalLimits::default(), true, 500);
        let (tx, mut rx) = channel();
        tx.send(record(501, 64)).unwrap();

        let commit = committer.commit_available(&mut rx).unwrap().unwrap();

        assert_eq!(commit.durable_correlation, 501);
    }

    #[test]
    fn a_closed_channel_ends_the_loop() {
        let (mut committer, tx, mut rx) = committer("closed");
        tx.send(record(1, 64)).unwrap();
        drop(tx);

        assert!(committer.commit_available(&mut rx).unwrap().is_some());
        assert!(committer.commit_available(&mut rx).unwrap().is_none());
    }

    #[test]
    fn a_failed_durability_operation_acknowledges_nothing() {
        let path = scratch("failure");
        let (mut wal, _) = Wal::open(&path, WalLimits::default()).unwrap();
        wal.fail_next_sync();
        let mut committer = Committer::new(wal, WalLimits::default(), true, 0);
        let (tx, mut rx) = channel();
        tx.send(record(1, 64)).unwrap();

        // No commit is returned, so nothing can be acknowledged from it.
        assert!(committer.commit_available(&mut rx).is_err());

        // And the log stays refused afterwards: a durability failure is
        // terminal, not a hiccup.
        tx.send(record(2, 64)).unwrap();
        assert!(matches!(
            committer.commit_available(&mut rx),
            Err(StorageError::Poisoned)
        ));
        assert_eq!(committer.durable_sequence(), 0);
    }

    #[test]
    fn the_batch_bound_stays_between_its_limits() {
        let (mut committer, tx, mut rx) = committer("bound");
        assert_eq!(
            committer.batch_bound(),
            WalLimits::default().initial_batch_bytes
        );

        for round in 0..(ADAPT_EVERY * 2) {
            tx.send(record(round + 1, 512)).unwrap();
            committer.commit_available(&mut rx).unwrap();
        }

        let limits = WalLimits::default();
        let bound = committer.batch_bound();
        assert!(
            bound >= limits.min_batch_bytes && bound <= limits.max_batch_bytes,
            "{bound} B left the configured range"
        );
    }

    #[test]
    fn a_record_larger_than_the_limit_is_refused() {
        let (mut committer, tx, mut rx) = committer("toolarge");
        let limits = WalLimits::default();
        tx.send(record(1, limits.max_record_bytes + 1)).unwrap();

        assert!(matches!(
            committer.commit_available(&mut rx),
            Err(StorageError::RecordTooLarge)
        ));
    }
}
