//! Durable storage: a write-ahead log with adaptive group commit.
//!
//! The rules this crate implements are in `docs/storage.md`. Two of them shape
//! everything else: the storage device provides the pacing signal, and nothing
//! is acknowledged as durable before the durability operation that covers it
//! has completed.
//!
//! This crate does blocking file I/O and reads the clock to measure storage
//! latency. It owns no threads: the runtime gives it one.

mod commit;
mod crc;
mod record;
mod wal;

pub use commit::{Commit, Committer, RecordSource, WalRecord};
pub use wal::{Committed, Compaction, HEADER_BYTES, Recovery, Wal, compact, replay};

/// Limits of the write-ahead log. Defaults are the ones documented in
/// `docs/configuration.md`; changing one is a behavior change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalLimits {
    /// Largest payload one record may carry.
    pub max_record_bytes: usize,
    /// Floor for the adaptive batch bound.
    pub min_batch_bytes: usize,
    /// Upper safety boundary for one batch — not a size to wait for (§77).
    pub max_batch_bytes: usize,
    /// Where adaptation starts, before any storage latency is known.
    pub initial_batch_bytes: usize,
}

impl Default for WalLimits {
    fn default() -> Self {
        Self {
            max_record_bytes: 1_048_576,
            min_batch_bytes: 65_536,
            max_batch_bytes: 4_194_304,
            initial_batch_bytes: 1_048_576,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("the log is not a straum WAL")]
    NotAWal,
    /// The magic is ours but the state after it does not check out. The
    /// header carries the allocation high-water, and a damaged one reads as
    /// a plausible number that would repeat identifiers silently.
    #[error("the log header is damaged")]
    CorruptHeader,
    #[error("record is empty or larger than the configured maximum")]
    RecordTooLarge,
    /// A durability operation failed. Nothing may be acknowledged afterwards.
    #[error("the log failed a durability operation and cannot be trusted")]
    Poisoned,
    /// Compaction was asked for with a batch still staged. The batch would
    /// have been written to a file about to be replaced.
    #[error("the log cannot be rewritten while a batch is staged")]
    StagedDuringCompaction,
    /// A replicated run did not decompose into whole records. Appending it
    /// would leave a half record where truncation cannot reach it.
    #[error("the bytes are not a whole number of framed records")]
    NotFramedRecords,
    #[error("{0}")]
    Io(#[from] std::io::Error),
}

/// Exposed for `benches/record.rs`: the per-byte cost of the write path is a
/// number this project is required to know (docs/storage.md §97).
#[doc(hidden)]
pub fn checksum_for_benchmarks(bytes: &[u8]) -> u32 {
    crc::checksum(bytes)
}

/// Exposed for `benches/record.rs`, see above.
#[doc(hidden)]
pub fn encode_for_benchmarks(payload: &[u8], out: &mut Vec<u8>) {
    record::encode(payload, out);
}

/// The header a fresh log starts with, for tests and benchmarks that build a
/// log file by hand instead of through [`Wal::open`].
#[doc(hidden)]
pub fn empty_header_for_benchmarks() -> Vec<u8> {
    wal::empty_header()
}
