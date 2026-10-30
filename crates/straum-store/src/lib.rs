//! Content-addressed log storage engine: one shard, local only. Objects are
//! deduplicated by BLAKE3 id; the new objects of each batch are stored as
//! one zstd-compressed block.
//!
//! On-disk formats and recovery rules: `docs/format.md` in this crate.

mod block;
mod codec;
mod fault;
mod fuse;
mod recent;
mod store;

pub use codec::{FrameMode, MAX_TOPIC, ObjectId};
/// Deterministic fault injection, for tests (ADR-0014, before step 6b).
#[doc(hidden)]
pub use fault::{FaultPoint, arm as arm_fault, disarm as disarm_fault};
/// The recent-id cache, public for its measurement (`straum-store-bench`'s
/// `recent-ids`).
pub use recent::RecentIds;
pub use store::{
    CommitTiming, CompactStats, CompactStep, Config, ConfigOverride, DropReason, Entry, Message,
    RecoveryAction, RecoveryReport, Stats, Store, TopicStats, WriteMode,
};

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("store is unusable after an earlier i/o failure; reopen it")]
    Poisoned,
    #[error("topic must be 1..={MAX_TOPIC} bytes")]
    InvalidTopic,
    #[error("object larger than u32::MAX bytes")]
    ObjectTooLarge,
    /// The operation would produce a WAL frame larger than
    /// `Config::max_frame_bytes`. Returned before any I/O.
    #[error("operation exceeds the maximum WAL frame size")]
    FrameTooLarge,
    /// A WAL file does not start with this version's file header (another
    /// version, another program's file, a damaged header), or the
    /// directory holds the single `wal.log` of a format before 13.
    #[error("WAL format not supported by this version")]
    UnsupportedFormat,
    /// A frame with a valid CRC that is malformed, out of sequence, or
    /// inconsistent with the frames before it.
    /// Also WAL files that do not continue one another. `file` names the
    /// WAL file by the sequence number of its first frame.
    #[error("corrupt WAL file {file} at offset {offset}: {reason}")]
    CorruptWal {
        file: u64,
        offset: u64,
        reason: &'static str,
    },
    /// A frame fails its CRC (or is short) but a frame with a valid CRC
    /// starts later in the file, or a later WAL file exists (one is created
    /// only after the previous file's last frame is durable): mid-log
    /// damage, not a torn tail. The WAL is left unmodified.
    #[error(
        "WAL file {file} damaged at offset {offset}; file {next_file} continues at {next_offset}"
    )]
    WalCorrupted {
        file: u64,
        offset: u64,
        next_file: u64,
        next_offset: u64,
    },
    #[error("object {0:?} is missing or truncated on disk")]
    MissingObject(ObjectId),
    #[error("object {0:?} failed its hash check")]
    HashMismatch(ObjectId),
    /// A block's stored bytes are missing, do not decompress, or fail the
    /// raw length, CRC32C or layout the WAL records for them.
    #[error("block {block} is corrupt: {reason}")]
    CorruptBlock { block: u64, reason: &'static str },
    /// A topic entry or Publish record refers to an object number the
    /// object index does not hold: at open, a live entry whose object a
    /// Compact record reclaimed; in compaction's mark phase, a live entry
    /// read from the topic index.
    #[error("object number {0} is not in the object index")]
    UnknownObjectNumber(u64),
    /// An intact index block record of a durable WAL frame holds other
    /// entries than the WAL: it was written that way, or a damaged index
    /// segment header lost a fence (docs/format.md, "Index segment"). The
    /// index is derived from the WAL: `Config::rebuild_index` rebuilds it
    /// (the broker's one-start flag `--store-rebuild-index`).
    #[error("index of topic {topic:?} disagrees with the WAL at entry {seq}")]
    TopicMismatch { topic: Vec<u8>, seq: u64 },
    /// A full block of a topic index, or a seek page leading to it, read
    /// after open, is missing, fails its CRC or is another block's: its
    /// entries are not returned.
    #[error("index of topic {topic:?}: block {block} is corrupt")]
    TopicIndexCorrupt { topic: Vec<u8>, block: u64 },
    /// An intact record of an index segment, from a durable WAL frame,
    /// that cannot be: an unknown kind, an undecodable body, or a block the
    /// WAL does not have. No file is changed.
    #[error("index segment {segment} at offset {offset}: {reason}")]
    IndexSegmentCorrupt {
        segment: u64,
        offset: u64,
        reason: &'static str,
    },
    /// The newest checkpoint manifest, or a table or run it lists, is
    /// missing, short, fails a CRC or is inconsistent (docs/format.md,
    /// "Checkpoint"). No file is changed.
    #[error("checkpoint {checkpoint}: {reason}")]
    CorruptCheckpoint {
        checkpoint: u64,
        reason: &'static str,
    },
    /// A checkpoint's object table or relocation run gives object `number`
    /// another place (or none) than the WAL has at the frames the
    /// checkpoint covers. No file is changed.
    #[error("checkpoint {checkpoint} disagrees with the WAL on object number {number}")]
    CheckpointMismatch { checkpoint: u64, number: u64 },
    /// A checkpoint's block table or block relocation run gives block
    /// `block` another place, layout or dictionary (or none) than the WAL
    /// has at the frames the checkpoint covers. No file is changed.
    #[error("checkpoint {checkpoint} disagrees with the WAL on block {block}")]
    CheckpointBlockMismatch { checkpoint: u64, block: u64 },
}
