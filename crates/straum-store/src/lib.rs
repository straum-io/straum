//! Content-addressed log storage engine, phase 1: one shard, local only.
//!
//! On-disk formats and recovery rules: `docs/format.md` in this crate.

mod codec;
mod store;

use std::path::PathBuf;

pub use codec::{FrameMode, MAX_TOPIC, ObjectId};
pub use store::{
    CompactStats, Config, DropReason, Entry, Message, RecoveryAction, RecoveryReport, Stats, Store,
    WriteMode,
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
    /// `wal.log` does not start with this version's file header (a phase 1
    /// WAL, another program's file, or a future version).
    #[error("WAL format not supported by this version")]
    UnsupportedFormat,
    /// A frame with a valid CRC that is malformed, out of sequence, or
    /// inconsistent with the frames before it.
    #[error("corrupt WAL at offset {offset}: {reason}")]
    CorruptWal { offset: u64, reason: &'static str },
    /// A frame fails its CRC (or is short) but a frame with a valid CRC
    /// starts later in the file: mid-log damage, not a torn tail. The WAL is
    /// left unmodified.
    #[error("WAL damaged at offset {offset}; a valid frame follows at {next_frame}")]
    WalCorrupted { offset: u64, next_frame: u64 },
    #[error("object {0:?} is missing or truncated on disk")]
    MissingObject(ObjectId),
    #[error("object {0:?} failed its hash check")]
    HashMismatch(ObjectId),
    /// A topic entry or Publish record refers to an object number the
    /// object index does not hold.
    #[error("object number {0} is not in the object index")]
    UnknownObjectNumber(u64),
    #[error("topic index {path:?} disagrees with the WAL at entry {seq}")]
    TopicMismatch { path: PathBuf, seq: u64 },
    #[error("refcount of {id:?}: WAL replay says {wal}, topic indexes say {topics}")]
    RefcountMismatch { id: ObjectId, wal: u64, topics: u64 },
}
