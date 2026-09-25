//! Replication: the wire format, and the state machine that decides what a
//! position means. Sans-IO throughout — no sockets, no tasks, no clock, no
//! files, and no other workspace crate. What it moves is opaque bytes.
//!
//! The rules are in `docs/replication.md`. One shapes everything else: a
//! durable position is a contiguous prefix or it is nothing, on either side
//! of the wire (§111).

mod frame;
mod state;

pub use frame::{
    DURABLE_BYTES, Limits, Message, RECORDS_HEADER_BYTES, Records, ReplicationError, decode,
    encode_durable, encode_records_header,
};
pub use state::{Accepted, Follower, Peer, PeerError, Refused, quorum_watermark};
