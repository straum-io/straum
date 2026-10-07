//! What crosses a link between two nodes, as bytes (ADR-0008 step 4).
//!
//! ```text
//! frame    [ length u32 ][ type u8 ][ payload ]     length = 1 + payload
//! HELLO    1  [ node u8 ][ slot u64 ][ limits u64 ]
//! APPEND   2  [ slot u64 ][ commit u64 ][ count u32 ]( [ len u32 ][ record ] ){count}
//! ACK      3  [ slot u64 ][ end u64 ][ durable u64 ]
//! FORWARD  4  [ kind u8 ][ properties_len u32 ][ properties ][ body ]
//! ```
//!
//! A link carries one direction: the dialling node sends, the accepting
//! node reads. It opens with the sender's `HELLO`, which names it and
//! carries a digest of its `apply` limits; a receiver whose limits differ
//! refuses the link (they are part of what the log means). Nothing sent to
//! one process reaches its successor: a link does not outlive either end,
//! and a sender drops what it queued for a link that broke.
//!
//! Every length is remote-controlled: this module refuses indexing,
//! unchecked arithmetic and truncating casts, and a frame longer than
//! `max_frame` is refused before it is waited for.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use journal::Kind;

use crate::{Message, NodeId, Proposal};

const HELLO: u8 = 1;
const APPEND: u8 = 2;
const ACK: u8 = 3;
const FORWARD: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    Hello {
        node: NodeId,
        slot: u64,
        limits: u64,
    },
    Message(Message),
    /// A gateway's proposal on its way to the sequencer.
    Forward(Proposal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WireError {
    #[error("frame longer than the limit")]
    TooLarge,
    #[error("frame ends before its fields do")]
    Truncated,
    #[error("bytes after the last field of a frame")]
    Trailing,
    #[error("unknown frame type {0}")]
    UnknownType(u8),
    #[error("unknown record kind {0}")]
    UnknownKind(u8),
}

fn put_u32(value: usize, out: &mut Vec<u8>) -> Result<(), WireError> {
    let value = u32::try_from(value).map_err(|_| WireError::TooLarge)?;
    out.extend_from_slice(&value.to_be_bytes());
    Ok(())
}

/// Appends one frame.
pub fn encode(frame: &Frame, out: &mut Vec<u8>) -> Result<(), WireError> {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    match frame {
        Frame::Hello { node, slot, limits } => {
            out.push(HELLO);
            out.push(*node);
            out.extend_from_slice(&slot.to_be_bytes());
            out.extend_from_slice(&limits.to_be_bytes());
        }
        Frame::Message(Message::Append {
            slot,
            records,
            commit,
        }) => {
            out.push(APPEND);
            out.extend_from_slice(&slot.to_be_bytes());
            out.extend_from_slice(&commit.to_be_bytes());
            put_u32(records.len(), out)?;
            for record in records {
                put_u32(record.len(), out)?;
                out.extend_from_slice(record);
            }
        }
        Frame::Message(Message::Ack { slot, end, durable }) => {
            out.push(ACK);
            out.extend_from_slice(&slot.to_be_bytes());
            out.extend_from_slice(&end.to_be_bytes());
            out.extend_from_slice(&durable.to_be_bytes());
        }
        Frame::Forward(proposal) => {
            out.push(FORWARD);
            out.push(proposal.kind as u8);
            put_u32(proposal.properties.len(), out)?;
            out.extend_from_slice(&proposal.properties);
            out.extend_from_slice(&proposal.body);
        }
    }
    let length = out
        .len()
        .checked_sub(start)
        .and_then(|n| n.checked_sub(4))
        .ok_or(WireError::TooLarge)?;
    let length = u32::try_from(length).map_err(|_| WireError::TooLarge)?;
    if let Some(slot) = out.get_mut(start..start.saturating_add(4)) {
        slot.copy_from_slice(&length.to_be_bytes());
    }
    Ok(())
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, count: usize) -> Result<&'a [u8], WireError> {
        if self.0.len() < count {
            return Err(WireError::Truncated);
        }
        let (head, rest) = self.0.split_at(count);
        self.0 = rest;
        Ok(head)
    }
    fn u8(&mut self) -> Result<u8, WireError> {
        self.take(1)?.first().copied().ok_or(WireError::Truncated)
    }
    fn u32(&mut self) -> Result<usize, WireError> {
        let bytes: [u8; 4] = self.take(4)?.try_into().map_err(|_| WireError::Truncated)?;
        usize::try_from(u32::from_be_bytes(bytes)).map_err(|_| WireError::TooLarge)
    }
    fn u64(&mut self) -> Result<u64, WireError> {
        let bytes: [u8; 8] = self.take(8)?.try_into().map_err(|_| WireError::Truncated)?;
        Ok(u64::from_be_bytes(bytes))
    }
    fn finish(self) -> Result<(), WireError> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(WireError::Trailing)
        }
    }
}

/// Reads one frame from the front of `buf`. `Ok(None)` while it is
/// incomplete; otherwise the frame and how many bytes it took.
pub fn decode(buf: &[u8], max_frame: usize) -> Result<Option<(Frame, usize)>, WireError> {
    let Some(header) = buf.get(..4) else {
        return Ok(None);
    };
    let length: [u8; 4] = header.try_into().map_err(|_| WireError::Truncated)?;
    let length = usize::try_from(u32::from_be_bytes(length)).map_err(|_| WireError::TooLarge)?;
    // Refused before it is waited for: a peer cannot make the reader hold
    // more than one frame's limit.
    if length > max_frame {
        return Err(WireError::TooLarge);
    }
    let total = length.checked_add(4).ok_or(WireError::TooLarge)?;
    let Some(frame) = buf.get(4..total) else {
        return Ok(None);
    };
    let mut r = Reader(frame);
    let frame = match r.u8()? {
        HELLO => Frame::Hello {
            node: r.u8()?,
            slot: r.u64()?,
            limits: r.u64()?,
        },
        APPEND => {
            let slot = r.u64()?;
            let commit = r.u64()?;
            let count = r.u32()?;
            // Each record takes at least its four length bytes: a count the
            // frame cannot hold is refused before anything is allocated.
            if count > r.0.len() / 4 {
                return Err(WireError::Truncated);
            }
            let mut records = Vec::with_capacity(count);
            for _ in 0..count {
                let len = r.u32()?;
                records.push(r.take(len)?.to_vec());
            }
            Frame::Message(Message::Append {
                slot,
                records,
                commit,
            })
        }
        ACK => Frame::Message(Message::Ack {
            slot: r.u64()?,
            end: r.u64()?,
            durable: r.u64()?,
        }),
        FORWARD => {
            let kind = r.u8()?;
            let kind = Kind::from_u8(kind).ok_or(WireError::UnknownKind(kind))?;
            let len = r.u32()?;
            let properties = r.take(len)?.to_vec();
            let body = std::mem::take(&mut r.0).to_vec();
            Frame::Forward(Proposal {
                kind,
                properties,
                body,
            })
        }
        other => return Err(WireError::UnknownType(other)),
    };
    r.finish()?;
    Ok(Some((frame, total)))
}
