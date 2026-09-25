//! The wire format.
//!
//! One logical frame carries one complete WAL commit batch, exactly the bytes
//! the leader's `Wal::commit_with` handed to `write()`. Replication does not
//! re-encode the records inside it and does not invent an ordering of its own:
//! the WAL sequence already established by the log is the identity, and a
//! frame names the range of it that it covers (docs/replication.md §114).
//!
//! A follower acknowledges a frame only once the whole of it is durable. The
//! network is free to deliver a frame in any number of pieces — [`decode`]
//! asks for more bytes until the frame is complete — but a partial read is
//! never a partial durability boundary.
//!
//! Bytes arriving here come off a socket, so lengths are remote-controlled and
//! this module refuses indexing and unchecked arithmetic the same way the MQTT
//! decoder and the WAL record module do.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

const KIND_RECORDS: u8 = 1;
const KIND_DURABLE: u8 = 2;

/// `kind` + `previous` + `last` + `bytes`. The checksum covers these and the
/// batch that follows: a per-record CRC32 inside the batch says whether one
/// record is intact, not whether these are the records the header was written
/// for (docs/replication.md §114).
const RECORDS_FIELDS: usize = 21;
/// The whole records header, checksum included.
pub const RECORDS_HEADER_BYTES: usize = 25;

/// `kind` + `sequence`.
const DURABLE_FIELDS: usize = 9;
/// The whole acknowledgement.
pub const DURABLE_BYTES: usize = 13;

/// Bounds on what a peer may ask this side to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// Largest WAL batch one frame may carry. A frame claiming more than this
    /// is refused before the reader is asked to buffer it, so a bogus length
    /// cannot grow the read buffer (CLAUDE.md §8).
    ///
    /// The default leaves headroom over `WalLimits::max_batch_bytes`, which
    /// bounds what the leader can produce. It becomes a `STRAUM_*` setting
    /// when replication is wired up; until then nothing reads it from the
    /// environment.
    pub max_frame_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_frame_bytes: 8_388_608,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReplicationError {
    #[error("unknown frame kind {0}")]
    UnknownKind(u8),
    #[error("frame claims {bytes} B, limit is {limit} B")]
    FrameTooLarge { bytes: usize, limit: usize },
    #[error("frame failed its checksum")]
    Checksum,
    /// `last <= previous`, or a batch of no bytes. A commit that carried
    /// nothing is not a commit, and a frame is one commit.
    #[error("frame covers no records")]
    Empty,
}

/// One decoded message.
///
/// `Records` borrows the batch out of the caller's buffer: the follower hands
/// those bytes to its own WAL, which is the only thing that will parse them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message<'a> {
    Records(Records<'a>),
    /// The sender has this WAL sequence, and every sequence below it,
    /// durable. Contiguous by construction: it is the last sequence of a
    /// whole frame, and a frame is acknowledged only complete.
    Durable(u64),
}

/// One WAL commit batch, on its way to a follower.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Records<'a> {
    /// The last WAL sequence the receiver must already hold. Whether it does
    /// is the state machine's question, not the codec's.
    pub previous: u64,
    /// The last WAL sequence this frame carries. What the receiver
    /// acknowledges, once the batch is durable on its side.
    pub last: u64,
    /// The leader's batch, byte for byte as its WAL wrote it.
    pub batch: &'a [u8],
}

impl Records<'_> {
    /// How many records the batch holds. The WAL advances its sequence once
    /// per staged record, so the range says this without parsing anything.
    pub fn records(&self) -> u64 {
        self.last.saturating_sub(self.previous)
    }
}

/// Appends the header for `batch`. The caller writes the batch itself.
///
/// `batch` is **read and not copied**: the checksum covers it, so the bytes
/// have to be hashed here, but they never enter `out`. That is what keeps the
/// question of how the batch reaches the socket — copied after the header,
/// handed over as an owned buffer, or written vectored — a measurement the
/// runtime makes rather than one the codec settled (docs/replication.md §112).
pub fn encode_records_header(previous: u64, last: u64, batch: &[u8], out: &mut Vec<u8>) {
    let start = out.len();
    out.push(KIND_RECORDS);
    out.extend_from_slice(&previous.to_be_bytes());
    out.extend_from_slice(&last.to_be_bytes());
    out.extend_from_slice(&u32::try_from(batch.len()).unwrap_or(u32::MAX).to_be_bytes());
    put_checksum(out, start, RECORDS_FIELDS, batch);
}

/// Appends one acknowledgement.
pub fn encode_durable(sequence: u64, out: &mut Vec<u8>) {
    let start = out.len();
    out.push(KIND_DURABLE);
    out.extend_from_slice(&sequence.to_be_bytes());
    put_checksum(out, start, DURABLE_FIELDS, &[]);
}

/// Reads the message at the start of `input`.
///
/// `Ok(None)` means the bytes are not all here yet and the caller should read
/// more — never that anything is wrong. `Ok(Some((message, consumed)))` hands
/// back a complete message and how much of `input` it used. An `Err` is a
/// protocol violation: the peer is not speaking this format, and the
/// connection is the thing to end, not the frame to skip.
pub fn decode(
    input: &[u8],
    limits: Limits,
) -> Result<Option<(Message<'_>, usize)>, ReplicationError> {
    let Some(&kind) = input.first() else {
        return Ok(None);
    };
    match kind {
        KIND_RECORDS => decode_records(input, limits),
        KIND_DURABLE => decode_durable(input),
        other => Err(ReplicationError::UnknownKind(other)),
    }
}

fn decode_records(
    input: &[u8],
    limits: Limits,
) -> Result<Option<(Message<'_>, usize)>, ReplicationError> {
    let Some(header) = input.get(..RECORDS_HEADER_BYTES) else {
        return Ok(None);
    };
    let previous = be_u64(header, 1)?;
    let last = be_u64(header, 9)?;
    let bytes = usize::try_from(be_u32(header, 17)?).unwrap_or(usize::MAX);

    // The length is acted on before it is trusted, because it decides how
    // much the caller is asked to buffer. Refusing it here is what keeps a
    // bogus length from growing the read buffer (CLAUDE.md §8); everything
    // else waits for the checksum.
    let Some(end) = RECORDS_HEADER_BYTES
        .checked_add(bytes)
        .filter(|_| bytes <= limits.max_frame_bytes)
    else {
        return Err(ReplicationError::FrameTooLarge {
            bytes,
            limit: limits.max_frame_bytes,
        });
    };
    let Some(batch) = input.get(RECORDS_HEADER_BYTES..end) else {
        return Ok(None);
    };

    // Covers the header fields and the batch together, so what is checked is
    // "these are the bytes that were sent, under this header". A per-record
    // CRC32 inside the batch answers a different question — whether that one
    // record is intact — and would still pass if a whole record were replaced
    // by another well-formed one (docs/replication.md §114).
    verify(header, RECORDS_FIELDS, batch)?;

    if last <= previous || bytes == 0 {
        return Err(ReplicationError::Empty);
    }

    Ok(Some((
        Message::Records(Records {
            previous,
            last,
            batch,
        }),
        end,
    )))
}

fn decode_durable(input: &[u8]) -> Result<Option<(Message<'_>, usize)>, ReplicationError> {
    let Some(header) = input.get(..DURABLE_BYTES) else {
        return Ok(None);
    };
    verify(header, DURABLE_FIELDS, &[])?;
    Ok(Some((Message::Durable(be_u64(header, 1)?), DURABLE_BYTES)))
}

fn put_checksum(out: &mut Vec<u8>, start: usize, fields: usize, payload: &[u8]) {
    let end = start.saturating_add(fields);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(out.get(start..end).unwrap_or_default());
    hasher.update(payload);
    out.extend_from_slice(&hasher.finalize().to_be_bytes());
}

fn verify(header: &[u8], fields: usize, payload: &[u8]) -> Result<(), ReplicationError> {
    let (Some(covered), Some(expected)) = (header.get(..fields), header.get(fields..)) else {
        return Err(ReplicationError::Checksum);
    };
    let expected = <[u8; 4]>::try_from(expected)
        .map(u32::from_be_bytes)
        .map_err(|_| ReplicationError::Checksum)?;
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(covered);
    hasher.update(payload);
    if hasher.finalize() != expected {
        return Err(ReplicationError::Checksum);
    }
    Ok(())
}

fn be_u64(bytes: &[u8], at: usize) -> Result<u64, ReplicationError> {
    let end = at.checked_add(8).ok_or(ReplicationError::Checksum)?;
    bytes
        .get(at..end)
        .and_then(|slice| <[u8; 8]>::try_from(slice).ok())
        .map(u64::from_be_bytes)
        .ok_or(ReplicationError::Checksum)
}

fn be_u32(bytes: &[u8], at: usize) -> Result<u32, ReplicationError> {
    let end = at.checked_add(4).ok_or(ReplicationError::Checksum)?;
    bytes
        .get(at..end)
        .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
        .map(u32::from_be_bytes)
        .ok_or(ReplicationError::Checksum)
}
