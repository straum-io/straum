//! The log's record format, version 5 (ADR-0008 step 1; unified-stream §2.2;
//! D-188 A).
//!
//! `storage` frames records on disk (length, checksum) and recovers the
//! durable prefix; this crate says what a record *is*:
//!
//! ```text
//! record    [ version u8 = 5 ][ kind u8 ][ slot u64 ][ lsn u64 ]
//!           [ prev_hash u128 ][ properties_len u32 ][ properties ][ body ]
//! property  [ id u8 ][ len u16 ][ value ]
//! header    a record of kind BatchHeader whose body is [ records u32 ]
//! ```
//!
//! Every record carries `(slot, lsn, prev_hash)`, and `prev_hash` is computed
//! over the group-commit batch, not per message (§2.2, D-56 A): every record
//! of a batch names the hash of the batch before it, which is xxh3-128 over
//! all of that batch's record bytes, header included. Each batch therefore
//! names the whole history before it, and two logs that diverged anywhere
//! disagree from the next batch on. Every commit batch starts with a header
//! that counts its records: the batch is the unit of commit and replication,
//! and a batch cut short by a crash is a torn tail, never half a commit.
//! `lsn` is global and monotonic and never restarts in a new slot.
//!
//! Integers are big-endian. Lengths read from a file or a socket are
//! untrusted: this module refuses indexing and unchecked arithmetic.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use xxhash_rust::xxh3::Xxh3;

pub const VERSION: u8 = 5;

/// Version, kind, slot, lsn, prev_hash, properties length.
pub const RECORD_HEADER_BYTES: usize = 1 + 1 + 8 + 8 + 16 + 4;
/// The record count.
pub const BATCH_HEADER_BODY_BYTES: usize = 4;

/// What a record is. The set is closed per version: a kind this version does
/// not know is refused, not skipped, because skipping would apply a log
/// differently from a node that knows it.
///
/// Version 5 (D-188 A) dropped the MQTT kinds of version 4: a version 4
/// record is refused by its version byte, never read as `Data`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    BatchHeader = 0,
    /// A client's record. Its body is opaque to the log.
    Data = 1,
    /// Handoff between sequencers (§2.4).
    SlotStart = 24,
    SlotEnd = 25,
}

impl Kind {
    pub fn from_u8(value: u8) -> Option<Self> {
        Some(match value {
            0 => Self::BatchHeader,
            1 => Self::Data,
            24 => Self::SlotStart,
            25 => Self::SlotEnd,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum JournalError {
    #[error("record is shorter than its header")]
    Truncated,
    #[error("record version {0}, this build reads {VERSION}")]
    Version(u8),
    #[error("record kind {0} is not known to this version")]
    Kind(u8),
    #[error("properties length runs past the record")]
    Properties,
    #[error("a batch header's body is malformed")]
    Header,
}

/// One decoded record, borrowing the bytes it came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record<'a> {
    pub kind: Kind,
    pub slot: u64,
    pub lsn: u64,
    pub prev_hash: u128,
    pub properties: Properties<'a>,
    pub body: &'a [u8],
}

/// The property every node puts on its own proposals (D-188 A): `[node
/// u8][incarnation u64][sequence u64]`, unique per proposal, so a node tells
/// its own record from another node's with the same body.
pub const ORIGIN_PROPERTY: u8 = 1;
/// The origin property's value length.
pub const ORIGIN_BYTES: u16 = 17;

/// The properties section, as stored: `[id][len u16][value]` repeated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Properties<'a>(&'a [u8]);

impl<'a> Properties<'a> {
    /// A properties section held elsewhere, read the same way.
    pub fn new(bytes: &'a [u8]) -> Self {
        Self(bytes)
    }

    pub fn bytes(&self) -> &'a [u8] {
        self.0
    }

    /// Each property as `(id, value)`; a malformed one ends the iteration
    /// with an error.
    pub fn iter(&self) -> PropertyIter<'a> {
        PropertyIter(self.0)
    }
}

pub struct PropertyIter<'a>(&'a [u8]);

impl<'a> Iterator for PropertyIter<'a> {
    type Item = Result<(u8, &'a [u8]), JournalError>;

    fn next(&mut self) -> Option<Self::Item> {
        let (&id, rest) = self.0.split_first()?;
        let parsed = rest
            .get(..2)
            .and_then(|len| <[u8; 2]>::try_from(len).ok())
            .map(|len| usize::from(u16::from_be_bytes(len)))
            .and_then(|len| {
                let value = rest.get(2..len.checked_add(2)?)?;
                let after = rest.get(len.checked_add(2)?..)?;
                Some((value, after))
            });
        match parsed {
            Some((value, after)) => {
                self.0 = after;
                Some(Ok((id, value)))
            }
            None => {
                self.0 = &[];
                Some(Err(JournalError::Properties))
            }
        }
    }
}

/// Appends one property. `value` longer than `u16::MAX` is a caller's bug
/// that is refused rather than truncated.
pub fn encode_property(id: u8, value: &[u8], out: &mut Vec<u8>) -> Result<(), JournalError> {
    let len = u16::try_from(value.len()).map_err(|_| JournalError::Properties)?;
    out.push(id);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

/// The fields every record carries before its properties.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Position {
    pub slot: u64,
    pub lsn: u64,
    pub prev_hash: u128,
}

/// Appends one record. `properties` is an already encoded section.
pub fn encode(
    kind: Kind,
    at: Position,
    properties: &[u8],
    body: &[u8],
    out: &mut Vec<u8>,
) -> Result<(), JournalError> {
    let len = u32::try_from(properties.len()).map_err(|_| JournalError::Properties)?;
    out.push(VERSION);
    out.push(kind as u8);
    out.extend_from_slice(&at.slot.to_be_bytes());
    out.extend_from_slice(&at.lsn.to_be_bytes());
    out.extend_from_slice(&at.prev_hash.to_be_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(properties);
    out.extend_from_slice(body);
    Ok(())
}

fn array_at<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    let end = at.checked_add(N)?;
    bytes.get(at..end).and_then(|b| <[u8; N]>::try_from(b).ok())
}

/// Reads one record's bytes, as `storage` handed them over.
pub fn decode(bytes: &[u8]) -> Result<Record<'_>, JournalError> {
    if bytes.len() < RECORD_HEADER_BYTES {
        return Err(JournalError::Truncated);
    }
    let (&version, rest) = bytes.split_first().ok_or(JournalError::Truncated)?;
    if version != VERSION {
        return Err(JournalError::Version(version));
    }
    let (&kind, _) = rest.split_first().ok_or(JournalError::Truncated)?;
    let kind = Kind::from_u8(kind).ok_or(JournalError::Kind(kind))?;
    let slot = array_at(bytes, 2)
        .map(u64::from_be_bytes)
        .ok_or(JournalError::Truncated)?;
    let lsn = array_at(bytes, 10)
        .map(u64::from_be_bytes)
        .ok_or(JournalError::Truncated)?;
    let prev_hash = array_at(bytes, 18)
        .map(u128::from_be_bytes)
        .ok_or(JournalError::Truncated)?;
    let properties_len = array_at(bytes, 34)
        .map(u32::from_be_bytes)
        .and_then(|len| usize::try_from(len).ok())
        .ok_or(JournalError::Truncated)?;
    let end = RECORD_HEADER_BYTES
        .checked_add(properties_len)
        .ok_or(JournalError::Properties)?;
    let properties = bytes
        .get(RECORD_HEADER_BYTES..end)
        .ok_or(JournalError::Properties)?;
    let body = bytes.get(end..).ok_or(JournalError::Properties)?;
    Ok(Record {
        kind,
        slot,
        lsn,
        prev_hash,
        properties: Properties(properties),
        body,
    })
}

/// The body of a batch header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchHeader {
    /// Records that follow the header in this batch.
    pub records: u32,
}

impl BatchHeader {
    pub fn decode(body: &[u8]) -> Result<Self, JournalError> {
        if body.len() != BATCH_HEADER_BODY_BYTES {
            return Err(JournalError::Header);
        }
        let records = array_at(body, 0)
            .map(u32::from_be_bytes)
            .ok_or(JournalError::Header)?;
        Ok(Self { records })
    }
}

/// One record of a batch being built, before it has a number.
#[derive(Debug, Clone, Copy)]
pub struct Entry<'a> {
    pub kind: Kind,
    pub properties: &'a [u8],
    pub body: &'a [u8],
}

/// Where a log stands after its last complete batch: that batch's hash —
/// the `prev_hash` every record of the next batch carries — the next `lsn`
/// to hand out, and the slot it was written in. The writer extends it; a
/// reader resumes verification from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chain {
    hash: u128,
    next_lsn: u64,
    slot: u64,
}

impl Chain {
    /// An empty log: no batch before the first, lsn from 1, no slot yet.
    pub fn genesis() -> Self {
        Self {
            hash: 0,
            next_lsn: 1,
            slot: 0,
        }
    }

    /// Continues a log whose last complete batch hashed to `hash`, ended at
    /// `last_lsn` and was written in `slot`. The first record of the batch
    /// after it carries all three: its `prev_hash`, its `lsn` minus one and
    /// a slot no lower.
    pub fn resume(hash: u128, last_lsn: u64, slot: u64) -> Self {
        Self {
            hash,
            next_lsn: last_lsn.saturating_add(1),
            slot,
        }
    }

    /// The hash of the last complete batch.
    pub fn hash(&self) -> u128 {
        self.hash
    }

    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn slot(&self) -> u64 {
        self.slot
    }

    /// Builds one batch for `slot`: a header, then `entries`, each record's
    /// bytes appended to `out` as its own element. Every record carries the
    /// previous batch's hash; this batch's hash is xxh3-128 over all of its
    /// records' bytes, header included. A slot lower than the chain's is
    /// refused: slots only grow (§2.2).
    pub fn batch(
        &mut self,
        slot: u64,
        entries: &[Entry<'_>],
        out: &mut Vec<Vec<u8>>,
    ) -> Result<(), ChainError> {
        if slot < self.slot {
            return Err(ChainError::SlotBack {
                lsn: self.next_lsn,
                from: self.slot,
                to: slot,
            });
        }
        let count = u32::try_from(entries.len()).map_err(|_| JournalError::Header)?;
        let mut hasher = Xxh3::new();
        let mut lsn = self.next_lsn;
        let mut records = Vec::with_capacity(entries.len().saturating_add(1));
        let header = Entry {
            kind: Kind::BatchHeader,
            properties: &[],
            body: &count.to_be_bytes(),
        };
        for entry in std::iter::once(&header).chain(entries) {
            let mut record = Vec::with_capacity(
                RECORD_HEADER_BYTES
                    .saturating_add(entry.properties.len())
                    .saturating_add(entry.body.len()),
            );
            let at = Position {
                slot,
                lsn,
                prev_hash: self.hash,
            };
            encode(entry.kind, at, entry.properties, entry.body, &mut record)?;
            hasher.update(&record);
            records.push(record);
            lsn = lsn.saturating_add(1);
        }
        // Only a whole batch moves the chain: a refused entry leaves it, and
        // `out`, as they were.
        out.append(&mut records);
        *self = Self {
            hash: hasher.digest128(),
            next_lsn: lsn,
            slot,
        };
        Ok(())
    }
}

/// Why a sequence of records is not one log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChainError {
    #[error("record at lsn {found} where {expected} was next")]
    Lsn { expected: u64, found: u64 },
    #[error("record at lsn {lsn} names prev_hash {found:#x}, the chain is at {expected:#x}")]
    PrevHash {
        lsn: u64,
        expected: u128,
        found: u128,
    },
    #[error("record at lsn {lsn} is outside any batch")]
    OutsideBatch { lsn: u64 },
    #[error("a new batch began at lsn {lsn} before the last one was complete")]
    Interrupted { lsn: u64 },
    #[error("record at lsn {lsn} is in slot {found}, its batch is in {expected}")]
    Slot { lsn: u64, expected: u64, found: u64 },
    #[error("slot went back from {from} to {to} at lsn {lsn}")]
    SlotBack { lsn: u64, from: u64, to: u64 },
    #[error("{0}")]
    Record(#[from] JournalError),
}

/// The reader's side: feeds records in order and says whether they are one
/// unbroken history (unified-stream §5, invariant 5).
#[derive(Clone)]
pub struct Verifier {
    /// After the last complete batch.
    committed: Chain,
    open: Option<Open>,
}

#[derive(Clone)]
struct Open {
    hasher: Xxh3,
    /// Records the batch still expects after the last one fed.
    remaining: u32,
    slot: u64,
    next_lsn: u64,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("committed", &self.committed)
            .field(
                "open",
                &self
                    .open
                    .as_ref()
                    .map(|open| (open.next_lsn, open.remaining)),
            )
            .finish()
    }
}

/// Where a verified log ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tail {
    /// The chain after the last complete batch.
    pub chain: Chain,
    /// Records of a batch that was begun and not finished, header included:
    /// a torn tail the writer has to cut before it appends.
    pub partial: u64,
}

impl Verifier {
    pub fn new(chain: Chain) -> Self {
        Self {
            committed: chain,
            open: None,
        }
    }

    pub fn feed(&mut self, bytes: &[u8]) -> Result<(), ChainError> {
        let record = decode(bytes)?;
        let expected = self
            .open
            .as_ref()
            .map_or(self.committed.next_lsn, |open| open.next_lsn);
        if record.kind == Kind::BatchHeader && self.open.is_some() {
            return Err(ChainError::Interrupted { lsn: record.lsn });
        }
        if record.lsn != expected {
            return Err(ChainError::Lsn {
                expected,
                found: record.lsn,
            });
        }
        // Every record of a batch names the batch before it.
        if record.prev_hash != self.committed.hash {
            return Err(ChainError::PrevHash {
                lsn: record.lsn,
                expected: self.committed.hash,
                found: record.prev_hash,
            });
        }
        let open = if record.kind == Kind::BatchHeader {
            if record.slot < self.committed.slot {
                return Err(ChainError::SlotBack {
                    lsn: record.lsn,
                    from: self.committed.slot,
                    to: record.slot,
                });
            }
            let mut hasher = Xxh3::new();
            hasher.update(bytes);
            self.open.insert(Open {
                hasher,
                remaining: BatchHeader::decode(record.body)?.records,
                slot: record.slot,
                next_lsn: record.lsn.saturating_add(1),
            })
        } else {
            let Some(open) = self.open.as_mut() else {
                return Err(ChainError::OutsideBatch { lsn: record.lsn });
            };
            if record.slot != open.slot {
                return Err(ChainError::Slot {
                    lsn: record.lsn,
                    expected: open.slot,
                    found: record.slot,
                });
            }
            open.hasher.update(bytes);
            open.remaining = open.remaining.saturating_sub(1);
            open.next_lsn = record.lsn.saturating_add(1);
            open
        };
        if open.remaining == 0 {
            self.committed = Chain {
                hash: open.hasher.digest128(),
                next_lsn: open.next_lsn,
                slot: open.slot,
            };
            self.open = None;
        }
        Ok(())
    }

    /// Ends the walk. A batch still open is a torn tail, not an error: the
    /// log keeps its complete batches and the writer cuts the rest.
    pub fn finish(self) -> Tail {
        Tail {
            chain: self.committed,
            partial: self.open.map_or(0, |open| {
                open.next_lsn.saturating_sub(self.committed.next_lsn)
            }),
        }
    }
}

#[cfg(test)]
mod stable {
    use super::Kind;

    /// D-141 A: the kinds `docs/configuration.md` declares stable, with
    /// their names and numbers, and the reserved ones, by number, are
    /// together exactly the ones a record may carry.
    #[test]
    fn the_stable_kinds_are_the_ones_decoded() {
        let doc = include_str!("../../../docs/configuration.md");
        let kinds = |marker: &str| -> Vec<(&str, u8)> {
            doc.split(&format!("<!-- {marker}:begin -->"))
                .nth(1)
                .and_then(|rest| rest.split(&format!("<!-- {marker}:end -->")).next())
                .expect("a kinds block")
                .lines()
                .filter_map(|l| l.split_once(" = "))
                .map(|(name, n)| (name.trim(), n.trim().parse().expect("a kind's number")))
                .collect()
        };
        let declared = kinds("stable-kinds");
        let reserved = kinds("reserved-kinds");
        for &(_, n) in &reserved {
            assert!(
                Kind::from_u8(n).is_some(),
                "reserved {n} decodes to no kind"
            );
        }
        for &(name, n) in &declared {
            let kind = Kind::from_u8(n).unwrap_or_else(|| panic!("{n} decodes to no kind"));
            assert_eq!(format!("{kind:?}"), name, "kind {n}");
        }
        let decoded = (0..=u8::MAX)
            .filter(|&n| Kind::from_u8(n).is_some())
            .count();
        assert_eq!(
            decoded,
            declared.len() + reserved.len(),
            "a kind neither stable nor reserved"
        );
    }
}
