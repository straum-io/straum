//! On-disk framing of one WAL record.
//!
//! ```text
//! [ length: u32 LE ][ checksum: u32 LE ][ payload ]
//! ```
//!
//! Lengths read back from a file are as untrusted as lengths read from a
//! socket: a crash can leave anything in the tail. Same rules as the decoder.
#![deny(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    clippy::cast_possible_truncation
)]

use crate::crc::checksum;

/// Identifies the file and its format. The record body is the caller's, so a
/// change to what a record means is a change to this version too: an older
/// log is then refused rather than misread.
///
/// `STRAUMW3` added the logical allocation state to the header. `STRAUMW2`
/// logs carried no such state and their sequence can only be guessed at from
/// what compaction left behind, which is the guess that made identifiers
/// repeat (docs/replication.md §116), so they are refused rather than
/// migrated.
pub const MAGIC: [u8; 8] = *b"STRAUMW3";

pub const HEADER_BYTES: usize = 8;

/// What the next bytes of a WAL file turned out to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Frame {
    /// A complete record: `payload` within the input, `total` bytes consumed.
    Record {
        start: usize,
        end: usize,
        total: usize,
    },
    /// The file ends inside this record. Everything from here is the tail.
    Incomplete,
    /// The framing itself is impossible: a torn or corrupted tail.
    Invalid,
}

/// Appends one framed record to `out`.
pub fn encode(payload: &[u8], out: &mut Vec<u8>) {
    let length = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    out.extend_from_slice(&length.to_le_bytes());
    out.extend_from_slice(&checksum(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Counts the records in a run of already-encoded ones, or `None` if the
/// bytes are not exactly a whole number of them.
///
/// Checksums are deliberately **not** recomputed. These bytes arrive under a
/// checksum of their own that covered all of them at once
/// (docs/replication.md §114), and the per-record CRC32s inside are what the
/// next reader of this log will check — recomputing them here would be the
/// third pass over the same bytes. What is verified is the structure, because
/// appending a run that does not decompose cleanly would put a half record in
/// the middle of a log, where truncation cannot reach it.
pub fn count(bytes: &[u8], max_record_bytes: usize) -> Option<usize> {
    let mut at = 0usize;
    let mut records = 0usize;
    while at < bytes.len() {
        let header = at.checked_add(HEADER_BYTES)?;
        let length = bytes
            .get(at..header)
            .and_then(|slice| slice.get(..4))
            .and_then(|slice| <[u8; 4]>::try_from(slice).ok())
            .map(u32::from_le_bytes)?;
        let length = usize::try_from(length).ok()?;
        if length == 0 || length > max_record_bytes {
            return None;
        }
        at = header.checked_add(length)?;
        if at > bytes.len() {
            return None;
        }
        records = records.checked_add(1)?;
    }
    Some(records)
}

/// Reads the record at the start of `bytes`.
pub fn read(bytes: &[u8], max_record_bytes: usize) -> Frame {
    let Some(header) = bytes.get(..HEADER_BYTES) else {
        return Frame::Incomplete;
    };
    let (Some(length), Some(expected)) = (header.get(..4), header.get(4..8)) else {
        return Frame::Incomplete;
    };
    let (Ok(length), Ok(expected)) = (
        <[u8; 4]>::try_from(length).map(u32::from_le_bytes),
        <[u8; 4]>::try_from(expected).map(u32::from_le_bytes),
    ) else {
        return Frame::Incomplete;
    };

    let length = usize::try_from(length).unwrap_or(usize::MAX);
    // A zero-length record is not something the writer produces, and a length
    // past the configured maximum is a torn tail, not a huge record.
    if length == 0 || length > max_record_bytes {
        return Frame::Invalid;
    }

    let Some(end) = HEADER_BYTES.checked_add(length) else {
        return Frame::Invalid;
    };
    let Some(payload) = bytes.get(HEADER_BYTES..end) else {
        return Frame::Incomplete;
    };
    if checksum(payload) != expected {
        return Frame::Invalid;
    }

    Frame::Record {
        start: HEADER_BYTES,
        end,
        total: end,
    }
}

#[cfg(test)]
#[allow(
    clippy::indexing_slicing,
    reason = "a test that indexes out of bounds is a failing test"
)]
mod tests {
    use super::*;

    const MAX: usize = 1024;

    fn framed(payload: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        encode(payload, &mut out);
        out
    }

    #[test]
    fn round_trip() {
        let bytes = framed(b"hello");
        assert_eq!(bytes.len(), HEADER_BYTES + 5);
        assert_eq!(
            read(&bytes, MAX),
            Frame::Record {
                start: 8,
                end: 13,
                total: 13
            }
        );
    }

    #[test]
    fn every_truncation_is_incomplete_never_a_record() {
        let bytes = framed(b"hello");
        for cut in 0..bytes.len() {
            assert_eq!(read(&bytes[..cut], MAX), Frame::Incomplete, "cut at {cut}");
        }
    }

    #[test]
    fn a_flipped_payload_byte_is_invalid() {
        let mut bytes = framed(b"hello");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        assert_eq!(read(&bytes, MAX), Frame::Invalid);
    }

    #[test]
    fn counting_a_run_walks_framing_and_nothing_else() {
        let mut run = framed(b"one");
        run.extend_from_slice(&framed(b"two"));
        assert_eq!(count(&run, MAX), Some(2));
        assert_eq!(count(&[], MAX), Some(0));

        // Past the per-record limit: refused here rather than written and
        // found unreadable later.
        let big = framed(&[0u8; 64]);
        assert_eq!(count(&big, 64), Some(1));
        assert_eq!(count(&big, 63), None);

        // A flipped payload byte is not this function's business: the run
        // arrived under a checksum of its own, and the record's own CRC32 is
        // what the next reader of the log checks.
        let mut flipped = framed(b"one");
        let last = flipped.len() - 1;
        flipped[last] ^= 0x01;
        assert_eq!(count(&flipped, MAX), Some(1));
    }

    #[test]
    fn impossible_lengths_are_invalid() {
        let mut zero = framed(b"");
        zero[0] = 0;
        assert_eq!(read(&zero, MAX), Frame::Invalid);

        let mut huge = framed(b"hello");
        huge[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(read(&huge, MAX), Frame::Invalid);
    }
}
