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

/// Identifies the file and its format. A format change changes the version.
pub const MAGIC: [u8; 8] = *b"STRAUMW1";

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
    fn impossible_lengths_are_invalid() {
        let mut zero = framed(b"");
        zero[0] = 0;
        assert_eq!(read(&zero, MAX), Frame::Invalid);

        let mut huge = framed(b"hello");
        huge[0..4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(read(&huge, MAX), Frame::Invalid);
    }
}
