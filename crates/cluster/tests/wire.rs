//! Link frames: every frame survives encode and decode, whole or split; a
//! frame too long is refused from its header; malformed frames are refused.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use cluster::wire::{Frame, WireError, decode, encode};
use cluster::{Message, Proposal};
use journal::Kind;

fn frames() -> Vec<Frame> {
    vec![
        Frame::Hello {
            node: 2,
            slot: 1,
            limits: 0xdead_beef,
        },
        Frame::Message(Message::Append {
            slot: 1,
            records: vec![b"header".to_vec(), Vec::new(), vec![7; 300]],
            commit: 42,
        }),
        Frame::Message(Message::Append {
            slot: 1,
            records: Vec::new(),
            commit: 9,
        }),
        Frame::Message(Message::Ack {
            slot: 1,
            end: 10,
            durable: 8,
        }),
        Frame::Forward(Proposal {
            kind: Kind::Publish,
            properties: vec![1, 0, 1, 1],
            body: b"body".to_vec(),
        }),
    ]
}

#[test]
fn every_frame_survives_encode_and_decode_whole_or_split() {
    for frame in frames() {
        let mut bytes = Vec::new();
        encode(&frame, &mut bytes).unwrap();
        assert_eq!(
            decode(&bytes, 1 << 20).unwrap(),
            Some((frame.clone(), bytes.len()))
        );
        for cut in 0..bytes.len() {
            assert_eq!(
                decode(&bytes[..cut], 1 << 20).unwrap(),
                None,
                "cut at {cut}"
            );
        }
    }
    // Back to back in one buffer: one frame at a time.
    let mut bytes = Vec::new();
    for frame in frames() {
        encode(&frame, &mut bytes).unwrap();
    }
    let mut at = 0;
    let mut seen = Vec::new();
    while let Some((frame, used)) = decode(&bytes[at..], 1 << 20).unwrap() {
        seen.push(frame);
        at += used;
    }
    assert_eq!(seen, frames());
}

#[test]
fn a_frame_too_long_is_refused_from_its_header() {
    let header = 1_000_000u32.to_be_bytes();
    assert_eq!(decode(&header, 1 << 16), Err(WireError::TooLarge));
}

#[test]
fn malformed_frames_are_refused() {
    let frame = |body: &[u8]| {
        let mut out = u32::try_from(body.len()).unwrap().to_be_bytes().to_vec();
        out.extend_from_slice(body);
        out
    };
    assert_eq!(
        decode(&frame(&[9]), 1 << 20),
        Err(WireError::UnknownType(9))
    );
    assert_eq!(
        decode(&frame(&[3, 0, 0]), 1 << 20),
        Err(WireError::Truncated)
    );
    // An APPEND claiming more records than its bytes could hold.
    let mut append = vec![2];
    append.extend_from_slice(&[0; 16]);
    append.extend_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(decode(&frame(&append), 1 << 20), Err(WireError::Truncated));
    // An ACK with a byte too many.
    let mut ack = vec![3];
    ack.extend_from_slice(&[0; 25]);
    assert_eq!(decode(&frame(&ack), 1 << 20), Err(WireError::Trailing));
    // A FORWARD of a kind no version knows.
    assert_eq!(
        decode(&frame(&[4, 200, 0, 0, 0, 0]), 1 << 20),
        Err(WireError::UnknownKind(200))
    );
}
