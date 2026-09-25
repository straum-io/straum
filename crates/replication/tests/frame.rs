//! The wire format, including every way a peer can get it wrong.

use replication::{DURABLE_BYTES, Limits, Message, RECORDS_HEADER_BYTES, ReplicationError, decode};

fn records(previous: u64, last: u64, batch: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    replication::encode_records_header(previous, last, batch.len(), &mut out);
    out.extend_from_slice(batch);
    out
}

fn durable(sequence: u64) -> Vec<u8> {
    let mut out = Vec::new();
    replication::encode_durable(sequence, &mut out);
    out
}

#[test]
fn a_batch_survives_the_round_trip_byte_for_byte() {
    let batch = b"whatever the WAL wrote, unchanged";
    let frame = records(100, 104, batch);
    assert_eq!(frame.len(), RECORDS_HEADER_BYTES + batch.len());

    let (message, consumed) = decode(&frame, Limits::default()).unwrap().unwrap();
    assert_eq!(consumed, frame.len());
    let Message::Records(held) = message else {
        panic!("not a records frame");
    };
    assert_eq!(held.previous, 100);
    assert_eq!(held.last, 104);
    assert_eq!(held.batch, batch);
    // The WAL advances its sequence once per staged record, so the range is
    // the count and nothing has to parse the batch to learn it.
    assert_eq!(held.records(), 4);
}

#[test]
fn an_acknowledgement_survives_the_round_trip() {
    let frame = durable(5000);
    assert_eq!(frame.len(), DURABLE_BYTES);

    let (message, consumed) = decode(&frame, Limits::default()).unwrap().unwrap();
    assert_eq!(consumed, DURABLE_BYTES);
    assert_eq!(message, Message::Durable(5000));
}

#[test]
fn every_truncation_asks_for_more_and_never_yields_a_message() {
    // The whole point of the split between network reads and durability: a
    // frame that arrived in pieces is not a frame yet.
    for frame in [records(7, 9, b"two records worth of bytes"), durable(9)] {
        for cut in 0..frame.len() {
            assert_eq!(
                decode(frame.get(..cut).unwrap(), Limits::default()),
                Ok(None),
                "a {cut}-byte prefix was treated as complete"
            );
        }
    }
}

#[test]
fn frames_are_read_one_at_a_time_out_of_a_stream() {
    let mut stream = records(1, 3, b"first");
    stream.extend_from_slice(&records(3, 4, b"second"));
    stream.extend_from_slice(&durable(4));

    let mut at = 0;
    let mut seen = 0;
    while let Some((_, consumed)) =
        decode(stream.get(at..).unwrap(), Limits::default()).expect("the stream is well formed")
    {
        seen += 1;
        at += consumed;
    }

    assert_eq!(at, stream.len());
    assert_eq!(seen, 3);
}

#[test]
fn a_flipped_header_byte_is_refused_rather_than_believed() {
    // The header is the part nothing else protects: the records inside the
    // batch carry their own checksums, the fields around them do not.
    let clean = records(10, 12, b"payload");
    for at in 0..RECORDS_HEADER_BYTES {
        let mut dirty = clean.clone();
        *dirty.get_mut(at).unwrap() ^= 0x01;
        match decode(&dirty, Limits::default()) {
            Err(ReplicationError::Checksum) => {}
            // Flipping a bit of the kind byte makes it a different frame
            // kind, which is refused just as hard.
            Err(ReplicationError::UnknownKind(_)) if at == 0 => {}
            other => panic!("byte {at} flipped and decode returned {other:?}"),
        }
    }
}

#[test]
fn a_flipped_acknowledgement_byte_is_refused() {
    let clean = durable(4242);
    for at in 1..DURABLE_BYTES {
        let mut dirty = clean.clone();
        *dirty.get_mut(at).unwrap() ^= 0x01;
        assert_eq!(
            decode(&dirty, Limits::default()),
            Err(ReplicationError::Checksum),
            "byte {at}"
        );
    }
}

#[test]
fn an_unknown_kind_ends_the_connection_rather_than_being_skipped() {
    assert_eq!(
        decode(&[9, 0, 0, 0], Limits::default()),
        Err(ReplicationError::UnknownKind(9))
    );
}

#[test]
fn a_frame_larger_than_the_limit_is_refused_before_it_is_buffered() {
    let limits = Limits {
        max_frame_bytes: 1024,
    };
    // The bytes themselves are never supplied: the point is that the reader
    // is not asked to collect them first.
    let mut header = Vec::new();
    replication::encode_records_header(0, 1, 4096, &mut header);

    assert_eq!(
        decode(&header, limits),
        Err(ReplicationError::FrameTooLarge {
            bytes: 4096,
            limit: 1024
        })
    );
}

#[test]
fn a_frame_that_covers_nothing_is_refused() {
    // A commit that wrote no records does not exist, and one frame is one
    // commit.
    let mut empty_range = Vec::new();
    replication::encode_records_header(5, 5, 8, &mut empty_range);
    empty_range.extend_from_slice(&[0; 8]);
    assert_eq!(
        decode(&empty_range, Limits::default()),
        Err(ReplicationError::Empty)
    );

    let mut backwards = Vec::new();
    replication::encode_records_header(9, 4, 8, &mut backwards);
    backwards.extend_from_slice(&[0; 8]);
    assert_eq!(
        decode(&backwards, Limits::default()),
        Err(ReplicationError::Empty)
    );

    let mut no_bytes = Vec::new();
    replication::encode_records_header(1, 2, 0, &mut no_bytes);
    assert_eq!(
        decode(&no_bytes, Limits::default()),
        Err(ReplicationError::Empty)
    );
}

#[test]
fn nothing_at_all_asks_for_more() {
    assert_eq!(decode(&[], Limits::default()), Ok(None));
}

mod properties {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn any_batch_comes_back_unchanged(
            previous in 0u64..u64::MAX / 2,
            span in 1u64..4096,
            batch in proptest::collection::vec(any::<u8>(), 1..2048),
        ) {
            let last = previous + span;
            let frame = records(previous, last, &batch);
            let (message, consumed) = decode(&frame, Limits::default()).unwrap().unwrap();

            prop_assert_eq!(consumed, frame.len());
            prop_assert_eq!(message, Message::Records(replication::Records {
                previous,
                last,
                batch: &batch,
            }));
        }

        /// Whatever a peer sends, the decoder either asks for more, hands back
        /// a message, or names a violation. It never panics and never reads
        /// past what it was given.
        #[test]
        fn arbitrary_bytes_never_panic(input in proptest::collection::vec(any::<u8>(), 0..512)) {
            let _ = decode(&input, Limits::default());
        }
    }
}
