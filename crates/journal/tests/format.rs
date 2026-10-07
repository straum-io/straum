//! Record v4 and the `prev_hash` chain (ADR-0008 step 1).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use journal::{
    Chain, ChainError, Entry, JournalError, Kind, Position, Verifier, decode, encode,
    encode_property,
};
use proptest::prelude::*;

fn publish(body: &[u8]) -> Entry<'_> {
    Entry {
        kind: Kind::Publish,
        properties: &[],
        body,
    }
}

/// `count` batches of `per` records each, as one flat list of record bytes.
fn log(count: usize, per: usize) -> (Vec<Vec<u8>>, Chain) {
    let mut chain = Chain::genesis();
    let mut out = Vec::new();
    for batch in 0..count {
        let bodies: Vec<Vec<u8>> = (0..per)
            .map(|i| format!("b{batch}r{i}").into_bytes())
            .collect();
        let entries: Vec<Entry<'_>> = bodies.iter().map(|b| publish(b)).collect();
        chain.batch(1, &entries, &mut out).unwrap();
    }
    (out, chain)
}

fn verify(records: &[Vec<u8>]) -> Result<journal::Tail, ChainError> {
    let mut verifier = Verifier::new(Chain::genesis());
    for record in records {
        verifier.feed(record)?;
    }
    Ok(verifier.finish())
}

proptest! {
    #[test]
    fn a_record_survives_the_round_trip(
        slot in any::<u64>(),
        lsn in any::<u64>(),
        prev_hash in any::<u128>(),
        props in proptest::collection::vec((any::<u8>(), proptest::collection::vec(any::<u8>(), 0..40)), 0..5),
        body in proptest::collection::vec(any::<u8>(), 0..200),
    ) {
        let mut properties = Vec::new();
        for (id, value) in &props {
            encode_property(*id, value, &mut properties).unwrap();
        }
        let mut bytes = Vec::new();
        encode(Kind::Publish, Position { slot, lsn, prev_hash }, &properties, &body, &mut bytes).unwrap();
        let record = decode(&bytes).unwrap();
        prop_assert_eq!(record.kind, Kind::Publish);
        prop_assert_eq!(record.slot, slot);
        prop_assert_eq!(record.lsn, lsn);
        prop_assert_eq!(record.prev_hash, prev_hash);
        prop_assert_eq!(record.body, &body[..]);
        let read: Vec<(u8, Vec<u8>)> = record
            .properties
            .iter()
            .map(|p| p.map(|(id, v)| (id, v.to_vec())))
            .collect::<Result<_, _>>()
            .unwrap();
        prop_assert_eq!(read, props);
    }

    /// Any one changed byte anywhere in the log is refused: in a record's own
    /// fields, or through the chain at the next batch.
    #[test]
    fn a_changed_byte_breaks_the_chain(record in 0usize..12, at in any::<usize>(), flip in 1u8..=255) {
        let (mut records, _) = log(3, 3);
        // A change shows in the record itself or in the next batch's prev_hash;
        // the last batch has no next one, so add a batch after it.
        let mut chain = verify(&records).unwrap().chain;
        chain.batch(1, &[publish(b"after")], &mut records).unwrap();
        let target = &mut records[record];
        let position = at % target.len();
        target[position] ^= flip;
        prop_assert!(verify(&records).is_err(), "record {record} byte {position} changed unnoticed");
    }
}

#[test]
fn a_whole_log_verifies_and_ends_where_the_writer_did() {
    let (records, chain) = log(5, 4);
    let tail = verify(&records).unwrap();
    assert_eq!(tail.partial, 0);
    assert_eq!(tail.chain, chain);
    assert_eq!(tail.chain.next_lsn(), 26, "5 headers and 20 records");
}

#[test]
fn a_dropped_record_is_refused() {
    let (mut records, _) = log(3, 3);
    records.remove(5);
    assert!(matches!(verify(&records), Err(ChainError::Lsn { .. })));
}

#[test]
fn batches_in_the_wrong_order_are_refused() {
    let (records, _) = log(3, 2);
    let mut swapped = records[3..6].to_vec();
    swapped.extend_from_slice(&records[..3]);
    swapped.extend_from_slice(&records[6..]);
    assert!(verify(&swapped).is_err());
}

/// Two logs that share a prefix and then differ never agree again: the
/// first batch after the fork names a different previous hash.
#[test]
fn a_foreign_history_is_refused_at_the_next_batch() {
    let mut ours = Chain::genesis();
    let mut theirs = Chain::genesis();
    let mut log_ours = Vec::new();
    let mut log_theirs = Vec::new();
    ours.batch(1, &[publish(b"same")], &mut log_ours).unwrap();
    theirs
        .batch(1, &[publish(b"same")], &mut log_theirs)
        .unwrap();
    ours.batch(1, &[publish(b"a")], &mut log_ours).unwrap();
    theirs.batch(1, &[publish(b"b")], &mut log_theirs).unwrap();
    ours.batch(1, &[publish(b"next")], &mut log_ours).unwrap();
    theirs
        .batch(1, &[publish(b"next")], &mut log_theirs)
        .unwrap();
    assert_ne!(ours.hash(), theirs.hash());

    // Their third batch after our first two.
    let mut mixed = log_ours[..4].to_vec();
    mixed.extend_from_slice(&log_theirs[4..]);
    assert!(matches!(verify(&mixed), Err(ChainError::PrevHash { .. })));
}

/// A batch cut short by a crash is a torn tail: the chain stands after the
/// last complete batch, and a writer resuming from there produces a log
/// that verifies.
#[test]
fn a_torn_batch_is_a_tail_and_writing_resumes_before_it() {
    let (records, _) = log(3, 4);
    let complete = verify(&records[..10]).unwrap().chain;
    for cut in 11..15 {
        let tail = verify(&records[..cut]).unwrap();
        assert_eq!(tail.partial as usize, cut - 10, "cut at {cut}");
        assert_eq!(tail.chain, complete);
    }
    let mut resumed = records[..10].to_vec();
    let mut chain = complete;
    chain
        .batch(1, &[publish(b"after the cut")], &mut resumed)
        .unwrap();
    assert_eq!(verify(&resumed).unwrap().partial, 0);
}

#[test]
fn lsn_continues_across_slots_and_never_goes_back() {
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    chain.batch(1, &[publish(b"x")], &mut records).unwrap();
    chain.batch(2, &[publish(b"y")], &mut records).unwrap();
    assert!(verify(&records).is_ok());
    assert_eq!(chain.next_lsn(), 5);

    // The writer refuses to go back by itself.
    let mut refused = Vec::new();
    assert!(matches!(
        chain.batch(1, &[publish(b"z")], &mut refused),
        Err(ChainError::SlotBack { .. })
    ));
    assert!(refused.is_empty());

    // A writer that forgot its slot is caught by the reader, also by one
    // resumed from the middle of the log rather than from genesis.
    let mut forgetful = Chain::resume(chain.hash(), chain.next_lsn() - 1, 0);
    let mut back = Vec::new();
    forgetful.batch(1, &[publish(b"z")], &mut back).unwrap();
    let mut whole = records.clone();
    whole.extend_from_slice(&back);
    assert!(matches!(verify(&whole), Err(ChainError::SlotBack { .. })));
    let mut resumed = Verifier::new(chain);
    assert!(matches!(
        resumed.feed(&back[0]),
        Err(ChainError::SlotBack { .. })
    ));
}

#[test]
fn malformed_records_are_refused() {
    let mut bytes = Vec::new();
    encode(Kind::Publish, AT, &[], b"x", &mut bytes).unwrap();
    assert_eq!(decode(&bytes[..10]), Err(JournalError::Truncated));
    let mut wrong_version = bytes.clone();
    wrong_version[0] = 3;
    assert_eq!(decode(&wrong_version), Err(JournalError::Version(3)));
    let mut unknown = bytes.clone();
    unknown[1] = 200;
    assert_eq!(decode(&unknown), Err(JournalError::Kind(200)));
    let mut long_props = bytes.clone();
    long_props[34..38].copy_from_slice(&1000u32.to_be_bytes());
    assert_eq!(decode(&long_props), Err(JournalError::Properties));
    let record = decode(&bytes).unwrap();
    assert_eq!(record.properties.iter().count(), 0);
}

#[test]
fn a_record_outside_a_batch_is_refused() {
    let mut bytes = Vec::new();
    encode(Kind::Publish, AT, &[], b"x", &mut bytes).unwrap();
    assert!(matches!(
        verify(&[bytes]),
        Err(ChainError::OutsideBatch { .. })
    ));
}

/// The chain is an on-disk format: the hash function must not change under
/// it. The first value is the published xxh3-128 digest of empty input
/// (the xxHash reference, seed 0); the second pins this crate's own batch
/// bytes, so a change to the record layout or to what is hashed shows here.
#[test]
fn the_hash_is_xxh3_128_and_the_format_is_pinned() {
    assert_eq!(
        xxhash_rust::xxh3::xxh3_128(b""),
        0x99aa_06d3_0147_98d8_6001_c324_468d_497f
    );
    let mut chain = Chain::genesis();
    chain.batch(1, &[publish(b"x")], &mut Vec::new()).unwrap();
    assert_eq!(chain.hash(), PINNED);
}
/// Recorded from this implementation on 2026-10-05 (batch hash on every record, D-56 A); not an external vector.
const PINNED: u128 = 0x54b8_079d_c207_a33a_eeb2_d76c_c3ff_6ee6;

const AT: Position = Position {
    slot: 1,
    lsn: 1,
    prev_hash: 0,
};

/// §2.2 in full (D-56 A): every record carries `prev_hash`, and it is the
/// hash of the batch before — the same value on every record of a batch.
#[test]
fn every_record_of_a_batch_names_the_batch_before() {
    let mut chain = Chain::genesis();
    let mut records = Vec::new();
    chain
        .batch(1, &[publish(b"a"), publish(b"b")], &mut records)
        .unwrap();
    let first = chain.hash();
    chain
        .batch(1, &[publish(b"c"), publish(b"d")], &mut records)
        .unwrap();
    let names: Vec<u128> = records
        .iter()
        .map(|r| decode(r).unwrap().prev_hash)
        .collect();
    assert_eq!(names, vec![0, 0, 0, first, first, first]);
}
