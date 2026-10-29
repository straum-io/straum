//! A publication's key and headers: stored with its Publish record,
//! returned by `read_entry`, kept across restart, release and compaction,
//! and independent of the object, which is the payload alone.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use common::TempDir;
use straum_store::{Config, Entry, Message, ObjectId, Store, StoreError, WriteMode};

const MODES: [WriteMode; 3] = [
    WriteMode::Sequential,
    WriteMode::ParallelSync,
    WriteMode::INLINE_SMALL,
];

fn entry(ts: u64, key: Option<&[u8]>, headers: &[(&[u8], &[u8])], payload: &[u8]) -> Entry {
    Entry {
        timestamp: ts,
        key: key.map(<[u8]>::to_vec),
        headers: headers
            .iter()
            .map(|(n, v)| (n.to_vec(), v.to_vec()))
            .collect(),
        payload: payload.to_vec(),
    }
}

#[compio::test]
async fn keys_and_headers_round_trip_and_survive_restart() {
    for mode in MODES {
        let dir = TempDir::new("pub-roundtrip");
        let config = Config {
            write_mode: mode,
            ..Config::default()
        };
        let big = vec![b'v'; 5000];
        let headers: [(&[u8], &[u8]); 3] =
            [(b"trace", b"abc"), (b"trace", b""), (&[0, 0xff], &big)];
        let batch = [
            Message {
                topic: b"t",
                payload: b"plain",
                ..Message::default()
            },
            Message {
                topic: b"t",
                payload: b"empty key",
                key: Some(b""),
                ..Message::default()
            },
            Message {
                topic: b"t",
                payload: b"keyed",
                key: Some(b"user-42"),
                headers: &headers,
            },
            Message {
                topic: b"t",
                payload: b"headers only",
                headers: &headers[..1],
                ..Message::default()
            },
        ];
        let want = [
            entry(5, None, &[], b"plain"),
            entry(5, Some(b""), &[], b"empty key"),
            entry(5, Some(b"user-42"), &headers, b"keyed"),
            entry(5, None, &headers[..1], b"headers only"),
        ];
        let mut store = Store::open(&dir.0, config).await.unwrap();
        assert_eq!(store.put_batch(&batch, 5).await.unwrap(), [0, 1, 2, 3]);
        for (seq, w) in want.iter().enumerate() {
            let got = store.read_entry(b"t", seq as u64).await.unwrap();
            assert_eq!(got.as_ref(), Some(w), "{mode:?} seq {seq}");
        }
        assert_eq!(store.read_entry(b"t", 4).await.unwrap(), None);
        assert_eq!(store.read_entry(b"x", 0).await.unwrap(), None);
        drop(store);
        let store = Store::open(&dir.0, config).await.unwrap();
        assert!(store.recovery_report().is_empty(), "{mode:?}");
        for (seq, w) in want.iter().enumerate() {
            let got = store.read_entry(b"t", seq as u64).await.unwrap();
            assert_eq!(got.as_ref(), Some(w), "{mode:?} after restart, seq {seq}");
        }
    }
}

/// The object id is the payload's hash only: one object, each publication
/// with its own key and headers.
#[compio::test]
async fn the_same_payload_with_other_keys_is_one_object() {
    for mode in MODES {
        let dir = TempDir::new("pub-dedup");
        let config = Config {
            write_mode: mode,
            ..Config::default()
        };
        let mut store = Store::open(&dir.0, config).await.unwrap();
        let h: [(&[u8], &[u8]); 1] = [(b"h", b"1")];
        store
            .put_batch(
                &[Message {
                    topic: b"t",
                    payload: b"same",
                    key: Some(b"a"),
                    ..Message::default()
                }],
                1,
            )
            .await
            .unwrap();
        store
            .put_batch(
                &[Message {
                    topic: b"t",
                    payload: b"same",
                    key: Some(b"b"),
                    headers: &h,
                }],
                2,
            )
            .await
            .unwrap();
        assert_eq!(store.object_count(), 1, "{mode:?}");
        assert!(store.contains(&ObjectId::of(b"same")));
        assert_eq!(store.stats().messages_deduplicated, 1);
        assert_eq!(
            store.read_entry(b"t", 0).await.unwrap(),
            Some(entry(1, Some(b"a"), &[], b"same"))
        );
        assert_eq!(
            store.read_entry(b"t", 1).await.unwrap(),
            Some(entry(2, Some(b"b"), &h, b"same"))
        );
    }
}

#[compio::test]
async fn attributes_survive_release_and_compaction() {
    let dir = TempDir::new("pub-compact");
    let config = Config {
        segment_max_bytes: 64,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    let keys: Vec<Vec<u8>> = (0..6u8).map(|i| vec![b'k', i]).collect();
    let payloads: Vec<Vec<u8>> = (0..6u8).map(|i| vec![i; 40]).collect();
    for i in 0..6 {
        store
            .put_batch(
                &[Message {
                    topic: b"t",
                    payload: &payloads[i],
                    key: Some(&keys[i]),
                    ..Message::default()
                }],
                i as u64,
            )
            .await
            .unwrap();
    }
    assert_eq!(store.release_before(b"t", 3).await.unwrap(), 3);
    assert!(store.compact().await.unwrap().objects_dropped >= 3);
    for round in 0..2 {
        for i in 3..6 {
            let got = store.read_entry(b"t", i as u64).await.unwrap().unwrap();
            assert_eq!(got.key.as_deref(), Some(&keys[i][..]), "round {round}");
            assert_eq!(got.payload, payloads[i]);
        }
        assert_eq!(store.read_entry(b"t", 2).await.unwrap(), None, "released");
        drop(store);
        store = Store::open(&dir.0, config).await.unwrap();
    }
}

#[compio::test]
async fn the_frame_bound_counts_attributes_and_is_additive() {
    let dir = TempDir::new("pub-bound");
    let config = Config {
        max_frame_bytes: 2048,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    let key = vec![b'k'; 4000];
    let big = Message {
        topic: b"t",
        payload: b"x",
        key: Some(&key),
        ..Message::default()
    };
    let small = Message {
        topic: b"t",
        payload: b"x",
        ..Message::default()
    };
    assert!(store.batch_frame_bytes(&[big]) > 4000);
    let err = store.put_batch(&[big], 0).await.unwrap_err();
    assert!(matches!(err, StoreError::FrameTooLarge), "{err}");
    assert_eq!(store.stats().wal_bytes, 0, "refused before any I/O");
    let empty = store.batch_frame_bytes(&[]);
    for batch in [vec![small, small], vec![small, big, small]] {
        let parts: usize = batch
            .iter()
            .map(|m| store.batch_frame_bytes(std::slice::from_ref(m)) - empty)
            .sum();
        assert_eq!(store.batch_frame_bytes(&batch), empty + parts);
    }
    assert_eq!(store.max_frame_bytes(), 2048);
    store.put_batch(&[small], 0).await.unwrap();
}

#[compio::test]
async fn topic_stats_follow_publish_and_release() {
    let dir = TempDir::new("pub-stats");
    let mut store = Store::open(&dir.0, Config::default()).await.unwrap();
    assert_eq!(store.topic_stats(b"t"), None);
    let msgs: Vec<Message> = (0..3)
        .map(|_| Message {
            topic: b"t",
            payload: b"p",
            ..Message::default()
        })
        .collect();
    store.put_batch(&msgs, 1).await.unwrap();
    store.release_before(b"t", 2).await.unwrap();
    let s = store.topic_stats(b"t").unwrap();
    assert_eq!((s.first_live, s.next_seq), (3, 3));
    assert!(s.index_bytes > 0);
    store.put_batch(&msgs[..1], 5).await.unwrap();
    assert_eq!(store.topic_stats(b"t").unwrap().next_seq, 4);
}
