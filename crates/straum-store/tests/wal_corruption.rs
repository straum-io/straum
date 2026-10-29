//! WAL recovery with one frame per batch: only the last frame may be torn
//! and is truncated; damage followed by a valid frame is `WalCorrupted` and
//! leaves the file untouched; an old-format WAL is `UnsupportedFormat`.

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design

mod common;

use std::path::Path;

use common::{TempDir, copy_dir, wal_frame_starts};
use straum_store::{Config, Message, Store, StoreError};

const BATCHES: u8 = 5;

/// A WAL of 5 frames, one per batch (PutBlock + PutObject + Publish each).
async fn build(dir: &Path) {
    let mut store = Store::open(dir, Config::default()).await.unwrap();
    for i in 0..BATCHES {
        let payload = [i; 16];
        let batch = [Message {
            topic: b"t",
            payload: &payload,
            ..Default::default()
        }];
        store.put_batch(&batch, u64::from(i)).await.unwrap();
    }
}

/// Copies `src`, replaces its WAL with `wal`, opens it. Returns the result
/// and the WAL bytes after the open.
async fn open_with_wal(src: &Path, wal: &[u8]) -> (Result<Store, StoreError>, Vec<u8>, TempDir) {
    let work = TempDir::new("walframe");
    copy_dir(src, &work.0);
    let path = work.0.join("wal.log");
    std::fs::write(&path, wal).unwrap();
    let res = Store::open(&work.0, Config::default()).await;
    let after = std::fs::read(&path).unwrap();
    (res, after, work)
}

/// The store kept frames 0..BATCHES-1 and dropped the last one.
async fn assert_last_frame_dropped(store: &Store, what: &str) {
    let n = u64::from(BATCHES);
    assert_eq!(store.topic_range(b"t"), Some((0, n - 1)), "{what}");
    for i in 0..BATCHES - 1 {
        let got = store.read(b"t", u64::from(i)).await.unwrap();
        assert_eq!(got, Some(vec![i; 16]), "{what}: earlier frame {i}");
    }
}

#[compio::test]
async fn bit_flip_in_middle_frame_fails_and_leaves_wal_unchanged() {
    let src = TempDir::new("walmid");
    build(&src.0).await;
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    let starts = wal_frame_starts(&wal);
    assert_eq!(starts.len(), usize::from(BATCHES));
    let mid = starts.len() / 2;
    let (start, end) = (starts[mid], starts[mid + 1]);
    let mut checked = 0;
    for at in start..end {
        for bit in 0..8 {
            let mut bad = wal.clone();
            bad[at] ^= 1 << bit;
            let (res, after, _work) = open_with_wal(&src.0, &bad).await;
            match res {
                Err(StoreError::WalCorrupted { offset, next_frame }) => {
                    assert_eq!(offset, start as u64, "byte {at} bit {bit}");
                    assert_eq!(next_frame, end as u64, "byte {at} bit {bit}");
                }
                Err(e) => panic!("byte {at} bit {bit}: unexpected error {e}"),
                Ok(_) => panic!("byte {at} bit {bit}: opened a WAL with mid-log damage"),
            }
            assert_eq!(after, bad, "byte {at} bit {bit}: WAL modified");
            checked += 1;
        }
    }
    println!("middle frame: {checked} single-bit flips, all WalCorrupted, WAL unchanged");
}

#[compio::test]
async fn bit_flip_in_last_frame_drops_only_that_frame() {
    let src = TempDir::new("wallast");
    build(&src.0).await;
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    let last = *wal_frame_starts(&wal).last().unwrap();
    let mut checked = 0;
    for at in last..wal.len() {
        for bit in 0..8 {
            let mut bad = wal.clone();
            bad[at] ^= 1 << bit;
            let (res, after, _work) = open_with_wal(&src.0, &bad).await;
            let store = res.unwrap_or_else(|e| panic!("byte {at} bit {bit}: {e}"));
            assert_eq!(
                after,
                wal[..last],
                "byte {at} bit {bit}: truncated at the frame start"
            );
            assert_last_frame_dropped(&store, &format!("byte {at} bit {bit}")).await;
            checked += 1;
        }
    }
    println!("last frame: {checked} single-bit flips, all truncated as a torn tail");
}

/// The first half of the last frame never reached the disk, its second
/// half did. That frame was never acknowledged: a torn tail, not an error.
#[compio::test]
async fn last_frame_with_lost_first_half_is_a_torn_tail() {
    let src = TempDir::new("walhalf");
    build(&src.0).await;
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    let last = *wal_frame_starts(&wal).last().unwrap();
    let half = last + (wal.len() - last) / 2;
    for fill in [0x00, 0xff, 0x5a] {
        let mut bad = wal.clone();
        bad[last..half].fill(fill);
        let (res, after, _work) = open_with_wal(&src.0, &bad).await;
        let store = res.unwrap_or_else(|e| panic!("fill {fill:#x}: {e}"));
        assert_eq!(after, wal[..last], "fill {fill:#x}");
        assert_last_frame_dropped(&store, &format!("fill {fill:#x}")).await;
    }
}

#[compio::test]
async fn truncation_at_every_offset_of_last_frame() {
    let src = TempDir::new("walcut");
    build(&src.0).await;
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    let last = *wal_frame_starts(&wal).last().unwrap();
    for cut in last..wal.len() {
        let (res, after, _work) = open_with_wal(&src.0, &wal[..cut]).await;
        let store = res.unwrap_or_else(|e| panic!("cut {cut}: {e}"));
        assert_eq!(after, wal[..last], "cut {cut}");
        assert_last_frame_dropped(&store, &format!("cut {cut}")).await;
    }
}

#[compio::test]
async fn frame_sequence_gap_is_corruption() {
    let src = TempDir::new("walgap");
    build(&src.0).await;
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    let starts = wal_frame_starts(&wal);
    // Drop frame 2: frames 0, 1, 3, 4 remain, each with a valid CRC.
    let gap = [&wal[..starts[2]], &wal[starts[3]..]].concat();
    let (res, after, _work) = open_with_wal(&src.0, &gap).await;
    assert!(
        matches!(res, Err(StoreError::CorruptWal { offset, .. }) if offset == starts[2] as u64),
        "{:?}",
        res.err()
    );
    assert_eq!(after, gap);
    // Repeat frame 1.
    let repeat = [
        &wal[..starts[2]],
        &wal[starts[1]..starts[2]],
        &wal[starts[2]..],
    ]
    .concat();
    let (res, _, _work) = open_with_wal(&src.0, &repeat).await;
    assert!(matches!(res, Err(StoreError::CorruptWal { .. })));
}

#[compio::test]
async fn old_format_wal_is_unsupported() {
    let src = TempDir::new("walold");
    build(&src.0).await;
    // Phase 1 WAL: records with `len u32 | crc u32 | type u8`, no file header.
    let mut old = Vec::new();
    old.extend_from_slice(&10u32.to_le_bytes());
    old.extend_from_slice(&0xdead_beefu32.to_le_bytes());
    old.push(3);
    old.extend_from_slice(&[1, 0, b't', 0, 0, 0, 0, 0, 0, 0]);
    let wal = std::fs::read(src.0.join("wal.log")).unwrap();
    // The same header with another version and a valid header CRC.
    let with_version = |v: u32| {
        let mut w = wal.clone();
        w[8..12].copy_from_slice(&v.to_le_bytes());
        let crc = crc32c::crc32c(&w[..20]);
        w[20..24].copy_from_slice(&crc.to_le_bytes());
        w
    };
    let current = u32::from_le_bytes(wal[8..12].try_into().unwrap());
    let mut cases = vec![
        ("phase 1 records".to_owned(), old),
        ("empty".to_owned(), Vec::new()),
    ];
    // Every earlier version (docs/format.md lists what each lacked) and the
    // next one.
    for v in (1..current).chain([current + 1]) {
        cases.push((format!("version {v}"), with_version(v)));
    }
    for (what, bytes) in cases {
        let (res, after, _work) = open_with_wal(&src.0, &bytes).await;
        assert!(
            matches!(res, Err(StoreError::UnsupportedFormat)),
            "{what}: {:?}",
            res.err()
        );
        assert_eq!(after, bytes, "{what}: WAL modified");
    }
}

#[compio::test]
async fn batch_over_max_frame_is_rejected_before_io() {
    let dir = TempDir::new("walmax");
    let config = Config {
        // Bound: frame header 21, PutBlock 75, per message PutObject 73,
        // Publish 51 and PutTopic 22 (new topic, counted per message).
        // 10 messages: 1556 bytes; 2: 388.
        max_frame_bytes: 512,
        ..Config::default()
    };
    let mut store = Store::open(&dir.0, config).await.unwrap();
    let payloads: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 8]).collect();
    let batch: Vec<Message> = payloads
        .iter()
        .map(|p| Message {
            topic: b"t",
            payload: p,
            ..Default::default()
        })
        .collect();
    let wal_before = std::fs::read(dir.0.join("wal.log")).unwrap();
    let err = store.put_batch(&batch, 0).await.unwrap_err();
    assert!(matches!(err, StoreError::FrameTooLarge), "{err}");
    assert_eq!(store.stats(), Default::default(), "no I/O");
    assert_eq!(std::fs::read(dir.0.join("wal.log")).unwrap(), wal_before);
    // Not poisoned: a batch that fits still works.
    store.put_batch(&batch[..2], 0).await.unwrap();
}
