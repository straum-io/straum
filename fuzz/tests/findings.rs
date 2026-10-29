//! Open findings (README, "Findings"), each built through the store's
//! API, so they outlive format version changes that make a saved fuzz
//! input stale. Ignored while open.

use straum_fuzz::{check_dir, config, scratch};
use straum_store::{Message, Store, WriteMode};

/// F1: three ParallelSync batches, the blocks of the last two damaged in
/// their segment. The first open drops frame 2 (never acknowledged) and
/// checks frame 1's block for presence and bounds only; the reopen then
/// drops frame 1 as never acknowledged, though frame 2 followed it.
#[test]
#[ignore = "open finding F1 (fuzz/README.md, Findings)"]
fn f1_an_acknowledged_parallel_sync_frame_is_dropped_on_the_second_open() {
    let dir = scratch().with_extension("f1");
    let _ = std::fs::remove_dir_all(&dir);
    let seg = dir.join("seg-0000000000000000.dat");
    let mut ends = Vec::new();
    compio::runtime::Runtime::new().unwrap().block_on(async {
        let mut store = Store::open(&dir, config(WriteMode::ParallelSync))
            .await
            .unwrap();
        for b in 0..3u64 {
            let body = format!("batch {b} {}", "x".repeat(100)).into_bytes();
            let msg = Message {
                topic: b"t",
                payload: &body,
                ..Default::default()
            };
            store.put_batch(&[msg], b).await.unwrap();
            ends.push(std::fs::metadata(&seg).unwrap().len() as usize);
        }
    });
    let mut bytes = std::fs::read(&seg).unwrap();
    for b in 1..3 {
        bytes[(ends[b - 1] + ends[b]) / 2] ^= 0xff;
    }
    std::fs::write(&seg, bytes).unwrap();
    check_dir(&dir);
    let _ = std::fs::remove_dir_all(&dir);
}
