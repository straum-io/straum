//! Metadata bytes per message, measured from the store's write counters
//! (`Stats::metadata_bytes`: WAL and topic index bytes minus inline blocks),
//! not estimated.
//!
//! For each write mode, size and batch size: 16 batches of new (unique)
//! payloads, then the same 16 batches again (every message deduplicated).
//! `block_bytes` is the compressed new objects: segment bytes plus inline
//! blocks in the WAL.
//!
//!   cargo run --release --example meta_bytes -- <empty scratch dir>

use straum_store::{Config, Message, Store, WriteMode};

const TOPIC: &[u8] = b"bench";
const BATCHES: usize = 16;
/// 2023-11-14T22:13:20Z.
const NOW_MS: u64 = 1_700_000_000_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = std::path::PathBuf::from(std::env::args().nth(1).ok_or("usage: meta_bytes <dir>")?);
    let modes = [
        ("sequential", WriteMode::Sequential),
        ("parallel", WriteMode::ParallelSync),
        ("inline", WriteMode::INLINE_SMALL),
    ];
    println!(
        "mode,msg_size,batch,kind,messages,wal_bytes,topic_bytes,block_bytes,meta_bytes_per_msg"
    );
    let rt = compio::runtime::Runtime::new()?;
    for (name, mode) in modes {
        for size in [100usize, 1024, 10240] {
            for batch in [1usize, 64] {
                let dir = root.join(format!("{name}-{size}-{batch}"));
                let _ = std::fs::remove_dir_all(&dir);
                let payloads: Vec<Vec<u8>> = (0..BATCHES * batch)
                    .map(|i| {
                        let mut p = vec![0xa5; size];
                        p[..8].copy_from_slice(&(i as u64).to_le_bytes());
                        p
                    })
                    .collect();
                rt.block_on(async {
                    let config = Config {
                        write_mode: mode,
                        ..Config::default()
                    };
                    let mut store = Store::open(&dir, config).await?;
                    for kind in ["new", "repeated"] {
                        let before = store.stats();
                        for (b, chunk) in payloads.chunks(batch).enumerate() {
                            let msgs: Vec<Message> = chunk
                                .iter()
                                .map(|p| Message {
                                    topic: TOPIC,
                                    payload: p,
                                })
                                .collect();
                            // Realistic Unix-millisecond timestamps: varint
                            // sizes depend on the value (fixed-width before
                            // the compact Publish format, so earlier results
                            // do not).
                            store.put_batch(&msgs, NOW_MS + b as u64).await?;
                        }
                        let after = store.stats();
                        let wal = after.wal_bytes - before.wal_bytes;
                        let topic = after.topic_bytes - before.topic_bytes;
                        let meta = after.metadata_bytes - before.metadata_bytes;
                        let blocks =
                            after.segment_bytes - before.segment_bytes + wal + topic - meta;
                        let n = payloads.len() as u64;
                        let meta = meta as f64 / n as f64;
                        println!(
                            "{name},{size},{batch},{kind},{n},{wal},{topic},{blocks},{meta:.3}"
                        );
                    }
                    Ok::<_, Box<dyn std::error::Error>>(())
                })?;
                std::fs::remove_dir_all(&dir)?;
            }
        }
    }
    Ok(())
}
