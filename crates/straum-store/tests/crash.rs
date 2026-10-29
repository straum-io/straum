//! Crash test: a writer runs in a child process (this test binary re-executed)
//! and is killed with SIGKILL at a random moment. After each kill the parent
//! reopens the store and checks:
//!
//! * every acknowledged publish is readable with the right bytes
//! * no WAL reference points to a missing object (recovery and reads verify)
//! * no topic_seq is assigned twice
//! * refcounts equal the live topic entries pointing at each object
//!
//! One test per write mode, each with its own directory and the same seed.
//! Iterations: `STRAUM_STORE_CRASH_ITERATIONS` (default 200). Seed:
//! `STRAUM_STORE_CRASH_SEED` (default 24301).

#![allow(clippy::unwrap_used)] // test crate: helpers outside #[test] fns panic on failure by design
mod common;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::time::Duration;

use common::{Rng, TempDir};
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

const CHILD_DIR: &str = "STRAUM_STORE_CRASH_CHILD_DIR";
const CHILD_SEED: &str = "STRAUM_STORE_CRASH_CHILD_SEED";
const CHILD_MODE: &str = "STRAUM_STORE_CRASH_CHILD_MODE";

fn mode(name: &str) -> WriteMode {
    match name {
        "sequential" => WriteMode::Sequential,
        "parallel" => WriteMode::ParallelSync,
        // Applies to a batch's compressed block (payloads up to ~3 KiB, up
        // to 16 a batch): a mix of inline and segment blocks.
        "inline" => WriteMode::InlineSmall { threshold: 1024 },
        other => panic!("unknown write mode {other}"),
    }
}

fn config(mode_name: &str) -> Config {
    Config {
        segment_max_bytes: 4096,
        write_mode: mode(mode_name),
        // Index segments rotate and seek pages fill often, so kills also
        // land around both.
        index_segment_max_bytes: 4096,
        seek_page_entries: 2,
        // The dictionary is trained early in the run, so kills also land
        // before, during and after training.
        dict_train_bytes: 32 << 10,
        // Batches of up to 16 payloads of up to ~3 KiB: several blocks per
        // frame, so kills also land between them.
        max_block_raw_bytes: 4096,
        // WAL files rotate every few frames, so kills also land around a
        // file's creation and recovery spans many files.
        wal_file_max_bytes: 16 << 10,
        // Reads and compaction reopen segments and WAL files often.
        data_open_files: 2,
        ..Config::default()
    }
}
/// Released and compacted by the child; acknowledgements are not tracked.
const SCRATCH: &[u8] = b"scratch";
const TRACKED: [&[u8]; 2] = [b"orders", b"events"];

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

/// The key of a published payload: one for payloads of even length, so a
/// key needs no line of its own in the ACK protocol; its one header holds
/// the same bytes.
fn key_of(payload: &[u8]) -> Option<Vec<u8>> {
    payload
        .len()
        .is_multiple_of(2)
        .then(|| format!("k{}", payload.len()).into_bytes())
}

/// The writer. A no-op unless started by `sigkill_at_random_points`.
#[test]
fn crash_child() {
    let Ok(dir) = std::env::var(CHILD_DIR) else {
        return;
    };
    let mode_name = std::env::var(CHILD_MODE).unwrap();
    let mut rng = Rng(env_u64(CHILD_SEED, 1) | 1);
    let shared: Vec<Vec<u8>> = (0..8u8).map(|i| vec![i; 1 + 97 * usize::from(i)]).collect();
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let mut store = Store::open(&dir, config(&mode_name)).await.unwrap();
        let mut out = std::io::stdout().lock();
        let mut round = 0u64;
        loop {
            round += 1;
            let mut payloads = Vec::new();
            let mut topics = Vec::new();
            for j in 0..1 + rng.below(16) {
                let topic = match rng.below(3) {
                    0 => SCRATCH,
                    t => TRACKED[t as usize - 1],
                };
                let payload = if rng.below(3) == 0 {
                    shared[rng.below(8) as usize].clone()
                } else {
                    let mut p = format!("{}-{round}-{j}-", rng.next()).into_bytes();
                    p.resize(p.len() + rng.below(3000) as usize, b'x');
                    p
                };
                topics.push(topic);
                payloads.push(payload);
            }
            let keys: Vec<Option<Vec<u8>>> = payloads.iter().map(|p| key_of(p)).collect();
            let headers: Vec<[(&[u8], &[u8]); 1]> = keys
                .iter()
                .map(|k| [(&b"h"[..], k.as_deref().unwrap_or_default())])
                .collect();
            let batch: Vec<Message> = topics
                .iter()
                .zip(&payloads)
                .zip(keys.iter().zip(&headers))
                .map(|((&topic, payload), (key, headers))| Message {
                    topic,
                    payload,
                    key: key.as_deref(),
                    headers: if key.is_some() { headers } else { &[] },
                })
                .collect();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            let seqs = store.put_batch(&batch, now).await.unwrap();
            // Printed only after put_batch returned: an ACK line is a
            // durability promise. " END" marks a line not cut by the kill.
            for ((topic, payload), seq) in topics.iter().zip(&payloads).zip(seqs) {
                if *topic != SCRATCH {
                    let topic = std::str::from_utf8(topic).unwrap();
                    writeln!(out, "ACK {topic} {seq} {} END", hex(payload)).unwrap();
                }
            }
            out.flush().unwrap();
            if round % 2 == 1 {
                store.release_before(SCRATCH, now).await.unwrap();
            }
            if round % 5 == 4 {
                store.checkpoint().await.unwrap();
            }
            if round % 3 == 2 {
                let stats = store.compact().await.unwrap();
                writeln!(out, "COMPACTED {} END", stats.segments_rewritten).unwrap();
                out.flush().unwrap();
            }
        }
    });
}

#[test]
#[ignore = "long: SIGKILL loop, STRAUM_STORE_CRASH_ITERATIONS (default 200) iterations"]
fn sigkill_sequential() {
    sigkill_at_random_points("sequential");
}

#[test]
#[ignore = "long: SIGKILL loop, STRAUM_STORE_CRASH_ITERATIONS (default 200) iterations"]
fn sigkill_parallel_sync() {
    sigkill_at_random_points("parallel");
}

#[test]
#[ignore = "long: SIGKILL loop, STRAUM_STORE_CRASH_ITERATIONS (default 200) iterations"]
fn sigkill_inline_small() {
    sigkill_at_random_points("inline");
}

fn sigkill_at_random_points(mode_name: &str) {
    if std::env::var(CHILD_DIR).is_ok() {
        return;
    }
    let iterations = env_u64("STRAUM_STORE_CRASH_ITERATIONS", 200);
    let seed = env_u64("STRAUM_STORE_CRASH_SEED", 0x5eed);
    println!("crash test {mode_name}: {iterations} iterations, seed {seed}");
    let mut rng = Rng(seed | 1);
    let dir = TempDir::new(&format!("crash-{mode_name}"));
    let exe = std::env::current_exe().unwrap();
    let mut acked: HashMap<(String, u64), Vec<u8>> = HashMap::new();
    let mut killed_mid_run = 0;
    let mut compactions = 0;
    let mut dictionary = None;

    for i in 0..iterations {
        let mut child = Command::new(&exe)
            .args(["--exact", "crash_child", "--nocapture", "--test-threads=1"])
            .env(CHILD_DIR, &dir.0)
            .env(CHILD_SEED, rng.next().to_string())
            .env(CHILD_MODE, mode_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let reader = std::thread::spawn(move || {
            BufReader::new(stdout)
                .lines()
                .map_while(Result::ok)
                .collect::<Vec<_>>()
        });
        std::thread::sleep(Duration::from_millis(10 + rng.below(150)));
        // SIGKILL on Unix.
        child.kill().unwrap();
        let status = child.wait().unwrap();
        let lines = reader.join().unwrap();
        if status.code().is_some() {
            let mut err = String::new();
            std::io::Read::read_to_string(&mut child.stderr.take().unwrap(), &mut err).unwrap();
            panic!("iteration {i}: writer exited on its own ({status}) before the kill:\n{err}");
        }
        killed_mid_run += 1;

        compactions += lines.iter().filter(|l| l.starts_with("COMPACTED ")).count();
        for line in &lines {
            let Some(rest) = line
                .strip_prefix("ACK ")
                .and_then(|l| l.strip_suffix(" END"))
            else {
                continue;
            };
            let mut parts = rest.split(' ');
            let (Some(topic), Some(seq), Some(payload)) =
                (parts.next(), parts.next(), parts.next())
            else {
                panic!("malformed ack line: {line}");
            };
            let key = (topic.to_owned(), seq.parse().unwrap());
            let payload = unhex(payload).unwrap();
            if let Some(prev) = acked.insert(key.clone(), payload.clone()) {
                assert_eq!(
                    prev, payload,
                    "iteration {i}: topic_seq {key:?} acknowledged twice"
                );
            }
        }

        dictionary = verify(&dir.0, &acked, i, mode_name);
    }
    println!(
        "crash test {mode_name}: {killed_mid_run} kills, {} acknowledged publishes verified, {compactions} completed compactions, dictionary {dictionary:?}",
        acked.len()
    );
    assert_eq!(killed_mid_run, iterations);
}

/// Returns the store's dictionary length (`Store::dictionary_bytes`).
fn verify(
    dir: &std::path::Path,
    acked: &HashMap<(String, u64), Vec<u8>>,
    i: u64,
    mode_name: &str,
) -> Option<usize> {
    let rt = compio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let store = match Store::open(dir, config(mode_name)).await {
            Ok(store) => store,
            Err(e) => panic!("iteration {i}: recovery failed: {e}"),
        };
        // In topic order, so consecutive reads share decompressed blocks.
        let mut sorted: Vec<_> = acked.iter().collect();
        sorted.sort_unstable_by(|a, b| a.0.cmp(b.0));
        for ((topic, seq), payload) in sorted {
            let got = store.read_entry(topic.as_bytes(), *seq).await.unwrap();
            let key = key_of(payload);
            let headers = match &key {
                Some(k) => vec![(b"h".to_vec(), k.clone())],
                None => vec![],
            };
            assert_eq!(
                got.map(|e| (e.payload, e.key, e.headers)),
                Some((payload.clone(), key, headers)),
                "iteration {i}: acked {topic}/{seq}"
            );
        }
        // Every live entry resolves to an existing object with matching
        // bytes, and refcounts equal the live entries per object.
        let mut counts: HashMap<ObjectId, u64> = HashMap::new();
        let topics: Vec<Vec<u8>> = store.topics().map(<[u8]>::to_vec).collect();
        for topic in &topics {
            let (first, next) = store.topic_range(topic).unwrap();
            for seq in first..next {
                let id = store.entry(topic, seq).await.unwrap().unwrap();
                let bytes = store.read(topic, seq).await.unwrap().unwrap();
                assert_eq!(ObjectId::of(&bytes), id);
                *counts.entry(id).or_default() += 1;
            }
        }
        for (id, n) in &counts {
            assert_eq!(store.refcount(id), Some(*n), "iteration {i}: refcount");
        }
        store.dictionary_bytes()
    })
}
