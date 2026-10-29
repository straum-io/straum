//! End to end: the broker binary as a child process, producing with durable
//! acks while it is killed with SIGKILL at a random moment, then
//! restarted. Every acknowledged message must be fetched with its seq,
//! timestamp, key, headers and body. `STRAUM_BROKER_CRASH_ITERATIONS`
//! (default 200) and `STRAUM_BROKER_CRASH_SEED` (default 24301) set the run;
//! the seed is printed.

#![allow(clippy::unwrap_used)]

mod common;

use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::TempDir;
use straum_client::proto::{Header, Message};
use straum_client::{Client, ClientConfig, ProduceOptions};

/// xorshift64*: the test's only source of randomness, from the seed.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

fn start(data: &std::path::Path) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_straum-broker"))
        .args([
            "--data-dir",
            &data.to_string_lossy(),
            "--listen",
            "127.0.0.1:0",
        ])
        .args(["--cluster-id", "crash", "--node-id", "1"])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut lines = BufReader::new(stderr).lines();
    let addr = loop {
        let Some(line) = lines.next() else {
            panic!("broker exited before listening")
        };
        let line = line.unwrap();
        if let Some(a) = line.strip_prefix("straum-broker: listening on ") {
            break a.to_owned();
        }
    };
    // Keep draining stderr so the child never blocks on it.
    std::thread::spawn(move || for _ in lines {});
    (child, addr)
}

/// Message `n` of the run: its key, headers and body follow from `n`.
fn message(n: u64) -> Message {
    let key = (!n.is_multiple_of(3)).then(|| format!("key-{}", n % 7).into_bytes());
    let headers = (0..n % 3)
        .map(|i| Header {
            name: format!("h{i}").into_bytes(),
            value: n.to_le_bytes()[..(i as usize + 1)].to_vec(),
        })
        .collect();
    // Repeated bodies exercise deduplication across crashes.
    let body = if n.is_multiple_of(5) {
        b"repeated body".to_vec()
    } else {
        format!("body {n} {}", "x".repeat((n % 64) as usize)).into_bytes()
    };
    Message { key, headers, body }
}

#[test]
#[ignore = "long: SIGKILL loop, STRAUM_BROKER_CRASH_ITERATIONS (default 200) iterations"]
fn every_acknowledged_message_survives_sigkill() {
    let iterations: u64 = std::env::var("STRAUM_BROKER_CRASH_ITERATIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200);
    let seed: u64 = std::env::var("STRAUM_BROKER_CRASH_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(24301);
    println!("durability: {iterations} iterations, seed {seed}");
    let dir = TempDir::new("durability");
    let data = dir.0.join("data");
    let mut rng = Rng(seed.max(1));
    // seq → (message, timestamp) of every acknowledged produce.
    let mut acked: Vec<Option<(Message, u64)>> = Vec::new();
    let mut next_n = 0u64;
    let mut kills_mid_produce = 0;
    for it in 0..iterations {
        let (mut child, addr) = start(&data);
        let mut config = ClientConfig::new(addr.clone());
        config.max_attempts = 1;
        config.request_timeout = Some(Duration::from_secs(10));
        let mut c = Client::connect(config.clone()).unwrap();
        if it == 0 {
            c.create_topic("crash", 1, 0).unwrap();
        }
        // Verify what earlier iterations acknowledged: at most 400 seqs,
        // the newest ones and a random older range.
        let total = acked.len() as u64;
        let from = total.saturating_sub(200);
        let older = rng.below(from.max(1));
        for range in [older..(older + 200).min(from), from..total] {
            let mut seq = range.start;
            while seq < range.end {
                let f = c.fetch("crash", 0, seq, 1 << 20, 0).unwrap();
                assert!(
                    !f.messages.is_empty(),
                    "iteration {it}: seq {seq} missing (next_seq {})",
                    f.next_seq
                );
                for m in &f.messages {
                    if m.seq >= range.end {
                        break;
                    }
                    if let Some((want, ts)) = &acked[m.seq as usize] {
                        assert_eq!(&m.message, want, "iteration {it}: seq {}", m.seq);
                        assert_eq!(
                            m.timestamp_ms, *ts,
                            "iteration {it}: seq {} timestamp",
                            m.seq
                        );
                    }
                    seq = m.seq + 1;
                }
            }
        }
        // Produce from two threads while a third kills the broker.
        let delay = Duration::from_millis(rng.below(40));
        let results = Arc::new(Mutex::new(Vec::new()));
        let mut producers = Vec::new();
        for t in 0..2u64 {
            let (config, results) = (config.clone(), Arc::clone(&results));
            let base = next_n + t * 1_000_000;
            let batch = 1 + rng.below(8);
            producers.push(std::thread::spawn(move || {
                let Ok(mut c) = Client::connect(config) else {
                    return;
                };
                for k in 0.. {
                    let ns: Vec<u64> = (0..batch).map(|j| base + k * batch + j).collect();
                    let msgs = ns.iter().map(|&n| message(n)).collect();
                    match c.produce(
                        "crash",
                        0,
                        msgs,
                        ProduceOptions {
                            producer_id: t,
                            producer_seq: k,
                            ..ProduceOptions::default()
                        },
                    ) {
                        Ok(ok) => results.lock().unwrap().push((ok, ns)),
                        Err(_) => return,
                    }
                }
            }));
        }
        std::thread::sleep(delay);
        child.kill().unwrap();
        child.wait().unwrap();
        for p in producers {
            p.join().unwrap();
        }
        let results = std::mem::take(&mut *results.lock().unwrap());
        if !results.is_empty() {
            kills_mid_produce += 1;
        }
        for (ok, ns) in results {
            assert_eq!(ok.count as usize, ns.len());
            for (i, n) in ns.into_iter().enumerate() {
                let seq = (ok.base_seq + i as u64) as usize;
                if acked.len() <= seq {
                    acked.resize(seq + 1, None);
                }
                acked[seq] = Some((message(n), ok.timestamp_ms));
            }
        }
        next_n += 2_000_000;
    }
    // Last restart: every acknowledged message, all of them.
    let (mut child, addr) = start(&data);
    let mut c = Client::connect(ClientConfig::new(addr)).unwrap();
    let mut seq = 0;
    let mut checked = 0;
    while (seq as usize) < acked.len() {
        let f = c.fetch("crash", 0, seq, 4 << 20, 0).unwrap();
        assert!(!f.messages.is_empty(), "seq {seq} missing at the end");
        for m in &f.messages {
            if let Some(Some((want, ts))) = acked.get(m.seq as usize) {
                assert_eq!(&m.message, want, "seq {}", m.seq);
                assert_eq!(m.timestamp_ms, *ts);
                checked += 1;
            }
            seq = m.seq + 1;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let acknowledged = acked.iter().filter(|a| a.is_some()).count();
    println!(
        "durability: seed {seed}: {acknowledged} acknowledged messages checked ({checked}), {kills_mid_produce} of {iterations} kills after at least one ack"
    );
    assert_eq!(checked, acknowledged);
    assert!(
        kills_mid_produce > iterations / 4,
        "most kills happen while producing"
    );
}
