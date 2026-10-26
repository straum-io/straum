//! End-to-end load on a running broker: `producers` threads, each with its
//! own connection and one Produce in flight at a time (durable acks), for
//! every (message size, producers, messages per Produce) of the matrix.
//!
//! ```text
//! straum-broker-bench --addr <host:port> [--out results.csv] [--label <text>]
//!     [--sizes 100,1024,10240] [--producers 1,4,16] [--per-produce 1,64]
//!     [--reps 1] [--rep-offset 0] [--seconds 5] [--max-bytes 1073741824]
//!     [--min-requests 100] [--no-header]
//! ```
//!
//! A run lasts `--seconds` or until `--max-bytes` of bodies are acknowledged,
//! whichever comes first, and at least `--min-requests` requests. Each run
//! uses a topic of its own. Bodies are bytes of a xorshift64* stream, unique,
//! so nothing is deduplicated. Ack latency is from sending a Produce to
//! receiving its ProduceOk; groups and fsyncs come from the broker's
//! BrokerStats before and after the run. One CSV row per run.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use straum_client::proto::Message;
use straum_client::{Client, ClientConfig, ProduceOptions};

const HEADER: &str = "label,msg_size,producers,per_produce,rep,requests,messages,elapsed_ns,msgs_per_s,mib_per_s,ack_p50_ns,ack_p99_ns,groups,groups_per_s,fsyncs,fsyncs_per_s,messages_per_group";

struct Args {
    addr: String,
    out: Option<String>,
    label: String,
    sizes: Vec<usize>,
    producers: Vec<usize>,
    per_produce: Vec<usize>,
    reps: usize,
    rep_offset: usize,
    seconds: f64,
    max_bytes: u64,
    min_requests: u64,
    header: bool,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        addr: String::new(),
        out: None,
        label: String::new(),
        sizes: vec![100, 1024, 10240],
        producers: vec![1, 4, 16],
        per_produce: vec![1, 64],
        reps: 1,
        rep_offset: 0,
        seconds: 5.0,
        max_bytes: 1 << 30,
        min_requests: 100,
        header: true,
    };
    let list = |v: &str| -> Result<Vec<usize>, String> {
        v.split(',')
            .map(|x| x.parse().map_err(|_| format!("bad number {x}")))
            .collect()
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--no-header" {
            a.header = false;
            continue;
        }
        let v = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--addr" => a.addr = v,
            "--out" => a.out = Some(v),
            "--label" => a.label = v,
            "--sizes" => a.sizes = list(&v)?,
            "--producers" => a.producers = list(&v)?,
            "--per-produce" => a.per_produce = list(&v)?,
            "--reps" => a.reps = v.parse().map_err(|_| "bad --reps")?,
            "--rep-offset" => a.rep_offset = v.parse().map_err(|_| "bad --rep-offset")?,
            "--seconds" => a.seconds = v.parse().map_err(|_| "bad --seconds")?,
            "--max-bytes" => a.max_bytes = v.parse().map_err(|_| "bad --max-bytes")?,
            "--min-requests" => a.min_requests = v.parse().map_err(|_| "bad --min-requests")?,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if a.addr.is_empty() {
        return Err("--addr is required".into());
    }
    if a.producers.contains(&0) || a.per_produce.contains(&0) || a.reps == 0 {
        return Err("producers, per-produce and reps must be positive".into());
    }
    Ok(a)
}

struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let v = self.next().to_le_bytes();
            chunk.copy_from_slice(&v[..chunk.len()]);
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    sorted
        .get(((sorted.len() as f64 - 1.0) * p).round() as usize)
        .copied()
        .unwrap_or(0)
}

struct Run {
    requests: u64,
    messages: u64,
    elapsed: Duration,
    lat: Vec<u64>,
    groups: u64,
    fsyncs: u64,
}

fn run(a: &Args, topic: &str, size: usize, producers: usize, per: usize) -> Result<Run, String> {
    let config = ClientConfig::new(a.addr.clone());
    let mut admin = Client::connect(config.clone()).map_err(|e| e.to_string())?;
    admin.create_topic(topic, 1, 0).map_err(|e| e.to_string())?;
    let before = admin.broker_stats().map_err(|e| e.to_string())?;
    // Signed: producers subtract without a compare loop; at or below zero
    // the budget is spent.
    let bytes_left = Arc::new(AtomicI64::new(
        i64::try_from(a.max_bytes).unwrap_or(i64::MAX),
    ));
    let requests_done = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs_f64(a.seconds);
    let start = Instant::now();
    let threads: Vec<_> = (0..producers)
        .map(|p| {
            let (config, topic) = (config.clone(), topic.to_owned());
            let (bytes_left, requests_done) = (Arc::clone(&bytes_left), Arc::clone(&requests_done));
            let min_requests = a.min_requests;
            std::thread::spawn(move || -> Result<(Vec<u64>, u64), String> {
                let mut c = Client::connect(config).map_err(|e| e.to_string())?;
                let mut rng = XorShift(0x9e37_79b9_7f4a_7c15 ^ (p as u64 + 1));
                let mut lat = Vec::new();
                let mut msgs = 0;
                for seq in 0.. {
                    let done = requests_done.load(Ordering::Relaxed);
                    let out_of_time =
                        Instant::now() >= deadline || bytes_left.load(Ordering::Relaxed) <= 0;
                    if out_of_time && done >= min_requests {
                        break;
                    }
                    let batch: Vec<Message> = (0..per)
                        .map(|_| {
                            let mut b = vec![0; size];
                            rng.fill(&mut b);
                            Message::body(b)
                        })
                        .collect();
                    let t = Instant::now();
                    let ok = c
                        .produce(
                            &topic,
                            0,
                            batch,
                            ProduceOptions {
                                producer_id: p as u64,
                                producer_seq: seq,
                                ..ProduceOptions::default()
                            },
                        )
                        .map_err(|e| e.to_string())?;
                    lat.push(t.elapsed().as_nanos() as u64);
                    msgs += u64::from(ok.count);
                    requests_done.fetch_add(1, Ordering::Relaxed);
                    bytes_left.fetch_sub((size * per) as i64, Ordering::Relaxed);
                }
                Ok((lat, msgs))
            })
        })
        .collect();
    let mut lat = Vec::new();
    let mut messages = 0;
    for t in threads {
        let (l, m) = t.join().map_err(|_| "producer panicked".to_owned())??;
        lat.extend(l);
        messages += m;
    }
    let elapsed = start.elapsed();
    let after = admin.broker_stats().map_err(|e| e.to_string())?;
    lat.sort_unstable();
    Ok(Run {
        requests: lat.len() as u64,
        messages,
        elapsed,
        lat,
        groups: after.groups_committed - before.groups_committed,
        fsyncs: after.fsyncs - before.fsyncs,
    })
}

fn main() {
    let a = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!(
                "{e}\nusage: straum-broker-bench --addr <host:port> [--out file.csv] [--label text] [--sizes 100,1024,10240] [--producers 1,4,16] [--per-produce 1,64] [--reps 1] [--rep-offset 0] [--seconds 5] [--max-bytes 1073741824] [--min-requests 100] [--no-header]"
            );
            std::process::exit(2);
        }
    };
    let mut out: Box<dyn Write> = match &a.out {
        Some(p) => match std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
        {
            Ok(f) => Box::new(f),
            Err(e) => {
                eprintln!("straum-broker-bench: {p}: {e}");
                std::process::exit(1);
            }
        },
        None => Box::new(std::io::stdout()),
    };
    if a.header {
        let _ = writeln!(out, "{HEADER}");
    }
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    for &size in &a.sizes {
        for &producers in &a.producers {
            for &per in &a.per_produce {
                for rep in a.rep_offset + 1..=a.rep_offset + a.reps {
                    let topic = format!("bench-{stamp}-{size}-{producers}-{per}-{rep}");
                    let r = match run(&a, &topic, size, producers, per) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("straum-broker-bench: {topic}: {e}");
                            std::process::exit(1);
                        }
                    };
                    let secs = r.elapsed.as_secs_f64();
                    let line = format!(
                        "{},{size},{producers},{per},{rep},{},{},{},{:.1},{:.2},{},{},{},{:.1},{},{:.1},{:.1}",
                        a.label,
                        r.requests,
                        r.messages,
                        r.elapsed.as_nanos(),
                        r.messages as f64 / secs,
                        (r.messages * size as u64) as f64 / secs / (1 << 20) as f64,
                        percentile(&r.lat, 0.50),
                        percentile(&r.lat, 0.99),
                        r.groups,
                        r.groups as f64 / secs,
                        r.fsyncs,
                        r.fsyncs as f64 / secs,
                        r.messages as f64 / r.groups.max(1) as f64,
                    );
                    let _ = writeln!(out, "{line}");
                    let _ = out.flush();
                    eprintln!(
                        "{} size={size} producers={producers} per={per} rep={rep}: {:.0} msg/s, p99 {} us",
                        a.label,
                        r.messages as f64 / secs,
                        percentile(&r.lat, 0.99) / 1000
                    );
                }
            }
        }
    }
}
