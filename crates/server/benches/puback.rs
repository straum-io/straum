//! Publish to PUBACK over MQTT, through three real server processes
//! (D-124): what a client sees, and what the servers spend for it.
//!
//! Three `straum-server` processes on loopback form a cluster. For
//! `STRAUM_BENCH_SECONDS` after a `STRAUM_BENCH_WARMUP` warm-up:
//!
//! * `STRAUM_BENCH_PUBLISHERS` MQTT 3.1.1 clients, spread over the nodes,
//!   each keep `STRAUM_BENCH_WINDOW` QoS 1 publishes of
//!   `STRAUM_BENCH_PAYLOAD` bytes in flight (a closed loop: a PUBACK sends
//!   the next);
//! * `STRAUM_BENCH_SUBSCRIBERS` clients take every message at QoS 0, so
//!   deliveries are on the path too.
//!
//! One line of results: PUBACKs per second, publish-to-PUBACK latency
//! (p50, p99), and the three servers' CPU time per 1 000 PUBACKs — the
//! work the path costs, which a VM's scheduling disturbs far less than it
//! does wall-clock time (CPU time is read from `/proc` where there is one,
//! from `ps` otherwise). Accepted numbers come from a quiet Linux host
//! (CLAUDE.md §11); anything else is preliminary.
//!
//! `cargo bench -p server --bench puback`; `STRAUM_BENCH_SERVER=<path>`
//! measures another build's `straum-server` with this client. Refuses an
//! unoptimised build.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

const NODES: u8 = 3;

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// The server measured: `STRAUM_BENCH_SERVER` if set — another build's
/// binary, so one client measures two builds (D-124) — or this build's.
fn server_binary() -> PathBuf {
    std::env::var_os("STRAUM_BENCH_SERVER").map_or_else(
        || PathBuf::from(env!("CARGO_BIN_EXE_straum-server")),
        PathBuf::from,
    )
}

struct Servers {
    dir: PathBuf,
    /// Each node's settings, to start it again after a kill (D-128).
    settings: Vec<Vec<(String, String)>>,
    children: Vec<Child>,
    mqtt: Vec<SocketAddr>,
    /// Where each node serves its metrics; empty when none listens.
    metrics: Vec<SocketAddr>,
}

impl Servers {
    /// `metrics`: a metrics listener on each node — only a build with
    /// D-112's metrics accepts the setting; 071db00 refuses it as unknown.
    fn start(metrics: bool) -> Self {
        let dir = std::env::temp_dir().join(format!("straum-bench-puback-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let links: Vec<u16> = (0..NODES).map(|_| free_port()).collect();
        let mqtt: Vec<u16> = (0..NODES).map(|_| free_port()).collect();
        let listens: Vec<u16> = if metrics {
            (0..NODES).map(|_| free_port()).collect()
        } else {
            Vec::new()
        };
        let peers = links
            .iter()
            .enumerate()
            .map(|(id, port)| format!("{id}=127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",");
        let settings: Vec<Vec<(String, String)>> = (0..NODES)
            .map(|id| {
                let i = usize::from(id);
                let mut s = vec![
                    ("STRAUM_EXPERIMENTAL_CLUSTER".to_owned(), "1".to_owned()),
                    ("STRAUM_CLUSTER_NODE_ID".to_owned(), id.to_string()),
                    ("STRAUM_CLUSTER_PEERS".to_owned(), peers.clone()),
                    (
                        "STRAUM_CLUSTER_MQTT_LISTEN".to_owned(),
                        format!("127.0.0.1:{}", mqtt[i]),
                    ),
                    (
                        "STRAUM_CLUSTER_DATA_DIR".to_owned(),
                        dir.join(format!("data-{id}")).display().to_string(),
                    ),
                    (
                        "STRAUM_LOG_LEVEL".to_owned(),
                        std::env::var("STRAUM_BENCH_LOG_LEVEL")
                            .unwrap_or_else(|_| "warn".to_owned()),
                    ),
                ];
                if let Some(port) = listens.get(i) {
                    s.push((
                        "STRAUM_CLUSTER_METRICS_LISTEN".to_owned(),
                        format!("127.0.0.1:{port}"),
                    ));
                }
                // D-131 A: one catch-up read's size, passed through.
                if let Ok(bytes) = std::env::var("STRAUM_BENCH_READ_BYTES") {
                    s.push(("STRAUM_READ_BYTES".to_owned(), bytes));
                }
                s
            })
            .collect();
        let children = settings
            .iter()
            .enumerate()
            .map(|(id, s)| spawn(s, &dir.join(format!("log-{id}-0"))))
            .collect();
        Self {
            dir,
            settings,
            children,
            mqtt: mqtt
                .iter()
                .map(|&p| SocketAddr::from(([127, 0, 0, 1], p)))
                .collect(),
            metrics: listens
                .iter()
                .map(|&p| SocketAddr::from(([127, 0, 0, 1], p)))
                .collect(),
        }
    }

    /// The three servers' CPU time so far, user and system.
    fn cpu(&self) -> Duration {
        self.children.iter().map(|c| cpu_time(c.id())).sum()
    }

    /// The node that sequences now, from its metrics.
    fn leader(&self) -> Option<usize> {
        self.metrics
            .iter()
            .position(|&a| fetch(a).is_some_and(|t| value(&t, "straum_leading") == 1.0))
    }

    /// A planned handoff (D-128): SIGUSR1 to the sequencer.
    fn hand_off(&self, node: usize) {
        let _ = Command::new("kill")
            .arg("-USR1")
            .arg(self.children[node].id().to_string())
            .status();
    }

    /// A takeover (D-128): the sequencer's process killed outright, and
    /// started again over its data after `after`.
    fn kill_and_restart(&mut self, node: usize, after: Duration) {
        let _ = self.children[node].kill();
        let _ = self.children[node].wait();
        std::thread::sleep(after);
        let log = self.dir.join(format!("log-{node}-1"));
        self.children[node] = spawn(&self.settings[node], &log);
        // Back when its MQTT listener accepts; if not within 30 s, its log.
        let deadline = Instant::now() + Duration::from_secs(30);
        while TcpStream::connect(self.mqtt[node]).is_err() {
            assert!(
                Instant::now() < deadline,
                "node {node} did not come back after the kill:\n{}",
                std::fs::read_to_string(&log).unwrap_or_default()
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

/// A server, its output into `log`: kept to explain a node that does not
/// come back.
fn spawn(settings: &[(String, String)], log: &std::path::Path) -> Child {
    let file = std::fs::File::create(log).unwrap();
    let mut command = Command::new(server_binary());
    command.env_clear();
    for (name, value) in settings {
        command.env(name, value);
    }
    command
        .stdin(Stdio::null())
        .stdout(file.try_clone().unwrap())
        .stderr(file)
        .spawn()
        .unwrap()
}

impl Drop for Servers {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A process's CPU time and, where known, its disk bytes read and written.
type Usage = (Duration, Option<(u64, u64)>);

/// A process's bytes read from and written to storage, from
/// `/proc/<pid>/io` (Linux only; `None` elsewhere).
fn io_bytes(pid: u32) -> Option<(u64, u64)> {
    let text = std::fs::read_to_string(format!("/proc/{pid}/io")).ok()?;
    let field = |name: &str| {
        text.lines()
            .find_map(|l| l.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    Some((field("read_bytes:")?, field("write_bytes:")?))
}

/// A process's CPU time: `/proc/<pid>/stat` (utime and stime, in the
/// 100 Hz ticks Linux reports to user space), or `ps -o cputime=`.
fn cpu_time(pid: u32) -> Duration {
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        let fields: Vec<&str> = stat
            .rsplit_once(')')
            .map(|(_, rest)| rest.split_whitespace().collect())
            .unwrap_or_default();
        // After the command: state is field 3, utime 14, stime 15.
        let ticks: u64 = [11, 12]
            .iter()
            .filter_map(|&i| fields.get(i)?.parse::<u64>().ok())
            .sum();
        return Duration::from_millis(ticks * 10);
    }
    let out = Command::new("ps")
        .args(["-o", "cputime=", "-p", &pid.to_string()])
        .output()
        .unwrap();
    // `[[hh:]mm:]ss[.cc]`
    let text = String::from_utf8_lossy(&out.stdout);
    let seconds = text.trim().split(':').fold(0.0f64, |acc, part| {
        acc * 60.0 + part.parse::<f64>().unwrap_or(0.0)
    });
    Duration::from_secs_f64(seconds)
}

// A minimal MQTT 3.1.1 client, as in the server's end-to-end test.

fn remaining(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (n % 128) as u8;
        n /= 128;
        out.push(if n > 0 { byte | 0x80 } else { byte });
        if n == 0 {
            return out;
        }
    }
}

fn string(s: &[u8]) -> Vec<u8> {
    let mut out = u16::try_from(s.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(s);
    out
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    out.extend(remaining(body.len()));
    out.extend_from_slice(body);
    out
}

fn read_packet(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
    let mut header = [0u8; 1];
    stream.read_exact(&mut header).ok()?;
    let (mut length, mut shift) = (0usize, 0);
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).ok()?;
        length |= usize::from(byte[0] & 0x7f) << shift;
        shift += 7;
        if byte[0] & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).ok()?;
    Some((header[0], body))
}

/// A clean session, once the node takes clients.
/// A clean session, once the node takes clients; panics after 30 s — for
/// the start of a run, when every node must be serving.
fn connect(address: SocketAddr, client: &str) -> TcpStream {
    try_connect(address, client, Duration::from_secs(30))
        .unwrap_or_else(|| panic!("{client}: never connected to {address}"))
}

/// A clean session within `within`, or `None`: a node killed and started
/// again takes clients only once it has caught up to its own reset (D-128).
fn try_connect(address: SocketAddr, client: &str, within: Duration) -> Option<TcpStream> {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(mut stream) = TcpStream::connect(address) {
            stream
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            stream.set_nodelay(true).unwrap();
            let mut body = string(b"MQTT");
            body.extend_from_slice(&[4, 0x02, 0, 60]);
            body.extend(string(client.as_bytes()));
            if stream.write_all(&packet(0x10, &body)).is_ok()
                && matches!(read_packet(&mut stream), Some((0x20, ack)) if ack == [0, 0])
            {
                return Some(stream);
            }
        }
        if Instant::now() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn publish_packet(topic: &[u8], id: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = string(topic);
    body.extend_from_slice(&id.to_be_bytes());
    body.extend_from_slice(payload);
    packet(0x32, &body)
}

/// A publish slower than this is counted as delayed, not lost (D-128).
const LATE: Duration = Duration::from_secs(1);
/// After the run, how long a publisher waits for what it still has in
/// flight before counting it unacknowledged.
const DRAIN: Duration = Duration::from_secs(15);
/// A pause between PUBACKs at least this long is kept, with when it
/// happened, so it can be laid against the events.
const GAP: Duration = Duration::from_millis(20);
/// How long a publisher tries to reconnect to its node before giving up.
const RECONNECT: Duration = Duration::from_secs(30);

/// What one publisher saw (D-128). Every publish is one of: acknowledged
/// (in time, or `late`), or unacknowledged once the run has drained. A
/// publish sent again after a reconnect is a retransmission, not a new
/// one; a PUBACK for nothing in flight is a duplicate, never a success.
#[derive(Default)]
struct Account {
    /// First send → PUBACK, in µs, while measuring.
    latencies: Vec<u32>,
    acknowledged: u64,
    late: u64,
    retransmitted: u64,
    reconnects: u64,
    duplicates: u64,
    unacknowledged: u64,
    /// When it gave up reconnecting to its node, if it did.
    gave_up: Option<Instant>,
    /// Pauses between this publisher's PUBACKs of at least `GAP`: (start,
    /// end).
    gaps: Vec<(Instant, Instant)>,
}

/// A closed loop of `window` QoS 1 publishes. When the connection breaks
/// (its node killed), it connects again — a clean session, the same node —
/// and sends what was in flight again, as an at-least-once client does.
fn publisher(
    address: SocketAddr,
    name: String,
    window: u16,
    payload: usize,
    measuring: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> Account {
    let payload = vec![b'x'; payload];
    let mut account = Account {
        latencies: Vec::with_capacity(1 << 20),
        ..Account::default()
    };
    // Per packet id: when that publish was first sent.
    let mut in_flight: Vec<Option<Instant>> = vec![None; usize::from(window) + 1];
    let mut stream = connect(address, &name);
    for id in 1..=window {
        in_flight[usize::from(id)] = Some(Instant::now());
        stream
            .write_all(&publish_packet(b"bench/t", id, &payload))
            .unwrap();
    }
    let mut last = Instant::now();
    let mut drain_until: Option<Instant> = None;
    loop {
        if drain_until.is_none() && stop.load(Ordering::Relaxed) {
            drain_until = Some(Instant::now() + DRAIN);
        }
        if let Some(until) = drain_until {
            if in_flight.iter().all(Option::is_none) || Instant::now() >= until {
                break;
            }
        }
        let Some((kind, body)) = read_packet(&mut stream) else {
            // The connection broke: again, and resend what was in flight.
            let Some(again) = try_connect(address, &name, RECONNECT) else {
                // Its node is not taking clients again: given up, with what
                // was in flight unacknowledged.
                account.gave_up = Some(Instant::now());
                break;
            };
            stream = again;
            account.reconnects += 1;
            for id in 1..=window {
                if in_flight[usize::from(id)].is_some() {
                    account.retransmitted += 1;
                    let _ = stream.write_all(&publish_packet(b"bench/t", id, &payload));
                }
            }
            continue;
        };
        if kind != 0x40 || body.len() != 2 {
            continue;
        }
        let id = u16::from_be_bytes([body[0], body[1]]);
        let Some(first) = in_flight.get_mut(usize::from(id)).and_then(Option::take) else {
            account.duplicates += 1;
            continue;
        };
        let now = Instant::now();
        let took = now.saturating_duration_since(first);
        account.acknowledged += 1;
        if took > LATE {
            account.late += 1;
        }
        if measuring.load(Ordering::Relaxed) {
            account
                .latencies
                .push(u32::try_from(took.as_micros()).unwrap_or(u32::MAX));
        }
        if now.saturating_duration_since(last) >= GAP {
            account.gaps.push((last, now));
        }
        last = now;
        if drain_until.is_none() {
            in_flight[usize::from(id)] = Some(now);
            let _ = stream.write_all(&publish_packet(b"bench/t", id, &payload));
        }
    }
    account.unacknowledged = in_flight.iter().filter(|f| f.is_some()).count() as u64;
    account
}

fn subscriber(address: SocketAddr, name: String, delivered: Arc<AtomicU64>, stop: Arc<AtomicBool>) {
    let mut stream = connect(address, &name);
    let mut body = 1u16.to_be_bytes().to_vec();
    body.extend(string(b"bench/t"));
    body.push(0);
    stream.write_all(&packet(0x82, &body)).unwrap();
    while !stop.load(Ordering::Relaxed) {
        match read_packet(&mut stream) {
            Some((kind, _)) if kind & 0xf0 == 0x30 => {
                delivered.fetch_add(1, Ordering::Relaxed);
            }
            Some(_) => {}
            None => return,
        }
    }
}

/// One scrape: the metrics text, or `None`.
fn fetch(address: SocketAddr) -> Option<String> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    stream.write_all(b"GET /metrics HTTP/1.1\r\n\r\n").ok()?;
    let mut text = String::new();
    stream.read_to_string(&mut text).ok()?;
    text.split_once("\r\n\r\n").map(|(_, body)| body.to_owned())
}

/// A scraper polling every node at a fixed interval, as a monitoring
/// system would (D-127): its cost is what scraping adds.
fn scraper(addresses: Vec<SocketAddr>, every: Duration, stop: Arc<AtomicBool>) -> u64 {
    let mut scrapes = 0u64;
    while !stop.load(Ordering::Relaxed) {
        let started = Instant::now();
        for &address in &addresses {
            if fetch(address).is_some() {
                scrapes += 1;
            }
        }
        std::thread::sleep(every.saturating_sub(started.elapsed()));
    }
    scrapes
}

/// A plain sample's value.
fn value(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
        .unwrap_or(0.0)
}

/// A histogram's quantile, as the upper bound of the bucket it falls in,
/// in milliseconds; `-` when it has no observations.
fn quantile(text: &str, name: &str, q: f64) -> String {
    let count = value(text, &format!("{name}_count"));
    if count == 0.0 {
        return "-".to_owned();
    }
    let prefix = format!("{name}_bucket{{le=\"");
    for line in text.lines() {
        let Some(rest) = line.strip_prefix(&prefix) else {
            continue;
        };
        let Some((le, n)) = rest.split_once("\"} ") else {
            continue;
        };
        if n.parse::<f64>().unwrap_or(0.0) >= q * count {
            return match le.parse::<f64>() {
                Ok(seconds) => format!("{:.3}", seconds * 1000.0),
                Err(_) => "+Inf".to_owned(),
            };
        }
    }
    "-".to_owned()
}

/// Where the delay forms, per node (D-112, D-127 step 4), since the node
/// started — warm-up included. Quantiles are bucket bounds (powers of two
/// microseconds), not exact values. Takeover, succession and fencing are
/// reported apart.
fn diagnose(node: usize, text: &str) {
    let v = |n: &str| value(text, n);
    println!(
        "diag node={node} leading={} commit={} \
         proposal_commit_p50_ms={} p99_ms={} append_durable_p50_ms={} p99_ms={} \
         append_commit_p50_ms={} p99_ms={} \
         accepted={} committed={} refused={} given_up={} forwards_dropped={} forwards_refused={} \
         outbox_entries={} outbox_bytes={} read_block_spells={} read_block_longest_s={} \
         takeovers_started={} takeovers_completed={} successions={} stepped_down={} \
         closed_keep_alive={} closed_commit_timeout={}",
        v("straum_leading"),
        v("straum_commit"),
        quantile(text, "straum_proposal_commit_seconds", 0.5),
        quantile(text, "straum_proposal_commit_seconds", 0.99),
        quantile(text, "straum_append_durable_seconds", 0.5),
        quantile(text, "straum_append_durable_seconds", 0.99),
        quantile(text, "straum_append_commit_seconds", 0.5),
        quantile(text, "straum_append_commit_seconds", 0.99),
        v("straum_proposals_accepted_total"),
        v("straum_proposals_committed_total"),
        v("straum_proposals_refused_total"),
        v("straum_proposals_given_up_total"),
        v("straum_forwards_dropped_total"),
        v("straum_forwards_refused_total"),
        v("straum_outbox_entries"),
        v("straum_outbox_bytes"),
        v("straum_read_block_spells_total"),
        v("straum_read_block_longest_seconds"),
        v("straum_takeovers_started_total"),
        v("straum_takeovers_completed_total"),
        v("straum_successions_total"),
        v("straum_stepped_down_total"),
        v("straum_mqtt_closed_keep_alive_total"),
        v("straum_mqtt_closed_commit_timeout_total"),
    );
}

fn main() {
    if cfg!(debug_assertions) {
        eprintln!("puback: built without optimisation; use `cargo bench` or a --release build");
        return;
    }
    let publishers: usize = env("STRAUM_BENCH_PUBLISHERS", 6);
    let subscribers: usize = env("STRAUM_BENCH_SUBSCRIBERS", 3);
    let window: u16 = env("STRAUM_BENCH_WINDOW", 16);
    let payload: usize = env("STRAUM_BENCH_PAYLOAD", 64);
    let warmup = Duration::from_secs(env("STRAUM_BENCH_WARMUP", 3));
    let seconds = Duration::from_secs(env("STRAUM_BENCH_SECONDS", 10));

    // 0: no metrics listener, no scraper — the only setting a build
    // without metrics (071db00) accepts.
    let scrape_ms: u64 = env("STRAUM_BENCH_SCRAPE_MS", 0);
    // D-128: `handoff` (SIGUSR1 to the sequencer) and `kill` (its process
    // killed, started again 2 s later), spread evenly over the measured
    // window. They need the metrics listener to find the sequencer.
    let events: Vec<String> = std::env::var("STRAUM_BENCH_EVENTS")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|e| !e.is_empty())
        .map(str::to_owned)
        .collect();
    let mut servers = Servers::start(scrape_ms > 0 || !events.is_empty());
    let measuring = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(AtomicU64::new(0));
    let subs: Vec<_> = (0..subscribers)
        .map(|i| {
            let (address, delivered, stop) = (
                servers.mqtt[i % 3],
                Arc::clone(&delivered),
                Arc::clone(&stop),
            );
            std::thread::spawn(move || subscriber(address, format!("sub-{i}"), delivered, stop))
        })
        .collect();
    let pubs: Vec<_> = (0..publishers)
        .map(|i| {
            let (address, measuring, stop) = (
                servers.mqtt[i % 3],
                Arc::clone(&measuring),
                Arc::clone(&stop),
            );
            std::thread::spawn(move || {
                publisher(
                    address,
                    format!("pub-{i}"),
                    window,
                    payload,
                    measuring,
                    stop,
                )
            })
        })
        .collect();
    let scraping = (scrape_ms > 0).then(|| {
        let (addresses, stop) = (servers.metrics.clone(), Arc::clone(&stop));
        std::thread::spawn(move || scraper(addresses, Duration::from_millis(scrape_ms), stop))
    });
    std::thread::sleep(warmup);
    let cpu_before = servers.cpu();
    let delivered_before = delivered.load(Ordering::Relaxed);
    measuring.store(true, Ordering::Relaxed);
    let started = Instant::now();
    let mut happened: Vec<(String, usize, Instant)> = Vec::new();
    let mut restarted: Option<(Instant, Vec<Usage>)> = None;
    let mut lags: Vec<(f64, f64)> = Vec::new();
    for (i, kind) in events.iter().enumerate() {
        let at = started + seconds.mul_f64((i + 1) as f64 / (events.len() + 1) as f64);
        std::thread::sleep(at.saturating_duration_since(Instant::now()));
        let deadline = Instant::now() + Duration::from_secs(10);
        let leader = loop {
            if let Some(leader) = servers.leader() {
                break leader;
            }
            assert!(Instant::now() < deadline, "no sequencer found for {kind}");
            std::thread::sleep(Duration::from_millis(100));
        };
        happened.push((kind.clone(), leader, Instant::now()));
        match kind.as_str() {
            "handoff" => servers.hand_off(leader),
            "kill" => {
                servers.kill_and_restart(leader, Duration::from_secs(2));
                // Each node's CPU and disk from here to the end of the load:
                // what the catch-up costs (D-133 A).
                restarted = Some((
                    Instant::now(),
                    servers
                        .children
                        .iter()
                        .map(|c| (cpu_time(c.id()), io_bytes(c.id())))
                        .collect(),
                ));
            }
            other => panic!("unknown event {other}: handoff or kill"),
        }
    }
    // After a kill, every second until the load ends: the restarted node's
    // position against the cluster's commit — its lag growing or
    // shrinking under load (D-131 A).
    let killed = happened
        .iter()
        .find(|(kind, ..)| kind == "kill")
        .map(|(_, node, _)| *node);
    while Instant::now() < started + seconds {
        let left = (started + seconds).saturating_duration_since(Instant::now());
        std::thread::sleep(left.min(Duration::from_secs(1)));
        if let Some(node) = killed {
            let commit = |n: usize| {
                servers
                    .metrics
                    .get(n)
                    .and_then(|&a| fetch(a))
                    .map(|t| value(&t, "straum_commit"))
            };
            let cluster = (0..usize::from(NODES))
                .filter(|&n| n != node)
                .filter_map(commit)
                .fold(0.0f64, f64::max);
            if let Some(replica) = commit(node) {
                println!(
                    "catchup t_s={:.0} node={node} replica_commit={replica} cluster_commit={cluster} lag={}",
                    started.elapsed().as_secs_f64(),
                    cluster - replica
                );
                lags.push((started.elapsed().as_secs_f64(), cluster - replica));
            }
        }
    }
    measuring.store(false, Ordering::Relaxed);
    let cpu = servers.cpu().saturating_sub(cpu_before);
    // After a kill: each node's CPU and disk from the restart to the end of
    // the load (D-133 A); disk only where `/proc/<pid>/io` exists.
    let resources: Vec<String> = restarted
        .as_ref()
        .map(|(at, before)| {
            let seconds = at.elapsed().as_secs_f64();
            servers
                .children
                .iter()
                .zip(before)
                .enumerate()
                .map(|(node, (child, (cpu0, io0)))| {
                    let cpu = cpu_time(child.id()).saturating_sub(*cpu0);
                    let disk = match (io0, io_bytes(child.id())) {
                        (Some((r0, w0)), Some((r, w))) => format!(
                            "disk_read_bytes={} disk_write_bytes={}",
                            r.saturating_sub(*r0),
                            w.saturating_sub(*w0)
                        ),
                        _ => "disk=-".to_owned(),
                    };
                    format!(
                        "resources node={node} seconds={seconds:.1} cpu_ms={} {disk}",
                        cpu.as_millis()
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    let deliveries = delivered.load(Ordering::Relaxed) - delivered_before;
    stop.store(true, Ordering::Relaxed);
    let accounts: Vec<Account> = pubs
        .into_iter()
        .map(|p| {
            p.join().unwrap_or_else(|_| {
                // A publisher gave up: every node's log, then fail.
                let mut logs: Vec<_> = std::fs::read_dir(&servers.dir)
                    .map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect())
                    .unwrap_or_default();
                logs.sort();
                for log in logs.iter().filter(|p| p.to_string_lossy().contains("log-")) {
                    eprintln!(
                        "===== {} =====\n{}",
                        log.display(),
                        std::fs::read_to_string(log).unwrap_or_default()
                    );
                }
                for (node, &address) in servers.metrics.iter().enumerate() {
                    let state: Vec<String> = fetch(address)
                        .map(|t| {
                            [
                                "straum_slot",
                                "straum_leading",
                                "straum_log_end",
                                "straum_log_durable",
                                "straum_commit",
                                "straum_applied",
                                "straum_takeovers_started_total",
                                "straum_mqtt_connections",
                            ]
                            .iter()
                            .map(|n| format!("{n}={}", value(&t, n)))
                            .collect()
                        })
                        .unwrap_or_default();
                    eprintln!("node {node}: {}", state.join(" "));
                }
                panic!("a publisher failed");
            })
        })
        .collect();
    let mut latencies: Vec<u32> = accounts
        .iter()
        .flat_map(|a| a.latencies.iter().copied())
        .collect();
    let scrapes = scraping.map_or(0, |s| s.join().unwrap_or(0));
    let finals: Vec<Option<String>> = servers.metrics.iter().map(|&a| fetch(a)).collect();
    // After events, the same positions 10 s later, with no load: a node
    // still behind is catching up only if its commit moved.
    let settled: Vec<Option<String>> = if events.is_empty() {
        Vec::new()
    } else {
        std::thread::sleep(Duration::from_secs(10));
        servers.metrics.iter().map(|&a| fetch(a)).collect()
    };
    drop(servers);
    for s in subs {
        let _ = s.join();
    }
    latencies.sort_unstable();
    let n = latencies.len();
    let at = |q: f64| {
        latencies
            .get(((n as f64 * q) as usize).min(n.saturating_sub(1)))
            .copied()
            .unwrap_or(0)
    };
    println!(
        "puback publishers={publishers} subscribers={subscribers} window={window} payload={payload} \
         scrape_ms={scrape_ms} scrapes={scrapes} \
         pubacks_per_s={:.0} p50_ms={:.3} p99_ms={:.3} deliveries_per_s={:.0} cpu_ms_per_1k={}",
        n as f64 / seconds.as_secs_f64(),
        f64::from(at(0.50)) / 1000.0,
        f64::from(at(0.99)) / 1000.0,
        deliveries as f64 / seconds.as_secs_f64(),
        // A killed process takes its CPU time with it.
        if happened.iter().any(|(kind, ..)| kind == "kill") {
            "-".to_owned()
        } else {
            format!(
                "{:.2}",
                cpu.as_secs_f64() * 1000.0 / (n.max(1) as f64 / 1000.0)
            )
        },
    );
    let sum = |f: fn(&Account) -> u64| accounts.iter().map(f).sum::<u64>();
    println!(
        "publishes acknowledged={} late={} retransmitted={} reconnects={} unacknowledged={} duplicate_acks={} publishers_gave_up={}",
        sum(|a| a.acknowledged),
        sum(|a| a.late),
        sum(|a| a.retransmitted),
        sum(|a| a.reconnects),
        sum(|a| a.unacknowledged),
        sum(|a| a.duplicates),
        accounts.iter().filter(|a| a.gave_up.is_some()).count(),
    );
    // Each event apart: the longest any publisher waited between two
    // PUBACKs, among the pauses that ended after it and before the next
    // event, or before the end of the run (D-128): a pause the event caused
    // begins at the last PUBACK before it.
    for (i, (kind, node, at)) in happened.iter().enumerate() {
        let until = happened
            .get(i + 1)
            .map_or(Instant::now(), |&(_, _, next)| next);
        // Publisher `i` talks to node `i % 3`: the event's own node's
        // clients apart from the others' — after a kill, its clients wait
        // for it to come back, the others only for the takeover.
        let longest = |own: bool| {
            accounts
                .iter()
                .enumerate()
                .filter(|(p, _)| (p % usize::from(NODES) == *node) == own)
                .flat_map(|(_, a)| a.gaps.iter())
                .filter(|&&(_, e)| e > *at && e <= until)
                .map(|&(s, e)| e.saturating_duration_since(s))
                .max()
                .unwrap_or_default()
        };
        // Its node's clients that could not reconnect within `RECONNECT`:
        // their wait has no end to measure.
        let gave_up = accounts
            .iter()
            .enumerate()
            .filter(|(p, a)| p % usize::from(NODES) == *node && a.gave_up.is_some_and(|g| g > *at))
            .count();
        println!(
            "event kind={kind} node={node} at_s={:.1} max_gap_ms_other_nodes={:.1} max_gap_ms_its_node={:.1} its_clients_gave_up={gave_up}",
            at.saturating_duration_since(started).as_secs_f64(),
            longest(false).as_secs_f64() * 1000.0,
            longest(true).as_secs_f64() * 1000.0
        );
    }
    // After events, each node's position: one behind the commit point is
    // still catching up.
    if !happened.is_empty() {
        for (node, text) in finals.iter().enumerate() {
            if let Some(t) = text {
                println!(
                    "after node={node} slot={} leading={} commit={} applied={} log_end={}",
                    value(t, "straum_slot"),
                    value(t, "straum_leading"),
                    value(t, "straum_commit"),
                    value(t, "straum_applied"),
                    value(t, "straum_log_end"),
                );
                if let Some(Some(later)) = settled.get(node) {
                    println!(
                        "after+10s node={node} commit={} applied={}",
                        value(later, "straum_commit"),
                        value(later, "straum_applied"),
                    );
                }
            }
        }
    }
    for line in &resources {
        println!("{line}");
    }
    // One line per run with a kill (D-133 A): the lag's rate under load,
    // from the per-second samples, and the restarted node's recovery after
    // the load, from its commit at the end and 10 s later.
    if let Some(node) = killed {
        let under_load = match (lags.first(), lags.last()) {
            (Some(&(t0, l0)), Some(&(t1, l1))) if t1 > t0 => {
                format!("{:.0}", (l1 - l0) / (t1 - t0))
            }
            _ => "-".to_owned(),
        };
        let commit = |t: &Option<String>| t.as_ref().map(|t| value(t, "straum_commit"));
        // Caught up before the load stopped — the last sample's lag at 0 or
        // below — leaves nothing to recover: said so, not reported as a
        // rate of 0 (D-135 A, the outlier of D-133 A).
        let caught_up = lags.last().is_some_and(|&(_, lag)| lag <= 0.0);
        let recovery = match (
            finals.get(node).and_then(commit),
            settled.get(node).and_then(commit),
        ) {
            _ if caught_up => "caught_up".to_owned(),
            (Some(a), Some(b)) => format!("{:.0}", (b - a) / 10.0),
            _ => "-".to_owned(),
        };
        println!(
            "catchup_summary node={node} lag_rate_under_load={under_load} recovery_rate={recovery}"
        );
    }
    for (node, text) in finals.iter().enumerate() {
        if let Some(text) = text {
            diagnose(node, text);
        }
    }
}
