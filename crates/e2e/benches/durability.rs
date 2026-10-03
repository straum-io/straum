//! D-6 B: what a publish costs at each durability level, before and after
//! admission (D-7).
//!
//! One harness, built unchanged against every variant being compared, so the
//! only thing that differs between two rows is the broker underneath. That is
//! why it reads the pending-acknowledgement depth from the metrics endpoint
//! rather than from a struct field: the field does not exist in the build it
//! is compared against.
//!
//! What is timed, exactly: from the first publish to the last PUBACK. The
//! client's teardown is outside the interval — timing around it once made a
//! section read 667 msg/s (docs/replication.md §124). Latency is per packet:
//! the moment the client wrote PUBLISH with a packet id to the moment the
//! PUBACK for that id arrived, so it is what a client sees, not how long
//! `publish()` took to queue.
//!
//! Output is one `row,` line per measured point, for the analysis to read. The
//! last four columns — rewrites during the interval, p99.9, the slowest
//! PUBACK and the latest rewrite's duration in ms (the last rewrite of the run only, one sample — not a per-rewrite distribution) — were appended for D-24, so the ones before them read as before.
//!
//! ```text
//! cargo bench -p e2e --bench durability                  # every point but `replicated`
//! STRAUM_BENCH_ONLY=fsynced cargo bench -p e2e --bench durability
//! STRAUM_BENCH_ONLY=replicated cargo bench -p e2e --bench durability    # D-24, opt-in
//! STRAUM_BENCH_ONLY=replicated STRAUM_BENCH_COMPACT_BYTES=8388608 \
//!     STRAUM_BENCH_RETAIN_BYTES=0 cargo bench -p e2e --bench durability
//! ```

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
use runtime::{
    Durability, ReplicationSettings, Runtime, RuntimeConfig, TransportConfig, WalSettings,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Payload sizes and counts. The sizes match the replication table
/// (docs/performance.md) so the numbers stay comparable; the counts keep each
/// point at a few seconds or more under `fsynced`.
const POINTS: [(usize, usize); 3] = [(64, 20_000), (1024, 20_000), (65_536, 2_000)];

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Memory,
    Fsynced,
    Quorum,
    /// `fsynced` with an asynchronous follower, run long enough for the log
    /// to be rewritten several times: each rewrite copies the retained
    /// suffix on the writer thread while commits wait (D-24). Opt-in, as
    /// `STRAUM_BENCH_ONLY=replicated`: at the 64 MiB defaults the two or three
    /// rewrites it takes are minutes of publishing at 64 B.
    Replicated,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Fsynced => "fsynced",
            Self::Quorum => "quorum",
            Self::Replicated => "replicated",
        }
    }
}

/// A byte setting from the environment, for scaling the `replicated` run
/// down on a development machine; the defaults are the broker's own.
fn bytes_from_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("{error}"))
}

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-durability-{name}-{}.wal",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn any_port() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A port nothing holds, for the metrics endpoint: the runtime does not say
/// which port it bound when given 0.
fn free_port() -> SocketAddr {
    let probe = ok(std::net::TcpListener::bind(any_port()));
    ok(probe.local_addr())
}

fn node(path: &Path, durability: Durability, replication: ReplicationSettings) -> RuntimeConfig {
    RuntimeConfig {
        transport: TransportConfig {
            listen: any_port(),
            ..TransportConfig::default()
        },
        durability,
        wal: WalSettings {
            path: path.to_path_buf(),
            ..WalSettings::default()
        },
        replication,
        metrics: Some(free_port()),
        ..RuntimeConfig::default()
    }
}

/// One value from the Prometheus text the node serves, or `None` when this
/// build does not export it.
async fn scrape(at: SocketAddr, name: &str) -> Option<u64> {
    let mut stream = tokio::net::TcpStream::connect(at).await.ok()?;
    stream.write_all(b"GET / HTTP/1.0\r\n\r\n").await.ok()?;
    let mut text = String::new();
    stream.read_to_string(&mut text).await.ok()?;
    let prefix = format!("straum_{name} ");
    text.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .and_then(|value| value.trim().parse().ok())
}

/// CPU time this process has used, from `ps`: everything in it — leader,
/// follower and client — which is the same set in every variant compared.
fn cpu_seconds() -> f64 {
    let out = std::process::Command::new("ps")
        .args(["-o", "time=", "-p", &std::process::id().to_string()])
        .output()
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_default();
    // `[[hh:]mm:]ss.ss`
    out.split(':')
        .rev()
        .enumerate()
        .map(|(place, part)| part.parse::<f64>().unwrap_or(0.0) * 60f64.powi(place as i32))
        .sum()
}

fn percentile(sorted: &[u64], fraction: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted.get(index).copied().unwrap_or(0) as f64 / 1e6
}

struct Measured {
    seconds: f64,
    latencies: Vec<u64>,
    cpu: f64,
    pending_max: Option<u64>,
    sync_ms_median: f64,
}

/// Publishes `count` messages of `payload` bytes at QoS 1 and waits for every
/// PUBACK. `metrics` is the leader's endpoint, sampled while it runs.
async fn publish(leader: &Runtime, metrics: SocketAddr, payload: usize, count: usize) -> Measured {
    let addr = leader.local_addr();
    let mut options = MqttOptions::new("bench", addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(60));
    options.set_max_packet_size(payload + 1024, payload + 1024);
    let (client, mut eventloop) = AsyncClient::new(options, 256);

    // Packet ids are reused once acknowledged, so a slot per id is enough.
    let written: Arc<Vec<AtomicU64>> =
        Arc::new((0..=u16::MAX).map(|_| AtomicU64::new(0)).collect());
    let acked = Arc::new(AtomicUsize::new(0));
    let origin = Instant::now();
    let (done_tx, done_rx) = tokio::sync::oneshot::channel::<Vec<u64>>();

    let stamps = Arc::clone(&written);
    let counter = Arc::clone(&acked);
    let driving = tokio::spawn(async move {
        let mut latencies = Vec::with_capacity(count);
        let mut done = Some(done_tx);
        while let Ok(event) = eventloop.poll().await {
            let now = origin.elapsed().as_nanos() as u64;
            match event {
                Event::Outgoing(Outgoing::Publish(pkid)) => {
                    if let Some(slot) = stamps.get(usize::from(pkid)) {
                        slot.store(now, Ordering::Relaxed);
                    }
                }
                Event::Incoming(Packet::PubAck(ack)) => {
                    if let Some(slot) = stamps.get(usize::from(ack.pkid)) {
                        latencies.push(now.saturating_sub(slot.load(Ordering::Relaxed)));
                    }
                    if counter.fetch_add(1, Ordering::Relaxed) + 1 == count {
                        if let Some(done) = done.take() {
                            let _ = done.send(std::mem::take(&mut latencies));
                        }
                    }
                }
                Event::Outgoing(Outgoing::Disconnect) => break,
                _ => {}
            }
        }
    });

    // Sampled beside the run, not in it: every 20 ms is often enough to see
    // the queue's depth and the sync time, and too rare to cost anything a
    // variant would not also pay.
    let sampling = Arc::new(AtomicU64::new(1));
    let running = Arc::clone(&sampling);
    let handle = leader.metrics_handle();
    let sampler = tokio::spawn(async move {
        let mut pending_max: Option<u64> = None;
        let mut syncs = Vec::new();
        while running.load(Ordering::Relaxed) == 1 {
            if let Some(depth) = scrape(metrics, "pending_acknowledgements").await {
                pending_max = Some(pending_max.unwrap_or(0).max(depth));
            }
            let sync = handle.wal_sync_nanos.load(Ordering::Relaxed);
            if sync > 0 {
                syncs.push(sync);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        (pending_max, syncs)
    });

    let body = vec![b'x'; payload];
    let cpu_before = cpu_seconds();
    let started = Instant::now();
    for _ in 0..count {
        ok(client
            .publish("bench/topic", QoS::AtLeastOnce, false, body.clone())
            .await);
    }
    let mut latencies = ok(done_rx.await);
    let seconds = started.elapsed().as_secs_f64();
    let cpu = cpu_seconds() - cpu_before;

    sampling.store(0, Ordering::Relaxed);
    let (pending_max, mut syncs) = ok(sampler.await);
    // Outside the interval, on purpose.
    ok(client.disconnect().await);
    let _ = driving.await;

    latencies.sort_unstable();
    syncs.sort_unstable();
    Measured {
        seconds,
        latencies,
        cpu,
        pending_max,
        sync_ms_median: syncs.get(syncs.len() / 2).copied().unwrap_or(0) as f64 / 1e6,
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if cfg!(debug_assertions) {
        eprintln!("durability: built without optimisation; use `cargo bench` or a --release build");
        return;
    }
    let variant = std::env::var("STRAUM_BENCH_VARIANT").unwrap_or_else(|_| "?".to_owned());
    let round = std::env::var("STRAUM_BENCH_ROUND").unwrap_or_else(|_| "0".to_owned());
    let only = std::env::var("STRAUM_BENCH_ONLY").unwrap_or_default();

    for mode in [Mode::Memory, Mode::Fsynced, Mode::Quorum, Mode::Replicated] {
        if !only.is_empty() && only != mode.name() {
            continue;
        }
        if mode == Mode::Replicated && only != mode.name() {
            continue;
        }
        let compact_bytes = bytes_from_env(
            "STRAUM_BENCH_COMPACT_BYTES",
            WalSettings::default().compact_bytes,
        );
        let retain_bytes = bytes_from_env(
            "STRAUM_BENCH_RETAIN_BYTES",
            WalSettings::default().retain_bytes,
        );
        for (payload, base_count) in POINTS {
            // Memory acknowledges at once, so the same count finishes in tens of
            // milliseconds — too short for a stable time or for `ps`, whose
            // CPU figure moves in steps of 10 ms. Ten times more there.
            let count = match mode {
                Mode::Memory => base_count * 10,
                // Three rewrites' worth of appended bytes, counting about 64 bytes of record
                // framing and topic on top of each payload.
                Mode::Replicated => base_count
                    .max((3 * compact_bytes as usize).div_ceil(payload.saturating_add(64))),
                Mode::Fsynced | Mode::Quorum => base_count,
            };
            let tag = format!("{}-{payload}", mode.name());
            let leader_path = scratch(&format!("{tag}-leader"));
            let follower_path = scratch(&format!("{tag}-follower"));

            // The follower exists only for `quorum`, and it is always on the
            // same loopback in the same process (D-6 B, point 8).
            let follower = matches!(mode, Mode::Quorum | Mode::Replicated).then(|| {
                ok(Runtime::start(node(
                    &follower_path,
                    Durability::Fsynced,
                    ReplicationSettings {
                        listen: Some(any_port()),
                        ..ReplicationSettings::default()
                    },
                )))
            });
            let replication = match &follower {
                Some(standby) => ReplicationSettings {
                    follower: standby.replication_addr(),
                    required_copies: 2,
                    ..ReplicationSettings::default()
                },
                None => ReplicationSettings::default(),
            };
            let durability = match mode {
                Mode::Memory => Durability::Memory,
                Mode::Fsynced | Mode::Replicated => Durability::Fsynced,
                Mode::Quorum => Durability::Quorum,
            };
            let mut config = node(&leader_path, durability, replication);
            config.wal.compact_bytes = compact_bytes;
            config.wal.retain_bytes = retain_bytes;
            let metrics = config.metrics.unwrap_or_else(any_port);
            let leader = ok(Runtime::start(config));

            // Warm-up: the first sync, the allocator and the page cache are
            // not what the row describes.
            // Fan-out, when asked for: `STRAUM_BENCH_SUBSCRIBERS` clients
            // subscribed at QoS 0 before the warm-up, each counting what it
            // receives. Delivery is not in the timed interval; the PUBACK
            // follows routing to all of them (CLAUDE.md §5), so the interval
            // already pays for the fan-out.
            let subscribers: usize = std::env::var("STRAUM_BENCH_SUBSCRIBERS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(0);
            let received = Arc::new(AtomicU64::new(0));
            let mut listening = Vec::new();
            for index in 0..subscribers {
                listening.push(subscribe(&leader, index, payload, Arc::clone(&received)).await);
            }
            let _ = publish(&leader, metrics, payload, 200).await;
            let before = leader.metrics();
            let received_before = received.load(Ordering::Relaxed);
            // From the endpoint rather than the snapshot, so a build without
            // the counter still compiles and prints "-" (D-15).
            let dropped_before = scrape(metrics, "dropped_messages").await;
            let measured = publish(&leader, metrics, payload, count).await;
            // Delivery to every subscriber is waited for after the interval,
            // so the count below is what the fan-out actually delivered.
            let expected = received_before + (count * subscribers) as u64;
            let deadline = Instant::now() + Duration::from_secs(30);
            while received.load(Ordering::Relaxed) < expected && Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            let delivered = received.load(Ordering::Relaxed) - received_before;
            // Read after the deliveries were waited for: a shortfall in
            // `delivered` and the drops that explain it sit in one row.
            let dropped = match (dropped_before, scrape(metrics, "dropped_messages").await) {
                (Some(before), Some(after)) => after.saturating_sub(before).to_string(),
                _ => "-".to_owned(),
            };
            for task in listening {
                task.abort();
            }
            let after = leader.metrics();
            // The latest rewrite's duration, from the endpoint so a build
            // without the gauge still compiles and prints "-".
            let rewrite_ms = scrape(metrics, "wal_rewrite_nanos").await.map_or_else(
                || "-".to_owned(),
                |nanos| format!("{:.3}", nanos as f64 / 1e6),
            );

            let commits = after.wal_commits.saturating_sub(before.wal_commits);
            let records = after
                .wal_records_committed
                .saturating_sub(before.wal_records_committed);
            let bytes = after
                .wal_bytes_committed
                .saturating_sub(before.wal_bytes_committed);
            let per = |total: u64| {
                if commits == 0 {
                    0.0
                } else {
                    total as f64 / commits as f64
                }
            };
            let lat = &measured.latencies;
            println!(
                "row,{variant},{round},{}{},{payload},{count},{:.4},{:.0},{:.3},{:.3},{:.3},{commits},{:.2},{:.0},{:.3},{},{:.2},{},{},{},{},{},{},{},{},{:.3},{:.3},{}",
                mode.name(),
                if subscribers > 0 {
                    format!("+{subscribers}sub")
                } else {
                    String::new()
                },
                measured.seconds,
                count as f64 / measured.seconds,
                percentile(lat, 0.5),
                percentile(lat, 0.95),
                percentile(lat, 0.99),
                per(records),
                per(bytes),
                measured.sync_ms_median,
                measured
                    .pending_max
                    .map_or_else(|| "-".to_owned(), |depth| depth.to_string()),
                measured.cpu / count as f64 * 1e6,
                after.replication_follower_sequence,
                after.wal_durable_sequence,
                after
                    .replication_catchups
                    .saturating_sub(before.replication_catchups),
                after
                    .replication_shipments_dropped
                    .saturating_sub(before.replication_shipments_dropped),
                lat.len(),
                delivered,
                dropped,
                // Appended, so the columns before them read as they did:
                // rewrites during the interval, and the tail they show up in.
                after.wal_compactions.saturating_sub(before.wal_compactions),
                percentile(lat, 0.999),
                percentile(lat, 1.0),
                rewrite_ms,
            );
            use std::io::Write;
            let _ = std::io::stdout().flush();

            leader.shutdown().await;
            if let Some(standby) = follower {
                standby.shutdown().await;
            }
            let _ = std::fs::remove_file(&leader_path);
            let _ = std::fs::remove_file(&follower_path);
        }
    }
}

/// One QoS 0 subscriber on the benchmark topic, counting what it receives.
async fn subscribe(
    leader: &Runtime,
    index: usize,
    payload: usize,
    received: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    let addr = leader.local_addr();
    let mut options = MqttOptions::new(format!("sub-{index}"), addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(60));
    options.set_max_packet_size(payload + 1024, payload + 1024);
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    ok(client.subscribe("bench/topic", QoS::AtMostOnce).await);
    // Subscribed before anything is published, so the counts are complete.
    loop {
        if let Event::Incoming(Packet::SubAck(_)) = ok(eventloop.poll().await) {
            break;
        }
    }
    tokio::spawn(async move {
        let _client = client;
        while let Ok(event) = eventloop.poll().await {
            if let Event::Incoming(Packet::Publish(_)) = event {
                received.fetch_add(1, Ordering::Relaxed);
            }
        }
    })
}
