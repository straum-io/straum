//! The complete replication path, end to end.
//!
//! What is measured is the architecture Phase 6B added, not TCP and not the
//! codec:
//!
//! ```text
//! publish → leader WAL stage → commit + sync_data → committed batch
//!         → fan-out → frame → TCP → follower decode → stage_framed
//!         → commit + sync_data → durability report → leader
//! ```
//!
//! Every workload runs twice: once with the leader alone, once with a real
//! follower over a real socket. The pair is the point — the difference is
//! what replication costs, and both numbers are printed rather than
//! subtracted (docs/replication.md §122).
//!
//! Leader and follower are two nodes in **one process on one host**, talking
//! over loopback. That is not a network measurement and is not described as
//! one: it is the deterministic environment the integration suite already
//! uses, and it isolates the software cost from a switch.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, MqttOptions, QoS};
use runtime::{
    Durability, ReplicationSettings, Runtime, RuntimeConfig, Snapshot, TransportConfig, WalSettings,
};

struct Workload {
    name: &'static str,
    payload: usize,
    messages: usize,
    /// Publish in bursts of this many with a quiet moment between, so the
    /// adaptive group commit is asked to do both things.
    burst: Option<usize>,
}

const WORKLOADS: &[Workload] = &[
    Workload {
        name: "small 64 B",
        payload: 64,
        messages: 2_000,
        burst: None,
    },
    Workload {
        name: "medium 1 KiB",
        payload: 1024,
        messages: 2_000,
        burst: None,
    },
    Workload {
        name: "large 64 KiB",
        payload: 65_536,
        messages: 400,
        burst: None,
    },
    Workload {
        name: "sustained 64 B",
        payload: 64,
        messages: 20_000,
        burst: None,
    },
    Workload {
        // The control for the slow-follower section, which uses this shape:
        // without it there is no way to tell a slow follower from the volume.
        name: "sustained 1 KiB",
        payload: 1024,
        messages: 20_000,
        burst: None,
    },
    Workload {
        name: "burst 64 B",
        payload: 64,
        messages: 4_000,
        burst: Some(500),
    },
];

/// A benchmark that cannot set itself up has no result to report, so the
/// failure is loud rather than swallowed. The project's other hand-rolled
/// benchmarks do the same (storage/benches/scan.rs).
fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("{error}"))
}

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!("straum-bench-{name}-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&path);
    path
}

fn any_port() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

fn node(path: &Path, replication: ReplicationSettings) -> RuntimeConfig {
    RuntimeConfig {
        transport: TransportConfig {
            listen: any_port(),
            ..TransportConfig::default()
        },
        durability: Durability::Fsynced,
        wal: WalSettings {
            path: path.to_path_buf(),
            ..WalSettings::default()
        },
        replication,
        ..RuntimeConfig::default()
    }
}

fn percentiles(samples: &mut [u64]) -> (f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    samples.sort_unstable();
    let at = |fraction: f64| {
        let index = ((samples.len() - 1) as f64 * fraction).round() as usize;
        samples.get(index).copied().unwrap_or(0) as f64 / 1e6
    };
    (at(0.5), at(0.95), at(0.99))
}

struct Run {
    elapsed: Duration,
    /// Publish to PUBACK: what a client sees, which replication must not
    /// change in this phase.
    acked: Vec<u64>,
    /// Publish to the leader hearing the follower call it durable.
    replicated: Vec<u64>,
    leader: Snapshot,
    follower: Option<Snapshot>,
    before_leader: Snapshot,
    before_follower: Option<Snapshot>,
    payload: usize,
    messages: usize,
}

/// Publishes the workload and records both latencies.
///
/// The replication latency comes from the leader's watermark: the nth record
/// in a fresh log is sequence n, so when the watermark moves from a to b the
/// messages in that range have just become durable on the follower. Sampling
/// it costs a busy task, which is why CPU is measured in a separate pass.
async fn run(work: &Workload, leader: &Runtime, follower: Option<&Runtime>) -> Run {
    // Where the log already is. The warm-up put records in it, so sequence
    // `base + n` is this run's nth message and not the nth overall.
    let base = leader.metrics().wal_durable_sequence;
    let base_snapshot = leader.metrics();
    let before = follower.map(Runtime::metrics);
    let addr = leader.local_addr();
    let mut options = MqttOptions::new("bench", addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(30));
    options.set_max_packet_size(work.payload + 1024, work.payload + 1024);
    let (client, mut eventloop) = AsyncClient::new(options, 256);
    let driving = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });

    let sent: Arc<Vec<std::sync::Mutex<Option<Instant>>>> = Arc::new(
        (0..work.messages)
            .map(|_| std::sync::Mutex::new(None))
            .collect(),
    );

    let watching = follower.is_some().then(|| {
        let metrics = leader.metrics_handle();
        let sent = Arc::clone(&sent);
        let total = base + work.messages as u64;
        let work_messages = work.messages;
        let stop = Arc::new(AtomicU64::new(0));
        let flag = Arc::clone(&stop);
        let task = tokio::spawn(async move {
            let mut seen = base;
            let mut samples = Vec::with_capacity(work_messages);
            while seen < total && flag.load(Ordering::Relaxed) == 0 {
                let now = metrics
                    .replication_follower_sequence
                    .load(Ordering::Relaxed);
                while seen < now.min(total) {
                    if let Some(at) = sent
                        .get((seen - base) as usize)
                        .and_then(|slot| *slot.lock().ok()?)
                    {
                        samples.push(at.elapsed().as_nanos() as u64);
                    }
                    seen += 1;
                }
                tokio::task::yield_now().await;
            }
            samples
        });
        (task, stop)
    });

    let payload = vec![b'x'; work.payload];
    let started = Instant::now();
    let mut acked = Vec::with_capacity(work.messages);
    for index in 0..work.messages {
        if let Some(slot) = sent.get(index) {
            if let Ok(mut held) = slot.lock() {
                *held = Some(Instant::now());
            }
        }
        let at = Instant::now();
        client
            .publish("bench/topic", QoS::AtLeastOnce, false, payload.clone())
            .await
            .unwrap_or_else(|error| panic!("{error}"));
        acked.push(at.elapsed().as_nanos() as u64);
        if let Some(burst) = work.burst {
            if index % burst == burst - 1 {
                // A quiet moment, so the next batch is built from an idle
                // log rather than from a queue that never drained.
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
    let elapsed = started.elapsed();

    let replicated = match watching {
        None => Vec::new(),
        Some((task, stop)) => {
            let deadline = Instant::now() + Duration::from_secs(120);
            while leader.metrics().replication_follower_sequence < base + work.messages as u64 {
                if Instant::now() > deadline {
                    eprintln!("  (the follower never caught up; latencies are partial)");
                    break;
                }
                tokio::task::yield_now().await;
            }
            stop.store(1, Ordering::Relaxed);
            task.await.unwrap_or_default()
        }
    };

    ok(client.disconnect().await);
    let _ = driving.await;

    Run {
        elapsed,
        acked,
        replicated,
        leader: leader.metrics(),
        // Deltas, so the warm-up is not in the averages.
        follower: follower.map(Runtime::metrics),
        before_leader: base_snapshot,
        before_follower: before,
        payload: work.payload,
        messages: work.messages,
    }
}

fn report(name: &str, mode: &str, mut run: Run) {
    let seconds = run.elapsed.as_secs_f64();
    let (ack50, ack95, ack99) = percentiles(&mut run.acked);
    let (rep50, rep95, rep99) = percentiles(&mut run.replicated);
    let per = |total: u64, of: u64| {
        if of == 0 {
            0.0
        } else {
            total as f64 / of as f64
        }
    };

    let (follower_commits, follower_records) = match (run.follower, run.before_follower) {
        (Some(after), Some(before)) => (
            after.wal_commits.saturating_sub(before.wal_commits),
            after
                .wal_records_committed
                .saturating_sub(before.wal_records_committed),
        ),
        _ => (0, 0),
    };
    let leader_commits = run
        .leader
        .wal_commits
        .saturating_sub(run.before_leader.wal_commits);
    let leader_records = run
        .leader
        .wal_records_committed
        .saturating_sub(run.before_leader.wal_records_committed);
    let leader_bytes = run
        .leader
        .wal_bytes_committed
        .saturating_sub(run.before_leader.wal_bytes_committed);

    println!(
        "{:<16} {:<10} {:>9.0} {:>8.2} {:>7.2} {:>7.2} {:>7.2} {:>8.2} {:>8.2} {:>8.2} {:>7.1} {:>9.0} {:>7.1} {:>9}",
        name,
        mode,
        run.messages as f64 / seconds,
        (run.messages * run.payload) as f64 / seconds / 1e6,
        ack50,
        ack95,
        ack99,
        rep50,
        rep95,
        rep99,
        per(leader_records, leader_commits),
        per(leader_bytes, leader_commits),
        per(follower_records, follower_commits),
        run.leader.replication_shipments_dropped,
    );
    use std::io::Write;
    let _ = std::io::stdout().flush();
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    println!(
        "{:<16} {:<10} {:>9} {:>8} {:>7} {:>7} {:>7} {:>8} {:>8} {:>8} {:>7} {:>9} {:>7} {:>9}",
        "workload",
        "mode",
        "msg/s",
        "MB/s",
        "ack50",
        "ack95",
        "ack99",
        "rep50",
        "rep95",
        "rep99",
        "m/lcmt",
        "B/lcmt",
        "m/fcmt",
        "dropped",
    );
    println!("{}", "-".repeat(136));

    // One section at a time, so an anomaly can be chased without paying for
    // the whole table (CONSTITUTION.md §28: repeat, then counters).
    let only = std::env::var("STRAUM_BENCH_ONLY").unwrap_or_default();
    if only == "slow" {
        slow_follower().await;
        return;
    }

    for work in WORKLOADS {
        if !only.is_empty() && !work.name.contains(&only) {
            continue;
        }
        // Local only.
        let path = scratch(&format!("{}-local", work.name.replace(' ', "-")));
        let leader = ok(Runtime::start(node(&path, ReplicationSettings::default())));
        // A short warm-up so the first sync, the allocator and the page
        // cache are not what the numbers describe.
        let warm = Workload {
            name: work.name,
            payload: work.payload,
            messages: 200.min(work.messages),
            burst: None,
        };
        let _ = run(&warm, &leader, None).await;
        let measured = run(work, &leader, None).await;
        report(work.name, "local", measured);
        leader.shutdown().await;
        let _ = std::fs::remove_file(&path);

        // Leader and follower over loopback.
        let leader_path = scratch(&format!("{}-leader", work.name.replace(' ', "-")));
        let follower_path = scratch(&format!("{}-follower", work.name.replace(' ', "-")));
        let standby = ok(Runtime::start(node(
            &follower_path,
            ReplicationSettings {
                listen: Some(any_port()),
                ..ReplicationSettings::default()
            },
        )));
        let at = standby
            .replication_addr()
            .unwrap_or_else(|| panic!("no listener"));
        let leader = ok(Runtime::start(node(
            &leader_path,
            ReplicationSettings {
                follower: Some(at),
                ..ReplicationSettings::default()
            },
        )));
        let _ = run(&warm, &leader, Some(&standby)).await;
        let measured = run(work, &leader, Some(&standby)).await;
        report(work.name, "replicated", measured);
        leader.shutdown().await;
        standby.shutdown().await;
        let _ = std::fs::remove_file(&leader_path);
        let _ = std::fs::remove_file(&follower_path);
    }

    if only.is_empty() {
        slow_follower().await;
    }
}

/// A follower whose durability is slower than the leader's publishing.
///
/// The follower here is this process reading the socket and acknowledging
/// nothing, which is what a node with a slower disk looks like from the
/// leader. What is checked is not that the two keep pace — they are not
/// expected to — but that nothing grows with the producer.
async fn slow_follower() {
    use std::io::Write;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = ok(tokio::net::TcpListener::bind(any_port()).await);
    let at = ok(listener.local_addr());
    let path = scratch("slow");
    let leader = ok(Runtime::start(node(
        &path,
        ReplicationSettings {
            follower: Some(at),
            ..ReplicationSettings::default()
        },
    )));

    let (mut stream, _) = ok(listener.accept().await);
    let mut out = Vec::new();
    replication::encode_durable(0, &mut out);
    ok(stream.write_all(&out).await);

    let draining = tokio::spawn(async move {
        let mut sink = vec![0u8; 64 * 1024];
        while let Ok(read) = stream.read(&mut sink).await {
            if read == 0 {
                break;
            }
        }
    });

    let work = Workload {
        name: "slow follower",
        payload: 1024,
        messages: 20_000,
        burst: None,
    };
    let started = Instant::now();
    let _ = run(&work, &leader, None).await;
    let seconds = started.elapsed().as_secs_f64();
    let seen = leader.metrics();
    let capacity = ReplicationSettings::default().queue as u64;

    println!("\nslow follower: 20000 x 1 KiB, the follower acknowledging nothing");
    println!(
        "  leader throughput         {:.0} msg/s",
        work.messages as f64 / seconds
    );
    println!(
        "  follower durable          {}",
        seen.replication_follower_sequence
    );
    println!("  in flight, sequences      {}", seen.replication_in_flight);
    println!(
        "  shipments dropped         {}",
        seen.replication_shipments_dropped
    );
    println!("  leader commits            {}", seen.wal_commits);
    println!("  catch-ups from the log    {}", seen.replication_catchups);
    println!(
        "  last sync                 {:.2} ms",
        seen.wal_sync_nanos as f64 / 1e6
    );
    println!(
        "  per commit                {:.1} ms",
        seconds * 1000.0 / seen.wal_commits.max(1) as f64
    );
    println!(
        "  dropped + queue >= commits {} + {} >= {}",
        seen.replication_shipments_dropped, capacity, seen.wal_commits
    );
    let _ = std::io::stdout().flush();

    leader.shutdown().await;
    let _ = draining.await;
    let _ = std::fs::remove_file(&path);
}
