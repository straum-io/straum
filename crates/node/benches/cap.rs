//! The `max_uncommitted` cap against throughput and failover (D-84 A).
//!
//! Three nodes on loopback, logs on disk. For each cap:
//!
//! * **throughput** — a closed loop keeps `STRAUM_BENCH_WINDOW` publishes
//!   in flight at the sequencer (below its proposal queue, so none is
//!   refused) and counts those it reports committed (`Effect::Accepted`)
//!   over `STRAUM_BENCH_SECONDS`;
//! * **failover** — under that load the sequencer is stopped, and the time
//!   to a commit in a later slot on the two others is measured;
//!   `STRAUM_BENCH_FAILOVERS` times, the stopped node restarted between;
//!   with the largest survivor lag, sampled just before the stop (watch
//!   channels of separate nodes, not one instant) — what the new majority
//!   must close before it commits.
//!
//! The target (D-84 A): at least 9 failovers in 10 commit within 1 s; the
//! cap chosen is the one with the highest throughput that meets it. Only a
//! quiet Linux host gives numbers for that choice (CLAUDE.md §11); anything
//! from macOS or a container is preliminary.
//!
//! `cargo bench -p node --bench cap`; `STRAUM_BENCH_CAPS=64,4096` narrows
//! it, `STRAUM_BENCH_HEARTBEAT_MS` and `STRAUM_BENCH_TAKEOVER_TICKS` set
//! the timer that starts a takeover (50 ms and 3 by default).
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use apply::{Effect, Limits, Origin};
use bytes::Bytes;
use domain::{DeliveryGuarantee, Topic};
use node::{Node, NodeConfig};
use storage::{SegmentLimits, WalLimits};
use transport::TransportConfig;

const NODES: u8 = 3;
/// The largest survivor lag sampled just before a stop, in records, for
/// the cap being measured.
static LAG: AtomicU64 = AtomicU64::new(0);

fn env<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn free_port() -> SocketAddr {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

fn data_dir(cap: u64, id: u8) -> PathBuf {
    std::env::temp_dir().join(format!(
        "straum-bench-cap-{}-{cap}-{id}",
        std::process::id()
    ))
}

fn config(cap: u64, id: u8, addresses: &[SocketAddr]) -> NodeConfig {
    NodeConfig {
        core: cluster::Config {
            id,
            nodes: (0..NODES).collect(),
            owner: 0,
            slot: 1,
            window: 64,
            max_queue: 1024,
            max_batch: 64,
            recent_batches: 16,
            max_uncommitted: cap,
            takeover_ticks: env("STRAUM_BENCH_TAKEOVER_TICKS", 3),
            lag_bound: None,
        },
        listen: addresses[usize::from(id)],
        peers: (0..NODES)
            .filter(|&p| p != id)
            .map(|p| (p, addresses[usize::from(p)]))
            .collect(),
        data: data_dir(cap, id),
        segments: SegmentLimits {
            wal: WalLimits::default(),
            segment_bytes: 64 << 20,
        },
        limits: Limits::default(),
        heartbeat: Duration::from_millis(env("STRAUM_BENCH_HEARTBEAT_MS", 50)),
        // Holds the largest batch: 64 records of the default 1 MiB packet.
        max_frame: 1 << 27,
        link_queue: 1024,
        transport: TransportConfig {
            listen: addresses[usize::from(id)],
            ..TransportConfig::default()
        },
        mqtt: None,
        retain_bytes: 256 << 20,
        // Rare: a snapshot encodes the whole state on the node loop.
        snapshot_every: 1 << 30,
        rotate_every: None,
        read_bytes: 64 * 1024,
    }
}

fn publish(at: u8, request: u64) -> cluster::Proposal {
    let proposal = apply::Proposal::Publish {
        origin: Origin {
            node: at,
            session: 1,
        },
        request,
        topic: Topic::new(Bytes::from_static(b"bench")).unwrap(),
        payload: Bytes::from_static(&[7; 64]),
        properties: Bytes::new(),
        guarantee: DeliveryGuarantee::AtMostOnce,
        retain: false,
    };
    let mut body = Vec::new();
    proposal.encode(&mut body).unwrap();
    cluster::Proposal {
        kind: proposal.kind(),
        properties: Vec::new(),
        body,
    }
}

struct Cluster {
    cap: u64,
    addresses: Vec<SocketAddr>,
    nodes: Vec<Option<Node>>,
}

impl Cluster {
    async fn start(cap: u64) -> Self {
        let addresses: Vec<SocketAddr> = (0..NODES).map(|_| free_port()).collect();
        let mut nodes = Vec::new();
        for id in 0..NODES {
            let _ = std::fs::remove_dir_all(data_dir(cap, id));
            nodes.push(Some(
                Node::start(config(cap, id, &addresses)).await.unwrap(),
            ));
        }
        Self {
            cap,
            addresses,
            nodes,
        }
    }

    /// The node leading, once one does.
    async fn leader(&self, deadline: Duration) -> Option<u8> {
        let until = Instant::now() + deadline;
        while Instant::now() < until {
            for (id, node) in self.nodes.iter().enumerate() {
                if node.as_ref().is_some_and(|n| n.status().borrow().leading) {
                    return Some(id as u8);
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        None
    }

    async fn shutdown(mut self) {
        for (id, node) in self.nodes.iter_mut().enumerate() {
            if let Some(node) = node.take() {
                node.shutdown().await;
            }
            let _ = std::fs::remove_dir_all(data_dir(self.cap, id as u8));
        }
    }
}

/// Publishes at `at` in a closed loop of `window` until `stop`; the count
/// committed goes back through the handle.
fn load(
    node: &Node,
    at: u8,
    window: u64,
    stop: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<u64> {
    let mut effects = node.effects();
    let proposals: Vec<cluster::Proposal> = (0..window).map(|r| publish(at, r)).collect();
    let sender = node.proposer();
    tokio::spawn(async move {
        let mut next = window;
        for proposal in proposals {
            let _ = sender.send(proposal).await;
        }
        let mut committed = 0u64;
        loop {
            if *stop.borrow() {
                return committed;
            }
            let received = tokio::time::timeout(Duration::from_millis(50), effects.recv()).await;
            let Ok(Ok((_, out))) = received else { continue };
            for effect in out {
                if let Effect::Accepted { to, .. } = effect {
                    if to.node == at {
                        committed += 1;
                        let _ = sender.send(publish(at, next)).await;
                        next += 1;
                    }
                }
            }
        }
    })
}

async fn throughput(cluster: &Cluster, window: u64, seconds: u64) -> f64 {
    let leader = cluster
        .leader(Duration::from_secs(10))
        .await
        .expect("no sequencer");
    let node = cluster.nodes[usize::from(leader)].as_ref().unwrap();
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let task = load(node, leader, window, stop);
    let start = Instant::now();
    tokio::time::sleep(Duration::from_secs(seconds)).await;
    let _ = stop_tx.send(true);
    let committed = task.await.unwrap();
    committed as f64 / start.elapsed().as_secs_f64()
}

/// Stops the sequencer under load; the time until a later slot commits on
/// the others, or `None` past 10 s.
async fn failover(cluster: &mut Cluster, window: u64) -> Option<Duration> {
    let leader = cluster.leader(Duration::from_secs(10)).await?;
    let node = cluster.nodes[usize::from(leader)].as_ref().unwrap();
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let task = load(node, leader, window, stop);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let before = *node.status().borrow();
    // How far the slower survivor's log was behind the sequencer's commit
    // point: a new slot commits only once the new majority holds it all.
    let lag = (0..NODES)
        .filter(|&id| id != leader)
        .map(|id| {
            let s = *cluster.nodes[usize::from(id)]
                .as_ref()
                .unwrap()
                .status()
                .borrow();
            before.commit.saturating_sub(s.end)
        })
        .max()
        .unwrap_or(0);
    LAG.fetch_max(lag, Ordering::Relaxed);
    // Stopped while the load still runs: the clock includes nothing but the
    // stop and the takeover. A process kill is not available in-process;
    // `shutdown` aborts the node's tasks and joins its log thread.
    let start = Instant::now();
    cluster.nodes[usize::from(leader)]
        .take()
        .unwrap()
        .shutdown()
        .await;
    let _ = stop_tx.send(true);
    let _ = task.await;
    // Proposals keep coming at the survivors: a new slot commits its own
    // records, not only its `SlotStart`.
    let survivors: Vec<u8> = (0..NODES).filter(|&id| id != leader).collect();
    let mut request = 1u64 << 40;
    let taken = loop {
        if start.elapsed() > Duration::from_secs(10) {
            break None;
        }
        let moved = survivors.iter().any(|&id| {
            let s = *cluster.nodes[usize::from(id)]
                .as_ref()
                .unwrap()
                .status()
                .borrow();
            s.slot > before.slot && s.commit > before.commit && s.leading
        });
        if moved {
            break Some(start.elapsed());
        }
        for &id in &survivors {
            let n = cluster.nodes[usize::from(id)].as_ref().unwrap();
            n.propose(publish(id, request)).await;
            request += 1;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    };
    let restarted = Node::start(config(cluster.cap, leader, &cluster.addresses))
        .await
        .unwrap();
    cluster.nodes[usize::from(leader)] = Some(restarted);
    // Settle before the next trial.
    tokio::time::sleep(Duration::from_secs(1)).await;
    taken
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if cfg!(debug_assertions) {
        eprintln!("cap: built without optimisation; use `cargo bench` or a --release build");
        return;
    }
    let caps: Vec<u64> = std::env::var("STRAUM_BENCH_CAPS")
        .unwrap_or_else(|_| "64,96,128,256,1024,4096".to_owned())
        .split(',')
        .filter_map(|c| c.trim().parse().ok())
        .collect();
    let window: u64 = env("STRAUM_BENCH_WINDOW", 512);
    let seconds: u64 = env("STRAUM_BENCH_SECONDS", 3);
    let rounds: usize = env("STRAUM_BENCH_ROUNDS", 3);
    let failovers: usize = env("STRAUM_BENCH_FAILOVERS", 10);
    println!(
        "cap bench: {NODES} nodes on loopback, 64 B publishes, window {window}, {seconds} s x {rounds} rounds, {failovers} failovers; heartbeat {} ms, takeover after {} heartbeats",
        env::<u64>("STRAUM_BENCH_HEARTBEAT_MS", 50),
        env::<u32>("STRAUM_BENCH_TAKEOVER_TICKS", 3)
    );
    println!(
        "cap      publishes/s (median)   failovers within 1 s   failover p50 / max (ms)   largest survivor lag before the stop"
    );
    for cap in caps {
        LAG.store(0, Ordering::Relaxed);
        let mut cluster = Cluster::start(cap).await;
        let mut rates = Vec::new();
        for _ in 0..rounds {
            rates.push(throughput(&cluster, window, seconds).await);
        }
        rates.sort_by(f64::total_cmp);
        let mut times = Vec::new();
        for _ in 0..failovers {
            times.push(failover(&mut cluster, window).await);
        }
        let within = times
            .iter()
            .filter(|t| t.is_some_and(|t| t <= Duration::from_secs(1)))
            .count();
        let mut done: Vec<u128> = times.iter().flatten().map(Duration::as_millis).collect();
        done.sort_unstable();
        println!(
            "{cap:<8} {:>20.0}   {within:>8} of {failovers:<10}   {:>8} / {:<8}   {}",
            rates[rates.len() / 2],
            done.get(done.len() / 2)
                .map_or("-".to_owned(), u128::to_string),
            done.last().map_or("-".to_owned(), u128::to_string),
            LAG.load(Ordering::Relaxed),
        );
        cluster.shutdown().await;
    }
}
