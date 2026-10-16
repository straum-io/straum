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
use std::fmt::Write as _;

use node::metrics::{Stage, Stamp, Trace};
use node::{Node, NodeConfig};
use storage::{SegmentLimits, WalLimits};
use transport::TransportConfig;

const NODES: u8 = 3;
/// The largest survivor lag sampled just before a stop, in records, for
/// the cap being measured.
static LAG: AtomicU64 = AtomicU64::new(0);
/// The replication window: 64 for the cap runs; the catch-up mode sets it
/// (D-133 B).
static WINDOW: AtomicU64 = AtomicU64::new(64);
/// The heartbeat, ms: 0 for `STRAUM_BENCH_HEARTBEAT_MS` (the cap runs); the
/// catch-up mode sets it (D-135 A).
static HEARTBEAT_MS: AtomicU64 = AtomicU64::new(0);

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
            // 64 for the cap runs; the catch-up mode sets it (D-133 B).
            window: WINDOW.load(Ordering::Relaxed),
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
        heartbeat: Duration::from_millis(match HEARTBEAT_MS.load(Ordering::Relaxed) {
            0 => env("STRAUM_BENCH_HEARTBEAT_MS", 50),
            ms => ms,
        }),
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
        metrics: None,
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
            let Ok(Ok((_, _, out))) = received else {
                continue;
            };
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
/// One failover (D-130): how long it took, and what the survivors did
/// meanwhile — slots the cluster moved on (more than one: a candidate's
/// turn passed), takeovers they abandoned, and the slower survivor's lag
/// behind the commit point at the stop.
struct Failover {
    took: Option<Duration>,
    slots: u64,
    abandoned: u64,
    lag: u64,
}

async fn abandoned(cluster: &Cluster, survivors: &[u8]) -> u64 {
    let mut total = 0;
    for &id in survivors {
        if let Some(m) = cluster.nodes[usize::from(id)]
            .as_ref()
            .unwrap()
            .metrics()
            .await
        {
            total += m.takeovers_abandoned;
        }
    }
    total
}

async fn failover(cluster: &mut Cluster, window: u64) -> Option<Failover> {
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
    let survivors: Vec<u8> = (0..NODES).filter(|&id| id != leader).collect();
    let abandoned_before = abandoned(cluster, &survivors).await;
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
    let slot = survivors
        .iter()
        .map(|&id| {
            cluster.nodes[usize::from(id)]
                .as_ref()
                .unwrap()
                .status()
                .borrow()
                .slot
        })
        .max()
        .unwrap_or(before.slot);
    let result = Failover {
        took: taken,
        slots: slot.saturating_sub(before.slot),
        abandoned: abandoned(cluster, &survivors)
            .await
            .saturating_sub(abandoned_before),
        lag,
    };
    let restarted = Node::start(config(cluster.cap, leader, &cluster.addresses))
        .await
        .unwrap();
    cluster.nodes[usize::from(leader)] = Some(restarted);
    // Settle before the next trial.
    tokio::time::sleep(Duration::from_secs(1)).await;
    Some(result)
}

/// A node's own lag behind the commit point it heard (D-132).
async fn own_lag(cluster: &Cluster, id: u8) -> Option<u64> {
    cluster.nodes[usize::from(id)]
        .as_ref()?
        .metrics()
        .await
        .map(|m| m.own.lag)
}

/// As `load`, timing each proposal from its send to its acceptance while
/// `measuring` is true: raw commit-latency samples, in µs (D-135 A).
fn load_timed(
    node: &Node,
    at: u8,
    window: u64,
    stop: tokio::sync::watch::Receiver<bool>,
    measuring: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<Vec<u32>> {
    let mut effects = node.effects();
    let sender = node.proposer();
    tokio::spawn(async move {
        let mut sent: std::collections::HashMap<u64, Instant> = std::collections::HashMap::new();
        for r in 0..window {
            sent.insert(r, Instant::now());
            let _ = sender.send(publish(at, r)).await;
        }
        let mut next = window;
        let mut samples = Vec::with_capacity(1 << 20);
        loop {
            if *stop.borrow() {
                return samples;
            }
            let received = tokio::time::timeout(Duration::from_millis(50), effects.recv()).await;
            let Ok(Ok((_, _, out))) = received else {
                continue;
            };
            for effect in out {
                if let Effect::Accepted { to, request, .. } = effect {
                    if to.node != at {
                        continue;
                    }
                    if let Some(t) = sent.remove(&request) {
                        if *measuring.borrow() {
                            samples
                                .push(u32::try_from(t.elapsed().as_micros()).unwrap_or(u32::MAX));
                        }
                    }
                    sent.insert(next, Instant::now());
                    let _ = sender.send(publish(at, next)).await;
                    next += 1;
                }
            }
        }
    })
}

/// Stamps a node keeps per trial (D-136 A): about 20 MB at most; a trial
/// records far fewer, and any dropped are reported.
const TRACE_STAMPS: usize = 1 << 19;

/// One append's stage durations, ms; `None` where its path has none.
type Durations = [Option<f64>; 13];

/// One node's stamps of one stage, in time order.
struct Stamps(Vec<Stamp>);

impl Stamps {
    fn of(traces: &[(u8, Trace)], node: u8, stage: Stage) -> Self {
        let mut v: Vec<Stamp> = traces
            .iter()
            .filter(|(id, _)| *id == node)
            .flat_map(|(_, t)| t.stamps.iter().filter(|s| s.stage == stage).copied())
            .collect();
        v.sort_by_key(|s| s.at);
        Self(v)
    }

    /// The first at or after `from` that `matches`.
    fn after(&self, from: tokio::time::Instant, matches: impl Fn(&Stamp) -> bool) -> Option<Stamp> {
        let start = self.0.partition_point(|s| s.at < from);
        self.0[start..].iter().find(|s| matches(s)).copied()
    }

    /// The last at or before `to` that `matches`.
    fn before(&self, to: tokio::time::Instant, matches: impl Fn(&Stamp) -> bool) -> Option<Stamp> {
        let end = self.0.partition_point(|s| s.at <= to);
        self.0[..end].iter().rev().find(|s| matches(s)).copied()
    }
}

/// D-136 A: each append the sequencer shipped to a replica, followed to
/// the durable report that covers it — per stage, matched by position
/// over this process's one clock. `W` marks waiting (in a queue, a
/// channel, the link), `K` work (the loop, the log thread's read, write
/// and sync). From the log (a read before the send) or from memory; to the
/// replica restarted (`lagging`; in the control, the one that would be) or
/// the other. One row each: samples, and p50/p90/p99 per stage in ms.
fn stages(traces: &[(u8, Trace)], sequencer: u8, lagging: u8) -> String {
    const NAMES: [&str; 13] = [
        "read_queue_W",
        "read_K",
        "read_return_W",
        "ship_K",
        "link_out_W",
        "replica_loop_K",
        "append_queue_W",
        "write_K",
        "sync_K",
        "durable_return_W",
        "report_K",
        "link_back_W",
        "next_ask_K",
    ];
    let s = |stage| Stamps::of(traces, sequencer, stage);
    let (asked, started, done, handled, shipped, received_back) = (
        s(Stage::ReadAsked),
        s(Stage::ReadStarted),
        s(Stage::ReadDone),
        s(Stage::ReadHandled),
        s(Stage::Shipped),
        s(Stage::ReportReceived),
    );
    let mut out = String::new();
    for peer in (0..NODES).filter(|&p| p != sequencer) {
        let r = |stage| Stamps::of(traces, peer, stage);
        let (received, appended, wal_started, written, synced, durable, sent) = (
            r(Stage::Received),
            r(Stage::Appended),
            r(Stage::WalStarted),
            r(Stage::WalWritten),
            r(Stage::Synced),
            r(Stage::DurableHandled),
            r(Stage::ReportSent),
        );
        // Per path: the stage durations (None where the path has none), and
        // the round trip.
        let mut rows: [(Vec<Durations>, Vec<f64>); 2] = Default::default();
        for ship in shipped.0.iter().filter(|x| x.peer == peer) {
            let (f, l) = (ship.first, ship.last);
            let ms = |a: tokio::time::Instant, b: tokio::time::Instant| {
                b.saturating_duration_since(a).as_secs_f64() * 1000.0
            };
            let mut d = [None; 13];
            // From the log: this append is the answer to a read.
            let read = handled
                .before(ship.at, |x| x.peer == peer && x.first == f && x.last == l)
                .and_then(|h| {
                    let dn = done.before(h.at, |x| x.peer == peer && x.first == f)?;
                    let st = started.before(dn.at, |x| x.peer == peer && x.first == f)?;
                    let ask = asked.before(st.at, |x| x.peer == peer && x.first == f)?;
                    Some((ask, st, dn, h))
                });
            if let Some((ask, st, dn, h)) = read {
                d[0] = Some(ms(ask.at, st.at));
                d[1] = Some(ms(st.at, dn.at));
                d[2] = Some(ms(dn.at, h.at));
                d[3] = Some(ms(h.at, ship.at));
            }
            let chain = (|| {
                let rcv = received.after(ship.at, |x| {
                    x.peer == sequencer && x.first == f && x.last == l
                })?;
                let app = appended.after(rcv.at, |x| x.first == f && x.last == l)?;
                let syn = synced.after(app.at, |x| x.last >= l)?;
                let ws = wal_started.before(syn.at, |x| x.last == syn.last)?;
                let wr = written.before(syn.at, |x| x.last == syn.last)?;
                let dh = durable.after(syn.at, |x| x.last == syn.last)?;
                let rs = sent.after(dh.at, |x| x.peer == sequencer && x.last >= l)?;
                let rr = received_back.after(rs.at, |x| x.peer == peer && x.last == rs.last)?;
                Some((rcv, app, ws, wr, syn, dh, rs, rr))
            })();
            let Some((rcv, app, ws, wr, syn, dh, rs, rr)) = chain else {
                continue;
            };
            d[4] = Some(ms(ship.at, rcv.at));
            d[5] = Some(ms(rcv.at, app.at));
            // A group taken up before this append arrived in it: its wait
            // is 0 and its write starts at the hand-over.
            let write_from = ws.at.max(app.at);
            d[6] = Some(ms(app.at, write_from));
            d[7] = Some(ms(write_from, wr.at));
            d[8] = Some(ms(wr.at, syn.at));
            d[9] = Some(ms(syn.at, dh.at));
            d[10] = Some(ms(dh.at, rs.at));
            d[11] = Some(ms(rs.at, rr.at));
            let path = match read {
                Some((ask, ..)) => {
                    // The next read is asked once this one is shipped, not
                    // once it is reported: reads follow one another, one
                    // outstanding (the smoke run's raw stamps).
                    if let Some(next) = asked.after(ship.at, |x| x.peer == peer && x.first == l + 1)
                    {
                        d[12] = Some(ms(ship.at, next.at));
                    }
                    rows[0].1.push(ms(ask.at, rr.at));
                    0
                }
                None => {
                    rows[1].1.push(ms(ship.at, rr.at));
                    1
                }
            };
            rows[path].0.push(d);
        }
        let role = if peer == lagging {
            "lagging"
        } else {
            "healthy"
        };
        for (path, (samples, totals)) in ["log", "memory"].iter().zip(rows.iter_mut()) {
            if totals.is_empty() {
                continue;
            }
            let q = |v: &mut Vec<f64>| {
                v.sort_by(f64::total_cmp);
                let at = |p: f64| v[((v.len() as f64 * p) as usize).min(v.len() - 1)];
                format!("{:.2}/{:.2}/{:.2}", at(0.5), at(0.9), at(0.99))
            };
            let _ = write!(
                out,
                "peer={role} path={path} n={} round_trip={}",
                totals.len(),
                q(totals)
            );
            for (i, name) in NAMES.iter().enumerate() {
                let mut v: Vec<f64> = samples.iter().filter_map(|d| d[i]).collect();
                if !v.is_empty() {
                    let _ = write!(out, " {name}={}", q(&mut v));
                }
            }
            if *path == "log" {
                // From one read asked to the next, and what each brought.
                let reads: Vec<&Stamp> = asked.0.iter().filter(|x| x.peer == peer).collect();
                let mut cycles: Vec<f64> = reads
                    .windows(2)
                    .map(|w| w[1].at.saturating_duration_since(w[0].at).as_secs_f64() * 1000.0)
                    .collect();
                let brought: Vec<u64> = handled
                    .0
                    .iter()
                    .filter(|x| x.peer == peer && x.last >= x.first)
                    .map(|x| x.last - x.first + 1)
                    .collect();
                if !cycles.is_empty() {
                    let _ = write!(
                        out,
                        " read_cycle={} positions_per_read_mean={}",
                        q(&mut cycles),
                        brought.iter().sum::<u64>() / (brought.len() as u64).max(1)
                    );
                }
            }
            out.push('\n');
        }
    }
    let dropped: u64 = traces.iter().map(|(_, t)| t.dropped).sum();
    if dropped > 0 {
        let _ = writeln!(out, "stamps_dropped={dropped}");
    }
    out
}

/// p50/p90/p99 of `v`, sorted in place.
fn quantiles(v: &mut [f64]) -> String {
    if v.is_empty() {
        return "-".to_owned();
    }
    v.sort_by(f64::total_cmp);
    let at = |p: f64| v[((v.len() as f64 * p) as usize).min(v.len() - 1)];
    format!("{:.2}/{:.2}/{:.2}", at(0.5), at(0.9), at(0.99))
}

/// Time `a` to `b`, ms; 0 when `b` is earlier.
fn ms(a: tokio::time::Instant, b: tokio::time::Instant) -> f64 {
    b.saturating_duration_since(a).as_secs_f64() * 1000.0
}

/// How much of `[from, to)` the intervals cover; `spans` sorted by start,
/// one thread's, so they do not overlap one another.
fn covered(
    spans: &[(tokio::time::Instant, tokio::time::Instant)],
    from: tokio::time::Instant,
    to: tokio::time::Instant,
) -> f64 {
    let end = spans.partition_point(|s| s.0 < to);
    spans[..end]
        .iter()
        .rev()
        .take_while(|s| s.1 > from || s.0 >= from)
        .map(|s| ms(s.0.max(from), s.1.min(to)))
        .sum()
}

/// The intervals of one node's activity on its log thread: each `start`
/// stamp with the next `end` stamp at or after it.
fn spans(
    traces: &[(u8, Trace)],
    node: u8,
    start: Stage,
    end: Stage,
) -> Vec<(tokio::time::Instant, tokio::time::Instant)> {
    let (starts, ends) = (
        Stamps::of(traces, node, start),
        Stamps::of(traces, node, end),
    );
    starts
        .0
        .iter()
        .filter_map(|s| {
            ends.after(s.at, |e| {
                e.last == s.last || matches!(start, Stage::RangeStarted | Stage::ReadStarted)
            })
            .map(|e| (s.at, e.at))
        })
        .collect()
}

/// D-139 A, D-142 A: how long a catch-up read takes, and what holds the
/// lagging replica's log thread while its appends wait.
///
/// `scan` row: the sequencer's reads for the lagging replica, from the log
/// thread taking one up to finishing it.
///
/// `blocking` row: each append the lagging replica handed to its log
/// thread, and the wait until a group took it up; of all that waiting,
/// how much overlapped the thread's earlier groups (append and sync),
/// read-backs for apply (`Range`), reads for others, and none of these.
fn scans(traces: &[(u8, Trace)], sequencer: u8, lagging: u8) -> String {
    let mut out = String::new();
    let mut read: Vec<f64> = spans(traces, sequencer, Stage::ReadStarted, Stage::ReadDone)
        .iter()
        .map(|s| ms(s.0, s.1))
        .collect();
    if !read.is_empty() {
        let _ = writeln!(
            out,
            "scan reads={} read_ms={}",
            read.len(),
            quantiles(&mut read)
        );
    }
    // The lagging replica's log thread.
    let groups = spans(traces, lagging, Stage::WalStarted, Stage::Synced);
    let ranges = spans(traces, lagging, Stage::RangeStarted, Stage::RangeDone);
    let reads = spans(traces, lagging, Stage::ReadStarted, Stage::ReadDone);
    let r = |stage| Stamps::of(traces, lagging, stage);
    let (appended, wal_started, synced) =
        (r(Stage::Appended), r(Stage::WalStarted), r(Stage::Synced));
    let (mut total, mut by_groups, mut by_ranges, mut by_reads, mut waited) =
        (0.0, 0.0, 0.0, 0.0, 0usize);
    let mut waits = Vec::new();
    for a in &appended.0 {
        let Some(syn) = synced.after(a.at, |x| x.last >= a.last) else {
            continue;
        };
        let Some(ws) = wal_started.before(syn.at, |x| x.last == syn.last) else {
            continue;
        };
        let w = ms(a.at, ws.at);
        waits.push(w);
        if w <= 0.0 {
            continue;
        }
        waited += 1;
        total += w;
        by_groups += covered(&groups, a.at, ws.at);
        by_ranges += covered(&ranges, a.at, ws.at);
        by_reads += covered(&reads, a.at, ws.at);
    }
    let mut range_ms: Vec<f64> = ranges.iter().map(|s| ms(s.0, s.1)).collect();
    let range_records =
        r(Stage::RangeDone).0.iter().map(|x| x.first).sum::<u64>() / (ranges.len().max(1) as u64);
    let _ = writeln!(
        out,
        "blocking peer=lagging appends={} waited={waited} wait_ms={} wait_total_ms={total:.0} by_groups_ms={by_groups:.0} by_range_ms={by_ranges:.0} by_reads_ms={by_reads:.0} unattributed_ms={:.0} ranges={} range_ms={} range_records_mean={range_records}",
        waits.len(),
        quantiles(&mut waits),
        (total - by_groups - by_ranges - by_reads).max(0.0),
        ranges.len(),
        quantiles(&mut range_ms),
    );
    out
}

/// D-133 B, D-135 A: one catch-up trial at a replication window and a
/// heartbeat. A replica is stopped for 2 s while the sequencer commits,
/// started again, and read every second for 20 s under load, then 10 s
/// after the load stops: its own lag (D-132); per heartbeat, the positions
/// it moved and the heartbeats it did not; the bytes and positions in
/// flight to it, as the sequencer counts them; and the sequencer's commit
/// latency from raw samples over the same 20 s. A different harness from
/// `server/benches/puback.rs`: node handles, no MQTT, so no PUBACK; a stop
/// is `shutdown`, not a kill. Flow control stays off.
async fn catch_up(window: u64, heartbeat_ms: u64, restart: bool) -> Option<(String, String)> {
    WINDOW.store(window, Ordering::Relaxed);
    HEARTBEAT_MS.store(heartbeat_ms, Ordering::Relaxed);
    let mut cluster = Cluster::start(64).await;
    let leader = cluster.leader(Duration::from_secs(10)).await?;
    let replica = (leader + 1) % NODES;
    let (stop_tx, stop) = tokio::sync::watch::channel(false);
    let (measure_tx, measuring) = tokio::sync::watch::channel(false);
    let task = {
        let node = cluster.nodes[usize::from(leader)].as_ref()?;
        load_timed(node, leader, 512, stop, measuring)
    };
    tokio::time::sleep(Duration::from_secs(2)).await;
    // D-136 A: the control keeps every node up, the same timing otherwise.
    if restart {
        cluster.nodes[usize::from(replica)].take()?.shutdown().await;
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    if restart {
        let restarted = Node::start(config(64, replica, &cluster.addresses))
            .await
            .ok()?;
        cluster.nodes[usize::from(replica)] = Some(restarted);
    }
    for node in cluster.nodes.iter().flatten() {
        node.trace_start(TRACE_STAMPS).await;
    }
    let _ = measure_tx.send(true);
    let mut samples: Vec<(f64, u64)> = Vec::new();
    let (mut flight_sum, mut flight_max, mut positions_sum, mut flight_n) =
        (0u64, 0u64, 0u64, 0u64);
    let started = Instant::now();
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(l) = own_lag(&cluster, replica).await {
            samples.push((started.elapsed().as_secs_f64(), l));
        }
        let sequencer = cluster.nodes[usize::from(leader)].as_ref()?.metrics().await;
        if let Some((_, f)) = sequencer
            .as_ref()
            .and_then(|m| m.flights.iter().find(|(p, _)| *p == replica))
        {
            flight_sum += f.bytes;
            positions_sum += f.positions;
            flight_max = flight_max.max(f.max_bytes);
            flight_n += 1;
        }
    }
    let _ = measure_tx.send(false);
    let mut traces = Vec::new();
    for (id, node) in cluster.nodes.iter().enumerate() {
        if let Some(trace) = node.as_ref()?.trace_take().await {
            traces.push((id as u8, trace));
        }
    }
    let progress = cluster.nodes[usize::from(replica)]
        .as_ref()?
        .metrics()
        .await
        .map(|m| m.own);
    let _ = stop_tx.send(true);
    let mut latencies = task.await.unwrap_or_default();
    let at_stop = own_lag(&cluster, replica).await.unwrap_or(0);
    tokio::time::sleep(Duration::from_secs(10)).await;
    let later = own_lag(&cluster, replica).await.unwrap_or(0);
    cluster.shutdown().await;
    latencies.sort_unstable();
    let quantile = |q: f64| {
        latencies
            .get(((latencies.len() as f64 * q) as usize).min(latencies.len().saturating_sub(1)))
            .map_or("-".to_owned(), |&us| {
                format!("{:.3}", f64::from(us) / 1000.0)
            })
    };
    let rate = match (samples.first(), samples.last()) {
        (Some(&(t0, l0)), Some(&(t1, l1))) if t1 > t0 => (l1 as f64 - l0 as f64) / (t1 - t0),
        _ => 0.0,
    };
    let (per_tick, still) = progress.map_or((0.0, 0.0), |p| {
        (
            p.advanced as f64 / p.ticks.max(1) as f64,
            p.still_ticks as f64 / p.ticks.max(1) as f64,
        )
    });
    // Caught up before the load stopped: nothing left to recover.
    let recovery = if at_stop == 0 {
        "caught_up".to_owned()
    } else {
        format!("{:.0}", (at_stop as f64 - later as f64) / 10.0)
    };
    let run = if restart { "catchup" } else { "control" };
    let stages = stages(&traces, leader, replica) + &scans(&traces, leader, replica);
    // The raw stamps, for a stage the rows do not explain:
    // `STRAUM_BENCH_TRACE_DIR` writes one CSV per trial (node, stage, peer,
    // first, last, µs from the earliest stamp).
    if let Ok(dir) = std::env::var("STRAUM_BENCH_TRACE_DIR") {
        let origin = traces
            .iter()
            .flat_map(|(_, t)| t.stamps.iter().map(|s| s.at))
            .min();
        if let Some(origin) = origin {
            let mut csv = String::new();
            for (id, t) in &traces {
                for s in &t.stamps {
                    let _ = writeln!(
                        csv,
                        "{id},{:?},{},{},{},{}",
                        s.stage,
                        s.peer,
                        s.first,
                        s.last,
                        s.at.duration_since(origin).as_micros()
                    );
                }
            }
            let name = format!(
                "{dir}/{run}-w{window}-hb{heartbeat_ms}-{}.csv",
                std::process::id()
            );
            let _ = std::fs::write(name, csv);
        }
    }
    Some((
        format!(
            "{run} window={window} heartbeat_ms={heartbeat_ms} lag_first={} lag_rate_under_load={rate:.0} lag_at_stop={at_stop} recovery_rate={recovery} \
         positions_per_tick={per_tick:.0} still_tick_share={still:.3} bytes_in_flight_mean={} bytes_in_flight_max={flight_max} positions_in_flight_mean={} \
         commit_samples={} commit_p50_ms={} commit_p99_ms={} commit_p999_ms={} commit_max_ms={}",
            samples.first().map_or(0, |s| s.1),
            flight_sum / flight_n.max(1),
            positions_sum / flight_n.max(1),
            latencies.len(),
            quantile(0.5),
            quantile(0.99),
            quantile(0.999),
            quantile(1.0),
        ),
        stages,
    ))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    if cfg!(debug_assertions) {
        eprintln!("cap: built without optimisation; use `cargo bench` or a --release build");
        return;
    }
    // D-133 B: `STRAUM_BENCH_MODE=catchup` measures catch-up at each
    // replication window instead, `STRAUM_BENCH_WINDOWS` (256,1024,4096),
    // `STRAUM_BENCH_ROUNDS` rounds with the order turned each round.
    if std::env::var("STRAUM_BENCH_MODE").as_deref() == Ok("catchup") {
        let windows: Vec<u64> = std::env::var("STRAUM_BENCH_WINDOWS")
            .unwrap_or_else(|_| "256,1024,4096".to_owned())
            .split(',')
            .filter_map(|w| w.trim().parse().ok())
            .collect();
        // D-135 A: each window at each heartbeat of `STRAUM_BENCH_HEARTBEATS`
        // (50 alone by default), the combinations' order turned each round.
        let heartbeats: Vec<u64> = std::env::var("STRAUM_BENCH_HEARTBEATS")
            .unwrap_or_else(|_| "50".to_owned())
            .split(',')
            .filter_map(|h| h.trim().parse().ok())
            .collect();
        let combinations: Vec<(u64, u64)> = windows
            .iter()
            .flat_map(|&w| heartbeats.iter().map(move |&h| (w, h)))
            .collect();
        for round in 0..env::<usize>("STRAUM_BENCH_ROUNDS", 3) {
            for i in 0..combinations.len() {
                let (window, heartbeat) = combinations[(i + round) % combinations.len()];
                // D-136 A: each trial with its control, which goes first
                // in every other round.
                let order = if round % 2 == 0 {
                    [true, false]
                } else {
                    [false, true]
                };
                for restart in order {
                    match catch_up(window, heartbeat, restart).await {
                        Some((line, stages)) => {
                            println!("round={} {line}", round + 1);
                            for row in stages.lines() {
                                println!(
                                    "round={} stages run={} window={window} heartbeat_ms={heartbeat} {row}",
                                    round + 1,
                                    if restart { "catchup" } else { "control" }
                                );
                            }
                        }
                        None => println!(
                            "round={} window={window} heartbeat_ms={heartbeat} restart={restart} failed",
                            round + 1
                        ),
                    }
                }
            }
        }
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
        let mut trials = Vec::new();
        for _ in 0..failovers {
            // No sequencer to stop is a failed failover, not a dropped trial
            // (D-130).
            trials.push(failover(&mut cluster, window).await.unwrap_or(Failover {
                took: None,
                slots: 0,
                abandoned: 0,
                lag: 0,
            }));
        }
        let times: Vec<Option<Duration>> = trials.iter().map(|t| t.took).collect();
        let within = times
            .iter()
            .filter(|t| t.is_some_and(|t| t <= Duration::from_secs(1)))
            .count();
        // Every trial ranks: one that timed out, or had no sequencer, ranks
        // above every completed one and prints as `timeout` (D-130) — a p90
        // over completed failovers alone would understate the tail.
        let mut done: Vec<u128> = times
            .iter()
            .map(|t| t.map_or(u128::MAX, |t| t.as_millis()))
            .collect();
        done.sort_unstable();
        let at = |q: f64| {
            done.get(((done.len() as f64 * q) as usize).min(done.len().saturating_sub(1)))
                .map_or("-".to_owned(), |&ms| {
                    if ms == u128::MAX {
                        "timeout".to_owned()
                    } else {
                        ms.to_string()
                    }
                })
        };
        // The columns before `p90=` are the format older builds print too,
        // so scripts/overhead.sh reads both (D-130).
        println!(
            "{cap:<8} {:>20.0}   {within:>8} of {failovers:<10}   {:>8} / {:<8}   {}   p90={}",
            rates[rates.len() / 2],
            at(0.5),
            at(1.0),
            LAG.load(Ordering::Relaxed),
            at(0.9),
        );
        // The slow mode apart from the fast one (D-130): slots moved on,
        // takeovers abandoned and lag at the stop, averaged per mode.
        for (mode, slow) in [("fast", false), ("slow", true)] {
            let of: Vec<&Failover> = trials
                .iter()
                .filter(|t| t.took.is_none_or(|t| t > Duration::from_secs(1)) == slow)
                .collect();
            if of.is_empty() {
                continue;
            }
            let mean = |f: fn(&Failover) -> u64| {
                of.iter().map(|t| f(t)).sum::<u64>() as f64 / of.len() as f64
            };
            println!(
                "failover mode={mode} count={} slots_moved={:.2} takeovers_abandoned={:.2} lag_at_stop={:.0} timed_out={}",
                of.len(),
                mean(|t| t.slots),
                mean(|t| t.abandoned),
                mean(|t| t.lag),
                of.iter().filter(|t| t.took.is_none()).count(),
            );
        }
        cluster.shutdown().await;
    }
}
