//! Three nodes on loopback with their logs on disk (ADR-0008 step 4): the
//! driver around the cores, over real sockets and files.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use apply::{Effect, Limits, Origin};
use bytes::Bytes;
use domain::{DeliveryGuarantee, Topic};
use node::{MqttConfig, Node, NodeConfig, Status};
use storage::{SegmentLimits, WalLimits};
use transport::TransportConfig;

const NODES: u8 = 3;

/// A port of this test binary's own (D-108): a block below the ephemeral
/// range chosen by the process id, numbered by a counter, so no two tests
/// running in parallel here ever get the same one. Binding port 0 and
/// releasing it before the node bound it left a window in which another
/// test could be handed the same port. A port taken by something else still
/// fails the node's start, loudly — nothing retries.
fn free_port() -> SocketAddr {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    // 40 blocks of 300 from 20 000 end below 32 768, where Linux's ephemeral
    // range starts; every test binary uses the same blocks, so two never
    // overlap. The node tests take about 120 ports a run (D-122 added some).
    let block = u16::try_from(std::process::id() % 40).unwrap();
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert!(n < 300, "this test binary used up its block of ports");
    SocketAddr::from(([127, 0, 0, 1], 20_000 + block * 300 + n))
}

fn data_dir(test: &str, id: u8) -> PathBuf {
    std::env::temp_dir().join(format!("straum-node-{}-{test}-{id}", std::process::id()))
}

struct Cluster {
    test: &'static str,
    addresses: Vec<SocketAddr>,
    mqtt: Vec<SocketAddr>,
    nodes: Vec<Option<Node>>,
    /// One link dialled through a proxy: (from, to, the proxy's address).
    via: Option<(u8, u8, SocketAddr)>,
}

fn gateway() -> gateway::Config {
    gateway::Config {
        node: 0,
        first_session: 0,
        codec: codec::Limits::default(),
        max_connections: 64,
        connect_timeout: Duration::from_secs(10),
        max_pending: 64,
        max_pending_bytes: 1 << 20,
        max_total_pending_bytes: 16 << 20,
        commit_timeout: Duration::from_secs(30),
        retry_interval: Duration::from_millis(200),
        receive_maximum: NonZeroU16::new(64).unwrap(),
        topic_alias_maximum: 16,
        max_waiting: 1024,
        max_waiting_bytes: 1 << 20,
        max_total_waiting_bytes: 16 << 20,
        max_retries: 1024,
    }
}

/// Segment size, retention bytes and snapshot interval: small for the
/// compaction test, so its log spans many segments; roomy elsewhere, with
/// snapshots still taken.
fn disk(test: &str) -> (u64, u64, u64) {
    if test == "compaction" || test == "rotation-compaction" {
        (2048, 8192, 16)
    } else {
        (64 * 1024, 1 << 20, 64)
    }
}

fn config(
    test: &str,
    id: u8,
    addresses: &[SocketAddr],
    mqtt: &[SocketAddr],
    limits: Limits,
) -> NodeConfig {
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
            max_uncommitted: 4096,
            // Rotation: only a handoff may change the sequencer.
            takeover_ticks: if test == "rotation" { 1_000_000 } else { 3 },
            lag_bound: None,
        },
        listen: addresses[usize::from(id)],
        peers: (0..NODES)
            .filter(|&p| p != id)
            .map(|p| (p, addresses[usize::from(p)]))
            .collect(),
        data: data_dir(test, id),
        segments: SegmentLimits {
            wal: WalLimits::default(),
            segment_bytes: disk(test).0,
        },
        limits,
        heartbeat: Duration::from_millis(50),
        // Holds the largest batch: 64 records of the default 1 MiB packet.
        max_frame: 1 << 27,
        link_queue: 1024,
        transport: TransportConfig {
            listen: addresses[usize::from(id)],
            ..TransportConfig::default()
        },
        mqtt: Some(MqttConfig {
            listen: mqtt[usize::from(id)],
            gateway: gateway(),
            write_queue: 1024,
        }),
        retain_bytes: disk(test).1,
        snapshot_every: disk(test).2,
        rotate_every: test.starts_with("rotation").then_some(6),
        read_bytes: 64 * 1024,
    }
}

/// The lowest segment key left in a node's data directory, and whether it
/// holds a snapshot.
fn on_disk(test: &str, id: u8) -> (u64, bool) {
    let dir = data_dir(test, id);
    let lowest = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| {
            let path = e.unwrap().path();
            (path.extension()? == "seg").then(|| path.file_stem()?.to_str()?.parse::<u64>().ok())?
        })
        .min()
        .unwrap();
    (lowest, dir.join("snapshot").exists())
}

/// A publish proposed at node `id`, as its gateway would make it: one
/// session, one request id.
fn publish(id: u8, request: u64) -> cluster::Proposal {
    let proposal = apply::Proposal::Publish {
        origin: Origin {
            node: id,
            session: 1,
        },
        request,
        topic: Topic::new(Bytes::from_static(b"t")).unwrap(),
        payload: Bytes::from(format!("m{request}")),
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

impl Cluster {
    async fn start(test: &'static str) -> Self {
        Self::start_via(test, None).await
    }

    /// As `start`, with node `from`'s link to node `to` dialled through
    /// `address` instead (a proxy, D-122).
    async fn start_via(test: &'static str, via: Option<(u8, u8, SocketAddr)>) -> Self {
        for id in 0..NODES {
            let _ = std::fs::remove_dir_all(data_dir(test, id));
        }
        let addresses: Vec<SocketAddr> = (0..NODES).map(|_| free_port()).collect();
        let mqtt: Vec<SocketAddr> = (0..NODES).map(|_| free_port()).collect();
        let mut cluster = Cluster {
            test,
            addresses,
            mqtt,
            nodes: Vec::new(),
            via,
        };
        for id in 0..NODES {
            let node = Node::start(cluster.config(id, Limits::default()))
                .await
                .unwrap_or_else(|e| panic!("{test}: node {id} did not start: {e}"));
            cluster.nodes.push(Some(node));
        }
        cluster
    }

    fn config(&self, id: u8, limits: Limits) -> NodeConfig {
        let mut config = config(self.test, id, &self.addresses, &self.mqtt, limits);
        if let Some((_, to, address)) = self.via.filter(|v| v.0 == id) {
            for peer in &mut config.peers {
                if peer.0 == to {
                    peer.1 = address;
                }
            }
        }
        config
    }

    async fn restart(&mut self, id: u8, limits: Limits) {
        if let Some(node) = self.nodes[usize::from(id)].take() {
            node.shutdown().await;
        }
        let node = Node::start(self.config(id, limits))
            .await
            .unwrap_or_else(|e| panic!("{}: node {id} did not restart: {e}", self.test));
        self.nodes[usize::from(id)] = Some(node);
    }

    async fn stop(&mut self, id: u8) {
        if let Some(node) = self.nodes[usize::from(id)].take() {
            node.shutdown().await;
        }
    }

    fn node(&self, id: u8) -> &Node {
        self.nodes[usize::from(id)].as_ref().unwrap()
    }

    /// Proposes a publish at `id` until its acceptance is applied there:
    /// what a gateway does with a proposal that does not commit.
    async fn publish(&self, id: u8, request: u64) {
        let node = self.node(id);
        let mut effects = node.effects();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            node.propose(publish(id, request)).await;
            let retry = tokio::time::sleep(Duration::from_millis(300));
            tokio::pin!(retry);
            loop {
                tokio::select! {
                    _ = &mut retry => break,
                    received = effects.recv() => {
                        if let Ok((_, _, out)) = received {
                            let accepted = out.iter().any(|e| matches!(
                                e,
                                Effect::Accepted { to, request: r, .. } if to.node == id && *r == request
                            ));
                            if accepted {
                                return;
                            }
                        }
                    }
                }
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "request {request} never committed at node {id}"
            );
        }
    }

    /// Waits until every running node has applied the same log as far as
    /// `at_least`, and they agree on the state.
    async fn converged(&self, at_least: u64) -> Status {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            let statuses: Vec<Status> = self
                .nodes
                .iter()
                .flatten()
                .map(|n| *n.status().borrow())
                .collect();
            let first = statuses[0];
            let same = statuses.iter().all(|s| {
                s.applied == first.applied && s.digest == first.digest && s.commit == s.applied
            });
            if same && first.applied >= at_least {
                return first;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "nodes did not converge: {statuses:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn shutdown(mut self) {
        for id in 0..NODES {
            self.stop(id).await;
            let _ = std::fs::remove_dir_all(data_dir(self.test, id));
        }
    }
}

/// A test that fails never reaches `shutdown` (D-116): its nodes are let go
/// here — they end with the test's runtime, which drops right after — and
/// their data directories removed. Nothing in here panics, so a cleanup that
/// fails cannot hide the test's own failure.
impl Drop for Cluster {
    fn drop(&mut self) {
        self.nodes.clear();
        for id in 0..NODES {
            let _ = std::fs::remove_dir_all(data_dir(self.test, id));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_commit_and_apply_one_log() {
    let cluster = Cluster::start("one-log").await;
    for request in 0..30u64 {
        cluster.publish((request % 3) as u8, request).await;
    }
    // Thirty publishes, each in a batch of its own at least: two lsns each.
    let status = cluster.converged(60).await;
    assert!(status.applied >= 60);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_replica_catches_up_from_its_log_and_the_sequencers() {
    let mut cluster = Cluster::start("replica-restart").await;
    for request in 0..10 {
        cluster.publish(0, request).await;
    }
    cluster.converged(20).await;
    cluster.stop(2).await;
    // Two of three still commit.
    for request in 10..20 {
        cluster.publish(1, request).await;
    }
    cluster.restart(2, Limits::default()).await;
    cluster.converged(40).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_sequencer_resumes_where_its_log_ends() {
    let mut cluster = Cluster::start("sequencer-restart").await;
    for request in 0..10 {
        cluster.publish(1, request).await;
    }
    let before = cluster.converged(20).await;
    cluster.restart(0, Limits::default()).await;
    for request in 10..20 {
        cluster.publish(2, request).await;
    }
    let after = cluster.converged(before.applied + 20).await;
    assert!(after.applied > before.applied);
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_with_other_limits_is_refused_and_the_others_go_on() {
    let mut cluster = Cluster::start("other-limits").await;
    let other = Limits {
        max_inflight: 7,
        ..Limits::default()
    };
    // A node configured differently from the start: over its old data
    // directory it would not start at all (`a_data_directory_of_another_cluster_is_refused`).
    cluster.stop(2).await;
    let _ = std::fs::remove_dir_all(data_dir("other-limits", 2));
    cluster.restart(2, other).await;
    for request in 0..5 {
        cluster.publish(0, request).await;
    }
    // Nodes 0 and 1 commit; node 2 is refused by both and refuses both.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let s0 = *cluster.node(0).status().borrow();
        let s1 = *cluster.node(1).status().borrow();
        if s0.applied >= 10 && s1.applied == s0.applied && s0.digest == s1.digest {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the two did not commit: {s0:?} {s1:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let s2 = *cluster.node(2).status().borrow();
    assert_eq!(s2.applied, 0, "a node with other limits took part: {s2:?}");
    cluster.shutdown().await;
}

/// An MQTT client of one node; its event loop runs on a task of its own and
/// hands over every incoming packet.
fn client(
    address: SocketAddr,
    id: &str,
) -> (
    rumqttc::AsyncClient,
    tokio::sync::mpsc::UnboundedReceiver<rumqttc::Packet>,
) {
    let mut options = rumqttc::MqttOptions::new(id, address.ip().to_string(), address.port());
    options.set_keep_alive(Duration::from_secs(30));
    let (client, mut eventloop) = rumqttc::AsyncClient::new(options, 16);
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            match eventloop.poll().await {
                Ok(rumqttc::Event::Incoming(packet)) => {
                    if tx.send(packet).is_err() {
                        return;
                    }
                }
                Ok(_) => {}
                // Refused until the gateway's NodeReset is committed: the
                // event loop dials again on the next poll.
                Err(_) => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    });
    (client, rx)
}

async fn next(
    packets: &mut tokio::sync::mpsc::UnboundedReceiver<rumqttc::Packet>,
    wanted: impl Fn(&rumqttc::Packet) -> bool,
) -> rumqttc::Packet {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let packet = packets.recv().await.expect("event loop ended");
            if wanted(&packet) {
                return packet;
            }
        }
    })
    .await
    .expect("the packet never came")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_mqtt_publish_on_one_node_reaches_a_subscriber_on_another() {
    let cluster = Cluster::start("mqtt").await;
    let (subscriber, mut sub_packets) = client(cluster.mqtt[1], "subscriber");
    next(&mut sub_packets, |p| {
        matches!(p, rumqttc::Packet::ConnAck(_))
    })
    .await;
    subscriber
        .subscribe("sensors/+/t", rumqttc::QoS::AtLeastOnce)
        .await
        .unwrap();
    next(&mut sub_packets, |p| {
        matches!(p, rumqttc::Packet::SubAck(_))
    })
    .await;

    // Through the sequencer's gateway, then through a replica's.
    for (node, name) in [(0usize, "pub-0"), (2, "pub-2")] {
        let (publisher, mut pub_packets) = client(cluster.mqtt[node], name);
        next(&mut pub_packets, |p| {
            matches!(p, rumqttc::Packet::ConnAck(_))
        })
        .await;
        // The node publishes each applied record's effects before its
        // gateway sees them, so an acceptance is here before its PUBACK.
        let mut effects = cluster.node(node as u8).effects();
        let mut accepted = 0;
        for i in 0..10u8 {
            publisher
                .publish("sensors/a/t", rumqttc::QoS::AtLeastOnce, false, vec![i; 8])
                .await
                .unwrap();
            next(&mut pub_packets, |p| {
                matches!(p, rumqttc::Packet::PubAck(_))
            })
            .await;
            // PUBACK only once the publish is committed (invariant 4).
            while let Ok((_, _, out)) = effects.try_recv() {
                accepted += out
                    .iter()
                    .filter(|e| matches!(e, Effect::Accepted { to, .. } if usize::from(to.node) == node))
                    .count();
            }
            assert!(
                accepted > usize::from(i),
                "PUBACK {i} from {name} before its publish was committed"
            );
        }
        // In publish order (CLAUDE.md §7).
        for i in 0..10u8 {
            let rumqttc::Packet::Publish(publish) = next(&mut sub_packets, |p| {
                matches!(p, rumqttc::Packet::Publish(_))
            })
            .await
            else {
                unreachable!()
            };
            assert_eq!(publish.topic, "sensors/a/t");
            assert_eq!(&publish.payload[..], &[i; 8][..], "from {name}");
        }
    }
    // Every node applied the same log, client sessions included.
    cluster.converged(1).await;
    cluster.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compacted_nodes_restart_from_their_snapshots() {
    let mut cluster = Cluster::start("compaction").await;
    for request in 0..150u64 {
        cluster.publish((request % 3) as u8, request).await;
    }
    let before = cluster.converged(300).await;
    // Every node gave back the start of its log, and kept a snapshot.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let disks: Vec<(u64, bool)> = (0..NODES).map(|id| on_disk("compaction", id)).collect();
        if disks
            .iter()
            .all(|&(lowest, snapshot)| lowest > 1 && snapshot)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "not compacted: {disks:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // A replica and the sequencer restart from snapshot and log; each
    // restart commits its gateway's NodeReset, and the state they then
    // reach must equal that of node 1, which never stopped.
    cluster.restart(2, Limits::default()).await;
    cluster.restart(0, Limits::default()).await;
    cluster.converged(before.applied + 2).await;
    for request in 150..160u64 {
        cluster.publish((request % 3) as u8, request).await;
    }
    cluster.converged(before.applied + 20).await;
    cluster.shutdown().await;
}

/// Ungraceful takeover (ADR-0008 step 5): the sequencer stops, the next
/// node in the schedule takes a new slot over and the cluster commits again;
/// the old sequencer comes back as a follower of that slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_node_takes_the_slot_over_when_the_sequencer_stops() {
    let mut cluster = Cluster::start("takeover").await;
    for request in 0..10 {
        cluster.publish(1, request).await;
    }
    let before = cluster.converged(20).await;
    let leading = |cluster: &Cluster| -> Vec<u8> {
        (0..NODES)
            .filter(|&id| {
                cluster.nodes[usize::from(id)]
                    .as_ref()
                    .is_some_and(|n| n.status().borrow().leading)
            })
            .collect()
    };
    let old = leading(&cluster);
    assert_eq!(old.len(), 1, "one sequencer: {old:?}");
    let old = old[0];
    cluster.stop(old).await;

    let other = (0..NODES).find(|&id| id != old).unwrap();
    for request in 10..20 {
        cluster.publish(other, request).await;
    }
    let after = cluster.converged(before.applied + 20).await;
    assert!(
        after.slot > before.slot,
        "no new slot: {before:?} {after:?}"
    );
    let now = leading(&cluster);
    assert_eq!(now.len(), 1, "one sequencer: {now:?}");
    assert_ne!(now[0], old);

    cluster.restart(old, Limits::default()).await;
    for request in 20..25 {
        cluster.publish(old, request).await;
    }
    let last = cluster.converged(after.applied + 10).await;
    // The old sequencer never resumes its old slot. Another takeover in
    // between is allowed — the schedule moves on whenever a heartbeat is
    // late — but there is one sequencer, of a slot no older than before.
    assert_eq!(leading(&cluster).len(), 1, "one sequencer");
    assert!(last.slot >= after.slot, "{after:?} {last:?}");
    cluster.shutdown().await;
}

/// Planned rotation (ADR-0008 step 6): the sequencer hands its slot over
/// every few heartbeats, by `SlotEnd`, with takeovers by silence out of
/// reach; publishes keep committing through the changes, on one log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rotation_hands_the_slot_over_and_commits_go_on() {
    let cluster = Cluster::start("rotation").await;
    let first = cluster.converged(2).await;
    let mut request = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        cluster
            .publish((request % u64::from(NODES)) as u8, request)
            .await;
        request += 1;
        let now = *cluster.node(0).status().borrow();
        if now.slot >= first.slot + 3 && request >= 10 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no rotation: {first:?} {now:?}"
        );
    }
    let last = cluster.converged(first.applied + request).await;
    assert!(last.slot >= first.slot + 3, "{first:?} {last:?}");
    cluster.shutdown().await;
}

/// Nodes that compacted past slot changes restart from their snapshots
/// (D-79 A): the snapshot carries the committed slot table, pruned by
/// compaction, and the restarted node places itself and the others by it.
/// Rotation makes the slot changes; takeovers by silence stay in reach, so
/// a handoff to a stopped node is taken over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nodes_compacted_past_slot_changes_restart_from_their_snapshots() {
    let test = "rotation-compaction";
    let mut cluster = Cluster::start(test).await;
    let first = cluster.converged(2).await;
    for request in 0..150u64 {
        cluster.publish((request % 3) as u8, request).await;
    }
    let before = cluster.converged(first.applied + 150).await;
    assert!(
        before.slot >= first.slot + 3,
        "no rotation: {first:?} {before:?}"
    );
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let disks: Vec<(u64, bool)> = (0..NODES).map(|id| on_disk(test, id)).collect();
        if disks
            .iter()
            .all(|&(lowest, snapshot)| lowest > 1 && snapshot)
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "not compacted: {disks:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cluster.restart(2, Limits::default()).await;
    cluster.restart(0, Limits::default()).await;
    for request in 150..160u64 {
        cluster.publish((request % 3) as u8, request).await;
    }
    let after = cluster.converged(before.applied + 10).await;
    assert!(after.slot > before.slot, "{before:?} {after:?}");
    cluster.shutdown().await;
}

/// A link opens with HELLO, which carries the frame version (ADR-0008 step
/// 7a): a node closes a link from another version at once, and keeps one
/// from its own version open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_from_another_frame_version_is_refused() {
    use cluster::wire::{Frame, WIRE_VERSION, encode};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let cluster = Cluster::start("other-version").await;
    let hello = |version| {
        let mut bytes = Vec::new();
        encode(
            &Frame::Hello {
                version,
                node: 2,
                slot: 1,
                config: node::cluster_digest(&config(
                    "other-version",
                    2,
                    &cluster.addresses,
                    &cluster.mqtt,
                    Limits::default(),
                )),
            },
            &mut bytes,
        )
        .unwrap();
        bytes
    };
    // Nothing comes back on a link: a refused one ends, an accepted one
    // waits for frames.
    let open_after = |version| {
        let address = cluster.addresses[0];
        let bytes = hello(version);
        async move {
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream.write_all(&bytes).await.unwrap();
            let mut byte = [0u8; 1];
            match tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte)).await {
                Ok(Ok(0) | Err(_)) => false,
                Ok(Ok(_)) => panic!("a node wrote on an inbound link"),
                Err(_) => true,
            }
        }
    };
    assert!(
        !open_after(WIRE_VERSION.wrapping_add(1)).await,
        "a link from another frame version stayed open"
    );
    assert!(
        open_after(WIRE_VERSION).await,
        "a link from this version was closed"
    );
    cluster.shutdown().await;
}

/// A data directory keeps the configuration digest of the cluster it was
/// first used with (D-104 A): a node started over it with other cluster
/// parameters or limits is refused before it opens its log or listens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_data_directory_of_another_cluster_is_refused() {
    let mut cluster = Cluster::start("other-cluster").await;
    cluster.stop(2).await;
    let other = Limits {
        max_inflight: 7,
        ..Limits::default()
    };
    let refused = Node::start(config(
        "other-cluster",
        2,
        &cluster.addresses,
        &cluster.mqtt,
        other,
    ))
    .await;
    assert!(
        matches!(refused, Err(node::NodeError::OtherCluster { .. })),
        "started over another cluster's data: {:?}",
        refused.err()
    );
    // The same configuration starts again.
    cluster.restart(2, Limits::default()).await;
    cluster.shutdown().await;
}

/// The operator's handoff (D-105 A): asked of a node that does not
/// sequence, it is refused; asked of the sequencer, the slot passes to the
/// next owner by `SlotEnd` while every node keeps serving — commits go on,
/// and the old sequencer applies them as a follower.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_operator_handoff_moves_the_slot_and_every_node_keeps_serving() {
    let cluster = Cluster::start("hand-off").await;
    let first = cluster.converged(2).await;
    let leader = (0..NODES)
        .find(|&id| cluster.node(id).status().borrow().leading)
        .expect("a sequencer");
    let other = (leader + 1) % NODES;
    assert_eq!(
        cluster.node(other).hand_off().await,
        Err(cluster::ClusterError::NotLeader)
    );
    cluster.node(leader).hand_off().await.unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let successor = loop {
        if let Some(id) = (0..NODES).find(|&id| {
            let s = *cluster.node(id).status().borrow();
            id != leader && s.leading && s.slot > first.slot
        }) {
            break id;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no successor took the slot"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    };
    for request in 0..5 {
        cluster.publish(successor, request).await;
    }
    let last = cluster.converged(first.applied + 10).await;
    assert!(last.slot > first.slot);
    assert!(!cluster.node(leader).status().borrow().leading);
    cluster.shutdown().await;
}

/// Proposes `0..count` once each at every node of `origins`, a millisecond
/// apart, never again: only the node may hold or resend them (D-109 B).
/// Each stops halfway at the barrier twice: whatever the caller does
/// between its two waits falls after the first half was proposed and
/// before the second.
fn propose_once(
    cluster: &Cluster,
    origins: &[u8],
    count: u64,
) -> (Vec<tokio::task::JoinHandle<()>>, Arc<tokio::sync::Barrier>) {
    let barrier = Arc::new(tokio::sync::Barrier::new(origins.len() + 1));
    let tasks = origins
        .iter()
        .map(|&id| {
            let proposer = cluster.node(id).proposer();
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                for request in 0..count {
                    if request == count / 2 {
                        barrier.wait().await;
                        barrier.wait().await;
                    }
                    proposer.send(publish(id, request)).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
        })
        .collect();
    (tasks, barrier)
}

/// One of the caller's two waits at `propose_once`'s barrier, bounded: a
/// sender that failed never arrives.
async fn halfway_wait(barrier: &tokio::sync::Barrier) {
    tokio::time::timeout(Duration::from_secs(20), barrier.wait())
        .await
        .expect("a sender never reached halfway");
}

/// The acceptances `effects` sees from `origins`, in commit order, until
/// each origin has `count` and then for a further half second — a
/// duplicate would come after. Checks the committed log never goes back to
/// an older slot: no record of a fenced owner's slot commits after the
/// first of its successor's. Returns the requests per origin and the slots
/// they committed in.
async fn acceptances(
    effects: &mut tokio::sync::broadcast::Receiver<(u64, u64, Vec<Effect>)>,
    origins: &[u8],
    count: usize,
) -> (BTreeMap<u8, Vec<u64>>, BTreeSet<u64>) {
    let mut seen: BTreeMap<u8, Vec<u64>> = origins.iter().map(|&o| (o, Vec::new())).collect();
    let mut slots = BTreeSet::new();
    let (mut last_lsn, mut last_slot) = (0, 0);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    let mut quiet: Option<tokio::time::Instant> = None;
    loop {
        let until = quiet.unwrap_or(deadline);
        let Ok(received) = tokio::time::timeout_at(until, effects.recv()).await else {
            assert!(quiet.is_some(), "not all committed: {seen:?}");
            return (seen, slots);
        };
        let (lsn, slot, out) = received.expect("the observer kept up");
        assert!(lsn > last_lsn, "lsn {lsn} after {last_lsn}");
        assert!(
            slot >= last_slot,
            "slot {slot} committed after slot {last_slot}"
        );
        (last_lsn, last_slot) = (lsn, slot);
        for effect in out {
            if let Effect::Accepted { to, request, .. } = effect {
                if let Some(requests) = seen.get_mut(&to.node) {
                    requests.push(request);
                    slots.insert(slot);
                }
            }
        }
        if quiet.is_none() && seen.values().all(|r| r.len() >= count) {
            quiet = Some(tokio::time::Instant::now() + Duration::from_millis(500));
        }
    }
}

/// D-109 B, a planned handoff: proposals made on every node while the
/// slot changes hands — at the old sequencer after it sealed its slot, and
/// forwarded to it as it steps down — each commit exactly once, in the
/// order its node made them, and nothing of the old slot commits after the
/// new one began.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_handoff_loses_no_proposal_and_commits_each_once() {
    let cluster = Cluster::start("hand-off-proposals").await;
    let first = cluster.converged(2).await;
    let leader = (0..NODES)
        .find(|&id| cluster.node(id).status().borrow().leading)
        .expect("a sequencer");
    let observer = (leader + 1) % NODES;
    let mut effects = cluster.node(observer).effects();
    let all: Vec<u8> = (0..NODES).collect();
    let (senders, halfway) = propose_once(&cluster, &all, 200);
    halfway_wait(&halfway).await;
    cluster.node(leader).hand_off().await.unwrap();
    halfway_wait(&halfway).await;
    let (seen, slots) = acceptances(&mut effects, &all, 200).await;
    for sender in senders {
        sender.await.unwrap();
    }
    for (origin, requests) in &seen {
        assert_eq!(*requests, (0..200).collect::<Vec<u64>>(), "node {origin}");
    }
    assert!(
        slots.iter().any(|&s| s > first.slot),
        "nothing committed in a later slot: {slots:?}"
    );
    cluster.converged(first.applied + 600).await;
    cluster.shutdown().await;
}

/// D-109 B, an emergency failure: the sequencer stops while proposals
/// forwarded to it sit in its queue or its uncommitted tail. The nodes that
/// made them hold them, and once a new slot has committed they send them to
/// its sequencer: each commits exactly once, in order, nothing of the dead
/// owner's slot commits after the new one began, and the old sequencer,
/// restarted, converges on that log.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sequencer_failure_loses_no_proposal_the_others_hold() {
    let mut cluster = Cluster::start("failure-proposals").await;
    let first = cluster.converged(2).await;
    let leader = (0..NODES)
        .find(|&id| cluster.node(id).status().borrow().leading)
        .expect("a sequencer");
    let others: Vec<u8> = (0..NODES).filter(|&id| id != leader).collect();
    let mut effects = cluster.node(others[0]).effects();
    let (senders, halfway) = propose_once(&cluster, &others, 200);
    halfway_wait(&halfway).await;
    // What the sequencer held uncommitted as it stopped, for the report:
    // not asserted, as how much it is depends on the scheduler.
    let at_stop = *cluster.node(leader).status().borrow();
    eprintln!(
        "the sequencer stopped with {} records appended past its commit",
        at_stop.end.saturating_sub(at_stop.commit)
    );
    cluster.stop(leader).await;
    halfway_wait(&halfway).await;
    let (seen, slots) = acceptances(&mut effects, &others, 200).await;
    for sender in senders {
        sender.await.unwrap();
    }
    for (origin, requests) in &seen {
        assert_eq!(*requests, (0..200).collect::<Vec<u64>>(), "node {origin}");
    }
    assert!(
        slots.iter().any(|&s| s > first.slot),
        "nothing committed in a later slot: {slots:?}"
    );
    cluster.restart(leader, Limits::default()).await;
    cluster.converged(first.applied + 400).await;
    cluster.shutdown().await;
}

/// A replica down longer than a link's redial backoff once reached (1 s)
/// comes back, and the slot stays with its live owner (D-122): the
/// sequencer redials within a heartbeat, so the restarted node hears it
/// before `takeover_ticks` of silence and never asks for the slot. Commits
/// go on through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_replica_leaves_the_slot_with_its_live_owner() {
    let mut cluster = Cluster::start("restart-quiet").await;
    let first = cluster.converged(2).await;
    let leader = (0..NODES)
        .find(|&id| cluster.node(id).status().borrow().leading)
        .expect("a sequencer");
    let replica = (leader + 1) % NODES;
    cluster.stop(replica).await;
    // Longer than the old backoff's cap, so its redial waited up to 1 s.
    tokio::time::sleep(Duration::from_secs(3)).await;
    cluster.restart(replica, Limits::default()).await;
    let watch = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < watch {
        for id in 0..NODES {
            let s = *cluster.node(id).status().borrow();
            // A node just started reports slot 0 until its loop runs.
            assert!(
                s.slot <= first.slot,
                "node {id} moved to slot {} after node {replica} restarted: {s:?}",
                s.slot
            );
        }
        assert!(cluster.node(leader).status().borrow().leading);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster.publish(replica, 0).await;
    cluster.converged(first.applied + 2).await;
    cluster.shutdown().await;
}

/// A link proxy that holds every new connection for `delay` before it
/// connects on to `target` (D-122): a peer reached late, as over a slow or
/// congested path. Bytes then pass both ways as they come. Until `target`
/// is set, a connection is dropped, and its dialler tries again.
fn delaying_proxy(
    target: Arc<std::sync::OnceLock<SocketAddr>>,
    delay: Arc<std::sync::atomic::AtomicU64>,
) -> SocketAddr {
    let address = free_port();
    let listener = std::net::TcpListener::bind(address).unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut inbound, _)) = listener.accept().await else {
                return;
            };
            let delay = delay.load(std::sync::atomic::Ordering::Relaxed);
            let target = Arc::clone(&target);
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let Some(&target) = target.get() else { return };
                if let Ok(mut outbound) = tokio::net::TcpStream::connect(target).await {
                    let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                }
            });
        }
    });
    address
}

/// A cluster whose sequencer (node 0, the first slot's owner) dials node 1
/// through a delaying proxy: it commits, node 1 stops for 3 s, the delay is
/// set, and node 1 restarts. Returns the cluster and its first status.
async fn restart_behind_a_slow_link(test: &'static str, delay_ms: u64) -> (Cluster, Status) {
    let delay = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let target = Arc::new(std::sync::OnceLock::new());
    let proxy = delaying_proxy(Arc::clone(&target), Arc::clone(&delay));
    let mut cluster = Cluster::start_via(test, Some((0, 1, proxy))).await;
    let _ = target.set(cluster.addresses[1]);
    let first = cluster.converged(2).await;
    assert!(
        cluster.node(0).status().borrow().leading,
        "node 0 sequences the first slot"
    );
    cluster.stop(1).await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    delay.store(delay_ms, std::sync::atomic::Ordering::Relaxed);
    cluster.restart(1, Limits::default()).await;
    (cluster, first)
}

/// D-122 A, a delayed connection inside the window: the sequencer's link to
/// a restarted replica connects a heartbeat late, on top of its redial, and
/// the replica still hears its owner before `takeover_ticks` of silence —
/// the slot stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_a_heartbeat_late_leaves_the_slot_with_its_live_owner() {
    let (cluster, first) = restart_behind_a_slow_link("late-link", 50).await;
    let watch = tokio::time::Instant::now() + Duration::from_secs(2);
    while tokio::time::Instant::now() < watch {
        for id in 0..NODES {
            let s = *cluster.node(id).status().borrow();
            assert!(
                s.slot <= first.slot,
                "node {id} moved to slot {}: {s:?}",
                s.slot
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster.publish(1, 0).await;
    cluster.converged(first.applied + 2).await;
    cluster.shutdown().await;
}

/// D-122 A, a delayed connection past the window, and a fenced owner that
/// is still running: the restarted replica hears nothing from node 0 for a
/// second and starts a takeover — the protocol allows it, as it would for a
/// dead owner. The new slot goes to node 1, or, if its turn passes before
/// it finishes, to node 2. Node 0 runs on, fenced: from the first record of
/// the new slot on, nothing of the old slot commits; node 0 stops
/// sequencing that slot (it may lead a later one at its turn); and a publish proposed at node 0 commits once, through the
/// new owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_link_slower_than_the_takeover_window_fences_the_live_owner() {
    let (cluster, first) = restart_behind_a_slow_link("slow-link", 1_000).await;
    let mut effects = cluster.node(2).effects();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let moved = (1..NODES).any(|id| {
            let s = *cluster.node(id).status().borrow();
            s.leading && s.slot > first.slot
        });
        if moved {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no other node took the slot from node 0"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // Proposed once, never again: only node 0's outbox may send it again,
    // and it must reach the log once (`Cluster::publish` resends, as a
    // client would, and could commit it twice).
    let mut accepted = cluster.node(0).effects();
    cluster.node(0).propose(publish(0, 7)).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let (_, _, out) = tokio::time::timeout_at(deadline, accepted.recv())
            .await
            .expect("node 0's request 7 never committed")
            .unwrap();
        if out
            .iter()
            .any(|e| matches!(e, Effect::Accepted { to, request: 7, .. } if to.node == 0))
        {
            break;
        }
    }
    let after = cluster.converged(first.applied + 2).await;
    assert!(after.slot > first.slot);
    // Fenced, node 0 no longer sequences its old slot. It may lead a later
    // one at its turn in the schedule (slot 4, after nodes 1 and 2): seen
    // once under load in the stop gate.
    let zero = *cluster.node(0).status().borrow();
    assert!(
        !(zero.leading && zero.slot == first.slot),
        "the fenced owner still sequences slot {}: {zero:?}",
        first.slot
    );
    // What node 2 applied: slots never go back, and request 7 of node 0
    // commits once.
    let (mut last_slot, mut sevens) = (0, 0);
    while let Ok((lsn, slot, out)) = effects.try_recv() {
        assert!(
            slot >= last_slot,
            "lsn {lsn} of slot {slot} after slot {last_slot}"
        );
        last_slot = slot;
        sevens += out
            .iter()
            .filter(|e| matches!(e, Effect::Accepted { to, request: 7, .. } if to.node == 0))
            .count();
    }
    assert_eq!(sevens, 1, "node 0's request 7 committed {sevens} times");
    cluster.shutdown().await;
}
