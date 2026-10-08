//! Three nodes on loopback with their logs on disk (ADR-0008 step 4): the
//! driver around the cores, over real sockets and files.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::time::Duration;

use apply::{Effect, Limits, Origin};
use bytes::Bytes;
use domain::{DeliveryGuarantee, Topic};
use node::{MqttConfig, Node, NodeConfig, Status};
use storage::{SegmentLimits, WalLimits};
use transport::TransportConfig;

const NODES: u8 = 3;

fn free_port() -> SocketAddr {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.local_addr().unwrap()
}

fn data_dir(test: &str, id: u8) -> PathBuf {
    std::env::temp_dir().join(format!("straum-node-{}-{test}-{id}", std::process::id()))
}

struct Cluster {
    test: &'static str,
    addresses: Vec<SocketAddr>,
    mqtt: Vec<SocketAddr>,
    nodes: Vec<Option<Node>>,
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
    if test == "compaction" {
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
            takeover_ticks: 3,
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
        max_frame: 1 << 20,
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

impl Cluster {
    async fn start(test: &'static str) -> Self {
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
        };
        for id in 0..NODES {
            let node = Node::start(config(
                test,
                id,
                &cluster.addresses,
                &cluster.mqtt,
                Limits::default(),
            ))
            .await
            .unwrap();
            cluster.nodes.push(Some(node));
        }
        cluster
    }

    async fn restart(&mut self, id: u8, limits: Limits) {
        if let Some(node) = self.nodes[usize::from(id)].take() {
            node.shutdown().await;
        }
        let node = Node::start(config(self.test, id, &self.addresses, &self.mqtt, limits))
            .await
            .unwrap();
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
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        loop {
            node.propose(cluster::Proposal {
                kind: proposal.kind(),
                properties: Vec::new(),
                body: body.clone(),
            })
            .await;
            let retry = tokio::time::sleep(Duration::from_millis(300));
            tokio::pin!(retry);
            loop {
                tokio::select! {
                    _ = &mut retry => break,
                    received = effects.recv() => {
                        if let Ok((_, out)) = received {
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
            while let Ok((_, out)) = effects.try_recv() {
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
