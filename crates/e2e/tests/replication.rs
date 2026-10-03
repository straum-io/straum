//! Leader and follower over a real TCP socket.
//!
//! What is proved here is the whole durability path, not the transport:
//! a commit on the leader reaches the follower's disk, and the leader learns
//! that it did — from a report the follower sends only after its own
//! `sync_data` returned (docs/replication.md §119).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use runtime::{
    Durability, ReplicationSettings, Runtime, RuntimeConfig, TransportConfig, WalSettings,
};

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-replication-{name}-{}-{:?}.wal",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_file(&path);
    path
}

fn any_port() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

/// A node that only receives a log: no MQTT listener worth speaking to, no
/// broker state, no publishers. A WAL endpoint, which is all this phase is
/// about.
fn follower(path: &Path, listen: SocketAddr) -> RuntimeConfig {
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
        replication: ReplicationSettings {
            listen: Some(listen),
            ..ReplicationSettings::default()
        },
        ..RuntimeConfig::default()
    }
}

fn leader(path: &Path, ships_to: SocketAddr) -> RuntimeConfig {
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
        replication: ReplicationSettings {
            follower: Some(ships_to),
            ..ReplicationSettings::default()
        },
        ..RuntimeConfig::default()
    }
}

/// Waits for an observable state transition rather than for a duration: the
/// deadline is only there so a broken run fails instead of hanging.
async fn until(broker: &Runtime, want: u64) -> u64 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        let seen = broker.metrics().replication_follower_sequence;
        if seen >= want {
            return seen;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the follower reached {seen}, not {want}"
        );
        tokio::task::yield_now().await;
    }
}

async fn publish(broker: &Runtime, name: &str, count: usize) {
    use rumqttc::{AsyncClient, Event, MqttOptions, Outgoing, Packet, QoS};
    let addr = broker.local_addr();
    let mut options = MqttOptions::new(name, addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(options, 16);

    // Requests leave from a second task: `publish` only queues, and nothing
    // leaves the queue unless the event loop is being polled.
    let sending = client.clone();
    let publisher = tokio::spawn(async move {
        for index in 0..count {
            sending
                .publish(
                    "replicated/topic",
                    QoS::AtLeastOnce,
                    false,
                    format!("payload-{index}").into_bytes(),
                )
                .await
                .unwrap();
        }
    });

    // The PUBACK is the broker saying it is done with the publish at the
    // configured durability level. Waiting for the socket to close instead
    // costs one keep-alive interval per call and proves nothing.
    let mut acked = 0usize;
    while acked < count {
        if let Event::Incoming(Packet::PubAck(_)) = eventloop.poll().await.unwrap() {
            acked = acked.saturating_add(1);
        }
    }
    publisher.await.unwrap();

    client.disconnect().await.unwrap();
    while !matches!(
        eventloop.poll().await,
        Ok(Event::Outgoing(Outgoing::Disconnect)) | Err(_)
    ) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_commit_on_the_leader_reaches_the_follower_disk() {
    let leader_log = scratch("leader");
    let follower_log = scratch("follower");

    // The follower binds first so the leader has something to dial; the
    // leader retries anyway, but this keeps the test from measuring backoff.
    let listen = SocketAddr::from(([127, 0, 0, 1], 0));
    let standby = Runtime::start(follower(&follower_log, listen)).unwrap();
    // The replication listener asked for port 0, so ask the node what it got.
    let at = standby
        .replication_addr()
        .expect("the follower is listening");

    let primary = Runtime::start(leader(&leader_log, at)).unwrap();
    publish(&primary, "publisher", 50).await;

    // Every publish is one record, and the follower reports only what its
    // own sync_data covered.
    let reached = until(&primary, 50).await;
    assert!(reached >= 50, "follower durable through {reached}");

    primary.shutdown().await;
    standby.shutdown().await;

    // The bytes are the leader's bytes. Not a re-encoding of them: the
    // records were framed once, by the log that wrote them first.
    let sent = std::fs::read(&leader_log).unwrap();
    let held = std::fs::read(&follower_log).unwrap();
    let header = storage::HEADER_BYTES;
    assert_eq!(
        &held[header..],
        &sent[header..],
        "the follower's log is not byte for byte the leader's"
    );

    let _ = std::fs::remove_file(&leader_log);
    let _ = std::fs::remove_file(&follower_log);
}
