//! The whole stack through a real socket, driven by somebody else's client.
//!
//! Using our own codec here would hide any place where we misread the
//! specification, so the client is `rumqttc` (docs/testing.md).
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::time::Duration;

use rumqttc::{AsyncClient, ConnectionError, Event, Incoming, MqttOptions, QoS};
use runtime::{Runtime, RuntimeConfig, TransportConfig};

/// A broker on a port the kernel picked.
fn start() -> Runtime {
    let config = RuntimeConfig {
        transport: TransportConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..TransportConfig::default()
        },
        ..RuntimeConfig::default()
    };
    Runtime::start(config).unwrap()
}

fn client(name: &str, broker: &Runtime) -> (AsyncClient, rumqttc::EventLoop) {
    let addr = broker.local_addr();
    let mut options = MqttOptions::new(name, addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(5));
    AsyncClient::new(options, 16)
}

/// CleanSession = 0: keep what this identity is owed.
fn durable_client(name: &str, broker: &Runtime) -> (AsyncClient, rumqttc::EventLoop) {
    let addr = broker.local_addr();
    let mut options = MqttOptions::new(name, addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(5));
    options.set_clean_session(false);
    AsyncClient::new(options, 16)
}

/// Polls until the goodbye is on the wire, so what happens next happens to
/// a broker that is being told about it.
///
/// Stops at the DISCONNECT rather than at the event loop noticing a closed
/// socket. It only notices when its keep-alive timer fires, so waiting for
/// that cost one keep-alive interval per call and said nothing about the
/// broker: measured on this machine, the broker drops the session 218 µs
/// after `disconnect()` and the event loop gave up 5.004 s after it.
async fn closed(eventloop: &mut rumqttc::EventLoop) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, eventloop.poll()).await {
            Ok(Ok(Event::Outgoing(rumqttc::Outgoing::Disconnect))) | Ok(Err(_)) | Err(_) => return,
            Ok(Ok(_)) => {}
        }
    }
}

/// Publishes `count` numbered messages at QoS 1 and waits for every
/// acknowledgement.
///
/// The event loop is driven while the publishes go out, not after them. A
/// client's request channel is small, so publishing more than fits without
/// polling blocks in the client and never reaches the broker at all — which
/// looks exactly like a broker that stopped answering.
async fn publish_many(broker: &Runtime, topic: &str, count: u32) {
    let (publisher, mut publisher_loop) = client("publisher", broker);
    let acknowledged = tokio::spawn(async move {
        let mut seen = 0u32;
        while seen < count {
            match tokio::time::timeout(Duration::from_secs(10), publisher_loop.poll()).await {
                Ok(Ok(Event::Incoming(Incoming::PubAck(_)))) => seen += 1,
                Ok(Ok(_)) => {}
                _ => break,
            }
        }
        seen
    });

    let topic = topic.to_owned();
    for sequence in 0..count {
        publisher
            .publish(
                topic.clone(),
                QoS::AtLeastOnce,
                false,
                sequence.to_be_bytes().to_vec(),
            )
            .await
            .unwrap();
    }
    assert_eq!(
        acknowledged.await.unwrap(),
        count,
        "the broker did not acknowledge every publish"
    );
}

/// Polls until `wanted` says yes, or gives up rather than hanging a test.
async fn until(
    eventloop: &mut rumqttc::EventLoop,
    wanted: impl Fn(&Incoming) -> bool,
) -> Result<Incoming, ConnectionError> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let event = tokio::time::timeout_at(deadline, eventloop.poll())
            .await
            .expect("timed out waiting for the broker")?;
        if let Event::Incoming(incoming) = event {
            if wanted(&incoming) {
                return Ok(incoming);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_connects_subscribes_and_receives() {
    let broker = start();
    let (client, mut eventloop) = client("subscriber", &broker);

    let connected = until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::ConnAck(_))
    })
    .await
    .unwrap();
    let Incoming::ConnAck(ack) = connected else {
        panic!("expected a CONNACK");
    };
    assert_eq!(ack.code, rumqttc::ConnectReturnCode::Success);
    assert!(!ack.session_present, "no persistent sessions");

    client
        .subscribe("sensors/+/temp", QoS::AtMostOnce)
        .await
        .unwrap();
    until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();

    client
        .publish("sensors/eu/temp", QoS::AtMostOnce, false, "21")
        .await
        .unwrap();

    let delivered = until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::Publish(_))
    })
    .await
    .unwrap();
    let Incoming::Publish(publish) = delivered else {
        panic!("expected a PUBLISH");
    };
    assert_eq!(publish.topic, "sensors/eu/temp");
    assert_eq!(publish.payload.as_ref(), b"21");
    assert_eq!(publish.qos, QoS::AtMostOnce);

    assert_eq!(broker.metrics().connections_accepted, 1);
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_travels_between_two_clients_at_least_once() {
    let broker = start();
    let (subscriber, mut subscriber_loop) = client("subscriber", &broker);
    subscriber.subscribe("a/b", QoS::AtLeastOnce).await.unwrap();
    until(&mut subscriber_loop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();

    let (publisher, mut publisher_loop) = client("publisher", &broker);
    publisher
        .publish("a/b", QoS::AtLeastOnce, false, "payload")
        .await
        .unwrap();

    // The publisher is told the broker took it.
    until(&mut publisher_loop, |incoming| {
        matches!(incoming, Incoming::PubAck(_))
    })
    .await
    .unwrap();

    // The subscriber receives it, and acknowledges it in turn.
    let delivered = tokio::spawn(async move {
        let incoming = until(&mut subscriber_loop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = incoming else {
            panic!("expected a PUBLISH");
        };
        (publish, subscriber_loop)
    });

    let (publish, mut subscriber_loop) = delivered.await.unwrap();
    assert_eq!(publish.qos, QoS::AtLeastOnce);
    assert_eq!(publish.payload.as_ref(), b"payload");

    // rumqttc sends the PUBACK itself; the broker must accept it and not
    // close the connection over it.
    let _ = tokio::time::timeout(Duration::from_millis(300), subscriber_loop.poll()).await;
    assert_eq!(broker.metrics().protocol_errors, 0);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unsubscribed_client_stops_receiving() {
    let broker = start();
    let (client, mut eventloop) = client("subscriber", &broker);
    client.subscribe("a/#", QoS::AtMostOnce).await.unwrap();
    until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();

    client.unsubscribe("a/#").await.unwrap();
    until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::UnsubAck(_))
    })
    .await
    .unwrap();

    client
        .publish("a/b", QoS::AtMostOnce, false, "nobody home")
        .await
        .unwrap();

    // Nothing arrives, and the connection stays healthy.
    let idle = tokio::time::timeout(Duration::from_millis(500), async {
        loop {
            if let Ok(Event::Incoming(Incoming::Publish(_))) = eventloop.poll().await {
                return;
            }
        }
    })
    .await;
    assert!(idle.is_err(), "a message arrived after unsubscribing");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_broker_answers_pings() {
    let broker = start();
    let addr = broker.local_addr();
    let mut options = MqttOptions::new("pinger", addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(1));
    let (_client, mut eventloop) = AsyncClient::new(options, 8);

    until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::PingResp)
    })
    .await
    .unwrap();

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_connection_with_the_same_name_takes_over() {
    let broker = start();
    let (_first, mut first_loop) = client("same-name", &broker);
    until(&mut first_loop, |incoming| {
        matches!(incoming, Incoming::ConnAck(_))
    })
    .await
    .unwrap();

    let (_second, mut second_loop) = client("same-name", &broker);
    until(&mut second_loop, |incoming| {
        matches!(incoming, Incoming::ConnAck(_))
    })
    .await
    .unwrap();

    // The first connection is closed by the broker, not left hanging.
    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if first_loop.poll().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the old connection survived the takeover");

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_closes_what_is_connected() {
    let broker = start();
    let (client, mut eventloop) = client("victim", &broker);
    client.subscribe("a/b", QoS::AtMostOnce).await.unwrap();
    until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();

    broker.shutdown().await;

    let ended = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if eventloop.poll().await.is_err() {
                return;
            }
        }
    })
    .await;
    assert!(ended.is_ok(), "the connection outlived the broker");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_durable_publish_reaches_the_disk_before_it_is_acknowledged() {
    use runtime::{Durability, WalSettings};

    let mut path = std::env::temp_dir();
    path.push(format!("straum-e2e-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let config = RuntimeConfig {
        transport: TransportConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..TransportConfig::default()
        },
        durability: Durability::Fsynced,
        wal: WalSettings {
            path: path.clone(),
            ..WalSettings::default()
        },
        ..RuntimeConfig::default()
    };
    let broker = Runtime::start(config).unwrap();

    let (client, mut eventloop) = client("durable", &broker);
    client
        .publish("a/b", QoS::AtLeastOnce, false, "durable-payload")
        .await
        .unwrap();

    // The PUBACK only exists because the log said the record is durable.
    until(&mut eventloop, |incoming| {
        matches!(incoming, Incoming::PubAck(_))
    })
    .await
    .unwrap();

    let metrics = broker.metrics();
    assert!(metrics.wal_commits >= 1, "nothing was committed");
    assert!(metrics.wal_records_committed >= 1);
    assert_eq!(metrics.wal_commit_failures, 0);
    assert!(metrics.wal_durable_sequence >= 1);

    broker.shutdown().await;

    // And it is on the disk, in a log that identifies itself.
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.starts_with(b"STRAUMW3"));
    assert!(
        bytes.windows(15).any(|window| window == b"durable-payload"),
        "the payload never reached the file"
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_recovered_log_keeps_growing() {
    use runtime::{Durability, WalSettings};

    let mut path = std::env::temp_dir();
    path.push(format!("straum-e2e-reopen-{}.wal", std::process::id()));
    let _ = std::fs::remove_file(&path);

    let config = |path: std::path::PathBuf| RuntimeConfig {
        transport: TransportConfig {
            listen: SocketAddr::from(([127, 0, 0, 1], 0)),
            ..TransportConfig::default()
        },
        durability: Durability::Fsynced,
        wal: WalSettings {
            path,
            ..WalSettings::default()
        },
        ..RuntimeConfig::default()
    };

    for round in 0..2 {
        let broker = Runtime::start(config(path.clone())).unwrap();
        let (client, mut eventloop) = client(&format!("round-{round}"), &broker);
        client
            .publish("a/b", QoS::AtLeastOnce, false, format!("round-{round}"))
            .await
            .unwrap();
        until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::PubAck(_))
        })
        .await
        .unwrap();
        broker.shutdown().await;
    }

    // Both rounds survived: the second start did not truncate the first.
    let bytes = std::fs::read(&path).unwrap();
    for round in 0..2 {
        let needle = format!("round-{round}");
        assert!(
            bytes
                .windows(needle.len())
                .any(|window| window == needle.as_bytes()),
            "{needle} is missing from the log"
        );
    }
    let _ = std::fs::remove_file(&path);
}

/// Credentials and rules, through a real client.
mod authentication {
    use super::*;

    use runtime::{AuthSettings, Authority};

    fn secured(passwords: &str, acl: &str, allow_anonymous: bool) -> Runtime {
        let config = RuntimeConfig {
            transport: TransportConfig {
                listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..TransportConfig::default()
            },
            auth: AuthSettings {
                passwords: passwords.to_owned(),
                acl: acl.to_owned(),
                allow_anonymous,
            },
            ..RuntimeConfig::default()
        };
        Runtime::start(config).unwrap()
    }

    fn credentials() -> String {
        let hash = Authority::hash_password(b"s3cret").unwrap();
        format!("device:{hash}\n")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_right_password_gets_in_and_the_wrong_one_does_not() {
        let broker = secured(&credentials(), "", false);
        let addr = broker.local_addr();

        let mut options = MqttOptions::new("device", addr.ip().to_string(), addr.port());
        options.set_credentials("device", "s3cret");
        let (_client, mut eventloop) = AsyncClient::new(options, 8);
        let connected = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::ConnAck(_))
        })
        .await
        .unwrap();
        let Incoming::ConnAck(ack) = connected else {
            panic!("expected a CONNACK");
        };
        assert_eq!(ack.code, rumqttc::ConnectReturnCode::Success);

        let mut options = MqttOptions::new("device", addr.ip().to_string(), addr.port());
        options.set_credentials("device", "not-the-password");
        let (_client, mut eventloop) = AsyncClient::new(options, 8);
        let refused = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::ConnAck(_))
        })
        .await;
        // rumqttc may surface the close before the CONNACK; either way the
        // session never started.
        if let Ok(Incoming::ConnAck(ack)) = refused {
            assert_eq!(
                ack.code,
                rumqttc::ConnectReturnCode::BadUserNamePassword,
                "a wrong password was accepted"
            );
        }
        assert!(broker.metrics().authentication_failures >= 1);

        broker.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn anonymous_is_refused_where_it_is_not_allowed() {
        let broker = secured(&credentials(), "", false);
        let addr = broker.local_addr();

        let options = MqttOptions::new("stranger", addr.ip().to_string(), addr.port());
        let (_client, mut eventloop) = AsyncClient::new(options, 8);
        let refused = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::ConnAck(_))
        })
        .await;
        if let Ok(Incoming::ConnAck(ack)) = refused {
            assert_eq!(ack.code, rumqttc::ConnectReturnCode::NotAuthorized);
        }
        assert!(broker.metrics().authentication_failures >= 1);

        broker.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_rules_decide_what_a_session_may_touch() {
        let acl = "user device\npublish sensors/+/temp\nsubscribe sensors/#\n";
        let broker = secured(&credentials(), acl, false);
        let addr = broker.local_addr();

        let mut options = MqttOptions::new("device", addr.ip().to_string(), addr.port());
        options.set_credentials("device", "s3cret");
        let (client, mut eventloop) = AsyncClient::new(options, 8);

        // A subscription the rules cover, and one they do not.
        client
            .subscribe("sensors/eu/temp", QoS::AtMostOnce)
            .await
            .unwrap();
        let Incoming::SubAck(granted) = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::SubAck(_))
        })
        .await
        .unwrap() else {
            panic!("expected a SUBACK");
        };
        assert_eq!(granted.return_codes.len(), 1);
        assert!(!matches!(
            granted.return_codes[0],
            rumqttc::SubscribeReasonCode::Failure
        ));

        client
            .subscribe("secrets/#", QoS::AtMostOnce)
            .await
            .unwrap();
        let Incoming::SubAck(refused) = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::SubAck(_))
        })
        .await
        .unwrap() else {
            panic!("expected a SUBACK");
        };
        assert!(matches!(
            refused.return_codes[0],
            rumqttc::SubscribeReasonCode::Failure
        ));

        // A publish the rules cover comes back; one they do not is dropped.
        client
            .publish("sensors/eu/temp", QoS::AtMostOnce, false, "21")
            .await
            .unwrap();
        let delivered = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.topic, "sensors/eu/temp");

        client
            .publish("secrets/payroll", QoS::AtMostOnce, false, "nope")
            .await
            .unwrap();
        let leaked = tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                if let Ok(Event::Incoming(Incoming::Publish(publish))) = eventloop.poll().await {
                    return publish.topic;
                }
            }
        })
        .await;
        assert!(leaked.is_err(), "an unauthorised publish was delivered");

        broker.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retained_message_greets_the_next_subscriber() {
    let broker = start();

    // Publish the state, then go away.
    let (publisher, mut publisher_loop) = client("publisher", &broker);
    publisher
        .publish("sensors/eu/temp", QoS::AtLeastOnce, true, "21")
        .await
        .unwrap();
    until(&mut publisher_loop, |incoming| {
        matches!(incoming, Incoming::PubAck(_))
    })
    .await
    .unwrap();
    publisher.disconnect().await.unwrap();

    // A subscriber that was never there hears the last value anyway.
    let (subscriber, mut subscriber_loop) = client("subscriber", &broker);
    subscriber
        .subscribe("sensors/#", QoS::AtMostOnce)
        .await
        .unwrap();
    let delivered = until(&mut subscriber_loop, |incoming| {
        matches!(incoming, Incoming::Publish(_))
    })
    .await
    .unwrap();
    let Incoming::Publish(publish) = delivered else {
        panic!("expected a PUBLISH");
    };

    assert_eq!(publish.topic, "sensors/eu/temp");
    assert_eq!(publish.payload.as_ref(), b"21");
    assert!(
        publish.retain,
        "the client could not tell it was stored state"
    );

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_survives_its_connection() {
    let broker = start();
    let addr = broker.local_addr();

    // CleanSession = 0: keep what this identity is owed.
    let mut options = MqttOptions::new("device", addr.ip().to_string(), addr.port());
    options.set_clean_session(false);
    let (device, mut device_loop) = AsyncClient::new(options.clone(), 8);
    device
        .subscribe("sensors/#", QoS::AtLeastOnce)
        .await
        .unwrap();
    until(&mut device_loop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();
    device.disconnect().await.unwrap();
    // Let the disconnect reach the broker before anything is published.
    let _ = tokio::time::timeout(Duration::from_millis(200), device_loop.poll()).await;
    let _ = tokio::time::timeout(Duration::from_millis(200), device_loop.poll()).await;

    // Published while the device is away.
    let (publisher, mut publisher_loop) = client("publisher", &broker);
    publisher
        .publish("sensors/eu/temp", QoS::AtLeastOnce, false, "21")
        .await
        .unwrap();
    until(&mut publisher_loop, |incoming| {
        matches!(incoming, Incoming::PubAck(_))
    })
    .await
    .unwrap();

    // The device comes back and is told its session was still there.
    let (_device, mut device_loop) = AsyncClient::new(options, 8);
    let connected = until(&mut device_loop, |incoming| {
        matches!(incoming, Incoming::ConnAck(_))
    })
    .await
    .unwrap();
    let Incoming::ConnAck(ack) = connected else {
        panic!("expected a CONNACK");
    };
    assert!(ack.session_present, "the session was not kept");

    let delivered = until(&mut device_loop, |incoming| {
        matches!(incoming, Incoming::Publish(_))
    })
    .await
    .unwrap();
    let Incoming::Publish(publish) = delivered else {
        panic!("expected a PUBLISH");
    };
    assert_eq!(publish.topic, "sensors/eu/temp");
    assert_eq!(publish.payload.as_ref(), b"21");

    broker.shutdown().await;
}

/// What the log is for: state that outlives the process.
mod restart {
    use super::*;

    use runtime::{Durability, WalSettings};

    fn durable(path: &std::path::Path) -> Runtime {
        with_compaction(path, 0)
    }

    /// `compact_bytes` of 0 never rewrites the log, which is what most of
    /// these tests want: they are about what survives, not about size.
    fn with_compaction(path: &std::path::Path, compact_bytes: u64) -> Runtime {
        let config = RuntimeConfig {
            transport: TransportConfig {
                listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..TransportConfig::default()
            },
            durability: Durability::Fsynced,
            wal: WalSettings {
                path: path.to_path_buf(),
                compact_bytes,
                ..WalSettings::default()
            },
            ..RuntimeConfig::default()
        };
        Runtime::start(config).unwrap()
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("straum-restart-{name}-{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&path);
        path
    }

    async fn publish_retained(broker: &Runtime, topic: &str, payload: &str) {
        let (client, mut eventloop) = client("publisher", broker);
        client
            .publish(topic, QoS::AtLeastOnce, true, payload)
            .await
            .unwrap();
        until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::PubAck(_))
        })
        .await
        .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retained_state_outlives_the_process() {
        let path = scratch("retained");

        let broker = durable(&path);
        publish_retained(&broker, "sensors/eu/temp", "21").await;
        broker.shutdown().await;

        // A different broker, the same log.
        let broker = durable(&path);
        let (subscriber, mut eventloop) = client("subscriber", &broker);
        subscriber
            .subscribe("sensors/#", QoS::AtMostOnce)
            .await
            .unwrap();

        let delivered = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.topic, "sensors/eu/temp");
        assert_eq!(publish.payload.as_ref(), b"21");
        assert!(publish.retain);

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_deleted_state_stays_deleted() {
        let path = scratch("deleted");

        let broker = durable(&path);
        publish_retained(&broker, "a/b", "value").await;
        // An empty payload is how the last value is forgotten.
        publish_retained(&broker, "a/b", "").await;
        broker.shutdown().await;

        let broker = durable(&path);
        let (subscriber, mut eventloop) = client("subscriber", &broker);
        subscriber.subscribe("a/#", QoS::AtMostOnce).await.unwrap();
        until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::SubAck(_))
        })
        .await
        .unwrap();

        let resurrected = tokio::time::timeout(Duration::from_millis(400), async {
            loop {
                if let Ok(Event::Incoming(Incoming::Publish(publish))) = eventloop.poll().await {
                    return publish.topic;
                }
            }
        })
        .await;
        assert!(resurrected.is_err(), "a deleted retained message came back");

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_last_value_is_the_one_that_comes_back() {
        let path = scratch("last");

        let broker = durable(&path);
        publish_retained(&broker, "a/b", "first").await;
        publish_retained(&broker, "a/b", "second").await;
        publish_retained(&broker, "a/b", "third").await;
        broker.shutdown().await;

        let broker = durable(&path);
        let (subscriber, mut eventloop) = client("subscriber", &broker);
        subscriber.subscribe("a/b", QoS::AtMostOnce).await.unwrap();
        let delivered = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.payload.as_ref(), b"third");

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A durable client that was not connected when the message was
    /// published, and not connected when the broker stopped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_kept_session_outlives_the_process() {
        let path = scratch("session");

        let broker = durable(&path);
        {
            let (device, mut device_loop) = durable_client("device", &broker);
            device
                .subscribe("sensors/+/temp", QoS::AtLeastOnce)
                .await
                .unwrap();
            until(&mut device_loop, |incoming| {
                matches!(incoming, Incoming::SubAck(_))
            })
            .await
            .unwrap();
            device.disconnect().await.unwrap();
            closed(&mut device_loop).await;
        }

        // Published while the device is away, and while it is away the
        // broker stops. Only the log connects the two.
        let (publisher, mut publisher_loop) = client("publisher", &broker);
        publisher
            .publish("sensors/eu/temp", QoS::AtLeastOnce, false, "21")
            .await
            .unwrap();
        until(&mut publisher_loop, |incoming| {
            matches!(incoming, Incoming::PubAck(_))
        })
        .await
        .unwrap();
        broker.shutdown().await;

        let broker = durable(&path);
        let (_device, mut device_loop) = durable_client("device", &broker);
        let connected = until(&mut device_loop, |incoming| {
            matches!(incoming, Incoming::ConnAck(_))
        })
        .await
        .unwrap();
        let Incoming::ConnAck(ack) = connected else {
            panic!("expected a CONNACK");
        };
        assert!(
            ack.session_present,
            "the session did not survive the restart"
        );

        let delivered = until(&mut device_loop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.topic, "sensors/eu/temp");
        assert_eq!(publish.payload.as_ref(), b"21");

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A clean start says the old session is gone, and a restart has to
    /// agree with that rather than resurrect it from the log.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_discarded_session_stays_discarded() {
        let path = scratch("discarded");

        let broker = durable(&path);
        {
            let (device, mut device_loop) = durable_client("device", &broker);
            device.subscribe("a/b", QoS::AtLeastOnce).await.unwrap();
            until(&mut device_loop, |incoming| {
                matches!(incoming, Incoming::SubAck(_))
            })
            .await
            .unwrap();
            device.disconnect().await.unwrap();
            closed(&mut device_loop).await;
        }
        {
            // CleanSession = 1 throws away what was kept.
            let (device, mut device_loop) = client("device", &broker);
            until(&mut device_loop, |incoming| {
                matches!(incoming, Incoming::ConnAck(_))
            })
            .await
            .unwrap();
            device.disconnect().await.unwrap();
            closed(&mut device_loop).await;
        }
        broker.shutdown().await;

        let broker = durable(&path);
        let (_device, mut device_loop) = durable_client("device", &broker);
        let connected = until(&mut device_loop, |incoming| {
            matches!(incoming, Incoming::ConnAck(_))
        })
        .await
        .unwrap();
        let Incoming::ConnAck(ack) = connected else {
            panic!("expected a CONNACK");
        };
        assert!(!ack.session_present, "a discarded session came back");

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    /// A log that records every version of every retained message grows
    /// without a bound. Compaction is what makes the old versions a size
    /// rather than a comment — and it has to keep the current one.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_drops_history_and_keeps_the_present() {
        let path = scratch("compaction");

        let broker = durable(&path);
        // The same topic over and over: every version but the last is
        // history the moment the next one lands.
        for version in 0..200 {
            publish_retained(&broker, "a/b", &format!("value-{version}")).await;
        }
        broker.shutdown().await;
        let before = std::fs::metadata(&path).unwrap().len();

        // A threshold below the current size, so this start rewrites it.
        let broker = with_compaction(&path, 1);
        let after = std::fs::metadata(&path).unwrap().len();
        assert!(
            after < before / 10,
            "the log did not shrink: {before} -> {after}"
        );

        // And the present survived the rewrite.
        let (subscriber, mut eventloop) = client("subscriber", &broker);
        subscriber.subscribe("a/b", QoS::AtMostOnce).await.unwrap();
        let delivered = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.payload.as_ref(), b"value-199");
        broker.shutdown().await;

        // A second restart reads back what the rewrite wrote, which is the
        // half that a rewrite gets wrong when it gets anything wrong.
        let broker = durable(&path);
        let (subscriber, mut eventloop) = client("again", &broker);
        subscriber.subscribe("a/b", QoS::AtMostOnce).await.unwrap();
        let delivered = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.payload.as_ref(), b"value-199");

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    /// Compaction must not drop what an offline session has not received.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn compaction_keeps_what_a_kept_session_is_still_owed() {
        let path = scratch("compaction-session");

        let broker = durable(&path);
        {
            let (device, mut device_loop) = durable_client("device", &broker);
            device.subscribe("a/b", QoS::AtLeastOnce).await.unwrap();
            until(&mut device_loop, |incoming| {
                matches!(incoming, Incoming::SubAck(_))
            })
            .await
            .unwrap();
            device.disconnect().await.unwrap();
            closed(&mut device_loop).await;
        }

        publish_many(&broker, "a/b", 50).await;
        broker.shutdown().await;

        // Rewrite, then make the device come back and count what it gets.
        let broker = with_compaction(&path, 1);
        let (device, mut device_loop) = durable_client("device", &broker);
        let mut seen = std::collections::HashSet::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while seen.len() < 50 {
            let Ok(Ok(event)) = tokio::time::timeout_at(deadline, device_loop.poll()).await else {
                break;
            };
            if let Event::Incoming(Incoming::Publish(publish)) = event {
                if let Ok(bytes) = <[u8; 4]>::try_from(publish.payload.as_ref()) {
                    seen.insert(u32::from_be_bytes(bytes));
                }
            }
        }
        assert_eq!(seen.len(), 50, "compaction dropped messages still owed");

        device.disconnect().await.unwrap();
        closed(&mut device_loop).await;
        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }

    /// The gap that made compaction a half-measure: a broker that never
    /// restarts never compacted. The log is rewritten beside the writer now.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_log_shrinks_without_a_restart() {
        let path = scratch("online-compaction");
        let threshold = 8 * 1024;
        let broker = with_compaction(&path, threshold);

        // The same topic over and over: every version but the last is
        // history the moment the next one lands, so a log nobody rewrites
        // grows without bound while the live set stays at one message.
        let size = || std::fs::metadata(&path).map(|held| held.len()).unwrap_or(0);
        let mut peak = 0;
        // One connection for all of them. The log grows because of the
        // records, not because of the clients, and reconnecting for each
        // publish cost 1200 handshakes to prove nothing about compaction.
        let (writer, mut writer_loop) = client("versions", &broker);
        // Enough versions to bury the live set in history and cross the
        // threshold with room to spare — the assertions below check that it
        // did. Each one is a separate durable commit, so the count is paid
        // for in fsyncs: what the test needs is bytes, and those come more
        // cheaply from the payload than from a thousand more round trips.
        for version in 0..250 {
            writer
                .publish(
                    "a/b",
                    QoS::AtLeastOnce,
                    true,
                    format!("value-{version:-<64}"),
                )
                .await
                .unwrap();
            until(&mut writer_loop, |incoming| {
                matches!(incoming, Incoming::PubAck(_))
            })
            .await
            .unwrap();
            peak = peak.max(size());
        }
        drop(writer);

        assert!(
            peak > threshold,
            "the workload never crossed the threshold, so nothing is proved: peak {peak} bytes against {threshold}"
        );
        assert!(
            broker.metrics().wal_compactions >= 1,
            "the log passed {threshold} bytes and was never rewritten"
        );
        assert!(
            size() < peak,
            "the log never came back down: {peak} bytes at its peak, {} now",
            size()
        );

        // And the present survived every rewrite.
        publish_retained(&broker, "a/b", "final").await;
        let (subscriber, mut eventloop) = client("subscriber", &broker);
        subscriber.subscribe("a/b", QoS::AtMostOnce).await.unwrap();
        let delivered = until(&mut eventloop, |incoming| {
            matches!(incoming, Incoming::Publish(_))
        })
        .await
        .unwrap();
        let Incoming::Publish(publish) = delivered else {
            panic!("expected a PUBLISH");
        };
        assert_eq!(publish.payload.as_ref(), b"final");

        broker.shutdown().await;
        let _ = std::fs::remove_file(&path);
    }
}

/// Shutting down under a client that stopped reading mid-delivery used to
/// hang forever. The accept loop aborts the connection task at the end of
/// the grace period, but the reader and writer it had spawned were detached
/// and survived it — and the reader holds a command sender, which is the one
/// thing keeping the broker loop from ending (§16).
///
/// The broker has to stop even while a subscriber has stopped reading with
/// deliveries in flight — the case where the accept loop runs out of grace
/// and aborts a connection that is still busy.
///
/// The timeout is the assertion: without it a regression is an unkillable
/// test rather than a failing one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broker_stops_under_a_client_that_went_silent() {
    let broker = start();
    let (subscriber, mut subscriber_loop) = client("silent", &broker);
    subscriber.subscribe("a/b", QoS::AtLeastOnce).await.unwrap();
    until(&mut subscriber_loop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();

    publish_many(&broker, "a/b", 40).await;

    // From here nobody polls the subscriber: deliveries are in flight, and
    // its socket stays open with nothing reading it.
    drop(subscriber_loop);

    tokio::time::timeout(Duration::from_secs(20), broker.shutdown())
        .await
        .expect("the broker did not stop");
}

/// A will is the client's last words, and 3.1.1 §3.1.2.5 says they are
/// spoken when the connection ends any way but a DISCONNECT.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_will_reaches_subscribers_when_the_client_vanishes() {
    let broker = start();
    let (watcher, mut watcher_loop) = client("watcher", &broker);
    watcher.subscribe("gone/#", QoS::AtMostOnce).await.unwrap();
    until(&mut watcher_loop, |incoming| {
        matches!(incoming, Incoming::SubAck(_))
    })
    .await
    .unwrap();

    {
        let addr = broker.local_addr();
        let mut options = MqttOptions::new("doomed", addr.ip().to_string(), addr.port());
        options.set_keep_alive(Duration::from_secs(5));
        options.set_last_will(rumqttc::LastWill::new(
            "gone/doomed",
            "offline",
            QoS::AtMostOnce,
            false,
        ));
        let (_doomed, mut doomed_loop) = AsyncClient::new(options, 8);
        until(&mut doomed_loop, |incoming| {
            matches!(incoming, Incoming::ConnAck(_))
        })
        .await
        .unwrap();
        // Dropped without a DISCONNECT: the socket just goes.
    }

    let delivered = until(&mut watcher_loop, |incoming| {
        matches!(incoming, Incoming::Publish(_))
    })
    .await
    .unwrap();
    let Incoming::Publish(publish) = delivered else {
        panic!("expected a PUBLISH");
    };
    assert_eq!(publish.topic, "gone/doomed");
    assert_eq!(publish.payload.as_ref(), b"offline");

    broker.shutdown().await;
}
