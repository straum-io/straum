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
    assert!(bytes.starts_with(b"STRAUMW1"));
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
