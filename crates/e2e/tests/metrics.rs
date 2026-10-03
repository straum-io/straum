//! The counters, as an operator sees them.
//!
//! Until now they existed and left the process by no route at all, which
//! made every one of them unactionable. What is tested here is the route.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::time::Duration;

use rumqttc::{AsyncClient, MqttOptions, QoS};
use runtime::{Runtime, RuntimeConfig, TransportConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn any_port() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], 0))
}

async fn scrape(at: SocketAddr) -> String {
    let mut socket = tokio::net::TcpStream::connect(at).await.unwrap();
    socket
        .write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let mut answer = String::new();
    socket.read_to_string(&mut answer).await.unwrap();
    answer
}

fn value(body: &str, name: &str) -> u64 {
    body.lines()
        .find(|line| line.starts_with(&format!("{name} ")))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|held| held.parse().ok())
        .unwrap_or_else(|| panic!("{name} is not in the exposition:\n{body}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_counters_can_be_read_from_outside_the_process() {
    // Port 0 would leave the test with no address to scrape, so ask the
    // kernel for one the ordinary way and hand it over.
    let probe = tokio::net::TcpListener::bind(any_port()).await.unwrap();
    let at = probe.local_addr().unwrap();
    drop(probe);

    let broker = Runtime::start(RuntimeConfig {
        transport: TransportConfig {
            listen: any_port(),
            ..TransportConfig::default()
        },
        metrics: Some(at),
        ..RuntimeConfig::default()
    })
    .unwrap();

    let body = scrape(at).await;
    assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
    assert!(body.contains("Content-Type: text/plain; version=0.0.4"));
    // Every metric carries its type, because a counter and a gauge are read
    // differently and a reader cannot tell them apart from the value.
    assert!(body.contains("# TYPE straum_connections_accepted counter"));
    assert!(body.contains("# TYPE straum_active_sessions gauge"));
    assert_eq!(value(&body, "straum_connections_accepted"), 0);

    // Something an operator would actually watch: a client connects and
    // publishes, and the numbers move.
    let addr = broker.local_addr();
    let mut options = MqttOptions::new("watched", addr.ip().to_string(), addr.port());
    options.set_keep_alive(Duration::from_secs(5));
    let (client, mut eventloop) = AsyncClient::new(options, 16);
    // Requests leave from a second task: `publish` only queues, and nothing
    // leaves the queue unless the event loop is being polled.
    let sending = client.clone();
    let publisher = tokio::spawn(async move {
        for _ in 0..8 {
            sending
                .publish("metrics/topic", QoS::AtLeastOnce, false, vec![b'x'; 32])
                .await
                .unwrap();
        }
    });
    // The PUBACKs, not the socket closing: the event loop only notices a
    // closed socket when its keep-alive timer fires, which cost five
    // seconds here and proved nothing.
    let mut acked = 0usize;
    while acked < 8 {
        if let rumqttc::Event::Incoming(rumqttc::Packet::PubAck(_)) =
            eventloop.poll().await.unwrap()
        {
            acked += 1;
        }
    }
    publisher.await.unwrap();
    client.disconnect().await.unwrap();
    while !matches!(
        eventloop.poll().await,
        Ok(rumqttc::Event::Outgoing(rumqttc::Outgoing::Disconnect)) | Err(_)
    ) {}

    let body = scrape(at).await;
    assert_eq!(value(&body, "straum_connections_accepted"), 1);
    assert!(
        value(&body, "straum_bytes_in") > 0,
        "bytes_in stayed at zero:\n{body}"
    );

    // A scrape is not a connection the broker counts as a client.
    assert_eq!(value(&body, "straum_connections_accepted"), 1);

    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nothing_listens_unless_an_address_is_configured() {
    let broker = Runtime::start(RuntimeConfig {
        transport: TransportConfig {
            listen: any_port(),
            ..TransportConfig::default()
        },
        ..RuntimeConfig::default()
    })
    .unwrap();
    // The default is off: counters that are served to anybody who can reach
    // a port are an operator's decision, not a default.
    assert!(RuntimeConfig::default().metrics.is_none());
    broker.shutdown().await;
}

/// An at-most-once delivery a full session queue had no room for is
/// dropped — CLAUDE.md §8 allows it — and an operator can see that it was
/// (D-14). Until then the broker counted it and nobody outside could read it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropped_at_most_once_deliveries_are_counted_outside() {
    let probe = tokio::net::TcpListener::bind(any_port()).await.unwrap();
    let at = probe.local_addr().unwrap();
    drop(probe);

    let broker = Runtime::start(RuntimeConfig {
        transport: TransportConfig {
            listen: any_port(),
            ..TransportConfig::default()
        },
        // A short queue, so a subscriber that stops reading fills it quickly.
        session_queue_messages: 4,
        metrics: Some(at),
        ..RuntimeConfig::default()
    })
    .unwrap();
    assert_eq!(value(&scrape(at).await, "straum_dropped_messages"), 0);

    let addr = broker.local_addr();
    // Subscribes, then stops polling: nothing reads its socket again, so the
    // kernel's buffers fill, its writer stalls and its queue fills behind it.
    let (stuck, mut stuck_loop) = AsyncClient::new(
        MqttOptions::new("stuck", addr.ip().to_string(), addr.port()),
        16,
    );
    stuck
        .subscribe("load/topic", QoS::AtMostOnce)
        .await
        .unwrap();
    loop {
        if let rumqttc::Event::Incoming(rumqttc::Packet::SubAck(_)) =
            stuck_loop.poll().await.unwrap()
        {
            break;
        }
    }

    let (publisher, mut publisher_loop) = AsyncClient::new(
        MqttOptions::new("publisher", addr.ip().to_string(), addr.port()),
        16,
    );
    let publishing = tokio::spawn(async move { while publisher_loop.poll().await.is_ok() {} });
    // Published until the drop shows, with a bound so a broken run fails
    // instead of hanging. Each message is 4 KiB, so the socket buffers in
    // between fill after a few hundred.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let payload = vec![b'x'; 4096];
    while broker.metrics().dropped_messages == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "no at-most-once delivery was dropped"
        );
        for _ in 0..64 {
            publisher
                .publish("load/topic", QoS::AtMostOnce, false, payload.clone())
                .await
                .unwrap();
        }
        tokio::task::yield_now().await;
    }

    // And outside the process, as a counter.
    let body = scrape(at).await;
    assert!(
        body.contains("# TYPE straum_dropped_messages counter"),
        "{body}"
    );
    assert!(value(&body, "straum_dropped_messages") > 0);

    publishing.abort();
    drop(stuck_loop);
    broker.shutdown().await;
}
