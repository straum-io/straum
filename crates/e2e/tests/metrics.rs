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
    let driving = tokio::spawn(async move { while eventloop.poll().await.is_ok() {} });
    for index in 0..8 {
        client
            .publish("metrics/topic", QoS::AtLeastOnce, false, vec![b'x'; 32])
            .await
            .unwrap();
        let _ = index;
    }
    client.disconnect().await.unwrap();
    let _ = driving.await;

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
