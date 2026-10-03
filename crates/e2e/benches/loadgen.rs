//! An open-loop load generator that talks to any MQTT 3.1.1 broker.
//!
//! It is a separate binary pointed at an address on purpose: the same
//! generator measures straum and whatever it is being compared with, so the
//! numbers differ because the brokers differ and not because the harnesses do
//! (docs/benchmarks.md §2).
//!
//! Open loop means the send schedule is fixed in advance. A generator that
//! sends the next message only after the previous reply arrives cannot
//! observe a latency longer than its own think time — it would report the p99
//! of a system that had slowed it down. Here the intended send time travels
//! inside the payload, and latency is measured against it, so a broker that
//! cannot keep up shows growing latency rather than a lower rate.
//!
//!   loadgen --address 127.0.0.1:1883 --publishers 4 --subscribers 4 \
//!           --rate 20000 --payload 64 --qos 1 --seconds 10

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::sync::Mutex;

struct Settings {
    address: String,
    publishers: usize,
    subscribers: usize,
    /// Messages per second across all publishers; 0 means as fast as possible.
    rate: u64,
    payload: usize,
    qos: QoS,
    seconds: u64,
    topic: String,
    mode: Mode,
    label: String,
}

#[derive(PartialEq, Eq)]
enum Mode {
    Throughput,
    /// How fast the broker takes new connections, which is where
    /// authentication and TLS show up.
    Connects,
}

fn settings() -> Settings {
    let mut settings = Settings {
        address: "127.0.0.1:1883".to_owned(),
        publishers: 1,
        subscribers: 1,
        rate: 0,
        payload: 64,
        qos: QoS::AtMostOnce,
        seconds: 5,
        topic: "bench/topic".to_owned(),
        mode: Mode::Throughput,
        label: "broker".to_owned(),
    };

    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().unwrap_or_default();
        match flag.as_str() {
            "--address" => settings.address = value(),
            "--publishers" => settings.publishers = value().parse().unwrap_or(1),
            "--subscribers" => settings.subscribers = value().parse().unwrap_or(1),
            "--rate" => settings.rate = value().parse().unwrap_or(0),
            "--payload" => settings.payload = value().parse().unwrap_or(64),
            "--qos" => {
                settings.qos = match value().as_str() {
                    "1" => QoS::AtLeastOnce,
                    _ => QoS::AtMostOnce,
                }
            }
            "--seconds" => settings.seconds = value().parse().unwrap_or(5),
            "--topic" => settings.topic = value(),
            "--label" => settings.label = value(),
            "--connects" => settings.mode = Mode::Connects,
            _ => {}
        }
    }
    settings
}

fn options(settings: &Settings, name: &str) -> MqttOptions {
    let (host, port) = settings
        .address
        .rsplit_once(':')
        .unwrap_or((settings.address.as_str(), "1883"));
    let mut options = MqttOptions::new(name, host, port.parse().unwrap_or(1883));
    options.set_keep_alive(Duration::from_secs(30));
    options.set_max_packet_size(2 * 1024 * 1024, 2 * 1024 * 1024);
    options
}

/// Latencies in nanoseconds, allocated before the run and sorted afterwards.
struct Samples(Mutex<Vec<u64>>);

impl Samples {
    fn new(capacity: usize) -> Self {
        Self(Mutex::new(Vec::with_capacity(capacity)))
    }

    async fn record(&self, nanos: u64) {
        self.0.lock().await.push(nanos);
    }

    async fn percentiles(&self) -> (u64, u64, u64) {
        let mut samples = self.0.lock().await;
        if samples.is_empty() {
            return (0, 0, 0);
        }
        samples.sort_unstable();
        let at = |fraction: f64| {
            let index = ((samples.len() - 1) as f64 * fraction).round() as usize;
            samples.get(index).copied().unwrap_or(0)
        };
        (at(0.5), at(0.95), at(0.99))
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    // `cargo test --all-targets` runs this program too, as a debug build and
    // with no arguments, and would spend minutes measuring a build whose
    // numbers mean nothing. Every real run is a release build — `cargo
    // bench`, or `cargo build --release --bench` and the binary run directly
    // — so that is the line, not the arguments (CLAUDE.md §17).
    if cfg!(debug_assertions) {
        eprintln!("loadgen: built without optimisation; use `cargo bench` or a --release build");
        return;
    }
    let settings = settings();
    match settings.mode {
        Mode::Throughput => throughput(settings).await,
        Mode::Connects => connects(settings).await,
    }
}

async fn throughput(settings: Settings) {
    let settings = Arc::new(settings);
    let received = Arc::new(AtomicU64::new(0));
    let published = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicUsize::new(0));
    let samples = Arc::new(Samples::new(1_000_000));
    let started = Instant::now();
    let deadline = started + Duration::from_secs(settings.seconds);

    // Subscribers first, so nothing is published into an empty broker.
    let mut consumers = Vec::new();
    for index in 0..settings.subscribers {
        let settings = Arc::clone(&settings);
        let received = Arc::clone(&received);
        let bytes = Arc::clone(&bytes);
        let samples = Arc::clone(&samples);
        let (client, mut eventloop) = AsyncClient::new(
            options(&settings, &format!("{}-sub-{index}", settings.label)),
            1024,
        );
        client
            .subscribe(settings.topic.clone(), settings.qos)
            .await
            .unwrap_or_else(|error| panic!("subscribe failed: {error}"));

        consumers.push(tokio::spawn(async move {
            loop {
                let event = tokio::time::timeout_at(deadline.into(), eventloop.poll()).await;
                match event {
                    Ok(Ok(Event::Incoming(Incoming::Publish(publish)))) => {
                        received.fetch_add(1, Ordering::Relaxed);
                        bytes.fetch_add(publish.payload.len(), Ordering::Relaxed);
                        if let Some(sent) = intended(&publish.payload) {
                            let elapsed = started.elapsed().as_nanos() as u64;
                            samples.record(elapsed.saturating_sub(sent)).await;
                        }
                    }
                    Ok(Ok(_)) => {}
                    // A subscriber that stops counting is the loudest thing
                    // this generator can observe, so it says why. Breaking
                    // quietly here once hid a broker dropping connections as
                    // a broker that had gone slow.
                    Ok(Err(error)) => {
                        eprintln!("subscriber {index} lost its connection: {error}");
                        break;
                    }
                    Err(_) => break,
                }
            }
        }));
    }
    // Let the subscriptions settle before the first message.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut producers = Vec::new();
    for index in 0..settings.publishers {
        let settings = Arc::clone(&settings);
        let published = Arc::clone(&published);
        let (client, mut eventloop) = AsyncClient::new(
            options(&settings, &format!("{}-pub-{index}", settings.label)),
            1024,
        );
        // The event loop has to be polled for anything to be sent.
        tokio::spawn(async move {
            while tokio::time::timeout_at(deadline.into(), eventloop.poll())
                .await
                .is_ok_and(|event| event.is_ok())
            {}
        });

        producers.push(tokio::spawn(async move {
            let per_publisher = settings.rate / settings.publishers.max(1) as u64;
            let mut sequence = 0u64;
            let mut payload = vec![b'x'; settings.payload.max(8)];

            while Instant::now() < deadline {
                // The schedule is fixed in advance: falling behind it shows up
                // as latency, never as a lower rate.
                // The intended time, not the actual one: a generator that
                // timestamps when it got around to sending hides exactly
                // the delay it is supposed to measure.
                let mut intended = started.elapsed().as_nanos() as u64;
                if per_publisher > 0 {
                    let due = Duration::from_nanos(
                        sequence.saturating_mul(1_000_000_000) / per_publisher.max(1),
                    );
                    intended = due.as_nanos() as u64;
                    let elapsed = started.elapsed();
                    if due > elapsed {
                        tokio::time::sleep(due - elapsed).await;
                    }
                }
                payload[..8].copy_from_slice(&intended.to_be_bytes());

                if client
                    .publish(settings.topic.clone(), settings.qos, false, payload.clone())
                    .await
                    .is_err()
                {
                    break;
                }
                published.fetch_add(1, Ordering::Relaxed);
                sequence = sequence.saturating_add(1);
            }
        }));
    }

    for producer in producers {
        let _ = producer.await;
    }
    for consumer in consumers {
        let _ = consumer.await;
    }

    let elapsed = started.elapsed().as_secs_f64();
    let (p50, p95, p99) = samples.percentiles().await;
    let published = published.load(Ordering::Relaxed);
    let received = received.load(Ordering::Relaxed);

    println!(
        "{:<12} {:>10} {:>10} {:>11.0} {:>11.0} {:>9.2} {:>9.2} {:>9.2} {:>8}",
        settings.label,
        published,
        received,
        published as f64 / elapsed,
        received as f64 / elapsed,
        p50 as f64 / 1e6,
        p95 as f64 / 1e6,
        p99 as f64 / 1e6,
        settings.payload,
    );
}

/// The first eight bytes carry the time the message was meant to be sent.
fn intended(payload: &[u8]) -> Option<u64> {
    let head: [u8; 8] = payload.get(..8)?.try_into().ok()?;
    Some(u64::from_be_bytes(head))
}

/// Connect storms: what a fleet coming back after a network blip costs.
async fn connects(settings: Settings) {
    let settings = Arc::new(settings);
    let samples = Samples::new(settings.publishers.max(1));
    let started = Instant::now();
    let mut tasks = Vec::new();

    for index in 0..settings.publishers {
        let settings = Arc::clone(&settings);
        tasks.push(tokio::spawn(async move {
            let began = Instant::now();
            let (_client, mut eventloop) = AsyncClient::new(
                options(&settings, &format!("{}-conn-{index}", settings.label)),
                8,
            );
            loop {
                match tokio::time::timeout(Duration::from_secs(30), eventloop.poll()).await {
                    Ok(Ok(Event::Incoming(Incoming::ConnAck(_)))) => {
                        return Some(began.elapsed().as_nanos() as u64);
                    }
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) | Err(_) => return None,
                }
            }
        }));
    }

    let mut connected = 0u64;
    for task in tasks {
        if let Ok(Some(nanos)) = task.await {
            connected += 1;
            samples.record(nanos).await;
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    let (p50, p95, p99) = samples.percentiles().await;
    println!(
        "{:<12} {:>10} {:>10} {:>11.0} {:>11} {:>9.2} {:>9.2} {:>9.2} {:>8}",
        settings.label,
        settings.publishers,
        connected,
        connected as f64 / elapsed,
        "-",
        p50 as f64 / 1e6,
        p95 as f64 / 1e6,
        p99 as f64 / 1e6,
        "-",
    );
}
