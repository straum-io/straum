//! What a broker still has after it is killed, and what it promised before.
//!
//! This is the one comparison that does not need a quiet host. It counts
//! messages, not nanoseconds: the answer is an integer that a virtualised
//! network cannot move (docs/benchmarks.md §7, and the reason latency
//! numbers are not taken on a development Mac).
//!
//! The question is narrow and the whole protocol answers it. A QoS 1
//! PUBACK is the broker saying "this message is mine now". A CleanSession=0
//! subscriber that is not connected is a promise to keep what arrives for
//! it. Kill the broker between the promise and the delivery, and whatever
//! does not come back was a promise the broker could not keep.
//!
//! Two phases, because killing the broker is the shell's job:
//!
//!   survive --address host:1883 --arm    --messages 5000
//!   <kill -9 the broker, start it again on the same state>
//!   survive --address host:1883 --verify --messages 5000 --label straum
//!
//! Arming subscribes a durable session, takes it offline, then publishes
//! and counts acknowledgements. Verifying brings the same identity back and
//! counts what the broker hands over.

use std::collections::HashSet;
use std::time::Duration;

use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};

const DATA: &str = "survive/data";
const RETAINED: &str = "survive/retained";
const SUBSCRIBER: &str = "survive-session";

struct Settings {
    address: String,
    messages: u64,
    label: String,
    /// Seconds to wait for a broker to hand over what it kept.
    grace: u64,
    phase: Phase,
}

#[derive(PartialEq, Eq)]
enum Phase {
    Arm,
    Verify,
}

fn settings() -> Settings {
    let mut settings = Settings {
        address: "127.0.0.1:1883".to_owned(),
        messages: 1000,
        label: "broker".to_owned(),
        grace: 10,
        phase: Phase::Arm,
    };
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let mut value = || args.next().unwrap_or_default();
        match flag.as_str() {
            "--address" => settings.address = value(),
            "--messages" => settings.messages = value().parse().unwrap_or(1000),
            "--label" => settings.label = value(),
            "--grace" => settings.grace = value().parse().unwrap_or(10),
            "--arm" => settings.phase = Phase::Arm,
            "--verify" => settings.phase = Phase::Verify,
            _ => {}
        }
    }
    settings
}

fn options(settings: &Settings, name: &str, clean: bool) -> MqttOptions {
    let (host, port) = settings
        .address
        .rsplit_once(':')
        .unwrap_or((settings.address.as_str(), "1883"));
    let mut options = MqttOptions::new(name, host, port.parse().unwrap_or(1883));
    options.set_keep_alive(Duration::from_secs(30));
    options.set_clean_session(clean);
    options
}

/// Polls until `wanted` says yes, or gives up rather than hanging.
async fn until(
    eventloop: &mut rumqttc::EventLoop,
    seconds: u64,
    wanted: impl Fn(&Incoming) -> bool,
) -> Option<Incoming> {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    loop {
        let polled = tokio::time::timeout_at(deadline, eventloop.poll())
            .await
            .ok()?;
        if let Ok(Event::Incoming(incoming)) = polled {
            if wanted(&incoming) {
                return Some(incoming);
            }
        }
    }
}

/// Polls until the connection is gone, so what happens next happens to a
/// broker that already knows the subscriber left.
async fn closed(eventloop: &mut rumqttc::EventLoop, seconds: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(seconds);
    while tokio::time::timeout_at(deadline, eventloop.poll())
        .await
        .is_ok_and(|polled| polled.is_ok())
    {}
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let settings = settings();
    match settings.phase {
        Phase::Arm => arm(settings).await,
        Phase::Verify => verify(settings).await,
    }
}

/// Leaves the broker holding a promise: one offline durable session, one
/// retained message, and `messages` acknowledged publishes for that session.
async fn arm(settings: Settings) {
    // A durable identity that subscribes and then goes away. From here on
    // the broker owes it everything published to the filter.
    let (subscriber, mut subscriber_loop) =
        AsyncClient::new(options(&settings, SUBSCRIBER, false), 64);
    if subscriber
        .subscribe("survive/#", QoS::AtLeastOnce)
        .await
        .is_err()
        || until(&mut subscriber_loop, 10, |incoming| {
            matches!(incoming, Incoming::SubAck(_))
        })
        .await
        .is_none()
    {
        println!("arm-failed subscribe");
        return;
    }
    let _ = subscriber.disconnect().await;
    closed(&mut subscriber_loop, 5).await;

    let (publisher, mut publisher_loop) =
        AsyncClient::new(options(&settings, "survive-pub", true), 64);

    // Retained first, so a broker that keeps nothing else still has this
    // to show.
    let _ = publisher
        .publish(RETAINED, QoS::AtLeastOnce, true, b"retained".to_vec())
        .await;

    let mut acknowledged = 0u64;
    let publishing = tokio::spawn({
        let publisher = publisher.clone();
        let total = settings.messages;
        async move {
            for sequence in 0..total {
                // The sequence travels in the payload: what comes back has
                // to be identifiable, not merely counted.
                if publisher
                    .publish(
                        DATA,
                        QoS::AtLeastOnce,
                        false,
                        sequence.to_be_bytes().to_vec(),
                    )
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    });

    // One PUBACK per publish, plus the retained one.
    let wanted = settings.messages.saturating_add(1);
    let deadline =
        tokio::time::Instant::now() + Duration::from_secs(settings.grace.saturating_mul(6));
    while acknowledged < wanted {
        match tokio::time::timeout_at(deadline, publisher_loop.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::PubAck(_)))) => acknowledged += 1,
            Ok(Ok(_)) => {}
            _ => break,
        }
    }
    let _ = publishing.await;

    // Only acknowledged publishes are a promise; the rest were in flight
    // and the broker never claimed them.
    println!("{}", acknowledged.saturating_sub(1).min(settings.messages));
}

/// Counts what the broker still has. `promised` is what it acknowledged
/// before it was killed.
async fn verify(settings: Settings) {
    let (subscriber, mut subscriber_loop) =
        AsyncClient::new(options(&settings, SUBSCRIBER, false), 64);

    let connected = until(&mut subscriber_loop, settings.grace, |incoming| {
        matches!(incoming, Incoming::ConnAck(_))
    })
    .await;
    let session_present = matches!(
        connected,
        Some(Incoming::ConnAck(ref ack)) if ack.session_present
    );

    // A session the broker forgot has to be re-subscribed, or the retained
    // message would be counted as lost for the wrong reason.
    if !session_present {
        let _ = subscriber.subscribe("survive/#", QoS::AtLeastOnce).await;
    }

    let mut recovered: HashSet<u64> = HashSet::new();
    let mut retained = false;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(settings.grace);
    // Deliveries stop arriving before the deadline on a broker that kept
    // nothing; the deadline is there so one that kept everything is not cut
    // short, and so neither case hangs.
    while let Ok(Ok(event)) = tokio::time::timeout_at(deadline, subscriber_loop.poll()).await {
        let Event::Incoming(Incoming::Publish(publish)) = event else {
            continue;
        };
        if publish.topic == RETAINED {
            retained = true;
            continue;
        }
        if let Ok(sequence) = <[u8; 8]>::try_from(publish.payload.as_ref()) {
            recovered.insert(u64::from_be_bytes(sequence));
        }
    }

    let promised = settings.messages;
    let kept = recovered.len() as u64;
    let lost = promised.saturating_sub(kept);
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>9.2} {:>10} {:>10}",
        settings.label,
        promised,
        kept,
        lost,
        if promised == 0 {
            0.0
        } else {
            lost as f64 * 100.0 / promised as f64
        },
        if session_present { "yes" } else { "no" },
        if retained { "yes" } else { "no" },
    );
}
