//! Routing one publish: the required broker benchmarks of
//! docs/benchmarks.md §3.

use auth::Permissions;
use broker::{Broker, BrokerLimits};
use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee, SessionId, Topic, TopicFilter};
use protocol::{Command, Connect, Outbox, Publish, RequestId};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const TOPIC: &str = "sensors/eu-west/rack12/node7/temperature";

fn topic(value: &str) -> Topic {
    match Topic::new(Bytes::copy_from_slice(value.as_bytes())) {
        Ok(topic) => topic,
        Err(error) => panic!("invalid benchmark topic: {error}"),
    }
}

fn filter(value: &str) -> TopicFilter {
    match TopicFilter::new(Bytes::copy_from_slice(value.as_bytes())) {
        Ok(filter) => filter,
        Err(error) => panic!("invalid benchmark filter: {error}"),
    }
}

fn client(index: usize) -> ClientId {
    match ClientId::new(Bytes::from(format!("client-{index}"))) {
        Ok(client) => client,
        Err(error) => panic!("invalid benchmark client id: {error}"),
    }
}

/// A broker with `subscribers` sessions on `filters` each, plus `noise`
/// subscriptions that never match.
fn broker(subscribers: usize, pattern: &str, noise: usize) -> (Broker, Outbox) {
    let mut broker = Broker::new(BrokerLimits::default());
    let mut out = Outbox::new();

    for index in 0..subscribers {
        let session = SessionId(index as u64);
        broker.handle(
            session,
            Command::Connect(Connect {
                client_id: client(index),
                permissions: Permissions::unrestricted(),
                resume: false,
            }),
            &mut out,
        );
        broker.handle(
            session,
            Command::Subscribe {
                request: RequestId(1),
                filters: vec![(filter(pattern), DeliveryGuarantee::AtMostOnce)],
            },
            &mut out,
        );
    }

    let noisy = SessionId(u64::MAX);
    if noise > 0 {
        broker.handle(
            noisy,
            Command::Connect(Connect {
                client_id: client(usize::MAX),
                permissions: Permissions::unrestricted(),
                resume: false,
            }),
            &mut out,
        );
        let filters = (0..noise)
            .map(|index| {
                (
                    filter(&format!("devices/site{index}/+/state")),
                    DeliveryGuarantee::AtMostOnce,
                )
            })
            .collect();
        broker.handle(
            noisy,
            Command::Subscribe {
                request: RequestId(2),
                filters,
            },
            &mut out,
        );
    }

    let _ = out.take();
    (broker, out)
}

fn publish() -> Publish {
    Publish {
        topic: topic(TOPIC),
        payload: Bytes::from_static(&[b'x'; 64]),
        guarantee: DeliveryGuarantee::AtMostOnce,
        retain: false,
    }
}

fn bench_route(bencher: divan::Bencher, subscribers: usize, pattern: &str, noise: usize) {
    let (mut broker, mut out) = broker(subscribers, pattern, noise);
    let publish = publish();
    // The publisher is a session too: an unknown one is ignored.
    let publisher = SessionId(1_000_000);
    broker.handle(
        publisher,
        Command::Connect(Connect {
            client_id: client(usize::MAX - 1),
            permissions: Permissions::unrestricted(),
            resume: false,
        }),
        &mut out,
    );
    let _ = out.take();

    bencher
        .with_inputs(|| publish.clone())
        .bench_local_values(|publish| {
            broker.handle(
                publisher,
                Command::Publish {
                    ack: None,
                    publish: divan::black_box(publish),
                },
                &mut out,
            );
            let delivered = out.take().count();
            divan::black_box(delivered)
        });
}

/// Exact-match subscriptions, one per session.
#[divan::bench(args = [1, 10, 100, 1000])]
fn route(bencher: divan::Bencher, subscribers: usize) {
    bench_route(bencher, subscribers, TOPIC, 0);
}

#[divan::bench(args = [1, 100])]
fn route_wildcard(bencher: divan::Bencher, subscribers: usize) {
    bench_route(bencher, subscribers, "sensors/+/rack12/+/temperature", 0);
}

/// One matching subscriber behind a large index: does the index care how many
/// subscriptions exist that do not match?
#[divan::bench(args = [100, 10_000])]
fn many_subscriptions(bencher: divan::Bencher, noise: usize) {
    bench_route(bencher, 1, TOPIC, noise);
}

/// The whole command path for one publish, acknowledgement included.
#[divan::bench(args = [1, 100])]
fn publish_route(bencher: divan::Bencher, subscribers: usize) {
    let (mut broker, mut out) = broker(subscribers, TOPIC, 0);
    let publish = publish();
    // The publisher is a session too: an unknown one is ignored.
    let publisher = SessionId(1_000_000);
    broker.handle(
        publisher,
        Command::Connect(Connect {
            client_id: client(usize::MAX - 1),
            permissions: Permissions::unrestricted(),
            resume: false,
        }),
        &mut out,
    );
    let _ = out.take();

    bencher
        .with_inputs(|| publish.clone())
        .bench_local_values(|publish| {
            broker.handle(
                publisher,
                Command::Publish {
                    ack: Some(RequestId(7)),
                    publish: divan::black_box(publish),
                },
                &mut out,
            );
            divan::black_box(out.take().count())
        });
}
