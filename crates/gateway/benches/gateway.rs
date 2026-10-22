//! The gateway per packet (ADR-0008 step 4 baseline): an incoming publish
//! (decode, proposal) and an outgoing delivery (effect, encode).

use std::num::NonZeroU16;
use std::time::Duration;

use apply::{Effect, Origin};
use bytes::{Bytes, BytesMut};
use codec::{Connect, ConnectProperties, Packet, Publish, Qos, Sender, Version};
use domain::{ClientId, DeliveryGuarantee, Topic};
use gateway::{Config, Gateway};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const T0: Duration = Duration::ZERO;
const TOPIC: &[u8] = b"sensors/eu-west/rack12/node7/temperature";

fn config() -> Config {
    Config {
        node: 1,
        first_session: 1,
        codec: codec::Limits::default(),
        max_connections: 16,
        connect_timeout: Duration::from_secs(10),
        max_pending: usize::MAX,
        max_pending_bytes: usize::MAX,
        max_total_pending_bytes: usize::MAX,
        commit_timeout: Duration::from_secs(30),
        retry_interval: Duration::from_secs(1),
        receive_maximum: match NonZeroU16::new(64) {
            Some(n) => n,
            None => panic!("receive maximum"),
        },
        topic_alias_maximum: 0,
        max_waiting: 1024,
        max_waiting_bytes: 1 << 20,
        max_total_waiting_bytes: 1 << 24,
        max_retries: 1024,
        authenticate: false,
        connect_through_log: false,
    }
}

fn bytes(packet: &Packet, version: Version) -> Vec<u8> {
    let mut out = BytesMut::new();
    if codec::encode(packet, version, Sender::Client, &mut out).is_err() {
        panic!("benchmark packet does not encode");
    }
    out.to_vec()
}

/// A ready gateway with connection 1 open.
fn connected(version: Version) -> Gateway {
    let mut g = Gateway::new(config(), T0);
    g.effect(
        &Effect::Reset {
            node: 1,
            below: 1,
            lsn: 1,
        },
        T0,
    );
    let _ = g.accept(1, T0);
    let connect = Packet::Connect(Box::new(Connect {
        version,
        client_id: match ClientId::new(Bytes::from_static(b"bench")) {
            Ok(id) => id,
            Err(_) => panic!("client id"),
        },
        clean_start: true,
        keep_alive_secs: 0,
        will: None,
        username: None,
        password: None,
        properties: ConnectProperties::default(),
    }));
    g.received(1, &bytes(&connect, version), T0);
    g.take_actions();
    g
}

fn topic() -> Topic {
    match Topic::new(Bytes::from_static(TOPIC)) {
        Ok(t) => t,
        Err(_) => panic!("topic"),
    }
}

#[divan::bench(args = [64, 1024])]
fn gateway_publish_in(bencher: divan::Bencher, payload: usize) {
    let publish = Packet::Publish(Publish {
        topic: Some(topic()),
        id: None,
        qos: Qos::AtMostOnce,
        duplicate: false,
        retain: false,
        payload: Bytes::from(vec![b'x'; payload]),
        properties: None,
    });
    let packet = bytes(&publish, Version::V311);
    let mut gateway = connected(Version::V311);
    bencher.bench_local(|| {
        gateway.received(1, divan::black_box(&packet), T0);
        gateway.take_actions()
    });
}

#[divan::bench(args = [64, 1024])]
fn gateway_deliver_out(bencher: divan::Bencher, payload: usize) {
    let effect = Effect::Deliver {
        to: Origin {
            node: 1,
            session: 1,
        },
        id: None,
        topic: topic(),
        payload: Bytes::from(vec![b'x'; payload]),
        properties: Bytes::new(),
        guarantee: DeliveryGuarantee::AtMostOnce,
        retain: false,
        lsn: 2,
    };
    let mut gateway = connected(Version::V311);
    bencher.bench_local(|| {
        gateway.effect(divan::black_box(&effect), T0);
        gateway.take_actions()
    });
}
