//! Codec level of docs/benchmarks.md §1 for the new codec (ADR-0008 step
//! 4 baseline): the same 3.1.1 packets as `mqtt/benches/decode.rs`, so the
//! two compare directly, and their MQTT 5 counterparts.

use bytes::{Bytes, BytesMut};
use codec::{
    Connect, ConnectProperties, Limits, Packet, PacketId, Publish, PublishProperties, Qos, Sender,
    Subscribe, SubscriptionOptions, UserProperty, Version, decode, encode,
};
use domain::{ClientId, Topic, TopicFilter};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const TOPIC: &[u8] = b"sensors/eu-west/rack12/node7/temperature";

fn bytes(packet: &Packet, version: Version) -> Vec<u8> {
    let mut out = BytesMut::new();
    if encode(packet, version, Sender::Client, &mut out).is_err() {
        panic!("benchmark packet does not encode");
    }
    out.to_vec()
}

fn publish(payload: usize, properties: bool) -> Packet {
    Packet::Publish(Publish {
        topic: Topic::new(Bytes::from_static(TOPIC)).ok(),
        id: None,
        qos: Qos::AtMostOnce,
        duplicate: false,
        retain: false,
        payload: Bytes::from(vec![b'x'; payload]),
        properties: properties.then(|| {
            Box::new(PublishProperties {
                user: vec![UserProperty {
                    name: Bytes::from_static(b"trace"),
                    value: Bytes::from_static(b"0af7651916cd43dd8448eb211c80319c"),
                }],
                ..Default::default()
            })
        }),
    })
}

fn bench_decode(bencher: divan::Bencher, packet: Vec<u8>, version: Option<Version>) {
    let limits = Limits::default();
    bencher
        .with_inputs(|| BytesMut::from(packet.as_slice()))
        .bench_values(|mut buf| {
            decode(&mut buf, version, Sender::Client, divan::black_box(&limits))
        });
}

#[divan::bench]
fn codec_connect_decode(bencher: divan::Bencher) {
    let connect = Packet::Connect(Box::new(Connect {
        version: Version::V311,
        client_id: match ClientId::new(Bytes::from_static(b"benchmark-client")) {
            Ok(id) => id,
            Err(_) => panic!("client id"),
        },
        clean_start: true,
        keep_alive_secs: 60,
        will: None,
        username: None,
        password: None,
        properties: ConnectProperties::default(),
    }));
    bench_decode(bencher, bytes(&connect, Version::V311), None);
}

/// 64 B is below the payload copy threshold, 1 KiB above it.
#[divan::bench(args = [64, 1024])]
fn codec_publish_decode_v311(bencher: divan::Bencher, payload: usize) {
    bench_decode(
        bencher,
        bytes(&publish(payload, false), Version::V311),
        Some(Version::V311),
    );
}

/// The same publish in MQTT 5 with one user property.
#[divan::bench(args = [64, 1024])]
fn codec_publish_decode_v5(bencher: divan::Bencher, payload: usize) {
    bench_decode(
        bencher,
        bytes(&publish(payload, true), Version::V5),
        Some(Version::V5),
    );
}

#[divan::bench(args = [1, 8])]
fn codec_subscribe_decode(bencher: divan::Bencher, filters: usize) {
    let subscribe = Packet::Subscribe(Subscribe {
        id: match PacketId::new(1) {
            Some(id) => id,
            None => panic!("packet id"),
        },
        subscription_identifier: None,
        user: Vec::new(),
        filters: (0..filters)
            .map(|index| {
                let filter = format!("sensors/rack{index}/+/temperature");
                match TopicFilter::new(Bytes::from(filter)) {
                    Ok(f) => (f, SubscriptionOptions::qos(Qos::AtLeastOnce)),
                    Err(_) => panic!("filter"),
                }
            })
            .collect(),
    });
    bench_decode(
        bencher,
        bytes(&subscribe, Version::V311),
        Some(Version::V311),
    );
}

/// What a delivery costs to encode: server to client, into a reused buffer.
#[divan::bench(args = [64, 1024])]
fn codec_publish_encode_v311(bencher: divan::Bencher, payload: usize) {
    let packet = publish(payload, false);
    let mut out = BytesMut::with_capacity(2048);
    bencher.bench_local(|| {
        out.clear();
        encode(
            divan::black_box(&packet),
            Version::V311,
            Sender::Server,
            &mut out,
        )
    });
}

#[divan::bench(args = [64, 1024])]
fn codec_publish_encode_v5(bencher: divan::Bencher, payload: usize) {
    let packet = publish(payload, true);
    let mut out = BytesMut::with_capacity(2048);
    bencher.bench_local(|| {
        out.clear();
        encode(
            divan::black_box(&packet),
            Version::V5,
            Sender::Server,
            &mut out,
        )
    });
}
