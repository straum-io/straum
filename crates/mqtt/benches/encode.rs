//! Codec level of docs/benchmarks.md §1: one packet into a reused buffer.

use bytes::{Bytes, BytesMut};
use domain::Topic;
use mqtt::{MqttLimits, PacketId, puback, publish};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

fn topic() -> Topic {
    match Topic::new(Bytes::from_static(
        b"sensors/eu-west/rack12/node7/temperature",
    )) {
        Ok(topic) => topic,
        Err(error) => panic!("invalid benchmark topic: {error}"),
    }
}

fn packet_id() -> PacketId {
    match PacketId::new(1) {
        Some(id) => id,
        None => unreachable!(),
    }
}

/// The writer encodes into a buffer it reuses, so the measured operation is
/// an append into warm capacity, not an allocation.
#[divan::bench(args = [64, 1024])]
fn mqtt_publish_encode(bencher: divan::Bencher, payload: usize) {
    let topic = topic();
    let limits = MqttLimits::default();
    let payload = vec![b'x'; payload];

    bencher
        .with_inputs(|| BytesMut::with_capacity(8192))
        .bench_values(|mut buf| {
            publish(
                &mut buf,
                divan::black_box(&topic),
                Some(packet_id()),
                divan::black_box(&payload),
                &limits,
            )
        });
}

#[divan::bench]
fn mqtt_ack_encode(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| BytesMut::with_capacity(64))
        .bench_values(|mut buf| {
            puback(&mut buf, divan::black_box(packet_id()));
            buf
        });
}
