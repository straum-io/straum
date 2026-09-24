//! Codec level of docs/benchmarks.md §1: one packet in, one packet out.

use bytes::BytesMut;
use mqtt::{MqttLimits, decode};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

fn string(value: &[u8]) -> Vec<u8> {
    let length = match u16::try_from(value.len()) {
        Ok(length) => length,
        Err(_) => panic!("benchmark string longer than 65535 bytes"),
    };
    let mut out = length.to_be_bytes().to_vec();
    out.extend_from_slice(value);
    out
}

fn frame(kind: u8, flags: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![(kind << 4) | flags];
    let mut remaining = body.len();
    loop {
        let mut byte = match u8::try_from(remaining % 128) {
            Ok(byte) => byte,
            Err(_) => unreachable!(),
        };
        remaining /= 128;
        if remaining > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if remaining == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

fn bench(bencher: divan::Bencher, packet: Vec<u8>) {
    let limits = MqttLimits::default();
    bencher
        .with_inputs(|| BytesMut::from(packet.as_slice()))
        .bench_values(|mut buf| decode(&mut buf, divan::black_box(&limits)));
}

#[divan::bench]
fn mqtt_connect_decode(bencher: divan::Bencher) {
    let mut body = string(b"MQTT");
    body.push(4);
    body.push(0b0000_0010);
    body.extend_from_slice(&60u16.to_be_bytes());
    body.extend_from_slice(&string(b"benchmark-client"));
    bench(bencher, frame(1, 0, &body));
}

/// 64 B is below the payload copy threshold, 1 KiB above it: the two take
/// different paths through `payload`.
#[divan::bench(args = [64, 1024])]
fn mqtt_publish_decode(bencher: divan::Bencher, payload: usize) {
    let mut body = string(b"sensors/eu-west/rack12/node7/temperature");
    body.extend_from_slice(&vec![b'x'; payload]);
    bench(bencher, frame(3, 0, &body));
}

#[divan::bench(args = [1, 8])]
fn mqtt_subscribe_decode(bencher: divan::Bencher, filters: usize) {
    let mut body = 1u16.to_be_bytes().to_vec();
    for index in 0..filters {
        body.extend_from_slice(&string(
            format!("sensors/rack{index}/+/temperature").as_bytes(),
        ));
        body.push(1);
    }
    bench(bencher, frame(8, 0b0010, &body));
}
