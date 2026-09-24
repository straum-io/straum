//! In-memory end to end: the whole per-message CPU cost with no sockets and
//! no channels (docs/benchmarks.md §1, "in-memory e2e").
//!
//! Bytes in → decode → protocol mapping → broker routing → encode → bytes out.
//! This is the number to put next to another broker's, because it is the work
//! itself, without the scheduler in the way.

use std::sync::Mutex;
use std::time::Instant;

use auth::Permissions;
use broker::{Broker, BrokerLimits};
use bytes::{Bytes, BytesMut};
use domain::{ClientId, DeliveryGuarantee, SessionId, TopicFilter};
use mqtt::{Connection, MqttLimits, PacketIds, Reaction, decode, encode_outbound};
use protocol::{Command, Connect, Outbox, RequestId};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const TOPIC: &str = "sensors/eu-west/rack12/node7/temperature";

fn client(index: usize) -> ClientId {
    match ClientId::new(Bytes::from(format!("client-{index}"))) {
        Ok(client) => client,
        Err(error) => panic!("invalid benchmark client id: {error}"),
    }
}

fn filter(value: &str) -> TopicFilter {
    match TopicFilter::new(Bytes::copy_from_slice(value.as_bytes())) {
        Ok(filter) => filter,
        Err(error) => panic!("invalid benchmark filter: {error}"),
    }
}

/// One PUBLISH on the wire, as a client would send it.
fn publish_packet(payload: usize) -> Vec<u8> {
    let mut body = (TOPIC.len() as u16).to_be_bytes().to_vec();
    body.extend_from_slice(TOPIC.as_bytes());
    body.extend(std::iter::repeat_n(b'x', payload));

    let mut packet = vec![0x30];
    let mut remaining = body.len();
    loop {
        let mut byte = (remaining % 128) as u8;
        remaining /= 128;
        if remaining > 0 {
            byte |= 0x80;
        }
        packet.push(byte);
        if remaining == 0 {
            break;
        }
    }
    packet.extend_from_slice(&body);
    packet
}

/// A broker with `subscribers` sessions on the published topic.
fn broker(subscribers: usize, guarantee: DeliveryGuarantee) -> (Broker, Outbox) {
    let mut broker = Broker::new(BrokerLimits::default());
    let mut out = Outbox::new();

    for index in 0..subscribers {
        let session = SessionId(index as u64);
        broker.handle(
            session,
            Command::Connect(Connect {
                client_id: client(index),
                permissions: Permissions::unrestricted(),
            }),
            &mut out,
        );
        broker.handle(
            session,
            Command::Subscribe {
                request: RequestId(1),
                filters: vec![(filter(TOPIC), guarantee)],
            },
            &mut out,
        );
    }
    let _ = out.take();
    (broker, out)
}

struct Pipeline {
    broker: Broker,
    out: Outbox,
    connection: Connection,
    ids: Mutex<PacketIds>,
    limits: MqttLimits,
    write: BytesMut,
    publisher: SessionId,
}

impl Pipeline {
    fn new(subscribers: usize, guarantee: DeliveryGuarantee) -> Self {
        let limits = MqttLimits::default();
        let (mut broker, mut out) = broker(subscribers, guarantee);
        let publisher = SessionId(1_000_000);
        broker.handle(
            publisher,
            Command::Connect(Connect {
                client_id: client(usize::MAX),
                permissions: Permissions::unrestricted(),
            }),
            &mut out,
        );
        let _ = out.take();

        let mut connection = Connection::new(Instant::now(), &limits);
        let mut ids = PacketIds::with_capacity(BrokerLimits::default().max_inflight);
        // Past the handshake, so the publish path is what gets measured.
        let mut hello = BytesMut::from(&connect_packet()[..]);
        if let Ok(Some(packet)) = decode(&mut hello, &limits) {
            // The handshake is not the measured work; the publish path is.
            let _ = connection.on_packet(packet, Instant::now(), &mut ids);
        }

        Self {
            broker,
            out,
            connection,
            ids: Mutex::new(ids),
            limits,
            write: BytesMut::with_capacity(64 * 1024),
            publisher,
        }
    }

    /// One message, all the way through.
    fn run(&mut self, mut buf: BytesMut) -> usize {
        let packet = match decode(&mut buf, &self.limits) {
            Ok(Some(packet)) => packet,
            _ => panic!("the benchmark packet did not decode"),
        };

        let mut ids = match self.ids.lock() {
            Ok(ids) => ids,
            Err(poisoned) => poisoned.into_inner(),
        };
        let reaction = self.connection.on_packet(packet, Instant::now(), &mut ids);
        let command = match reaction {
            Ok(Reaction::Forward(command)) => command,
            _ => panic!("the benchmark packet did not map to a command"),
        };

        self.broker.handle(self.publisher, command, &mut self.out);

        self.write.clear();
        let mut delivered = 0usize;
        for (_, outbound) in self.out.take() {
            if encode_outbound(&outbound, &mut ids, &mut self.write, &self.limits).is_err() {
                panic!("encoding a delivery failed");
            }
            delivered += 1;
        }
        delivered
    }
}

fn connect_packet() -> Vec<u8> {
    let mut body = 4u16.to_be_bytes().to_vec();
    body.extend_from_slice(b"MQTT");
    body.push(4);
    body.push(0b0000_0010);
    body.extend_from_slice(&60u16.to_be_bytes());
    body.extend_from_slice(&6u16.to_be_bytes());
    body.extend_from_slice(b"pubbed");

    let mut packet = vec![0x10, body.len() as u8];
    packet.extend_from_slice(&body);
    packet
}

/// The quick tier of the workload matrix: 64 B and 1 KiB, 1 and 100
/// subscribers (docs/benchmarks.md §4).
#[divan::bench(args = [(1, 64), (1, 1024), (100, 64), (100, 1024)])]
fn publish_consume(bencher: divan::Bencher, workload: (usize, usize)) {
    let (subscribers, payload) = workload;
    let packet = publish_packet(payload);
    let mut pipeline = Pipeline::new(subscribers, DeliveryGuarantee::AtMostOnce);

    bencher
        .with_inputs(|| BytesMut::from(packet.as_slice()))
        .bench_local_values(|buf| divan::black_box(pipeline.run(buf)));
}

/// The same path with at-least-once delivery: identifiers are assigned and
/// the inflight window is touched.
#[divan::bench(args = [1, 100])]
fn publish_consume_at_least_once(bencher: divan::Bencher, subscribers: usize) {
    let packet = publish_packet(64);
    let mut pipeline = Pipeline::new(subscribers, DeliveryGuarantee::AtLeastOnce);

    bencher
        .with_inputs(|| BytesMut::from(packet.as_slice()))
        .bench_local_values(|buf| divan::black_box(pipeline.run(buf)));
}
