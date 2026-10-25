//! A client that floods Fetch and Produce and never reads a response: once
//! its unread responses fill the socket, the broker stops reading from it.
//! Asserted on the broker's metrics, not on memory: pending bytes stay
//! within `max_pending_bytes`, not every request is read, and another
//! connection is still served.

#![allow(clippy::unwrap_used)]

mod common;

use std::time::Duration;

use common::{TempDir, client, config};
use straum_broker::Broker;
use straum_client::proto::{Fetch, Message, Produce, Request, ack};
use straum_client::{Connection, ProduceOptions};

#[test]
fn a_reader_that_never_reads_is_bounded() {
    let dir = TempDir::new("backpressure");
    let mut cfg = config(&dir);
    cfg.max_frame_bytes = 8192;
    cfg.max_message_bytes = 4096;
    cfg.max_fetch_bytes = 4096;
    cfg.max_inflight_bytes = 16384;
    cfg.max_pending_bytes = 8192;
    let b = Broker::start(cfg).unwrap();
    let mut c = client(&b);
    c.create_topic("bp", 1, 0).unwrap();
    c.produce(
        "bp",
        0,
        vec![Message::body(vec![1; 4000])],
        ProduceOptions::default(),
    )
    .unwrap();
    let flood = 10_000u64;
    let addr = b.addr.to_string();
    let flooder = std::thread::spawn(move || {
        let mut f = Connection::connect(&addr, Duration::from_secs(5)).unwrap();
        let _ = f.call(&Request::Hello(straum_client::proto::Hello {
            min_version: 0,
            max_version: 0,
            client_name: "flood".into(),
            auth_mechanism: "none".into(),
            auth_payload: vec![],
        }));
        for i in 0..flood {
            let fetch = Request::Fetch(Fetch {
                topic: "bp".into(),
                partition: 0,
                from_seq: 0,
                max_bytes: 4096,
                max_wait_ms: 0,
                expected_leader_epoch: 0,
            });
            let produce = Request::Produce(Produce {
                topic: "bp".into(),
                partition: 0,
                producer_id: 9,
                producer_seq: i,
                ack_level: ack::DURABLE,
                expected_leader_epoch: 0,
                timeout_ms: 0,
                messages: vec![Message::body(vec![2; 3000])],
            });
            // Blocks once the broker stops reading; the connection is shut
            // down at the end of the test, which ends this thread.
            if f.send(&fetch).is_err() || f.send(&produce).is_err() {
                return;
            }
        }
    });
    let mut last = 0;
    let mut stalled = 0;
    for _ in 0..40 {
        std::thread::sleep(Duration::from_millis(100));
        let s = c.broker_stats().unwrap();
        assert!(s.pending_bytes <= s.max_pending_bytes, "{s:?}");
        assert!(s.pending_bytes_peak <= s.max_pending_bytes, "{s:?}");
        if s.produce_requests == last {
            stalled += 1;
            if stalled >= 5 {
                break;
            }
        } else {
            stalled = 0;
        }
        last = s.produce_requests;
    }
    assert!(
        stalled >= 5,
        "the broker kept reading: {last} produce requests"
    );
    assert!(last < flood + 1, "not every request was read: {last}");
    assert_eq!(
        c.ping(42).unwrap(),
        42,
        "another connection is still served"
    );
    b.stop().unwrap();
    drop(flooder);
}

/// Produce frames whose bodies arrive after their headers, on several
/// connections at once: the global budget is reserved when a header is
/// read, so the readers waiting for bodies cannot together pass
/// `max_pending_bytes`.
#[test]
fn slow_bodies_on_many_connections_stay_within_the_pending_bound() {
    let dir = TempDir::new("pending-bound");
    let mut cfg = config(&dir);
    cfg.max_frame_bytes = 8192;
    cfg.max_message_bytes = 8192;
    cfg.max_fetch_bytes = 1024;
    cfg.max_pending_bytes = 8192;
    let b = Broker::start(cfg).unwrap();
    let mut c = client(&b);
    c.create_topic("pb", 1, 0).unwrap();
    let frame = |seq: u64| {
        let mut f = Vec::new();
        straum_client::proto::encode_request(
            &mut f,
            seq,
            &Request::Produce(Produce {
                topic: "pb".into(),
                partition: 0,
                producer_id: 1,
                producer_seq: seq,
                ack_level: ack::DURABLE,
                expected_leader_epoch: 0,
                timeout_ms: 0,
                messages: vec![Message::body(vec![7; 6000])],
            }),
        );
        f
    };
    let mut conns: Vec<Connection> = (0..3)
        .map(|_| {
            let mut k = Connection::connect(&b.addr.to_string(), Duration::from_secs(5)).unwrap();
            k.set_read_timeout(Some(Duration::from_secs(10))).unwrap();
            let r = k
                .call(&Request::Hello(straum_client::proto::Hello {
                    min_version: 0,
                    max_version: 0,
                    client_name: "slow".into(),
                    auth_mechanism: "none".into(),
                    auth_payload: vec![],
                }))
                .unwrap();
            assert!(matches!(r, straum_client::proto::Response::HelloOk(_)));
            k
        })
        .collect();
    let frames: Vec<Vec<u8>> = (0..3).map(frame).collect();
    for (k, f) in conns.iter_mut().zip(&frames) {
        k.send_raw(&f[..straum_client::proto::FRAME_HEADER])
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(200));
    let s = c.broker_stats().unwrap();
    assert!(
        s.pending_bytes <= s.max_pending_bytes,
        "headers alone: {s:?}"
    );
    for (k, f) in conns.iter_mut().zip(&frames) {
        k.send_raw(&f[straum_client::proto::FRAME_HEADER..])
            .unwrap();
    }
    for k in &mut conns {
        let (_, r) = k.recv().unwrap();
        assert!(
            matches!(r, straum_client::proto::Response::ProduceOk(_)),
            "{r:?}"
        );
    }
    let s = c.broker_stats().unwrap();
    assert!(s.pending_bytes_peak <= s.max_pending_bytes, "{s:?}");
    assert_eq!(s.pending_bytes, 0, "every reservation released");
    b.stop().unwrap();
}
