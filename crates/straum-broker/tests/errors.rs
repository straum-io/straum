//! Every error code a version 0 broker sends, each produced by a request.
//! NotLeader is never sent by version 0 (docs/protocol.md section 5).

#![allow(clippy::unwrap_used)]

mod common;

use std::collections::HashSet;

use common::{TempDir, client, config};
use straum_broker::Broker;
use straum_client::proto::{
    self, CreateTopic, ErrorCode, Fetch, Header, Hello, Message, Produce, Request, Response, ack,
};
use straum_client::{Connection, ProduceOptions};

fn conn(b: &Broker, hello: bool) -> Connection {
    let mut c =
        Connection::connect(&b.addr.to_string(), std::time::Duration::from_secs(5)).unwrap();
    c.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    if hello {
        assert!(matches!(
            c.call(&Request::Hello(hello_req())).unwrap(),
            Response::HelloOk(_)
        ));
    }
    c
}

fn hello_req() -> Hello {
    Hello {
        min_version: 0,
        max_version: 0,
        client_name: "errors".into(),
        auth_mechanism: "none".into(),
        auth_payload: vec![],
    }
}

fn produce(topic: &str, messages: Vec<Message>) -> Produce {
    Produce {
        topic: topic.into(),
        partition: 0,
        producer_id: 1,
        producer_seq: 1,
        ack_level: ack::DURABLE,
        expected_leader_epoch: 0,
        timeout_ms: 0,
        messages,
    }
}

fn fetch(topic: &str, wait: u32) -> Fetch {
    Fetch {
        topic: topic.into(),
        partition: 0,
        from_seq: 0,
        max_bytes: 1 << 20,
        max_wait_ms: wait,
        expected_leader_epoch: 0,
    }
}

fn code(r: Response) -> ErrorCode {
    match r {
        Response::Error(e) => e.code().unwrap(),
        r => panic!("expected an error, got {r:?}"),
    }
}

#[test]
fn every_v0_error_code_is_sent() {
    let dir = TempDir::new("errors");
    let mut cfg = config(&dir);
    cfg.max_fetch_waiters = 1;
    cfg.max_frame_bytes = 1 << 20;
    cfg.max_message_bytes = 64 << 10;
    let b = Broker::start(cfg).unwrap();
    let mut seen = HashSet::new();
    let mut check = |got: ErrorCode, want: ErrorCode| {
        assert_eq!(got, want);
        seen.insert(want);
    };

    let mut c = conn(&b, false);
    check(
        code(c.call(&Request::BrokerStats).unwrap()),
        ErrorCode::Unauthenticated,
    );
    check(
        code(
            c.call(&Request::Hello(Hello {
                min_version: 3,
                max_version: 4,
                ..hello_req()
            }))
            .unwrap(),
        ),
        ErrorCode::UnsupportedVersion,
    );
    check(
        code(
            c.call(&Request::Hello(Hello {
                auth_mechanism: "plain".into(),
                ..hello_req()
            }))
            .unwrap(),
        ),
        ErrorCode::Unauthenticated,
    );
    assert!(matches!(
        c.call(&Request::Hello(hello_req())).unwrap(),
        Response::HelloOk(_)
    ));
    check(
        code(c.call(&Request::Hello(hello_req())).unwrap()),
        ErrorCode::Unsupported,
    );
    let create = |name: &str, partitions| {
        Request::CreateTopic(CreateTopic {
            name: name.into(),
            partitions,
            replication_factor: 1,
            retention_ms: 0,
        })
    };
    assert!(matches!(
        c.call(&create("e", 1)).unwrap(),
        Response::CreateTopicOk
    ));
    check(
        code(c.call(&create("e", 1)).unwrap()),
        ErrorCode::TopicExists,
    );
    check(
        code(c.call(&create("p2", 2)).unwrap()),
        ErrorCode::Unsupported,
    );
    check(
        code(c.call(&create("__x", 1)).unwrap()),
        ErrorCode::Forbidden,
    );
    check(
        code(c.call(&create("a b", 1)).unwrap()),
        ErrorCode::MalformedRequest,
    );
    check(
        code(c.call(&Request::DeleteTopic { name: "e".into() }).unwrap()),
        ErrorCode::Unsupported,
    );
    check(
        code(
            c.call(&Request::Produce(produce(
                "missing",
                vec![Message::body(&b"x"[..])],
            )))
            .unwrap(),
        ),
        ErrorCode::UnknownTopic,
    );
    check(
        code(
            c.call(&Request::Produce(Produce {
                partition: 1,
                ..produce("e", vec![Message::body(&b"x"[..])])
            }))
            .unwrap(),
        ),
        ErrorCode::UnknownPartition,
    );
    for level in [ack::WRITTEN, ack::REPLICATED] {
        check(
            code(
                c.call(&Request::Produce(Produce {
                    ack_level: level,
                    ..produce("e", vec![Message::body(&b"x"[..])])
                }))
                .unwrap(),
            ),
            ErrorCode::UnsupportedAckLevel,
        );
    }
    check(
        code(
            c.call(&Request::Produce(Produce {
                expected_leader_epoch: 2,
                ..produce("e", vec![Message::body(&b"x"[..])])
            }))
            .unwrap(),
        ),
        ErrorCode::FencedLeaderEpoch,
    );
    // Each message limit, one past it.
    let over = [
        Message::body(vec![0; (64 << 10) + 1]),
        Message {
            key: Some(vec![0; 1025]),
            ..Message::body(&b"x"[..])
        },
        Message {
            headers: vec![Header::default(); 65],
            ..Message::body(&b"x"[..])
        },
        Message {
            headers: vec![Header {
                name: vec![0; 257],
                value: vec![],
            }],
            ..Message::body(&b"x"[..])
        },
        Message {
            headers: vec![Header {
                name: vec![1],
                value: vec![0; 16 << 10],
            }],
            ..Message::body(&b"x"[..])
        },
    ];
    for m in over {
        check(
            code(c.call(&Request::Produce(produce("e", vec![m]))).unwrap()),
            ErrorCode::MessageTooLarge,
        );
    }
    // An unknown type and a body that does not decode keep the connection.
    let mut raw = Vec::new();
    raw.extend_from_slice(&10u32.to_le_bytes());
    raw.extend_from_slice(&0x0777u16.to_le_bytes());
    raw.extend_from_slice(&900u64.to_le_bytes());
    c.send_raw(&raw).unwrap();
    let (corr, r) = c.recv().unwrap();
    assert_eq!(corr, 900);
    check(code(r), ErrorCode::Unsupported);
    check(
        code(c.call(&Request::Produce(produce("e", vec![]))).unwrap()),
        ErrorCode::MalformedRequest,
    );

    // Overloaded: one fetch waits, the next would wait too.
    let mut w = conn(&b, true);
    w.send(&Request::Fetch(fetch("e", 2000))).unwrap();
    std::thread::sleep(std::time::Duration::from_millis(200));
    check(
        code(c.call(&Request::Fetch(fetch("e", 2000))).unwrap()),
        ErrorCode::Overloaded,
    );
    assert!(matches!(w.recv().unwrap().1, Response::FetchOk(_)));

    // FrameTooLarge closes the connection.
    let mut f = conn(&b, true);
    let mut big = Vec::new();
    big.extend_from_slice(&(2u32 << 20).to_le_bytes());
    big.extend_from_slice(&proto::types::PING.to_le_bytes());
    big.extend_from_slice(&5u64.to_le_bytes());
    f.send_raw(&big).unwrap();
    let (corr, r) = f.recv().unwrap();
    assert_eq!(corr, 5);
    check(code(r), ErrorCode::FrameTooLarge);
    assert!(f.recv().is_err(), "closed");

    b.stop().unwrap();

    // StorageError: damage the bytes of an earlier message at rest.
    // Recovery checks the last frame's objects only; the damaged one is found
    // when it is read.
    let b = Broker::start(config(&dir)).unwrap();
    let mut cl = client(&b);
    cl.produce(
        "e",
        0,
        vec![Message::body(&b"damaged-body-1234"[..])],
        ProduceOptions::default(),
    )
    .unwrap();
    cl.produce(
        "e",
        0,
        vec![Message::body(&b"intact"[..])],
        ProduceOptions::default(),
    )
    .unwrap();
    drop(cl);
    b.stop().unwrap();
    let store = dir.0.join("data/store");
    // A restart starts a new segment, so the body is in one of several.
    let mut damaged = 0;
    for entry in std::fs::read_dir(&store).unwrap() {
        let seg = entry.unwrap().path();
        if !seg
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("seg-")
        {
            continue;
        }
        let mut bytes = std::fs::read(&seg).unwrap();
        if let Some(at) = bytes.windows(17).position(|w| w == b"damaged-body-1234") {
            bytes[at] ^= 0xff;
            std::fs::write(&seg, &bytes).unwrap();
            damaged += 1;
        }
    }
    assert_eq!(damaged, 1);
    let b = Broker::start(config(&dir)).unwrap();
    let mut c = conn(&b, true);
    check(
        code(c.call(&Request::Fetch(fetch("e", 0))).unwrap()),
        ErrorCode::StorageError,
    );
    b.stop().unwrap();

    let all: HashSet<_> = ErrorCode::ALL
        .into_iter()
        .filter(|c| *c != ErrorCode::NotLeader)
        .collect();
    let missing: Vec<_> = all.difference(&seen).collect();
    assert!(missing.is_empty(), "codes not produced: {missing:?}");
}
