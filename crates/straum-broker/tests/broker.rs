//! Create, produce, fetch and admin requests against an in-process broker.

#![allow(clippy::unwrap_used)]

mod common;

use common::{TempDir, client, config};
use straum_broker::Broker;
use straum_client::proto::{ErrorCode, Header, MAX_REQUEST_TOPICS, Message};
use straum_client::{ClientError, ProduceOptions};

#[test]
fn produce_then_fetch_keeps_order_keys_and_headers() {
    let dir = TempDir::new("basic");
    let b = Broker::start(config(&dir)).unwrap();
    let mut c = client(&b);
    c.create_topic("orders", 1, 0).unwrap();
    let msgs = vec![
        Message::body(&b"one"[..]),
        Message {
            key: Some(b"k".to_vec()),
            headers: vec![Header {
                name: b"h".to_vec(),
                value: b"v".to_vec(),
            }],
            body: b"two".to_vec(),
        },
    ];
    let ok = c
        .produce("orders", 0, msgs.clone(), ProduceOptions::default())
        .unwrap();
    assert_eq!((ok.base_seq, ok.count, ok.leader_epoch), (0, 2, 1));
    let f = c.fetch("orders", 0, 0, 1 << 20, 0).unwrap();
    assert_eq!(f.next_seq, 2);
    let got: Vec<_> = f
        .messages
        .iter()
        .map(|m| (m.seq, m.message.clone()))
        .collect();
    assert_eq!(got, vec![(0, msgs[0].clone()), (1, msgs[1].clone())]);
    assert!(f.messages.iter().all(|m| m.timestamp_ms == ok.timestamp_ms));
    b.stop().unwrap();
}

/// Producers on many connections at once: every request gets its own
/// contiguous range of seqs, and the ranges tile the partition.
#[test]
fn concurrent_producers_get_distinct_contiguous_seqs() {
    let dir = TempDir::new("concurrent");
    let b = Broker::start(config(&dir)).unwrap();
    client(&b).create_topic("c", 1, 0).unwrap();
    let threads: Vec<_> = (0..8u8)
        .map(|t| {
            let addr = b.addr.to_string();
            std::thread::spawn(move || {
                let mut c =
                    straum_client::Client::connect(straum_client::ClientConfig::new(addr)).unwrap();
                (0..40u8)
                    .map(|i| {
                        let msgs: Vec<Message> =
                            (0..3u8).map(|j| Message::body(vec![t, i, j])).collect();
                        let ok = c.produce("c", 0, msgs, ProduceOptions::default()).unwrap();
                        (ok.base_seq, ok.count, t, i)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let mut acks: Vec<_> = threads
        .into_iter()
        .flat_map(|h| h.join().unwrap())
        .collect();
    acks.sort();
    let mut next = 0;
    for &(base, count, _, _) in &acks {
        assert_eq!(
            (base, count),
            (next, 3),
            "ranges are contiguous and disjoint"
        );
        next += 3;
    }
    let mut c = client(&b);
    let mut seq = 0;
    while seq < next {
        let f = c.fetch("c", 0, seq, 1 << 20, 0).unwrap();
        for m in &f.messages {
            let (_, _, t, i) = acks[(m.seq / 3) as usize];
            assert_eq!(
                m.message.body,
                vec![t, i, (m.seq % 3) as u8],
                "seq {}",
                m.seq
            );
        }
        seq = f.messages.last().unwrap().seq + 1;
    }
    b.stop().unwrap();
}

#[test]
fn fetch_waits_for_a_commit_and_times_out_without_one() {
    let dir = TempDir::new("wait");
    let b = Broker::start(config(&dir)).unwrap();
    client(&b).create_topic("w", 1, 0).unwrap();
    let addr = b.addr.to_string();
    let waiter = std::thread::spawn(move || {
        let mut c = straum_client::Client::connect(straum_client::ClientConfig::new(addr)).unwrap();
        let t = std::time::Instant::now();
        let f = c.fetch("w", 0, 0, 1 << 20, 5000).unwrap();
        (f, t.elapsed())
    });
    std::thread::sleep(std::time::Duration::from_millis(300));
    client(&b)
        .produce(
            "w",
            0,
            vec![Message::body(&b"x"[..])],
            ProduceOptions::default(),
        )
        .unwrap();
    let (f, waited) = waiter.join().unwrap();
    assert_eq!(f.messages.len(), 1);
    assert!(
        waited >= std::time::Duration::from_millis(250),
        "{waited:?}"
    );
    assert!(
        waited < std::time::Duration::from_secs(4),
        "woken by the commit: {waited:?}"
    );
    let t = std::time::Instant::now();
    let f = client(&b).fetch("w", 0, 1, 1 << 20, 300).unwrap();
    assert!(f.messages.is_empty());
    assert!(t.elapsed() >= std::time::Duration::from_millis(250));
    b.stop().unwrap();
}

#[test]
fn admin_requests_agree_with_what_was_produced() {
    let dir = TempDir::new("admin");
    let b = Broker::start(config(&dir)).unwrap();
    let mut c = client(&b);
    c.create_topic("zeta", 1, 0).unwrap();
    c.create_topic("alpha", 1, 3_600_000).unwrap();
    for n in 1..=4 {
        let msgs = (0..n).map(|i| Message::body(vec![n, i])).collect();
        c.produce("alpha", 0, msgs, ProduceOptions::default())
            .unwrap();
    }
    let (page, next) = c.list_topics("", 1000).unwrap();
    let names: Vec<_> = page.into_iter().map(|t| (t.name, t.partitions)).collect();
    assert_eq!(names, [("alpha".to_owned(), 1), ("zeta".to_owned(), 1)]);
    assert_eq!(next, "");
    let (page, next) = c.list_topics("", 1).unwrap();
    assert_eq!((page[0].name.as_str(), next.as_str()), ("alpha", "alpha"));
    let (page, next) = c.list_topics(&next, u32::MAX).unwrap();
    assert_eq!((page[0].name.as_str(), next.as_str()), ("zeta", ""));
    assert!(c.list_topics("zeta", 1).unwrap().0.is_empty());
    let malformed = |r: Result<_, ClientError>| matches!(r, Err(ClientError::Broker(e)) if e.code == ErrorCode::MalformedRequest as u16);
    assert!(malformed(c.list_topics("", 0).map(|_| ())));
    let many: Vec<String> = (0..=MAX_REQUEST_TOPICS).map(|i| format!("t{i}")).collect();
    let many: Vec<&str> = many.iter().map(String::as_str).collect();
    assert!(malformed(c.metadata(&many).map(|_| ())));
    let m = c.metadata(&many[1..]).unwrap();
    assert_eq!(m.topics.len(), 1000);
    assert!(c.metadata(&[]).unwrap().topics.is_empty());
    let d = c.describe_topic("alpha").unwrap();
    assert_eq!((d.retention_ms, d.replication_factor), (3_600_000, 1));
    let p = &d.partitions[0];
    assert_eq!((p.start_seq, p.next_seq, p.message_count), (0, 10, 10));
    assert!(p.index_bytes > 0);
    let m = c.metadata(&["alpha"]).unwrap();
    assert_eq!(m.topics[0].partitions[0].next_seq, 10);
    let s = c.broker_stats().unwrap();
    assert_eq!(
        (s.produce_requests, s.messages_accepted, s.pending_bytes),
        (4, 10 + 2, 0)
    );
    assert!(s.groups_committed >= 1 && s.groups_committed <= 4);
    assert!(
        s.fsyncs >= 2 * s.groups_committed,
        "segment and WAL per group"
    );
    assert_eq!(s.open_connections, 1);
    b.stop().unwrap();
}

#[test]
fn a_restart_keeps_topics_messages_and_timestamps() {
    let dir = TempDir::new("restart");
    let b = Broker::start(config(&dir)).unwrap();
    let mut c = client(&b);
    c.create_topic("r", 1, 0).unwrap();
    let msg = Message {
        key: Some(b"k".to_vec()),
        headers: vec![Header {
            name: b"a".to_vec(),
            value: b"b".to_vec(),
        }],
        body: b"kept".to_vec(),
    };
    let ok = c
        .produce("r", 0, vec![msg.clone()], ProduceOptions::default())
        .unwrap();
    drop(c);
    b.stop().unwrap();
    let b = Broker::start(config(&dir)).unwrap();
    let mut c = client(&b);
    let f = c.fetch("r", 0, 0, 1 << 20, 0).unwrap();
    assert_eq!(f.messages.len(), 1);
    assert_eq!(f.messages[0].message, msg);
    assert_eq!(f.messages[0].timestamp_ms, ok.timestamp_ms);
    let again = c
        .produce(
            "r",
            0,
            vec![Message::body(&b"after"[..])],
            ProduceOptions::default(),
        )
        .unwrap();
    assert_eq!(again.base_seq, 1);
    assert!(
        again.timestamp_ms >= ok.timestamp_ms,
        "timestamps never go back"
    );
    assert!(
        c.create_topic("r", 1, 0).is_err(),
        "the topic survived the restart"
    );
    b.stop().unwrap();
}

#[test]
fn another_identity_on_the_same_data_directory_refuses_to_start() {
    let dir = TempDir::new("identity");
    Broker::start(config(&dir)).unwrap().stop().unwrap();
    let mut other = config(&dir);
    other.node_id = 2;
    let err = Broker::start(other).err().unwrap();
    assert!(err.contains("belongs to cluster"), "{err}");
    let mut other = config(&dir);
    other.cluster_id = "another".into();
    assert!(Broker::start(other).is_err());
    Broker::start(config(&dir)).unwrap().stop().unwrap();
}
