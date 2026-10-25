//! Sample frames for every request and response type, with boundary
//! values: the round-trip tests and the golden frames in
//! `conformance/frames/` are built from this list.

#![allow(dead_code)] // each test binary uses a different subset

use straum_proto::*;

pub enum Frame {
    Request(Request),
    Response(Response),
}

pub struct Sample {
    pub name: &'static str,
    pub correlation_id: u64,
    pub frame: Frame,
    pub note: &'static str,
}

fn req(name: &'static str, correlation_id: u64, r: Request, note: &'static str) -> Sample {
    Sample {
        name,
        correlation_id,
        frame: Frame::Request(r),
        note,
    }
}

fn resp(name: &'static str, correlation_id: u64, r: Response, note: &'static str) -> Sample {
    Sample {
        name,
        correlation_id,
        frame: Frame::Response(r),
        note,
    }
}

fn hello() -> Hello {
    Hello {
        min_version: 0,
        max_version: 0,
        client_name: "straum-client".into(),
        auth_mechanism: "none".into(),
        auth_payload: vec![],
    }
}

fn produce(messages: Vec<Message>) -> Produce {
    Produce {
        topic: "orders".into(),
        partition: 0,
        producer_id: 7,
        producer_seq: 1,
        ack_level: ack::DURABLE,
        expected_leader_epoch: 0,
        timeout_ms: 0,
        messages,
    }
}

fn header(name: &[u8], value: &[u8]) -> Header {
    Header {
        name: name.to_vec(),
        value: value.to_vec(),
    }
}

fn keyed(key: &[u8], body: &[u8]) -> Message {
    Message {
        key: Some(key.to_vec()),
        headers: vec![],
        body: body.to_vec(),
    }
}

/// Headers totalling `total` bytes over `count` headers with 1-byte names.
fn headers_totalling(count: usize, total: usize) -> Vec<Header> {
    let per = total / count - 1;
    let mut v: Vec<Header> = (0..count)
        .map(|i| header(&[b'a' + (i % 26) as u8], &vec![b'v'; per]))
        .collect();
    let used: usize = v.iter().map(|h| h.name.len() + h.value.len()).sum();
    v[0].value.extend(std::iter::repeat_n(b'v', total - used));
    v
}

pub fn samples() -> Vec<Sample> {
    let max_name = "t".repeat(80);
    let mut out = vec![
        req("hello", 1, Request::Hello(hello()), "the v0 handshake"),
        req(
            "hello_range",
            2,
            Request::Hello(Hello {
                min_version: 0,
                max_version: u16::MAX,
                client_name: String::new(),
                ..hello()
            }),
            "a client offering versions 0 to 65535, empty client name",
        ),
        req(
            "hello_auth",
            3,
            Request::Hello(Hello {
                auth_mechanism: "plain".into(),
                auth_payload: b"user\0pass".to_vec(),
                ..hello()
            }),
            "a mechanism v0 refuses with Unauthenticated",
        ),
        req(
            "ping",
            4,
            Request::Ping {
                nonce: 0x0102_0304_0506_0708,
            },
            "",
        ),
        req(
            "ping_max",
            u64::MAX,
            Request::Ping { nonce: u64::MAX },
            "largest correlation id and nonce",
        ),
        req(
            "metadata_all",
            5,
            Request::Metadata { topics: vec![] },
            "empty list: every topic",
        ),
        req(
            "metadata_topics",
            6,
            Request::Metadata {
                topics: vec!["orders".into(), max_name.clone()],
            },
            "a topic name of the maximum 80 bytes",
        ),
        req(
            "create_topic",
            7,
            Request::CreateTopic(CreateTopic {
                name: "orders".into(),
                partitions: 1,
                replication_factor: 1,
                retention_ms: 0,
            }),
            "",
        ),
        req(
            "create_topic_max",
            8,
            Request::CreateTopic(CreateTopic {
                name: max_name.clone(),
                partitions: u32::MAX,
                replication_factor: u16::MAX,
                retention_ms: u64::MAX,
            }),
            "largest values; v0 answers Unsupported (partitions > 1)",
        ),
        req(
            "delete_topic",
            9,
            Request::DeleteTopic {
                name: "orders".into(),
            },
            "v0 answers Unsupported",
        ),
        req(
            "produce_body",
            10,
            Request::Produce(produce(vec![Message::body(&b"hello"[..])])),
            "no key, no headers: flags 0",
        ),
        req(
            "produce_keyed",
            11,
            Request::Produce(produce(vec![
                keyed(b"user-42", b"hello"),
                Message {
                    key: None,
                    headers: vec![header(b"trace", b"abc"), header(b"trace", b"")],
                    body: b"hello".to_vec(),
                },
            ])),
            "a key; repeated header names and an empty header value",
        ),
        req(
            "produce_empty_key",
            12,
            Request::Produce(produce(vec![keyed(b"", b""), Message::body(Vec::new())])),
            "a present empty key, and an empty body without key",
        ),
        req(
            "produce_full",
            13,
            Request::Produce(Produce {
                topic: max_name.clone(),
                partition: u32::MAX,
                producer_id: u64::MAX,
                producer_seq: u64::MAX,
                ack_level: ack::REPLICATED,
                expected_leader_epoch: u32::MAX,
                timeout_ms: u32::MAX,
                messages: vec![Message {
                    key: Some(vec![0xff; 3]),
                    headers: vec![header(&[0, 1, 2], &[0xfe; 2])],
                    body: vec![0, 0xff],
                }],
            }),
            "largest field values, binary key, header and body",
        ),
        req(
            "produce_at_limits",
            14,
            Request::Produce(produce(vec![Message {
                key: Some(vec![b'k'; 1024]),
                headers: headers_totalling(64, 16 * 1024)
                    .into_iter()
                    .enumerate()
                    .map(|(i, mut h)| {
                        if i == 0 {
                            h.name = vec![b'n'; 256];
                            h.value.truncate(h.value.len() - 255);
                        }
                        h
                    })
                    .collect(),
                body: b"x".to_vec(),
            }])),
            "at the default limits: key 1024 B, 64 headers, a 256 B name, 16384 header bytes; accepted",
        ),
        req(
            "produce_key_over",
            15,
            Request::Produce(produce(vec![keyed(&[b'k'; 1025], b"x")])),
            "key 1025 B: MessageTooLarge with default limits",
        ),
        req(
            "produce_headers_over",
            16,
            Request::Produce(produce(vec![Message {
                key: None,
                headers: (0..65).map(|_| header(b"h", b"")).collect(),
                body: b"x".to_vec(),
            }])),
            "65 headers: MessageTooLarge with default limits",
        ),
        req(
            "produce_header_name_over",
            17,
            Request::Produce(produce(vec![Message {
                key: None,
                headers: vec![header(&[b'n'; 257], b"")],
                body: b"x".to_vec(),
            }])),
            "a 257 B header name: MessageTooLarge with default limits",
        ),
        req(
            "produce_header_bytes_over",
            18,
            Request::Produce(produce(vec![Message {
                key: None,
                headers: headers_totalling(2, 16 * 1024 + 1),
                body: b"x".to_vec(),
            }])),
            "16385 header bytes: MessageTooLarge with default limits",
        ),
        req(
            "fetch",
            19,
            Request::Fetch(Fetch {
                topic: "orders".into(),
                partition: 0,
                from_seq: 0,
                max_bytes: 1 << 20,
                max_wait_ms: 500,
                expected_leader_epoch: 0,
            }),
            "",
        ),
        req(
            "fetch_max",
            20,
            Request::Fetch(Fetch {
                topic: "orders".into(),
                partition: u32::MAX,
                from_seq: u64::MAX,
                max_bytes: u32::MAX,
                max_wait_ms: u32::MAX,
                expected_leader_epoch: u32::MAX,
            }),
            "largest values",
        ),
        req("list_topics", 21, Request::ListTopics, "empty body"),
        req(
            "describe_topic",
            22,
            Request::DescribeTopic {
                name: "orders".into(),
            },
            "",
        ),
        req("broker_stats", 23, Request::BrokerStats, "empty body"),
        resp(
            "hello_ok",
            1,
            Response::HelloOk(HelloOk {
                version: 0,
                cluster_id: "c1".into(),
                node_id: 1,
                max_frame_bytes: 8 << 20,
                max_message_bytes: 1 << 20,
            }),
            "",
        ),
        resp(
            "pong",
            4,
            Response::Pong {
                nonce: 0x0102_0304_0506_0708,
            },
            "",
        ),
        resp(
            "metadata_ok",
            6,
            Response::MetadataOk(MetadataOk {
                cluster_id: "c1".into(),
                controller_node_id: 1,
                nodes: vec![Node {
                    node_id: 1,
                    host: "127.0.0.1".into(),
                    port: 9092,
                    rack: String::new(),
                }],
                topics: vec![
                    TopicMetadata {
                        name: "orders".into(),
                        error: 0,
                        partitions: vec![PartitionMetadata {
                            partition: 0,
                            leader_node_id: 1,
                            leader_epoch: 1,
                            replicas: vec![1],
                            in_sync_replicas: vec![1],
                            start_seq: 0,
                            next_seq: 42,
                        }],
                    },
                    TopicMetadata {
                        name: "missing".into(),
                        error: ErrorCode::UnknownTopic as u16,
                        partitions: vec![],
                    },
                ],
            }),
            "one known topic and one UnknownTopic",
        ),
        resp(
            "metadata_ok_empty",
            5,
            Response::MetadataOk(MetadataOk {
                cluster_id: String::new(),
                controller_node_id: u32::MAX,
                nodes: vec![],
                topics: vec![],
            }),
            "no nodes, no topics",
        ),
        resp("create_topic_ok", 7, Response::CreateTopicOk, "empty body"),
        resp(
            "delete_topic_ok",
            9,
            Response::DeleteTopicOk,
            "empty body; never sent by v0",
        ),
        resp(
            "produce_ok",
            10,
            Response::ProduceOk(ProduceOk {
                base_seq: 0,
                count: 1,
                leader_epoch: 1,
                timestamp_ms: 1_761_350_400_000,
            }),
            "",
        ),
        resp(
            "produce_ok_max",
            13,
            Response::ProduceOk(ProduceOk {
                base_seq: u64::MAX,
                count: u32::MAX,
                leader_epoch: u32::MAX,
                timestamp_ms: u64::MAX,
            }),
            "largest values",
        ),
        resp(
            "fetch_ok",
            19,
            Response::FetchOk(FetchOk {
                leader_epoch: 1,
                start_seq: 0,
                next_seq: 3,
                messages: vec![
                    FetchedMessage {
                        seq: 0,
                        timestamp_ms: 1_761_350_400_000,
                        message: Message::body(&b"hello"[..]),
                    },
                    FetchedMessage {
                        seq: 1,
                        timestamp_ms: 1_761_350_400_000,
                        message: keyed(b"a", b"hello"),
                    },
                    FetchedMessage {
                        seq: 2,
                        timestamp_ms: 1_761_350_400_001,
                        message: Message {
                            key: Some(b"b".to_vec()),
                            headers: vec![header(b"h", b"v")],
                            body: b"hello".to_vec(),
                        },
                    },
                ],
            }),
            "the same body three times: plain, with a key, with a key and a header",
        ),
        resp(
            "fetch_ok_empty",
            19,
            Response::FetchOk(FetchOk {
                leader_epoch: 1,
                start_seq: 5,
                next_seq: 5,
                messages: vec![],
            }),
            "a wait that timed out",
        ),
        resp(
            "list_topics_ok",
            21,
            Response::ListTopicsOk {
                topics: vec![
                    TopicSummary {
                        name: "orders".into(),
                        partitions: 1,
                    },
                    TopicSummary {
                        name: max_name.clone(),
                        partitions: 1,
                    },
                ],
            },
            "",
        ),
        resp(
            "list_topics_ok_empty",
            21,
            Response::ListTopicsOk { topics: vec![] },
            "",
        ),
        resp(
            "describe_topic_ok",
            22,
            Response::DescribeTopicOk(DescribeTopicOk {
                name: "orders".into(),
                replication_factor: 1,
                retention_ms: 86_400_000,
                partitions: vec![PartitionDescription {
                    partition: 0,
                    start_seq: 0,
                    next_seq: 42,
                    message_count: 42,
                    index_bytes: 512,
                }],
            }),
            "",
        ),
        resp(
            "broker_stats_ok",
            23,
            Response::BrokerStatsOk(BrokerStatsOk {
                node_id: 1,
                open_connections: 2,
                pending_bytes: 0,
                pending_bytes_peak: 4096,
                max_pending_bytes: 64 << 20,
                fetch_waiters: 1,
                produce_requests: 10,
                groups_committed: 4,
                fsyncs: 8,
                messages_accepted: 12,
                messages_deduplicated: 1,
                body_bytes_received: 600,
                body_bytes_new: 550,
                wal_bytes: 1000,
                segment_bytes: 550,
                index_bytes: 96,
            }),
            "",
        ),
        resp(
            "broker_stats_ok_max",
            23,
            Response::BrokerStatsOk(BrokerStatsOk {
                node_id: u32::MAX,
                open_connections: u32::MAX,
                pending_bytes: u64::MAX,
                pending_bytes_peak: u64::MAX,
                max_pending_bytes: u64::MAX,
                fetch_waiters: u32::MAX,
                produce_requests: u64::MAX,
                groups_committed: u64::MAX,
                fsyncs: u64::MAX,
                messages_accepted: u64::MAX,
                messages_deduplicated: u64::MAX,
                body_bytes_received: u64::MAX,
                body_bytes_new: u64::MAX,
                wal_bytes: u64::MAX,
                segment_bytes: u64::MAX,
                index_bytes: u64::MAX,
            }),
            "largest values",
        ),
        resp(
            "error_not_leader",
            10,
            Response::Error(ErrorBody {
                code: ErrorCode::NotLeader as u16,
                message: "not the leader".into(),
                leader_node_id: 2,
                leader_epoch: 3,
                ..ErrorBody::default()
            }),
            "leader and epoch set; never sent by v0",
        ),
        resp(
            "error_fenced",
            10,
            Response::Error(ErrorBody {
                code: ErrorCode::FencedLeaderEpoch as u16,
                message: "expected epoch 2, partition is at 1".into(),
                leader_node_id: 1,
                leader_epoch: 1,
                ..ErrorBody::default()
            }),
            "",
        ),
        resp(
            "error_overloaded",
            19,
            Response::Error(ErrorBody {
                code: ErrorCode::Overloaded as u16,
                message: "too many waiting fetches".into(),
                retry_after_ms: 100,
                ..ErrorBody::default()
            }),
            "with a retry hint",
        ),
        resp(
            "error_unsupported_version",
            2,
            Response::Error(ErrorBody {
                code: ErrorCode::UnsupportedVersion as u16,
                message: "no common version".into(),
                min_version: 0,
                max_version: 0,
                ..ErrorBody::default()
            }),
            "the broker's range in min_version and max_version",
        ),
        resp(
            "error_unknown_code",
            99,
            Response::Error(ErrorBody {
                code: 999,
                message: String::new(),
                ..ErrorBody::default()
            }),
            "a code v0 does not define: clients keep it and treat it as not retryable",
        ),
    ];
    for code in ErrorCode::ALL {
        out.push(resp(
            error_name(code),
            100 + code as u64,
            Response::error(code, code.name()),
            "every error code, plain",
        ));
    }
    out
}

fn error_name(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::UnknownTopic => "error_unknown_topic",
        ErrorCode::UnknownPartition => "error_unknown_partition",
        ErrorCode::TopicExists => "error_topic_exists",
        ErrorCode::MessageTooLarge => "error_message_too_large",
        ErrorCode::FrameTooLarge => "error_frame_too_large",
        ErrorCode::Overloaded => "error_overloaded_plain",
        ErrorCode::UnsupportedAckLevel => "error_unsupported_ack_level",
        ErrorCode::UnsupportedVersion => "error_unsupported_version_plain",
        ErrorCode::Unsupported => "error_unsupported",
        ErrorCode::Unauthenticated => "error_unauthenticated",
        ErrorCode::Forbidden => "error_forbidden",
        ErrorCode::MalformedRequest => "error_malformed_request",
        ErrorCode::NotLeader => "error_not_leader_plain",
        ErrorCode::FencedLeaderEpoch => "error_fenced_plain",
        ErrorCode::StorageError => "error_storage_error",
    }
}

pub fn encode(s: &Sample) -> Vec<u8> {
    let mut out = Vec::new();
    match &s.frame {
        Frame::Request(r) => encode_request(&mut out, s.correlation_id, r),
        Frame::Response(r) => encode_response(&mut out, s.correlation_id, r),
    }
    out
}
