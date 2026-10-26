//! The Straum protocol, version 0 (docs/protocol.md): frames, requests,
//! responses and the default partitioner. Encoding and decoding only, no
//! I/O. Decoding never panics: every input gives a value or a
//! `DecodeError`.

pub mod json;
mod wire;

use wire::{
    Reader, put_bytes, put_count, put_short_bytes, put_string, put_u8, put_u16, put_u32, put_u64,
};

/// The protocol version this crate speaks.
pub const VERSION: u16 = 0;
/// `length u32 | type u16 | correlation_id u64`.
pub const FRAME_HEADER: usize = 14;
/// `length` counts the type and correlation id too.
pub const MIN_FRAME_LENGTH: u32 = 10;
/// Type of the `Error` response.
pub const ERROR_TYPE: u16 = 0xffff;
/// Bit set in a success response's type.
pub const RESPONSE_BIT: u16 = 0x8000;
/// The most topics a Metadata request may name, and the most a ListTopics
/// page holds (sections 4.3 and 4.9).
pub const MAX_REQUEST_TOPICS: u32 = 1000;

const FLAG_KEY: u8 = 1;
const FLAG_HEADERS: u8 = 2;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    /// The body ends before a field, or a count exceeds the bytes left.
    Truncated,
    InvalidUtf8,
    /// Bytes after the last field.
    TrailingBytes,
    /// A field value this version does not define.
    InvalidValue(&'static str),
    UnknownType(u16),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "body ends early"),
            Self::InvalidUtf8 => write!(f, "string is not UTF-8"),
            Self::TrailingBytes => write!(f, "bytes after the last field"),
            Self::InvalidValue(what) => write!(f, "invalid value: {what}"),
            Self::UnknownType(t) => write!(f, "unknown message type {t:#06x}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// The fixed part of every frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Bytes after the length field: 10 + body length.
    pub length: u32,
    pub msg_type: u16,
    pub correlation_id: u64,
}

impl FrameHeader {
    pub fn parse(b: &[u8; FRAME_HEADER]) -> Self {
        Self {
            length: u32::from_le_bytes([b[0], b[1], b[2], b[3]]),
            msg_type: u16::from_le_bytes([b[4], b[5]]),
            correlation_id: u64::from_le_bytes([
                b[6], b[7], b[8], b[9], b[10], b[11], b[12], b[13],
            ]),
        }
    }

    /// Body bytes that follow the header; `None` if `length` is below 10.
    pub fn body_len(&self) -> Option<usize> {
        self.length
            .checked_sub(MIN_FRAME_LENGTH)
            .map(|n| n as usize)
    }

    /// Size of the whole frame, length field included.
    pub fn frame_size(&self) -> u64 {
        4 + u64::from(self.length)
    }
}

/// Error codes of docs/protocol.md section 5.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    UnknownTopic = 1,
    UnknownPartition = 2,
    TopicExists = 3,
    MessageTooLarge = 4,
    FrameTooLarge = 5,
    Overloaded = 6,
    UnsupportedAckLevel = 7,
    UnsupportedVersion = 8,
    Unsupported = 9,
    Unauthenticated = 10,
    Forbidden = 11,
    MalformedRequest = 12,
    NotLeader = 13,
    FencedLeaderEpoch = 14,
    StorageError = 15,
}

impl ErrorCode {
    pub const ALL: [Self; 15] = [
        Self::UnknownTopic,
        Self::UnknownPartition,
        Self::TopicExists,
        Self::MessageTooLarge,
        Self::FrameTooLarge,
        Self::Overloaded,
        Self::UnsupportedAckLevel,
        Self::UnsupportedVersion,
        Self::Unsupported,
        Self::Unauthenticated,
        Self::Forbidden,
        Self::MalformedRequest,
        Self::NotLeader,
        Self::FencedLeaderEpoch,
        Self::StorageError,
    ];

    pub fn from_u16(v: u16) -> Option<Self> {
        Self::ALL.into_iter().find(|c| *c as u16 == v)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::UnknownTopic => "UnknownTopic",
            Self::UnknownPartition => "UnknownPartition",
            Self::TopicExists => "TopicExists",
            Self::MessageTooLarge => "MessageTooLarge",
            Self::FrameTooLarge => "FrameTooLarge",
            Self::Overloaded => "Overloaded",
            Self::UnsupportedAckLevel => "UnsupportedAckLevel",
            Self::UnsupportedVersion => "UnsupportedVersion",
            Self::Unsupported => "Unsupported",
            Self::Unauthenticated => "Unauthenticated",
            Self::Forbidden => "Forbidden",
            Self::MalformedRequest => "MalformedRequest",
            Self::NotLeader => "NotLeader",
            Self::FencedLeaderEpoch => "FencedLeaderEpoch",
            Self::StorageError => "StorageError",
        }
    }

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|c| c.name() == name)
    }

    /// Section 6: retried after a Metadata refresh or a backoff.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::NotLeader | Self::FencedLeaderEpoch | Self::Overloaded
        )
    }
}

/// `Produce.ack_level` values.
pub mod ack {
    pub const WRITTEN: u8 = 0;
    pub const DURABLE: u8 = 1;
    pub const REPLICATED: u8 = 2;
}

/// A message of Produce and Fetch (section 4.6). `key: Some(vec![])` is a
/// present, empty key; `None` is no key.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Message {
    pub key: Option<Vec<u8>>,
    pub headers: Vec<Header>,
    pub body: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Header {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

impl Message {
    pub fn body(body: impl Into<Vec<u8>>) -> Self {
        Self {
            body: body.into(),
            ..Self::default()
        }
    }

    /// Bytes of all header names and values (`max_header_bytes`).
    pub fn header_bytes(&self) -> usize {
        self.headers
            .iter()
            .map(|h| h.name.len() + h.value.len())
            .sum()
    }

    /// Key, header and body bytes, as Fetch's `max_bytes` counts them.
    pub fn content_bytes(&self) -> usize {
        self.key.as_ref().map_or(0, Vec::len) + self.header_bytes() + self.body.len()
    }

    fn encode(&self, out: &mut Vec<u8>) {
        let mut flags = 0;
        if self.key.is_some() {
            flags |= FLAG_KEY;
        }
        if !self.headers.is_empty() {
            flags |= FLAG_HEADERS;
        }
        put_u8(out, flags);
        if let Some(key) = &self.key {
            put_short_bytes(out, key);
        }
        if !self.headers.is_empty() {
            put_u16(out, self.headers.len().min(u16::MAX as usize) as u16);
            for h in self.headers.iter().take(u16::MAX as usize) {
                put_short_bytes(out, &h.name);
                put_bytes(out, &h.value);
            }
        }
        put_bytes(out, &self.body);
    }

    fn decode(r: &mut Reader<'_>) -> Result<Self, DecodeError> {
        let flags = r.u8()?;
        if flags & !(FLAG_KEY | FLAG_HEADERS) != 0 {
            return Err(DecodeError::InvalidValue("reserved message flags"));
        }
        let key = if flags & FLAG_KEY != 0 {
            Some(r.short_bytes()?)
        } else {
            None
        };
        let mut headers = Vec::new();
        if flags & FLAG_HEADERS != 0 {
            let n = r.u16()?;
            if n == 0 {
                return Err(DecodeError::InvalidValue("headers flag with no headers"));
            }
            for _ in 0..n {
                let name = r.short_bytes()?;
                let value = r.bytes()?;
                headers.push(Header { name, value });
            }
        }
        let body = r.bytes()?;
        Ok(Self { key, headers, body })
    }
}

/// Smallest encoded message: flags and an empty body.
const MIN_MESSAGE: usize = 5;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hello {
    pub min_version: u16,
    pub max_version: u16,
    pub client_name: String,
    pub auth_mechanism: String,
    pub auth_payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTopic {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
    pub retention_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Produce {
    pub topic: String,
    pub partition: u32,
    pub producer_id: u64,
    pub producer_seq: u64,
    pub ack_level: u8,
    pub expected_leader_epoch: u32,
    pub timeout_ms: u32,
    pub messages: Vec<Message>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fetch {
    pub topic: String,
    pub partition: u32,
    pub from_seq: u64,
    pub max_bytes: u32,
    pub max_wait_ms: u32,
    pub expected_leader_epoch: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Hello(Hello),
    Ping { nonce: u64 },
    Metadata { topics: Vec<String> },
    CreateTopic(CreateTopic),
    DeleteTopic { name: String },
    Produce(Produce),
    Fetch(Fetch),
    ListTopics { cursor: String, limit: u32 },
    DescribeTopic { name: String },
    BrokerStats,
}

pub mod types {
    pub const HELLO: u16 = 0x0001;
    pub const PING: u16 = 0x0002;
    pub const METADATA: u16 = 0x0003;
    pub const CREATE_TOPIC: u16 = 0x0004;
    pub const DELETE_TOPIC: u16 = 0x0005;
    pub const PRODUCE: u16 = 0x0006;
    pub const FETCH: u16 = 0x0007;
    pub const LIST_TOPICS: u16 = 0x0008;
    pub const DESCRIBE_TOPIC: u16 = 0x0009;
    pub const BROKER_STATS: u16 = 0x000a;
}

impl Request {
    pub fn msg_type(&self) -> u16 {
        use types::*;
        match self {
            Self::Hello(_) => HELLO,
            Self::Ping { .. } => PING,
            Self::Metadata { .. } => METADATA,
            Self::CreateTopic(_) => CREATE_TOPIC,
            Self::DeleteTopic { .. } => DELETE_TOPIC,
            Self::Produce(_) => PRODUCE,
            Self::Fetch(_) => FETCH,
            Self::ListTopics { .. } => LIST_TOPICS,
            Self::DescribeTopic { .. } => DESCRIBE_TOPIC,
            Self::BrokerStats => BROKER_STATS,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Hello(_) => "Hello",
            Self::Ping { .. } => "Ping",
            Self::Metadata { .. } => "Metadata",
            Self::CreateTopic(_) => "CreateTopic",
            Self::DeleteTopic { .. } => "DeleteTopic",
            Self::Produce(_) => "Produce",
            Self::Fetch(_) => "Fetch",
            Self::ListTopics { .. } => "ListTopics",
            Self::DescribeTopic { .. } => "DescribeTopic",
            Self::BrokerStats => "BrokerStats",
        }
    }

    pub fn encode_body(&self, out: &mut Vec<u8>) {
        match self {
            Self::Hello(h) => {
                put_u16(out, h.min_version);
                put_u16(out, h.max_version);
                put_string(out, &h.client_name);
                put_string(out, &h.auth_mechanism);
                put_bytes(out, &h.auth_payload);
            }
            Self::Ping { nonce } => put_u64(out, *nonce),
            Self::Metadata { topics } => {
                put_count(out, topics.len());
                for t in topics {
                    put_string(out, t);
                }
            }
            Self::CreateTopic(c) => {
                put_string(out, &c.name);
                put_u32(out, c.partitions);
                put_u16(out, c.replication_factor);
                put_u64(out, c.retention_ms);
            }
            Self::DeleteTopic { name } | Self::DescribeTopic { name } => put_string(out, name),
            Self::Produce(p) => {
                put_string(out, &p.topic);
                put_u32(out, p.partition);
                put_u64(out, p.producer_id);
                put_u64(out, p.producer_seq);
                put_u8(out, p.ack_level);
                put_u32(out, p.expected_leader_epoch);
                put_u32(out, p.timeout_ms);
                put_count(out, p.messages.len());
                for m in &p.messages {
                    m.encode(out);
                }
            }
            Self::Fetch(f) => {
                put_string(out, &f.topic);
                put_u32(out, f.partition);
                put_u64(out, f.from_seq);
                put_u32(out, f.max_bytes);
                put_u32(out, f.max_wait_ms);
                put_u32(out, f.expected_leader_epoch);
            }
            Self::ListTopics { cursor, limit } => {
                put_string(out, cursor);
                put_u32(out, *limit);
            }
            Self::BrokerStats => {}
        }
    }

    pub fn decode(msg_type: u16, body: &[u8]) -> Result<Self, DecodeError> {
        use types::*;
        let mut r = Reader::new(body);
        let req = match msg_type {
            HELLO => Self::Hello(Hello {
                min_version: r.u16()?,
                max_version: r.u16()?,
                client_name: r.string()?,
                auth_mechanism: r.string()?,
                auth_payload: r.bytes()?,
            }),
            PING => Self::Ping { nonce: r.u64()? },
            METADATA => {
                let n = r.count(2)?;
                let mut topics = Vec::new();
                for _ in 0..n {
                    topics.push(r.string()?);
                }
                Self::Metadata { topics }
            }
            CREATE_TOPIC => Self::CreateTopic(CreateTopic {
                name: r.string()?,
                partitions: r.u32()?,
                replication_factor: r.u16()?,
                retention_ms: r.u64()?,
            }),
            DELETE_TOPIC => Self::DeleteTopic { name: r.string()? },
            PRODUCE => {
                let topic = r.string()?;
                let partition = r.u32()?;
                let producer_id = r.u64()?;
                let producer_seq = r.u64()?;
                let ack_level = r.u8()?;
                let expected_leader_epoch = r.u32()?;
                let timeout_ms = r.u32()?;
                let n = r.count(MIN_MESSAGE)?;
                let mut messages = Vec::new();
                for _ in 0..n {
                    messages.push(Message::decode(&mut r)?);
                }
                Self::Produce(Produce {
                    topic,
                    partition,
                    producer_id,
                    producer_seq,
                    ack_level,
                    expected_leader_epoch,
                    timeout_ms,
                    messages,
                })
            }
            FETCH => Self::Fetch(Fetch {
                topic: r.string()?,
                partition: r.u32()?,
                from_seq: r.u64()?,
                max_bytes: r.u32()?,
                max_wait_ms: r.u32()?,
                expected_leader_epoch: r.u32()?,
            }),
            LIST_TOPICS => Self::ListTopics {
                cursor: r.string()?,
                limit: r.u32()?,
            },
            DESCRIBE_TOPIC => Self::DescribeTopic { name: r.string()? },
            BROKER_STATS => Self::BrokerStats,
            t => return Err(DecodeError::UnknownType(t)),
        };
        r.finish()?;
        Ok(req)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HelloOk {
    pub version: u16,
    pub cluster_id: String,
    pub node_id: u32,
    pub max_frame_bytes: u32,
    pub max_message_bytes: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Node {
    pub node_id: u32,
    pub host: String,
    pub port: u16,
    pub rack: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub partition: u32,
    pub leader_node_id: u32,
    pub leader_epoch: u32,
    pub replicas: Vec<u32>,
    pub in_sync_replicas: Vec<u32>,
    pub start_seq: u64,
    pub next_seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    /// 0, or `ErrorCode::UnknownTopic`.
    pub error: u16,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataOk {
    pub cluster_id: String,
    pub controller_node_id: u32,
    pub nodes: Vec<Node>,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProduceOk {
    pub base_seq: u64,
    pub count: u32,
    pub leader_epoch: u32,
    pub timestamp_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchedMessage {
    pub seq: u64,
    pub timestamp_ms: u64,
    pub message: Message,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchOk {
    pub leader_epoch: u32,
    pub start_seq: u64,
    pub next_seq: u64,
    pub messages: Vec<FetchedMessage>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicSummary {
    pub name: String,
    pub partitions: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionDescription {
    pub partition: u32,
    pub start_seq: u64,
    pub next_seq: u64,
    pub message_count: u64,
    pub index_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeTopicOk {
    pub name: String,
    pub replication_factor: u16,
    pub retention_ms: u64,
    pub partitions: Vec<PartitionDescription>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BrokerStatsOk {
    pub node_id: u32,
    pub open_connections: u32,
    pub pending_bytes: u64,
    pub pending_bytes_peak: u64,
    pub max_pending_bytes: u64,
    pub fetch_waiters: u32,
    pub produce_requests: u64,
    pub groups_committed: u64,
    pub fsyncs: u64,
    pub messages_accepted: u64,
    pub messages_deduplicated: u64,
    pub body_bytes_received: u64,
    pub body_bytes_new: u64,
    pub wal_bytes: u64,
    pub segment_bytes: u64,
    pub index_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ErrorBody {
    /// Kept as sent: a code this version does not know stays readable.
    pub code: u16,
    pub message: String,
    pub leader_node_id: u32,
    pub leader_epoch: u32,
    pub retry_after_ms: u32,
    pub min_version: u16,
    pub max_version: u16,
}

impl ErrorBody {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code: code as u16,
            message: message.into(),
            ..Self::default()
        }
    }

    pub fn code(&self) -> Option<ErrorCode> {
        ErrorCode::from_u16(self.code)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    HelloOk(HelloOk),
    Pong {
        nonce: u64,
    },
    MetadataOk(MetadataOk),
    CreateTopicOk,
    DeleteTopicOk,
    ProduceOk(ProduceOk),
    FetchOk(FetchOk),
    ListTopicsOk {
        topics: Vec<TopicSummary>,
        next_cursor: String,
    },
    DescribeTopicOk(DescribeTopicOk),
    BrokerStatsOk(BrokerStatsOk),
    Error(ErrorBody),
}

fn put_u32s(out: &mut Vec<u8>, v: &[u32]) {
    put_count(out, v.len());
    for x in v {
        put_u32(out, *x);
    }
}

fn read_u32s(r: &mut Reader<'_>) -> Result<Vec<u32>, DecodeError> {
    let n = r.count(4)?;
    let mut v = Vec::new();
    for _ in 0..n {
        v.push(r.u32()?);
    }
    Ok(v)
}

impl Response {
    pub fn msg_type(&self) -> u16 {
        use types::*;
        match self {
            Self::HelloOk(_) => HELLO | RESPONSE_BIT,
            Self::Pong { .. } => PING | RESPONSE_BIT,
            Self::MetadataOk(_) => METADATA | RESPONSE_BIT,
            Self::CreateTopicOk => CREATE_TOPIC | RESPONSE_BIT,
            Self::DeleteTopicOk => DELETE_TOPIC | RESPONSE_BIT,
            Self::ProduceOk(_) => PRODUCE | RESPONSE_BIT,
            Self::FetchOk(_) => FETCH | RESPONSE_BIT,
            Self::ListTopicsOk { .. } => LIST_TOPICS | RESPONSE_BIT,
            Self::DescribeTopicOk(_) => DESCRIBE_TOPIC | RESPONSE_BIT,
            Self::BrokerStatsOk(_) => BROKER_STATS | RESPONSE_BIT,
            Self::Error(_) => ERROR_TYPE,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::HelloOk(_) => "HelloOk",
            Self::Pong { .. } => "Pong",
            Self::MetadataOk(_) => "MetadataOk",
            Self::CreateTopicOk => "CreateTopicOk",
            Self::DeleteTopicOk => "DeleteTopicOk",
            Self::ProduceOk(_) => "ProduceOk",
            Self::FetchOk(_) => "FetchOk",
            Self::ListTopicsOk { .. } => "ListTopicsOk",
            Self::DescribeTopicOk(_) => "DescribeTopicOk",
            Self::BrokerStatsOk(_) => "BrokerStatsOk",
            Self::Error(_) => "Error",
        }
    }

    pub fn error(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::Error(ErrorBody::new(code, message))
    }

    pub fn encode_body(&self, out: &mut Vec<u8>) {
        match self {
            Self::HelloOk(h) => {
                put_u16(out, h.version);
                put_string(out, &h.cluster_id);
                put_u32(out, h.node_id);
                put_u32(out, h.max_frame_bytes);
                put_u32(out, h.max_message_bytes);
            }
            Self::Pong { nonce } => put_u64(out, *nonce),
            Self::MetadataOk(m) => {
                put_string(out, &m.cluster_id);
                put_u32(out, m.controller_node_id);
                put_count(out, m.nodes.len());
                for n in &m.nodes {
                    put_u32(out, n.node_id);
                    put_string(out, &n.host);
                    put_u16(out, n.port);
                    put_string(out, &n.rack);
                }
                put_count(out, m.topics.len());
                for t in &m.topics {
                    put_string(out, &t.name);
                    put_u16(out, t.error);
                    put_count(out, t.partitions.len());
                    for p in &t.partitions {
                        put_u32(out, p.partition);
                        put_u32(out, p.leader_node_id);
                        put_u32(out, p.leader_epoch);
                        put_u32s(out, &p.replicas);
                        put_u32s(out, &p.in_sync_replicas);
                        put_u64(out, p.start_seq);
                        put_u64(out, p.next_seq);
                    }
                }
            }
            Self::CreateTopicOk | Self::DeleteTopicOk => {}
            Self::ProduceOk(p) => {
                put_u64(out, p.base_seq);
                put_u32(out, p.count);
                put_u32(out, p.leader_epoch);
                put_u64(out, p.timestamp_ms);
            }
            Self::FetchOk(f) => {
                put_u32(out, f.leader_epoch);
                put_u64(out, f.start_seq);
                put_u64(out, f.next_seq);
                put_count(out, f.messages.len());
                for m in &f.messages {
                    put_u64(out, m.seq);
                    put_u64(out, m.timestamp_ms);
                    m.message.encode(out);
                }
            }
            Self::ListTopicsOk {
                topics,
                next_cursor,
            } => {
                put_count(out, topics.len());
                for t in topics {
                    put_string(out, &t.name);
                    put_u32(out, t.partitions);
                }
                put_string(out, next_cursor);
            }
            Self::DescribeTopicOk(d) => {
                put_string(out, &d.name);
                put_u16(out, d.replication_factor);
                put_u64(out, d.retention_ms);
                put_count(out, d.partitions.len());
                for p in &d.partitions {
                    put_u32(out, p.partition);
                    put_u64(out, p.start_seq);
                    put_u64(out, p.next_seq);
                    put_u64(out, p.message_count);
                    put_u64(out, p.index_bytes);
                }
            }
            Self::BrokerStatsOk(s) => {
                put_u32(out, s.node_id);
                put_u32(out, s.open_connections);
                put_u64(out, s.pending_bytes);
                put_u64(out, s.pending_bytes_peak);
                put_u64(out, s.max_pending_bytes);
                put_u32(out, s.fetch_waiters);
                for v in [
                    s.produce_requests,
                    s.groups_committed,
                    s.fsyncs,
                    s.messages_accepted,
                    s.messages_deduplicated,
                    s.body_bytes_received,
                    s.body_bytes_new,
                    s.wal_bytes,
                    s.segment_bytes,
                    s.index_bytes,
                ] {
                    put_u64(out, v);
                }
            }
            Self::Error(e) => {
                put_u16(out, e.code);
                put_string(out, &e.message);
                put_u32(out, e.leader_node_id);
                put_u32(out, e.leader_epoch);
                put_u32(out, e.retry_after_ms);
                put_u16(out, e.min_version);
                put_u16(out, e.max_version);
            }
        }
    }

    pub fn decode(msg_type: u16, body: &[u8]) -> Result<Self, DecodeError> {
        use types::*;
        let mut r = Reader::new(body);
        if msg_type == ERROR_TYPE {
            let e = ErrorBody {
                code: r.u16()?,
                message: r.string()?,
                leader_node_id: r.u32()?,
                leader_epoch: r.u32()?,
                retry_after_ms: r.u32()?,
                min_version: r.u16()?,
                max_version: r.u16()?,
            };
            r.finish()?;
            return Ok(Self::Error(e));
        }
        if msg_type & RESPONSE_BIT == 0 {
            return Err(DecodeError::UnknownType(msg_type));
        }
        let resp = match msg_type & !RESPONSE_BIT {
            HELLO => Self::HelloOk(HelloOk {
                version: r.u16()?,
                cluster_id: r.string()?,
                node_id: r.u32()?,
                max_frame_bytes: r.u32()?,
                max_message_bytes: r.u32()?,
            }),
            PING => Self::Pong { nonce: r.u64()? },
            METADATA => {
                let cluster_id = r.string()?;
                let controller_node_id = r.u32()?;
                let n = r.count(4 + 2 + 2 + 2)?;
                let mut nodes = Vec::new();
                for _ in 0..n {
                    nodes.push(Node {
                        node_id: r.u32()?,
                        host: r.string()?,
                        port: r.u16()?,
                        rack: r.string()?,
                    });
                }
                let n = r.count(2 + 2 + 4)?;
                let mut topics = Vec::new();
                for _ in 0..n {
                    let name = r.string()?;
                    let error = r.u16()?;
                    let np = r.count(4 * 5 + 16)?;
                    let mut partitions = Vec::new();
                    for _ in 0..np {
                        partitions.push(PartitionMetadata {
                            partition: r.u32()?,
                            leader_node_id: r.u32()?,
                            leader_epoch: r.u32()?,
                            replicas: read_u32s(&mut r)?,
                            in_sync_replicas: read_u32s(&mut r)?,
                            start_seq: r.u64()?,
                            next_seq: r.u64()?,
                        });
                    }
                    topics.push(TopicMetadata {
                        name,
                        error,
                        partitions,
                    });
                }
                Self::MetadataOk(MetadataOk {
                    cluster_id,
                    controller_node_id,
                    nodes,
                    topics,
                })
            }
            CREATE_TOPIC => Self::CreateTopicOk,
            DELETE_TOPIC => Self::DeleteTopicOk,
            PRODUCE => Self::ProduceOk(ProduceOk {
                base_seq: r.u64()?,
                count: r.u32()?,
                leader_epoch: r.u32()?,
                timestamp_ms: r.u64()?,
            }),
            FETCH => {
                let leader_epoch = r.u32()?;
                let start_seq = r.u64()?;
                let next_seq = r.u64()?;
                let n = r.count(16 + MIN_MESSAGE)?;
                let mut messages = Vec::new();
                for _ in 0..n {
                    let seq = r.u64()?;
                    let timestamp_ms = r.u64()?;
                    let message = Message::decode(&mut r)?;
                    messages.push(FetchedMessage {
                        seq,
                        timestamp_ms,
                        message,
                    });
                }
                Self::FetchOk(FetchOk {
                    leader_epoch,
                    start_seq,
                    next_seq,
                    messages,
                })
            }
            LIST_TOPICS => {
                let n = r.count(2 + 4)?;
                let mut topics = Vec::new();
                for _ in 0..n {
                    topics.push(TopicSummary {
                        name: r.string()?,
                        partitions: r.u32()?,
                    });
                }
                Self::ListTopicsOk {
                    topics,
                    next_cursor: r.string()?,
                }
            }
            DESCRIBE_TOPIC => {
                let name = r.string()?;
                let replication_factor = r.u16()?;
                let retention_ms = r.u64()?;
                let n = r.count(4 + 32)?;
                let mut partitions = Vec::new();
                for _ in 0..n {
                    partitions.push(PartitionDescription {
                        partition: r.u32()?,
                        start_seq: r.u64()?,
                        next_seq: r.u64()?,
                        message_count: r.u64()?,
                        index_bytes: r.u64()?,
                    });
                }
                Self::DescribeTopicOk(DescribeTopicOk {
                    name,
                    replication_factor,
                    retention_ms,
                    partitions,
                })
            }
            BROKER_STATS => {
                let node_id = r.u32()?;
                let open_connections = r.u32()?;
                let pending_bytes = r.u64()?;
                let pending_bytes_peak = r.u64()?;
                let max_pending_bytes = r.u64()?;
                let fetch_waiters = r.u32()?;
                let mut v = [0u64; 10];
                for x in &mut v {
                    *x = r.u64()?;
                }
                Self::BrokerStatsOk(BrokerStatsOk {
                    node_id,
                    open_connections,
                    pending_bytes,
                    pending_bytes_peak,
                    max_pending_bytes,
                    fetch_waiters,
                    produce_requests: v[0],
                    groups_committed: v[1],
                    fsyncs: v[2],
                    messages_accepted: v[3],
                    messages_deduplicated: v[4],
                    body_bytes_received: v[5],
                    body_bytes_new: v[6],
                    wal_bytes: v[7],
                    segment_bytes: v[8],
                    index_bytes: v[9],
                })
            }
            _ => return Err(DecodeError::UnknownType(msg_type)),
        };
        r.finish()?;
        Ok(resp)
    }
}

/// Appends a whole frame: header, then `body` as written by `encode_body`.
fn frame(out: &mut Vec<u8>, msg_type: u16, correlation_id: u64, body: impl FnOnce(&mut Vec<u8>)) {
    let start = out.len();
    put_u32(out, 0);
    put_u16(out, msg_type);
    put_u64(out, correlation_id);
    body(out);
    let length = (out.len() - start - 4).min(u32::MAX as usize) as u32;
    out[start..start + 4].copy_from_slice(&length.to_le_bytes());
}

pub fn encode_request(out: &mut Vec<u8>, correlation_id: u64, req: &Request) {
    frame(out, req.msg_type(), correlation_id, |o| req.encode_body(o));
}

pub fn encode_response(out: &mut Vec<u8>, correlation_id: u64, resp: &Response) {
    frame(out, resp.msg_type(), correlation_id, |o| {
        resp.encode_body(o)
    });
}

/// Splits a complete frame into its header and body; `None` if `buf` is
/// not exactly one frame.
pub fn split_frame(buf: &[u8]) -> Option<(FrameHeader, &[u8])> {
    let head: &[u8; FRAME_HEADER] = buf.get(..FRAME_HEADER)?.try_into().ok()?;
    let h = FrameHeader::parse(head);
    let body = buf.get(FRAME_HEADER..)?;
    (h.body_len()? == body.len()).then_some((h, body))
}

/// The default partitioner (docs/protocol.md section 7):
/// `crc32c(key) mod partition_count`. `partition_count` 0 gives 0.
pub fn partition_for_key(key: &[u8], partition_count: u32) -> u32 {
    crc32c::crc32c(key)
        .checked_rem(partition_count)
        .unwrap_or(0)
}
