//! JSON forms of requests and responses, for the conformance suite
//! (`conformance/README.md`): the descriptions of the golden frames and the
//! messages in scenarios. A small reader and writer of their own: the
//! suite needs objects, arrays, strings, integers, booleans and null only.
//!
//! Conventions: field names as in docs/protocol.md; `u8`, `u16` and `u32`
//! are JSON numbers; `u64` values are decimal strings (they exceed what
//! many JSON readers hold exactly); bytes are lowercase hex strings; a key
//! is `null` when absent; error codes are their names.

use std::fmt::Write as _;

use crate::{
    BrokerStatsOk, CreateTopic, DescribeTopicOk, ErrorBody, ErrorCode, Fetch, FetchOk,
    FetchedMessage, Header, Hello, HelloOk, Message, MetadataOk, Node, PartitionDescription,
    PartitionMetadata, Produce, ProduceOk, Request, Response, TopicMetadata, TopicSummary,
};

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    /// Integers only.
    Num(i64),
    Str(String),
    Arr(Vec<Value>),
    /// Keys in their written order.
    Obj(Vec<(String, Value)>),
}

impl Value {
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Self::Obj(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Str(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Num(n) => Some(*n),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Self::Arr(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Two-space indented JSON with a final newline, keys in order.
    pub fn to_pretty(&self) -> String {
        let mut out = String::new();
        self.write(&mut out, 0);
        out.push('\n');
        out
    }

    fn write(&self, out: &mut String, depth: usize) {
        let pad = |out: &mut String, d: usize| out.extend(std::iter::repeat_n("  ", d));
        match self {
            Self::Null => out.push_str("null"),
            Self::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Self::Num(n) => {
                let _ = write!(out, "{n}");
            }
            Self::Str(s) => write_str(out, s),
            Self::Arr(v) if v.is_empty() => out.push_str("[]"),
            Self::Arr(v) => {
                out.push_str("[\n");
                for (i, x) in v.iter().enumerate() {
                    pad(out, depth + 1);
                    x.write(out, depth + 1);
                    out.push_str(if i + 1 < v.len() { ",\n" } else { "\n" });
                }
                pad(out, depth);
                out.push(']');
            }
            Self::Obj(f) if f.is_empty() => out.push_str("{}"),
            Self::Obj(f) => {
                out.push_str("{\n");
                for (i, (k, x)) in f.iter().enumerate() {
                    pad(out, depth + 1);
                    write_str(out, k);
                    out.push_str(": ");
                    x.write(out, depth + 1);
                    out.push_str(if i + 1 < f.len() { ",\n" } else { "\n" });
                }
                pad(out, depth);
                out.push('}');
            }
        }
    }
}

fn write_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Parses one JSON document. Numbers must be integers.
pub fn parse(text: &str) -> Result<Value, String> {
    let mut p = Parser {
        s: text.as_bytes(),
        i: 0,
    };
    let v = p.value(0)?;
    p.ws();
    if p.i != p.s.len() {
        return Err(p.err("trailing characters"));
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a [u8],
    i: usize,
}

impl Parser<'_> {
    fn err(&self, what: &str) -> String {
        format!("JSON: {what} at byte {}", self.i)
    }

    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], b' ' | b'\n' | b'\r' | b'\t') {
            self.i += 1;
        }
    }

    fn eat(&mut self, c: u8) -> bool {
        self.ws();
        if self.s.get(self.i) == Some(&c) {
            self.i += 1;
            true
        } else {
            false
        }
    }

    fn value(&mut self, depth: usize) -> Result<Value, String> {
        if depth > 64 {
            return Err(self.err("nesting too deep"));
        }
        self.ws();
        match self.s.get(self.i) {
            Some(b'{') => {
                self.i += 1;
                let mut fields = Vec::new();
                if self.eat(b'}') {
                    return Ok(Value::Obj(fields));
                }
                loop {
                    self.ws();
                    let k = self.string()?;
                    if !self.eat(b':') {
                        return Err(self.err("expected ':'"));
                    }
                    fields.push((k, self.value(depth + 1)?));
                    if self.eat(b'}') {
                        return Ok(Value::Obj(fields));
                    }
                    if !self.eat(b',') {
                        return Err(self.err("expected ',' or '}'"));
                    }
                }
            }
            Some(b'[') => {
                self.i += 1;
                let mut items = Vec::new();
                if self.eat(b']') {
                    return Ok(Value::Arr(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    if self.eat(b']') {
                        return Ok(Value::Arr(items));
                    }
                    if !self.eat(b',') {
                        return Err(self.err("expected ',' or ']'"));
                    }
                }
            }
            Some(b'"') => self.string().map(Value::Str),
            Some(b't') if self.s[self.i..].starts_with(b"true") => {
                self.i += 4;
                Ok(Value::Bool(true))
            }
            Some(b'f') if self.s[self.i..].starts_with(b"false") => {
                self.i += 5;
                Ok(Value::Bool(false))
            }
            Some(b'n') if self.s[self.i..].starts_with(b"null") => {
                self.i += 4;
                Ok(Value::Null)
            }
            Some(b'-' | b'0'..=b'9') => {
                let start = self.i;
                self.i += 1;
                while self.i < self.s.len() && self.s[self.i].is_ascii_digit() {
                    self.i += 1;
                }
                std::str::from_utf8(&self.s[start..self.i])
                    .ok()
                    .and_then(|t| t.parse().ok())
                    .map(Value::Num)
                    .ok_or_else(|| self.err("bad integer"))
            }
            _ => Err(self.err("unexpected character")),
        }
    }

    fn string(&mut self) -> Result<String, String> {
        if self.s.get(self.i) != Some(&b'"') {
            return Err(self.err("expected a string"));
        }
        self.i += 1;
        let mut out = Vec::new();
        loop {
            let Some(&c) = self.s.get(self.i) else {
                return Err(self.err("unterminated string"));
            };
            self.i += 1;
            match c {
                b'"' => return String::from_utf8(out).map_err(|_| self.err("invalid UTF-8")),
                b'\\' => {
                    let Some(&e) = self.s.get(self.i) else {
                        return Err(self.err("unterminated escape"));
                    };
                    self.i += 1;
                    match e {
                        b'"' => out.push(b'"'),
                        b'\\' => out.push(b'\\'),
                        b'/' => out.push(b'/'),
                        b'n' => out.push(b'\n'),
                        b'r' => out.push(b'\r'),
                        b't' => out.push(b'\t'),
                        b'b' => out.push(8),
                        b'f' => out.push(12),
                        b'u' => {
                            let hex = self
                                .s
                                .get(self.i..self.i + 4)
                                .and_then(|h| std::str::from_utf8(h).ok())
                                .and_then(|h| u32::from_str_radix(h, 16).ok())
                                .and_then(char::from_u32)
                                .ok_or_else(|| self.err("bad \\u escape"))?;
                            self.i += 4;
                            let mut b = [0; 4];
                            out.extend_from_slice(hex.encode_utf8(&mut b).as_bytes());
                        }
                        _ => return Err(self.err("bad escape")),
                    }
                }
                c => out.push(c),
            }
        }
    }
}

pub fn hex(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for x in b {
        let _ = write!(s, "{x:02x}");
    }
    s
}

pub fn unhex(s: &str) -> Option<Vec<u8>> {
    // `from_str_radix` alone would take a sign: `"+f"` is 15.
    if !s.len().is_multiple_of(2) || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
        .collect()
}

// --- Building and reading objects -------------------------------------------

fn obj(fields: Vec<(&str, Value)>) -> Value {
    Value::Obj(fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

fn n(v: impl Into<i64>) -> Value {
    Value::Num(v.into())
}

fn big(v: u64) -> Value {
    Value::Str(v.to_string())
}

fn s(v: &str) -> Value {
    Value::Str(v.to_owned())
}

fn b(v: &[u8]) -> Value {
    Value::Str(hex(v))
}

fn u32s(v: &[u32]) -> Value {
    Value::Arr(v.iter().map(|x| n(*x)).collect())
}

/// Reads fields of one object with a path for error messages.
struct Fields<'a> {
    v: &'a Value,
    path: &'a str,
}

impl<'a> Fields<'a> {
    fn new(v: &'a Value, path: &'a str) -> Result<Self, String> {
        match v {
            Value::Obj(_) => Ok(Self { v, path }),
            _ => Err(format!("{path}: expected an object")),
        }
    }

    fn get(&self, k: &str) -> Result<&'a Value, String> {
        self.v
            .get(k)
            .ok_or_else(|| format!("{}: missing field {k}", self.path))
    }

    fn num<T: TryFrom<i64>>(&self, k: &str) -> Result<T, String> {
        self.get(k)?
            .as_i64()
            .and_then(|x| T::try_from(x).ok())
            .ok_or_else(|| format!("{}.{k}: expected an integer in range", self.path))
    }

    fn big(&self, k: &str) -> Result<u64, String> {
        self.get(k)?
            .as_str()
            .and_then(|x| x.parse().ok())
            .ok_or_else(|| format!("{}.{k}: expected a decimal string", self.path))
    }

    fn str(&self, k: &str) -> Result<String, String> {
        self.get(k)?
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| format!("{}.{k}: expected a string", self.path))
    }

    fn bytes(&self, k: &str) -> Result<Vec<u8>, String> {
        self.get(k)?
            .as_str()
            .and_then(unhex)
            .ok_or_else(|| format!("{}.{k}: expected a hex string", self.path))
    }

    fn arr(&self, k: &str) -> Result<&'a [Value], String> {
        self.get(k)?
            .as_array()
            .ok_or_else(|| format!("{}.{k}: expected an array", self.path))
    }

    fn u32s(&self, k: &str) -> Result<Vec<u32>, String> {
        self.arr(k)?
            .iter()
            .map(|x| {
                x.as_i64()
                    .and_then(|x| u32::try_from(x).ok())
                    .ok_or_else(|| format!("{}.{k}: expected u32 values", self.path))
            })
            .collect()
    }
}

pub fn message_to_json(m: &Message) -> Value {
    obj(vec![
        ("key", m.key.as_deref().map_or(Value::Null, b)),
        (
            "headers",
            Value::Arr(
                m.headers
                    .iter()
                    .map(|h| obj(vec![("name", b(&h.name)), ("value", b(&h.value))]))
                    .collect(),
            ),
        ),
        ("body", b(&m.body)),
    ])
}

pub fn message_from_json(v: &Value) -> Result<Message, String> {
    let f = Fields::new(v, "message")?;
    let key = match f.get("key")? {
        Value::Null => None,
        _ => Some(f.bytes("key")?),
    };
    let headers = f
        .arr("headers")?
        .iter()
        .map(|h| {
            let h = Fields::new(h, "header")?;
            Ok(Header {
                name: h.bytes("name")?,
                value: h.bytes("value")?,
            })
        })
        .collect::<Result<_, String>>()?;
    Ok(Message {
        key,
        headers,
        body: f.bytes("body")?,
    })
}

/// `{"type": <name>, "body": {...}}`.
pub fn request_to_json(r: &Request) -> Value {
    let body = match r {
        Request::Hello(h) => obj(vec![
            ("min_version", n(h.min_version)),
            ("max_version", n(h.max_version)),
            ("client_name", s(&h.client_name)),
            ("auth_mechanism", s(&h.auth_mechanism)),
            ("auth_payload", b(&h.auth_payload)),
        ]),
        Request::Ping { nonce } => obj(vec![("nonce", big(*nonce))]),
        Request::Metadata { topics } => obj(vec![(
            "topics",
            Value::Arr(topics.iter().map(|t| s(t)).collect()),
        )]),
        Request::CreateTopic(c) => obj(vec![
            ("name", s(&c.name)),
            ("partitions", n(c.partitions)),
            ("replication_factor", n(c.replication_factor)),
            ("retention_ms", big(c.retention_ms)),
        ]),
        Request::DeleteTopic { name } | Request::DescribeTopic { name } => {
            obj(vec![("name", s(name))])
        }
        Request::Produce(p) => obj(vec![
            ("topic", s(&p.topic)),
            ("partition", n(p.partition)),
            ("producer_id", big(p.producer_id)),
            ("producer_seq", big(p.producer_seq)),
            ("ack_level", n(p.ack_level)),
            ("expected_leader_epoch", n(p.expected_leader_epoch)),
            ("timeout_ms", n(p.timeout_ms)),
            (
                "messages",
                Value::Arr(p.messages.iter().map(message_to_json).collect()),
            ),
        ]),
        Request::Fetch(f) => obj(vec![
            ("topic", s(&f.topic)),
            ("partition", n(f.partition)),
            ("from_seq", big(f.from_seq)),
            ("max_bytes", n(f.max_bytes)),
            ("max_wait_ms", n(f.max_wait_ms)),
            ("expected_leader_epoch", n(f.expected_leader_epoch)),
        ]),
        Request::ListTopics { cursor, limit } => {
            obj(vec![("cursor", s(cursor)), ("limit", n(*limit))])
        }
        Request::BrokerStats => obj(vec![]),
    };
    obj(vec![("type", s(r.name())), ("body", body)])
}

pub fn request_from_json(v: &Value) -> Result<Request, String> {
    let top = Fields::new(v, "request")?;
    let ty = top.str("type")?;
    let empty = Value::Obj(Vec::new());
    let body = v.get("body").unwrap_or(&empty);
    let f = Fields::new(body, "body")?;
    Ok(match ty.as_str() {
        "Hello" => Request::Hello(Hello {
            min_version: f.num("min_version")?,
            max_version: f.num("max_version")?,
            client_name: f.str("client_name")?,
            auth_mechanism: f.str("auth_mechanism")?,
            auth_payload: f.bytes("auth_payload")?,
        }),
        "Ping" => Request::Ping {
            nonce: f.big("nonce")?,
        },
        "Metadata" => Request::Metadata {
            topics: f
                .arr("topics")?
                .iter()
                .map(|t| {
                    t.as_str()
                        .map(str::to_owned)
                        .ok_or("body.topics: expected strings".to_owned())
                })
                .collect::<Result<_, _>>()?,
        },
        "CreateTopic" => Request::CreateTopic(CreateTopic {
            name: f.str("name")?,
            partitions: f.num("partitions")?,
            replication_factor: f.num("replication_factor")?,
            retention_ms: f.big("retention_ms")?,
        }),
        "DeleteTopic" => Request::DeleteTopic {
            name: f.str("name")?,
        },
        "Produce" => Request::Produce(Produce {
            topic: f.str("topic")?,
            partition: f.num("partition")?,
            producer_id: f.big("producer_id")?,
            producer_seq: f.big("producer_seq")?,
            ack_level: f.num("ack_level")?,
            expected_leader_epoch: f.num("expected_leader_epoch")?,
            timeout_ms: f.num("timeout_ms")?,
            messages: f
                .arr("messages")?
                .iter()
                .map(message_from_json)
                .collect::<Result<_, _>>()?,
        }),
        "Fetch" => Request::Fetch(Fetch {
            topic: f.str("topic")?,
            partition: f.num("partition")?,
            from_seq: f.big("from_seq")?,
            max_bytes: f.num("max_bytes")?,
            max_wait_ms: f.num("max_wait_ms")?,
            expected_leader_epoch: f.num("expected_leader_epoch")?,
        }),
        "ListTopics" => Request::ListTopics {
            cursor: f.str("cursor")?,
            limit: f.num("limit")?,
        },
        "DescribeTopic" => Request::DescribeTopic {
            name: f.str("name")?,
        },
        "BrokerStats" => Request::BrokerStats,
        other => return Err(format!("unknown request type {other}")),
    })
}

fn code_to_json(code: u16) -> Value {
    match ErrorCode::from_u16(code) {
        Some(c) => s(c.name()),
        None => n(code),
    }
}

fn code_from_json(v: &Value, path: &str) -> Result<u16, String> {
    match v {
        Value::Str(name) => ErrorCode::from_name(name)
            .map(|c| c as u16)
            .ok_or_else(|| format!("{path}: unknown error code {name}")),
        Value::Num(x) => u16::try_from(*x).map_err(|_| format!("{path}: code out of range")),
        _ => Err(format!("{path}: expected an error code")),
    }
}

/// `{"type": <name>, "body": {...}}`. A topic's `error` in Metadata is 0 or
/// an error code name.
pub fn response_to_json(r: &Response) -> Value {
    let body = match r {
        Response::HelloOk(h) => obj(vec![
            ("version", n(h.version)),
            ("cluster_id", s(&h.cluster_id)),
            ("node_id", n(h.node_id)),
            ("max_frame_bytes", n(h.max_frame_bytes)),
            ("max_message_bytes", n(h.max_message_bytes)),
        ]),
        Response::Pong { nonce } => obj(vec![("nonce", big(*nonce))]),
        Response::MetadataOk(m) => obj(vec![
            ("cluster_id", s(&m.cluster_id)),
            ("controller_node_id", n(m.controller_node_id)),
            (
                "nodes",
                Value::Arr(
                    m.nodes
                        .iter()
                        .map(|x| {
                            obj(vec![
                                ("node_id", n(x.node_id)),
                                ("host", s(&x.host)),
                                ("port", n(x.port)),
                                ("rack", s(&x.rack)),
                            ])
                        })
                        .collect(),
                ),
            ),
            (
                "topics",
                Value::Arr(
                    m.topics
                        .iter()
                        .map(|t| {
                            obj(vec![
                                ("name", s(&t.name)),
                                (
                                    "error",
                                    if t.error == 0 {
                                        n(0)
                                    } else {
                                        code_to_json(t.error)
                                    },
                                ),
                                (
                                    "partitions",
                                    Value::Arr(
                                        t.partitions
                                            .iter()
                                            .map(|p| {
                                                obj(vec![
                                                    ("partition", n(p.partition)),
                                                    ("leader_node_id", n(p.leader_node_id)),
                                                    ("leader_epoch", n(p.leader_epoch)),
                                                    ("replicas", u32s(&p.replicas)),
                                                    ("in_sync_replicas", u32s(&p.in_sync_replicas)),
                                                    ("start_seq", big(p.start_seq)),
                                                    ("next_seq", big(p.next_seq)),
                                                ])
                                            })
                                            .collect(),
                                    ),
                                ),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]),
        Response::CreateTopicOk | Response::DeleteTopicOk => obj(vec![]),
        Response::ProduceOk(p) => obj(vec![
            ("base_seq", big(p.base_seq)),
            ("count", n(p.count)),
            ("leader_epoch", n(p.leader_epoch)),
            ("timestamp_ms", big(p.timestamp_ms)),
        ]),
        Response::FetchOk(f) => obj(vec![
            ("leader_epoch", n(f.leader_epoch)),
            ("start_seq", big(f.start_seq)),
            ("next_seq", big(f.next_seq)),
            (
                "messages",
                Value::Arr(
                    f.messages
                        .iter()
                        .map(|m| {
                            let Value::Obj(mut fields) = message_to_json(&m.message) else {
                                return Value::Null;
                            };
                            fields.insert(0, ("timestamp_ms".into(), big(m.timestamp_ms)));
                            fields.insert(0, ("seq".into(), big(m.seq)));
                            Value::Obj(fields)
                        })
                        .collect(),
                ),
            ),
        ]),
        Response::ListTopicsOk {
            topics,
            next_cursor,
        } => obj(vec![
            (
                "topics",
                Value::Arr(
                    topics
                        .iter()
                        .map(|t| obj(vec![("name", s(&t.name)), ("partitions", n(t.partitions))]))
                        .collect(),
                ),
            ),
            ("next_cursor", s(next_cursor)),
        ]),
        Response::DescribeTopicOk(d) => obj(vec![
            ("name", s(&d.name)),
            ("replication_factor", n(d.replication_factor)),
            ("retention_ms", big(d.retention_ms)),
            (
                "partitions",
                Value::Arr(
                    d.partitions
                        .iter()
                        .map(|p| {
                            obj(vec![
                                ("partition", n(p.partition)),
                                ("start_seq", big(p.start_seq)),
                                ("next_seq", big(p.next_seq)),
                                ("message_count", big(p.message_count)),
                                ("index_bytes", big(p.index_bytes)),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]),
        Response::BrokerStatsOk(x) => obj(vec![
            ("node_id", n(x.node_id)),
            ("open_connections", n(x.open_connections)),
            ("pending_bytes", big(x.pending_bytes)),
            ("pending_bytes_peak", big(x.pending_bytes_peak)),
            ("max_pending_bytes", big(x.max_pending_bytes)),
            ("fetch_waiters", n(x.fetch_waiters)),
            ("produce_requests", big(x.produce_requests)),
            ("groups_committed", big(x.groups_committed)),
            ("fsyncs", big(x.fsyncs)),
            ("messages_accepted", big(x.messages_accepted)),
            ("messages_deduplicated", big(x.messages_deduplicated)),
            ("body_bytes_received", big(x.body_bytes_received)),
            ("body_bytes_new", big(x.body_bytes_new)),
            ("wal_bytes", big(x.wal_bytes)),
            ("segment_bytes", big(x.segment_bytes)),
            ("index_bytes", big(x.index_bytes)),
        ]),
        Response::Error(e) => obj(vec![
            ("code", code_to_json(e.code)),
            ("message", s(&e.message)),
            ("leader_node_id", n(e.leader_node_id)),
            ("leader_epoch", n(e.leader_epoch)),
            ("retry_after_ms", n(e.retry_after_ms)),
            ("min_version", n(e.min_version)),
            ("max_version", n(e.max_version)),
        ]),
    };
    obj(vec![("type", s(r.name())), ("body", body)])
}

pub fn response_from_json(v: &Value) -> Result<Response, String> {
    let top = Fields::new(v, "response")?;
    let ty = top.str("type")?;
    let empty = Value::Obj(Vec::new());
    let body = v.get("body").unwrap_or(&empty);
    let f = Fields::new(body, "body")?;
    Ok(match ty.as_str() {
        "HelloOk" => Response::HelloOk(HelloOk {
            version: f.num("version")?,
            cluster_id: f.str("cluster_id")?,
            node_id: f.num("node_id")?,
            max_frame_bytes: f.num("max_frame_bytes")?,
            max_message_bytes: f.num("max_message_bytes")?,
        }),
        "Pong" => Response::Pong {
            nonce: f.big("nonce")?,
        },
        "MetadataOk" => Response::MetadataOk(MetadataOk {
            cluster_id: f.str("cluster_id")?,
            controller_node_id: f.num("controller_node_id")?,
            nodes: f
                .arr("nodes")?
                .iter()
                .map(|x| {
                    let x = Fields::new(x, "node")?;
                    Ok(Node {
                        node_id: x.num("node_id")?,
                        host: x.str("host")?,
                        port: x.num("port")?,
                        rack: x.str("rack")?,
                    })
                })
                .collect::<Result<_, String>>()?,
            topics: f
                .arr("topics")?
                .iter()
                .map(|t| {
                    let t = Fields::new(t, "topic")?;
                    Ok(TopicMetadata {
                        name: t.str("name")?,
                        error: code_from_json(t.get("error")?, "topic.error")?,
                        partitions: t
                            .arr("partitions")?
                            .iter()
                            .map(|p| {
                                let p = Fields::new(p, "partition")?;
                                Ok(PartitionMetadata {
                                    partition: p.num("partition")?,
                                    leader_node_id: p.num("leader_node_id")?,
                                    leader_epoch: p.num("leader_epoch")?,
                                    replicas: p.u32s("replicas")?,
                                    in_sync_replicas: p.u32s("in_sync_replicas")?,
                                    start_seq: p.big("start_seq")?,
                                    next_seq: p.big("next_seq")?,
                                })
                            })
                            .collect::<Result<_, String>>()?,
                    })
                })
                .collect::<Result<_, String>>()?,
        }),
        "CreateTopicOk" => Response::CreateTopicOk,
        "DeleteTopicOk" => Response::DeleteTopicOk,
        "ProduceOk" => Response::ProduceOk(ProduceOk {
            base_seq: f.big("base_seq")?,
            count: f.num("count")?,
            leader_epoch: f.num("leader_epoch")?,
            timestamp_ms: f.big("timestamp_ms")?,
        }),
        "FetchOk" => Response::FetchOk(FetchOk {
            leader_epoch: f.num("leader_epoch")?,
            start_seq: f.big("start_seq")?,
            next_seq: f.big("next_seq")?,
            messages: f
                .arr("messages")?
                .iter()
                .map(|m| {
                    let mf = Fields::new(m, "message")?;
                    Ok(FetchedMessage {
                        seq: mf.big("seq")?,
                        timestamp_ms: mf.big("timestamp_ms")?,
                        message: message_from_json(m)?,
                    })
                })
                .collect::<Result<_, String>>()?,
        }),
        "ListTopicsOk" => Response::ListTopicsOk {
            topics: f
                .arr("topics")?
                .iter()
                .map(|t| {
                    let t = Fields::new(t, "topic")?;
                    Ok(TopicSummary {
                        name: t.str("name")?,
                        partitions: t.num("partitions")?,
                    })
                })
                .collect::<Result<_, String>>()?,
            next_cursor: f.str("next_cursor")?,
        },
        "DescribeTopicOk" => Response::DescribeTopicOk(DescribeTopicOk {
            name: f.str("name")?,
            replication_factor: f.num("replication_factor")?,
            retention_ms: f.big("retention_ms")?,
            partitions: f
                .arr("partitions")?
                .iter()
                .map(|p| {
                    let p = Fields::new(p, "partition")?;
                    Ok(PartitionDescription {
                        partition: p.num("partition")?,
                        start_seq: p.big("start_seq")?,
                        next_seq: p.big("next_seq")?,
                        message_count: p.big("message_count")?,
                        index_bytes: p.big("index_bytes")?,
                    })
                })
                .collect::<Result<_, String>>()?,
        }),
        "BrokerStatsOk" => Response::BrokerStatsOk(BrokerStatsOk {
            node_id: f.num("node_id")?,
            open_connections: f.num("open_connections")?,
            pending_bytes: f.big("pending_bytes")?,
            pending_bytes_peak: f.big("pending_bytes_peak")?,
            max_pending_bytes: f.big("max_pending_bytes")?,
            fetch_waiters: f.num("fetch_waiters")?,
            produce_requests: f.big("produce_requests")?,
            groups_committed: f.big("groups_committed")?,
            fsyncs: f.big("fsyncs")?,
            messages_accepted: f.big("messages_accepted")?,
            messages_deduplicated: f.big("messages_deduplicated")?,
            body_bytes_received: f.big("body_bytes_received")?,
            body_bytes_new: f.big("body_bytes_new")?,
            wal_bytes: f.big("wal_bytes")?,
            segment_bytes: f.big("segment_bytes")?,
            index_bytes: f.big("index_bytes")?,
        }),
        "Error" => Response::Error(ErrorBody {
            code: code_from_json(f.get("code")?, "body.code")?,
            message: f.str("message")?,
            leader_node_id: f.num("leader_node_id")?,
            leader_epoch: f.num("leader_epoch")?,
            retry_after_ms: f.num("retry_after_ms")?,
            min_version: f.num("min_version")?,
            max_version: f.num("max_version")?,
        }),
        other => return Err(format!("unknown response type {other}")),
    })
}
