//! The reference Straum client: blocking, over `std::net`, one request at a
//! time per `Client` (pipelining is available on `Connection`). It
//! implements the normative client behavior of docs/protocol.md section 6:
//! leader errors refresh Metadata and retry at the leader, `Overloaded`
//! backs off, a lost connection is reopened and the request retried with
//! the same producer id and seq.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

pub use straum_proto as proto;
use straum_proto::{
    BrokerStatsOk, CreateTopic, DescribeTopicOk, ErrorBody, ErrorCode, Fetch, FetchOk, FrameHeader,
    Hello, HelloOk, Message, MetadataOk, Produce, ProduceOk, Request, Response, TopicSummary, ack,
};

#[derive(Debug)]
pub enum ClientError {
    Io(io::Error),
    /// The broker answered with an error the client does not retry, or a
    /// retryable one after the last attempt.
    Broker(ErrorBody),
    /// A response that does not decode or does not fit the request.
    Protocol(String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(e) => write!(f, "{e}"),
            Self::Broker(e) => match e.code() {
                Some(c) => write!(f, "{}: {}", c.name(), e.message),
                None => write!(f, "error code {}: {}", e.code, e.message),
            },
            Self::Protocol(m) => write!(f, "protocol: {m}"),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<io::Error> for ClientError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl ClientError {
    pub fn code(&self) -> Option<ErrorCode> {
        match self {
            Self::Broker(e) => e.code(),
            _ => None,
        }
    }
}

/// One TCP connection: frames out and in, no retries. Requests may be
/// pipelined: `send` several, then `recv` their responses in any order.
pub struct Connection {
    stream: TcpStream,
    next_correlation: u64,
}

impl Connection {
    pub fn connect(addr: &str, timeout: Duration) -> io::Result<Self> {
        let mut last = io::Error::new(io::ErrorKind::NotFound, format!("{addr}: no address"));
        for a in addr.to_socket_addrs()? {
            match TcpStream::connect_timeout(&a, timeout) {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    return Ok(Self {
                        stream,
                        next_correlation: 1,
                    });
                }
                Err(e) => last = e,
            }
        }
        Err(last)
    }

    pub fn set_read_timeout(&self, t: Option<Duration>) -> io::Result<()> {
        self.stream.set_read_timeout(t)
    }

    /// Sends `req` with a fresh correlation id and returns it.
    pub fn send(&mut self, req: &Request) -> io::Result<u64> {
        let id = self.next_correlation;
        self.next_correlation += 1;
        self.send_with(id, req)?;
        Ok(id)
    }

    pub fn send_with(&mut self, correlation_id: u64, req: &Request) -> io::Result<()> {
        let mut buf = Vec::new();
        proto::encode_request(&mut buf, correlation_id, req);
        self.stream.write_all(&buf)
    }

    /// Sends bytes as they are, for tests of malformed frames.
    pub fn send_raw(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes)
    }

    /// The next response: `(correlation id, response)`.
    pub fn recv(&mut self) -> Result<(u64, Response), ClientError> {
        let mut head = [0; proto::FRAME_HEADER];
        self.stream.read_exact(&mut head)?;
        let h = FrameHeader::parse(&head);
        let len = h
            .body_len()
            .ok_or_else(|| ClientError::Protocol("frame length below 10".into()))?;
        let mut body = vec![0; len];
        self.stream.read_exact(&mut body)?;
        let resp = Response::decode(h.msg_type, &body).map_err(|e| {
            ClientError::Protocol(format!("response type {:#06x}: {e}", h.msg_type))
        })?;
        Ok((h.correlation_id, resp))
    }

    /// Sends `req` and waits for its response (no other request outstanding).
    pub fn call(&mut self, req: &Request) -> Result<Response, ClientError> {
        let id = self.send(req)?;
        loop {
            let (got, resp) = self.recv()?;
            if got == id {
                return Ok(resp);
            }
        }
    }

    pub fn shutdown(&self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// `host:port` of any broker of the cluster.
    pub bootstrap: String,
    pub client_name: String,
    /// Attempts of one request in total, retries included (section 6).
    pub max_attempts: u32,
    pub backoff_min: Duration,
    pub backoff_max: Duration,
    pub connect_timeout: Duration,
    /// Longest wait for one response; `None` waits forever.
    pub request_timeout: Option<Duration>,
}

impl ClientConfig {
    pub fn new(bootstrap: impl Into<String>) -> Self {
        Self {
            bootstrap: bootstrap.into(),
            client_name: "straum-client".into(),
            max_attempts: 5,
            backoff_min: Duration::from_millis(10),
            backoff_max: Duration::from_secs(1),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Some(Duration::from_secs(60)),
        }
    }
}

/// Options of one Produce beyond its messages.
#[derive(Clone, Copy, Debug)]
pub struct ProduceOptions {
    pub producer_id: u64,
    pub producer_seq: u64,
    pub ack_level: u8,
}

impl Default for ProduceOptions {
    fn default() -> Self {
        Self {
            producer_id: 0,
            producer_seq: 0,
            ack_level: ack::DURABLE,
        }
    }
}

/// A client with the normative retry behavior. Not thread-safe: one per
/// thread.
pub struct Client {
    config: ClientConfig,
    conn: Option<(Connection, HelloOk)>,
    /// Where the current connection goes.
    target: String,
    /// node id → `host:port`, from the last Metadata.
    nodes: HashMap<u32, String>,
    /// (topic, partition) → (leader, epoch), from the last Metadata.
    leaders: HashMap<(String, u32), (u32, u32)>,
    partition_counts: HashMap<String, u32>,
    round_robin: u32,
}

impl Client {
    pub fn connect(config: ClientConfig) -> Result<Self, ClientError> {
        let mut c = Self {
            target: config.bootstrap.clone(),
            config,
            conn: None,
            nodes: HashMap::new(),
            leaders: HashMap::new(),
            partition_counts: HashMap::new(),
            round_robin: 0,
        };
        c.connection()?;
        Ok(c)
    }

    /// The `HelloOk` of the current connection.
    pub fn hello(&mut self) -> Result<HelloOk, ClientError> {
        Ok(self.connection()?.1.clone())
    }

    fn connection(&mut self) -> Result<&mut (Connection, HelloOk), ClientError> {
        if self.conn.is_none() {
            let mut conn = Connection::connect(&self.target, self.config.connect_timeout)?;
            conn.set_read_timeout(self.config.request_timeout)?;
            let resp = conn.call(&Request::Hello(Hello {
                min_version: proto::VERSION,
                max_version: proto::VERSION,
                client_name: self.config.client_name.clone(),
                auth_mechanism: "none".into(),
                auth_payload: Vec::new(),
            }))?;
            match resp {
                Response::HelloOk(ok) => self.conn = Some((conn, ok)),
                Response::Error(e) => return Err(ClientError::Broker(e)),
                r => {
                    return Err(ClientError::Protocol(format!(
                        "Hello answered with {}",
                        r.name()
                    )));
                }
            }
        }
        self.conn
            .as_mut()
            .ok_or_else(|| ClientError::Protocol("no connection".into()))
    }

    /// One request with no retries, on the current connection; a lost
    /// connection is dropped so the next call reconnects.
    fn once(&mut self, req: &Request) -> Result<Response, ClientError> {
        let res = self.connection()?.0.call(req);
        if let Err(ClientError::Io(_)) = &res {
            self.conn = None;
        }
        res
    }

    pub fn metadata(&mut self, topics: &[&str]) -> Result<MetadataOk, ClientError> {
        let req = Request::Metadata {
            topics: topics.iter().map(|t| (*t).to_owned()).collect(),
        };
        match self.retrying(None, |_| req.clone())? {
            Response::MetadataOk(m) => {
                self.learn(&m);
                Ok(m)
            }
            r => Err(unexpected(&r)),
        }
    }

    fn learn(&mut self, m: &MetadataOk) {
        for n in &m.nodes {
            self.nodes
                .insert(n.node_id, format!("{}:{}", n.host, n.port));
        }
        for t in m.topics.iter().filter(|t| t.error == 0) {
            self.partition_counts
                .insert(t.name.clone(), t.partitions.len() as u32);
            for p in &t.partitions {
                self.leaders.insert(
                    (t.name.clone(), p.partition),
                    (p.leader_node_id, p.leader_epoch),
                );
            }
        }
    }

    pub fn create_topic(
        &mut self,
        name: &str,
        partitions: u32,
        retention_ms: u64,
    ) -> Result<(), ClientError> {
        let req = Request::CreateTopic(CreateTopic {
            name: name.into(),
            partitions,
            replication_factor: 1,
            retention_ms,
        });
        match self.retrying(None, |_| req.clone())? {
            Response::CreateTopicOk => Ok(()),
            r => Err(unexpected(&r)),
        }
    }

    pub fn list_topics(&mut self) -> Result<Vec<TopicSummary>, ClientError> {
        match self.retrying(None, |_| Request::ListTopics)? {
            Response::ListTopicsOk { topics } => Ok(topics),
            r => Err(unexpected(&r)),
        }
    }

    pub fn describe_topic(&mut self, name: &str) -> Result<DescribeTopicOk, ClientError> {
        let req = Request::DescribeTopic { name: name.into() };
        match self.retrying(None, |_| req.clone())? {
            Response::DescribeTopicOk(d) => Ok(d),
            r => Err(unexpected(&r)),
        }
    }

    pub fn broker_stats(&mut self) -> Result<BrokerStatsOk, ClientError> {
        match self.retrying(None, |_| Request::BrokerStats)? {
            Response::BrokerStatsOk(s) => Ok(s),
            r => Err(unexpected(&r)),
        }
    }

    pub fn ping(&mut self, nonce: u64) -> Result<u64, ClientError> {
        match self.once(&Request::Ping { nonce })? {
            Response::Pong { nonce } => Ok(nonce),
            r => Err(unexpected(&r)),
        }
    }

    /// Produces `messages` to one partition. Retries keep `producer_id` and
    /// `producer_seq` (section 6).
    pub fn produce(
        &mut self,
        topic: &str,
        partition: u32,
        messages: Vec<Message>,
        opts: ProduceOptions,
    ) -> Result<ProduceOk, ClientError> {
        let build = |epoch: u32| {
            Request::Produce(Produce {
                topic: topic.into(),
                partition,
                producer_id: opts.producer_id,
                producer_seq: opts.producer_seq,
                ack_level: opts.ack_level,
                expected_leader_epoch: epoch,
                timeout_ms: 0,
                messages: messages.clone(),
            })
        };
        match self.retrying(Some((topic, partition)), build)? {
            Response::ProduceOk(ok) => Ok(ok),
            r => Err(unexpected(&r)),
        }
    }

    /// The partition the default partitioner picks for `key`
    /// (docs/protocol.md section 7), or round robin without a key. Needs the
    /// topic's partition count, from Metadata if not known yet.
    pub fn partition_for(&mut self, topic: &str, key: Option<&[u8]>) -> Result<u32, ClientError> {
        let count = match self.partition_counts.get(topic) {
            Some(&n) => n,
            None => {
                let m = self.metadata(&[topic])?;
                match m.topics.first() {
                    Some(t) if t.error != 0 => {
                        return Err(ClientError::Broker(ErrorBody {
                            code: t.error,
                            message: topic.into(),
                            ..ErrorBody::default()
                        }));
                    }
                    Some(t) => t.partitions.len() as u32,
                    None => 0,
                }
            }
        };
        Ok(match key {
            Some(k) => proto::partition_for_key(k, count),
            None => {
                self.round_robin = self.round_robin.wrapping_add(1);
                self.round_robin.checked_rem(count).unwrap_or(0)
            }
        })
    }

    pub fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        from_seq: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    ) -> Result<FetchOk, ClientError> {
        let build = |epoch: u32| {
            Request::Fetch(Fetch {
                topic: topic.into(),
                partition,
                from_seq,
                max_bytes,
                max_wait_ms,
                expected_leader_epoch: epoch,
            })
        };
        match self.retrying(Some((topic, partition)), build)? {
            Response::FetchOk(f) => Ok(f),
            r => Err(unexpected(&r)),
        }
    }

    /// Section 6. `build(epoch)` makes the request with
    /// `expected_leader_epoch = epoch` (0 until Metadata reported one).
    fn retrying(
        &mut self,
        partition: Option<(&str, u32)>,
        build: impl Fn(u32) -> Request,
    ) -> Result<Response, ClientError> {
        let mut epoch = 0;
        let mut backoff = self.config.backoff_min;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let last = attempt >= self.config.max_attempts.max(1);
            match self.once(&build(epoch)) {
                Err(ClientError::Io(e)) => {
                    // Rule 3: reconnect (next call) and retry the same request.
                    if last {
                        return Err(ClientError::Io(e));
                    }
                }
                Ok(Response::Error(e)) => match e.code() {
                    Some(ErrorCode::NotLeader | ErrorCode::FencedLeaderEpoch) if !last => {
                        // Rule 1: refresh Metadata, go to the leader, retry with its epoch.
                        if let Some((topic, p)) = partition {
                            epoch = self.refresh_leader(topic, p)?;
                        }
                    }
                    Some(ErrorCode::Overloaded) if !last => {
                        // Rule 2: back off, at least retry_after_ms.
                        let wait = backoff.max(Duration::from_millis(u64::from(e.retry_after_ms)));
                        std::thread::sleep(wait);
                        backoff = (backoff * 2).min(self.config.backoff_max);
                    }
                    _ => return Err(ClientError::Broker(e)),
                },
                Ok(r) => return Ok(r),
                Err(e) => return Err(e),
            }
        }
    }

    /// Metadata of `topic` on the current connection; switches the
    /// connection to the partition's leader if it is another node. Returns
    /// the leader epoch.
    fn refresh_leader(&mut self, topic: &str, partition: u32) -> Result<u32, ClientError> {
        let resp = self.once(&Request::Metadata {
            topics: vec![topic.into()],
        })?;
        let Response::MetadataOk(m) = resp else {
            return Err(unexpected(&resp));
        };
        self.learn(&m);
        let (leader, epoch) = self
            .leaders
            .get(&(topic.to_owned(), partition))
            .copied()
            .ok_or_else(|| {
                ClientError::Protocol(format!("Metadata has no leader for {topic}/{partition}"))
            })?;
        let current = self.conn.as_ref().map(|(_, h)| h.node_id);
        if current != Some(leader) {
            let addr = self.nodes.get(&leader).cloned().ok_or_else(|| {
                ClientError::Protocol(format!("Metadata has no address for node {leader}"))
            })?;
            if let Some((c, _)) = self.conn.take() {
                c.shutdown();
            }
            self.target = addr;
        }
        Ok(epoch)
    }
}

fn unexpected(r: &Response) -> ClientError {
    match r {
        Response::Error(e) => ClientError::Broker(e.clone()),
        r => ClientError::Protocol(format!("unexpected {}", r.name())),
    }
}
