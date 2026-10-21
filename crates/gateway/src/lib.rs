//! The gateway, sans-IO (ADR-0008 step 3b-2; unified-stream §2.1).
//!
//! Every node runs one. It owns its MQTT connections and nothing else: a
//! packet becomes a [`Proposal`] for the log, and only a committed record's
//! [`Effect`] becomes a PUBACK, a SUBACK, an UNSUBACK or a delivery
//! (ADR-0005, invariant 4). What is local to a connection stays here —
//! keep-alive, topic aliases, the client's receive maximum, packet
//! identifiers — and is not replicated (ADR-0003).
//!
//! Bytes arrive with [`Gateway::received`], effects with [`Gateway::effect`],
//! time with [`Gateway::tick`]; what to do comes back from
//! [`Gateway::take_actions`]. Time is a monotonic `Duration` the driver
//! passes in: nothing here reads a clock.
//!
//! Bounded:
//!
//! * connections, and lifecycle proposals awaiting their effect — at either
//!   bound new connections are refused;
//! * per connection the proposals awaiting commit, in messages and bytes,
//!   and all connections' together — beyond them the gateway stops reading
//!   the connection ([`Gateway::wants_read`]); no proposal is dropped;
//! * topic aliases, by the maximum advertised;
//! * deliveries waiting for the client's receive maximum: QoS 1 ones by
//!   `apply`'s inflight window, the at-most-once ones behind them per
//!   connection in messages and bytes and in total — beyond, an at-most-once
//!   delivery is dropped and counted ([`Load::dropped`]).
//!
//! What a lost proposal costs: a publish, subscribe or unsubscribe that is
//! not committed within `commit_timeout` closes its connection, and the
//! client resends after reconnecting (at-least-once). An `Ack`, a
//! `SessionGone` and the gateway's own `NodeReset` are sent again every
//! `retry_interval` until their effect is seen; applying one twice changes
//! nothing.
//!
//! Not yet (ADR-0008 step 7c): sessions that outlive their connection,
//! wills, session expiry, takeover across gateways. A CONNECT asking for
//! any of the first three — a will, a 3.1.1 CleanSession=0, an MQTT 5
//! session expiry above zero — is refused (`ImplementationSpecificError`,
//! 3.1.1 "server unavailable"), so no client believes this server keeps
//! them (D-78 A, step 7a). Also not yet: enhanced authentication; message
//! properties through the log (D-65).
//!
//! Credentials (D-110 A, D-158 A): with [`Config::authenticate`] set, a
//! CONNECT is answered only after the driver has checked its user name and
//! password off the node loop ([`Action::Authenticate`],
//! [`Gateway::authenticated`]): a password hash is slow by design. The
//! [`Grant`] it hands back decides each PUBLISH and SUBSCRIBE filter before
//! anything is proposed, so a refused one writes nothing to the log.

mod properties;

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::{NonZeroU16, NonZeroU32};
use std::time::Duration;

use apply::{Effect, Options, Origin, Proposal, RetainHandling as Handling};
use bytes::{Bytes, BytesMut};
use codec::{
    AckProperties, ConnAck, ConnAckProperties, Disconnect, DisconnectProperties, Packet, PacketId,
    PubAck, Publish, Qos, ReasonCode, RetainHandling, Sender, SubAck, UnsubAck, Version,
};
use domain::{ClientId, DeliveryGuarantee, Topic, TopicFilter};

pub type ConnId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Config {
    pub node: u8,
    /// Sessions of this incarnation are numbered from here up. The driver
    /// keeps it durable and growing across restarts: a number is never
    /// reused, and everything below it belongs to earlier incarnations,
    /// which this gateway's `NodeReset` clears.
    pub first_session: u64,
    pub codec: codec::Limits,
    pub max_connections: usize,
    pub connect_timeout: Duration,
    /// Proposals of one connection awaiting their effect, and their bytes.
    pub max_pending: usize,
    pub max_pending_bytes: usize,
    /// All connections' pending bytes together.
    pub max_total_pending_bytes: usize,
    pub commit_timeout: Duration,
    pub retry_interval: Duration,
    /// Advertised to MQTT 5 clients.
    pub receive_maximum: NonZeroU16,
    pub topic_alias_maximum: u16,
    /// Deliveries of one connection held behind a QoS 1 delivery that waits
    /// for the client's receive maximum, in messages and payload bytes, and
    /// all connections' together. QoS 1 is bounded by `apply`'s inflight
    /// window; beyond these an at-most-once delivery is dropped.
    pub max_waiting: usize,
    pub max_waiting_bytes: usize,
    pub max_total_waiting_bytes: usize,
    /// Lifecycle proposals awaiting their effect. At the bound new
    /// connections are refused: while commits stall, every closed session
    /// would otherwise add one more.
    pub max_retries: usize,
    /// Check every CONNECT's credentials through the driver before it is
    /// accepted (D-158 A). Off: every client is let in and may do anything,
    /// as before credentials existed.
    pub authenticate: bool,
}

/// What an authenticated session may do: asked for each PUBLISH and each
/// SUBSCRIBE filter. The driver builds it from its credentials; the gateway
/// only asks.
pub trait Grant: std::fmt::Debug + Send {
    fn may_publish(&self, topic: &Topic) -> bool;
    fn may_subscribe(&self, filter: &TopicFilter) -> bool;
}

/// The driver's answer to [`Action::Authenticate`].
#[derive(Debug)]
pub enum Verdict {
    Allowed(Box<dyn Grant>),
    /// An unknown user name, or a password that does not match.
    BadCredentials,
    /// No user name, and anonymous clients are not let in.
    NotAuthorized,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Send {
        conn: ConnId,
        bytes: Bytes,
    },
    /// Close the socket. The connection is already gone here.
    Close {
        conn: ConnId,
    },
    /// Check a CONNECT's credentials off the node loop, and answer with
    /// [`Gateway::authenticated`]. Only with [`Config::authenticate`].
    Authenticate {
        conn: ConnId,
        username: Option<Bytes>,
        password: Option<Bytes>,
    },
    /// Forward to the sequencer, over a link that keeps order: one session's
    /// publishes must reach the log in the order they were sent. A broken link may lose its tail; it may not reorder (found by
    /// the simulation).
    Propose(Proposal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RetryKey {
    Reset,
    Gone(u64),
    Ack(u64, u64),
}

#[derive(Debug)]
struct Retry {
    proposal: Proposal,
    due: Duration,
    /// Doubles with every resend, up to 32 × `retry_interval`: a fixed
    /// interval shorter than a slow commit fills the log with duplicates
    /// (found by the gateway simulation).
    interval: Duration,
}

#[derive(Debug)]
enum Waiting {
    Publish {
        conn: ConnId,
        packet: Option<PacketId>,
        bytes: usize,
        deadline: Duration,
    },
    /// One filter of a SUBSCRIBE or UNSUBSCRIBE: `index` into its group.
    Filter {
        conn: ConnId,
        group: u64,
        index: usize,
        bytes: usize,
        deadline: Duration,
    },
}

impl Waiting {
    fn conn(&self) -> ConnId {
        match self {
            Self::Publish { conn, .. } | Self::Filter { conn, .. } => *conn,
        }
    }
    fn bytes(&self) -> usize {
        match self {
            Self::Publish { bytes, .. } | Self::Filter { bytes, .. } => *bytes,
        }
    }
    fn deadline(&self) -> Duration {
        match self {
            Self::Publish { deadline, .. } | Self::Filter { deadline, .. } => *deadline,
        }
    }
}

/// A SUBSCRIBE or UNSUBSCRIBE waiting for all its filters.
#[derive(Debug)]
struct Group {
    packet: PacketId,
    subscribe: bool,
    reasons: Vec<Option<ReasonCode>>,
}

#[derive(Debug)]
struct Delivery {
    id: Option<u64>,
    topic: Topic,
    payload: Bytes,
    properties: Bytes,
    retain: bool,
}

impl Delivery {
    fn size(&self) -> usize {
        self.payload.len().saturating_add(self.properties.len())
    }
}

#[derive(Debug)]
struct Open {
    version: Version,
    session: u64,
    client_id: Bytes,
    keep_alive: Option<Duration>,
    last_heard: Duration,
    aliases: HashMap<u16, Topic>,
    client_receive_maximum: u16,
    pending: usize,
    pending_bytes: usize,
    /// QoS 1 publishes from the client not yet acknowledged to it: what
    /// MQTT 5's receive maximum counts.
    inbound_qos1: usize,
    /// QoS 1 deliveries the client has not acknowledged: packet id →
    /// delivery id.
    outbound: BTreeMap<u16, u64>,
    waiting: VecDeque<Delivery>,
    waiting_bytes: usize,
    next_packet: u16,
    groups: HashMap<u64, Group>,
    /// What the session may do; `None`: anything (credentials off).
    grant: Option<Box<dyn Grant>>,
}

#[derive(Debug)]
enum Phase {
    Connecting {
        deadline: Duration,
    },
    /// The CONNECT waits for the driver's verdict on its credentials;
    /// nothing more is read from the connection meanwhile.
    Authenticating {
        deadline: Duration,
        connect: Box<codec::Connect>,
    },
    Open(Box<Open>),
}

#[derive(Debug)]
struct Conn {
    buf: BytesMut,
    phase: Phase,
}

#[derive(Debug)]
pub struct Gateway {
    config: Config,
    now: Duration,
    /// The `NodeReset` of this incarnation is committed: what earlier
    /// incarnations left is gone, and connections are accepted.
    ready: bool,
    next_session: u64,
    next_request: u64,
    conns: BTreeMap<ConnId, Conn>,
    by_client: HashMap<Bytes, ConnId>,
    by_session: HashMap<u64, ConnId>,
    requests: HashMap<u64, Waiting>,
    retries: BTreeMap<RetryKey, Retry>,
    total_pending_bytes: usize,
    total_waiting_bytes: usize,
    dropped: u64,
    /// Counted for metrics (D-112): see `Load`.
    counted: Counted,
    actions: Vec<Action>,
}

/// What the gateway has done, since it started (D-112).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counted {
    /// PUBLISH packets proposed to the log.
    pub publishes: u64,
    /// PUBACKs sent: each after its publish committed.
    pub pubacks: u64,
    /// PUBLISH packets sent to subscribers.
    pub deliveries: u64,
    /// Connections closed, and of them for keep-alive silence and for a
    /// proposal not committed within `commit_timeout`.
    pub closed: u64,
    pub closed_keep_alive: u64,
    pub closed_commit_timeout: u64,
}

/// What the gateway holds, for assertions and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Load {
    pub connections: usize,
    pub pending: usize,
    pub pending_bytes: usize,
    pub retries: usize,
    pub waiting_bytes: usize,
    /// At-most-once deliveries dropped at the waiting bounds.
    pub dropped: u64,
    pub counted: Counted,
}

fn room(config: &Config, total_pending_bytes: usize, open: &Open) -> bool {
    open.pending < config.max_pending
        && open.pending_bytes < config.max_pending_bytes
        && total_pending_bytes < config.max_total_pending_bytes
}

fn guarantee(qos: Qos) -> DeliveryGuarantee {
    match qos {
        Qos::AtMostOnce => DeliveryGuarantee::AtMostOnce,
        // QoS 2 subscriptions are granted QoS 1.
        Qos::AtLeastOnce | Qos::ExactlyOnce => DeliveryGuarantee::AtLeastOnce,
    }
}

fn encode(packet: &Packet, version: Version) -> Option<Bytes> {
    let mut out = BytesMut::new();
    // A packet the version has no form for (a server DISCONNECT in 3.1.1)
    // is not sent: the close says it.
    codec::encode(packet, version, Sender::Server, &mut out).ok()?;
    Some(out.freeze())
}

impl Gateway {
    /// A gateway that has just started: it proposes its `NodeReset` and
    /// accepts connections once that is committed.
    pub fn new(config: Config, now: Duration) -> Self {
        let mut gateway = Self {
            config,
            now,
            ready: false,
            next_session: config.first_session,
            next_request: 0,
            conns: BTreeMap::new(),
            by_client: HashMap::new(),
            by_session: HashMap::new(),
            requests: HashMap::new(),
            retries: BTreeMap::new(),
            total_pending_bytes: 0,
            total_waiting_bytes: 0,
            dropped: 0,
            counted: Counted::default(),
            actions: Vec::new(),
        };
        gateway.retry(
            RetryKey::Reset,
            Proposal::NodeReset {
                node: config.node,
                below: config.first_session,
            },
        );
        gateway
    }

    pub fn take_actions(&mut self) -> Vec<Action> {
        std::mem::take(&mut self.actions)
    }

    pub fn ready(&self) -> bool {
        self.ready
    }

    pub fn load(&self) -> Load {
        Load {
            connections: self.conns.len(),
            pending: self.requests.len(),
            pending_bytes: self.total_pending_bytes,
            retries: self.retries.len(),
            waiting_bytes: self.total_waiting_bytes,
            dropped: self.dropped,
            counted: self.counted,
        }
    }

    /// Client ids with an open session here, for checking invariant 7.
    pub fn client_ids(&self) -> impl Iterator<Item = &[u8]> {
        self.conns.values().filter_map(|c| match &c.phase {
            Phase::Open(open) => Some(open.client_id.as_ref()),
            Phase::Connecting { .. } | Phase::Authenticating { .. } => None,
        })
    }

    /// The session a connection is, once its CONNECT is accepted.
    pub fn session(&self, conn: ConnId) -> Option<Origin> {
        match &self.conns.get(&conn)?.phase {
            Phase::Open(open) => Some(Origin {
                node: self.config.node,
                session: open.session,
            }),
            Phase::Connecting { .. } | Phase::Authenticating { .. } => None,
        }
    }

    fn retry(&mut self, key: RetryKey, proposal: Proposal) {
        self.actions.push(Action::Propose(proposal.clone()));
        self.retries.insert(
            key,
            Retry {
                proposal,
                due: self.now.saturating_add(self.config.retry_interval),
                interval: self.config.retry_interval,
            },
        );
    }

    /// A new connection. `false`: the gateway is at its connection bound,
    /// or at its bound of lifecycle proposals awaiting commit, and the
    /// driver closes the socket.
    pub fn accept(&mut self, conn: ConnId, now: Duration) -> bool {
        self.now = now;
        if self.conns.len() >= self.config.max_connections
            || self.retries.len() >= self.config.max_retries
        {
            return false;
        }
        self.conns.insert(
            conn,
            Conn {
                buf: BytesMut::new(),
                phase: Phase::Connecting {
                    deadline: now.saturating_add(self.config.connect_timeout),
                },
            },
        );
        true
    }

    /// Whether the driver should read more from `conn`. False while its
    /// proposals, or everyone's, are at their bound: the client waits, as
    /// TCP makes it, and nothing is dropped.
    pub fn wants_read(&self, conn: ConnId) -> bool {
        match self.conns.get(&conn).map(|c| &c.phase) {
            Some(Phase::Connecting { .. }) => true,
            Some(Phase::Open(open)) => room(&self.config, self.total_pending_bytes, open),
            Some(Phase::Authenticating { .. }) | None => false,
        }
    }

    pub fn received(&mut self, conn: ConnId, bytes: &[u8], now: Duration) {
        self.now = now;
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        c.buf.extend_from_slice(bytes);
        if let Phase::Open(open) = &mut c.phase {
            open.last_heard = now;
        }
        self.pump(conn);
    }

    /// Decodes and handles buffered packets while the connection has room.
    fn pump(&mut self, conn: ConnId) {
        loop {
            let total = self.total_pending_bytes;
            let config = self.config;
            let Some(c) = self.conns.get_mut(&conn) else {
                return;
            };
            let version = match &c.phase {
                Phase::Connecting { .. } => None,
                // What follows the CONNECT waits for its verdict.
                Phase::Authenticating { .. } => return,
                Phase::Open(open) => {
                    if !room(&config, total, open) {
                        return;
                    }
                    Some(open.version)
                }
            };
            let decoded = codec::decode(&mut c.buf, version, Sender::Client, &config.codec);
            match decoded {
                Ok(Some(packet)) => self.packet(conn, packet),
                Ok(None) => return,
                Err(error) => {
                    self.close(conn, Some(error.reason()));
                    return;
                }
            }
        }
    }

    fn send(&mut self, conn: ConnId, packet: &Packet) {
        let version = match self.conns.get(&conn).map(|c| &c.phase) {
            Some(Phase::Open(open)) => open.version,
            _ => return,
        };
        if let Some(bytes) = encode(packet, version) {
            self.actions.push(Action::Send { conn, bytes });
        }
    }

    /// Ends a connection: an MQTT 5 client is told why, the socket closes,
    /// and the session's end goes to the log.
    fn close(&mut self, conn: ConnId, reason: Option<ReasonCode>) {
        if let (Some(reason), Some(Phase::Open(open))) =
            (reason, self.conns.get(&conn).map(|c| &c.phase))
        {
            let disconnect = Packet::Disconnect(Disconnect {
                reason,
                properties: DisconnectProperties::default(),
            });
            if let Some(bytes) = encode(&disconnect, open.version) {
                self.actions.push(Action::Send { conn, bytes });
            }
        }
        let Some(c) = self.conns.remove(&conn) else {
            return;
        };
        self.counted.closed = self.counted.closed.saturating_add(1);
        self.actions.push(Action::Close { conn });
        let stale: Vec<u64> = self
            .requests
            .iter()
            .filter(|(_, w)| w.conn() == conn)
            .map(|(r, _)| *r)
            .collect();
        for request in stale {
            if let Some(w) = self.requests.remove(&request) {
                self.total_pending_bytes = self.total_pending_bytes.saturating_sub(w.bytes());
            }
        }
        if let Phase::Open(open) = c.phase {
            self.total_waiting_bytes = self.total_waiting_bytes.saturating_sub(open.waiting_bytes);
            if self.by_client.get(&open.client_id) == Some(&conn) {
                self.by_client.remove(&open.client_id);
            }
            self.by_session.remove(&open.session);
            // Its acknowledgements no longer matter: the session goes.
            self.retries
                .retain(|key, _| !matches!(key, RetryKey::Ack(s, _) if *s == open.session));
            let origin = Origin {
                node: self.config.node,
                session: open.session,
            };
            self.retry(
                RetryKey::Gone(open.session),
                Proposal::SessionGone { origin },
            );
        }
    }

    /// The socket closed under the connection.
    pub fn closed(&mut self, conn: ConnId) {
        self.close(conn, None);
        // Nothing to send to a closed socket.
        self.actions
            .retain(|a| !matches!(a, Action::Send { conn: c, .. } | Action::Close { conn: c } if *c == conn));
    }

    fn packet(&mut self, conn: ConnId, packet: Packet) {
        match packet {
            Packet::Connect(connect) => self.connect(conn, *connect),
            Packet::Publish(publish) => self.publish(conn, publish),
            Packet::PubAck(ack) => self.puback(conn, ack.id),
            Packet::Subscribe(subscribe) => self.subscribe(conn, subscribe),
            Packet::Unsubscribe(unsubscribe) => self.unsubscribe(conn, unsubscribe),
            Packet::PingReq => self.send(conn, &Packet::PingResp),
            Packet::Disconnect(_) => self.close(conn, None),
            // Enhanced authentication is not offered (no method is
            // accepted at CONNECT), so an AUTH is out of turn.
            Packet::Auth(_) => self.close(conn, Some(ReasonCode::ProtocolError)),
            // The decoder refuses what a client may not send.
            _ => self.close(conn, Some(ReasonCode::ProtocolError)),
        }
    }

    fn refuse_connect(&mut self, conn: ConnId, version: Version, reason: ReasonCode) {
        let connack = Packet::ConnAck(Box::new(ConnAck {
            session_present: false,
            reason,
            properties: ConnAckProperties::default(),
        }));
        if let Some(bytes) = encode(&connack, version) {
            self.actions.push(Action::Send { conn, bytes });
        }
        self.conns.remove(&conn);
        self.actions.push(Action::Close { conn });
    }

    fn connect(&mut self, conn: ConnId, connect: codec::Connect) {
        let version = connect.version;
        if !self.ready {
            return self.refuse_connect(conn, version, ReasonCode::ServerUnavailable);
        }
        if connect.properties.authentication_method.is_some() {
            return self.refuse_connect(conn, version, ReasonCode::BadAuthenticationMethod);
        }
        let assigned = connect.client_id.is_empty();
        if assigned && version == Version::V311 && !connect.clean_start {
            // A session nobody can name again cannot be asked for
            // (3.1.1 §3.1.3.1).
            return self.refuse_connect(conn, version, ReasonCode::ClientIdentifierNotValid);
        }
        // Refused, not handled here: a will, a session that outlives its
        // connection, a session expiry. They go through the log at step 7c;
        // until then a client must not believe this server keeps them
        // (D-78 A, ADR-0008 step 7a). 3.1.1 has no closer code than
        // "server unavailable".
        let persistent = match version {
            Version::V311 => !connect.clean_start,
            _ => connect.properties.session_expiry_secs != 0,
        };
        if connect.will.is_some() || persistent {
            return self.refuse_connect(conn, version, ReasonCode::ImplementationSpecificError);
        }
        if !self.config.authenticate {
            return self.admit(conn, connect, None);
        }
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let Phase::Connecting { deadline } = c.phase else {
            return;
        };
        let (username, password) = (connect.username.clone(), connect.password.clone());
        c.phase = Phase::Authenticating {
            deadline,
            connect: Box::new(connect),
        };
        self.actions.push(Action::Authenticate {
            conn,
            username,
            password,
        });
    }

    /// The driver's verdict on a CONNECT's credentials. Ignored for a
    /// connection that closed or timed out meanwhile.
    pub fn authenticated(&mut self, conn: ConnId, verdict: Verdict, now: Duration) {
        self.now = now;
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        let Phase::Authenticating { deadline, .. } = c.phase else {
            return;
        };
        let Phase::Authenticating { connect, .. } =
            std::mem::replace(&mut c.phase, Phase::Connecting { deadline })
        else {
            return;
        };
        let version = connect.version;
        match verdict {
            Verdict::Allowed(grant) => {
                self.admit(conn, *connect, Some(grant));
                // What the client sent after its CONNECT.
                self.pump(conn);
            }
            Verdict::BadCredentials => {
                self.refuse_connect(conn, version, ReasonCode::BadUserNameOrPassword);
            }
            Verdict::NotAuthorized => {
                self.refuse_connect(conn, version, ReasonCode::NotAuthorized);
            }
        }
    }

    /// Opens the session of an accepted CONNECT.
    fn admit(&mut self, conn: ConnId, connect: codec::Connect, grant: Option<Box<dyn Grant>>) {
        let version = connect.version;
        let session = self.next_session;
        let assigned = connect.client_id.is_empty();
        let client_id = if assigned {
            Bytes::from(format!("straum-{}-{session}", self.config.node))
        } else {
            Bytes::copy_from_slice(connect.client_id.as_bytes())
        };
        self.next_session = self.next_session.saturating_add(1);
        // One live session per client id within this gateway: the newer
        // takes over (invariant 7, within one gateway until step 7).
        if let Some(&old) = self.by_client.get(&client_id) {
            self.close(old, Some(ReasonCode::SessionTakenOver));
        }
        let keep_alive = (connect.keep_alive_secs != 0)
            .then(|| Duration::from_millis(u64::from(connect.keep_alive_secs) * 1500));
        let open = Open {
            version,
            session,
            client_id: client_id.clone(),
            keep_alive,
            last_heard: self.now,
            aliases: HashMap::new(),
            client_receive_maximum: connect.properties.receive_maximum.get(),
            pending: 0,
            pending_bytes: 0,
            inbound_qos1: 0,
            outbound: BTreeMap::new(),
            waiting: VecDeque::new(),
            waiting_bytes: 0,
            next_packet: 1,
            groups: HashMap::new(),
            grant,
        };
        let Some(c) = self.conns.get_mut(&conn) else {
            return;
        };
        c.phase = Phase::Open(Box::new(open));
        self.by_client.insert(client_id.clone(), conn);
        self.by_session.insert(session, conn);
        let max_packet = u32::try_from(self.config.codec.max_packet_bytes)
            .ok()
            .and_then(NonZeroU32::new);
        let properties = ConnAckProperties {
            receive_maximum: Some(self.config.receive_maximum),
            maximum_qos: Some(Qos::AtLeastOnce),
            retain_available: Some(true),
            maximum_packet_size: max_packet,
            assigned_client_id: assigned
                .then(|| ClientId::new(client_id.clone()).ok())
                .flatten(),
            topic_alias_maximum: Some(self.config.topic_alias_maximum),
            wildcard_subscription_available: Some(true),
            subscription_identifiers_available: Some(false),
            shared_subscription_available: Some(false),
            ..ConnAckProperties::default()
        };
        self.send(
            conn,
            &Packet::ConnAck(Box::new(ConnAck {
                session_present: false,
                reason: ReasonCode::Success,
                properties,
            })),
        );
    }

    fn open(&mut self, conn: ConnId) -> Option<&mut Open> {
        match &mut self.conns.get_mut(&conn)?.phase {
            Phase::Open(open) => Some(open),
            Phase::Connecting { .. } | Phase::Authenticating { .. } => None,
        }
    }

    fn propose(&mut self, conn: ConnId, proposal: Proposal, waiting: Waiting) -> u64 {
        let request = self.next_request;
        self.next_request = self.next_request.saturating_add(1);
        let bytes = waiting.bytes();
        if let Some(open) = self.open(conn) {
            open.pending = open.pending.saturating_add(1);
            open.pending_bytes = open.pending_bytes.saturating_add(bytes);
        }
        self.total_pending_bytes = self.total_pending_bytes.saturating_add(bytes);
        self.requests.insert(request, waiting);
        self.actions.push(Action::Propose(proposal));
        request
    }

    fn publish(&mut self, conn: ConnId, publish: Publish) {
        let node = self.config.node;
        let max_alias = self.config.topic_alias_maximum;
        let receive_maximum = self.config.receive_maximum.get();
        let Some(open) = self.open(conn) else { return };
        if publish.qos == Qos::ExactlyOnce {
            return self.close(conn, Some(ReasonCode::QosNotSupported));
        }
        let alias = publish.properties.as_ref().and_then(|p| p.topic_alias);
        let topic = match (publish.topic, alias) {
            (topic, Some(alias)) if alias.get() > max_alias => {
                let _ = topic;
                return self.close(conn, Some(ReasonCode::TopicAliasInvalid));
            }
            (Some(topic), Some(alias)) => {
                open.aliases.insert(alias.get(), topic.clone());
                topic
            }
            (Some(topic), None) => topic,
            (None, Some(alias)) => match open.aliases.get(&alias.get()) {
                Some(topic) => topic.clone(),
                None => return self.close(conn, Some(ReasonCode::TopicAliasInvalid)),
            },
            (None, None) => return self.close(conn, Some(ReasonCode::ProtocolError)),
        };
        if open.grant.as_ref().is_some_and(|g| !g.may_publish(&topic)) {
            // Nothing is proposed. MQTT 5 answers a QoS 1 publish with
            // "not authorized" and drops a QoS 0 one; 3.1.1 has no refusal
            // to send, and allows a positive acknowledgement or a close
            // (§3.3.5): a close, so the client knows. PUBACKs go out in the
            // order the publishes came (MQTT 5 §4.6): while earlier QoS 1
            // publishes wait for their commit, the refusal cannot go first,
            // so the connection closes and the client resends them.
            let pending = open.inbound_qos1 > 0;
            return match (open.version, publish.id) {
                (Version::V311, _) => self.close(conn, Some(ReasonCode::NotAuthorized)),
                (_, Some(_)) if pending => self.close(conn, Some(ReasonCode::NotAuthorized)),
                (_, Some(id)) => self.send(
                    conn,
                    &Packet::PubAck(PubAck {
                        id,
                        reason: ReasonCode::NotAuthorized,
                        properties: AckProperties::default(),
                    }),
                ),
                (_, None) => {}
            };
        }
        if publish.qos == Qos::AtLeastOnce {
            if open.version == Version::V5 && usize::from(receive_maximum) <= open.inbound_qos1 {
                return self.close(conn, Some(ReasonCode::ReceiveMaximumExceeded));
            }
            open.inbound_qos1 = open.inbound_qos1.saturating_add(1);
        }
        let origin = Origin {
            node,
            session: open.session,
        };
        let Ok(section) = properties::encode(publish.properties.as_deref()) else {
            return self.close(conn, Some(ReasonCode::ImplementationSpecificError));
        };
        let bytes = publish
            .payload
            .len()
            .saturating_add(topic.as_bytes().len())
            .saturating_add(section.len());
        let deadline = self.now.saturating_add(self.config.commit_timeout);
        let request = self.next_request;
        let proposal = Proposal::Publish {
            origin,
            request,
            topic,
            payload: publish.payload,
            properties: section,
            guarantee: guarantee(publish.qos),
            retain: publish.retain,
        };
        self.counted.publishes = self.counted.publishes.saturating_add(1);
        self.propose(
            conn,
            proposal,
            Waiting::Publish {
                conn,
                packet: publish.id,
                bytes,
                deadline,
            },
        );
    }

    /// The client acknowledged a QoS 1 delivery: the acknowledgement goes to
    /// the log, and the next delivery waiting for room goes out.
    fn puback(&mut self, conn: ConnId, id: PacketId) {
        let node = self.config.node;
        let Some(open) = self.open(conn) else { return };
        let Some(delivery) = open.outbound.remove(&id.get()) else {
            // Acknowledging what was never sent (MQTT 5 §4.13: a protocol
            // error).
            return self.close(conn, Some(ReasonCode::ProtocolError));
        };
        let origin = Origin {
            node,
            session: open.session,
        };
        self.retry(
            RetryKey::Ack(origin.session, delivery),
            Proposal::Ack { origin, delivery },
        );
        self.flush_waiting(conn);
    }

    fn subscribe(&mut self, conn: ConnId, subscribe: codec::Subscribe) {
        let deadline = self.now.saturating_add(self.config.commit_timeout);
        let node = self.config.node;
        let group = self.next_request;
        self.next_request = self.next_request.saturating_add(1);
        let Some(open) = self.open(conn) else { return };
        if subscribe.subscription_identifier.is_some() {
            return self.close(conn, Some(ReasonCode::SubscriptionIdentifiersNotSupported));
        }
        let origin = Origin {
            node,
            session: open.session,
        };
        let mut reasons = Vec::with_capacity(subscribe.filters.len());
        let mut proposals = Vec::new();
        for (index, (filter, options)) in subscribe.filters.into_iter().enumerate() {
            if filter.as_bytes().starts_with(b"$share/") {
                reasons.push(Some(ReasonCode::SharedSubscriptionsNotSupported));
                continue;
            }
            // Refused before it is proposed: 3.1.1 sends it as 0x80.
            if open
                .grant
                .as_ref()
                .is_some_and(|g| !g.may_subscribe(&filter))
            {
                reasons.push(Some(ReasonCode::NotAuthorized));
                continue;
            }
            reasons.push(None);
            let handling = match options.retain_handling {
                RetainHandling::OnSubscribe => Handling::OnSubscribe,
                RetainHandling::OnNewSubscription => Handling::OnNewSubscription,
                RetainHandling::Never => Handling::Never,
            };
            proposals.push((index, filter, options.qos, handling, options));
        }
        let Some(open) = self.open(conn) else { return };
        open.groups.insert(
            group,
            Group {
                packet: subscribe.id,
                subscribe: true,
                reasons,
            },
        );
        for (index, filter, qos, handling, options) in proposals {
            let bytes = filter.as_bytes().len();
            let request = self.next_request;
            let proposal = Proposal::Subscribe {
                origin,
                request,
                filter,
                guarantee: guarantee(qos),
                options: Options {
                    no_local: options.no_local,
                    retain_as_published: options.retain_as_published,
                    retain_handling: handling,
                },
            };
            self.propose(
                conn,
                proposal,
                Waiting::Filter {
                    conn,
                    group,
                    index,
                    bytes,
                    deadline,
                },
            );
        }
        // Only refusals: nothing to commit, the answer is now.
        self.finish_group(conn, group);
    }

    fn unsubscribe(&mut self, conn: ConnId, unsubscribe: codec::Unsubscribe) {
        let deadline = self.now.saturating_add(self.config.commit_timeout);
        let node = self.config.node;
        let group = self.next_request;
        self.next_request = self.next_request.saturating_add(1);
        let Some(open) = self.open(conn) else { return };
        let origin = Origin {
            node,
            session: open.session,
        };
        open.groups.insert(
            group,
            Group {
                packet: unsubscribe.id,
                subscribe: false,
                reasons: vec![None; unsubscribe.filters.len()],
            },
        );
        for (index, filter) in unsubscribe.filters.into_iter().enumerate() {
            let bytes = filter.as_bytes().len();
            let request = self.next_request;
            self.propose(
                conn,
                Proposal::Unsubscribe {
                    origin,
                    request,
                    filter,
                },
                Waiting::Filter {
                    conn,
                    group,
                    index,
                    bytes,
                    deadline,
                },
            );
        }
    }

    /// Sends the SUBACK or UNSUBACK once every filter has its answer.
    fn finish_group(&mut self, conn: ConnId, group: u64) {
        let Some(open) = self.open(conn) else { return };
        let done = open
            .groups
            .get(&group)
            .is_some_and(|g| g.reasons.iter().all(Option::is_some));
        if !done {
            return;
        }
        let Some(g) = open.groups.remove(&group) else {
            return;
        };
        let reasons: Vec<ReasonCode> = g.reasons.into_iter().flatten().collect();
        let packet = if g.subscribe {
            Packet::SubAck(SubAck {
                id: g.packet,
                reasons,
                properties: AckProperties::default(),
            })
        } else {
            Packet::UnsubAck(UnsubAck {
                id: g.packet,
                reasons: if open.version == Version::V5 {
                    reasons
                } else {
                    Vec::new()
                },
                properties: AckProperties::default(),
            })
        };
        self.send(conn, &packet);
    }

    fn release(&mut self, request: u64) -> Option<Waiting> {
        let waiting = self.requests.remove(&request)?;
        let bytes = waiting.bytes();
        self.total_pending_bytes = self.total_pending_bytes.saturating_sub(bytes);
        let conn = waiting.conn();
        if let Some(open) = self.open(conn) {
            open.pending = open.pending.saturating_sub(1);
            open.pending_bytes = open.pending_bytes.saturating_sub(bytes);
        }
        Some(waiting)
    }

    /// A committed record's effect. The driver hands every effect over;
    /// those for other gateways' sessions are ignored.
    pub fn effect(&mut self, effect: &Effect, now: Duration) {
        self.now = now;
        let node = self.config.node;
        match effect {
            Effect::Reset { node: n, below, .. } => {
                if *n == node && *below == self.config.first_session {
                    self.ready = true;
                    self.retries.remove(&RetryKey::Reset);
                }
                return;
            }
            Effect::Gone { to, .. } if to.node == node => {
                self.retries.remove(&RetryKey::Gone(to.session));
                return;
            }
            Effect::Acked { to, delivery, .. } if to.node == node => {
                self.retries.remove(&RetryKey::Ack(to.session, *delivery));
                return;
            }
            _ => {}
        }
        let Some(to) = effect.to().filter(|to| to.node == node) else {
            return;
        };
        let Some(&conn) = self.by_session.get(&to.session) else {
            return;
        };
        match effect {
            Effect::Accepted { request, .. } => {
                if let Some(Waiting::Publish {
                    packet: Some(id), ..
                }) = self.release(*request)
                {
                    if let Some(open) = self.open(conn) {
                        open.inbound_qos1 = open.inbound_qos1.saturating_sub(1);
                    }
                    self.counted.pubacks = self.counted.pubacks.saturating_add(1);
                    self.send(
                        conn,
                        &Packet::PubAck(PubAck {
                            id,
                            reason: ReasonCode::Success,
                            properties: AckProperties::default(),
                        }),
                    );
                }
            }
            Effect::Subscribed {
                request, granted, ..
            } => {
                let reason = match granted {
                    Some(DeliveryGuarantee::AtMostOnce) => ReasonCode::Success,
                    Some(DeliveryGuarantee::AtLeastOnce) => ReasonCode::GrantedQos1,
                    None => ReasonCode::QuotaExceeded,
                };
                self.answer(conn, *request, reason);
            }
            Effect::Unsubscribed { request, .. } => {
                self.answer(conn, *request, ReasonCode::Success);
            }
            Effect::Deliver {
                id,
                topic,
                payload,
                properties,
                retain,
                ..
            } => {
                let (max, max_bytes, max_total) = (
                    self.config.max_waiting,
                    self.config.max_waiting_bytes,
                    self.config.max_total_waiting_bytes,
                );
                let total = self.total_waiting_bytes;
                let size = payload.len().saturating_add(properties.len());
                let mut dropped = false;
                let mut queued = false;
                if let Some(open) = self.open(conn) {
                    // QoS 1 is bounded by `apply`'s inflight window; an
                    // at-most-once delivery beyond these bounds is dropped.
                    let full = open.waiting.len() >= max
                        || open.waiting_bytes.saturating_add(size) > max_bytes
                        || total.saturating_add(size) > max_total;
                    if id.is_none() && full {
                        dropped = true;
                    } else {
                        queued = true;
                        open.waiting_bytes = open.waiting_bytes.saturating_add(size);
                        open.waiting.push_back(Delivery {
                            id: *id,
                            topic: topic.clone(),
                            payload: payload.clone(),
                            properties: properties.clone(),
                            retain: *retain,
                        });
                    }
                }
                if dropped {
                    self.dropped = self.dropped.saturating_add(1);
                }
                if queued {
                    self.total_waiting_bytes = total.saturating_add(size);
                }
                self.flush_waiting(conn);
            }
            Effect::Close { .. } => self.close(conn, Some(ReasonCode::QuotaExceeded)),
            Effect::Acked { .. } | Effect::Gone { .. } | Effect::Reset { .. } => {}
        }
        // An answer freed room: read on.
        self.pump(conn);
    }

    fn answer(&mut self, conn: ConnId, request: u64, reason: ReasonCode) {
        if let Some(Waiting::Filter { group, index, .. }) = self.release(request) {
            if let Some(open) = self.open(conn) {
                if let Some(slot) = open
                    .groups
                    .get_mut(&group)
                    .and_then(|g| g.reasons.get_mut(index))
                {
                    *slot = Some(reason);
                }
            }
            self.finish_group(conn, group);
        }
    }

    /// Sends deliveries in order while the client's receive maximum has
    /// room for the QoS 1 ones.
    fn flush_waiting(&mut self, conn: ConnId) {
        loop {
            let Some(open) = self.open(conn) else { return };
            let room = open.outbound.len() < usize::from(open.client_receive_maximum);
            let Some(next) = open.waiting.front() else {
                return;
            };
            if next.id.is_some() && !room {
                return;
            }
            let Some(delivery) = open.waiting.pop_front() else {
                return;
            };
            let size = delivery.size();
            let v5 = open.version == Version::V5;
            open.waiting_bytes = open.waiting_bytes.saturating_sub(size);
            let packet = match delivery.id {
                Some(delivery_id) => {
                    // A free 16-bit identifier: at most the receive
                    // maximum are in use.
                    let mut id = open.next_packet;
                    while open.outbound.contains_key(&id) || id == 0 {
                        id = id.wrapping_add(1);
                    }
                    open.next_packet = id.wrapping_add(1);
                    open.outbound.insert(id, delivery_id);
                    PacketId::new(id)
                }
                None => None,
            };
            let qos = if packet.is_some() {
                Qos::AtLeastOnce
            } else {
                Qos::AtMostOnce
            };
            self.total_waiting_bytes = self.total_waiting_bytes.saturating_sub(size);
            let publish = Packet::Publish(Publish {
                topic: Some(delivery.topic),
                id: packet,
                qos,
                duplicate: false,
                retain: delivery.retain,
                payload: delivery.payload,
                // 3.1.1 has no properties; the encoder would drop them.
                properties: if v5 {
                    properties::decode(&delivery.properties)
                } else {
                    None
                },
            });
            self.counted.deliveries = self.counted.deliveries.saturating_add(1);
            self.send(conn, &publish);
        }
    }

    /// Time passes: connect and keep-alive deadlines, proposals that did
    /// not commit in time, retries.
    pub fn tick(&mut self, now: Duration) {
        self.now = now;
        let mut expired = Vec::new();
        for (conn, c) in &self.conns {
            match &c.phase {
                Phase::Connecting { deadline } | Phase::Authenticating { deadline, .. }
                    if *deadline <= now =>
                {
                    expired.push((*conn, None));
                }
                Phase::Open(open) => {
                    if open
                        .keep_alive
                        .is_some_and(|ka| open.last_heard.saturating_add(ka) <= now)
                    {
                        expired.push((*conn, Some(ReasonCode::KeepAliveTimeout)));
                    }
                }
                Phase::Connecting { .. } | Phase::Authenticating { .. } => {}
            }
        }
        for waiting in self.requests.values() {
            if waiting.deadline() <= now {
                expired.push((waiting.conn(), Some(ReasonCode::ServerBusy)));
            }
        }
        for (conn, reason) in expired {
            match reason {
                Some(ReasonCode::KeepAliveTimeout) => {
                    self.counted.closed_keep_alive =
                        self.counted.closed_keep_alive.saturating_add(1);
                }
                Some(ReasonCode::ServerBusy) if self.conns.contains_key(&conn) => {
                    self.counted.closed_commit_timeout =
                        self.counted.closed_commit_timeout.saturating_add(1);
                }
                _ => {}
            }
            self.close(conn, reason);
        }
        let due: Vec<RetryKey> = self
            .retries
            .iter()
            .filter(|(_, r)| r.due <= now)
            .map(|(k, _)| *k)
            .collect();
        let longest = self.config.retry_interval.saturating_mul(32);
        for key in due {
            if let Some(retry) = self.retries.get_mut(&key) {
                retry.interval = retry.interval.saturating_mul(2).min(longest);
                retry.due = now.saturating_add(retry.interval);
                self.actions.push(Action::Propose(retry.proposal.clone()));
            }
        }
    }
}
