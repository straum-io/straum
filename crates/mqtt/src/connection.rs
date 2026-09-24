//! Per-connection protocol state: packets in, commands out, and the packet
//! identifiers the two directions share.
//!
//! Split the way the runtime uses it (CLAUDE.md §4): [`Connection`] belongs to
//! the reader task, [`encode_outbound`] runs in the writer task, and only
//! [`PacketIds`] is touched by both.

use std::time::Instant;

use bytes::BytesMut;
use domain::DeliveryId;
use protocol::{Command, ConnectRejectReason, Delivery, DisconnectReason, Outbound, RequestId};

use crate::deadline::Deadline;
use crate::packet::{ConnAck, Connect, ConnectRejected, Packet, PacketId};
use crate::{MqttError, MqttLimits, connack, pingresp, puback, publish, suback, unsuback};

/// What the reader does with a decoded packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reaction {
    /// Hand this to the broker.
    Forward(Command),
    /// A CONNECT. Not a command yet: it becomes one once somebody has
    /// checked the credentials it carries (docs/configuration.md).
    Connect(Connect),
    /// Answer PINGRESP here; keep-alive never reaches the broker (§5).
    Pong,
}
/// Whether the connection survives what was just written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    KeepOpen,
    /// Flush, then close: 3.1.1 has no server-side DISCONNECT (§6).
    CloseAfterWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    AwaitingConnect,
    Connected,
    /// The client sent DISCONNECT; anything after it is a violation.
    Closing,
}

/// Reader-side state of one connection.
#[derive(Debug)]
pub struct Connection {
    state: State,
    deadline: Deadline,
}

impl Connection {
    /// `now` is the moment the connection was accepted: it starts the
    /// deadline for the CONNECT that has to follow.
    pub fn new(now: Instant, limits: &MqttLimits) -> Self {
        Self {
            state: State::AwaitingConnect,
            deadline: Deadline::on_accept(now, limits),
        }
    }

    /// Translates one decoded packet.
    ///
    /// An error means the connection must be closed: an MQTT stream has no
    /// resynchronization point, and every case here is a protocol violation.
    pub fn on_packet(
        &mut self,
        packet: Packet,
        now: Instant,
        ids: &mut PacketIds,
    ) -> Result<Reaction, MqttError> {
        // Anything the client sends counts as a sign of life, a
        // rejected packet included: the caller closes on those anyway.
        self.deadline.on_packet(now);

        match (self.state, packet) {
            (State::AwaitingConnect, Packet::Connect(connect)) => {
                // The reader cannot see the broker's answer, so it moves on
                // immediately: a rejected CONNECT is closed by the writer.
                self.state = State::Connected;
                self.deadline.on_connect(now, connect.keep_alive_secs);
                Ok(Reaction::Connect(connect))
            }
            (State::AwaitingConnect, _) => Err(MqttError::NotConnected),
            (State::Connected, Packet::Connect(_)) => Err(MqttError::AlreadyConnected),

            (State::Connected, Packet::Publish(publish)) => {
                let guarantee = publish.guarantee();
                Ok(Reaction::Forward(Command::Publish {
                    ack: publish.id.map(request_id),
                    publish: protocol::Publish {
                        topic: publish.topic,
                        payload: publish.payload,
                        guarantee,
                    },
                }))
            }
            (State::Connected, Packet::PubAck(id)) => {
                let delivery = ids.release(id).ok_or(MqttError::UnknownPacketId)?;
                Ok(Reaction::Forward(Command::Ack(delivery)))
            }
            (State::Connected, Packet::Subscribe(subscribe)) => {
                Ok(Reaction::Forward(Command::Subscribe {
                    request: request_id(subscribe.id),
                    filters: subscribe.filters,
                }))
            }
            (State::Connected, Packet::Unsubscribe(unsubscribe)) => {
                Ok(Reaction::Forward(Command::Unsubscribe {
                    request: request_id(unsubscribe.id),
                    filters: unsubscribe.filters,
                }))
            }
            (State::Connected, Packet::PingReq) => Ok(Reaction::Pong),
            (State::Connected, Packet::Disconnect) => {
                self.state = State::Closing;
                Ok(Reaction::Forward(Command::Disconnect {
                    reason: DisconnectReason::Client,
                }))
            }

            (State::Closing, _) => Err(MqttError::AfterDisconnect),
        }
    }

    /// The command the runtime sends when the socket dies under the session.
    pub fn connection_lost() -> Command {
        Command::Disconnect {
            reason: DisconnectReason::ConnectionLost,
        }
    }

    /// When the connection must be closed if nothing more arrives:
    /// the CONNECT deadline before the handshake, keep-alive after it.
    /// `None` means the client disabled keep-alive.
    pub fn deadline_at(&self) -> Option<Instant> {
        self.deadline.at()
    }

    pub fn is_expired(&self, now: Instant) -> bool {
        self.deadline.is_expired(now)
    }
    pub fn is_connected(&self) -> bool {
        matches!(self.state, State::Connected)
    }
}

/// Writes one outbound item into the writer's buffer.
///
/// Stateless apart from `ids`, so the writer task can run it without touching
/// the reader's [`Connection`].
pub fn encode_outbound(
    outbound: &Outbound,
    ids: &mut PacketIds,
    buf: &mut BytesMut,
    limits: &MqttLimits,
) -> Result<Disposition, MqttError> {
    match outbound {
        Outbound::Reply(protocol::Response::Connected { session_present }) => {
            connack(
                buf,
                ConnAck::Accepted {
                    session_present: *session_present,
                },
            );
            Ok(Disposition::KeepOpen)
        }
        Outbound::Reply(protocol::Response::ConnectRejected(reason)) => {
            let reason = match reason {
                ConnectRejectReason::ServerUnavailable => ConnectRejected::ServerUnavailable,
                ConnectRejectReason::BadCredentials => ConnectRejected::BadCredentials,
                ConnectRejectReason::NotAuthorized => ConnectRejected::NotAuthorized,
            };
            connack(buf, ConnAck::Rejected(reason));
            Ok(Disposition::CloseAfterWrite)
        }
        Outbound::Reply(protocol::Response::PublishAccepted(request)) => {
            puback(buf, packet_id(*request)?);
            Ok(Disposition::KeepOpen)
        }
        Outbound::Reply(protocol::Response::Subscribed { request, granted }) => {
            suback(buf, packet_id(*request)?, granted, limits)?;
            Ok(Disposition::KeepOpen)
        }
        Outbound::Reply(protocol::Response::Unsubscribed(request)) => {
            unsuback(buf, packet_id(*request)?);
            Ok(Disposition::KeepOpen)
        }
        Outbound::Deliver(delivery) => {
            deliver(delivery, ids, buf, limits)?;
            Ok(Disposition::KeepOpen)
        }
        // Nothing to say: the broker's decision is expressed by the close.
        Outbound::Close(_) => Ok(Disposition::CloseAfterWrite),
    }
}

/// Answers a PINGREQ.
pub fn encode_pong(buf: &mut BytesMut) {
    pingresp(buf);
}

fn deliver(
    delivery: &Delivery,
    ids: &mut PacketIds,
    buf: &mut BytesMut,
    limits: &MqttLimits,
) -> Result<(), MqttError> {
    // The identifier is assigned here, in the writer, and released when the
    // matching PUBACK reaches the reader (§4).
    let id = match delivery.id {
        Some(delivery_id) => Some(ids.assign(delivery_id)?),
        None => None,
    };
    publish(buf, &delivery.topic, id, &delivery.payload, limits)
}

/// MQTT packet identifiers are what this connection correlates by, so a
/// request id is exactly the packet id it came from.
fn request_id(id: PacketId) -> RequestId {
    RequestId(u32::from(id.get()))
}

fn packet_id(request: RequestId) -> Result<PacketId, MqttError> {
    u16::try_from(request.0)
        .ok()
        .and_then(PacketId::new)
        .ok_or(MqttError::ForeignRequestId)
}

/// Outgoing at-least-once deliveries, by the identifier the client will
/// acknowledge. Shared by the reader and the writer of one connection.
///
/// Bounded by the inflight limit, so it is a short vector, not a map: a few
/// dozen entries are scanned faster than they are hashed.
#[derive(Debug)]
pub struct PacketIds {
    inflight: Vec<(PacketId, DeliveryId)>,
    capacity: usize,
    next: u16,
}

impl PacketIds {
    /// `capacity` is the broker's per-session inflight limit.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            // Grown on the first at-least-once delivery, not on connect: a
            // connection that only publishes QoS 0 pays nothing for this.
            inflight: Vec::new(),
            capacity,
            next: 1,
        }
    }

    pub fn assign(&mut self, delivery: DeliveryId) -> Result<PacketId, MqttError> {
        if self.inflight.len() >= self.capacity {
            return Err(MqttError::InflightFull);
        }

        // At most `capacity` identifiers are taken, so a free one is found
        // within `capacity + 1` steps.
        for _ in 0..=self.capacity {
            let candidate = self.next;
            self.next = self.next.checked_add(1).unwrap_or(1);
            let Some(candidate) = PacketId::new(candidate) else {
                continue;
            };
            if !self.inflight.iter().any(|(taken, _)| *taken == candidate) {
                self.inflight.push((candidate, delivery));
                return Ok(candidate);
            }
        }

        Err(MqttError::InflightFull)
    }

    /// Returns what the acknowledged identifier stood for, or `None` if the
    /// client acknowledged something it was never sent.
    pub fn release(&mut self, id: PacketId) -> Option<DeliveryId> {
        let index = self.inflight.iter().position(|(taken, _)| *taken == id)?;
        Some(self.inflight.swap_remove(index).1)
    }

    pub fn len(&self) -> usize {
        self.inflight.len()
    }

    /// Entries the table has room for right now. The runtime uses it to
    /// account for what a connection actually costs.
    pub fn capacity_in_use(&self) -> usize {
        self.inflight.capacity()
    }

    pub fn is_empty(&self) -> bool {
        self.inflight.is_empty()
    }
}
