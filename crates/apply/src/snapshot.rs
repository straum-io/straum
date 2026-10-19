//! The applied state at one lsn, as bytes (D-67 B): what lets a node give
//! back the log below it and still rebuild its state on the next start.
//!
//! A snapshot is a function of the committed log up to its lsn and of the
//! limits; decoding it and applying the log after it gives the state and
//! effects that applying the whole log gives (tested). Derived counters —
//! subscriptions, bytes retained, queued and per session, the attached
//! origins, persisted entries and detached bytes — are recomputed, not
//! stored, so a snapshot cannot disagree with itself about them.
//!
//! The format is versioned and local to one node: it changes when the state
//! does, and a node refuses a version it does not know rather than guess
//! (D-67 B). Version 1 is the state without client ids; version 2 adds step
//! 7c's (D-146 A), and only a `State::with_connect` writes it.
//!
//! ```text
//! [version][limits: 8 × u64, version 2: 12][applied u64]
//! [sessions u64] { [node u8][session u64] session }
//! [retained u64] { [guarantee u8][message] }
//! version 2:
//! [floor u64][clients u64] { [id u16+bytes][connected_at u64][persist u8]
//!                            [1][node u8][session u64] | [2] session }
//! [tombstones u32] { [id u16+bytes][lsn u64] }
//! session: [next_delivery u64]
//!          [subscriptions u32] { [filter u16+bytes][guarantee u8][options u8] }
//!          [inflight u32]      { [id u64][message] }
//!          [backlog u32]       { [has_id u8][id u64]?[message] }
//! message: [topic u16+bytes][payload u32+bytes][properties u32+bytes][retain u8]
//! ```

use std::collections::{BTreeMap, VecDeque};

use bytes::{BufMut, Bytes};
use domain::{ClientId, Topic, TopicFilter};

use crate::clients::{Client, Clients};
use crate::{
    Body, Limits, MAX_CLIENT_ID_BYTES, Message, Session, State, guarantee_byte, options_byte,
};

const VERSION: u8 = 1;
const VERSION_CONNECT: u8 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot version {0} is not known to this build")]
    Version(u8),
    #[error("the snapshot was taken under other limits")]
    Limits,
    #[error("the snapshot does not decode")]
    Malformed,
}

fn limits_fields(limits: &Limits, connect: bool) -> Vec<u64> {
    let wide = |n: usize| u64::try_from(n).unwrap_or(u64::MAX);
    let mut fields: Vec<u64> = [
        limits.max_subscriptions_per_session,
        limits.max_subscriptions,
        limits.max_retained,
        limits.max_retained_bytes,
        limits.max_inflight,
        limits.max_backlog,
        limits.max_backlog_bytes,
        limits.max_total_queued_bytes,
    ]
    .into_iter()
    .map(wide)
    .collect();
    if connect {
        fields.extend([
            limits.connect_window,
            wide(limits.max_connect_tombstones),
            wide(limits.max_persisted_sessions),
            wide(limits.max_detached_queued_bytes),
        ]);
    }
    fields
}

fn put_len32(bytes: &[u8], out: &mut Vec<u8>) {
    // A payload or properties section is bounded far below 4 GiB by the
    // codec's packet limit; saturating keeps this total.
    out.put_u32(u32::try_from(bytes.len()).unwrap_or(u32::MAX));
    out.put_slice(bytes);
}

fn put_count(count: usize, out: &mut Vec<u8>) {
    out.put_u32(u32::try_from(count).unwrap_or(u32::MAX));
}

fn put_short(bytes: &[u8], out: &mut Vec<u8>) {
    out.put_u16(u16::try_from(bytes.len()).unwrap_or(u16::MAX));
    out.put_slice(bytes);
}

fn put_message(message: &Message, out: &mut Vec<u8>) {
    put_short(message.topic.as_bytes(), out);
    put_len32(&message.payload, out);
    put_len32(&message.properties, out);
    out.put_u8(u8::from(message.retain));
}

fn put_session(session: &Session, out: &mut Vec<u8>) {
    out.put_u64(session.next_delivery);
    put_count(session.subscriptions.len(), out);
    for (filter, guarantee, options) in session.subscriptions.values() {
        put_short(filter.as_bytes(), out);
        out.put_u8(guarantee_byte(*guarantee));
        out.put_u8(options_byte(*options));
    }
    put_count(session.inflight.len(), out);
    for (id, message) in &session.inflight {
        out.put_u64(*id);
        put_message(message, out);
    }
    put_count(session.backlog.len(), out);
    for (id, message) in &session.backlog {
        match id {
            Some(id) => {
                out.put_u8(1);
                out.put_u64(*id);
            }
            None => out.put_u8(0),
        }
        put_message(message, out);
    }
}

impl Body<'_> {
    fn u32(&mut self) -> Result<u32, crate::ApplyError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| crate::ApplyError::Malformed(self.1))?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn long(&mut self) -> Result<Bytes, crate::ApplyError> {
        let length =
            usize::try_from(self.u32()?).map_err(|_| crate::ApplyError::Malformed(self.1))?;
        Ok(Bytes::copy_from_slice(self.take(length)?))
    }

    fn message(&mut self) -> Result<Message, crate::ApplyError> {
        let topic = Topic::new(self.short()?).map_err(|e| crate::ApplyError::Topic(0, e))?;
        Ok(Message {
            topic,
            payload: self.long()?,
            properties: self.long()?,
            retain: self.flag()?,
        })
    }

    fn session(&mut self) -> Result<Session, SnapshotError> {
        let malformed = |_| SnapshotError::Malformed;
        let mut session = Session {
            next_delivery: self.u64().map_err(malformed)?,
            ..Session::default()
        };
        for _ in 0..self.u32().map_err(malformed)? {
            let filter = TopicFilter::new(self.short().map_err(malformed)?)
                .map_err(|_| SnapshotError::Malformed)?;
            let guarantee = self.guarantee().map_err(malformed)?;
            let options = self.options().map_err(malformed)?;
            session.subscriptions.insert(
                Bytes::copy_from_slice(filter.as_bytes()),
                (filter, guarantee, options),
            );
        }
        for _ in 0..self.u32().map_err(malformed)? {
            let id = self.u64().map_err(malformed)?;
            let message = self.message().map_err(malformed)?;
            session.inflight_bytes = session.inflight_bytes.saturating_add(message.size());
            session.inflight.insert(id, message);
        }
        let mut backlog = VecDeque::new();
        for _ in 0..self.u32().map_err(malformed)? {
            let id = match self.flag().map_err(malformed)? {
                true => Some(self.u64().map_err(malformed)?),
                false => None,
            };
            let message = self.message().map_err(malformed)?;
            session.backlog_bytes = session.backlog_bytes.saturating_add(message.size());
            backlog.push_back((id, message));
        }
        session.backlog = backlog;
        Ok(session)
    }

    fn client_id(&mut self) -> Result<Bytes, SnapshotError> {
        let id = self.short().map_err(|_| SnapshotError::Malformed)?;
        if id.len() > MAX_CLIENT_ID_BYTES || ClientId::new(id.clone()).is_err() {
            return Err(SnapshotError::Malformed);
        }
        Ok(id)
    }
}

fn within(session: &Session, limits: &Limits) -> bool {
    session.subscriptions.len() <= limits.max_subscriptions_per_session
        && session.inflight.len() <= limits.max_inflight
        && session.backlog.len() <= limits.max_backlog
        && session.backlog_bytes <= limits.max_backlog_bytes
}

impl State {
    /// Appends the state as a snapshot at [`State::applied`].
    ///
    /// The whole state is written into `out`: as large as the state, which
    /// the limits bound.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let connect = self.clients.is_some();
        out.put_u8(if connect { VERSION_CONNECT } else { VERSION });
        for field in limits_fields(&self.limits, connect) {
            out.put_u64(field);
        }
        out.put_u64(self.applied);
        out.put_u64(u64::try_from(self.sessions.len()).unwrap_or(u64::MAX));
        for (origin, session) in &self.sessions {
            out.put_u8(origin.node);
            out.put_u64(origin.session);
            put_session(session, out);
        }
        out.put_u64(u64::try_from(self.retained.len()).unwrap_or(u64::MAX));
        for (message, guarantee) in self.retained.values() {
            out.put_u8(guarantee_byte(*guarantee));
            put_message(message, out);
        }
        let Some(clients) = &self.clients else {
            return;
        };
        out.put_u64(clients.floor);
        out.put_u64(u64::try_from(clients.by_id.len()).unwrap_or(u64::MAX));
        for (id, client) in &clients.by_id {
            put_short(id, out);
            out.put_u64(client.connected_at);
            out.put_u8(u8::from(client.persist));
            match (&client.attached, &client.detached) {
                (Some(origin), _) => {
                    out.put_u8(1);
                    out.put_u8(origin.node);
                    out.put_u64(origin.session);
                }
                (None, Some(session)) => {
                    out.put_u8(2);
                    put_session(session, out);
                }
                (None, None) => {
                    out.put_u8(2);
                    put_session(&Session::default(), out);
                }
            }
        }
        put_count(clients.tombstones.len(), out);
        for (id, lsn) in &clients.tombstones {
            put_short(id, out);
            out.put_u64(*lsn);
        }
    }

    /// The state a snapshot holds. Refused if it was taken under other
    /// limits — the limits are part of what the log means — or by another
    /// version of the format. Every length is checked against the bytes
    /// that are there; nothing is allocated from a count before its
    /// entries are read.
    pub fn decode(limits: Limits, bytes: &[u8]) -> Result<Self, SnapshotError> {
        let mut body = Body(bytes, 0);
        let malformed = |_| SnapshotError::Malformed;
        let version = body.byte().map_err(malformed)?;
        let connect = match version {
            VERSION => false,
            VERSION_CONNECT => true,
            other => return Err(SnapshotError::Version(other)),
        };
        for field in limits_fields(&limits, connect) {
            if body.u64().map_err(malformed)? != field {
                return Err(SnapshotError::Limits);
            }
        }
        let mut state = if connect {
            State::with_connect(limits)
        } else {
            State::new(limits)
        };
        state.applied = body.u64().map_err(malformed)?;
        let sessions = body.u64().map_err(malformed)?;
        for _ in 0..sessions {
            let origin = body.origin().map_err(malformed)?;
            let session = body.session()?;
            state.count(&session);
            state.sessions.insert(origin, session);
        }
        let mut retained = BTreeMap::new();
        for _ in 0..body.u64().map_err(malformed)? {
            let guarantee = body.guarantee().map_err(malformed)?;
            let message = body.message().map_err(malformed)?;
            state.retained_bytes = state.retained_bytes.saturating_add(message.size());
            retained.insert(
                Bytes::copy_from_slice(message.topic.as_bytes()),
                (message, guarantee),
            );
        }
        state.retained = retained;
        let mut detached_within = true;
        if connect {
            let mut clients = Clients {
                floor: body.u64().map_err(malformed)?,
                ..Clients::default()
            };
            for _ in 0..body.u64().map_err(malformed)? {
                let id = body.client_id()?;
                let connected_at = body.u64().map_err(malformed)?;
                let persist = body.flag().map_err(malformed)?;
                let (attached, detached) = match body.byte().map_err(malformed)? {
                    1 => (Some(body.origin().map_err(malformed)?), None),
                    2 if persist => (None, Some(body.session()?)),
                    _ => return Err(SnapshotError::Malformed),
                };
                if let Some(origin) = attached {
                    if clients.origins.insert(origin, id.clone()).is_some() {
                        return Err(SnapshotError::Malformed);
                    }
                }
                if let Some(session) = &detached {
                    detached_within &= within(session, &limits);
                    clients.detached_bytes = clients
                        .detached_bytes
                        .saturating_add(session.inflight_bytes)
                        .saturating_add(session.backlog_bytes);
                    state.count(session);
                }
                if persist {
                    clients.persisted = clients.persisted.saturating_add(1);
                }
                let client = Client {
                    attached,
                    connected_at,
                    persist,
                    detached,
                };
                if clients.by_id.insert(id, client).is_some() {
                    return Err(SnapshotError::Malformed);
                }
            }
            for _ in 0..body.u32().map_err(malformed)? {
                let id = body.client_id()?;
                let lsn = body.u64().map_err(malformed)?;
                if clients
                    .tombstones
                    .back()
                    .is_some_and(|&(_, last)| lsn < last)
                {
                    return Err(SnapshotError::Malformed);
                }
                clients.latest.insert(id.clone(), lsn);
                clients.tombstones.push_back((id, lsn));
            }
            let bounded = clients.tombstones.len() <= limits.max_connect_tombstones
                && clients.persisted <= limits.max_persisted_sessions
                && clients.detached_bytes <= limits.max_detached_queued_bytes;
            if !bounded {
                return Err(SnapshotError::Malformed);
            }
            state.clients = Some(clients);
        }
        body.finish().map_err(malformed)?;
        // A state the limits could not have produced did not come from this
        // node's log, checksum or not.
        let within = detached_within
            && state.sessions.values().all(|s| within(s, &limits))
            && state.subscriptions <= limits.max_subscriptions
            && state.retained.len() <= limits.max_retained
            && state.retained_bytes <= limits.max_retained_bytes
            && state.queued_bytes <= limits.max_total_queued_bytes;
        if !within {
            return Err(SnapshotError::Malformed);
        }
        Ok(state)
    }

    /// A decoded session's subscriptions and bytes join the global counts.
    fn count(&mut self, session: &Session) {
        self.subscriptions = self
            .subscriptions
            .saturating_add(session.subscriptions.len());
        self.queued_bytes = self
            .queued_bytes
            .saturating_add(session.inflight_bytes)
            .saturating_add(session.backlog_bytes);
    }
}
