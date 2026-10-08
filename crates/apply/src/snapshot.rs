//! The applied state at one lsn, as bytes (D-67 B): what lets a node give
//! back the log below it and still rebuild its state on the next start.
//!
//! A snapshot is a function of the committed log up to its lsn and of the
//! limits; decoding it and applying the log after it gives the state and
//! effects that applying the whole log gives (tested). Derived counters —
//! subscriptions, bytes retained, queued and per session — are recomputed,
//! not stored, so a snapshot cannot disagree with itself about them.
//!
//! The format is versioned and local to one node: it changes when the state
//! does (step 7 adds sessions, expiry and wills), and a node refuses a
//! version it does not know rather than guess (D-67 B).
//!
//! ```text
//! [version=1][limits: 8 × u64][applied u64]
//! [sessions u64] { [node u8][session u64][next_delivery u64]
//!                  [subscriptions u32] { [filter u16+bytes][guarantee u8][options u8] }
//!                  [inflight u32]      { [id u64][message] }
//!                  [backlog u32]       { [has_id u8][id u64]?[message] } }
//! [retained u64] { [guarantee u8][message] }
//! message: [topic u16+bytes][payload u32+bytes][properties u32+bytes][retain u8]
//! ```

use std::collections::{BTreeMap, VecDeque};

use bytes::{BufMut, Bytes};
use domain::{Topic, TopicFilter};

use crate::{Body, Limits, Message, Session, State, guarantee_byte, options_byte};

const VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SnapshotError {
    #[error("snapshot version {0} is not known to this build")]
    Version(u8),
    #[error("the snapshot was taken under other limits")]
    Limits,
    #[error("the snapshot does not decode")]
    Malformed,
}

fn limits_fields(limits: &Limits) -> [usize; 8] {
    [
        limits.max_subscriptions_per_session,
        limits.max_subscriptions,
        limits.max_retained,
        limits.max_retained_bytes,
        limits.max_inflight,
        limits.max_backlog,
        limits.max_backlog_bytes,
        limits.max_total_queued_bytes,
    ]
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

fn put_message(message: &Message, out: &mut Vec<u8>) {
    let topic = message.topic.as_bytes();
    out.put_u16(u16::try_from(topic.len()).unwrap_or(u16::MAX));
    out.put_slice(topic);
    put_len32(&message.payload, out);
    put_len32(&message.properties, out);
    out.put_u8(u8::from(message.retain));
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
}

impl State {
    /// Appends the state as a snapshot at [`State::applied`].
    ///
    /// The whole state is written into `out`: as large as the state, which
    /// the limits bound (CLAUDE.md §7).
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.put_u8(VERSION);
        for field in limits_fields(&self.limits) {
            out.put_u64(u64::try_from(field).unwrap_or(u64::MAX));
        }
        out.put_u64(self.applied);
        out.put_u64(u64::try_from(self.sessions.len()).unwrap_or(u64::MAX));
        for (origin, session) in &self.sessions {
            out.put_u8(origin.node);
            out.put_u64(origin.session);
            out.put_u64(session.next_delivery);
            put_count(session.subscriptions.len(), out);
            for (filter, guarantee, options) in session.subscriptions.values() {
                let bytes = filter.as_bytes();
                out.put_u16(u16::try_from(bytes.len()).unwrap_or(u16::MAX));
                out.put_slice(bytes);
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
        out.put_u64(u64::try_from(self.retained.len()).unwrap_or(u64::MAX));
        for (message, guarantee) in self.retained.values() {
            out.put_u8(guarantee_byte(*guarantee));
            put_message(message, out);
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
        if version != VERSION {
            return Err(SnapshotError::Version(version));
        }
        for field in limits_fields(&limits) {
            if body.u64().map_err(malformed)? != u64::try_from(field).unwrap_or(u64::MAX) {
                return Err(SnapshotError::Limits);
            }
        }
        let mut state = State::new(limits);
        state.applied = body.u64().map_err(malformed)?;
        let sessions = body.u64().map_err(malformed)?;
        for _ in 0..sessions {
            let origin = body.origin().map_err(malformed)?;
            let mut session = Session {
                next_delivery: body.u64().map_err(malformed)?,
                ..Session::default()
            };
            for _ in 0..body.u32().map_err(malformed)? {
                let filter = TopicFilter::new(body.short().map_err(malformed)?)
                    .map_err(|_| SnapshotError::Malformed)?;
                let guarantee = body.guarantee().map_err(malformed)?;
                let options = body.options().map_err(malformed)?;
                session.subscriptions.insert(
                    Bytes::copy_from_slice(filter.as_bytes()),
                    (filter, guarantee, options),
                );
            }
            for _ in 0..body.u32().map_err(malformed)? {
                let id = body.u64().map_err(malformed)?;
                let message = body.message().map_err(malformed)?;
                session.inflight_bytes = session.inflight_bytes.saturating_add(message.size());
                session.inflight.insert(id, message);
            }
            let mut backlog = VecDeque::new();
            for _ in 0..body.u32().map_err(malformed)? {
                let id = match body.flag().map_err(malformed)? {
                    true => Some(body.u64().map_err(malformed)?),
                    false => None,
                };
                let message = body.message().map_err(malformed)?;
                session.backlog_bytes = session.backlog_bytes.saturating_add(message.size());
                backlog.push_back((id, message));
            }
            session.backlog = backlog;
            state.subscriptions = state
                .subscriptions
                .saturating_add(session.subscriptions.len());
            state.queued_bytes = state
                .queued_bytes
                .saturating_add(session.inflight_bytes)
                .saturating_add(session.backlog_bytes);
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
        body.finish().map_err(malformed)?;
        // A state the limits could not have produced did not come from this
        // node's log, checksum or not.
        let within = state.sessions.values().all(|s| {
            s.subscriptions.len() <= limits.max_subscriptions_per_session
                && s.inflight.len() <= limits.max_inflight
                && s.backlog.len() <= limits.max_backlog
                && s.backlog_bytes <= limits.max_backlog_bytes
        }) && state.subscriptions <= limits.max_subscriptions
            && state.retained.len() <= limits.max_retained
            && state.retained_bytes <= limits.max_retained_bytes
            && state.queued_bytes <= limits.max_total_queued_bytes;
        if !within {
            return Err(SnapshotError::Malformed);
        }
        Ok(state)
    }
}
