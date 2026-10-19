//! Client ids across connections (ADR-0008 step 7c, D-144 A, D-146 A).
//!
//! A client id has an entry while a connection is attached to it or its
//! session persists. A `Connect` attaches an origin if it passes the fence:
//! its proposer had applied the client id's last change (`seen` at or past
//! it) and it committed within `connect_window` positions of `seen`. The
//! last change is the entry's last applied `Connect`, or its removal, kept
//! as a tombstone, or the floor the oldest tombstone rose to when the queue
//! was full. So no `Connect` replaces a newer attachment, clears newer
//! state, or brings back a session it had not seen removed (invariant B),
//! and the attachment is the origin of the last `Connect` that passed
//! (invariant A).
//!
//! While attached, a session is keyed by its origin in `State::sessions`,
//! and every per-origin rule applies to it unchanged. Detached, a persisted
//! session is held here, and what arrives for it waits in its backlog,
//! inside `max_detached_queued_bytes`.
//!
//! Memory: entries are bounded by attached origins and
//! `max_persisted_sessions`; tombstones by `max_connect_tombstones`, each a
//! client id of at most `MAX_CLIENT_ID_BYTES` and an lsn.

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;
use domain::{ClientId, DeliveryGuarantee};

use crate::{ClientEffect, ConnectLimits, Effect, Message, Origin, Refusal, Session, State};

/// One client id's entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Client {
    pub(crate) attached: Option<Origin>,
    /// The lsn of the last `Connect` applied for this client id.
    pub(crate) connected_at: u64,
    /// The session is kept across disconnects.
    pub(crate) persist: bool,
    /// The session while no origin is attached.
    pub(crate) detached: Option<Session>,
}

/// What `State::client` shows of an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientView {
    pub attached: Option<Origin>,
    pub connected_at: u64,
    pub persist: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub(crate) struct Clients {
    pub(crate) limits: ConnectLimits,
    pub(crate) by_id: BTreeMap<Bytes, Client>,
    /// The attached origins: the exact inverse of the entries' `attached`.
    pub(crate) origins: BTreeMap<Origin, Bytes>,
    /// Removed entries, oldest first: (client id, the lsn that removed it).
    pub(crate) tombstones: VecDeque<(Bytes, u64)>,
    /// The latest tombstone per client id in the queue.
    pub(crate) latest: BTreeMap<Bytes, u64>,
    /// The lsn of the last tombstone the full queue let go.
    pub(crate) floor: u64,
    /// Entries that persist.
    pub(crate) persisted: usize,
    /// Inflight and backlog bytes of detached sessions.
    pub(crate) detached_bytes: usize,
}

impl Clients {
    /// The lsn of the last change to `id` a `Connect` must have seen.
    fn last_change(&self, id: &[u8]) -> u64 {
        match self.by_id.get(id) {
            Some(client) => client.connected_at,
            None => self.latest.get(id).copied().unwrap_or(self.floor),
        }
    }

    /// Tombstones older than `window` positions before `lsn` fence nothing
    /// the window does not: a `Connect` that needed one fails the window.
    fn prune(&mut self, lsn: u64, window: u64) {
        let horizon = lsn.saturating_sub(window);
        while let Some((id, at)) = self.tombstones.front().cloned() {
            if at >= horizon {
                break;
            }
            self.tombstones.pop_front();
            if self.latest.get(&id) == Some(&at) {
                self.latest.remove(&id);
            }
        }
    }

    /// Removes `id`'s entry at `lsn` and leaves a tombstone. A full queue
    /// lets its oldest go, and the floor rises to it.
    fn remove(&mut self, id: &Bytes, lsn: u64, max: usize) {
        let Some(client) = self.by_id.remove(id) else {
            return;
        };
        if client.persist {
            self.persisted = self.persisted.saturating_sub(1);
        }
        if max == 0 {
            self.floor = self.floor.max(lsn);
            return;
        }
        while self.tombstones.len() >= max {
            let Some((old, at)) = self.tombstones.pop_front() else {
                break;
            };
            self.floor = self.floor.max(at);
            if self.latest.get(&old) == Some(&at) {
                self.latest.remove(&old);
            }
        }
        self.tombstones.push_back((id.clone(), lsn));
        self.latest.insert(id.clone(), lsn);
    }
}

fn queued(session: &Session) -> usize {
    session.inflight_bytes.saturating_add(session.backlog_bytes)
}

impl State {
    /// An origin that may not propose: step 7c is on and it is not
    /// attached.
    pub(crate) fn unattached(&self, origin: Origin) -> bool {
        self.clients
            .as_ref()
            .is_some_and(|c| !c.origins.contains_key(&origin))
    }

    pub(crate) fn attached_origins(&self) -> impl Iterator<Item = &Origin> {
        self.clients.iter().flat_map(|c| c.origins.keys())
    }

    /// Step 7c: `id`'s entry, if it has one.
    pub fn client(&self, id: &[u8]) -> Option<ClientView> {
        let client = self.clients.as_ref()?.by_id.get(id)?;
        Some(ClientView {
            attached: client.attached,
            connected_at: client.connected_at,
            persist: client.persist,
        })
    }

    /// Step 7c: the client id attached to `origin`.
    pub fn attached_to(&self, origin: Origin) -> Option<&[u8]> {
        self.clients
            .as_ref()?
            .origins
            .get(&origin)
            .map(|id| id.as_ref())
    }

    /// Step 7c: the fence's floor, and the tombstones held.
    pub fn connect_fence(&self) -> (u64, usize) {
        self.clients
            .as_ref()
            .map_or((0, 0), |c| (c.floor, c.tombstones.len()))
    }

    /// Step 7c: entries that persist, and the bytes detached sessions
    /// hold.
    pub fn persisted(&self) -> (usize, usize) {
        self.clients
            .as_ref()
            .map_or((0, 0), |c| (c.persisted, c.detached_bytes))
    }

    /// A session's subscriptions and bytes leave the global counts.
    fn release(&mut self, session: &Session) {
        self.subscriptions = self
            .subscriptions
            .saturating_sub(session.subscriptions.len());
        self.queued_bytes = self.queued_bytes.saturating_sub(queued(session));
    }

    #[expect(clippy::too_many_arguments, reason = "one Connect, unpacked once")]
    pub(crate) fn connect(
        &mut self,
        origin: Origin,
        client_id: &ClientId,
        seen: u64,
        clean_start: bool,
        persist: bool,
        lsn: u64,
        out: &mut Vec<Effect>,
    ) {
        let Some(mut clients) = self.clients.take() else {
            return;
        };
        let limits = clients.limits;
        let id = Bytes::copy_from_slice(client_id.as_bytes());
        clients.prune(lsn, limits.connect_window);
        // Proposed by a gateway that had applied the client id's last
        // change, within the window — and an origin connects once.
        let fresh = seen < lsn
            && lsn.saturating_sub(seen) <= limits.connect_window
            && seen >= clients.last_change(&id)
            && !clients.origins.contains_key(&origin);
        let refusal = if !fresh {
            Some(Refusal::Stale)
        } else if persist
            // A new persisted session, or a clean one made persistent: a
            // resumption of one that persists is never refused.
            && !clients.by_id.get(&id).is_some_and(|c| c.persist)
            && clients.persisted >= limits.max_persisted_sessions
        {
            Some(Refusal::Quota)
        } else {
            None
        };
        if let Some(reason) = refusal {
            self.clients = Some(clients);
            self.client_out.push(ClientEffect::ConnectRefused {
                to: origin,
                reason,
                lsn,
            });
            return;
        }
        let mut kept = None;
        if let Some(previous) = clients.by_id.remove(&id) {
            if previous.persist {
                clients.persisted = clients.persisted.saturating_sub(1);
            }
            kept = Some(match previous.attached {
                Some(old) => {
                    clients.origins.remove(&old);
                    self.client_out
                        .push(ClientEffect::TakenOver { to: old, lsn });
                    self.sessions.remove(&old).unwrap_or_default()
                }
                None => {
                    let session = previous.detached.unwrap_or_default();
                    clients.detached_bytes =
                        clients.detached_bytes.saturating_sub(queued(&session));
                    session
                }
            });
        }
        let session_present = kept.is_some() && !clean_start;
        if clean_start {
            if let Some(session) = kept.take() {
                self.release(&session);
            }
        }
        // A new origin holds nothing yet; whatever it held is not this
        // client's.
        self.drop_session(origin);
        if let Some(session) = kept {
            self.sessions.insert(origin, session);
        }
        if persist {
            clients.persisted = clients.persisted.saturating_add(1);
        }
        clients.by_id.insert(
            id.clone(),
            Client {
                attached: Some(origin),
                connected_at: lsn,
                persist,
                detached: None,
            },
        );
        clients.origins.insert(origin, id);
        self.clients = Some(clients);
        self.client_out.push(ClientEffect::Connected {
            to: origin,
            session_present,
            lsn,
        });
        // What waited while it was detached goes out as the window allows.
        self.drain(origin, lsn, out);
        self.forget_if_empty(origin);
    }

    /// Detaches `origin`, if attached: its session is kept detached if it
    /// persists and the detached budget has room, and deleted whole
    /// otherwise, leaving a tombstone. Returns whether it was attached.
    pub(crate) fn detach(&mut self, origin: Origin, lsn: u64) -> bool {
        let Some(mut clients) = self.clients.take() else {
            return false;
        };
        let limits = clients.limits;
        let attached = match clients.origins.remove(&origin) {
            None => false,
            Some(id) => {
                let session = self.sessions.remove(&origin).unwrap_or_default();
                let persist = clients.by_id.get(&id).is_some_and(|c| c.persist);
                let detached = clients.detached_bytes.saturating_add(queued(&session));
                if persist && detached <= limits.max_detached_queued_bytes {
                    clients.detached_bytes = detached;
                    if let Some(client) = clients.by_id.get_mut(&id) {
                        client.attached = None;
                        client.detached = Some(session);
                    }
                } else {
                    self.release(&session);
                    clients.remove(&id, lsn, limits.max_connect_tombstones);
                    if persist {
                        self.client_out.push(ClientEffect::SessionDeleted { lsn });
                    }
                }
                true
            }
        };
        self.clients = Some(clients);
        attached
    }

    /// The session of attached `origin` is gone (dropped for overflow):
    /// its entry goes too, leaving a tombstone.
    pub(crate) fn forget_client(&mut self, origin: Origin, lsn: u64) {
        if let Some(clients) = self.clients.as_mut() {
            let max = clients.limits.max_connect_tombstones;
            if let Some(id) = clients.origins.remove(&origin) {
                clients.remove(&id, lsn, max);
            }
        }
    }

    /// Hands `published` to every detached session it matches: into its
    /// backlog, if the session's backlog, the detached budget and the total
    /// have room. Without room an at-most-once message is dropped and an
    /// at-least-once one deletes the session whole (D-144 (3)).
    pub(crate) fn route_detached(
        &mut self,
        published: &Message,
        guarantee: DeliveryGuarantee,
        lsn: u64,
    ) {
        let limits = self.limits;
        let Some(mut clients) = self.clients.take() else {
            return;
        };
        let bounds = clients.limits;
        let mut matched = Vec::new();
        for (id, client) in &clients.by_id {
            let Some(session) = &client.detached else {
                continue;
            };
            let mut best: Option<(DeliveryGuarantee, bool)> = None;
            for (filter, granted, options) in session.subscriptions.values() {
                if !filter.matches(&published.topic) {
                    continue;
                }
                let keep_retain = published.retain && options.retain_as_published;
                best = Some(match best {
                    Some((g, r)) => (g.max(*granted), r || keep_retain),
                    None => (*granted, keep_retain),
                });
            }
            if let Some((granted, retain)) = best {
                matched.push((id.clone(), granted.min(guarantee), retain));
            }
        }
        for (id, guarantee, retain) in matched {
            let message = Message {
                retain,
                ..published.clone()
            };
            let size = message.size();
            let total = self.queued_bytes;
            let detached = clients.detached_bytes;
            let Some(session) = clients.by_id.get_mut(&id).and_then(|c| c.detached.as_mut()) else {
                continue;
            };
            let fits = session.backlog.len() < limits.max_backlog
                && session.backlog_bytes.saturating_add(size) <= limits.max_backlog_bytes
                && detached.saturating_add(size) <= bounds.max_detached_queued_bytes
                && total.saturating_add(size) <= limits.max_total_queued_bytes;
            let at_least_once = guarantee == DeliveryGuarantee::AtLeastOnce;
            if fits {
                let delivery = at_least_once.then(|| {
                    let next = session.next_delivery;
                    session.next_delivery = next.saturating_add(1);
                    next
                });
                session.backlog.push_back((delivery, message));
                session.backlog_bytes = session.backlog_bytes.saturating_add(size);
                clients.detached_bytes = detached.saturating_add(size);
                self.queued_bytes = total.saturating_add(size);
            } else if at_least_once {
                if let Some(session) = clients.by_id.get_mut(&id).and_then(|c| c.detached.take()) {
                    clients.detached_bytes =
                        clients.detached_bytes.saturating_sub(queued(&session));
                    self.release(&session);
                }
                clients.remove(&id, lsn, bounds.max_connect_tombstones);
                self.client_out.push(ClientEffect::SessionDeleted { lsn });
            }
        }
        self.clients = Some(clients);
    }
}
