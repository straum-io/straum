//! The single broker task: one state machine, no locks, no I/O.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use broker::{Broker, SessionEvent};
use domain::{Durability, SessionId};
use protocol::{Command, DurableToken, Outbound, Outbox};
use tokio::sync::{mpsc, watch};

use crate::config::RuntimeConfig;
use crate::metrics::Metrics;
use crate::session::{Egress, Sink, weight};
use crate::wal::Submitter;

/// An overflow decision produces at most a close, and a close never
/// overflows, so the flush settles in two rounds. The bound is here to make
/// "never loops without progress" (CLAUDE.md §7) true by construction.
const MAX_FLUSH_ROUNDS: usize = 4;

#[derive(Debug)]
pub(crate) enum Event {
    /// A connection is ready to receive; sent before its first command.
    Attach { session: SessionId, sink: Sink },
    Command {
        session: SessionId,
        command: Command,
        /// `Some` when the publish is on its way to the log.
        durable: Option<DurableToken>,
    },
}

/// One commit became durable on this node, as the pair the commit itself
/// established: the WAL sequence that orders it and the correlation the
/// broker knows it by. Kept as one fact so nothing has to look one up from
/// the other and drift from the log (docs/replication.md §128).
///
/// On a channel of its own rather than among the commands. When
/// `awaiting_quorum` is full the loop stops reading reports, and only
/// reports: the log writer waits, durable publishers wait behind it, and
/// every other command keeps flowing. On the command channel that same stop
/// would have halted every client (D-7).
#[derive(Debug, Clone, Copy)]
pub(crate) struct Durable {
    pub(crate) sequence: u64,
    pub(crate) correlation: DurableToken,
}

/// Reports from the log are few — one per commit — and the loop takes all
/// it can each wakeup. Bounded, and a full one makes the writer thread wait:
/// that is the backpressure `awaiting_quorum` relies on.
pub(crate) const DURABLE_REPORTS: usize = 1024;

pub(crate) struct Loop {
    broker: Broker,
    config: RuntimeConfig,
    metrics: Arc<Metrics>,
    global_queued: Arc<AtomicUsize>,
    /// `None` when durability is `memory`: there is nothing to write to.
    wal: Option<Submitter>,
    sinks: HashMap<SessionId, Sink>,
    out: Outbox,
    ready: Vec<(SessionId, Outbound)>,
    rejected: Vec<(SessionId, Outbound)>,
    /// Session state the log has not taken yet. Reused, and bounded: a log
    /// that cannot keep up must not turn session churn into memory growth.
    session_events: Vec<SessionEvent>,
    /// Commits durable here and waiting for a quorum, in sequence order.
    /// Empty unless durability is `quorum`.
    ///
    /// Never dropped from: losing a pair would leave a correlation that no
    /// watermark can ever release. When it fills, the loop stops reading the
    /// log's reports instead: the writer thread waits on that channel, the
    /// log queue fills, and the readers waiting to submit hold the publisher
    /// at its TCP window (CLAUDE.md §4). It counts commits, so it bounds the
    /// reports, not the acknowledgements — those are `max_pending_durable`.
    awaiting_quorum: VecDeque<(u64, DurableToken)>,
    /// The highest quorum watermark seen. Kept, not only acted on: a report
    /// can arrive after the watermark that already covers it, and nothing
    /// would release it if only a change of watermark could.
    watermark: u64,
}

/// Session records waiting for room in the log queue. Reaching this means
/// the log is far behind; the oldest go, and the sessions they describe are
/// not restored after a restart. State is lost, never corrupted.
const MAX_PENDING_SESSION_RECORDS: usize = 1024;

impl Loop {
    /// `broker` arrives with whatever the log held already restored, so a
    /// client that reconnects sees what it left behind.
    pub(crate) fn new(
        broker: Broker,
        config: RuntimeConfig,
        metrics: Arc<Metrics>,
        global_queued: Arc<AtomicUsize>,
        wal: Option<Submitter>,
    ) -> Self {
        Self {
            broker,
            config,
            metrics,
            global_queued,
            wal,
            sinks: HashMap::new(),
            out: Outbox::new(),
            ready: Vec::new(),
            rejected: Vec::new(),
            session_events: Vec::new(),
            awaiting_quorum: VecDeque::new(),
            watermark: 0,
        }
    }

    /// Runs until every command sender is gone.
    pub(crate) async fn run(
        mut self,
        mut events: mpsc::Receiver<Event>,
        mut reports: mpsc::Receiver<Durable>,
        mut quorum: watch::Receiver<u64>,
    ) {
        let mut batch = Vec::with_capacity(self.config.broker_batch);
        let mut durable = Vec::with_capacity(DURABLE_REPORTS);
        let mut watching = true;
        let mut reporting = true;

        loop {
            // The watermark arrives on its own channel, and it has to: when
            // the pending pairs fill, the reports that would add more stop
            // being read, and a watermark queued behind them would never
            // arrive to unblock it (docs/replication.md §128).
            let awaiting_room = self.awaiting_quorum.len() < self.config.max_awaiting_quorum;

            // The command branch is never switched off. The bound on pending
            // acknowledgements is enforced before a publish reaches the log,
            // at the publisher's own reader (D-7), so nothing here has to
            // stop taking commands — and with one branch always live,
            // `select!` cannot find every branch disabled.
            tokio::select! {
                biased;
                changed = quorum.changed(), if watching => {
                    if changed.is_err() {
                        // Nobody will report a watermark again. Stop
                        // selecting on it rather than spinning on the
                        // error it now returns forever.
                        watching = false;
                        continue;
                    }
                    let reached = *quorum.borrow_and_update();
                    self.watermark = self.watermark.max(reached);
                    self.release_through(self.watermark);
                }
                // Before commands: what is reported here releases what the
                // commands are waiting behind.
                received = reports.recv_many(&mut durable, DURABLE_REPORTS), if reporting && awaiting_room => {
                    if received == 0 {
                        // The writer is gone; nothing will be reported again.
                        reporting = false;
                        continue;
                    }
                    for report in durable.drain(..) {
                        if self.config.durability == Durability::Quorum {
                            // Durable here is not durable enough yet. The
                            // pair waits for a watermark to cover it.
                            self.awaiting_quorum
                                .push_back((report.sequence, report.correlation));
                        } else {
                            self.on_durable(report.correlation);
                        }
                    }
                    // The watermark is read before reports, on its own
                    // channel, so it can already cover what was just queued.
                    // Released here or not at all: a watermark that has
                    // caught up does not change again by itself.
                    self.release_through(self.watermark);
                }
                received = events.recv_many(&mut batch, self.config.broker_batch) => {
                    if received == 0 {
                        break;
                    }
                    for event in batch.drain(..) {
                        self.take(event);
                    }
                }
            }

            self.flush();
            self.persist_sessions();
            let stats = self.broker.stats();
            self.metrics
                .active_sessions
                .store(stats.active_sessions, Ordering::Relaxed);
            self.metrics
                .backlog_refused
                .store(stats.backlog_refused, Ordering::Relaxed);
            // QoS 0 dropped for a full session queue is allowed (§8), and
            // invisible unless counted out here (D-14).
            self.metrics
                .dropped_messages
                .store(stats.dropped_messages, Ordering::Relaxed);
            self.metrics
                .backlog_bytes
                .store(stats.backlog_bytes, Ordering::Relaxed);
            self.metrics
                .slow_consumer_closes
                .store(stats.slow_consumer_closes, Ordering::Relaxed);
            // PUBACKs held back for durability. Sitting at the configured
            // limit means publishers are being held at their TCP window —
            // under `quorum`, usually because no peer is confirming.
            self.metrics.pending_acknowledgements.store(
                u64::try_from(self.broker.pending_durable()).unwrap_or(u64::MAX),
                Ordering::Relaxed,
            );
        }

        tracing::debug!("broker loop stopped");
    }

    fn take(&mut self, event: Event) {
        match event {
            Event::Attach { session, sink } => {
                self.sinks.insert(session, sink);
            }
            Event::Command {
                session,
                command,
                durable,
            } => {
                let ends = matches!(command, Command::Disconnect { .. });
                match durable {
                    Some(token) => {
                        // This publish arrived holding a permit (D-7). If
                        // the broker did not hold its acknowledgement back —
                        // the record was already durable when it got here —
                        // there is nothing for the permit to stand for.
                        let held = self.broker.pending_durable();
                        self.broker
                            .handle_durable(session, command, token, &mut self.out);
                        if self.broker.pending_durable() == held {
                            self.release(1);
                        }
                    }
                    None => self.broker.handle(session, command, &mut self.out),
                }
                if ends {
                    self.sinks.remove(&session);
                }
            }
        }
    }

    /// Hands what is kept for each session to the log.
    ///
    /// Never awaits. The WAL thread reports back through this loop's own
    /// channel, so a loop that waited on a full log queue would be waiting on
    /// itself; what does not fit is tried again next wakeup.
    fn persist_sessions(&mut self) {
        let Some(wal) = &self.wal else {
            self.broker.drain_session_events(&mut self.session_events);
            self.session_events.clear();
            return;
        };
        self.broker.drain_session_events(&mut self.session_events);

        let mut sent = 0usize;
        for event in &self.session_events {
            if !wal.try_submit_session(event) {
                break;
            }
            sent = sent.saturating_add(1);
        }
        self.session_events.drain(..sent);

        if self.session_events.len() > MAX_PENDING_SESSION_RECORDS {
            let over = self
                .session_events
                .len()
                .saturating_sub(MAX_PENDING_SESSION_RECORDS);
            self.session_events.drain(..over);
            Metrics::increment(&self.metrics.session_records_dropped, over as u64);
            tracing::warn!(dropped = over, "the log is behind; session state was lost");
        }
    }

    /// Moves everything the broker produced into the session queues, and lets
    /// the broker decide about what did not fit.
    /// Releases every commit the quorum now covers.
    ///
    /// One call for the whole prefix: correlations advance with sequences,
    /// so the highest one released covers the rest, and one watermark from
    /// one replication frame frees a whole group commit rather than
    /// turning it back into one operation per message.
    fn release_through(&mut self, watermark: u64) {
        let mut highest = None;
        while let Some((sequence, correlation)) = self.awaiting_quorum.front().copied() {
            if sequence > watermark {
                break;
            }
            self.awaiting_quorum.pop_front();
            highest = Some(correlation);
        }
        if let Some(correlation) = highest {
            self.on_durable(correlation);
        }
    }

    /// Releases what is now durable, and the permits of the acknowledgements
    /// that go with it: one each, counted off the broker's own queue so the
    /// two cannot drift.
    fn on_durable(&mut self, correlation: DurableToken) {
        let held = self.broker.pending_durable();
        self.broker.on_durable(correlation, &mut self.out);
        self.release(held.saturating_sub(self.broker.pending_durable()));
    }

    fn release(&self, permits: usize) {
        if let Some(wal) = &self.wal {
            wal.release(permits);
        }
    }

    fn flush(&mut self) {
        for _ in 0..MAX_FLUSH_ROUNDS {
            if self.out.is_empty() {
                return;
            }
            // Moved out and put back so the capacity survives while
            // `enqueue` borrows the rest of `self`.
            let mut ready = std::mem::take(&mut self.ready);
            ready.clear();
            ready.extend(self.out.take());
            self.rejected.clear();

            for (session, outbound) in ready.drain(..) {
                if let Some(back) = self.enqueue(session, outbound) {
                    self.rejected.push((session, back));
                }
            }
            self.ready = ready;

            let mut rejected = std::mem::take(&mut self.rejected);
            for (session, back) in rejected.drain(..) {
                self.broker.on_overflow(session, back, &mut self.out);
            }
            self.rejected = rejected;
        }

        if !self.out.is_empty() {
            tracing::warn!("flush did not settle; dropping the remainder");
            let _ = self.out.take();
        }
    }

    /// `Some` means it did not fit and the broker has to decide.
    fn enqueue(&mut self, session: SessionId, outbound: Outbound) -> Option<Outbound> {
        let closing = matches!(outbound, Outbound::Close(_));
        if let Outbound::Close(protocol::CloseReason::SlowConsumer) = &outbound {
            // Once per session, at the moment the broker gives up on it —
            // not the hot path. Dropping a subscriber is the thing an
            // operator most needs told, and 3.1.1 has no way to tell the
            // client: it gets a closed socket and no reason (§6). Silent
            // here once looked exactly like a broker that had gone slow.
            tracing::warn!(
                ?session,
                inflight = self.config.broker.max_inflight,
                backlog = self.config.broker.session_backlog_messages,
                "closing a subscriber the broker cannot keep up with; \
                 STRAUM_MAX_INFLIGHT bounds how much it may have in flight"
            );
        }
        let egress = Egress::Out(outbound);
        let bytes = weight(&egress);

        let Some(sink) = self.sinks.get(&session) else {
            // The session is already gone; there is nobody to tell.
            return None;
        };

        // Bytes are bounded per session and globally, because a per-session
        // limit times the session count is a product, not a bound (§8).
        let session_queued = sink.queued.load(Ordering::Relaxed).saturating_add(bytes);
        let total_queued = self
            .global_queued
            .load(Ordering::Relaxed)
            .saturating_add(bytes);
        if session_queued > self.config.session_queue_bytes
            || total_queued > self.config.total_queued_bytes
        {
            return into_outbound(egress);
        }

        match sink.tx.try_send(egress) {
            Ok(()) => {
                sink.queued.fetch_add(bytes, Ordering::Relaxed);
                self.global_queued.fetch_add(bytes, Ordering::Relaxed);
                self.metrics.queued_messages.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .queued_bytes
                    .fetch_add(bytes, Ordering::Relaxed);
                if closing {
                    // Nothing else may be queued behind a close.
                    self.sinks.remove(&session);
                }
                None
            }
            Err(mpsc::error::TrySendError::Full(back)) => into_outbound(back),
            // The writer is gone; the reader will report the disconnect.
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.sinks.remove(&session);
                None
            }
        }
    }
}

fn into_outbound(egress: Egress) -> Option<Outbound> {
    match egress {
        Egress::Out(outbound) => Some(outbound),
        Egress::Pong => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::Sink;
    use auth::Permissions;
    use bytes::Bytes;
    use domain::{ClientId, DeliveryGuarantee, Topic};
    use protocol::{Connect, Publish, RequestId};
    use tokio::sync::Semaphore;

    /// The race the permit count has to survive: the log reports a record
    /// durable before the broker has seen the publish it belongs to. The
    /// acknowledgement then goes out at once and is never held, so the
    /// permit its reader took has nothing to stand for and must come back.
    /// Kept, it leaks — and `max_pending_durable` of those wedge every
    /// publisher for good.
    ///
    /// The order is exact, not hoped for: both are queued before the loop
    /// starts, and the loop reads reports before commands.
    #[tokio::test]
    async fn a_permit_comes_back_when_the_record_was_durable_before_its_publish() {
        let admission = Arc::new(Semaphore::new(1));
        let (submitter, _written) = Submitter::detached(Arc::clone(&admission));
        let config = RuntimeConfig {
            durability: Durability::Fsynced,
            max_pending_durable: 1,
            ..RuntimeConfig::default()
        };
        let broker = Loop::new(
            Broker::new(config.broker),
            config,
            Arc::new(Metrics::default()),
            Arc::new(AtomicUsize::new(0)),
            Some(submitter.clone()),
        );

        let (events, commands) = mpsc::channel(16);
        let (reports, reported) = mpsc::channel(16);
        let (_watermark, quorum) = watch::channel(0u64);
        let (egress, _delivered) = mpsc::channel(16);
        let session = SessionId(1);
        events
            .send(Event::Attach {
                session,
                sink: Sink {
                    tx: egress,
                    queued: Arc::new(AtomicUsize::new(0)),
                },
            })
            .await
            .unwrap();
        events
            .send(Event::Command {
                session,
                command: Command::Connect(Connect {
                    client_id: ClientId::new(Bytes::from_static(b"publisher")).unwrap(),
                    permissions: Permissions::unrestricted(),
                    resume: false,
                    will: None,
                }),
                durable: None,
            })
            .await
            .unwrap();

        // The reader's half: a permit taken, the record already durable.
        submitter.admit().await.unwrap().forget();
        assert_eq!(admission.available_permits(), 0);
        reports
            .send(Durable {
                sequence: 1,
                correlation: DurableToken(1),
            })
            .await
            .unwrap();
        events
            .send(Event::Command {
                session,
                command: Command::Publish {
                    ack: Some(RequestId(7)),
                    publish: Publish {
                        topic: Topic::new(Bytes::from_static(b"t")).unwrap(),
                        payload: Bytes::from_static(b"x"),
                        guarantee: DeliveryGuarantee::AtLeastOnce,
                        retain: false,
                    },
                },
                durable: Some(DurableToken(1)),
            })
            .await
            .unwrap();
        // The loop ends once the commands are gone.
        drop(events);

        broker.run(commands, reported, quorum).await;
        assert_eq!(
            admission.available_permits(),
            1,
            "the permit of an acknowledgement that was never held did not come back"
        );
    }

    /// A watermark can reach the loop before the report of a commit it
    /// already covers: they are different channels, and the loop reads the
    /// watermark first. A pair queued after the watermark that covers it must
    /// still be released — waiting for the next watermark change waits
    /// forever once the quorum has caught up and nothing new is published.
    /// Found under CPU load, where 18 of 100 acknowledgements stayed held.
    ///
    /// The order is exact: the watermark is set and the report queued before
    /// the loop starts, and the loop reads the watermark before reports.
    #[tokio::test]
    async fn a_report_behind_its_watermark_is_still_released() {
        let admission = Arc::new(Semaphore::new(1));
        let (submitter, _written) = Submitter::detached(Arc::clone(&admission));
        let config = RuntimeConfig {
            durability: Durability::Quorum,
            max_pending_durable: 1,
            ..RuntimeConfig::default()
        };
        let broker = Loop::new(
            Broker::new(config.broker),
            config,
            Arc::new(Metrics::default()),
            Arc::new(AtomicUsize::new(0)),
            Some(submitter.clone()),
        );

        let (events, commands) = mpsc::channel(16);
        let (reports, reported) = mpsc::channel(16);
        let (watermark, quorum) = watch::channel(0u64);
        let (egress, _delivered) = mpsc::channel(16);
        let session = SessionId(1);
        events
            .send(Event::Attach {
                session,
                sink: Sink {
                    tx: egress,
                    queued: Arc::new(AtomicUsize::new(0)),
                },
            })
            .await
            .unwrap();
        events
            .send(Event::Command {
                session,
                command: Command::Connect(Connect {
                    client_id: ClientId::new(Bytes::from_static(b"publisher")).unwrap(),
                    permissions: Permissions::unrestricted(),
                    resume: false,
                    will: None,
                }),
                durable: None,
            })
            .await
            .unwrap();
        submitter.admit().await.unwrap().forget();
        events
            .send(Event::Command {
                session,
                command: Command::Publish {
                    ack: Some(RequestId(7)),
                    publish: Publish {
                        topic: Topic::new(Bytes::from_static(b"t")).unwrap(),
                        payload: Bytes::from_static(b"x"),
                        guarantee: DeliveryGuarantee::AtLeastOnce,
                        retain: false,
                    },
                },
                durable: Some(DurableToken(1)),
            })
            .await
            .unwrap();
        // The quorum already covers sequence 1 ...
        watermark.send(1).unwrap();
        // ... and the report that pairs sequence 1 with its publish arrives
        // after it.
        reports
            .send(Durable {
                sequence: 1,
                correlation: DurableToken(1),
            })
            .await
            .unwrap();
        drop(events);

        broker.run(commands, reported, quorum).await;
        assert_eq!(
            admission.available_permits(),
            1,
            "an acknowledgement its quorum already covered stayed held"
        );
    }
}
