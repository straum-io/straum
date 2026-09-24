//! The single broker task: one state machine, no locks, no I/O.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use broker::{Broker, SessionEvent};
use domain::SessionId;
use protocol::{Command, DurableToken, Outbound, Outbox};
use tokio::sync::mpsc;

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
    /// Everything up to this token is durable (docs/storage.md §79).
    Durable(DurableToken),
}

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
        }
    }

    /// Runs until every sender is gone.
    pub(crate) async fn run(mut self, mut events: mpsc::Receiver<Event>) {
        let mut batch = Vec::with_capacity(self.config.broker_batch);

        loop {
            let received = events.recv_many(&mut batch, self.config.broker_batch).await;
            if received == 0 {
                break;
            }

            for event in batch.drain(..) {
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
                                self.broker
                                    .handle_durable(session, command, token, &mut self.out);
                            }
                            None => self.broker.handle(session, command, &mut self.out),
                        }
                        if ends {
                            self.sinks.remove(&session);
                        }
                    }
                    Event::Durable(token) => self.broker.on_durable(token, &mut self.out),
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
        }

        tracing::debug!("broker loop stopped");
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
