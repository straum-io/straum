//! Two tasks per connection: one reads, one writes.
//!
//! They are separate so a client that stops reading cannot stop us from
//! reading its packets, and vice versa (CLAUDE.md §4). All they share is the
//! packet identifier table, which is uncontended and never held across an
//! await.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use bytes::BytesMut;
use domain::SessionId;
use mqtt::{Connection as Protocol, Disposition, PacketIds, Reaction, decode, encode_outbound};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;

use crate::broker_loop::Event;
use crate::config::RuntimeConfig;
use crate::metrics::Metrics;
use crate::session::{Egress, Sink, weight};
use crate::wal::Submitter;
use auth::{Authenticated, Authority};
use domain::Durability;
use protocol::{Command, ConnectRejectReason, Outbound, Response};

#[derive(Clone)]
pub(crate) struct Wiring {
    pub(crate) config: RuntimeConfig,
    pub(crate) events: mpsc::Sender<Event>,
    pub(crate) metrics: Arc<Metrics>,
    pub(crate) global_queued: Arc<AtomicUsize>,
    pub(crate) shutdown: watch::Receiver<bool>,
    /// `None` when durability is `memory`: there is no log to submit to.
    pub(crate) wal: Option<Submitter>,
    /// Who may connect and what they may do once they have.
    pub(crate) authority: Arc<Authority>,
}

/// Takes one accepted socket all the way: handshake, wiring, and the two
/// tasks that serve it. Lives as long as the connection does, so the
/// accept loop can wait for it during shutdown.
pub(crate) async fn run(accepted: transport::Accepted, session: SessionId, wiring: Wiring) {
    let peer = accepted.peer();
    // A handshake is round trips with a stranger; it gets the same
    // patience as the CONNECT that has to follow it.
    let connection = match tokio::time::timeout(
        wiring.config.mqtt.connect_timeout,
        accepted.establish(),
    )
    .await
    {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            Metrics::increment(&wiring.metrics.transport_errors, 1);
            tracing::debug!(%peer, %error, "could not establish the connection");
            return;
        }
        Err(_) => {
            Metrics::increment(&wiring.metrics.transport_errors, 1);
            tracing::debug!(%peer, "handshake timed out");
            return;
        }
    };

    // A set rather than two loose handles: this task is aborted when the
    // accept loop gives up waiting at shutdown, and detached children would
    // outlive it still holding a command sender — which is the one thing
    // keeping the broker loop from ending (§16).
    let mut tasks = JoinSet::new();
    spawn(&mut tasks, connection, session, wiring);
    while tasks.join_next().await.is_some() {}
}

/// Starts the two tasks. They are separate so a client that stops reading
/// cannot stop us from reading its packets (§4).
fn spawn(
    tasks: &mut JoinSet<()>,
    connection: transport::Connection,
    session: SessionId,
    wiring: Wiring,
) {
    let (tx, rx) = mpsc::channel(wiring.config.session_queue_messages);
    let queued = Arc::new(AtomicUsize::new(0));
    let ids = Arc::new(Mutex::new(PacketIds::with_capacity(
        wiring.config.broker.max_inflight,
    )));

    let sink = Sink {
        tx: tx.clone(),
        queued: Arc::clone(&queued),
    };

    tasks.spawn(read(
        connection.reader,
        session,
        sink,
        tx,
        Arc::clone(&ids),
        wiring.config.clone(),
        Arc::clone(&wiring.metrics),
        wiring.events,
        wiring.shutdown,
        wiring.wal,
        wiring.authority,
    ));
    tasks.spawn(write(
        connection.writer,
        rx,
        ids,
        wiring.config.clone(),
        wiring.metrics,
        queued,
        wiring.global_queued,
    ));
}

#[expect(
    clippy::too_many_arguments,
    reason = "a connection is wired once; the alternative is a struct that exists only to be destructured"
)]
async fn read(
    mut reader: transport::Reader,
    session: SessionId,
    sink: Sink,
    egress: mpsc::Sender<Egress>,
    ids: Arc<Mutex<PacketIds>>,
    config: RuntimeConfig,
    metrics: Arc<Metrics>,
    events: mpsc::Sender<Event>,
    mut shutdown: watch::Receiver<bool>,
    wal: Option<Submitter>,
    authority: Arc<Authority>,
) {
    if events.send(Event::Attach { session, sink }).await.is_err() {
        return;
    }

    let mut protocol = Protocol::new(Instant::now(), &config.mqtt);

    'connection: loop {
        let read = tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            () = elapsed(protocol.deadline_at()) => {
                Metrics::increment(&metrics.keepalive_closes, 1);
                tracing::debug!(?session, "closing a connection that went quiet");
                break;
            }
            result = reader.fill() => result,
        };

        let read = match read {
            Ok(0) => break,
            Ok(read) => read,
            Err(error) => {
                Metrics::increment(&metrics.transport_errors, 1);
                tracing::debug!(?session, %error, "read failed");
                break;
            }
        };
        Metrics::increment(&metrics.bytes_in, read as u64);

        loop {
            let packet = match decode(reader.buf_mut(), &config.mqtt) {
                Ok(Some(packet)) => packet,
                Ok(None) => break,
                Err(error) => {
                    Metrics::increment(&metrics.protocol_errors, 1);
                    tracing::debug!(?session, %error, "protocol error");
                    break 'connection;
                }
            };

            // The lock is taken and dropped inside this statement: never held
            // across the awaits below (CLAUDE.md §10).
            let reaction = {
                let mut ids = lock(&ids);
                protocol.on_packet(packet, Instant::now(), &mut ids)
            };

            match reaction {
                Ok(Reaction::Connect(connect)) => {
                    // Hashing a password costs tens of milliseconds. On a
                    // worker thread that would stall everything else the
                    // executor holds, so it goes to a blocking one.
                    let checking = Arc::clone(&authority);
                    let username = connect.username.clone();
                    let password = connect.password.clone();
                    let decision = tokio::task::spawn_blocking(move || {
                        checking.authenticate(username.as_deref(), password.as_deref())
                    })
                    .await;

                    let permissions = match decision {
                        Ok(Authenticated::Allowed(permissions)) => permissions,
                        Ok(refused) => {
                            let reason = match refused {
                                Authenticated::NotAuthorized => ConnectRejectReason::NotAuthorized,
                                _ => ConnectRejectReason::BadCredentials,
                            };
                            Metrics::increment(&metrics.authentication_failures, 1);
                            tracing::debug!(?session, ?reason, "refused a connection");
                            // The writer sends the CONNACK and closes;
                            // the broker never hears about this session.
                            let _ = egress
                                .send(Egress::Out(Outbound::Reply(Response::ConnectRejected(
                                    reason,
                                ))))
                                .await;
                            return;
                        }
                        Err(_) => return,
                    };

                    let command = Command::Connect(protocol::Connect {
                        client_id: connect.client_id,
                        permissions,
                        // CleanSession = 0 asks for whatever this
                        // identity left behind (3.1.1 §3.1.2.4).
                        resume: !connect.clean_session,
                    });
                    if events
                        .send(Event::Command {
                            session,
                            command,
                            durable: None,
                        })
                        .await
                        .is_err()
                    {
                        break 'connection;
                    }
                }
                Ok(Reaction::Forward(command)) => {
                    // A publish that has to be durable goes to the log
                    // before the broker hears about it. Awaiting here is
                    // the backpressure: a full log queue slows the
                    // publisher down, it never shortens the guarantee.
                    let durable = match (&wal, &command) {
                        (
                            Some(wal),
                            Command::Publish {
                                ack: Some(_),
                                publish,
                            },
                        ) if config.durability == Durability::Fsynced => {
                            wal.submit(
                                &publish.topic,
                                &publish.payload,
                                publish.guarantee,
                                publish.retain,
                            )
                            .await
                        }
                        _ => None,
                    };

                    // Bounded on purpose: backpressure reaches the publisher's
                    // TCP window instead of growing a queue here (§4).
                    if events
                        .send(Event::Command {
                            session,
                            command,
                            durable,
                        })
                        .await
                        .is_err()
                    {
                        break 'connection;
                    }
                }
                Ok(Reaction::Pong) => {
                    // A client that cannot take a PINGRESP is not reading at
                    // all; keep-alive closes it soon enough.
                    let _ = egress.try_send(Egress::Pong);
                }
                Err(error) => {
                    Metrics::increment(&metrics.protocol_errors, 1);
                    tracing::debug!(?session, %error, "protocol violation");
                    break 'connection;
                }
            }
        }

        // Give back what a large packet forced, now that it is out of the way.
        reader.reclaim();
    }

    let _ = events
        .send(Event::Command {
            session,
            command: Protocol::connection_lost(),
            durable: None,
        })
        .await;
}

async fn write(
    mut writer: transport::Writer,
    mut egress: mpsc::Receiver<Egress>,
    ids: Arc<Mutex<PacketIds>>,
    config: RuntimeConfig,
    metrics: Arc<Metrics>,
    queued: Arc<AtomicUsize>,
    global_queued: Arc<AtomicUsize>,
) {
    let mut buf = BytesMut::with_capacity(config.transport.read_buffer_bytes);
    let mut batch = Vec::with_capacity(config.writer_batch);

    loop {
        let received = egress.recv_many(&mut batch, config.writer_batch).await;
        if received == 0 {
            break;
        }

        let mut closing = false;
        for item in batch.drain(..) {
            let bytes = weight(&item);
            queued.fetch_sub(bytes, Ordering::Relaxed);
            global_queued.fetch_sub(bytes, Ordering::Relaxed);
            metrics.queued_messages.fetch_sub(1, Ordering::Relaxed);
            metrics.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);

            let encoded = match &item {
                Egress::Pong => {
                    mqtt::encode_pong(&mut buf);
                    Ok(Disposition::KeepOpen)
                }
                Egress::Out(outbound) => {
                    let mut ids = lock(&ids);
                    encode_outbound(outbound, &mut ids, &mut buf, &config.mqtt)
                }
            };

            match encoded {
                Ok(Disposition::KeepOpen) => {}
                Ok(Disposition::CloseAfterWrite) => closing = true,
                Err(error) => {
                    Metrics::increment(&metrics.protocol_errors, 1);
                    tracing::debug!(%error, "failed to encode an outbound item");
                    closing = true;
                }
            }
        }

        if !buf.is_empty() {
            let write = tokio::time::timeout(config.write_stall_timeout, writer.write_all(&buf));
            match write.await {
                Ok(Ok(())) => Metrics::increment(&metrics.bytes_out, buf.len() as u64),
                Ok(Err(error)) => {
                    Metrics::increment(&metrics.transport_errors, 1);
                    tracing::debug!(%error, "write failed");
                    break;
                }
                Err(_) => {
                    // A writer that made no progress for this long is not
                    // going to (§8).
                    Metrics::increment(&metrics.transport_errors, 1);
                    tracing::warn!("closing a connection whose writes stalled");
                    break;
                }
            }
            buf.clear();
        }

        if closing {
            break;
        }
    }

    let _ = writer.shutdown().await;
}

/// The table is a plain map; a panic elsewhere does not make it unusable.
fn lock(ids: &Arc<Mutex<PacketIds>>) -> MutexGuard<'_, PacketIds> {
    match ids.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Resolves when the deadline passes; never, when there is none.
async fn elapsed(at: Option<Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await,
        None => std::future::pending().await,
    }
}
