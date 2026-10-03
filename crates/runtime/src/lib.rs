//! Tokio: tasks, channels, the broker loop, timers, shutdown, metrics.
//!
//! The only crate that spawns. Everything below it is synchronous and
//! testable without a runtime.

mod broker_loop;
mod config;
mod connection;
mod metrics;
mod observe;
mod replication;
mod session;
mod wal;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use domain::SessionId;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use transport::{BufferBudget, Listener};

pub use auth::{Authority, Permissions};

pub use config::{AuthSettings, ReplicationSettings, RuntimeConfig, TlsSettings, WalSettings};
pub use metrics::{Metrics, Snapshot};
// The server composes the runtime out of these, and depends on nothing else.
pub use broker::BrokerLimits;
pub use domain::Durability;
pub use mqtt::MqttLimits;
pub use storage::WalLimits;
pub use transport::sys::reuseport_balances;
pub use transport::{KernelLimits, TransportConfig};

use broker_loop::{Event, Loop};

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("{0}")]
    Auth(#[from] auth::AuthError),
    #[error("{0}")]
    Tls(#[from] transport::TlsError),
    #[error("{0}")]
    Storage(#[from] storage::StorageError),
    #[error("{0}")]
    Transport(#[from] transport::TransportError),
}

/// A running broker.
pub struct Runtime {
    addr: SocketAddr,
    metrics: Arc<Metrics>,
    shutdown: watch::Sender<bool>,
    accepting: Vec<JoinHandle<()>>,
    brokering: JoinHandle<()>,
    events: mpsc::Sender<Event>,
    /// Joined during shutdown so the last batch reaches the disk (§16).
    wal_writer: Option<std::thread::JoinHandle<()>>,
    /// Leader and follower links, drained after the log stops so what was
    /// committed is shipped (docs/replication.md §120).
    background: JoinSet<()>,
    replication_addr: Option<SocketAddr>,
    /// Closed first at shutdown, so publishers waiting for room end at once
    /// instead of at the end of the grace period (D-10).
    admission: Arc<Semaphore>,
    #[cfg(test)]
    log_fault: Arc<std::sync::atomic::AtomicBool>,
}

impl Runtime {
    /// Binds the listener and starts the tasks.
    pub fn start(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        // One budget for the whole process, however many listeners there are.
        let budget = Arc::new(BufferBudget::new(
            config.transport.total_connection_buffer_bytes,
        ));
        // Loaded once, at startup: a broker that cannot read its own key
        // must not come up in plaintext instead.
        let tls = match &config.tls {
            Some(files) => Some(transport::Tls::from_pem_files(
                &files.certificate,
                &files.key,
            )?),
            None => None,
        };
        let first = Listener::bind(config.transport, Arc::clone(&budget), tls.clone())?;
        let addr = first.local_addr()?;

        // With port 0 the kernel picks a port for the first socket; the
        // rest have to ask for that same one to share it.
        let mut listeners = vec![first];
        for _ in 1..config.transport.listeners {
            let mut transport = config.transport;
            transport.listen = addr;
            listeners.push(Listener::bind(transport, Arc::clone(&budget), tls.clone())?);
        }

        let metrics = Arc::new(Metrics::default());
        let global_queued = Arc::new(AtomicUsize::new(0));
        let (events, commands) = mpsc::channel(config.broker_queue);
        // The log's reports travel apart from the commands, so a full
        // pending-acknowledgement queue that stops the commands cannot stop
        // the reports that empty it (C8, docs/memory.md).
        let (reports, reported) = mpsc::channel(broker_loop::DURABLE_REPORTS);
        // Monotonic and latest-only, which is what a watermark is. On its
        // own channel so a full pending queue cannot stop the thing that
        // drains it (docs/replication.md §128).
        let (quorum, quorum_reached) = watch::channel(0u64);
        let (shutdown, stopping) = watch::channel(false);

        // No log unless durability asks for one.
        // Parsed once: a broker whose rules do not parse must not come up
        // with no rules at all.
        let authority = Arc::new(auth::Authority::parse(
            &config.auth.passwords,
            &config.auth.acl,
            config.auth.allow_anonymous,
        )?);

        // Read back before anything is served: the log is the recovery
        // boundary, so what it held is state again before the first
        // client can ask for it.
        // What a rewrite keeps readable for the follower: its own position
        // when acknowledgements wait for it, a byte budget when they do not,
        // nothing without one (docs/replication.md §130, D-18, D-20).
        let retention = match (config.replication.follower, config.durability) {
            (None, _) => wal::Retention::Nothing,
            (Some(_), Durability::Quorum) => wal::Retention::Follower,
            (Some(_), Durability::Memory | Durability::Written | Durability::Fsynced) => {
                wal::Retention::Bytes(config.wal.retain_bytes)
            }
        };
        let mut state = broker::Broker::new(config.broker);
        let restored = match config.durability {
            Durability::Memory => wal::Restored::default(),
            _ => wal::restore(&config.wal, retention, config.broker, &mut state)?,
        };

        // A leader ships committed batches to one follower. Bounded: a
        // follower that cannot keep up loses its place here and is caught up
        // from the log (docs/replication.md §119).
        //
        // The WAL sequence, not the correlation high-water. A replication
        // position is expressed in sequences, and the two spaces are
        // independent — they coincide in an ordinary run and stop coinciding
        // the moment the log holds a record that carries no correlation
        // (docs/storage.md, location-vs-identity).
        let local_sequence = Arc::new(AtomicU64::new(restored.sequence));
        // One permit per acknowledgement the broker may hold back for
        // durability: taken by a publisher's reader before its record goes
        // to the log, returned by the broker loop when the acknowledgement
        // goes (C8, D-7). Kept here as well, so shutdown can close it first.
        let admission = Arc::new(Semaphore::new(config.max_pending_durable));
        #[cfg(test)]
        let log_fault = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (shipments, outbound) = match config.replication.follower {
            Some(_) => {
                let (tx, rx) = mpsc::channel(config.replication.queue);
                (Some(tx), Some(rx))
            }
            None => (None, None),
        };

        let (wal, replicated, wal_writer) = match config.durability {
            Durability::Memory => (None, None, None),
            _ => {
                let (submitter, replicated, writer) = wal::start(
                    &config.wal,
                    config.broker,
                    // The correlation allocator continues the broker's own
                    // identity space, which is not the one above.
                    restored.correlation,
                    wal::FanOut {
                        shipments,
                        local: Arc::clone(&local_sequence),
                        retention,
                        #[cfg(test)]
                        fail: Arc::clone(&log_fault),
                    },
                    // Exhaustive on purpose. An equality against one level
                    // silently answered `false` for a level added later,
                    // which is how a quorum broker would have stopped
                    // syncing its own log.
                    match config.durability {
                        Durability::Memory | Durability::Written => false,
                        Durability::Fsynced | Durability::Quorum => true,
                    },
                    reports,
                    Arc::clone(&admission),
                    Arc::clone(&metrics),
                )?;
                (Some(submitter), Some(replicated), Some(writer))
            }
        };

        let mut background = JoinSet::new();
        let mut replication_addr = None;

        // Bound before anything is served, so a port that cannot be had is a
        // startup failure rather than a surprise later.
        if let Some(addr) = config.metrics {
            let listener = observe::bind(addr).map_err(transport::TransportError::from)?;
            tracing::info!(%addr, "serving metrics");
            background.spawn(observe::serve(
                listener,
                Arc::clone(&metrics),
                stopping.clone(),
            ));
        }
        if let (Some(peer), Some(outbound), Some(replicated)) =
            (config.replication.follower, outbound, replicated)
        {
            background.spawn(replication::lead(
                replication::Leading {
                    peer,
                    settings: config.replication.clone(),
                    wal: config.wal.clone(),
                    budget: Arc::clone(&budget),
                    local: Arc::clone(&local_sequence),
                    replicated,
                    metrics: Arc::clone(&metrics),
                    quorum: quorum.clone(),
                    required_copies: config.replication.required_copies,
                    acknowledges_on_quorum: config.durability == Durability::Quorum,
                    write_stall_timeout: config.write_stall_timeout,
                },
                outbound,
                stopping.clone(),
            ));
        }
        if let Some(listen) = config.replication.listen {
            let mut transport = config.transport;
            transport.listen = listen;
            let listener = Listener::bind(transport, Arc::clone(&budget), None)?;
            replication_addr = Some(listener.local_addr()?);
            background.spawn(replication::follow(
                replication::Following {
                    settings: config.replication.clone(),
                    wal: config.wal.clone(),
                    metrics: Arc::clone(&metrics),
                },
                listener,
                stopping.clone(),
            ));
        }

        let brokering = tokio::spawn(
            Loop::new(
                state,
                config.clone(),
                Arc::clone(&metrics),
                Arc::clone(&global_queued),
                wal.clone(),
            )
            .run(commands, reported, quorum_reached),
        );
        let sessions = Arc::new(AtomicU64::new(0));
        let accepting = listeners
            .into_iter()
            .map(|listener| {
                tokio::spawn(accept(
                    listener,
                    Arc::clone(&sessions),
                    connection::Wiring {
                        config: config.clone(),
                        events: events.clone(),
                        metrics: Arc::clone(&metrics),
                        global_queued: Arc::clone(&global_queued),
                        shutdown: stopping.clone(),
                        authority: Arc::clone(&authority),
                        wal: wal.clone(),
                    },
                ))
            })
            .collect();
        tracing::info!(%addr, "listening");
        Ok(Self {
            addr,
            metrics,
            shutdown,
            accepting,
            brokering,
            events,
            wal_writer,
            background,
            replication_addr,
            admission,
            #[cfg(test)]
            log_fault,
        })
    }

    /// The bound address; with port 0 this is the one the kernel picked.
    /// Where this node accepts a leader's replication stream, if it is a
    /// follower. With port 0 this is the one the kernel assigned.
    /// The counters themselves, for a reader that samples one of them in a
    /// loop rather than taking a whole snapshot each time.
    pub fn metrics_handle(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    pub fn replication_addr(&self) -> Option<SocketAddr> {
        self.replication_addr
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn metrics(&self) -> Snapshot {
        self.metrics.snapshot()
    }

    /// Test-only: the next commit fails the way a failed write or sync
    /// does, and the log's writer stops behind it.
    #[cfg(test)]
    pub(crate) fn fail_log(&self) {
        self.log_fault.store(true, Ordering::Relaxed);
    }

    /// Stops in the order of CLAUDE.md §16: publish admission closes first,
    /// so publishers waiting for room end at once (D-10); then accepting,
    /// then reading, then the writers drain, then the sessions and sockets
    /// go, then the broker loop.
    pub async fn shutdown(self) {
        // First, before anything waits: a publisher parked on a permit is in
        // no read and watches no signal, so only closing the semaphore ends
        // its wait. Left open, every such publisher costs the whole grace
        // period (D-10). Its publish never reached the log and is not
        // acknowledged; the client sends it again, as after any shutdown (§16).
        self.admission.close();
        let _ = self.shutdown.send(true);
        // The accept task owns the connection tasks and gives them the grace
        // period before it stops waiting.
        for accepting in self.accepting {
            let _ = accepting.await;
        }

        // The last command sender: the broker loop ends when it is gone. It
        // goes before the log is joined, because the loop holds a submitter
        // of its own — joining first would wait for a sender that is waiting
        // for the join.
        drop(self.events);
        let _ = self.brokering.await;

        // Every submitter is gone now, so the writer drains what it has —
        // the session records the last closes produced included — commits it
        // and stops.
        if let Some(writer) = self.wal_writer {
            let _ = tokio::task::spawn_blocking(move || writer.join()).await;
        }

        // Last, and only now: the writer dropping its end of the shipment
        // queue is what tells the leader task there is nothing more coming,
        // so it ships what was committed before it stops. What can still be
        // left on this node alone is anything the follower never
        // acknowledged — replication is asynchronous in this phase and a
        // PUBACK never waited for it (docs/replication.md §120).
        let mut background = self.background;
        while background.join_next().await.is_some() {}
        tracing::info!("stopped");
    }
}

async fn accept(listener: Listener, sessions: Arc<AtomicU64>, wiring: connection::Wiring) {
    let mut shutdown = wiring.shutdown.clone();
    let config = wiring.config.clone();
    let mut tasks = JoinSet::new();

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
            accepted = listener.accept() => match accepted {
                Ok(connection) => {
                    // Two tasks per connection, so this is the connection count.
                    // One task per connection, so this is the count.
                    if tasks.len() >= config.transport.max_connections {
                        tracing::warn!("refusing a connection: at the configured maximum");
                        drop(connection);
                        continue;
                    }
                    let next = sessions.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                    Metrics::increment(&wiring.metrics.connections_accepted, 1);
                    tasks.spawn(connection::run(
                        connection,
                        SessionId(next),
                        wiring.clone(),
                    ));
                }
                Err(error) => {
                    Metrics::increment(&wiring.metrics.transport_errors, 1);
                    tracing::warn!(%error, "accept failed");
                }
            },
        }
    }

    tracing::debug!(connections = tasks.len(), "draining");
    let drained = tokio::time::timeout(config.shutdown_grace, async {
        while tasks.join_next().await.is_some() {}
    });
    if drained.await.is_err() {
        tracing::warn!("grace period expired; closing the rest");
    }
    tasks.shutdown().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    fn connect_packet(client: &[u8]) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
        body.extend_from_slice(&u16::try_from(client.len()).unwrap().to_be_bytes());
        body.extend_from_slice(client);
        let mut packet = vec![0x10, u8::try_from(body.len()).unwrap()];
        packet.extend_from_slice(&body);
        packet
    }

    fn publish_packet(id: u16, payload: &[u8]) -> Vec<u8> {
        let mut body = vec![0x00, 0x01, b't'];
        body.extend_from_slice(&id.to_be_bytes());
        body.extend_from_slice(payload);
        let mut packet = vec![0x32, u8::try_from(body.len()).unwrap()];
        packet.extend_from_slice(&body);
        packet
    }

    /// One packet as `(first byte, body)`, or `None` once the socket closes.
    /// Every packet here is short, so the remaining length is one byte.
    async fn next_packet(stream: &mut TcpStream) -> Option<(u8, Vec<u8>)> {
        let mut head = [0u8; 2];
        stream.read_exact(&mut head).await.ok()?;
        let mut body = vec![0u8; usize::from(head[1])];
        stream.read_exact(&mut body).await.ok()?;
        Some((head[0], body))
    }

    /// D-9, in the order the decision names it: the log fails, the next
    /// publish arrives, the broker never takes it, no PUBACK is sent.
    ///
    /// Before the fix the reader got no token from the stopped log, passed
    /// the publish on without one, and the broker acknowledged it at once —
    /// under `fsynced`, for a record that was never written.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_publish_after_the_log_failed_is_never_acknowledged() {
        let mut path = std::env::temp_dir();
        path.push(format!("straum-log-failed-{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let node = Runtime::start(RuntimeConfig {
            transport: TransportConfig {
                listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..TransportConfig::default()
            },
            durability: Durability::Fsynced,
            wal: WalSettings {
                path: path.clone(),
                ..WalSettings::default()
            },
            ..RuntimeConfig::default()
        })
        .unwrap();

        let mut client = TcpStream::connect(node.local_addr()).await.unwrap();
        client
            .write_all(&connect_packet(b"publisher"))
            .await
            .unwrap();
        assert_eq!(
            next_packet(&mut client).await.unwrap().0,
            0x20,
            "no CONNACK"
        );

        // The log works: the first publish is acknowledged.
        client.write_all(&publish_packet(1, b"one")).await.unwrap();
        assert_eq!(
            next_packet(&mut client).await.unwrap(),
            (0x40, vec![0x00, 0x01]),
            "the first publish was not acknowledged"
        );

        // The commit carrying the second publish is the one that fails.
        node.fail_log();
        client.write_all(&publish_packet(2, b"two")).await.unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        while node.metrics().wal_commit_failures == 0 {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the log never failed"
            );
            tokio::task::yield_now().await;
        }

        // The log's queue is closed before the failure is counted, so this
        // publish finds no log to go to. A PINGREQ behind it is the anchor:
        // a reader that passed the publish on would answer the ping itself,
        // and the broker's PUBACK would be here in microseconds. Neither
        // comes, because the reader stopped at the publish.
        client
            .write_all(&publish_packet(3, b"three"))
            .await
            .unwrap();
        client.write_all(&[0xC0, 0x00]).await.unwrap();
        let heard =
            tokio::time::timeout(Duration::from_millis(500), next_packet(&mut client)).await;
        assert!(
            heard.is_err(),
            "the connection answered after the log failed: {heard:?}"
        );

        // Only that connection waits. Another client still subscribes and
        // gets a QoS 0 message through: it needs no log, and the broker took
        // nothing it could not keep its word on.
        let mut other = TcpStream::connect(node.local_addr()).await.unwrap();
        other
            .write_all(&connect_packet(b"bystander"))
            .await
            .unwrap();
        assert_eq!(next_packet(&mut other).await.unwrap().0, 0x20, "no CONNACK");
        other
            .write_all(&[0x82, 0x06, 0x00, 0x01, 0x00, 0x01, b'q', 0x00])
            .await
            .unwrap();
        assert_eq!(next_packet(&mut other).await.unwrap().0, 0x90, "no SUBACK");
        other
            .write_all(&[0x30, 0x07, 0x00, 0x01, b'q', b'a', b'l', b'i', b'v'])
            .await
            .unwrap();
        assert_eq!(
            next_packet(&mut other).await.unwrap(),
            (0x30, vec![0x00, 0x01, b'q', b'a', b'l', b'i', b'v']),
            "a QoS 0 message did not get through after the log failed"
        );

        // And nothing is sent late either: shutdown ends the waiting reader,
        // and everything up to the close is checked for an acknowledgement.
        node.shutdown().await;
        while let Some((kind, body)) = next_packet(&mut client).await {
            assert_ne!(kind, 0x40, "acknowledged after the log failed: {body:?}");
        }
        let _ = std::fs::remove_file(&path);
    }
}
