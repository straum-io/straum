//! Tokio: tasks, channels, the broker loop, timers, shutdown, metrics.
//!
//! The only crate that spawns. Everything below it is synchronous and
//! testable without a runtime.

mod broker_loop;
mod config;
mod connection;
mod metrics;
mod session;
mod wal;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use domain::SessionId;
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use transport::{Listener, ReadBudget};

pub use auth::{Authority, Permissions};

pub use config::{AuthSettings, RuntimeConfig, TlsSettings, WalSettings};
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
}

impl Runtime {
    /// Binds the listener and starts the tasks.
    pub fn start(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        // One budget for the whole process, however many listeners there are.
        let budget = Arc::new(ReadBudget::new(config.transport.total_read_bytes));
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
        let mut state = broker::Broker::new(config.broker);
        let restored = match config.durability {
            Durability::Memory => 0,
            _ => wal::restore(&config.wal, config.broker, &mut state)?,
        };

        let (wal, wal_writer) = match config.durability {
            Durability::Memory => (None, None),
            _ => {
                let (submitter, writer) = wal::start(
                    &config.wal,
                    config.broker,
                    restored,
                    config.durability == Durability::Fsynced,
                    events.clone(),
                    Arc::clone(&metrics),
                )?;
                (Some(submitter), Some(writer))
            }
        };

        let brokering = tokio::spawn(
            Loop::new(
                state,
                config.clone(),
                Arc::clone(&metrics),
                Arc::clone(&global_queued),
                wal.clone(),
            )
            .run(commands),
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
        })
    }

    /// The bound address; with port 0 this is the one the kernel picked.
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn metrics(&self) -> Snapshot {
        self.metrics.snapshot()
    }

    /// Stops in the order of CLAUDE.md §16: accepting, then reading, then the
    /// writers drain, then the sessions and sockets go, then the broker loop.
    pub async fn shutdown(self) {
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
