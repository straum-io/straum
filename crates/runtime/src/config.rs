//! Everything the runtime needs, already typed. The environment is parsed in
//! `server`; nothing below this line reads it (docs/configuration.md).

use std::time::Duration;

use std::path::PathBuf;

use broker::BrokerLimits;
use domain::Durability;
use mqtt::MqttLimits;
use storage::WalLimits;
use transport::TransportConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub transport: TransportConfig,
    pub mqtt: MqttLimits,
    pub broker: BrokerLimits,
    /// Capacity of the broker → writer channel, per session.
    pub session_queue_messages: usize,
    /// Payload bytes queued for one session.
    pub session_queue_bytes: usize,
    /// Payload bytes queued across all sessions.
    pub total_queued_bytes: usize,
    /// Capacity of the reader → broker channel.
    pub broker_queue: usize,
    /// Commands handled per broker loop wakeup.
    pub broker_batch: usize,
    pub replication: ReplicationSettings,
    /// Outbound items encoded per socket write.
    pub writer_batch: usize,
    pub shutdown_grace: Duration,
    pub write_stall_timeout: Duration,
    /// How far a publish has to get before it is acknowledged
    /// (docs/storage.md §80).
    pub durability: Durability,
    pub wal: WalSettings,
    /// `Some` makes every listener a TLS listener.
    pub tls: Option<TlsSettings>,
    pub auth: AuthSettings,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            transport: TransportConfig::default(),
            mqtt: MqttLimits::default(),
            broker: BrokerLimits::default(),
            session_queue_messages: 1024,
            session_queue_bytes: 8_388_608,
            total_queued_bytes: 1_073_741_824,
            broker_queue: 65_536,
            broker_batch: 256,
            replication: ReplicationSettings::default(),
            writer_batch: 64,
            shutdown_grace: Duration::from_millis(5_000),
            write_stall_timeout: Duration::from_millis(30_000),
            durability: Durability::Memory,
            wal: WalSettings::default(),
            tls: None,
            auth: AuthSettings::default(),
        }
    }
}

/// Where the log lives and how much may wait in front of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSettings {
    pub path: PathBuf,
    /// Records queued between the connections and the writer thread.
    pub queue: usize,
    /// Rewrite the log at startup once it is at least this large, dropping
    /// records nothing needs any more. `0` never rewrites it.
    pub compact_bytes: u64,
    pub limits: WalLimits,
}

impl Default for WalSettings {
    fn default() -> Self {
        Self {
            path: PathBuf::from("straum.wal"),
            queue: 65_536,
            compact_bytes: 67_108_864,
            limits: WalLimits::default(),
        }
    }
}

/// The certificate the broker presents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSettings {
    pub certificate: PathBuf,
    pub key: PathBuf,
}

/// The credential and rule files, already read. Parsing them is portable
/// logic and happens in the runtime; only the reading is the server's.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthSettings {
    pub passwords: String,
    pub acl: String,
    /// Whether a client without a user name may connect.
    pub allow_anonymous: bool,
}

impl Default for AuthSettings {
    fn default() -> Self {
        Self {
            passwords: String::new(),
            acl: String::new(),
            // An empty configuration is an open broker, which the server says
            // out loud at startup.
            allow_anonymous: true,
        }
    }
}

/// One static leader, one static follower (docs/replication.md §119).
///
/// A node is a leader if it has a `follower` to ship to and a follower if it
/// has an address to `listen` on. Neither is set by default, and then nothing
/// in this file costs anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationSettings {
    /// Where to accept a leader's stream. `None`: this node is not a
    /// follower.
    pub listen: Option<std::net::SocketAddr>,
    /// The follower to ship committed batches to. `None`: this node is not a
    /// leader.
    pub follower: Option<std::net::SocketAddr>,
    /// Committed batches held between the WAL thread and the socket. Bounded
    /// on purpose: a follower that cannot keep up loses its place here and is
    /// caught up from the log, rather than growing a queue in memory.
    pub queue: usize,
    /// Frames held between the catch-up reader and the socket.
    pub catch_up_queue: usize,
    /// How much of the log one catch-up frame carries.
    pub catch_up_batch_bytes: usize,
    /// Read buffer for one replication connection.
    pub read_buffer_bytes: usize,
    pub limits: replication::Limits,
}

impl Default for ReplicationSettings {
    fn default() -> Self {
        Self {
            listen: None,
            follower: None,
            queue: 64,
            catch_up_queue: 4,
            catch_up_batch_bytes: 1_048_576,
            read_buffer_bytes: 65_536,
            limits: replication::Limits::default(),
        }
    }
}
