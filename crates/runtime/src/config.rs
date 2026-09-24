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
