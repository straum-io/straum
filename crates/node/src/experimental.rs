//! The new path's configuration as the server builds it (ADR-0008 step 7a,
//! D-103 A, D-104 A): what an operator sets, the startup checks, and the
//! internal sizes 7a fixes. Experimental, not production-ready: the timing
//! defaults are placeholders, the sizes untuned, and nothing here is
//! measured on Linux (D-102 A).

use std::net::SocketAddr;
use std::num::NonZeroU16;
use std::path::PathBuf;
use std::time::Duration;

use storage::{SegmentLimits, WalLimits};
use transport::TransportConfig;

use crate::{MqttConfig, NodeConfig};

/// Heartbeat placeholder: the tests' value, not a recommendation (D-104 A).
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_millis(50);
/// Heartbeats of silence before a takeover: the tests' value, which the
/// simulation shows churns under random delivery (D-99), not measured on a
/// network (D-104 A).
pub const DEFAULT_TAKEOVER_TICKS: u32 = 3;
/// One catch-up or fetch read (D-83 A).
pub const DEFAULT_READ_BYTES: usize = 64 * 1024;

/// The fixed sizes of 7a: not settings, and not tuned. A batch of
/// `MAX_BATCH` records of the largest packet fits one frame.
const MAX_BATCH: usize = 16;
const MAX_PACKET_BYTES: usize = 256 * 1024;
const MAX_FRAME: usize = 8 << 20;

/// What an operator sets for the experimental cluster path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Experimental {
    pub id: u8,
    /// Every voter, this node included: its id and where its links listen.
    pub voters: Vec<(u8, SocketAddr)>,
    /// Where this node listens for links.
    pub listen: SocketAddr,
    /// Where it listens for MQTT clients.
    pub mqtt: SocketAddr,
    pub data: PathBuf,
    pub heartbeat: Duration,
    pub takeover_ticks: u32,
    pub read_bytes: usize,
    pub retain_bytes: u64,
    /// Where the metrics are served (D-112); `None` serves none.
    pub metrics: Option<SocketAddr>,
}

/// A configuration refused at startup: nothing is corrected silently
/// (D-104 A).
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConfigError {
    #[error("{count} voters: a cluster needs an odd number, at least three")]
    Voters { count: usize },
    #[error("voter {0} is listed twice")]
    DuplicateVoter(u8),
    #[error("this node, {0}, is not among the voters")]
    NotAVoter(u8),
    #[error("heartbeat {0:?} is outside 1 ms..=10 s")]
    Heartbeat(Duration),
    #[error("takeover after {0} heartbeats: at least 1")]
    TakeoverTicks(u32),
    #[error("read_bytes {read_bytes} is outside 1..={limit} (half the frame, {max_frame})")]
    ReadBytes {
        read_bytes: usize,
        limit: usize,
        max_frame: usize,
    },
    #[error("the largest batch, {batch} bytes, does not fit one frame of {max_frame}")]
    BatchOverFrame { batch: usize, max_frame: usize },
    #[error("max_uncommitted must be at least 1")]
    Uncommitted,
    #[error("max_queue {max_queue} is below one proposal per voter ({voters})")]
    Queue { max_queue: usize, voters: usize },
}

impl NodeConfig {
    /// The startup checks every configuration of the new path passes before
    /// anything is opened. `Node::start` runs them.
    ///
    /// # Errors
    /// The first rule broken.
    pub fn check(&self) -> Result<(), ConfigError> {
        let voters = &self.core.nodes;
        if voters.len() < 3 || voters.len() % 2 == 0 {
            return Err(ConfigError::Voters {
                count: voters.len(),
            });
        }
        let mut sorted = voters.clone();
        sorted.sort_unstable();
        if let Some(pair) = sorted.windows(2).find(|w| w.first() == w.get(1)) {
            return Err(ConfigError::DuplicateVoter(
                pair.first().copied().unwrap_or(0),
            ));
        }
        if !voters.contains(&self.core.id) {
            return Err(ConfigError::NotAVoter(self.core.id));
        }
        if self.heartbeat < Duration::from_millis(1) || self.heartbeat > Duration::from_secs(10) {
            return Err(ConfigError::Heartbeat(self.heartbeat));
        }
        // The operator's range is the server's (1–1000); a test may wait
        // longer, so that only a handoff changes the sequencer.
        if self.core.takeover_ticks == 0 {
            return Err(ConfigError::TakeoverTicks(self.core.takeover_ticks));
        }
        let limit = self.max_frame / 2;
        if self.read_bytes == 0 || self.read_bytes > limit {
            return Err(ConfigError::ReadBytes {
                read_bytes: self.read_bytes,
                limit,
                max_frame: self.max_frame,
            });
        }
        // An APPEND of the largest batch: its header record and `max_batch`
        // records of the largest packet, each with its record header and its
        // length prefix in the frame, plus the frame's own fields. A body
        // carries a packet's topic and payload and less than 256 bytes of
        // encoding around them.
        let packet = self
            .mqtt
            .as_ref()
            .map_or(0, |m| m.gateway.codec.max_packet_bytes);
        let record = journal::RECORD_HEADER_BYTES + 4 + packet + 256;
        let batch = self
            .core
            .max_batch
            .saturating_add(1)
            .saturating_mul(record)
            .saturating_add(64);
        if batch > self.max_frame {
            return Err(ConfigError::BatchOverFrame {
                batch,
                max_frame: self.max_frame,
            });
        }
        if self.core.max_uncommitted == 0 {
            return Err(ConfigError::Uncommitted);
        }
        // Each node's outbox holds `max_queue / voters` (D-120 A); below one
        // per voter the shares, never 0, would add up past the queue and a
        // resend burst after a slot change could overflow it.
        if self.core.max_queue < voters.len() {
            return Err(ConfigError::Queue {
                max_queue: self.core.max_queue,
                voters: voters.len(),
            });
        }
        Ok(())
    }
}

impl Experimental {
    /// The node configuration: the operator's settings, the decided
    /// defaults (`max_uncommitted` 64, D-82 A) and the fixed sizes of 7a.
    /// Flow control is off and cannot be turned on from here (D-102 A).
    /// Checked before it is returned.
    ///
    /// # Errors
    /// What [`NodeConfig::check`] refuses.
    pub fn node_config(&self) -> Result<NodeConfig, ConfigError> {
        let nodes: Vec<u8> = self.voters.iter().map(|&(id, _)| id).collect();
        let gateway = gateway::Config {
            node: self.id,
            first_session: 0,
            codec: codec::Limits {
                max_packet_bytes: MAX_PACKET_BYTES,
                ..codec::Limits::default()
            },
            max_connections: 1024,
            connect_timeout: Duration::from_secs(10),
            max_pending: 64,
            max_pending_bytes: 1 << 20,
            max_total_pending_bytes: 64 << 20,
            commit_timeout: Duration::from_secs(30),
            retry_interval: Duration::from_millis(200),
            receive_maximum: NonZeroU16::MIN.saturating_add(63),
            topic_alias_maximum: 16,
            max_waiting: 1024,
            max_waiting_bytes: 1 << 20,
            max_total_waiting_bytes: 64 << 20,
            max_retries: 1024,
            // The node sets it from `MqttConfig::authority`.
            authenticate: false,
        };
        let config = NodeConfig {
            core: cluster::Config {
                id: self.id,
                nodes,
                owner: self.voters.iter().map(|&(id, _)| id).min().unwrap_or(0),
                slot: 1,
                window: 256,
                max_queue: 1024,
                max_batch: MAX_BATCH,
                recent_batches: 64,
                max_uncommitted: cluster::DEFAULT_MAX_UNCOMMITTED,
                takeover_ticks: self.takeover_ticks,
                lag_bound: None,
            },
            listen: self.listen,
            peers: self
                .voters
                .iter()
                .filter(|&&(id, _)| id != self.id)
                .copied()
                .collect(),
            data: self.data.clone(),
            segments: SegmentLimits {
                wal: WalLimits::default(),
                segment_bytes: 64 << 20,
            },
            limits: apply::Limits::default(),
            heartbeat: self.heartbeat,
            max_frame: MAX_FRAME,
            link_queue: 1024,
            transport: TransportConfig {
                listen: self.listen,
                ..TransportConfig::default()
            },
            mqtt: Some(MqttConfig {
                listen: self.mqtt,
                gateway,
                write_queue: 1024,
                authority: None,
                tls: None,
            }),
            retain_bytes: self.retain_bytes,
            snapshot_every: 4096,
            rotate_every: None,
            read_bytes: self.read_bytes,
            metrics: self.metrics,
        };
        config.check()?;
        Ok(config)
    }
}
