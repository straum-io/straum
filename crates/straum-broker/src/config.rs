//! Broker configuration: a file of `key = value` lines (`#` starts a
//! comment) and `--key value` flags with the same names, flags last. Every
//! key, its default and its meaning: `crates/straum-broker/README.md`.

use std::net::SocketAddr;
use std::path::PathBuf;

use straum_store::WriteMode;

#[derive(Clone, Debug)]
pub struct Config {
    pub data_dir: PathBuf,
    pub listen: SocketAddr,
    /// Host and port other nodes and clients use; default: `listen`.
    pub advertised_host: Option<String>,
    pub advertised_port: Option<u16>,
    pub cluster_id: String,
    pub node_id: u32,
    pub max_frame_bytes: u32,
    pub max_message_bytes: u32,
    pub max_key_bytes: u32,
    pub max_headers: u32,
    pub max_header_name_bytes: u32,
    pub max_header_bytes: u32,
    pub max_group_bytes: u64,
    pub max_group_messages: u32,
    pub max_inflight_bytes: u64,
    pub max_pending_bytes: u64,
    pub max_fetch_bytes: u32,
    pub max_fetch_wait_ms: u32,
    pub max_fetch_waiters: u32,
    pub max_fetch_messages: u32,
    pub max_connections: u32,
    pub shutdown_timeout_ms: u32,
    pub store_write_mode: WriteMode,
    pub store_segment_max_bytes: u64,
    pub store_max_frame_bytes: u32,
    /// Rebuild the store's derived topic index from its WAL at this start
    /// (`straum_store::Config::rebuild_index`). Only the bare command-line
    /// flag `--store-rebuild-index` sets it, so it applies to one start.
    pub store_rebuild_index: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::new(),
            listen: SocketAddr::from(([127, 0, 0, 1], 7171)),
            advertised_host: None,
            advertised_port: None,
            cluster_id: String::new(),
            node_id: 0,
            max_frame_bytes: 8 << 20,
            max_message_bytes: 1 << 20,
            max_key_bytes: 1024,
            max_headers: 64,
            max_header_name_bytes: 256,
            max_header_bytes: 16 << 10,
            max_group_bytes: 4 << 20,
            max_group_messages: 4096,
            max_inflight_bytes: 16 << 20,
            max_pending_bytes: 64 << 20,
            max_fetch_bytes: 4 << 20,
            max_fetch_wait_ms: 30_000,
            max_fetch_waiters: 1024,
            max_fetch_messages: 10_000,
            max_connections: 1024,
            shutdown_timeout_ms: 5000,
            store_write_mode: WriteMode::Sequential,
            store_segment_max_bytes: 64 << 20,
            store_max_frame_bytes: 64 << 20,
            store_rebuild_index: false,
        }
    }
}

impl Config {
    /// Applies one `key = value` setting.
    pub fn set(&mut self, key: &str, value: &str) -> Result<(), String> {
        let key = key.trim().replace('-', "_");
        let value = value.trim();
        fn num<T: std::str::FromStr>(key: &str, v: &str) -> Result<T, String> {
            v.parse()
                .map_err(|_| format!("{key}: not a valid number: {v}"))
        }
        match key.as_str() {
            "data_dir" => self.data_dir = value.into(),
            "listen" => {
                self.listen = value
                    .parse()
                    .map_err(|_| format!("listen: not an address: {value}"))?
            }
            "advertised_host" => self.advertised_host = Some(value.to_owned()),
            "advertised_port" => self.advertised_port = Some(num(&key, value)?),
            "cluster_id" => self.cluster_id = value.to_owned(),
            "node_id" => self.node_id = num(&key, value)?,
            "max_frame_bytes" => self.max_frame_bytes = num(&key, value)?,
            "max_message_bytes" => self.max_message_bytes = num(&key, value)?,
            "max_key_bytes" => self.max_key_bytes = num(&key, value)?,
            "max_headers" => self.max_headers = num(&key, value)?,
            "max_header_name_bytes" => self.max_header_name_bytes = num(&key, value)?,
            "max_header_bytes" => self.max_header_bytes = num(&key, value)?,
            "max_group_bytes" => self.max_group_bytes = num(&key, value)?,
            "max_group_messages" => self.max_group_messages = num(&key, value)?,
            "max_inflight_bytes" => self.max_inflight_bytes = num(&key, value)?,
            "max_pending_bytes" => self.max_pending_bytes = num(&key, value)?,
            "max_fetch_bytes" => self.max_fetch_bytes = num(&key, value)?,
            "max_fetch_wait_ms" => self.max_fetch_wait_ms = num(&key, value)?,
            "max_fetch_waiters" => self.max_fetch_waiters = num(&key, value)?,
            "max_fetch_messages" => self.max_fetch_messages = num(&key, value)?,
            "max_connections" => self.max_connections = num(&key, value)?,
            "shutdown_timeout_ms" => self.shutdown_timeout_ms = num(&key, value)?,
            "store_write_mode" => {
                self.store_write_mode = match value {
                    "sequential" => WriteMode::Sequential,
                    "parallel" => WriteMode::ParallelSync,
                    "inline" => WriteMode::INLINE_SMALL,
                    _ => {
                        return Err(format!(
                            "store_write_mode: sequential, parallel or inline, not {value}"
                        ));
                    }
                }
            }
            "store_segment_max_bytes" => self.store_segment_max_bytes = num(&key, value)?,
            "store_max_frame_bytes" => self.store_max_frame_bytes = num(&key, value)?,
            // A one-shot operator action, never a standing setting: a
            // configuration file line would rebuild at every start.
            "store_rebuild_index" => {
                return Err(format!(
                    "{key} is not a setting: pass the flag --store-rebuild-index \
                     (no value) on the command line of the one start that rebuilds"
                ));
            }
            _ => return Err(format!("unknown setting {key}")),
        }
        Ok(())
    }

    /// Applies the lines of a configuration file.
    pub fn apply_file(&mut self, text: &str) -> Result<(), String> {
        for (i, line) in text.lines().enumerate() {
            let line = line.split('#').next().unwrap_or("").trim();
            if line.is_empty() {
                continue;
            }
            let (k, v) = line
                .split_once('=')
                .ok_or_else(|| format!("line {}: expected key = value", i + 1))?;
            self.set(k, v).map_err(|e| format!("line {}: {e}", i + 1))?;
        }
        Ok(())
    }

    /// `--config <file>` first, then every other `--key value` flag.
    pub fn from_args(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let args: Vec<String> = args.into_iter().collect();
        let mut config = Self::default();
        let mut pairs = Vec::new();
        let mut it = args.iter();
        while let Some(flag) = it.next() {
            if flag == "--store-rebuild-index" {
                config.store_rebuild_index = true;
                continue;
            }
            let key = flag
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {flag}"))?;
            let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
            pairs.push((key, value));
        }
        for (k, v) in &pairs {
            if *k == "config" {
                let text = std::fs::read_to_string(v).map_err(|e| format!("{v}: {e}"))?;
                config.apply_file(&text).map_err(|e| format!("{v}: {e}"))?;
            }
        }
        for (k, v) in pairs {
            if k != "config" {
                config.set(k, v)?;
            }
        }
        config.validate()?;
        Ok(config)
    }

    /// Bounds that keep every limit reachable: a frame always fits the
    /// per-connection and global budgets, so a reader never waits for room
    /// that cannot exist.
    pub fn validate(&self) -> Result<(), String> {
        let frame = u64::from(self.max_frame_bytes);
        let checks = [
            (
                !self.data_dir.as_os_str().is_empty(),
                "data_dir is required",
            ),
            (
                !self.cluster_id.is_empty() && self.cluster_id.len() <= 64,
                "cluster_id is required, at most 64 bytes",
            ),
            (self.node_id != 0, "node_id is required and not 0"),
            // Version 0 authenticates no client, and admin requests share
            // the listener: it never leaves loopback.
            (
                self.listen.ip().is_loopback(),
                "listen is a loopback address (version 0 has no authentication)",
            ),
            (self.max_frame_bytes >= 64, "max_frame_bytes is at least 64"),
            (
                self.max_message_bytes <= self.max_frame_bytes,
                "max_message_bytes is at most max_frame_bytes",
            ),
            (
                self.max_key_bytes <= u32::from(u16::MAX),
                "max_key_bytes is at most 65535",
            ),
            (
                self.max_header_name_bytes <= u32::from(u16::MAX)
                    && self.max_headers <= u32::from(u16::MAX),
                "max_header_name_bytes and max_headers are at most 65535",
            ),
            (
                self.max_inflight_bytes >= frame + u64::from(self.max_fetch_bytes),
                "max_inflight_bytes is at least max_frame_bytes + max_fetch_bytes",
            ),
            (
                self.max_pending_bytes >= frame,
                "max_pending_bytes is at least max_frame_bytes",
            ),
            (
                self.max_group_messages >= 1,
                "max_group_messages is at least 1",
            ),
            (self.max_connections >= 1, "max_connections is at least 1"),
            (
                self.max_fetch_messages >= 1,
                "max_fetch_messages is at least 1",
            ),
            (
                u64::from(self.store_max_frame_bytes) >= frame,
                "store_max_frame_bytes is at least max_frame_bytes",
            ),
        ];
        for (ok, what) in checks {
            if !ok {
                return Err(what.to_owned());
            }
        }
        Ok(())
    }

    pub fn advertised(&self) -> (String, u16) {
        (
            self.advertised_host
                .clone()
                .unwrap_or_else(|| self.listen.ip().to_string()),
            self.advertised_port.unwrap_or(self.listen.port()),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_then_flags() {
        let dir = std::env::temp_dir().join(format!("straum-broker-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("broker.conf");
        std::fs::write(
            &file,
            "# a comment\ndata_dir = /tmp/x\ncluster_id = c1\nnode_id = 3 # trailing\nmax-fetch-wait-ms = 10\n",
        )
        .unwrap();
        let c = Config::from_args(
            ["--node-id", "4", "--config", file.to_str().unwrap()].map(String::from),
        )
        .unwrap();
        assert_eq!(
            (c.node_id, c.max_fetch_wait_ms),
            (4, 10),
            "flags override the file"
        );
        assert_eq!(c.cluster_id, "c1");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn store_rebuild_index_is_a_bare_flag_only() {
        let base = ["--data-dir", "/d", "--cluster-id", "c", "--node-id", "1"];
        let args = |extra: &[&str]| -> Vec<String> {
            base.iter().chain(extra).map(|s| s.to_string()).collect()
        };
        assert!(!Config::from_args(args(&[])).unwrap().store_rebuild_index);
        let c = Config::from_args(args(&["--store-rebuild-index"])).unwrap();
        assert!(c.store_rebuild_index);
        let c = Config::from_args(args(&["--store-rebuild-index", "--node-id", "2"])).unwrap();
        assert!(c.store_rebuild_index && c.node_id == 2);
        // In a configuration file, or as a `key value` flag, it is refused
        // and the message names the flag.
        for (k, v) in [
            ("store_rebuild_index", "true"),
            ("store-rebuild-index", "false"),
        ] {
            let err = Config::default().set(k, v).unwrap_err();
            assert!(err.contains("--store-rebuild-index"), "{err}");
        }
        let err = Config::default()
            .apply_file("store_rebuild_index = true\n")
            .unwrap_err();
        assert!(
            err.starts_with("line 1: ") && err.contains("--store-rebuild-index"),
            "{err}"
        );
        assert!(Config::from_args(args(&["--store-rebuild-index", "true"])).is_err());
    }

    #[test]
    fn inconsistent_limits_are_refused() {
        let base = || Config {
            data_dir: "/d".into(),
            cluster_id: "c".into(),
            node_id: 1,
            ..Config::default()
        };
        assert!(base().validate().is_ok());
        let mut c = base();
        c.max_inflight_bytes = 1;
        assert!(c.validate().is_err());
        let mut c = base();
        c.node_id = 0;
        assert!(c.validate().is_err());
        for addr in ["0.0.0.0:7171", "192.168.1.2:7171", "[::]:7171"] {
            let mut c = base();
            c.set("listen", addr).unwrap();
            assert!(c.validate().is_err(), "{addr}");
        }
        let mut c = base();
        c.set("listen", "[::1]:0").unwrap();
        assert!(c.validate().is_ok());
        assert!(Config::default().set("nope", "1").is_err());
        assert!(Config::default().set("store_write_mode", "fast").is_err());
    }
}
