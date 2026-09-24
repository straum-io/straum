//! `STRAUM_*` into typed settings.
//!
//! Three rules from docs/configuration.md drive the shape of this file: an
//! unknown variable is an error, a malformed or out-of-range value is an
//! error, and `auto` is resolved from the kernel at startup. None of them
//! allows a silent fallback — a typo that turns into a default is a
//! production incident waiting for a quiet night.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use runtime::{
    AuthSettings, BrokerLimits, Durability, KernelLimits, MqttLimits, RuntimeConfig, TlsSettings,
    TransportConfig, WalLimits, WalSettings, reuseport_balances,
};
/// Descriptors kept for listeners, logs and the runtime itself.
const RESERVED_DESCRIPTORS: u64 = 64;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("{name}: cannot read {path}: {reason}")]
    Unreadable {
        name: &'static str,
        path: String,
        reason: String,
    },
    #[error("unknown variable {0}; a typo must not fall back to a default")]
    Unknown(String),
    #[error("{name}: {value:?} is not {expected}")]
    Malformed {
        name: &'static str,
        value: String,
        expected: &'static str,
    },
    #[error("{name}: {value} is outside {min}..={max}")]
    OutOfRange {
        name: &'static str,
        value: u64,
        min: u64,
        max: u64,
    },
    #[error("{name} ({value}) must be at least {other} ({limit})")]
    TooSmallFor {
        name: &'static str,
        value: u64,
        other: &'static str,
        limit: u64,
    },
}

/// Where a value came from, for the one startup log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Env,
    Default,
    Kernel,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub runtime: RuntimeConfig,
    pub log_level: String,
    pub worker_threads: Option<usize>,
    /// Options that were set but cannot apply on this platform.
    pub ignored: Vec<&'static str>,
    pub effective: Vec<(&'static str, String, Source)>,
}

struct Parser {
    vars: BTreeMap<String, String>,
    used: Vec<&'static str>,
    kernel: KernelLimits,
    effective: Vec<(&'static str, String, Source)>,
}

impl Settings {
    /// Parses from an iterator rather than the process environment, so tests
    /// do not have to fight over a global.
    pub fn parse(
        vars: impl IntoIterator<Item = (String, String)>,
        kernel: KernelLimits,
    ) -> Result<Self, ConfigError> {
        let mut parser = Parser {
            vars: vars
                .into_iter()
                .filter(|(name, _)| name.starts_with("STRAUM_"))
                .collect(),
            used: Vec::new(),
            kernel,
            effective: Vec::new(),
        };

        let listen: SocketAddr = parser.parse_value(
            "STRAUM_LISTEN_ADDR",
            SocketAddr::from(([0, 0, 0, 0], 1883)),
            "a socket address",
        )?;
        let mut ignored: Vec<&'static str> = Vec::new();
        let requested_listeners = parser.number("STRAUM_LISTENERS", 1u32, 1, 4096)?;
        // BSD SO_REUSEPORT hands every connection to the last binder, so
        // more listeners there would be more idle sockets (§13).
        let listeners = if requested_listeners > 1 && !reuseport_balances() {
            ignored.push("STRAUM_LISTENERS");
            1
        } else {
            requested_listeners
        };
        let worker_threads = parser.auto_usize("STRAUM_WORKER_THREADS", || {
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
        })?;
        let log_level = parser.parse_value("STRAUM_LOG_LEVEL", "info".to_owned(), "a log level")?;
        let shutdown_grace = parser.millis("STRAUM_SHUTDOWN_GRACE_MS", 5_000)?;

        let max_packet_bytes =
            parser.number("STRAUM_MAX_PACKET_BYTES", 1_048_576, 2, 268_435_460)?;

        let backlog = parser.auto_u32("STRAUM_BACKLOG", 1024, |kernel| kernel.somaxconn)?;
        let max_connections =
            parser.auto_usize_from("STRAUM_MAX_CONNECTIONS", 1024, |kernel, fallback| {
                kernel.max_connections(fallback, RESERVED_DESCRIPTORS)
            })?;
        let nodelay = parser.boolean("STRAUM_TCP_NODELAY", true)?;
        let keepalive_secs = parser.number("STRAUM_TCP_KEEPALIVE_SECS", 300, 0, 86_400)?;
        let read_buffer_bytes = parser.number(
            "STRAUM_READ_BUFFER_BYTES",
            16_384,
            1024,
            max_packet_bytes as u64,
        )?;
        let total_read_bytes = parser.number(
            "STRAUM_TOTAL_READ_BYTES",
            1_073_741_824,
            1024,
            u64::from(u32::MAX) * 16,
        )?;
        let write_stall = parser.millis("STRAUM_WRITE_STALL_TIMEOUT_MS", 30_000)?;
        let so_rcvbuf_bytes = parser.number("STRAUM_SO_RCVBUF_BYTES", 0usize, 0, 1 << 30)?;
        let password_file =
            parser.parse_value("STRAUM_AUTH_PASSWORD_FILE", String::new(), "a path")?;
        let acl_file = parser.parse_value("STRAUM_AUTH_ACL_FILE", String::new(), "a path")?;
        let allow_anonymous = parser.boolean("STRAUM_AUTH_ALLOW_ANONYMOUS", true)?;
        let certificate = parser.parse_value("STRAUM_TLS_CERT_PATH", String::new(), "a path")?;
        let key = parser.parse_value("STRAUM_TLS_KEY_PATH", String::new(), "a path")?;
        // Half a TLS setup is not a degraded TLS setup, it is plaintext
        // nobody asked for.
        let tls = match (certificate.is_empty(), key.is_empty()) {
            (true, true) => None,
            (false, false) => Some(TlsSettings {
                certificate: PathBuf::from(certificate),
                key: PathBuf::from(key),
            }),
            (true, false) => {
                return Err(ConfigError::Malformed {
                    name: "STRAUM_TLS_CERT_PATH",
                    value: String::new(),
                    expected: "a path, because STRAUM_TLS_KEY_PATH is set",
                });
            }
            (false, true) => {
                return Err(ConfigError::Malformed {
                    name: "STRAUM_TLS_KEY_PATH",
                    value: String::new(),
                    expected: "a path, because STRAUM_TLS_CERT_PATH is set",
                });
            }
        };
        let so_sndbuf_bytes = parser.number("STRAUM_SO_SNDBUF_BYTES", 0usize, 0, 1 << 30)?;
        let user_timeout = parser.millis("STRAUM_TCP_USER_TIMEOUT_MS", 0)?;
        let tcp_user_timeout = if user_timeout.is_zero() {
            None
        } else if reuseport_balances() {
            Some(user_timeout)
        } else {
            // Linux only; the same env file has to work on a laptop.
            ignored.push("STRAUM_TCP_USER_TIMEOUT_MS");
            None
        };

        let mqtt = MqttLimits {
            connect_timeout: parser.millis("STRAUM_CONNECT_TIMEOUT_MS", 10_000)?,
            max_packet_bytes,
            max_topic_bytes: parser.number("STRAUM_MAX_TOPIC_BYTES", 1024, 1, 65_535)?,
            max_topic_levels: parser.number("STRAUM_MAX_TOPIC_LEVELS", 32, 1, 65_535)?,
            max_client_id_bytes: parser.number("STRAUM_MAX_CLIENT_ID_BYTES", 256, 23, 65_535)?,
            max_filters_per_subscribe: parser.number(
                "STRAUM_MAX_FILTERS_PER_SUBSCRIBE",
                64,
                1,
                65_535,
            )?,
            payload_copy_threshold_bytes: parser.number(
                "STRAUM_PAYLOAD_COPY_THRESHOLD_BYTES",
                512,
                0,
                max_packet_bytes as u64,
            )?,
        };

        let broker = BrokerLimits {
            max_subscriptions_per_session: parser.number(
                "STRAUM_MAX_SUBSCRIPTIONS_PER_SESSION",
                1024,
                1,
                1_048_576,
            )?,
            max_inflight: parser.number("STRAUM_MAX_INFLIGHT", 32, 1, 65_534)?,
            max_offline_sessions: parser.number(
                "STRAUM_MAX_OFFLINE_SESSIONS",
                4096,
                0,
                1_048_576,
            )?,
            max_retained_messages: parser.number(
                "STRAUM_MAX_RETAINED_MESSAGES",
                10_000,
                0,
                1_048_576,
            )?,
            max_retained_bytes: parser.number(
                "STRAUM_MAX_RETAINED_BYTES",
                67_108_864,
                0,
                u64::MAX >> 8,
            )?,
            session_backlog_messages: parser.number(
                "STRAUM_SESSION_BACKLOG_MESSAGES",
                1024,
                1,
                1_048_576,
            )?,
        };

        let session_queue_bytes =
            parser.number("STRAUM_SESSION_QUEUE_BYTES", 8_388_608, 1024, u64::MAX >> 8)?;
        let total_queued_bytes = parser.number(
            "STRAUM_TOTAL_QUEUED_BYTES",
            1_073_741_824,
            1024,
            u64::MAX >> 8,
        )?;

        let wal = WalLimits {
            max_record_bytes: parser.number(
                "STRAUM_WAL_MAX_RECORD_BYTES",
                1_048_576,
                2,
                268_435_460,
            )?,
            min_batch_bytes: parser.number(
                "STRAUM_WAL_MIN_BATCH_BYTES",
                65_536,
                1024,
                u32::MAX as u64,
            )?,
            max_batch_bytes: parser.number(
                "STRAUM_WAL_MAX_BATCH_BYTES",
                4_194_304,
                1024,
                u32::MAX as u64,
            )?,
            initial_batch_bytes: parser.number(
                "STRAUM_WAL_INITIAL_BATCH_BYTES",
                1_048_576,
                1024,
                u32::MAX as u64,
            )?,
        };

        let settings = Self {
            runtime: RuntimeConfig {
                transport: TransportConfig {
                    listen,
                    backlog,
                    nodelay,
                    keepalive: (keepalive_secs > 0).then(|| Duration::from_secs(keepalive_secs)),
                    read_buffer_bytes,
                    total_read_bytes,
                    max_connections,
                    listeners,
                    so_rcvbuf_bytes,
                    so_sndbuf_bytes,
                    tcp_user_timeout,
                },
                mqtt,
                broker,
                session_queue_messages: parser.number(
                    "STRAUM_SESSION_QUEUE_MESSAGES",
                    1024,
                    1,
                    1_048_576,
                )?,
                session_queue_bytes,
                total_queued_bytes,
                broker_queue: parser.number("STRAUM_BROKER_QUEUE", 65_536, 1, 16_777_216)?,
                broker_batch: parser.number("STRAUM_BROKER_BATCH", 256, 1, 65_536)?,
                writer_batch: parser.number("STRAUM_WRITER_BATCH", 64, 1, 65_536)?,
                shutdown_grace,
                write_stall_timeout: write_stall,
                durability: parser.durability("STRAUM_DURABILITY", Durability::Memory)?,
                auth: AuthSettings {
                    passwords: read(&password_file, "STRAUM_AUTH_PASSWORD_FILE")?,
                    acl: read(&acl_file, "STRAUM_AUTH_ACL_FILE")?,
                    allow_anonymous,
                },
                tls,
                wal: WalSettings {
                    path: PathBuf::from(parser.parse_value(
                        "STRAUM_WAL_PATH",
                        "straum.wal".to_owned(),
                        "a path",
                    )?),
                    queue: parser.number("STRAUM_WAL_QUEUE", 65_536, 1, 16_777_216)?,
                    limits: wal,
                },
            },
            effective: Vec::new(),
            log_level,
            worker_threads,
            ignored,
        };

        let effective = parser.finish()?;
        check_cross_fields(&settings)?;
        Ok(Self {
            effective,
            ..settings
        })
    }
}

/// Constraints that involve two variables. The error names both, because
/// "out of range" without the other half is a riddle.
fn check_cross_fields(settings: &Settings) -> Result<(), ConfigError> {
    let packet = settings.runtime.mqtt.max_packet_bytes as u64;

    for (name, value) in [
        (
            "STRAUM_SESSION_QUEUE_BYTES",
            settings.runtime.session_queue_bytes as u64,
        ),
        (
            "STRAUM_TOTAL_QUEUED_BYTES",
            settings.runtime.total_queued_bytes as u64,
        ),
        (
            "STRAUM_TOTAL_READ_BYTES",
            settings.runtime.transport.total_read_bytes as u64,
        ),
        (
            "STRAUM_WAL_MAX_RECORD_BYTES",
            settings.runtime.wal.limits.max_record_bytes as u64,
        ),
    ] {
        if value < packet {
            return Err(ConfigError::TooSmallFor {
                name,
                value,
                other: "STRAUM_MAX_PACKET_BYTES",
                limit: packet,
            });
        }
    }

    if settings.runtime.wal.limits.max_batch_bytes < settings.runtime.wal.limits.min_batch_bytes {
        return Err(ConfigError::TooSmallFor {
            name: "STRAUM_WAL_MAX_BATCH_BYTES",
            value: settings.runtime.wal.limits.max_batch_bytes as u64,
            other: "STRAUM_WAL_MIN_BATCH_BYTES",
            limit: settings.runtime.wal.limits.min_batch_bytes as u64,
        });
    }
    if settings.runtime.wal.limits.initial_batch_bytes < settings.runtime.wal.limits.min_batch_bytes
    {
        return Err(ConfigError::TooSmallFor {
            name: "STRAUM_WAL_INITIAL_BATCH_BYTES",
            value: settings.runtime.wal.limits.initial_batch_bytes as u64,
            other: "STRAUM_WAL_MIN_BATCH_BYTES",
            limit: settings.runtime.wal.limits.min_batch_bytes as u64,
        });
    }
    if settings.runtime.wal.limits.initial_batch_bytes > settings.runtime.wal.limits.max_batch_bytes
    {
        return Err(ConfigError::TooSmallFor {
            name: "STRAUM_WAL_MAX_BATCH_BYTES",
            value: settings.runtime.wal.limits.max_batch_bytes as u64,
            other: "STRAUM_WAL_INITIAL_BATCH_BYTES",
            limit: settings.runtime.wal.limits.initial_batch_bytes as u64,
        });
    }
    Ok(())
}

impl Parser {
    fn record<T: std::fmt::Display>(&mut self, name: &'static str, value: T, source: Source) {
        self.used.push(name);
        self.effective.push((name, value.to_string(), source));
    }

    fn raw(&self, name: &'static str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    fn parse_value<T>(
        &mut self,
        name: &'static str,
        default: T,
        expected: &'static str,
    ) -> Result<T, ConfigError>
    where
        T: std::str::FromStr + std::fmt::Display + Clone,
    {
        match self.raw(name) {
            None => {
                self.record(name, default.clone(), Source::Default);
                Ok(default)
            }
            Some(raw) => {
                let parsed = raw.parse::<T>().map_err(|_| ConfigError::Malformed {
                    name,
                    value: raw.to_owned(),
                    expected,
                })?;
                self.record(name, parsed.clone(), Source::Env);
                Ok(parsed)
            }
        }
    }

    fn number<T>(
        &mut self,
        name: &'static str,
        default: T,
        min: u64,
        max: u64,
    ) -> Result<T, ConfigError>
    where
        T: TryFrom<u64> + Copy + std::fmt::Display,
    {
        let value: u64 = match self.raw(name) {
            None => {
                self.record(name, default, Source::Default);
                return Ok(default);
            }
            Some(raw) => raw.parse().map_err(|_| ConfigError::Malformed {
                name,
                value: raw.to_owned(),
                expected: "a number",
            })?,
        };

        if value < min || value > max {
            return Err(ConfigError::OutOfRange {
                name,
                value,
                min,
                max,
            });
        }
        let value = T::try_from(value).map_err(|_| ConfigError::OutOfRange {
            name,
            value,
            min,
            max,
        })?;
        self.record(name, value, Source::Env);
        Ok(value)
    }

    fn boolean(&mut self, name: &'static str, default: bool) -> Result<bool, ConfigError> {
        match self.raw(name) {
            None => {
                self.record(name, default, Source::Default);
                Ok(default)
            }
            Some("true") => {
                self.record(name, true, Source::Env);
                Ok(true)
            }
            Some("false") => {
                self.record(name, false, Source::Env);
                Ok(false)
            }
            Some(raw) => Err(ConfigError::Malformed {
                name,
                value: raw.to_owned(),
                expected: "`true` or `false`",
            }),
        }
    }

    fn millis(&mut self, name: &'static str, default: u64) -> Result<Duration, ConfigError> {
        let millis: u64 = self.number(name, default, 0, 86_400_000)?;
        Ok(Duration::from_millis(millis))
    }

    fn durability(
        &mut self,
        name: &'static str,
        default: Durability,
    ) -> Result<Durability, ConfigError> {
        let raw = match self.raw(name) {
            None => {
                self.record(name, format!("{default:?}").to_lowercase(), Source::Default);
                return Ok(default);
            }
            Some(raw) => raw.to_owned(),
        };
        let parsed = match raw.as_str() {
            "memory" => Durability::Memory,
            // Accepting it would mean promising a guarantee nothing provides.
            "written" => {
                return Err(ConfigError::Malformed {
                    name,
                    value: raw,
                    expected: "`memory` or `fsynced`; `written` is not implemented yet",
                });
            }

            "fsynced" => Durability::Fsynced,
            _ => {
                return Err(ConfigError::Malformed {
                    name,
                    value: raw,
                    expected: "`memory`, `written` or `fsynced`",
                });
            }
        };
        self.record(name, raw, Source::Env);
        Ok(parsed)
    }

    /// `auto` resolves from the kernel, or from the documented fallback where
    /// there is no kernel to ask (macOS, a restricted container).
    fn auto_u32(
        &mut self,
        name: &'static str,
        fallback: u32,
        from: impl Fn(&KernelLimits) -> Option<u32>,
    ) -> Result<u32, ConfigError> {
        match self.raw(name) {
            Some("auto") | None => {
                let (value, source) = match from(&self.kernel) {
                    Some(value) => (value, Source::Kernel),
                    None => (fallback, Source::Default),
                };
                self.record(name, value, source);
                Ok(value)
            }
            Some(_) => self.number(name, fallback, 1, u64::from(u32::MAX)),
        }
    }

    fn auto_usize_from(
        &mut self,
        name: &'static str,
        fallback: usize,
        from: impl Fn(&KernelLimits, usize) -> usize,
    ) -> Result<usize, ConfigError> {
        match self.raw(name) {
            Some("auto") | None => {
                let resolved = from(&self.kernel, fallback);
                let source = if resolved == fallback {
                    Source::Default
                } else {
                    Source::Kernel
                };
                self.record(name, resolved, source);
                Ok(resolved)
            }
            Some(_) => self.number(name, fallback, 1, 1 << 32),
        }
    }

    fn auto_usize(
        &mut self,
        name: &'static str,
        resolve: impl Fn() -> usize,
    ) -> Result<Option<usize>, ConfigError> {
        match self.raw(name) {
            Some("auto") | None => {
                let value = resolve();
                self.record(name, value, Source::Default);
                Ok(Some(value))
            }
            Some(_) => Ok(Some(self.number(name, 1usize, 1, 4096)?)),
        }
    }

    /// Anything left over was never asked for, which means nobody will ever
    /// act on it.
    fn finish(mut self) -> Result<Vec<(&'static str, String, Source)>, ConfigError> {
        if let Some(unknown) = self
            .vars
            .keys()
            .find(|name| !self.used.iter().any(|used| used == name))
        {
            return Err(ConfigError::Unknown(unknown.clone()));
        }
        self.effective.sort_by_key(|(name, _, _)| *name);
        Ok(std::mem::take(&mut self.effective))
    }
}

/// Reads a configured file, or nothing when the path is empty. A path that
/// was given and cannot be read is a startup error: coming up with no rules
/// because a file was missing is how a broker ends up open.
fn read(path: &str, name: &'static str) -> Result<String, ConfigError> {
    if path.is_empty() {
        return Ok(String::new());
    }
    std::fs::read_to_string(path).map_err(|error| ConfigError::Unreadable {
        name,
        path: path.to_owned(),
        reason: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn parse(pairs: &[(&str, &str)]) -> Result<Settings, ConfigError> {
        Settings::parse(vars(pairs), KernelLimits::default())
    }

    #[test]
    fn an_empty_environment_gives_the_documented_defaults() {
        let settings = parse(&[]).unwrap();

        assert_eq!(settings.runtime.transport.listen.port(), 1883);
        assert_eq!(settings.runtime.transport.backlog, 1024);
        assert_eq!(settings.runtime.mqtt.max_packet_bytes, 1_048_576);
        assert_eq!(settings.runtime.broker.max_inflight, 32);
        assert_eq!(settings.runtime.broker_batch, 256);
        assert_eq!(
            settings.runtime.shutdown_grace,
            Duration::from_millis(5_000)
        );
        assert_eq!(settings.runtime.durability, Durability::Memory);
        assert!(
            settings
                .effective
                .iter()
                .all(|(_, _, source)| *source != Source::Env)
        );
    }

    #[test]
    fn values_come_from_the_environment_when_it_has_them() {
        let settings = parse(&[
            ("STRAUM_LISTEN_ADDR", "127.0.0.1:18830"),
            ("STRAUM_MAX_INFLIGHT", "64"),
            ("STRAUM_TCP_NODELAY", "false"),
            ("STRAUM_DURABILITY", "fsynced"),
        ])
        .unwrap();

        assert_eq!(settings.runtime.transport.listen.port(), 18830);
        assert_eq!(settings.runtime.broker.max_inflight, 64);
        assert!(!settings.runtime.transport.nodelay);
        assert_eq!(settings.runtime.durability, Durability::Fsynced);

        let source = settings
            .effective
            .iter()
            .find(|(name, _, _)| *name == "STRAUM_MAX_INFLIGHT")
            .map(|(_, _, source)| *source);
        assert_eq!(source, Some(Source::Env));
    }

    #[test]
    fn a_typo_is_an_error_not_a_default() {
        // One character away from STRAUM_MAX_INFLIGHT.
        let error = parse(&[("STRAUM_MAX_INFLIGH", "64")]).unwrap_err();
        assert_eq!(error, ConfigError::Unknown("STRAUM_MAX_INFLIGH".to_owned()));

        // Variables that are not ours are left alone.
        assert!(
            Settings::parse(
                vars(&[("PATH", "/usr/bin"), ("HOME", "/root")]),
                KernelLimits::default()
            )
            .is_ok()
        );
    }

    #[test]
    fn malformed_values_are_errors() {
        assert!(matches!(
            parse(&[("STRAUM_MAX_INFLIGHT", "lots")]).unwrap_err(),
            ConfigError::Malformed {
                name: "STRAUM_MAX_INFLIGHT",
                ..
            }
        ));
        // Booleans are true/false, not 1/0.
        assert!(matches!(
            parse(&[("STRAUM_TCP_NODELAY", "1")]).unwrap_err(),
            ConfigError::Malformed {
                name: "STRAUM_TCP_NODELAY",
                ..
            }
        ));
        assert!(matches!(
            parse(&[("STRAUM_LISTEN_ADDR", "1883")]).unwrap_err(),
            ConfigError::Malformed {
                name: "STRAUM_LISTEN_ADDR",
                ..
            }
        ));
        assert!(matches!(
            parse(&[("STRAUM_DURABILITY", "safe")]).unwrap_err(),
            ConfigError::Malformed {
                name: "STRAUM_DURABILITY",
                ..
            }
        ));
    }

    #[test]
    fn out_of_range_values_are_errors() {
        for value in ["0", "65535"] {
            assert!(
                matches!(
                    parse(&[("STRAUM_MAX_INFLIGHT", value)]).unwrap_err(),
                    ConfigError::OutOfRange {
                        name: "STRAUM_MAX_INFLIGHT",
                        ..
                    }
                ),
                "{value} was accepted"
            );
        }

        // The read buffer cannot exceed the largest packet it has to hold.
        assert!(matches!(
            parse(&[
                ("STRAUM_MAX_PACKET_BYTES", "4096"),
                ("STRAUM_READ_BUFFER_BYTES", "8192"),
            ])
            .unwrap_err(),
            ConfigError::OutOfRange {
                name: "STRAUM_READ_BUFFER_BYTES",
                ..
            }
        ));
    }

    #[test]
    fn cross_field_errors_name_both_variables() {
        let error = parse(&[
            ("STRAUM_MAX_PACKET_BYTES", "1048576"),
            ("STRAUM_SESSION_QUEUE_BYTES", "4096"),
        ])
        .unwrap_err();

        assert_eq!(
            error,
            ConfigError::TooSmallFor {
                name: "STRAUM_SESSION_QUEUE_BYTES",
                value: 4096,
                other: "STRAUM_MAX_PACKET_BYTES",
                limit: 1_048_576,
            }
        );
        assert!(error.to_string().contains("STRAUM_MAX_PACKET_BYTES"));
    }

    #[test]
    fn the_wal_batch_bounds_have_to_make_sense_together() {
        assert!(matches!(
            parse(&[
                ("STRAUM_WAL_MIN_BATCH_BYTES", "1048576"),
                ("STRAUM_WAL_MAX_BATCH_BYTES", "65536"),
            ])
            .unwrap_err(),
            ConfigError::TooSmallFor {
                name: "STRAUM_WAL_MAX_BATCH_BYTES",
                ..
            }
        ));
        assert!(matches!(
            parse(&[("STRAUM_WAL_INITIAL_BATCH_BYTES", "8388608")]).unwrap_err(),
            ConfigError::TooSmallFor {
                name: "STRAUM_WAL_MAX_BATCH_BYTES",
                ..
            }
        ));
    }

    #[test]
    fn auto_asks_the_kernel_and_falls_back_when_there_is_none() {
        let kernel = KernelLimits {
            somaxconn: Some(8192),
            open_files: Some(1_048_576),
            ..KernelLimits::default()
        };
        let settings = Settings::parse(vars(&[("STRAUM_BACKLOG", "auto")]), kernel).unwrap();
        assert_eq!(settings.runtime.transport.backlog, 8192);
        assert_eq!(settings.runtime.transport.max_connections, 1_048_512);
        assert!(
            settings
                .effective
                .iter()
                .any(|(name, _, source)| *name == "STRAUM_BACKLOG" && *source == Source::Kernel)
        );

        // Nothing to ask: the documented fallback, marked as a default.
        let settings = parse(&[("STRAUM_BACKLOG", "auto")]).unwrap();
        assert_eq!(settings.runtime.transport.backlog, 1024);
        assert!(
            settings
                .effective
                .iter()
                .any(|(name, _, source)| *name == "STRAUM_BACKLOG" && *source == Source::Default)
        );

        // An explicit value wins over both.
        let settings = Settings::parse(vars(&[("STRAUM_BACKLOG", "256")]), kernel).unwrap();
        assert_eq!(settings.runtime.transport.backlog, 256);
    }

    #[test]
    fn written_is_refused_rather_than_pretended() {
        // Accepting it would promise a guarantee nothing provides.
        let error = parse(&[("STRAUM_DURABILITY", "written")]).unwrap_err();
        assert!(matches!(
            error,
            ConfigError::Malformed {
                name: "STRAUM_DURABILITY",
                ..
            }
        ));
        assert!(error.to_string().contains("not implemented"));

        assert!(parse(&[("STRAUM_DURABILITY", "fsynced")]).is_ok());
    }

    #[test]
    fn linux_only_options_are_ignored_off_linux_not_refused() {
        // The same env file has to work on the laptop and on the server.
        let settings = parse(&[
            ("STRAUM_LISTENERS", "4"),
            ("STRAUM_TCP_USER_TIMEOUT_MS", "5000"),
        ])
        .unwrap();

        if runtime::reuseport_balances() {
            assert_eq!(settings.runtime.transport.listeners, 4);
            assert_eq!(
                settings.runtime.transport.tcp_user_timeout,
                Some(Duration::from_millis(5000))
            );
            assert!(settings.ignored.is_empty());
        } else {
            assert_eq!(settings.runtime.transport.listeners, 1);
            assert_eq!(settings.runtime.transport.tcp_user_timeout, None);
            assert_eq!(
                settings.ignored,
                vec!["STRAUM_LISTENERS", "STRAUM_TCP_USER_TIMEOUT_MS"]
            );
        }
    }

    #[test]
    fn half_a_tls_setup_is_refused() {
        // Plaintext nobody asked for is worse than not starting.
        assert!(matches!(
            parse(&[("STRAUM_TLS_CERT_PATH", "/tmp/cert.pem")]).unwrap_err(),
            ConfigError::Malformed {
                name: "STRAUM_TLS_KEY_PATH",
                ..
            }
        ));
        assert!(matches!(
            parse(&[("STRAUM_TLS_KEY_PATH", "/tmp/key.pem")]).unwrap_err(),
            ConfigError::Malformed {
                name: "STRAUM_TLS_CERT_PATH",
                ..
            }
        ));

        assert!(parse(&[]).unwrap().runtime.tls.is_none());
        let both = parse(&[
            ("STRAUM_TLS_CERT_PATH", "/tmp/cert.pem"),
            ("STRAUM_TLS_KEY_PATH", "/tmp/key.pem"),
        ])
        .unwrap();
        assert!(both.runtime.tls.is_some());
    }

    #[test]
    fn socket_buffers_are_off_unless_asked_for() {
        let settings = parse(&[]).unwrap();
        assert_eq!(settings.runtime.transport.so_rcvbuf_bytes, 0);
        assert_eq!(settings.runtime.transport.so_sndbuf_bytes, 0);

        let settings = parse(&[("STRAUM_SO_RCVBUF_BYTES", "262144")]).unwrap();
        assert_eq!(settings.runtime.transport.so_rcvbuf_bytes, 262_144);
    }

    #[test]
    fn keepalive_zero_disables_it() {
        let settings = parse(&[("STRAUM_TCP_KEEPALIVE_SECS", "0")]).unwrap();
        assert_eq!(settings.runtime.transport.keepalive, None);

        let settings = parse(&[("STRAUM_TCP_KEEPALIVE_SECS", "60")]).unwrap();
        assert_eq!(
            settings.runtime.transport.keepalive,
            Some(Duration::from_secs(60))
        );
    }

    #[test]
    fn every_documented_variable_is_accepted() {
        // If this list and the parser disagree, one of them is wrong and the
        // operator finds out at the worst possible moment.
        let documented = [
            ("STRAUM_LISTEN_ADDR", "0.0.0.0:1883"),
            ("STRAUM_LISTENERS", "1"),
            ("STRAUM_WORKER_THREADS", "auto"),
            ("STRAUM_LOG_LEVEL", "info"),
            ("STRAUM_SHUTDOWN_GRACE_MS", "5000"),
            ("STRAUM_BACKLOG", "auto"),
            ("STRAUM_MAX_CONNECTIONS", "auto"),
            ("STRAUM_TCP_NODELAY", "true"),
            ("STRAUM_TCP_KEEPALIVE_SECS", "300"),
            ("STRAUM_SO_RCVBUF_BYTES", "0"),
            ("STRAUM_SO_SNDBUF_BYTES", "0"),
            ("STRAUM_TCP_USER_TIMEOUT_MS", "0"),
            ("STRAUM_TLS_CERT_PATH", ""),
            ("STRAUM_TLS_KEY_PATH", ""),
            ("STRAUM_AUTH_PASSWORD_FILE", ""),
            ("STRAUM_AUTH_ACL_FILE", ""),
            ("STRAUM_AUTH_ALLOW_ANONYMOUS", "true"),
            ("STRAUM_READ_BUFFER_BYTES", "16384"),
            ("STRAUM_TOTAL_READ_BYTES", "1073741824"),
            ("STRAUM_WRITE_STALL_TIMEOUT_MS", "30000"),
            ("STRAUM_MAX_PACKET_BYTES", "1048576"),
            ("STRAUM_MAX_TOPIC_BYTES", "1024"),
            ("STRAUM_MAX_TOPIC_LEVELS", "32"),
            ("STRAUM_MAX_CLIENT_ID_BYTES", "256"),
            ("STRAUM_MAX_FILTERS_PER_SUBSCRIBE", "64"),
            ("STRAUM_CONNECT_TIMEOUT_MS", "10000"),
            ("STRAUM_PAYLOAD_COPY_THRESHOLD_BYTES", "512"),
            ("STRAUM_MAX_SUBSCRIPTIONS_PER_SESSION", "1024"),
            ("STRAUM_MAX_INFLIGHT", "32"),
            ("STRAUM_SESSION_BACKLOG_MESSAGES", "1024"),
            ("STRAUM_MAX_OFFLINE_SESSIONS", "4096"),
            ("STRAUM_MAX_RETAINED_MESSAGES", "10000"),
            ("STRAUM_MAX_RETAINED_BYTES", "67108864"),
            ("STRAUM_SESSION_QUEUE_MESSAGES", "1024"),
            ("STRAUM_SESSION_QUEUE_BYTES", "8388608"),
            ("STRAUM_TOTAL_QUEUED_BYTES", "1073741824"),
            ("STRAUM_BROKER_QUEUE", "65536"),
            ("STRAUM_BROKER_BATCH", "256"),
            ("STRAUM_WRITER_BATCH", "64"),
            ("STRAUM_WAL_PATH", "straum.wal"),
            ("STRAUM_WAL_QUEUE", "65536"),
            ("STRAUM_WAL_MAX_RECORD_BYTES", "1048576"),
            ("STRAUM_WAL_MIN_BATCH_BYTES", "65536"),
            ("STRAUM_WAL_MAX_BATCH_BYTES", "4194304"),
            ("STRAUM_WAL_INITIAL_BATCH_BYTES", "1048576"),
            ("STRAUM_DURABILITY", "memory"),
        ];

        let settings = parse(&documented).unwrap();
        assert_eq!(settings.effective.len(), documented.len());
    }
}
