//! `STRAUM_*` into the node's configuration.
//!
//! The server runs one path since step 8 (D-111): a node of the cluster
//! path, experimental and not production-ready (D-102 A), started only when
//! `STRAUM_EXPERIMENTAL_CLUSTER=1` says the operator knows it. Three rules
//! drive the shape of this file: an unknown variable is an error, a
//! malformed or out-of-range value is an error, and nothing is corrected
//! silently (D-104 A) — a typo that turns into a default is a production
//! incident waiting for a quiet night.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
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
    #[error(
        "the only path is the experimental cluster path, not production-ready: set \
         STRAUM_EXPERIMENTAL_CLUSTER=1 to run it (the old broker was removed at step 8)"
    )]
    NotAcknowledged,
    #[error("{0} must be set")]
    Missing(&'static str),
    #[error(
        "{name} is {address}, reachable beyond this host; set STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1 \
         to accept that on an isolated, trusted network (for STRAUM_CLUSTER_MQTT_LISTEN alone, \
         TLS with a password file and no anonymous clients is enough)"
    )]
    Exposed {
        name: &'static str,
        address: SocketAddr,
    },
    #[error("{name}: {path} cannot be read: {reason}")]
    Unreadable {
        name: &'static str,
        path: String,
        reason: String,
    },
    #[error("credentials: {0}")]
    Credentials(#[from] auth::AuthError),
    #[error("TLS: {0}")]
    Tls(String),
    #[error("STRAUM_TLS_CERT_PATH and STRAUM_TLS_KEY_PATH go together: set both or neither")]
    TlsPair,
    #[error(
        "no client could connect: no user in STRAUM_AUTH_PASSWORD_FILE, and anonymous clients \
         refused (STRAUM_AUTH_ALLOW_ANONYMOUS=0)"
    )]
    NobodyAllowed,
    #[error("cluster: {0}")]
    Cluster(#[from] node::ConfigError),
}

/// Where a value came from, for the one startup log line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Env,
    Default,
}

/// The settings of the one path: `STRAUM_CLUSTER_*`, D-83 A's
/// `STRAUM_READ_BYTES`, the log level and the worker threads (D-104 A).
#[derive(Debug, Clone)]
pub struct Settings {
    pub node: node::NodeConfig,
    pub log_level: String,
    pub worker_threads: usize,
    pub effective: Vec<(&'static str, String, Source)>,
    /// Logged at startup: a configuration that starts but may not be what
    /// was meant (D-160 A).
    pub warnings: Vec<&'static str>,
    /// What a SIGHUP reloads (D-166 A); `None` when nothing is configured.
    pub access: Option<Access>,
}

const SWITCH: &str = "STRAUM_EXPERIMENTAL_CLUSTER";

struct Parser {
    vars: BTreeMap<String, String>,
    used: Vec<&'static str>,
    effective: Vec<(&'static str, String, Source)>,
}

impl Settings {
    /// Parses from an iterator rather than the process environment, so tests
    /// do not have to fight over a global.
    pub fn parse(vars: impl IntoIterator<Item = (String, String)>) -> Result<Self, ConfigError> {
        let mut parser = Parser {
            vars: vars
                .into_iter()
                .filter(|(name, _)| name.starts_with("STRAUM_"))
                .collect(),
            used: Vec::new(),
            effective: Vec::new(),
        };
        match parser.raw(SWITCH) {
            Some("1") => parser.record(SWITCH, 1, Source::Env),
            None | Some("0") => return Err(ConfigError::NotAcknowledged),
            Some(other) => {
                return Err(ConfigError::Malformed {
                    name: SWITCH,
                    value: other.to_owned(),
                    expected: "`1`",
                });
            }
        }
        let log_level = parser.parse_value("STRAUM_LOG_LEVEL", "info".to_owned(), "a log level")?;
        let worker_threads = parser.auto_threads("STRAUM_WORKER_THREADS")?;
        let id: u8 = parser.required("STRAUM_CLUSTER_NODE_ID", "a node id, 0..=255")?;
        let voters = parser.voters("STRAUM_CLUSTER_PEERS")?;
        let own = voters.iter().find(|&&(v, _)| v == id).map(|&(_, a)| a);
        let listen = match own {
            Some(own) => parser.parse_value("STRAUM_CLUSTER_LISTEN", own, "a socket address")?,
            None => parser.required("STRAUM_CLUSTER_LISTEN", "a socket address")?,
        };
        // Loopback unless asked, or unless TLS and a password file without
        // anonymous clients secure it (D-164 A): otherwise credentials would
        // cross the network in clear (D-158 A), and the experimental switch
        // is no security mechanism (D-107).
        let mqtt = parser.parse_value(
            "STRAUM_CLUSTER_MQTT_LISTEN",
            SocketAddr::from(([127, 0, 0, 1], 1883)),
            "a socket address",
        )?;
        // Off unless set (D-112): plain HTTP with no credentials, under the
        // same loopback rule as every other listener.
        let metrics = parser.optional_value("STRAUM_CLUSTER_METRICS_LISTEN", "a socket address")?;
        let exposed = parser.boolean_flag("STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK")?;
        let (passwords, acl, anonymous) = credentials(&mut parser)?;
        let (certificate, key) = tls_paths(&mut parser)?;
        let authority = load_authority(passwords.as_deref(), acl.as_deref(), anonymous)?;
        let tls = match (&certificate, &key) {
            (Some(certificate), Some(key)) => Some(
                transport::Tls::from_pem_files(Path::new(certificate), Path::new(key))
                    .map_err(|error| ConfigError::Tls(error.to_string()))?,
            ),
            _ => None,
        };
        // D-164 A: the MQTT listener alone may leave loopback once every
        // client is encrypted and named, never anonymous. Links, peers and
        // metrics keep the rule until node-to-node TLS (D-138 (6)).
        let mqtt_secured = tls.is_some() && authority.as_ref().is_some_and(secures);
        if !exposed {
            for (name, address) in [
                ("STRAUM_CLUSTER_MQTT_LISTEN", mqtt),
                ("STRAUM_CLUSTER_LISTEN", listen),
            ]
            .into_iter()
            .filter(|&(name, _)| !(mqtt_secured && name == "STRAUM_CLUSTER_MQTT_LISTEN"))
            .chain(voters.iter().map(|&(_, a)| ("STRAUM_CLUSTER_PEERS", a)))
            .chain(metrics.map(|a| ("STRAUM_CLUSTER_METRICS_LISTEN", a)))
            {
                if !address.ip().is_loopback() {
                    return Err(ConfigError::Exposed { name, address });
                }
            }
        }
        let data: String = parser.required("STRAUM_CLUSTER_DATA_DIR", "a directory")?;
        let heartbeat = parser.number(
            "STRAUM_CLUSTER_HEARTBEAT_MS",
            u64::try_from(node::experimental::DEFAULT_HEARTBEAT.as_millis()).unwrap_or(50),
            1,
            10_000,
        )?;
        let takeover_ticks = parser.number(
            "STRAUM_CLUSTER_TAKEOVER_TICKS",
            node::experimental::DEFAULT_TAKEOVER_TICKS,
            1,
            1000,
        )?;
        let read_bytes = parser.number(
            "STRAUM_READ_BYTES",
            node::experimental::DEFAULT_READ_BYTES,
            1,
            u64::from(u32::MAX),
        )?;
        let retain_bytes = parser.number(
            "STRAUM_CLUSTER_RETAIN_BYTES",
            node::DEFAULT_RETAIN_BYTES,
            0,
            u64::MAX,
        )?;
        // ADR-0009 (D-170 A): off until a host that passes D-129 confirms it.
        let wal_preallocate_bytes =
            parser.number("STRAUM_WAL_PREALLOCATE_BYTES", 0u64, 0, 1 << 30)?;
        let effective = parser.finish()?;
        let mut node = node::Experimental {
            id,
            voters,
            listen,
            mqtt,
            data: PathBuf::from(data),
            heartbeat: Duration::from_millis(heartbeat),
            takeover_ticks,
            read_bytes,
            retain_bytes,
            metrics,
            wal_preallocate_bytes,
        }
        .node_config()?;
        let warnings = warnings_for(authority.as_ref());
        // D-166 A: what a SIGHUP reloads, if anything is configured.
        let access =
            (passwords.is_some() || acl.is_some() || certificate.is_some()).then_some(Access {
                passwords,
                acl,
                anonymous,
                certificate,
                key,
            });
        if let Some(mqtt) = node.mqtt.as_mut() {
            mqtt.authority = authority.map(std::sync::Arc::new);
            mqtt.tls = tls;
        }
        Ok(Self {
            node,
            log_level,
            worker_threads,
            effective,
            warnings,
            access,
        })
    }
}

/// D-158 A: who may connect, as settings. No file: anybody, anything, as
/// before credentials. A password file: its users, and anonymous clients
/// only if `STRAUM_AUTH_ALLOW_ANONYMOUS=1`. An ACL file grants only what it
/// lists (D-159 A): its users their rules, anonymous clients its
/// `anonymous` section, anyone it does not name nothing.
fn credentials(parser: &mut Parser) -> Result<(Option<String>, Option<String>, bool), ConfigError> {
    let passwords: Option<String> = parser.optional_value("STRAUM_AUTH_PASSWORD_FILE", "a path")?;
    let acl: Option<String> = parser.optional_value("STRAUM_AUTH_ACL_FILE", "a path")?;
    let anonymous = parser.switch("STRAUM_AUTH_ALLOW_ANONYMOUS", passwords.is_none())?;
    if passwords.is_none() && acl.is_none() && !anonymous {
        return Err(ConfigError::NobodyAllowed);
    }
    Ok((passwords, acl, anonymous))
}

/// The credential files, read and checked: at startup and on every reload
/// (D-166 A), by the same rules. A file that cannot be read or parsed is an
/// error, as is a configuration no client could connect with.
fn load_authority(
    passwords: Option<&str>,
    acl: Option<&str>,
    anonymous: bool,
) -> Result<Option<auth::Authority>, ConfigError> {
    if passwords.is_none() && acl.is_none() {
        return Ok(None);
    }
    let read = |name: &'static str, path: &str| {
        std::fs::read_to_string(path).map_err(|error| ConfigError::Unreadable {
            name,
            path: path.to_owned(),
            reason: error.to_string(),
        })
    };
    let rules = acl
        .map(|path| read("STRAUM_AUTH_ACL_FILE", path))
        .transpose()?;
    let passwords = passwords
        .map(|path| read("STRAUM_AUTH_PASSWORD_FILE", path))
        .transpose()?
        .unwrap_or_default();
    let authority = auth::Authority::parse(&passwords, rules.as_deref(), anonymous)?;
    if authority.admits_nobody() {
        return Err(ConfigError::NobodyAllowed);
    }
    Ok(Some(authority))
}

/// Every client named, none anonymous: with TLS, what lets the MQTT
/// listener leave loopback (D-164 A).
fn secures(authority: &auth::Authority) -> bool {
    authority.users() > 0 && !authority.allows_anonymous()
}

/// D-160 A: a correct ACL file that grants nobody anything starts, with a
/// warning; it is never read as a permission.
fn warnings_for(authority: Option<&auth::Authority>) -> Vec<&'static str> {
    if authority.is_some_and(auth::Authority::grants_no_action) {
        vec![
            "STRAUM_AUTH_ACL_FILE grants no user and no anonymous client anything: clients \
             can connect, but every PUBLISH and SUBSCRIBE will be refused",
        ]
    } else {
        Vec::new()
    }
}

/// What a SIGHUP reloads (D-166 A): the files named at startup. The settings
/// are not read again, so a reload never turns TLS or credentials on or off,
/// nor lets anonymous clients in. An MQTT listener exposed on the strength
/// of TLS and credentials (D-164 A) stays secured: TLS stays on, anonymous
/// clients stay refused, and a password file left without users is refused
/// as a configuration nobody could connect with.
#[derive(Debug, Clone)]
pub struct Access {
    passwords: Option<String>,
    acl: Option<String>,
    anonymous: bool,
    certificate: Option<String>,
    key: Option<String>,
}

/// A reload's files, every one read and checked, none in use yet.
#[derive(Debug)]
pub struct Reloaded {
    pub authority: Option<auth::Authority>,
    pub tls: Option<transport::Loaded>,
    pub warnings: Vec<&'static str>,
}

impl Access {
    /// Reads and checks every file again by the start's rules; nothing is
    /// used unless all of them pass (D-166 A).
    pub fn reload(&self) -> Result<Reloaded, ConfigError> {
        let authority = load_authority(
            self.passwords.as_deref(),
            self.acl.as_deref(),
            self.anonymous,
        )?;
        let tls = match (&self.certificate, &self.key) {
            (Some(certificate), Some(key)) => Some(
                transport::Tls::load(Path::new(certificate), Path::new(key))
                    .map_err(|error| ConfigError::Tls(error.to_string()))?,
            ),
            _ => None,
        };
        Ok(Reloaded {
            warnings: warnings_for(authority.as_ref()),
            authority,
            tls,
        })
    }
}

/// D-164 A: TLS on the MQTT listener, from a PEM certificate chain and its
/// key, read once at startup and again on SIGHUP (D-166 A). Both or
/// neither; a file that does not load refuses the start: a node must never
/// come up in plaintext instead.
fn tls_paths(parser: &mut Parser) -> Result<(Option<String>, Option<String>), ConfigError> {
    let certificate: Option<String> = parser.optional_value("STRAUM_TLS_CERT_PATH", "a path")?;
    let key: Option<String> = parser.optional_value("STRAUM_TLS_KEY_PATH", "a path")?;
    if certificate.is_some() != key.is_some() {
        return Err(ConfigError::TlsPair);
    }
    Ok((certificate, key))
}

impl Parser {
    fn record<T: std::fmt::Display>(&mut self, name: &'static str, value: T, source: Source) {
        self.used.push(name);
        self.effective.push((name, value.to_string(), source));
    }

    fn raw(&self, name: &'static str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    /// A setting with no default: `None` when it is not set.
    fn optional_value<T>(
        &mut self,
        name: &'static str,
        expected: &'static str,
    ) -> Result<Option<T>, ConfigError>
    where
        T: std::str::FromStr + std::fmt::Display,
    {
        match self.raw(name) {
            None => {
                self.record(name, "off", Source::Default);
                Ok(None)
            }
            Some(raw) => {
                let parsed = raw.parse::<T>().map_err(|_| ConfigError::Malformed {
                    name,
                    value: raw.to_owned(),
                    expected,
                })?;
                self.record(name, &parsed, Source::Env);
                Ok(Some(parsed))
            }
        }
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
        let out_of_range = ConfigError::OutOfRange {
            name,
            value,
            min,
            max,
        };
        if value < min || value > max {
            return Err(out_of_range);
        }
        let value = T::try_from(value).map_err(|_| out_of_range)?;
        self.record(name, value, Source::Env);
        Ok(value)
    }

    /// `auto` or unset: the available parallelism.
    fn auto_threads(&mut self, name: &'static str) -> Result<usize, ConfigError> {
        match self.raw(name) {
            Some("auto") | None => {
                let value = std::thread::available_parallelism()
                    .map(std::num::NonZeroUsize::get)
                    .unwrap_or(1);
                self.record(name, value, Source::Default);
                Ok(value)
            }
            Some(_) => self.number(name, 1usize, 1, 4096),
        }
    }

    /// `1` or `0`, default `0`: a flag an operator sets on purpose.
    fn boolean_flag(&mut self, name: &'static str) -> Result<bool, ConfigError> {
        match self.raw(name) {
            None | Some("0") => {
                self.record(name, 0, Source::Default);
                Ok(false)
            }
            Some("1") => {
                self.record(name, 1, Source::Env);
                Ok(true)
            }
            Some(raw) => Err(ConfigError::Malformed {
                name,
                value: raw.to_owned(),
                expected: "`0` or `1`",
            }),
        }
    }

    /// `1` or `0`, with a default that depends on other settings.
    fn switch(&mut self, name: &'static str, default: bool) -> Result<bool, ConfigError> {
        let value = match self.raw(name) {
            None => {
                self.record(name, u8::from(default), Source::Default);
                return Ok(default);
            }
            Some("0") => false,
            Some("1") => true,
            Some(raw) => {
                return Err(ConfigError::Malformed {
                    name,
                    value: raw.to_owned(),
                    expected: "`0` or `1`",
                });
            }
        };
        self.record(name, u8::from(value), Source::Env);
        Ok(value)
    }

    /// A value with no default: unset is an error (the node's identity and
    /// data, D-104 A).
    fn required<T>(&mut self, name: &'static str, expected: &'static str) -> Result<T, ConfigError>
    where
        T: std::str::FromStr + std::fmt::Display + Clone,
    {
        let raw = self
            .raw(name)
            .map(str::trim)
            .filter(|raw| !raw.is_empty())
            .ok_or(ConfigError::Missing(name))?;
        let parsed = raw.parse::<T>().map_err(|_| ConfigError::Malformed {
            name,
            value: raw.to_owned(),
            expected,
        })?;
        self.record(name, parsed.clone(), Source::Env);
        Ok(parsed)
    }

    /// `id=address,…`, every voter with this node among them.
    fn voters(&mut self, name: &'static str) -> Result<Vec<(u8, SocketAddr)>, ConfigError> {
        let raw: String = self.required(name, "`id=address,…`")?;
        raw.split(',')
            .map(|entry| {
                let (id, address) = entry.trim().split_once('=')?;
                Some((id.trim().parse().ok()?, address.trim().parse().ok()?))
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(ConfigError::Malformed {
                name,
                value: raw.clone(),
                expected: "`id=address,…` with ids 0..=255 and socket addresses",
            })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(pairs: &[(&str, &str)]) -> Result<Settings, ConfigError> {
        Settings::parse(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
        )
    }

    const VALID: [(&str, &str); 4] = [
        ("STRAUM_EXPERIMENTAL_CLUSTER", "1"),
        ("STRAUM_CLUSTER_NODE_ID", "1"),
        (
            "STRAUM_CLUSTER_PEERS",
            "0=127.0.0.1:7000, 1=127.0.0.1:7001, 2=127.0.0.1:7002",
        ),
        ("STRAUM_CLUSTER_DATA_DIR", "/tmp/straum-node-1"),
    ];

    fn with(
        extra: &[(&'static str, &'static str)],
        without: &str,
    ) -> Result<Settings, ConfigError> {
        let mut pairs: Vec<(&str, &str)> = VALID
            .iter()
            .copied()
            .filter(|(k, _)| *k != without)
            .collect();
        pairs.extend_from_slice(extra);
        parse(&pairs)
    }

    /// D-141 A: the settings `docs/configuration.md` declares stable are
    /// exactly the ones the server reads — every one recorded in
    /// `effective` from a valid start, defaults included.
    #[test]
    fn the_stable_settings_are_the_ones_read() {
        let doc = include_str!("../../../docs/configuration.md");
        let block = doc
            .split("<!-- stable-settings:begin -->")
            .nth(1)
            .and_then(|rest| rest.split("<!-- stable-settings:end -->").next())
            .expect("the stable-settings block");
        let mut declared: Vec<&str> = block
            .lines()
            .map(str::trim)
            .filter(|l| l.starts_with("STRAUM_"))
            .collect();
        let mut read: Vec<&str> = parse(&VALID)
            .unwrap()
            .effective
            .iter()
            .map(|(name, _, _)| *name)
            .collect();
        declared.sort_unstable();
        read.sort_unstable();
        assert_eq!(declared, read);
    }

    #[test]
    fn nothing_starts_without_the_acknowledgement() {
        assert_eq!(parse(&[]).err(), Some(ConfigError::NotAcknowledged));
        assert_eq!(
            parse(&[("STRAUM_EXPERIMENTAL_CLUSTER", "0")]).err(),
            Some(ConfigError::NotAcknowledged)
        );
        assert_eq!(
            parse(&[("STRAUM_EXPERIMENTAL_CLUSTER", "yes")]).err(),
            Some(ConfigError::Malformed {
                name: "STRAUM_EXPERIMENTAL_CLUSTER",
                value: "yes".to_owned(),
                expected: "`1`",
            })
        );
    }

    #[test]
    fn valid_settings_give_the_documented_defaults() {
        let settings = parse(&VALID).unwrap();
        let node = &settings.node;
        assert_eq!((node.core.id, node.core.nodes.clone()), (1, vec![0, 1, 2]));
        assert_eq!(node.listen, "127.0.0.1:7001".parse().unwrap());
        assert_eq!(node.heartbeat, Duration::from_millis(50));
        assert_eq!(node.core.takeover_ticks, 3);
        assert_eq!(node.core.lag_bound, None, "flow control off (D-102 A)");
        assert_eq!(node.retain_bytes, node::DEFAULT_RETAIN_BYTES);
        assert_eq!(node.read_bytes, 64 * 1024);
        assert_eq!(
            node.mqtt.as_ref().map(|m| m.listen),
            Some("127.0.0.1:1883".parse().unwrap())
        );
        assert_eq!(settings.log_level, "info");
    }

    #[test]
    fn invalid_settings_are_refused_not_corrected() {
        assert_eq!(
            with(&[], "STRAUM_CLUSTER_DATA_DIR").err(),
            Some(ConfigError::Missing("STRAUM_CLUSTER_DATA_DIR"))
        );
        assert_eq!(
            with(&[], "STRAUM_CLUSTER_NODE_ID").err(),
            Some(ConfigError::Missing("STRAUM_CLUSTER_NODE_ID"))
        );
        // A typo, or a setting of the removed broker: refused, not ignored.
        assert_eq!(
            with(&[("STRAUM_TLS_CA_PATH", "/etc/ca.pem")], "").err(),
            Some(ConfigError::Unknown("STRAUM_TLS_CA_PATH".to_owned()))
        );
        assert_eq!(
            with(&[("STRAUM_CLUSTER_HEARTBEAT", "50")], "").err(),
            Some(ConfigError::Unknown("STRAUM_CLUSTER_HEARTBEAT".to_owned()))
        );
        assert_eq!(
            with(
                &[("STRAUM_CLUSTER_PEERS", "0=127.0.0.1:7000,1=127.0.0.1:7001")],
                "STRAUM_CLUSTER_PEERS"
            )
            .err(),
            Some(ConfigError::Cluster(node::ConfigError::Voters { count: 2 }))
        );
        assert!(matches!(
            with(
                &[("STRAUM_CLUSTER_PEERS", "0=nowhere")],
                "STRAUM_CLUSTER_PEERS"
            ),
            Err(ConfigError::Malformed { .. })
        ));
        assert!(matches!(
            with(&[("STRAUM_CLUSTER_HEARTBEAT_MS", "0")], ""),
            Err(ConfigError::OutOfRange { .. })
        ));
        assert!(matches!(
            with(&[("STRAUM_CLUSTER_TAKEOVER_TICKS", "0")], ""),
            Err(ConfigError::OutOfRange { .. })
        ));
        assert!(matches!(
            with(&[("STRAUM_READ_BYTES", "67108864")], ""),
            Err(ConfigError::Cluster(node::ConfigError::ReadBytes { .. }))
        ));
    }

    /// D-107: nothing reaches the node from beyond this host unless an
    /// operator says so on purpose — the switch alone is no security
    /// mechanism.
    #[test]
    fn loopback_by_default_and_beyond_it_only_when_acknowledged() {
        let wide = [("STRAUM_CLUSTER_MQTT_LISTEN", "0.0.0.0:1883")];
        assert!(matches!(
            with(&wide, ""),
            Err(ConfigError::Exposed {
                name: "STRAUM_CLUSTER_MQTT_LISTEN",
                ..
            })
        ));
        let hosts = [(
            "STRAUM_CLUSTER_PEERS",
            "0=10.0.0.1:7000,1=10.0.0.2:7000,2=10.0.0.3:7000",
        )];
        assert!(matches!(
            with(&hosts, "STRAUM_CLUSTER_PEERS"),
            Err(ConfigError::Exposed {
                name: "STRAUM_CLUSTER_LISTEN",
                ..
            })
        ));
        let acknowledged = [
            hosts[0],
            wide[0],
            ("STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK", "1"),
        ];
        assert!(with(&acknowledged, "STRAUM_CLUSTER_PEERS").is_ok());
        // The metrics listener (D-112) too: off by default, loopback when set,
        // beyond it only when acknowledged.
        assert_eq!(with(&[], "").unwrap().node.metrics, None);
        let metrics = [("STRAUM_CLUSTER_METRICS_LISTEN", "127.0.0.1:9100")];
        assert!(with(&metrics, "").unwrap().node.metrics.is_some());
        let open = [("STRAUM_CLUSTER_METRICS_LISTEN", "0.0.0.0:9100")];
        assert!(matches!(
            with(&open, ""),
            Err(ConfigError::Exposed {
                name: "STRAUM_CLUSTER_METRICS_LISTEN",
                ..
            })
        ));
        let open_ok = [open[0], ("STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK", "1")];
        assert!(with(&open_ok, "").is_ok());
        assert!(matches!(
            with(
                &[hosts[0], ("STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK", "yes")],
                "STRAUM_CLUSTER_PEERS"
            ),
            Err(ConfigError::Malformed { .. })
        ));
    }

    /// A file under the system temp directory with `contents`, removed on
    /// drop.
    struct File(PathBuf);

    impl File {
        fn new(name: &str, contents: &str) -> Self {
            let path =
                std::env::temp_dir().join(format!("straum-config-{}-{name}", std::process::id()));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }
        fn path(&self) -> &str {
            self.0.to_str().unwrap()
        }
    }

    impl Drop for File {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn authority(settings: &Settings) -> Option<&auth::Authority> {
        settings.node.mqtt.as_ref()?.authority.as_deref()
    }

    /// D-158 A: no file, everybody as before; a password file, its users
    /// and no anonymous client unless allowed.
    #[test]
    fn credentials_are_off_without_files_and_anonymous_is_off_with_a_password_file() {
        assert!(authority(&parse(&VALID).unwrap()).is_none());
        let hash = auth::Authority::hash_password(b"pw").unwrap();
        let passwords = File::new("passwords-a", &format!("alice:{hash}\n"));
        let set = [("STRAUM_AUTH_PASSWORD_FILE", passwords.path())];
        let mut pairs = VALID.to_vec();
        pairs.extend_from_slice(&set);
        let settings = parse(&pairs).unwrap();
        let authority = authority(&settings).unwrap();
        assert_eq!(authority.users(), 1);
        assert!(!authority.allows_anonymous());
        pairs.push(("STRAUM_AUTH_ALLOW_ANONYMOUS", "1"));
        let settings = parse(&pairs).unwrap();
        assert!(authority_allows_anonymous(&settings));
    }

    fn authority_allows_anonymous(settings: &Settings) -> bool {
        authority(settings).is_some_and(auth::Authority::allows_anonymous)
    }

    #[test]
    fn a_configuration_nobody_can_connect_with_is_refused() {
        assert_eq!(
            with(&[("STRAUM_AUTH_ALLOW_ANONYMOUS", "0")], "").err(),
            Some(ConfigError::NobodyAllowed)
        );
        let empty = File::new("passwords-empty", "# nobody yet\n");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_AUTH_PASSWORD_FILE", empty.path()));
        assert_eq!(parse(&pairs).err(), Some(ConfigError::NobodyAllowed));
        // D-161 A: an ACL file alone, with no `anonymous` section, lets
        // anonymous clients in to do nothing: it starts, with the D-160
        // warning, and grants nothing.
        let acl = File::new("acl-no-anonymous", "# nothing for anonymous clients\n");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_AUTH_ACL_FILE", acl.path()));
        let settings = parse(&pairs).unwrap();
        assert_eq!(settings.warnings.len(), 1);
        let auth::Authenticated::Allowed(anonymous) =
            authority(&settings).unwrap().authenticate(None, None)
        else {
            panic!("anonymous clients are let in");
        };
        assert!(anonymous.grants_nothing());
    }

    #[test]
    fn an_unreadable_or_malformed_credentials_file_refuses_the_start() {
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_AUTH_PASSWORD_FILE", "/nonexistent/straum-passwords"));
        assert!(matches!(
            parse(&pairs),
            Err(ConfigError::Unreadable {
                name: "STRAUM_AUTH_PASSWORD_FILE",
                ..
            })
        ));
        let malformed = File::new("passwords-malformed", "alice\n");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_AUTH_PASSWORD_FILE", malformed.path()));
        assert_eq!(
            parse(&pairs).err(),
            Some(ConfigError::Credentials(
                auth::AuthError::MalformedCredentials { line: 1 }
            ))
        );
        assert!(matches!(
            with(&[("STRAUM_AUTH_ALLOW_ANONYMOUS", "yes")], ""),
            Err(ConfigError::Malformed {
                name: "STRAUM_AUTH_ALLOW_ANONYMOUS",
                ..
            })
        ));
    }

    /// An ACL file alone restricts anonymous clients to its `anonymous`
    /// section.
    #[test]
    fn an_acl_file_alone_restricts_anonymous_clients() {
        let acl = File::new("acl-anonymous", "anonymous\nsubscribe public/#\n");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_AUTH_ACL_FILE", acl.path()));
        let settings = parse(&pairs).unwrap();
        let authority = authority(&settings).unwrap();
        let auth::Authenticated::Allowed(permissions) = authority.authenticate(None, None) else {
            panic!("anonymous clients are let in");
        };
        let filter = |f: &'static str| {
            domain::TopicFilter::new(bytes::Bytes::from_static(f.as_bytes())).unwrap()
        };
        assert!(permissions.may_subscribe(&filter("public/#")));
        assert!(!permissions.may_subscribe(&filter("private/#")));
    }

    /// D-160 A: a correct ACL file that grants nobody anything starts, with
    /// a warning, and grants nothing; a malformed one still refuses the
    /// start, never falling back to a looser policy.
    #[test]
    fn a_correct_acl_granting_nothing_starts_with_a_warning_and_a_broken_one_refuses() {
        let hash = auth::Authority::hash_password(b"pw").unwrap();
        let passwords = File::new("passwords-d160", &format!("alice:{hash}\n"));
        let empty = File::new("acl-d160-empty", "# policy not written yet\n");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_AUTH_PASSWORD_FILE", passwords.path()));
        pairs.push(("STRAUM_AUTH_ACL_FILE", empty.path()));
        let settings = parse(&pairs).unwrap();
        assert_eq!(settings.warnings.len(), 1);
        let authority = authority(&settings).unwrap();
        let auth::Authenticated::Allowed(alice) =
            authority.authenticate(Some(b"alice"), Some(b"pw"))
        else {
            panic!("alice still connects");
        };
        assert!(alice.grants_nothing(), "a warning is not a permission");

        let rules = File::new("acl-d160-rules", "user alice\nreadwrite a/#\n");
        pairs.pop();
        pairs.push(("STRAUM_AUTH_ACL_FILE", rules.path()));
        assert!(parse(&pairs).unwrap().warnings.is_empty());

        let broken = File::new("acl-d160-broken", "user alice\nforbid a/b\n");
        pairs.pop();
        pairs.push(("STRAUM_AUTH_ACL_FILE", broken.path()));
        assert!(matches!(parse(&pairs), Err(ConfigError::Credentials(_))));

        assert!(parse(&VALID).unwrap().warnings.is_empty());
    }

    /// A throwaway certificate and key for one test (no key is committed).
    fn certificate(name: &str) -> (File, File) {
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        (
            File::new(&format!("{name}-cert.pem"), &issued.cert.pem()),
            File::new(
                &format!("{name}-key.pem"),
                &issued.signing_key.serialize_pem(),
            ),
        )
    }

    /// D-164 A: both paths or neither, and a file that does not load
    /// refuses the start.
    #[test]
    fn tls_takes_both_paths_and_refuses_a_file_that_does_not_load() {
        assert_eq!(
            with(&[("STRAUM_TLS_CERT_PATH", "/etc/cert.pem")], "").err(),
            Some(ConfigError::TlsPair)
        );
        assert_eq!(
            with(&[("STRAUM_TLS_KEY_PATH", "/etc/key.pem")], "").err(),
            Some(ConfigError::TlsPair)
        );
        let not_pem = File::new("tls-not-pem", "not a certificate\n");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_TLS_CERT_PATH", not_pem.path()));
        pairs.push(("STRAUM_TLS_KEY_PATH", not_pem.path()));
        assert!(matches!(parse(&pairs), Err(ConfigError::Tls(_))));
        let (cert, key) = certificate("tls-loads");
        let mut pairs = VALID.to_vec();
        pairs.push(("STRAUM_TLS_CERT_PATH", cert.path()));
        pairs.push(("STRAUM_TLS_KEY_PATH", key.path()));
        let settings = parse(&pairs).unwrap();
        assert!(settings.node.mqtt.as_ref().unwrap().tls.is_some());
    }

    /// D-164 A: the MQTT listener alone may leave loopback once TLS and a
    /// password file without anonymous clients secure it; nothing else may.
    #[test]
    fn only_a_tls_and_password_secured_mqtt_listener_leaves_loopback() {
        let (cert, key) = certificate("tls-guard");
        let hash = auth::Authority::hash_password(b"pw").unwrap();
        let passwords = File::new("passwords-tls", &format!("alice:{hash}\n"));
        let wide = ("STRAUM_CLUSTER_MQTT_LISTEN", "0.0.0.0:8883");
        let tls = [
            ("STRAUM_TLS_CERT_PATH", cert.path()),
            ("STRAUM_TLS_KEY_PATH", key.path()),
        ];
        let secured = |extra: &[(&str, &str)]| {
            let mut pairs = VALID.to_vec();
            pairs.push(wide);
            pairs.extend_from_slice(extra);
            parse(&pairs)
        };
        let both = [
            tls[0],
            tls[1],
            ("STRAUM_AUTH_PASSWORD_FILE", passwords.path()),
        ];
        assert!(secured(&both).is_ok(), "TLS and named clients only");

        let exposed = |r: Result<Settings, ConfigError>| {
            matches!(
                r,
                Err(ConfigError::Exposed {
                    name: "STRAUM_CLUSTER_MQTT_LISTEN",
                    ..
                })
            )
        };
        assert!(exposed(secured(&tls)), "TLS without credentials");
        assert!(
            exposed(secured(&[("STRAUM_AUTH_PASSWORD_FILE", passwords.path())])),
            "credentials without TLS"
        );
        let anonymous = [
            both[0],
            both[1],
            both[2],
            ("STRAUM_AUTH_ALLOW_ANONYMOUS", "1"),
        ];
        assert!(exposed(secured(&anonymous)), "anonymous clients let in");

        // Links, peers and metrics keep the rule.
        let mut pairs = VALID.to_vec();
        pairs.extend_from_slice(&both);
        pairs.push(("STRAUM_CLUSTER_METRICS_LISTEN", "0.0.0.0:9100"));
        assert!(matches!(
            parse(&pairs),
            Err(ConfigError::Exposed {
                name: "STRAUM_CLUSTER_METRICS_LISTEN",
                ..
            })
        ));
    }

    /// D-166 A: a reload reads and checks every file by the start's rules;
    /// it fails on a file that fails, and on files that would no longer
    /// secure an MQTT listener beyond loopback.
    #[test]
    fn a_reload_applies_the_start_rules_and_refuses_what_would_loosen_them() {
        let (cert, key) = certificate("reload");
        let alice = auth::Authority::hash_password(b"pw").unwrap();
        let passwords = File::new("passwords-reload", &format!("alice:{alice}\n"));
        let mut pairs = VALID.to_vec();
        pairs.extend_from_slice(&[
            ("STRAUM_CLUSTER_MQTT_LISTEN", "0.0.0.0:8883"),
            ("STRAUM_TLS_CERT_PATH", cert.path()),
            ("STRAUM_TLS_KEY_PATH", key.path()),
            ("STRAUM_AUTH_PASSWORD_FILE", passwords.path()),
        ]);
        let settings = parse(&pairs).unwrap();
        let access = settings.access.unwrap();
        let reloaded = access.reload().unwrap();
        assert_eq!(reloaded.authority.unwrap().users(), 1);
        assert!(reloaded.tls.is_some());

        std::fs::write(&passwords.0, "alice\n").unwrap();
        assert!(matches!(access.reload(), Err(ConfigError::Credentials(_))));
        std::fs::write(&passwords.0, "# nobody now\n").unwrap();
        assert_eq!(access.reload().err(), Some(ConfigError::NobodyAllowed));
        std::fs::write(&key.0, "not a key\n").unwrap();
        std::fs::write(&passwords.0, format!("alice:{alice}\n")).unwrap();
        assert!(matches!(access.reload(), Err(ConfigError::Tls(_))));

        // Without TLS settings or credentials there is nothing to reload.
        assert!(parse(&VALID).unwrap().access.is_none());
    }
}
