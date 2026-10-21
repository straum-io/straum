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
use std::path::PathBuf;
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
        "{name} is {address}, reachable beyond this host, with no TLS: credentials, if any, \
         would cross it in clear; set STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK=1 to accept that \
         on an isolated, trusted network"
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
    #[error(
        "STRAUM_AUTH_ALLOW_ANONYMOUS=0 with no user in STRAUM_AUTH_PASSWORD_FILE: \
         no client could connect"
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
        // Loopback unless asked: the path has no TLS, so credentials would
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
        if !exposed {
            for (name, address) in [
                ("STRAUM_CLUSTER_MQTT_LISTEN", mqtt),
                ("STRAUM_CLUSTER_LISTEN", listen),
            ]
            .into_iter()
            .chain(voters.iter().map(|&(_, a)| ("STRAUM_CLUSTER_PEERS", a)))
            .chain(metrics.map(|a| ("STRAUM_CLUSTER_METRICS_LISTEN", a)))
            {
                if !address.ip().is_loopback() {
                    return Err(ConfigError::Exposed { name, address });
                }
            }
        }
        let authority = credentials(&mut parser)?;
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
        }
        .node_config()?;
        if let Some(mqtt) = node.mqtt.as_mut() {
            mqtt.authority = authority.map(std::sync::Arc::new);
        }
        Ok(Self {
            node,
            log_level,
            worker_threads,
            effective,
        })
    }
}

/// D-158 A: who may connect. No file: anybody, anything, as before
/// credentials. A password file: its users, and anonymous clients only if
/// `STRAUM_AUTH_ALLOW_ANONYMOUS=1`. An ACL file restricts the users it
/// names, and anonymous clients by its `anonymous` section. Both files are
/// read once, at startup; a file that cannot be read or parsed refuses the
/// start.
fn credentials(parser: &mut Parser) -> Result<Option<auth::Authority>, ConfigError> {
    let passwords: Option<String> = parser.optional_value("STRAUM_AUTH_PASSWORD_FILE", "a path")?;
    let acl: Option<String> = parser.optional_value("STRAUM_AUTH_ACL_FILE", "a path")?;
    let anonymous = parser.switch("STRAUM_AUTH_ALLOW_ANONYMOUS", passwords.is_none())?;
    if passwords.is_none() && acl.is_none() {
        return if anonymous {
            Ok(None)
        } else {
            Err(ConfigError::NobodyAllowed)
        };
    }
    let read = |name: &'static str, path: &Option<String>| match path {
        None => Ok(String::new()),
        Some(path) => std::fs::read_to_string(path).map_err(|error| ConfigError::Unreadable {
            name,
            path: path.clone(),
            reason: error.to_string(),
        }),
    };
    let authority = auth::Authority::parse(
        &read("STRAUM_AUTH_PASSWORD_FILE", &passwords)?,
        &read("STRAUM_AUTH_ACL_FILE", &acl)?,
        anonymous,
    )?;
    if authority.users() == 0 && !authority.allows_anonymous() {
        return Err(ConfigError::NobodyAllowed);
    }
    Ok(Some(authority))
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
            with(&[("STRAUM_TLS_CERT_PATH", "/etc/cert.pem")], "").err(),
            Some(ConfigError::Unknown("STRAUM_TLS_CERT_PATH".to_owned()))
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
}
