//! Kernel parameters the broker reads, never writes, and the socket options
//! only some platforms have.
//!
//! Only the file reads are OS-gated. Parsing and the decisions made from it
//! are portable functions, tested with fixture strings on every platform.

use std::time::Duration;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(not(target_os = "linux"))]
mod other;

#[cfg(target_os = "linux")]
use linux as host;
#[cfg(not(target_os = "linux"))]
use other as host;

/// What the kernel says. `None` means the value could not be read here —
/// another OS, a container restriction, an unreadable file. Never fatal.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KernelLimits {
    pub somaxconn: Option<u32>,
    pub max_syn_backlog: Option<u32>,
    pub rmem_max: Option<u64>,
    pub wmem_max: Option<u64>,
    /// Soft limit on open file descriptors.
    pub open_files: Option<u64>,
}

impl KernelLimits {
    pub fn probe() -> Self {
        host::probe()
    }

    /// `STRAUM_BACKLOG=auto`. Without a kernel to ask, the documented
    /// fallback stands.
    pub fn backlog(&self, fallback: u32) -> u32 {
        self.somaxconn.unwrap_or(fallback)
    }

    /// `STRAUM_MAX_CONNECTIONS=auto`: the descriptor limit minus what the
    /// listeners, logs and runtime need.
    pub fn max_connections(&self, fallback: usize, reserved: u64) -> usize {
        match self.open_files {
            Some(limit) => usize::try_from(limit.saturating_sub(reserved)).unwrap_or(fallback),
            None => fallback,
        }
    }
}

/// A `/proc/sys/...` file: one number, possibly with whitespace.
pub fn parse_sysctl(raw: &str) -> Option<u64> {
    raw.split_whitespace().next()?.parse().ok()
}

/// The soft limit from a `/proc/self/limits` table.
///
/// ```text
/// Limit                     Soft Limit           Hard Limit           Units
/// Max open files            1048576              1048576              files
/// ```
pub fn parse_open_files(raw: &str) -> Option<u64> {
    for line in raw.lines() {
        let Some(rest) = line.strip_prefix("Max open files") else {
            continue;
        };
        let soft = rest.split_whitespace().next()?;
        if soft == "unlimited" {
            return Some(u64::MAX);
        }
        return soft.parse().ok();
    }
    None
}

/// Whether `SO_REUSEPORT` balances accepts across sockets.
///
/// Linux hashes new connections across every listener bound to the address.
/// The BSD implementation hands them to the last binder instead, which turns
/// several listeners into one listener and several idle sockets — so on those
/// platforms the broker runs one (docs/configuration.md).
pub fn reuseport_balances() -> bool {
    cfg!(target_os = "linux")
}

/// `TCP_USER_TIMEOUT`: how long unacknowledged data may stay in flight before
/// the kernel drops the connection. Linux only; elsewhere this does nothing
/// and the caller has already warned about it.
pub fn set_user_timeout(socket: &socket2::SockRef<'_>, timeout: Duration) -> std::io::Result<()> {
    host::set_user_timeout(socket, timeout)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sysctl_files_are_one_number() {
        assert_eq!(parse_sysctl("4096\n"), Some(4096));
        assert_eq!(parse_sysctl("4096 131072 16777216\n"), Some(4096));
        assert_eq!(parse_sysctl(""), None);
        assert_eq!(parse_sysctl("not a number\n"), None);
    }

    #[test]
    fn the_soft_limit_is_the_one_that_binds() {
        let raw = "Limit                     Soft Limit           Hard Limit           Units\n\
                   Max processes             1024                 2048                 processes\n\
                   Max open files            1048576              2097152              files\n";
        assert_eq!(parse_open_files(raw), Some(1_048_576));

        let unlimited =
            "Max open files            unlimited            unlimited            files\n";
        assert_eq!(parse_open_files(unlimited), Some(u64::MAX));
        assert_eq!(parse_open_files("Max core file size        0\n"), None);
    }

    #[test]
    fn auto_falls_back_when_there_is_nothing_to_ask() {
        let unknown = KernelLimits::default();
        assert_eq!(unknown.backlog(1024), 1024);
        assert_eq!(unknown.max_connections(1024, 64), 1024);

        let known = KernelLimits {
            somaxconn: Some(8192),
            open_files: Some(1_048_576),
            ..KernelLimits::default()
        };
        assert_eq!(known.backlog(1024), 8192);
        assert_eq!(known.max_connections(1024, 64), 1_048_512);
    }
}
