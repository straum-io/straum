//! Reads the files. Everything it learns is parsed by `sys::mod`.

use std::fs;

use super::{KernelLimits, parse_open_files, parse_sysctl};

pub(super) fn probe() -> KernelLimits {
    KernelLimits {
        somaxconn: sysctl("/proc/sys/net/core/somaxconn").and_then(|v| u32::try_from(v).ok()),
        max_syn_backlog: sysctl("/proc/sys/net/ipv4/tcp_max_syn_backlog")
            .and_then(|v| u32::try_from(v).ok()),
        rmem_max: sysctl("/proc/sys/net/core/rmem_max"),
        wmem_max: sysctl("/proc/sys/net/core/wmem_max"),
        open_files: fs::read_to_string("/proc/self/limits")
            .ok()
            .as_deref()
            .and_then(parse_open_files),
    }
}

fn sysctl(path: &str) -> Option<u64> {
    fs::read_to_string(path)
        .ok()
        .as_deref()
        .and_then(parse_sysctl)
}

pub(super) fn set_user_timeout(
    socket: &socket2::SockRef<'_>,
    timeout: std::time::Duration,
) -> std::io::Result<()> {
    socket.set_tcp_user_timeout(Some(timeout))
}
