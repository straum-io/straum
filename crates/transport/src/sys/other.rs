//! No `/proc` here. Every probe reports "unavailable" and `auto` falls back
//! to the documented default (docs/configuration.md).

use super::KernelLimits;

pub(super) fn probe() -> KernelLimits {
    KernelLimits::default()
}

/// There is no `TCP_USER_TIMEOUT` here. The startup warning has already told
/// the operator; silently doing nothing beats pretending.
pub(super) fn set_user_timeout(
    _socket: &socket2::SockRef<'_>,
    _timeout: std::time::Duration,
) -> std::io::Result<()> {
    Ok(())
}
