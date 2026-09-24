//! Socket settings the broker applies. Kernel parameters are the operator's
//! (CLAUDE.md §13); these are the `STRAUM_*` values parsed in `server`.

use std::net::SocketAddr;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportConfig {
    pub listen: SocketAddr,
    /// Passed to `listen()`. The kernel clamps it to `net.core.somaxconn`.
    pub backlog: u32,
    pub nodelay: bool,
    /// Idle time before TCP keepalive probes; `None` disables them.
    pub keepalive: Option<Duration>,
    /// Sockets bound to the same address with `SO_REUSEPORT`, so the
    /// kernel spreads accepts across them. Linux only; elsewhere the
    /// caller resolves this to 1.
    pub listeners: u32,
    /// `0` leaves the kernel to size the receive buffer. Setting it
    /// turns off Linux receive autotuning for the socket.
    pub so_rcvbuf_bytes: usize,
    /// `0` leaves the kernel to size the send buffer.
    pub so_sndbuf_bytes: usize,
    /// `None` leaves the kernel default. Linux only.
    pub tcp_user_timeout: Option<Duration>,
    /// Steady-state read buffer per connection.
    pub read_buffer_bytes: usize,
    /// Global budget for read buffers across all connections.
    pub total_read_bytes: usize,
    pub max_connections: usize,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self {
            listen: SocketAddr::from(([0, 0, 0, 0], 1883)),
            backlog: 1024,
            nodelay: true,
            keepalive: Some(Duration::from_secs(300)),
            listeners: 1,
            so_rcvbuf_bytes: 0,
            so_sndbuf_bytes: 0,
            tcp_user_timeout: None,
            read_buffer_bytes: 16_384,
            total_read_bytes: 1_073_741_824,
            max_connections: 1024,
        }
    }
}
