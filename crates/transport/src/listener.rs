//! Accepting connections without spinning.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use tokio::net::TcpListener;

use crate::buffers::BufferBudget;
use crate::connection::Accepted;
use crate::sys;
use crate::tls::Tls;
use crate::{TransportConfig, TransportError};

/// How long the accept loop waits out a descriptor shortage. Retrying
/// immediately would burn a core and log a river (CLAUDE.md §3).
const EXHAUSTED_BACKOFF: Duration = Duration::from_millis(50);

pub struct Listener {
    inner: TcpListener,
    config: TransportConfig,
    budget: Arc<BufferBudget>,
    tls: Option<Tls>,
}

impl Listener {
    /// Binds and starts listening with the configured backlog.
    /// The budget is shared by every listener: it bounds the process,
    /// not the socket.
    /// `tls` makes it a TLS listener; plaintext and TLS do not share a
    /// port (§6).
    pub fn bind(
        config: TransportConfig,
        budget: Arc<BufferBudget>,
        tls: Option<Tls>,
    ) -> Result<Self, TransportError> {
        let domain = Domain::for_address(config.listen);
        let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_address(true)?;
        if config.listeners > 1 {
            // Several sockets on one address; the kernel decides which
            // one a connection lands on.
            socket.set_reuse_port(true)?;
        }
        socket.set_nonblocking(true)?;
        socket.bind(&config.listen.into())?;
        // The kernel silently clamps this to net.core.somaxconn; the startup
        // checks are what tell the operator (docs/linux-tuning.md).
        socket.listen(i32::try_from(config.backlog).unwrap_or(i32::MAX))?;

        let inner = TcpListener::from_std(socket.into())?;

        Ok(Self {
            inner,
            config,
            budget,
            tls,
        })
    }

    /// The address actually bound; with port 0 this is the assigned port.
    pub fn local_addr(&self) -> Result<SocketAddr, TransportError> {
        Ok(self.inner.local_addr()?)
    }

    pub fn budget(&self) -> &Arc<BufferBudget> {
        &self.budget
    }

    /// Accepts one connection, applying socket options to it.
    ///
    /// Descriptor exhaustion backs off instead of spinning; every other error
    /// is returned, because an accept loop that cannot tell them apart will
    /// eventually loop on a permanent one.
    /// Accepts one connection and applies socket options to it.
    ///
    /// Returns before any conversation happens: the TLS handshake is the
    /// connection task's work, not the accept loop's.
    pub async fn accept(&self) -> Result<Accepted, TransportError> {
        loop {
            match self.inner.accept().await {
                Ok((stream, peer)) => {
                    self.configure(&stream)?;
                    return Ok(Accepted::new(
                        stream,
                        peer,
                        self.config,
                        Arc::clone(&self.budget),
                        self.tls.clone(),
                    ));
                }
                Err(error) if is_exhausted(&error) => {
                    tracing::warn!(
                        error = %error,
                        backoff_ms = EXHAUSTED_BACKOFF.as_millis(),
                        "out of file descriptors, pausing the accept loop"
                    );
                    tokio::time::sleep(EXHAUSTED_BACKOFF).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn configure(&self, stream: &tokio::net::TcpStream) -> Result<(), TransportError> {
        stream.set_nodelay(self.config.nodelay)?;
        let socket = socket2::SockRef::from(stream);

        // A fixed buffer size disables Linux autotuning for this socket,
        // so it is off unless somebody measured that it helps.
        if self.config.so_rcvbuf_bytes > 0 {
            socket.set_recv_buffer_size(self.config.so_rcvbuf_bytes)?;
        }
        if self.config.so_sndbuf_bytes > 0 {
            socket.set_send_buffer_size(self.config.so_sndbuf_bytes)?;
        }
        if let Some(timeout) = self.config.tcp_user_timeout {
            sys::set_user_timeout(&socket, timeout)?;
        }

        if let Some(idle) = self.config.keepalive {
            // Detects peers that vanished without closing, which MQTT
            // keep-alive 0 leaves us blind to.
            socket.set_tcp_keepalive(&TcpKeepalive::new().with_time(idle))?;
        }
        Ok(())
    }
}

/// EMFILE (this process) and ENFILE (the machine). Both numbers are the
/// same on Linux and macOS.
const EMFILE: i32 = 24;
const ENFILE: i32 = 23;

fn is_exhausted(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(EMFILE) | Some(ENFILE))
}
