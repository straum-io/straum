//! TCP: listener, connection lifecycle, socket options, ingress memory.

mod buffers;
mod config;
mod connection;
mod listener;
pub mod sys;
mod tls;

pub use buffers::{BufferBudget, BufferError, ConnectionBuffer};
pub use config::TransportConfig;
pub use connection::{Accepted, Connection, Reader, Writer, connect};
pub use listener::Listener;
pub use sys::KernelLimits;
pub use tls::{Tls, TlsError};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("{0}")]
    Tls(#[from] TlsError),
    #[error("{0}")]
    Read(#[from] BufferError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}
