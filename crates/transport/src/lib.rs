//! TCP: listener, connection lifecycle, socket options, ingress memory.

mod config;
mod connection;
mod listener;
mod read;
pub mod sys;
mod tls;

pub use config::TransportConfig;
pub use connection::{Accepted, Connection, Reader, Writer};
pub use listener::Listener;
pub use read::{ReadBudget, ReadBuffer, ReadError};
pub use sys::KernelLimits;
pub use tls::{Tls, TlsError};

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("{0}")]
    Tls(#[from] TlsError),
    #[error("{0}")]
    Read(#[from] ReadError),
    #[error("{0}")]
    Io(#[from] std::io::Error),
}
