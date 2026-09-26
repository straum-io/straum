//! One accepted connection, split into halves that move bytes.
//!
//! `TcpStream` does not leave this crate (CLAUDE.md §3): callers get a reader
//! that fills a budgeted buffer and a writer that takes byte slices. Whether
//! there is TLS underneath is this module's business and nobody else's.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_rustls::server::TlsStream;

use crate::read::{ReadBudget, ReadBuffer};
use crate::tls::Tls;
use crate::{TransportConfig, TransportError};

/// How much room one read is given. Reads are sized by what the socket has,
/// not by what a length field claims.
const READ_CHUNK: usize = 4096;

/// A socket that has been accepted but not yet spoken to.
///
/// The TLS handshake lives here rather than in the accept loop: it is
/// round trips with a stranger, and one slow stranger must not stop the
/// broker from accepting anybody else.
pub struct Accepted {
    stream: TcpStream,
    peer: SocketAddr,
    config: TransportConfig,
    budget: Arc<ReadBudget>,
    tls: Option<Tls>,
}

impl Accepted {
    pub(crate) fn new(
        stream: TcpStream,
        peer: SocketAddr,
        config: TransportConfig,
        budget: Arc<ReadBudget>,
        tls: Option<Tls>,
    ) -> Self {
        Self {
            stream,
            peer,
            config,
            budget,
            tls,
        }
    }

    pub fn peer(&self) -> SocketAddr {
        self.peer
    }

    /// Completes the handshake, if there is one, and hands back the halves.
    pub async fn establish(self) -> Result<Connection, TransportError> {
        let buffer = ReadBuffer::new(self.config.read_buffer_bytes, self.budget)?;

        let (reader, writer) = match self.tls {
            None => {
                let (read, write) = self.stream.into_split();
                (Half::Plain(read), WriteSide::Plain(write))
            }
            Some(tls) => {
                let stream = tls.acceptor().accept(self.stream).await?;
                let (read, write) = tokio::io::split(stream);
                (Half::Tls(read), WriteSide::Tls(write))
            }
        };

        Ok(Connection {
            reader: Reader {
                half: reader,
                buffer,
            },
            writer: Writer { half: writer },
            peer: self.peer,
        })
    }
}

pub struct Connection {
    pub reader: Reader,
    pub writer: Writer,
    pub peer: SocketAddr,
}

/// Plain or encrypted; two real implementations, so an enum rather than a
/// trait object (CLAUDE.md §14).
enum Half {
    Plain(OwnedReadHalf),
    Tls(ReadHalf<TlsStream<TcpStream>>),
}

enum WriteSide {
    Plain(OwnedWriteHalf),
    Tls(WriteHalf<TlsStream<TcpStream>>),
}

pub struct Reader {
    half: Half,
    buffer: ReadBuffer,
}

impl Reader {
    /// Reads whatever is available into the buffer. `Ok(0)` is end of stream.
    ///
    /// The buffer grows only through the global budget, so a connection that
    /// announces a huge packet cannot take memory the broker does not have.
    pub async fn fill(&mut self) -> Result<usize, TransportError> {
        self.buffer.reserve(READ_CHUNK)?;
        let read = match &mut self.half {
            Half::Plain(half) => half.read_buf(self.buffer.buf_mut()).await?,
            Half::Tls(half) => half.read_buf(self.buffer.buf_mut()).await?,
        };
        Ok(read)
    }

    /// Makes room for a packet larger than the steady-state buffer.
    pub fn reserve(&mut self, additional: usize) -> Result<(), TransportError> {
        self.buffer.reserve(additional)?;
        Ok(())
    }

    pub fn buf_mut(&mut self) -> &mut BytesMut {
        self.buffer.buf_mut()
    }

    /// Returns capacity a large packet forced, once it is out of the way.
    pub fn reclaim(&mut self) {
        self.buffer.reclaim();
    }

    pub fn held_bytes(&self) -> usize {
        self.buffer.charged()
    }
}

pub struct Writer {
    half: WriteSide,
}

impl Writer {
    pub async fn write_all(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
        match &mut self.half {
            WriteSide::Plain(half) => half.write_all(bytes).await?,
            WriteSide::Tls(half) => half.write_all(bytes).await?,
        }
        Ok(())
    }

    /// Closes the write side. The broker has no DISCONNECT to send (§6), so
    /// this is how a session ends.
    pub async fn shutdown(&mut self) -> Result<(), TransportError> {
        match &mut self.half {
            WriteSide::Plain(half) => half.shutdown().await?,
            WriteSide::Tls(half) => half.shutdown().await?,
        }
        Ok(())
    }
}

/// Dials a peer and hands back the halves.
///
/// Plaintext. The replication link between nodes is not the client-facing
/// listener: it has its own address and its own configuration, and giving it
/// TLS is a separate decision with its own key material.
///
/// The read budget is the same global one the listener uses, so bytes read
/// from a peer count against the same ceiling as bytes read from a client
/// (CLAUDE.md §7).
pub async fn connect(
    peer: SocketAddr,
    read_buffer_bytes: usize,
    budget: Arc<ReadBudget>,
) -> Result<Connection, TransportError> {
    let stream = TcpStream::connect(peer).await?;
    // Small writes go out when they are written; a durability report waiting
    // on Nagle would be latency added for nothing.
    stream.set_nodelay(true)?;

    let buffer = ReadBuffer::new(read_buffer_bytes, budget)?;
    let (read, write) = stream.into_split();
    Ok(Connection {
        reader: Reader {
            half: Half::Plain(read),
            buffer,
        },
        writer: Writer {
            half: WriteSide::Plain(write),
        },
        peer,
    })
}
