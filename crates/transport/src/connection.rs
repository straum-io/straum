//! One accepted connection, split into halves that move bytes.
//!
//! `TcpStream` does not leave this crate: callers get a reader
//! that fills a budgeted buffer and a writer that takes byte slices. Whether
//! there is TLS underneath is this module's business and nobody else's.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt, ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_rustls::TlsStream;

use crate::buffers::{BufferBudget, ConnectionBuffer};
use crate::node_tls::{NodeTls, Peer};
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
    budget: Arc<BufferBudget>,
    tls: Option<Tls>,
}

impl Accepted {
    pub(crate) fn new(
        stream: TcpStream,
        peer: SocketAddr,
        config: TransportConfig,
        budget: Arc<BufferBudget>,
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
        // Both halves, against the same budget and before either is used: a
        // connection that cannot be given its memory is refused it rather
        // than given it and closed afterwards (docs/memory.md).
        let buffer =
            ConnectionBuffer::new(self.config.read_buffer_bytes, Arc::clone(&self.budget))?;
        let outgoing = ConnectionBuffer::new(self.config.write_buffer_bytes, self.budget)?;

        let (reader, writer) = match self.tls {
            None => {
                let (read, write) = self.stream.into_split();
                (Half::Plain(read), WriteSide::Plain(write))
            }
            Some(tls) => {
                let stream = tls.acceptor().accept(self.stream).await?;
                let (read, write) = tokio::io::split(TlsStream::Server(stream));
                (Half::Tls(read), WriteSide::Tls(write))
            }
        };

        Ok(Connection {
            reader: Reader {
                half: reader,
                buffer,
            },
            writer: Writer {
                half: writer,
                buffer: outgoing,
            },
            peer: self.peer,
        })
    }
}

impl Accepted {
    /// A link from a peer node over TLS (ADR-0010): the handshake first,
    /// then the buffers, so a connection still in its handshake holds none
    /// of the budget. Which node the peer is comes with its HELLO, for the
    /// caller to check with [`NodeTls::check`] before HELLO has any effect.
    pub async fn establish_node(self, tls: &NodeTls) -> Result<(Connection, Peer), TransportError> {
        let (stream, peer) = tls.accept(self.stream).await?;
        let connection = halves(
            stream,
            self.peer,
            self.config.read_buffer_bytes,
            self.config.write_buffer_bytes,
            self.budget,
        )?;
        Ok((connection, peer))
    }
}

fn halves(
    stream: TlsStream<TcpStream>,
    peer: SocketAddr,
    read_buffer_bytes: usize,
    write_buffer_bytes: usize,
    budget: Arc<BufferBudget>,
) -> Result<Connection, TransportError> {
    let buffer = ConnectionBuffer::new(read_buffer_bytes, Arc::clone(&budget))?;
    let outgoing = ConnectionBuffer::new(write_buffer_bytes, budget)?;
    let (read, write) = tokio::io::split(stream);
    Ok(Connection {
        reader: Reader {
            half: Half::Tls(read),
            buffer,
        },
        writer: Writer {
            half: WriteSide::Tls(write),
            buffer: outgoing,
        },
        peer,
    })
}

pub struct Connection {
    pub reader: Reader,
    pub writer: Writer,
    pub peer: SocketAddr,
}

/// Plain or encrypted; two real implementations, so an enum rather than a
/// trait object.
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
    buffer: ConnectionBuffer,
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
    /// Where the caller encodes what it wants sent, charged to the same
    /// budget as the read buffer. Both halves of a connection hold capacity
    /// and a budget that counted one of them was not a bound
    /// (docs/memory.md).
    buffer: ConnectionBuffer,
}

impl Writer {
    /// The buffer to encode into. Written out by [`Writer::flush`].
    pub fn buf_mut(&mut self) -> &mut BytesMut {
        self.buffer.buf_mut()
    }

    /// Capacity this writer is holding, as charged to the budget.
    pub fn held_bytes(&self) -> usize {
        self.buffer.charged()
    }

    /// Writes whatever was encoded, empties the buffer and returns any
    /// capacity a large batch forced. Returns how many bytes went out.
    ///
    /// The reclaim is what keeps the charge equal to what is retained: a
    /// `clear()` alone frees nothing, which is how this buffer used to keep
    /// the high-water mark of every batch it had ever seen for the life of
    /// the connection.
    pub async fn flush(&mut self) -> Result<usize, TransportError> {
        let Self { half, buffer } = self;
        let bytes = buffer.buf_mut();
        let wrote = bytes.len();
        if wrote == 0 {
            return Ok(0);
        }
        match half {
            WriteSide::Plain(half) => half.write_all(bytes).await?,
            WriteSide::Tls(half) => half.write_all(bytes).await?,
        }
        bytes.clear();
        buffer.reclaim();
        Ok(wrote)
    }

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

/// Dials a peer and hands back the halves, in plaintext. [`connect_node`]
/// is the same over TLS, with the node's own key material (ADR-0010).
///
/// The read budget is the same global one the listener uses, so bytes read
/// from a peer count against the same ceiling as bytes read from a client.
pub async fn connect(
    peer: SocketAddr,
    read_buffer_bytes: usize,
    write_buffer_bytes: usize,
    budget: Arc<BufferBudget>,
) -> Result<Connection, TransportError> {
    let stream = TcpStream::connect(peer).await?;
    // Small writes go out when they are written; a durability report waiting
    // on Nagle would be latency added for nothing.
    stream.set_nodelay(true)?;

    let buffer = ConnectionBuffer::new(read_buffer_bytes, Arc::clone(&budget))?;
    let outgoing = ConnectionBuffer::new(write_buffer_bytes, budget)?;
    let (read, write) = stream.into_split();
    Ok(Connection {
        reader: Reader {
            half: Half::Plain(read),
            buffer,
        },
        writer: Writer {
            half: WriteSide::Plain(write),
            buffer: outgoing,
        },
        peer,
    })
}

/// Dials node `id` at `address` over TLS (ADR-0010): the peer must prove
/// it is `id` ([`NodeTls::check`]) before anything is sent. The caller
/// bounds the whole of it in time.
pub async fn connect_node(
    address: SocketAddr,
    id: u8,
    tls: &NodeTls,
    read_buffer_bytes: usize,
    write_buffer_bytes: usize,
    budget: Arc<BufferBudget>,
) -> Result<(Connection, Peer), TransportError> {
    let stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    let (stream, peer) = tls.dial(stream, id).await?;
    let connection = halves(
        stream,
        address,
        read_buffer_bytes,
        write_buffer_bytes,
        budget,
    )?;
    Ok((connection, peer))
}
