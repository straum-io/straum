//! MQTT connections: sockets in, bytes to the node loop, bytes back out.
//! Each connection is a reader task and a writer task; nothing here knows
//! MQTT — the gateway in the node loop does.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use transport::Listener;

/// The node loop's hold on one connection. Dropping it closes the socket:
/// the writer's queue closes, and with it the connection's tasks.
pub struct Conn {
    pub writer: mpsc::Sender<Bytes>,
    /// One per read the reader may make: the gateway's backpressure.
    pub credit: mpsc::Sender<()>,
    reader: tokio::task::AbortHandle,
}

impl Drop for Conn {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

pub enum Event {
    Opened {
        conn: u64,
        handle: Conn,
    },
    Bytes {
        conn: u64,
        bytes: Vec<u8>,
    },
    Closed {
        conn: u64,
    },
    /// A credentials check finished (D-158 A).
    Authenticated {
        conn: u64,
        verdict: gateway::Verdict,
    },
}

static NEXT: AtomicU64 = AtomicU64::new(1);

/// Bounds on connections before the gateway sees them (D-164 A). A TLS
/// handshake happens there, so a client that never finishes one would
/// otherwise hold a task and a socket for good: each must finish `within`,
/// and at most `at_once` run together; past that, a new connection is
/// closed at once.
#[derive(Debug, Clone, Copy)]
pub struct Handshakes {
    pub within: Duration,
    pub at_once: usize,
}

pub async fn listen(
    listener: Listener,
    write_queue: usize,
    handshakes: Handshakes,
    events: mpsc::Sender<Event>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(handshakes.at_once.max(1)));
    loop {
        let Ok(accepted) = listener.accept().await else {
            continue;
        };
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            drop(accepted);
            continue;
        };
        let events = events.clone();
        tokio::spawn(async move {
            let established = tokio::time::timeout(handshakes.within, accepted.establish()).await;
            drop(permit);
            let Ok(Ok(connection)) = established else {
                return;
            };
            let conn = NEXT.fetch_add(1, Ordering::Relaxed);
            let (writer_tx, mut writer_rx) = mpsc::channel::<Bytes>(write_queue);
            let (credit_tx, mut credit_rx) = mpsc::channel::<()>(1);
            let mut reader = connection.reader;
            let mut writer = connection.writer;
            let reader_events = events.clone();
            let reading = tokio::spawn(async move {
                // Reads only with a credit from the node loop.
                while credit_rx.recv().await.is_some() {
                    match reader.fill().await {
                        Ok(0) | Err(_) => break,
                        Ok(_) => {
                            let bytes = reader.buf_mut().split().to_vec();
                            reader.reclaim();
                            if reader_events
                                .send(Event::Bytes { conn, bytes })
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                }
                let _ = reader_events.send(Event::Closed { conn }).await;
            });
            let handle = Conn {
                writer: writer_tx,
                credit: credit_tx,
                reader: reading.abort_handle(),
            };
            if events.send(Event::Opened { conn, handle }).await.is_err() {
                return;
            }
            // Writes until the node loop drops the connection.
            // One write for everything queued; the buffer is reused.
            let mut bytes = BytesMut::new();
            while let Some(first) = writer_rx.recv().await {
                bytes.clear();
                bytes.extend_from_slice(&first);
                while let Ok(more) = writer_rx.try_recv() {
                    bytes.extend_from_slice(&more);
                }
                if writer.write_all(&bytes).await.is_err() {
                    break;
                }
            }
            let _ = writer.shutdown().await;
        });
    }
}
