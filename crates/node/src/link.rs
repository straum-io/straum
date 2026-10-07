//! Links between nodes: one TCP connection per direction, framed by
//! `cluster::wire`. The dialler sends; the acceptor reads.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cluster::wire::{Frame, decode, encode};
use tokio::sync::mpsc;
use transport::{BufferBudget, Listener, TransportConfig};

/// What a link hands the node loop: who sent it, and what.
pub type Inbound = (u8, Frame);

const BACKOFF_FIRST: Duration = Duration::from_millis(50);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Sends to one peer for as long as the node runs: dials, introduces
/// itself, and writes what the node loop queues. A link that breaks drops
/// what was queued for it — none of it may reach the peer's next process
/// (`cluster::Message`) — and is dialled again.
pub async fn dial(
    peer: SocketAddr,
    hello: Frame,
    mut outbox: mpsc::Receiver<Frame>,
    transport: TransportConfig,
    budget: Arc<BufferBudget>,
) {
    let mut backoff = BACKOFF_FIRST;
    loop {
        let connected = transport::connect(
            peer,
            transport.read_buffer_bytes,
            transport.write_buffer_bytes,
            Arc::clone(&budget),
        )
        .await;
        let Ok(mut connection) = connected else {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
            continue;
        };
        backoff = BACKOFF_FIRST;
        // Queued for the last connection: addressed to a process that may
        // be gone.
        while outbox.try_recv().is_ok() {}
        let mut bytes = Vec::new();
        if encode(&hello, &mut bytes).is_err() || connection.writer.write_all(&bytes).await.is_err()
        {
            continue;
        }
        loop {
            let Some(first) = outbox.recv().await else {
                return;
            };
            // One write for everything waiting.
            bytes.clear();
            let mut frame = Some(first);
            while let Some(f) = frame.take() {
                if encode(&f, &mut bytes).is_err() {
                    tracing::error!("a frame to a peer does not encode");
                }
                frame = outbox.try_recv().ok();
            }
            if connection.writer.write_all(&bytes).await.is_err() {
                break;
            }
        }
    }
}

/// Accepts links from peers. A link opens with the sender's HELLO: an
/// unknown node, another slot, or other `apply` limits is refused (their
/// limits are part of what the log means).
pub async fn listen(
    listener: Listener,
    nodes: Vec<u8>,
    slot: u64,
    limits: u64,
    max_frame: usize,
    inbound: mpsc::Sender<Inbound>,
) {
    loop {
        let Ok(accepted) = listener.accept().await else {
            continue;
        };
        let inbound = inbound.clone();
        let nodes = nodes.clone();
        tokio::spawn(async move {
            let Ok(connection) = accepted.establish().await else {
                return;
            };
            read(connection.reader, nodes, slot, limits, max_frame, inbound).await;
        });
    }
}

async fn read(
    mut reader: transport::Reader,
    nodes: Vec<u8>,
    slot: u64,
    limits: u64,
    max_frame: usize,
    inbound: mpsc::Sender<Inbound>,
) {
    let mut from: Option<u8> = None;
    loop {
        loop {
            let parsed = decode(reader.buf_mut(), max_frame);
            let (frame, used) = match parsed {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "malformed frame from a peer, link closed");
                    return;
                }
            };
            let _ = reader.buf_mut().split_to(used);
            match (from, frame) {
                (
                    None,
                    Frame::Hello {
                        node,
                        slot: s,
                        limits: l,
                    },
                ) => {
                    if !nodes.contains(&node) || s != slot || l != limits {
                        tracing::error!(
                            node,
                            "link refused: unknown node, another slot, or other apply limits"
                        );
                        return;
                    }
                    from = Some(node);
                }
                (None, _) | (Some(_), Frame::Hello { .. }) => {
                    tracing::warn!("a link that did not open with HELLO, closed");
                    return;
                }
                (Some(node), frame) => {
                    // The reader waits when the node loop is behind: the
                    // peer's TCP window carries the backpressure.
                    if inbound.send((node, frame)).await.is_err() {
                        return;
                    }
                }
            }
        }
        reader.reclaim();
        match reader.fill().await {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}
