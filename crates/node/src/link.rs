//! Links between nodes: one TCP connection per direction, framed by
//! `cluster::wire`. The dialler sends; the acceptor reads.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cluster::wire::{Frame, WIRE_VERSION, decode, encode};
use tokio::sync::{mpsc, watch};
use transport::{BufferBudget, Listener, TransportConfig};

/// What a link hands the node loop: who sent it, and what.
pub type Inbound = (u8, Frame);

const BACKOFF_FIRST: Duration = Duration::from_millis(50);

/// The incarnation each peer last introduced itself with, as its links'
/// readers see it; 0 until one is seen (incarnations start at 1).
pub type Seen = Arc<BTreeMap<u8, watch::Sender<u64>>>;

/// Resolves once `peer` shows an incarnation other than the one `last`
/// holds, after it has shown one (D-151 A): the peer restarted, and a link
/// dialled before then may lead to a process that is gone, through a flow
/// that outlives it (a NAT or conntrack entry) and that a sender which only
/// writes would not notice for minutes. The first incarnation seen is only
/// noted. Never resolves once the readers are gone.
async fn restarted(peer: &mut watch::Receiver<u64>, last: &mut u64) {
    loop {
        if peer.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
        let now = *peer.borrow_and_update();
        let was = std::mem::replace(last, now);
        if was != 0 && was != now {
            return;
        }
    }
}

/// Sends to one peer for as long as the node runs: dials, introduces
/// itself, and writes what the node loop queues. A link that breaks drops
/// what was queued for it — none of it may reach the peer's next process
/// (`cluster::Message`) — and is dialled again; so is one, connecting or
/// connected, to a peer that has since introduced itself with another
/// incarnation (`restarted`). A redial carries this node's same
/// incarnation, so two live nodes never make each other redial.
pub async fn dial(
    peer: SocketAddr,
    hello: Frame,
    mut outbox: mpsc::Receiver<Frame>,
    transport: TransportConfig,
    budget: Arc<BufferBudget>,
    heartbeat: Duration,
    mut incarnation: watch::Receiver<u64>,
) {
    let mut last = *incarnation.borrow_and_update();
    // Never longer than a heartbeat (D-122): a peer that restarts hears
    // this node within one, inside the `takeover_ticks` of silence after
    // which it would take a live sequencer's slot over. A 1 s cap let it
    // (6 runs of 6 in `a_restarted_replica_leaves_the_slot_with_its_live_owner`).
    // The cost: one failed connect per heartbeat to a peer that is down.
    let first = BACKOFF_FIRST.min(heartbeat);
    let mut backoff = first;
    loop {
        let connected = tokio::select! {
            connected = transport::connect(
                peer,
                transport.read_buffer_bytes,
                transport.write_buffer_bytes,
                Arc::clone(&budget),
            ) => connected,
            () = restarted(&mut incarnation, &mut last) => continue,
        };
        let Ok(mut connection) = connected else {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(heartbeat);
            continue;
        };
        backoff = first;
        // Queued for the last connection: addressed to a process that may
        // be gone.
        while outbox.try_recv().is_ok() {}
        let mut bytes = Vec::new();
        if encode(&hello, &mut bytes).is_err() {
            continue;
        }
        loop {
            let written = tokio::select! {
                written = connection.writer.write_all(&bytes) => written,
                () = restarted(&mut incarnation, &mut last) => {
                    tracing::info!(%peer, "the peer restarted: its link is dialled again");
                    break;
                }
            };
            if written.is_err() {
                break;
            }
            let next = tokio::select! {
                next = outbox.recv() => next,
                () = restarted(&mut incarnation, &mut last) => {
                    tracing::info!(%peer, "the peer restarted: its link is dialled again");
                    break;
                }
            };
            let Some(first) = next else {
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
        }
    }
}

/// Accepts links from peers. A link opens with the sender's HELLO: another
/// frame version, an unknown node, another slot, or another configuration
/// digest — cluster parameters or `apply` limits — is refused.
pub async fn listen(
    listener: Listener,
    nodes: Vec<u8>,
    slot: u64,
    config: u64,
    max_frame: usize,
    inbound: mpsc::Sender<Inbound>,
    seen: Seen,
) {
    loop {
        let Ok(accepted) = listener.accept().await else {
            continue;
        };
        let inbound = inbound.clone();
        let nodes = nodes.clone();
        let seen = Arc::clone(&seen);
        tokio::spawn(async move {
            let Ok(connection) = accepted.establish().await else {
                return;
            };
            read(
                connection.reader,
                nodes,
                slot,
                config,
                max_frame,
                inbound,
                seen,
            )
            .await;
        });
    }
}

async fn read(
    mut reader: transport::Reader,
    nodes: Vec<u8>,
    slot: u64,
    config: u64,
    max_frame: usize,
    inbound: mpsc::Sender<Inbound>,
    seen: Seen,
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
                        version,
                        node,
                        slot: s,
                        config: c,
                        incarnation,
                    },
                ) => {
                    if version != WIRE_VERSION {
                        tracing::error!(
                            node,
                            version,
                            expected = WIRE_VERSION,
                            "link refused: another frame version"
                        );
                        return;
                    }
                    if !nodes.contains(&node) || s != slot || c != config {
                        tracing::error!(
                            node,
                            "link refused: unknown node, another slot, or another configuration (cluster parameters or apply limits)"
                        );
                        return;
                    }
                    // Its dialler here redials if this is another life (D-151 A).
                    if let Some(peer) = seen.get(&node) {
                        peer.send_if_modified(|last| {
                            std::mem::replace(last, incarnation) != incarnation
                        });
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

#[cfg(test)]
mod tests {
    use super::*;

    /// D-151 A: the first incarnation seen is noted, the same one again is
    /// nothing, another one is a restart.
    #[tokio::test]
    async fn only_another_incarnation_after_the_first_is_a_restart() {
        let (peer, mut rx) = watch::channel(0u64);
        let mut last = 0;
        let wait = Duration::from_millis(50);
        peer.send_replace(4);
        let first = tokio::time::timeout(wait, restarted(&mut rx, &mut last)).await;
        assert!(first.is_err(), "the first one is only noted");
        assert_eq!(last, 4);
        peer.send_replace(4);
        let same = tokio::time::timeout(wait, restarted(&mut rx, &mut last)).await;
        assert!(same.is_err(), "the same one again");
        peer.send_replace(5);
        let other = tokio::time::timeout(wait, restarted(&mut rx, &mut last)).await;
        assert!(other.is_ok(), "another one");
        assert_eq!(last, 5);
    }
}
