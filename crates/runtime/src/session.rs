//! What the broker loop hands to one connection's writer.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use protocol::Outbound;
use tokio::sync::mpsc;

/// Everything that can go out on a connection. PINGRESP never reaches the
/// broker (CLAUDE.md §5), so it travels beside its effects, not through them.
#[derive(Debug)]
pub(crate) enum Egress {
    Out(Outbound),
    Pong,
}

/// The broker loop's end of one session's writer queue.
#[derive(Debug)]
pub(crate) struct Sink {
    pub(crate) tx: mpsc::Sender<Egress>,
    /// Payload bytes queued for this session. Incremented here, decremented
    /// by the writer when it dequeues (CLAUDE.md §8).
    pub(crate) queued: Arc<AtomicUsize>,
}

/// Bytes an item occupies in a session queue. Replies and pongs are a handful
/// of bytes and are bounded by the message count instead.
pub(crate) fn weight(egress: &Egress) -> usize {
    match egress {
        Egress::Out(Outbound::Deliver(delivery)) => delivery
            .payload
            .len()
            .saturating_add(delivery.topic.as_bytes().len()),
        _ => 0,
    }
}
