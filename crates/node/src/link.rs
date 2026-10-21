//! Links between nodes: one TCP connection per direction, framed by
//! `cluster::wire`. The dialler sends; the acceptor reads. With
//! `NodeConfig::tls`, every link is TLS and each end proves which node it
//! is before the link counts (ADR-0010).

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cluster::wire::{Frame, WIRE_VERSION, decode, encode};
use tokio::sync::{Semaphore, mpsc, watch};
use transport::{
    BufferBudget, Connection, Installed, Listener, NodeTls, Peer, TransportConfig, TransportError,
};

/// What a link hands the node loop: who sent it, and what.
pub type Inbound = (u8, Frame);

const BACKOFF_FIRST: Duration = Duration::from_millis(50);

/// The incarnation each peer last introduced itself with, as its links'
/// readers see it; 0 until one is seen (incarnations start at 1).
pub type Seen = Arc<BTreeMap<u8, watch::Sender<u64>>>;

/// Links being opened at once, and how long one may take from accept to
/// its HELLO (ADR-0010): with or without TLS, a connection that has not
/// introduced itself holds a permit, never more than `at_once` of them.
#[derive(Debug, Clone, Copy)]
pub struct Handshakes {
    pub within: Duration,
    pub at_once: usize,
}

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

/// Resolves, with the reason, once a reload has installed files under which
/// `proof` no longer shows that the peer is node `id` (D-172 C): another
/// CA, an expired certificate. A link that has nothing to check again fails
/// at once rather than stay open unchecked. Never resolves without TLS.
async fn revoked(
    tls: Option<&NodeTls>,
    changes: &mut Option<watch::Receiver<Arc<Installed>>>,
    proof: Option<&Peer>,
    id: u8,
) -> String {
    let (Some(tls), Some(changes)) = (tls, changes.as_mut()) else {
        return std::future::pending().await;
    };
    let Some(proof) = proof else {
        return "no certificate chain kept to check again".into();
    };
    loop {
        if changes.changed().await.is_err() {
            return std::future::pending().await;
        }
        if let Err(error) = tls.check(proof, id) {
            return error.to_string();
        }
    }
}

/// Why a dial did not give a link.
enum Failed {
    /// No connection: the peer is down or unreachable.
    Down,
    /// The handshake failed, with the reason.
    Refused(String),
    /// Connect and handshake took the whole heartbeat they are given.
    Stalled,
}

/// Connects to node `id`. With TLS, connect and handshake together within
/// one heartbeat (ADR-0010).
async fn open(
    address: SocketAddr,
    id: u8,
    tls: Option<&NodeTls>,
    transport: TransportConfig,
    budget: &Arc<BufferBudget>,
    heartbeat: Duration,
) -> Result<(Connection, Option<Peer>), Failed> {
    let (read, write) = (transport.read_buffer_bytes, transport.write_buffer_bytes);
    let Some(tls) = tls else {
        return transport::connect(address, read, write, Arc::clone(budget))
            .await
            .map(|connection| (connection, None))
            .map_err(|_| Failed::Down);
    };
    let connected = transport::connect_node(address, id, tls, read, write, Arc::clone(budget));
    match tokio::time::timeout(heartbeat, connected).await {
        Ok(Ok((connection, proof))) => Ok((connection, Some(proof))),
        Ok(Err(TransportError::Io(_))) => Err(Failed::Down),
        Ok(Err(error)) => Err(Failed::Refused(error.to_string())),
        Err(_) => Err(Failed::Stalled),
    }
}

/// Sends to one peer for as long as the node runs: dials, introduces
/// itself, and writes what the node loop queues. A link that breaks drops
/// what was queued for it — none of it may reach the peer's next process
/// (`cluster::Message`) — and is dialled again; so is one, connecting or
/// connected, to a peer that has since introduced itself with another
/// incarnation (`restarted`), and one whose peer no longer proves its name
/// after a reload (`revoked`). A redial carries this node's same
/// incarnation, so two live nodes never make each other redial.
#[allow(clippy::too_many_arguments)]
pub async fn dial(
    peer: SocketAddr,
    id: u8,
    hello: Frame,
    mut outbox: mpsc::Receiver<Frame>,
    transport: TransportConfig,
    budget: Arc<BufferBudget>,
    heartbeat: Duration,
    mut incarnation: watch::Receiver<u64>,
    tls: Option<NodeTls>,
) {
    let mut last = *incarnation.borrow_and_update();
    // Never longer than a heartbeat (D-122): a peer that restarts hears
    // this node within one, inside the `takeover_ticks` of silence after
    // which it would take a live sequencer's slot over. A 1 s cap let it
    // (6 runs of 6 in `a_restarted_replica_leaves_the_slot_with_its_live_owner`).
    // The cost: one failed connect per heartbeat to a peer that is down.
    let first = BACKOFF_FIRST.min(heartbeat);
    let mut backoff = first;
    // The last refusal logged: the same one again every heartbeat is not
    // news.
    let mut refused: Option<String> = None;
    loop {
        // Before the handshake, so a reload during it is not missed.
        let mut changes = tls.as_ref().map(NodeTls::changes);
        let connected = tokio::select! {
            connected = open(peer, id, tls.as_ref(), transport, &budget, heartbeat) => connected,
            () = restarted(&mut incarnation, &mut last) => continue,
        };
        let (mut connection, proof) = match connected {
            Ok(connected) => connected,
            Err(failed) => {
                let stalled = matches!(failed, Failed::Stalled);
                let reason = match failed {
                    Failed::Down => None,
                    Failed::Refused(reason) => Some(reason),
                    Failed::Stalled => {
                        Some("connect and handshake did not finish within a heartbeat".into())
                    }
                };
                if let Some(reason) = reason {
                    if refused.as_ref() != Some(&reason) {
                        tracing::error!(%peer, node = id, %reason, "link to a peer refused");
                        refused = Some(reason);
                    }
                }
                // A stalled attempt has already waited its heartbeat: that
                // is its backoff. Another would let a backoff, a stall and
                // a backoff again pass before a restarted peer hears this
                // node. Without it, at most a backoff, a stall, the
                // handshake that succeeds and the wait for the next
                // heartbeat: `3h + t_ok`, inside the four heartbeats
                // `takeover_ticks` is held to with TLS (D-173 A, D-122 A).
                if !stalled {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(heartbeat);
                }
                continue;
            }
        };
        refused = None;
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
                reason = revoked(tls.as_ref(), &mut changes, proof.as_ref(), id) => {
                    tracing::error!(%peer, node = id, %reason, "the peer does not prove its name under the reloaded files: link closed");
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
                reason = revoked(tls.as_ref(), &mut changes, proof.as_ref(), id) => {
                    tracing::error!(%peer, node = id, %reason, "the peer does not prove its name under the reloaded files: link closed");
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

/// What every accepted link is checked against.
#[derive(Clone)]
pub struct Accepting {
    /// This node: a HELLO naming it comes from a second host holding its
    /// identity.
    pub own: u8,
    pub nodes: Vec<u8>,
    pub slot: u64,
    pub config: u64,
    pub max_frame: usize,
    pub tls: Option<NodeTls>,
    pub handshakes: Handshakes,
}

/// Accepts links from peers. A link opens with the sender's HELLO: another
/// frame version, an unknown node, this node's own id, another slot, or
/// another configuration digest — cluster parameters or `apply` limits — is
/// refused. With TLS, the sender's certificate must name the node its
/// HELLO names, checked before HELLO has any effect (D-171 A).
pub async fn listen(
    listener: Listener,
    accepting: Accepting,
    inbound: mpsc::Sender<Inbound>,
    seen: Seen,
) {
    let permits = Arc::new(Semaphore::new(accepting.handshakes.at_once));
    let mut full = false;
    loop {
        let Ok(accepted) = listener.accept().await else {
            continue;
        };
        // Past the bound, closed at once: a peer's dialler tries again
        // within a heartbeat.
        let Ok(permit) = Arc::clone(&permits).try_acquire_owned() else {
            if !full {
                tracing::warn!(
                    at_once = accepting.handshakes.at_once,
                    "links refused: as many handshakes as allowed are in progress"
                );
                full = true;
            }
            continue;
        };
        full = false;
        let inbound = inbound.clone();
        let accepting = accepting.clone();
        let seen = Arc::clone(&seen);
        let from = accepted.peer();
        tokio::spawn(async move {
            let mut changes = accepting.tls.as_ref().map(NodeTls::changes);
            let opened = tokio::time::timeout(
                accepting.handshakes.within,
                introduced(accepted, &accepting, &seen),
            )
            .await;
            drop(permit);
            let (connection, node, proof) = match opened {
                Ok(Some(opened)) => opened,
                Ok(None) => return,
                Err(_) => {
                    // Debug, as every refusal before a HELLO names a node:
                    // anyone who reaches the port can cause one.
                    tracing::debug!(%from, "link closed: no HELLO in time");
                    return;
                }
            };
            read(
                connection,
                node,
                proof,
                accepting.tls.as_ref(),
                &mut changes,
                accepting.max_frame,
                inbound,
            )
            .await;
        });
    }
}

/// The handshake and the HELLO: the connection, the node it proved to be,
/// and its certificate chain, or `None` for a link refused. Nothing a HELLO
/// says has an effect until every check has passed.
async fn introduced(
    accepted: transport::Accepted,
    accepting: &Accepting,
    seen: &Seen,
) -> Option<(Connection, u8, Option<Peer>)> {
    let accepted_from = accepted.peer();
    let (mut connection, proof) = match &accepting.tls {
        Some(tls) => match accepted.establish_node(tls).await {
            Ok((connection, proof)) => (connection, Some(proof)),
            Err(error) => {
                tracing::debug!(from = %accepted_from, %error, "link handshake failed");
                return None;
            }
        },
        None => (accepted.establish().await.ok()?, None),
    };
    // The writer stays with the connection: dropped, it would close the
    // link's other direction.
    let reader = &mut connection.reader;
    loop {
        match decode(reader.buf_mut(), accepting.max_frame) {
            Ok(Some((frame, used))) => {
                let _ = reader.buf_mut().split_to(used);
                let Frame::Hello {
                    version,
                    node,
                    slot,
                    config,
                    incarnation,
                } = frame
                else {
                    tracing::warn!("a link that did not open with HELLO, closed");
                    return None;
                };
                if let (Some(tls), Some(proof)) = (&accepting.tls, &proof) {
                    if let Err(error) = tls.check(proof, node) {
                        tracing::error!(
                            node,
                            %error,
                            "link refused: the certificate does not prove the node its HELLO names"
                        );
                        return None;
                    }
                }
                if node == accepting.own {
                    tracing::error!(
                        node,
                        "link refused: a HELLO names this node itself; two hosts hold one node's identity"
                    );
                    return None;
                }
                if version != WIRE_VERSION {
                    tracing::error!(
                        node,
                        version,
                        expected = WIRE_VERSION,
                        "link refused: another frame version"
                    );
                    return None;
                }
                if !accepting.nodes.contains(&node)
                    || slot != accepting.slot
                    || config != accepting.config
                {
                    tracing::error!(
                        node,
                        "link refused: unknown node, another slot, or another configuration (cluster parameters or apply limits)"
                    );
                    return None;
                }
                // Its dialler here redials if this is another life (D-151 A).
                if let Some(peer) = seen.get(&node) {
                    peer.send_if_modified(|last| {
                        std::mem::replace(last, incarnation) != incarnation
                    });
                }
                return Some((connection, node, proof));
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(%error, "malformed frame from a peer, link closed");
                return None;
            }
        }
        reader.reclaim();
        match reader.fill().await {
            Ok(0) | Err(_) => return None,
            Ok(_) => {}
        }
    }
}

/// Hands what an introduced link carries to the node loop, until it ends or
/// its peer no longer proves its name after a reload (D-172 C).
async fn read(
    mut connection: Connection,
    node: u8,
    proof: Option<Peer>,
    tls: Option<&NodeTls>,
    changes: &mut Option<watch::Receiver<Arc<Installed>>>,
    max_frame: usize,
    inbound: mpsc::Sender<Inbound>,
) {
    let reader = &mut connection.reader;
    loop {
        loop {
            let (frame, used) = match decode(reader.buf_mut(), max_frame) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(error) => {
                    tracing::warn!(%error, "malformed frame from a peer, link closed");
                    return;
                }
            };
            let _ = reader.buf_mut().split_to(used);
            if let Frame::Hello { .. } = frame {
                tracing::warn!(node, "a second HELLO on a link, closed");
                return;
            }
            // The reader waits when the node loop is behind: the peer's
            // TCP window carries the backpressure.
            if inbound.send((node, frame)).await.is_err() {
                return;
            }
        }
        reader.reclaim();
        // A reload first: a peer that no longer proves its name is not
        // read from again (D-172 C).
        let filled = tokio::select! {
            biased;
            reason = revoked(tls, changes, proof.as_ref(), node) => {
                tracing::error!(node, %reason, "the peer does not prove its name under the reloaded files: link closed");
                return;
            }
            filled = reader.fill() => filled,
        };
        match filled {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Node `id`'s link TLS, issued by `ca`.
    fn node_tls(ca: &(rcgen::Issuer<'static, rcgen::KeyPair>, String), id: u8) -> NodeTls {
        let dir = std::env::temp_dir().join(format!(
            "straum-link-{}-{id}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec![transport::node_name(id)]).unwrap();
        let certificate = params.signed_by(&key, &ca.0).unwrap();
        let (c, k, r) = (dir.join("c.pem"), dir.join("k.pem"), dir.join("ca.pem"));
        std::fs::write(&c, certificate.pem()).unwrap();
        std::fs::write(&k, key.serialize_pem()).unwrap();
        std::fs::write(&r, &ca.1).unwrap();
        let tls = NodeTls::from_pem_files(id, &c, &k, &r).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
        tls
    }

    fn ca() -> (rcgen::Issuer<'static, rcgen::KeyPair>, String) {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let pem = params.self_signed(&key).unwrap().pem();
        (rcgen::Issuer::new(params, key), pem)
    }

    fn hello(node: u8, incarnation: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        encode(
            &Frame::Hello {
                version: WIRE_VERSION,
                node,
                slot: 1,
                config: 7,
                incarnation,
            },
            &mut bytes,
        )
        .unwrap();
        bytes
    }

    /// Node 0 accepts one link: whether it is introduced, and the
    /// incarnations it has seen after. `dial` writes on the link.
    async fn accept_one<F, Fut>(tls: Option<NodeTls>, dial: F) -> (bool, Vec<u64>)
    where
        F: FnOnce(SocketAddr) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let listener = Listener::bind(
            TransportConfig {
                listen: SocketAddr::from(([127, 0, 0, 1], 0)),
                ..TransportConfig::default()
            },
            Arc::new(BufferBudget::new(1 << 20)),
            None,
        )
        .unwrap();
        let address = listener.local_addr().unwrap();
        let seen: Seen = Arc::new(
            [1u8, 2]
                .into_iter()
                .map(|id| (id, watch::Sender::new(5u64)))
                .collect(),
        );
        let accepting = Accepting {
            own: 0,
            nodes: vec![0, 1, 2],
            slot: 1,
            config: 7,
            max_frame: 1 << 20,
            tls,
            handshakes: Handshakes {
                within: Duration::from_secs(5),
                at_once: 4,
            },
        };
        let accepted = {
            let seen = Arc::clone(&seen);
            tokio::spawn(async move {
                let accepted = listener.accept().await.unwrap();
                introduced(accepted, &accepting, &seen).await.is_some()
            })
        };
        dial(address).await;
        let introduced = accepted.await.unwrap();
        let incarnations = seen.values().map(|s| *s.borrow()).collect();
        (introduced, incarnations)
    }

    /// D-171 A: a HELLO from a node other than the one the certificate names
    /// is refused before it has any effect; the one it names is taken.
    #[tokio::test]
    async fn a_hello_counts_only_from_the_node_its_certificate_names() {
        let ca = ca();
        let (zero, one) = (node_tls(&ca, 0), node_tls(&ca, 1));
        for (claims, introduced, incarnations) in
            [(2u8, false, vec![5u64, 5]), (1, true, vec![9, 5])]
        {
            let one = one.clone();
            let got = accept_one(Some(zero.clone()), |address| async move {
                let (mut connection, _) = transport::connect_node(
                    address,
                    0,
                    &one,
                    4096,
                    4096,
                    Arc::new(BufferBudget::new(1 << 20)),
                )
                .await
                .unwrap();
                connection
                    .writer
                    .write_all(&hello(claims, 9))
                    .await
                    .unwrap();
                // Held open until the acceptor has decided.
                tokio::time::sleep(Duration::from_millis(200)).await;
            })
            .await;
            assert_eq!(got, (introduced, incarnations), "node 1 claiming {claims}");
        }
    }

    /// No mixed mode (ADR-0010): a plaintext dialler at a TLS acceptor, and
    /// a TLS dialler at a plaintext acceptor, never link, and neither HELLO
    /// has an effect.
    #[tokio::test]
    async fn a_plaintext_node_and_a_tls_node_never_link() {
        let ca = ca();
        let (zero, one) = (node_tls(&ca, 0), node_tls(&ca, 1));
        let got = accept_one(Some(zero), |address| async move {
            use tokio::io::AsyncWriteExt;
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream.write_all(&hello(1, 9)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        assert_eq!(got, (false, vec![5, 5]), "a plaintext HELLO at a TLS node");
        let got = accept_one(None, |address| async move {
            let dialled = tokio::time::timeout(
                Duration::from_secs(1),
                transport::connect_node(
                    address,
                    0,
                    &one,
                    4096,
                    4096,
                    Arc::new(BufferBudget::new(1 << 20)),
                ),
            )
            .await;
            assert!(
                !matches!(dialled, Ok(Ok(_))),
                "a TLS dialler linked to a plaintext node"
            );
        })
        .await;
        assert_eq!(
            got,
            (false, vec![5, 5]),
            "a TLS dialler at a plaintext node"
        );
    }

    /// A HELLO naming the acceptor itself is refused: two hosts would hold
    /// one node's identity.
    #[tokio::test]
    async fn a_hello_naming_the_acceptor_itself_is_refused() {
        let got = accept_one(None, |address| async move {
            use tokio::io::AsyncWriteExt;
            let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
            stream.write_all(&hello(0, 9)).await.unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        })
        .await;
        assert_eq!(got, (false, vec![5, 5]));
    }

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
