//! TLS between nodes (ADR-0010): both sides prove a name under the cluster
//! CA, and a reload's CA, or the clock, can take that proof back.
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use transport::{BufferBudget, Listener, NodeTls, TransportConfig, connect_node};

/// A cluster CA that issues node certificates into a directory.
struct Ca {
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
    pem: String,
}

impl Ca {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = params.self_signed(&key).unwrap();
        Self {
            pem: certificate.pem(),
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    /// Node `id`'s files under `dir`, naming `names`, valid until `until`
    /// (rcgen's far future when `None`).
    fn issue(
        &self,
        dir: &Path,
        id: u8,
        names: &[String],
        until: Option<time::OffsetDateTime>,
    ) -> (PathBuf, PathBuf, PathBuf) {
        let mut params = rcgen::CertificateParams::new(names.to_vec()).unwrap();
        if let Some(until) = until {
            params.not_after = until;
        }
        let key = rcgen::KeyPair::generate().unwrap();
        let certificate = params.signed_by(&key, &self.issuer).unwrap();
        let files = (
            dir.join(format!("node-{id}.pem")),
            dir.join(format!("node-{id}.key")),
            dir.join("ca.pem"),
        );
        std::fs::write(&files.0, certificate.pem()).unwrap();
        std::fs::write(&files.1, key.serialize_pem()).unwrap();
        std::fs::write(&files.2, &self.pem).unwrap();
        files
    }

    fn node(&self, dir: &Path, id: u8) -> NodeTls {
        let (cert, key, ca) = self.issue(dir, id, &[transport::node_name(id)], None);
        NodeTls::from_pem_files(id, &cert, &key, &ca).unwrap()
    }
}

fn dir(test: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("straum-node-tls-{}-{test}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn listener() -> (Listener, SocketAddr) {
    let config = TransportConfig {
        listen: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..TransportConfig::default()
    };
    let listener = Listener::bind(config, budget(), None).unwrap();
    let address = listener.local_addr().unwrap();
    (listener, address)
}

fn budget() -> Arc<BufferBudget> {
    Arc::new(BufferBudget::new(16 << 20))
}

/// One link from `dialler` to node `id` at the accepting `acceptor`:
/// the dialler's result, and the acceptor's.
async fn link(
    dialler: &NodeTls,
    acceptor: &NodeTls,
    id: u8,
) -> (
    Result<(transport::Connection, transport::Peer), transport::TransportError>,
    Result<(transport::Connection, transport::Peer), transport::TransportError>,
) {
    let (listener, address) = listener();
    let acceptor = acceptor.clone();
    let accepted = tokio::spawn(async move {
        let accepted = listener.accept().await.unwrap();
        accepted.establish_node(&acceptor).await
    });
    let dialled = tokio::time::timeout(
        Duration::from_secs(5),
        connect_node(address, id, dialler, 4096, 4096, budget()),
    )
    .await
    .unwrap();
    let accepted = tokio::time::timeout(Duration::from_secs(5), accepted)
        .await
        .unwrap()
        .unwrap();
    (dialled, accepted)
}

/// D-171 A: the dialler proves the acceptor is the node it dialled, and the
/// acceptor learns a chain that proves the dialler's name, which is what its
/// HELLO is checked against. Bytes then pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn two_nodes_of_one_ca_link_and_each_proves_its_name() {
    let dir = dir("link");
    for d in ["1", "2"] {
        create(&dir.join(d));
    }
    let ca = Ca::new();
    let (one, two) = (ca.node(&dir.join("1"), 1), ca.node(&dir.join("2"), 2));
    let (dialled, accepted) = link(&one, &two, 2).await;
    let (mut dialled, _) = dialled.unwrap();
    let (mut accepted, peer) = accepted.unwrap();
    two.check(&peer, 1).unwrap();
    assert!(two.check(&peer, 0).is_err(), "the dialler is node 1, not 0");
    dialled.writer.write_all(b"hello").await.unwrap();
    let mut read = 0;
    while read < 5 {
        read += accepted.reader.fill().await.unwrap();
    }
    assert_eq!(accepted.reader.buf_mut().as_ref(), b"hello");
}

fn create(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
}

/// D-171 A: a node that answers with another node's name is refused by the
/// dialler before anything is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dialler_refuses_a_peer_that_names_another_node() {
    let dir = dir("other-name");
    let ca = Ca::new();
    for d in ["1", "3"] {
        create(&dir.join(d));
    }
    let (one, three) = (ca.node(&dir.join("1"), 1), ca.node(&dir.join("3"), 3));
    let (dialled, _) = link(&one, &three, 2).await;
    let error = dialled
        .err()
        .expect("node 3 was taken for node 2")
        .to_string();
    assert!(error.contains("node identity"), "{error}");
}

/// A certificate of another CA does not link, either way.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_of_another_ca_does_not_link() {
    let dir = dir("other-ca");
    for d in ["1", "2"] {
        create(&dir.join(d));
    }
    let one = Ca::new().node(&dir.join("1"), 1);
    let two = Ca::new().node(&dir.join("2"), 2);
    let (dialled, accepted) = link(&one, &two, 2).await;
    assert!(dialled.is_err());
    assert!(accepted.is_err());
}

/// D-171 A: a node's own certificate must name it, or its files do not load.
#[test]
fn a_node_whose_own_certificate_names_another_does_not_load() {
    let dir = dir("own-name");
    let ca = Ca::new();
    let (cert, key, root) = ca.issue(&dir, 1, &[transport::node_name(2)], None);
    let error = NodeTls::load(1, &cert, &key, &root)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("names 2.node.straum, not 1.node.straum"),
        "{error}"
    );
    let (cert, key, root) = ca.issue(&dir, 1, &["*.node.straum".to_owned()], None);
    assert!(NodeTls::load(1, &cert, &key, &root).is_err());
}

/// D-172 C: after a CA that does not sign the peer is installed, its
/// chain no longer checks on either side; under a renewal from the same CA
/// it still does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_ca_takes_the_proof_back_and_a_renewal_does_not() {
    let dir = dir("reload");
    for d in ["1", "2", "1-renewed", "1-new"] {
        create(&dir.join(d));
    }
    let ca = Ca::new();
    let (one, two) = (ca.node(&dir.join("1"), 1), ca.node(&dir.join("2"), 2));
    let (dialled, accepted) = link(&one, &two, 2).await;
    let (_, to_two) = dialled.unwrap();
    let (_, from_one) = accepted.unwrap();

    let (cert, key, root) = ca.issue(&dir.join("1-renewed"), 1, &[transport::node_name(1)], None);
    one.install(NodeTls::load(1, &cert, &key, &root).unwrap());
    one.check(&to_two, 2).unwrap();

    let other = Ca::new();
    let (cert, key, root) = other.issue(&dir.join("1-new"), 1, &[transport::node_name(1)], None);
    let (cert2, key2, root2) = other.issue(&dir.join("2"), 2, &[transport::node_name(2)], None);
    one.install(NodeTls::load(1, &cert, &key, &root).unwrap());
    assert!(
        one.check(&to_two, 2).is_err(),
        "node 2's chain checked under a CA that never signed it"
    );
    // And the other way: node 2 under the new CA no longer takes node 1's
    // old chain.
    two.install(NodeTls::load(2, &cert2, &key2, &root2).unwrap());
    assert!(two.check(&from_one, 1).is_err());
}

/// D-172 C: a peer certificate that has expired since its handshake fails
/// the next check.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_peer_certificate_fails_the_next_check() {
    let dir = dir("expiry");
    for d in ["1", "2"] {
        create(&dir.join(d));
    }
    let ca = Ca::new();
    let one = ca.node(&dir.join("1"), 1);
    let until = time::OffsetDateTime::now_utc() + time::Duration::seconds(3);
    let (cert, key, root) = ca.issue(&dir.join("2"), 2, &[transport::node_name(2)], Some(until));
    let two = NodeTls::from_pem_files(2, &cert, &key, &root).unwrap();
    let (dialled, _) = link(&one, &two, 2).await;
    let (_, to_two) = dialled.unwrap();
    one.check(&to_two, 2).unwrap();
    tokio::time::sleep(Duration::from_secs(4)).await;
    assert!(
        one.check(&to_two, 2).is_err(),
        "an expired certificate still checked"
    );
}

/// ADR-0010: every node both dials and accepts, so its certificate needs
/// client use too. One made for TLS servers alone is refused by the node
/// it dials.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_certificate_for_servers_alone_cannot_dial() {
    let dir = dir("server-only");
    for d in ["1", "2"] {
        create(&dir.join(d));
    }
    let ca = Ca::new();
    let two = ca.node(&dir.join("2"), 2);
    let mut params = rcgen::CertificateParams::new(vec![transport::node_name(1)]).unwrap();
    params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
    let key = rcgen::KeyPair::generate().unwrap();
    let certificate = params.signed_by(&key, &ca.issuer).unwrap();
    let (cert, key_file, root) = (
        dir.join("1/node-1.pem"),
        dir.join("1/node-1.key"),
        dir.join("1/ca.pem"),
    );
    std::fs::write(&cert, certificate.pem()).unwrap();
    std::fs::write(&key_file, key.serialize_pem()).unwrap();
    std::fs::write(&root, &ca.pem).unwrap();
    let one = NodeTls::from_pem_files(1, &cert, &key_file, &root).unwrap();
    let (_, accepted) = link(&one, &two, 2).await;
    assert!(
        accepted.is_err(),
        "a certificate without client use was accepted"
    );
}
