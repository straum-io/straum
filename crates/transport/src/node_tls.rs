//! TLS between nodes (ADR-0010, D-171 A, D-172 C).
//!
//! Every link presents this node's certificate and requires the peer's,
//! both signed by the cluster CA, over TLS 1.3 only, with no session
//! resumption and no early data: each link proves its chain against the CA
//! installed when it connects. A certificate names its node exactly once,
//! as the dNSName `<id>.node.straum`; [`NodeTls::check`] applies that rule
//! and the chain together, at the handshake and again after every reload.

use std::path::Path;
use std::sync::Arc;

use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio_rustls::rustls::client::{Resumption, WebPkiServerVerifier, danger::ServerCertVerifier};
use tokio_rustls::rustls::crypto::ring;
use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use tokio_rustls::rustls::server::{
    NoServerSessionStorage, WebPkiClientVerifier, danger::ClientCertVerifier,
};
use tokio_rustls::rustls::version::TLS13;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio_rustls::{TlsAcceptor, TlsConnector, TlsStream};

use crate::tls::{TlsError, certificates, private_key};

/// The domain every node's name sits under.
const DOMAIN: &str = ".node.straum";

/// The one name node `id`'s certificate must carry: decimal, no leading
/// zeros.
pub fn node_name(id: u8) -> String {
    format!("{id}{DOMAIN}")
}

/// How this node met a peer: it dialled the peer (the peer is the TLS
/// server) or accepted it (the peer is the client).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Dialled,
    Accepted,
}

/// A peer's certificate chain as the handshake verified it, kept for as long
/// as its link is open so a reload can check it again (D-172 C).
#[derive(Debug, Clone)]
pub struct Peer {
    side: Side,
    chain: Vec<CertificateDer<'static>>,
}

/// One installed set of files: this node's certificate and key, and the
/// cluster CA, with the configurations and verifiers built from them.
pub struct Installed {
    id: u8,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
    servers: Arc<WebPkiServerVerifier>,
    clients: Arc<dyn ClientCertVerifier>,
}

impl std::fmt::Debug for Installed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Installed").field("id", &self.id).finish()
    }
}

/// Files read and checked, not yet in use: a reload loads every file before
/// it installs anything (D-166 A).
#[derive(Debug)]
pub struct LoadedNode(Arc<Installed>);

/// This node's link TLS. Every clone shares what is installed; a change is
/// seen by every open link through [`NodeTls::changes`].
#[derive(Debug, Clone)]
pub struct NodeTls {
    current: Arc<watch::Sender<Arc<Installed>>>,
}

impl NodeTls {
    /// Loads node `id`'s certificate chain, its key and the cluster CA.
    pub fn from_pem_files(
        id: u8,
        certificate: &Path,
        key: &Path,
        ca: &Path,
    ) -> Result<Self, TlsError> {
        Self::load(id, certificate, key, ca).map(Self::from_loaded)
    }

    /// Files already loaded, in use from now on.
    pub fn from_loaded(LoadedNode(installed): LoadedNode) -> Self {
        Self {
            current: Arc::new(watch::Sender::new(installed)),
        }
    }

    /// Reads and checks the files without using them: the key matches the
    /// certificate, the certificate names node `id` by the rule, and the CA
    /// holds at least one certificate.
    pub fn load(id: u8, certificate: &Path, key: &Path, ca: &Path) -> Result<LoadedNode, TlsError> {
        let chain = certificates(certificate)?;
        let key = private_key(key)?;
        let mut roots = RootCertStore::empty();
        for root in certificates(ca)? {
            roots.add(root)?;
        }
        let roots = Arc::new(roots);
        if let Some(own) = chain.first() {
            names(own, id).map_err(|reason| {
                TlsError::Identity(format!("{}: {reason}", certificate.display()))
            })?;
        }
        let provider = Arc::new(ring::default_provider());
        let servers =
            WebPkiServerVerifier::builder_with_provider(Arc::clone(&roots), Arc::clone(&provider))
                .build()
                .map_err(|error| TlsError::Identity(error.to_string()))?;
        let clients = WebPkiClientVerifier::builder_with_provider(roots, Arc::clone(&provider))
            .build()
            .map_err(|error| TlsError::Identity(error.to_string()))?;
        let mut server = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_protocol_versions(&[&TLS13])?
            .with_client_cert_verifier(Arc::clone(&clients))
            .with_single_cert(chain.clone(), key.clone_key())?;
        server.session_storage = Arc::new(NoServerSessionStorage {});
        server.send_tls13_tickets = 0;
        server.max_early_data_size = 0;
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&TLS13])?
            .with_webpki_verifier(Arc::clone(&servers))
            .with_client_auth_cert(chain, key)?;
        client.resumption = Resumption::disabled();
        client.enable_early_data = false;
        Ok(LoadedNode(Arc::new(Installed {
            id,
            acceptor: TlsAcceptor::from(Arc::new(server)),
            connector: TlsConnector::from(Arc::new(client)),
            servers,
            clients,
        })))
    }

    /// From now on: new handshakes use `loaded`, and every open link is
    /// told to check its peer again.
    pub fn install(&self, LoadedNode(loaded): LoadedNode) {
        self.current.send_replace(loaded);
    }

    /// The node these files are for.
    pub fn id(&self) -> u8 {
        self.current.borrow().id
    }

    /// Changes after now, for an open link to check its peer again.
    pub fn changes(&self) -> watch::Receiver<Arc<Installed>> {
        self.current.subscribe()
    }

    /// The handshake to node `id`, which must prove that name.
    pub(crate) async fn dial(
        &self,
        stream: TcpStream,
        id: u8,
    ) -> Result<(TlsStream<TcpStream>, Peer), TlsError> {
        let connector = self.current.borrow().connector.clone();
        let name = ServerName::try_from(node_name(id))
            .map_err(|error| TlsError::Identity(error.to_string()))?;
        let stream = connector
            .connect(name, stream)
            .await
            .map_err(|error| TlsError::Identity(error.to_string()))?;
        let peer = Peer {
            side: Side::Dialled,
            chain: chain_of(stream.get_ref().1.peer_certificates()),
        };
        self.check(&peer, id)?;
        Ok((TlsStream::Client(stream), peer))
    }

    /// The handshake with a peer that dialled this node. Which node it is
    /// comes with its HELLO, checked by [`NodeTls::check`] before HELLO has
    /// any effect.
    pub(crate) async fn accept(
        &self,
        stream: TcpStream,
    ) -> Result<(TlsStream<TcpStream>, Peer), TlsError> {
        let acceptor = self.current.borrow().acceptor.clone();
        let stream = acceptor
            .accept(stream)
            .await
            .map_err(|error| TlsError::Identity(error.to_string()))?;
        let peer = Peer {
            side: Side::Accepted,
            chain: chain_of(stream.get_ref().1.peer_certificates()),
        };
        Ok((TlsStream::Server(stream), peer))
    }

    /// Whether `peer` is node `id` under what is installed now: its chain
    /// leads to the cluster CA and is valid at this moment, and its
    /// certificate names `id` and no other node. A chain that cannot be
    /// checked fails.
    pub fn check(&self, peer: &Peer, id: u8) -> Result<(), TlsError> {
        let installed = Arc::clone(&self.current.borrow());
        let Some((end, intermediates)) = peer.chain.split_first() else {
            return Err(TlsError::Identity(
                "the peer presented no certificate".into(),
            ));
        };
        let now = UnixTime::now();
        match peer.side {
            Side::Dialled => {
                let name = ServerName::try_from(node_name(id))
                    .map_err(|error| TlsError::Identity(error.to_string()))?;
                installed
                    .servers
                    .verify_server_cert(end, intermediates, &name, &[], now)
                    .map(|_| ())
            }
            Side::Accepted => installed
                .clients
                .verify_client_cert(end, intermediates, now)
                .map(|_| ()),
        }
        .map_err(|error| TlsError::Identity(error.to_string()))?;
        names(end, id).map_err(TlsError::Identity)
    }
}

fn chain_of(certificates: Option<&[CertificateDer<'_>]>) -> Vec<CertificateDer<'static>> {
    certificates
        .unwrap_or_default()
        .iter()
        .map(|c| c.clone().into_owned())
        .collect()
}

/// D-171 A: of the certificate's dNSNames, those under `node.straum`
/// (compared lowercased, wildcards and ids such as `01` included) must be
/// exactly one, and it must be `id`'s. The Subject CN is not read.
fn names(certificate: &CertificateDer<'_>, id: u8) -> Result<(), String> {
    let parsed = webpki::EndEntityCert::try_from(certificate)
        .map_err(|error| format!("not a certificate: {error}"))?;
    let under: Vec<String> = parsed
        .valid_dns_names()
        .map(str::to_ascii_lowercase)
        .filter(|name| name.ends_with(DOMAIN))
        .collect();
    let expected = node_name(id);
    match under.as_slice() {
        [one] if *one == expected => Ok(()),
        [one] => Err(format!("names {one}, not {expected}")),
        [] => Err(format!("names no node; {expected} expected")),
        more => Err(format!(
            "names {} nodes ({}); exactly one, {expected}, expected",
            more.len(),
            more.join(", ")
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn certificate(names: &[&str]) -> CertificateDer<'static> {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(
            names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>(),
        )
        .unwrap();
        params.self_signed(&key).unwrap().der().clone()
    }

    /// D-171 A: exactly one name under `node.straum`, the expected one.
    #[test]
    fn a_certificate_names_its_node_exactly_once() {
        assert_eq!(names(&certificate(&["2.node.straum"]), 2), Ok(()));
        assert_eq!(
            names(&certificate(&["2.NODE.Straum", "host.example"]), 2),
            Ok(())
        );
        for refused in [
            &["1.node.straum"][..],
            &["*.node.straum"],
            &["02.node.straum"],
            &["2.node.straum", "1.node.straum"],
            &["2.node.straum", "*.node.straum"],
            &["2.node.straum", "2.NODE.STRAUM"],
            &["x.2.node.straum"],
            &["host.example"],
        ] {
            assert!(
                names(&certificate(refused), 2).is_err(),
                "{refused:?} was taken as node 2"
            );
        }
    }

    /// The Subject CN does not name a node.
    #[test]
    fn the_common_name_is_not_read() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "2.node.straum");
        let certificate = params.self_signed(&key).unwrap().der().clone();
        assert!(names(&certificate, 2).is_err());
    }
}
