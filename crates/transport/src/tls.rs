//! Server-side TLS.
//!
//! The certificate is read once at startup: a broker that cannot load its own
//! key must not come up listening in plaintext instead. It can be replaced
//! later (D-166 A): every clone of a [`Tls`] presents the same certificate,
//! and [`Tls::install`] swaps it for new handshakes, leaving open connections
//! as they are.

use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, RwLock};

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::{ClientHello, ResolvesServerCert};
use tokio_rustls::rustls::sign::CertifiedKey;

#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    #[error("reading {path}: {source}")]
    Read {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{0} holds no certificate")]
    NoCertificate(String),
    #[error("{0} holds no private key")]
    NoPrivateKey(String),
    #[error("{0}")]
    Rustls(#[from] tokio_rustls::rustls::Error),
    /// A node certificate or a peer that does not prove the identity
    /// expected (ADR-0010).
    #[error("node identity: {0}")]
    Identity(String),
}

/// The certificate in use, shared by every clone of a [`Tls`].
#[derive(Debug)]
struct Current(RwLock<Arc<CertifiedKey>>);

impl ResolvesServerCert for Current {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        self.0.read().ok().map(|key| Arc::clone(&key))
    }
}

/// A certificate chain and its key, read and checked (the key matches the
/// certificate), not yet in use.
pub struct Loaded(Arc<CertifiedKey>);

impl std::fmt::Debug for Loaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Loaded")
    }
}

/// A loaded certificate and key, ready to wrap accepted connections.
#[derive(Clone)]
pub struct Tls {
    acceptor: TlsAcceptor,
    current: Arc<Current>,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Tls")
    }
}

impl Tls {
    /// Loads a PEM certificate chain and its private key.
    pub fn from_pem_files(certificate: &Path, key: &Path) -> Result<Self, TlsError> {
        let Loaded(loaded) = Self::load(certificate, key)?;
        let current = Arc::new(Current(RwLock::new(loaded)));
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(&current) as Arc<dyn ResolvesServerCert>);
        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
            current,
        })
    }

    /// Reads and checks a certificate chain and key without using them: a
    /// reload loads every file before it installs anything (D-166 A).
    pub fn load(certificate: &Path, key: &Path) -> Result<Loaded, TlsError> {
        let chain = certificates(certificate)?;
        let key = private_key(key)?;
        let provider = tokio_rustls::rustls::crypto::ring::default_provider();
        // `from_der` refuses a key that does not belong to the certificate.
        Ok(Loaded(Arc::new(CertifiedKey::from_der(
            chain, key, &provider,
        )?)))
    }

    /// From the next handshake on, presents `loaded`.
    pub fn install(&self, Loaded(loaded): Loaded) {
        if let Ok(mut current) = self.current.0.write() {
            *current = loaded;
        }
    }

    pub(crate) fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }
}

pub(crate) fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let file = std::fs::File::open(path).map_err(|source| TlsError::Read {
        path: path.display().to_string(),
        source,
    })?;
    let chain: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut BufReader::new(file))
        .filter_map(Result::ok)
        .collect();

    if chain.is_empty() {
        return Err(TlsError::NoCertificate(path.display().to_string()));
    }
    Ok(chain)
}

pub(crate) fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    let file = std::fs::File::open(path).map_err(|source| TlsError::Read {
        path: path.display().to_string(),
        source,
    })?;
    rustls_pemfile::private_key(&mut BufReader::new(file))
        .map_err(|source| TlsError::Read {
            path: path.display().to_string(),
            source,
        })?
        .ok_or_else(|| TlsError::NoPrivateKey(path.display().to_string()))
}
