//! Server-side TLS.
//!
//! The certificate is read once at startup: a broker that cannot load its own
//! key must not come up listening in plaintext instead.

use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};

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
}

/// A loaded certificate and key, ready to wrap accepted connections.
#[derive(Clone)]
pub struct Tls {
    acceptor: TlsAcceptor,
}

impl std::fmt::Debug for Tls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Tls")
    }
}

impl Tls {
    /// Loads a PEM certificate chain and its private key.
    pub fn from_pem_files(certificate: &Path, key: &Path) -> Result<Self, TlsError> {
        let chain = certificates(certificate)?;
        let key = private_key(key)?;

        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(chain, key)?;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    pub(crate) fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
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

fn private_key(path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
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
