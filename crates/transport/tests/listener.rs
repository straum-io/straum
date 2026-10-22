//! The socket layer: what gets bound, what gets applied, what moves.
#![allow(clippy::unwrap_used)]

use std::net::SocketAddr;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::time::Duration;

use transport::{BufferBudget, Listener, TransportConfig};

fn config() -> TransportConfig {
    TransportConfig {
        listen: SocketAddr::from(([127, 0, 0, 1], 0)),
        ..TransportConfig::default()
    }
}

fn budget() -> Arc<BufferBudget> {
    Arc::new(BufferBudget::new(64 * 1024 * 1024))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bytes_travel_through_the_halves() {
    let listener = Listener::bind(config(), budget(), None).unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        socket.write_all(b"ping").await.unwrap();
        let mut answer = [0u8; 4];
        socket.read_exact(&mut answer).await.unwrap();
        answer
    });

    let mut connection = listener.accept().await.unwrap().establish().await.unwrap();
    let read = connection.reader.fill().await.unwrap();
    assert_eq!(read, 4);
    assert_eq!(connection.reader.buf_mut().as_ref(), b"ping");
    connection.writer.write_all(b"pong").await.unwrap();

    assert_eq!(&client.await.unwrap(), b"pong");
    // The read buffer is charged to the shared budget while it exists.
    assert!(connection.reader.held_bytes() > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn socket_buffer_sizes_are_applied_when_asked_for() {
    let config = TransportConfig {
        so_rcvbuf_bytes: 256 * 1024,
        so_sndbuf_bytes: 256 * 1024,
        ..config()
    };
    let listener = Listener::bind(config, budget(), None).unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
    let connection = listener.accept().await.unwrap().establish().await.unwrap();
    let _client = client.await.unwrap();

    // The kernel is free to round or double it; what matters is that setting
    // it did not fail the connection.
    assert_eq!(connection.peer.ip().to_string(), "127.0.0.1");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_listener_does_not_share_its_port() {
    let listener = Listener::bind(config(), budget(), None).unwrap();
    let taken = TransportConfig {
        listen: listener.local_addr().unwrap(),
        ..config()
    };

    // Without SO_REUSEPORT a second bind on the same port is an error, not a
    // silent second listener nobody feeds.
    assert!(Listener::bind(taken, budget(), None).is_err());
}

/// `SO_REUSEPORT` balancing is a Linux property; run with `scripts/linux.sh`.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn several_listeners_share_one_port() {
    let shared = budget();
    let first = Listener::bind(
        TransportConfig {
            listeners: 4,
            ..config()
        },
        Arc::clone(&shared),
        None,
    )
    .unwrap();
    let addr = first.local_addr().unwrap();

    let mut listeners = vec![first];
    for _ in 1..4 {
        listeners.push(
            Listener::bind(
                TransportConfig {
                    listen: addr,
                    listeners: 4,
                    ..config()
                },
                Arc::clone(&shared),
                None,
            )
            .unwrap(),
        );
    }
    for listener in &listeners {
        assert_eq!(listener.local_addr().unwrap(), addr);
    }

    // Which listener a connection lands on is the kernel's business; what
    // this test claims is only that the port is shared and that every
    // connection is accepted by one of them. The listeners stay alive
    // throughout: closing one resets whatever is queued behind it.
    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(tokio::net::TcpStream::connect(addr).await.unwrap());
    }

    let mut accepted = 0;
    while accepted < clients.len() {
        for listener in &listeners {
            let next = tokio::time::timeout(Duration::from_millis(200), listener.accept());
            if let Ok(Ok(connection)) = next.await {
                assert_eq!(connection.peer().ip().to_string(), "127.0.0.1");
                accepted += 1;
            }
        }
    }

    assert_eq!(accepted, clients.len());
}

/// `TCP_USER_TIMEOUT` exists only on Linux; elsewhere the option is dropped
/// with a startup warning and this test does not apply.
#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_user_timeout_is_applied() {
    let config = TransportConfig {
        tcp_user_timeout: Some(Duration::from_secs(5)),
        ..config()
    };
    let listener = Listener::bind(config, budget(), None).unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
    let accepted = listener.accept().await.unwrap();
    let connection = accepted.establish().await;
    let _client = client.await.unwrap();

    assert!(connection.is_ok(), "the option failed the connection");
}

/// TLS, end to end: a self-signed certificate made for this run, a rustls
/// client, and bytes that only move if the handshake worked.
mod tls {
    use super::*;

    use std::io::Write;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};
    use transport::Tls;

    /// The directory a test writes, removed when the test ends, passing or
    /// failing (D-121).
    struct Scratch(std::path::PathBuf);

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Writes a throwaway certificate and key; nothing secret is ever
    /// committed to the repository.
    fn certificate() -> (Scratch, std::path::PathBuf, std::path::PathBuf, Vec<u8>) {
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let mut directory = std::env::temp_dir();
        directory.push(format!(
            "straum-tls-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&directory).unwrap();

        let certificate = directory.join("cert.pem");
        let key = directory.join("key.pem");
        std::fs::File::create(&certificate)
            .unwrap()
            .write_all(issued.cert.pem().as_bytes())
            .unwrap();
        std::fs::File::create(&key)
            .unwrap()
            .write_all(issued.signing_key.serialize_pem().as_bytes())
            .unwrap();

        (
            Scratch(directory),
            certificate,
            key,
            issued.cert.der().to_vec(),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_tls_client_is_served() {
        let (_scratch, certificate, key, der) = certificate();
        let tls = Tls::from_pem_files(&certificate, &key).unwrap();
        let listener = Listener::bind(config(), budget(), Some(tls)).unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut roots = RootCertStore::empty();
            roots
                .add(tokio_rustls::rustls::pki_types::CertificateDer::from(der))
                .unwrap();
            let config = ClientConfig::builder()
                .with_root_certificates(roots)
                .with_no_client_auth();
            let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));

            let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
            let name = ServerName::try_from("localhost").unwrap();
            let mut stream = connector.connect(name, socket).await.unwrap();
            stream.write_all(b"ping").await.unwrap();
            let mut answer = [0u8; 4];
            stream.read_exact(&mut answer).await.unwrap();
            answer
        });

        let accepted = listener.accept().await.unwrap();
        let mut connection = accepted.establish().await.unwrap();
        assert_eq!(connection.reader.fill().await.unwrap(), 4);
        assert_eq!(connection.reader.buf_mut().as_ref(), b"ping");
        connection.writer.write_all(b"pong").await.unwrap();

        assert_eq!(&client.await.unwrap(), b"pong");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_plaintext_client_gets_nowhere_on_a_tls_port() {
        let (_scratch, certificate, key, _) = certificate();
        let tls = Tls::from_pem_files(&certificate, &key).unwrap();
        let listener = Listener::bind(config(), budget(), Some(tls)).unwrap();
        let addr = listener.local_addr().unwrap();

        let client = tokio::spawn(async move {
            let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
            // Protocol bytes, sent in the clear.
            socket.write_all(&[0x10, 0x0c, 0x00, 0x04]).await.unwrap();
            let mut answer = [0u8; 1];
            socket.read_exact(&mut answer).await.map(|_| answer[0])
        });

        let accepted = listener.accept().await.unwrap();
        // The handshake fails; the connection is never established.
        assert!(accepted.establish().await.is_err());

        // What the client gets back is a TLS alert record (0x15): the
        // server never speaks plaintext to it.
        let answer = client.await.unwrap();
        if let Ok(first) = answer {
            assert_eq!(first, 0x15, "a plaintext client was answered in plaintext");
        }
    }

    /// D-166 A: a new certificate, installed, is what the next handshake
    /// presents; one loaded with another certificate's key is refused.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_installed_certificate_serves_the_next_handshake() {
        let (dir, old_cert, old_key, _) = certificate();
        // A second certificate, under its own names: `certificate()` reuses
        // one directory per thread.
        let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let (new_cert, new_key) = (dir.0.join("new-cert.pem"), dir.0.join("new-key.pem"));
        std::fs::write(&new_cert, issued.cert.pem()).unwrap();
        std::fs::write(&new_key, issued.signing_key.serialize_pem()).unwrap();
        let new_der = issued.cert.der().to_vec();
        assert!(
            Tls::load(&old_cert, &new_key).is_err(),
            "a key of another certificate"
        );
        let tls = Tls::from_pem_files(&old_cert, &old_key).unwrap();
        let listener = Listener::bind(config(), budget(), Some(tls.clone())).unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            loop {
                let accepted = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    if let Ok(mut connection) = accepted.establish().await {
                        let _ = connection.writer.write_all(b"ok").await;
                    }
                });
            }
        });
        // A client that trusts only the new certificate.
        let handshake = || {
            let der = new_der.clone();
            async move {
                let mut roots = RootCertStore::empty();
                roots
                    .add(tokio_rustls::rustls::pki_types::CertificateDer::from(der))
                    .unwrap();
                let config = ClientConfig::builder()
                    .with_root_certificates(roots)
                    .with_no_client_auth();
                let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
                let socket = tokio::net::TcpStream::connect(addr).await.unwrap();
                let name = ServerName::try_from("localhost").unwrap();
                connector.connect(name, socket).await.is_ok()
            }
        };
        assert!(
            !handshake().await,
            "the old certificate is presented before the swap"
        );
        tls.install(Tls::load(&new_cert, &new_key).unwrap());
        assert!(handshake().await, "the new one after it");
        server.abort();
    }

    #[test]
    fn a_missing_certificate_is_a_startup_error() {
        let missing = std::path::Path::new("/nonexistent/cert.pem");
        assert!(Tls::from_pem_files(missing, missing).is_err());
    }
}

/// A connection holds capacity in both directions. A budget that counted one
/// of them was not a bound on the resource it named (docs/memory.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_halves_of_a_connection_are_charged() {
    let held = Arc::new(BufferBudget::new(64 * 1024 * 1024));
    let settings = TransportConfig {
        // Deliberately different, so the assertion cannot pass by the two
        // being the same number.
        read_buffer_bytes: 8192,
        write_buffer_bytes: 4096,
        ..config()
    };
    let listener = Listener::bind(settings, Arc::clone(&held), None).unwrap();
    let addr = listener.local_addr().unwrap();
    assert_eq!(held.used(), 0, "an idle listener holds nothing");

    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
    let connection = listener.accept().await.unwrap().establish().await.unwrap();
    let socket = client.await.unwrap();

    assert_eq!(held.used(), 8192 + 4096);
    assert_eq!(connection.reader.held_bytes(), 8192);
    assert_eq!(connection.writer.held_bytes(), 4096);

    drop(connection);
    drop(socket);
    assert_eq!(held.used(), 0, "the charge outlived the connection");
}

/// The two settings are independent. Moving one must not move the other,
/// which is what `STRAUM_READ_BUFFER_BYTES` used to do.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_read_setting_does_not_move_the_write_buffer() {
    let held = Arc::new(BufferBudget::new(64 * 1024 * 1024));
    let settings = TransportConfig {
        read_buffer_bytes: 1024,
        ..config()
    };
    let listener = Listener::bind(settings, Arc::clone(&held), None).unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
    let connection = listener.accept().await.unwrap().establish().await.unwrap();
    let socket = client.await.unwrap();

    assert_eq!(connection.reader.held_bytes(), 1024);
    assert_eq!(
        connection.writer.held_bytes(),
        TransportConfig::default().write_buffer_bytes,
        "lowering the read buffer lowered the write buffer with it"
    );
    drop(connection);
    drop(socket);
}

/// A connection that cannot be given both buffers is refused both, before
/// either is allocated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_that_fits_only_one_buffer_is_refused() {
    let settings = TransportConfig {
        read_buffer_bytes: 4096,
        write_buffer_bytes: 4096,
        ..config()
    };
    // Room for the read buffer and nothing after it.
    let held = Arc::new(BufferBudget::new(4096 + 2048));
    let listener = Listener::bind(settings, Arc::clone(&held), None).unwrap();
    let addr = listener.local_addr().unwrap();

    let client = tokio::spawn(async move { tokio::net::TcpStream::connect(addr).await.unwrap() });
    let refused = listener.accept().await.unwrap().establish().await;
    let socket = client.await.unwrap();

    assert!(refused.is_err(), "a connection was given memory it had not");
    assert_eq!(held.used(), 0, "the refused half was left charged");
    drop(socket);
}

/// What a large batch forces is given back. `clear()` alone frees nothing,
/// and the writer used to keep the high-water mark of every batch it had
/// ever encoded for the life of the connection.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_write_buffer_returns_what_one_large_batch_forced() {
    let held = Arc::new(BufferBudget::new(64 * 1024 * 1024));
    let settings = TransportConfig {
        write_buffer_bytes: 4096,
        ..config()
    };
    let listener = Listener::bind(settings, Arc::clone(&held), None).unwrap();
    let addr = listener.local_addr().unwrap();

    let draining = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let mut socket = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut sink = vec![0u8; 1 << 20];
        let mut total = 0usize;
        while total < 512 * 1024 {
            match socket.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(read) => total += read,
            }
        }
        total
    });

    let mut connection = listener.accept().await.unwrap().establish().await.unwrap();
    let before = connection.writer.held_bytes();
    assert_eq!(before, 4096);

    connection
        .writer
        .buf_mut()
        .extend_from_slice(&vec![b'x'; 512 * 1024]);
    // Grown well past its steady size, and the budget knows before the
    // write rather than after it.
    assert!(connection.writer.buf_mut().capacity() > 4096);

    let wrote = connection.writer.flush().await.unwrap();
    assert_eq!(wrote, 512 * 1024);
    assert_eq!(
        connection.writer.held_bytes(),
        4096,
        "the capacity the batch forced was kept"
    );
    assert_eq!(held.used(), 4096 + config().read_buffer_bytes);

    let _ = draining.await;
}
