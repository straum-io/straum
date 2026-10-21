//! Client TLS end to end (D-164 A): three real `straum-server` processes
//! whose MQTT listeners bind beyond loopback, secured by TLS and a password
//! file with no anonymous clients, and no unauthenticated-network switch.
//! One test, its steps in order, so no other test in this file competes for
//! the ports it picks.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, ClientConnection, RootCertStore, StreamOwned};

const NODES: u16 = 3;

/// A block of ports by process id, apart from `cluster.rs`'s and
/// `credentials.rs`'s (D-108).
fn ports() -> u16 {
    let block = u16::try_from(std::process::id() % 40).unwrap();
    33_000 + block * 10
}

struct Servers {
    dir: PathBuf,
    children: Vec<Child>,
}

impl Drop for Servers {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn string(s: &[u8]) -> Vec<u8> {
    let mut out = u16::try_from(s.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(s);
    out
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header, u8::try_from(body.len()).unwrap()];
    out.extend_from_slice(body);
    out
}

/// One packet of under 128 bytes, or `None` on a close or an error.
fn read_packet(stream: &mut impl Read) -> Option<(u8, Vec<u8>)> {
    let mut head = [0u8; 2];
    stream.read_exact(&mut head).ok()?;
    let mut body = vec![0u8; usize::from(head[1])];
    stream.read_exact(&mut body).ok()?;
    Some((head[0], body))
}

fn connect_packet(user: &str, password: &str) -> Vec<u8> {
    let mut body = string(b"MQTT");
    body.extend_from_slice(&[4, 0xc2, 0, 60]);
    body.extend(string(b"client"));
    body.extend(string(user.as_bytes()));
    body.extend(string(password.as_bytes()));
    packet(0x10, &body)
}

type Tls = StreamOwned<ClientConnection, TcpStream>;

fn tls(port: u16, roots: &Arc<ClientConfig>) -> Option<Tls> {
    let socket = TcpStream::connect(("127.0.0.1", port)).ok()?;
    socket.set_read_timeout(Some(Duration::from_secs(5))).ok()?;
    let name = ServerName::try_from("localhost").ok()?;
    let connection = ClientConnection::new(Arc::clone(roots), name).ok()?;
    Some(StreamOwned::new(connection, socket))
}

/// A CONNECT over TLS; the CONNACK's return code.
fn connack(port: u16, roots: &Arc<ClientConfig>, password: &str) -> Option<(Tls, u8)> {
    let mut stream = tls(port, roots)?;
    stream.write_all(&connect_packet("alice", password)).ok()?;
    match read_packet(&mut stream)? {
        (0x20, ack) if ack.len() == 2 => Some((stream, ack[1])),
        _ => None,
    }
}

#[test]
fn an_mqtt_listener_secured_by_tls_and_credentials_serves_beyond_loopback() {
    let base = ports();
    let dir = std::env::temp_dir().join(format!("straum-server-tls-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let issued = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let (cert, key) = (dir.join("cert.pem"), dir.join("key.pem"));
    std::fs::write(&cert, issued.cert.pem()).unwrap();
    std::fs::write(&key, issued.signing_key.serialize_pem()).unwrap();
    let mut roots = RootCertStore::empty();
    roots
        .add(CertificateDer::from(issued.cert.der().to_vec()))
        .unwrap();
    let client = Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    );

    let hash = {
        let mut child = Command::new(env!("CARGO_BIN_EXE_straum-server"))
            .arg("hash-password")
            .env_clear()
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(b"right\n").unwrap();
        let out = child.wait_with_output().unwrap();
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    let passwords = dir.join("passwords");
    std::fs::write(&passwords, format!("alice:{hash}\n")).unwrap();

    let peers = (0..NODES)
        .map(|id| format!("{id}=127.0.0.1:{}", base + id))
        .collect::<Vec<_>>()
        .join(",");
    let mut servers = Servers {
        dir: dir.clone(),
        children: Vec::new(),
    };
    for id in 0..NODES {
        let log = std::fs::File::create(dir.join(format!("log-{id}"))).unwrap();
        servers.children.push(
            Command::new(env!("CARGO_BIN_EXE_straum-server"))
                .env_clear()
                .env("STRAUM_EXPERIMENTAL_CLUSTER", "1")
                .env("STRAUM_CLUSTER_NODE_ID", id.to_string())
                .env("STRAUM_CLUSTER_PEERS", &peers)
                // Beyond loopback, with no STRAUM_CLUSTER_UNAUTHENTICATED_NETWORK.
                .env(
                    "STRAUM_CLUSTER_MQTT_LISTEN",
                    format!("0.0.0.0:{}", base + 5 + id),
                )
                .env("STRAUM_CLUSTER_DATA_DIR", dir.join(format!("data-{id}")))
                .env("STRAUM_AUTH_PASSWORD_FILE", &passwords)
                .env("STRAUM_TLS_CERT_PATH", &cert)
                .env("STRAUM_TLS_KEY_PATH", &key)
                .env("NO_COLOR", "1")
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }
    let mqtt = base + 5;
    let log = || std::fs::read_to_string(dir.join("log-0")).unwrap_or_default();

    let deadline = Instant::now() + Duration::from_secs(30);
    let mut alice = loop {
        if let Some((stream, 0)) = connack(mqtt, &client, "right") {
            break stream;
        }
        assert!(Instant::now() < deadline, "no TLS CONNACK: {}", log());
        std::thread::sleep(Duration::from_millis(200));
    };

    // A QoS 1 publish over TLS is acknowledged once committed.
    let mut body = string(b"open/a");
    body.extend_from_slice(&7u16.to_be_bytes());
    body.extend_from_slice(b"m");
    alice.write_all(&packet(0x32, &body)).unwrap();
    assert_eq!(
        read_packet(&mut alice),
        Some((0x40, 7u16.to_be_bytes().to_vec()))
    );

    // Over TLS, a wrong password is still refused.
    assert_eq!(connack(mqtt, &client, "wrong").map(|(_, rc)| rc), Some(4));

    // A plaintext CONNECT on the TLS port gets no CONNACK.
    let mut plain = TcpStream::connect(("127.0.0.1", mqtt)).unwrap();
    plain
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    plain.write_all(&connect_packet("alice", "right")).unwrap();
    let mut answer = [0u8; 4];
    let read = plain.read(&mut answer).unwrap_or(0);
    assert!(
        read == 0 || answer[0] != 0x20,
        "a plaintext client got an MQTT answer: {:?}",
        &answer[..read]
    );

    // A socket that never starts its handshake is closed once the connect
    // timeout passes (10 s here): it holds no slot for good.
    let started = Instant::now();
    let mut idle = TcpStream::connect(("127.0.0.1", mqtt)).unwrap();
    idle.set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    let mut byte = [0u8; 1];
    let read = idle.read(&mut byte);
    let waited = started.elapsed();
    assert!(
        matches!(read, Ok(0))
            && waited >= Duration::from_secs(9)
            && waited < Duration::from_secs(19),
        "an idle handshake: {read:?} after {waited:?}"
    );
}
