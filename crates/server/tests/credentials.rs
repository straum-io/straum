//! Credentials end to end (D-110 A, D-158 A): three real `straum-server`
//! processes with a password file and an ACL file, spoken to over MQTT 3.1.1
//! by a minimal hand-written client. One test, its steps in order, so no
//! other test in this file competes for the ports it picks.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const NODES: u16 = 3;

/// A block of ports by process id, as in `cluster.rs` (D-108), offset so
/// the two test binaries' blocks differ for the same id.
fn ports() -> u16 {
    let block = u16::try_from(std::process::id() % 40).unwrap();
    32_000 + block * 10
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
    let mut out = vec![header];
    let mut n = body.len();
    loop {
        let mut byte = u8::try_from(n % 128).unwrap();
        n /= 128;
        if n > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if n == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
    out
}

/// One packet, or `None` on a close or after `within`.
fn read_packet(stream: &mut TcpStream, within: Duration) -> Option<(u8, Vec<u8>)> {
    stream.set_read_timeout(Some(within)).unwrap();
    let mut header = [0u8; 1];
    stream.read_exact(&mut header).ok()?;
    let (mut length, mut shift) = (0usize, 0);
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).ok()?;
        length |= usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).ok()?;
    Some((header[0], body))
}

/// A clean-session CONNECT with these credentials; the stream and the
/// CONNACK's return code, or `None` if the node did not answer.
fn connect(port: u16, user: Option<&str>, password: Option<&str>) -> Option<(TcpStream, u8)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).ok()?;
    let mut flags = 0x02;
    if user.is_some() {
        flags |= 0x80;
    }
    if password.is_some() {
        flags |= 0x40;
    }
    let mut body = string(b"MQTT");
    body.extend_from_slice(&[4, flags, 0, 60]);
    body.extend(string(b"client"));
    for field in [user, password].into_iter().flatten() {
        body.extend(string(field.as_bytes()));
    }
    stream.write_all(&packet(0x10, &body)).ok()?;
    match read_packet(&mut stream, Duration::from_secs(5))? {
        (0x20, ack) if ack.len() == 2 => Some((stream, ack[1])),
        _ => None,
    }
}

/// A SUBSCRIBE of one filter at QoS 1; the SUBACK's return code.
fn subscribe(stream: &mut TcpStream, filter: &str, id: u16) -> u8 {
    let mut body = id.to_be_bytes().to_vec();
    body.extend(string(filter.as_bytes()));
    body.push(1);
    stream.write_all(&packet(0x82, &body)).unwrap();
    match read_packet(stream, Duration::from_secs(10)) {
        Some((0x90, ack)) if ack.len() == 3 => ack[2],
        other => panic!("no SUBACK for {filter}: {other:?}"),
    }
}

fn publish(stream: &mut TcpStream, topic: &str, id: u16) {
    let mut body = string(topic.as_bytes());
    body.extend_from_slice(&id.to_be_bytes());
    body.extend_from_slice(b"m");
    stream.write_all(&packet(0x32, &body)).unwrap();
}

#[test]
fn credentials_decide_who_connects_and_what_each_may_do() {
    let base = ports();
    let dir =
        std::env::temp_dir().join(format!("straum-server-credentials-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
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
        assert!(out.status.success());
        String::from_utf8(out.stdout).unwrap().trim().to_owned()
    };
    let passwords = dir.join("passwords");
    std::fs::write(&passwords, format!("alice:{hash}\n")).unwrap();
    let acl = dir.join("acl");
    std::fs::write(&acl, "user alice\nreadwrite open/#\n").unwrap();
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
                .env(
                    "STRAUM_CLUSTER_MQTT_LISTEN",
                    format!("127.0.0.1:{}", base + 5 + id),
                )
                .env("STRAUM_CLUSTER_DATA_DIR", dir.join(format!("data-{id}")))
                .env("STRAUM_AUTH_PASSWORD_FILE", &passwords)
                .env("STRAUM_AUTH_ACL_FILE", &acl)
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

    // Once the node serves, the right password is let in.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut alice = loop {
        if let Some((stream, 0)) = connect(mqtt, Some("alice"), Some("right")) {
            break stream;
        }
        assert!(
            Instant::now() < deadline,
            "alice never connected: {}",
            log()
        );
        std::thread::sleep(Duration::from_millis(200));
    };

    // 3.1.1 return codes: 5 not authorized, 4 bad user name or password.
    assert_eq!(connect(mqtt, None, None).map(|(_, rc)| rc), Some(5));
    assert_eq!(
        connect(mqtt, Some("alice"), Some("wrong")).map(|(_, rc)| rc),
        Some(4)
    );
    assert_eq!(
        connect(mqtt, Some("mallory"), Some("right")).map(|(_, rc)| rc),
        Some(4)
    );

    // Subscribing outside her rules is refused (0x80); inside, granted QoS 1.
    assert_eq!(subscribe(&mut alice, "secret/#", 1), 0x80);
    assert_eq!(subscribe(&mut alice, "open/#", 2), 1);

    // A publish inside her rules is acknowledged and delivered back.
    publish(&mut alice, "open/a", 3);
    let mut acked = false;
    let mut delivered = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while !(acked && delivered) && Instant::now() < deadline {
        match read_packet(&mut alice, Duration::from_secs(10)) {
            Some((0x40, ack)) if ack == 3u16.to_be_bytes() => acked = true,
            Some((header, _)) if header & 0xf0 == 0x30 => {
                delivered = true;
            }
            Some(_) => {}
            None => break,
        }
    }
    assert!(acked && delivered, "acked {acked}, delivered {delivered}");

    // Outside them, 3.1.1 has no refusal to send: the connection closes.
    publish(&mut alice, "secret/a", 4);
    let deadline = Instant::now() + Duration::from_secs(10);
    let closed = loop {
        match read_packet(&mut alice, Duration::from_secs(10)) {
            // A delivery or acknowledgement still in flight.
            Some((header, _)) if header & 0xf0 == 0x30 || header == 0x40 => {}
            Some(_) => break false,
            None => break true,
        }
        if Instant::now() >= deadline {
            break false;
        }
    };
    assert!(closed, "the connection stayed open after a refused publish");
}
