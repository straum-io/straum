//! The experimental cluster path end to end (ADR-0008 step 7a, D-106 A):
//! three real `straum-server` processes on loopback, spoken to over MQTT
//! 3.1.1 by a minimal hand-written client. One test, its steps in order, so
//! no other test in this file competes for the ports it picks. Unix only:
//! signals drive the handoff and the stops.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const NODES: u8 = 3;

/// A port of this test binary's own, as in the node tests (D-108): a block
/// below the ephemeral range chosen by the process id, numbered by a
/// counter. Nothing retries a port that turns out taken.
fn free_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    let block = u16::try_from(std::process::id() % 120).unwrap();
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert!(n < 100, "this test binary used up its block of ports");
    20_000 + block * 100 + n
}

/// One node's process and where it logs.
struct Server {
    child: Child,
    log: PathBuf,
}

struct Cluster {
    dir: PathBuf,
    links: Vec<u16>,
    mqtt: Vec<u16>,
    servers: Vec<Option<Server>>,
}

impl Cluster {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("straum-server-cluster-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let links = (0..NODES).map(|_| free_port()).collect();
        let mqtt = (0..NODES).map(|_| free_port()).collect();
        Self {
            dir,
            links,
            mqtt,
            servers: (0..NODES).map(|_| None).collect(),
        }
    }

    fn peers(&self) -> String {
        self.links
            .iter()
            .enumerate()
            .map(|(id, port)| format!("{id}=127.0.0.1:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Starts node `id` over `data` (its usual directory unless given),
    /// with any extra variables. Nothing of this process's environment
    /// leaks in: every setting is named here.
    fn spawn(&mut self, id: u8, data: Option<&Path>, extra: &[(&str, &str)]) -> Server {
        let data = data.map_or_else(|| self.dir.join(format!("data-{id}")), Path::to_path_buf);
        let log = self.dir.join(format!("log-{id}-{}", self.lives(id)));
        let mut command = Command::new(env!("CARGO_BIN_EXE_straum-server"));
        command
            .env_clear()
            .env("STRAUM_EXPERIMENTAL_CLUSTER", "1")
            .env("STRAUM_CLUSTER_NODE_ID", id.to_string())
            .env("STRAUM_CLUSTER_PEERS", self.peers())
            .env(
                "STRAUM_CLUSTER_MQTT_LISTEN",
                format!("127.0.0.1:{}", self.mqtt[usize::from(id)]),
            )
            .env("STRAUM_CLUSTER_DATA_DIR", &data)
            .env("STRAUM_LOG_LEVEL", "info")
            .env("NO_COLOR", "1");
        for (name, value) in extra {
            command.env(name, value);
        }
        let file = std::fs::File::create(&log).unwrap();
        let child = command
            .stdin(Stdio::null())
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .unwrap();
        Server { child, log }
    }

    fn lives(&self, id: u8) -> usize {
        std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .filter(|e| {
                        e.as_ref().is_ok_and(|e| {
                            e.file_name()
                                .to_string_lossy()
                                .starts_with(&format!("log-{id}-"))
                        })
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    fn start(&mut self, id: u8) {
        let server = self.spawn(id, None, &[]);
        self.servers[usize::from(id)] = Some(server);
    }

    /// SIGTERM, and the process ends cleanly.
    fn stop(&mut self, id: u8) {
        let mut server = self.servers[usize::from(id)].take().unwrap();
        signal(&server.child, "-TERM");
        let status = wait(&mut server.child, Duration::from_secs(20));
        assert!(
            status.success(),
            "node {id} ended with {status}: {}",
            log(&server.log)
        );
    }

    fn signal(&self, id: u8, sig: &str) {
        signal(&self.servers[usize::from(id)].as_ref().unwrap().child, sig);
    }

    fn log(&self, id: u8) -> String {
        log(&self.servers[usize::from(id)].as_ref().unwrap().log)
    }

    fn mqtt(&self, id: u8) -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], self.mqtt[usize::from(id)]))
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for server in self.servers.iter_mut().flatten() {
            let _ = server.child.kill();
            let _ = server.child.wait();
        }
    }
}

fn signal(child: &Child, sig: &str) {
    let status = Command::new("kill")
        .arg(sig)
        .arg(child.id().to_string())
        .status()
        .unwrap();
    assert!(status.success(), "kill {sig} failed");
}

fn wait(child: &mut Child, within: Duration) -> std::process::ExitStatus {
    let deadline = Instant::now() + within;
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        assert!(Instant::now() < deadline, "a node did not end");
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Until `what` appears in a log, or the deadline.
fn wait_for_log(cluster: &Cluster, id: u8, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !cluster.log(id).contains(what) {
        assert!(
            Instant::now() < deadline,
            "node {id} never logged {what:?}: {}",
            cluster.log(id)
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// A minimal MQTT 3.1.1 client: what this test sends and reads, nothing more.

fn remaining(mut n: usize) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let byte = (n % 128) as u8;
        n /= 128;
        out.push(if n > 0 { byte | 0x80 } else { byte });
        if n == 0 {
            return out;
        }
    }
}

fn string(s: &[u8]) -> Vec<u8> {
    let mut out = u16::try_from(s.len()).unwrap().to_be_bytes().to_vec();
    out.extend_from_slice(s);
    out
}

fn packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![header];
    out.extend(remaining(body.len()));
    out.extend_from_slice(body);
    out
}

/// One whole packet, or `None` at the deadline or the end of the stream.
fn read_packet(stream: &mut TcpStream, within: Duration) -> Option<(u8, Vec<u8>)> {
    stream.set_read_timeout(Some(within)).unwrap();
    let mut header = [0u8; 1];
    stream.read_exact(&mut header).ok()?;
    let (mut length, mut shift) = (0usize, 0);
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).ok()?;
        length |= usize::from(byte[0] & 0x7f) << shift;
        shift += 7;
        if byte[0] & 0x80 == 0 {
            break;
        }
    }
    let mut body = vec![0u8; length];
    stream.read_exact(&mut body).ok()?;
    Some((header[0], body))
}

/// CONNECT and its CONNACK's return code; `None` if the node did not answer.
fn connect_with(
    address: SocketAddr,
    client: &str,
    flags: u8,
    will: Option<(&str, &str)>,
) -> Option<(TcpStream, u8)> {
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).ok()?;
    let mut body = string(b"MQTT");
    body.extend_from_slice(&[4, flags, 0, 60]);
    body.extend(string(client.as_bytes()));
    if let Some((topic, message)) = will {
        body.extend(string(topic.as_bytes()));
        body.extend(string(message.as_bytes()));
    }
    stream.write_all(&packet(0x10, &body)).ok()?;
    match read_packet(&mut stream, Duration::from_secs(5))? {
        (0x20, ack) if ack.len() == 2 => Some((stream, ack[1])),
        _ => None,
    }
}

/// A clean session, once the node takes clients (it refuses them until its
/// gateway's reset is committed).
fn connect(address: SocketAddr, client: &str) -> TcpStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some((stream, 0)) = connect_with(address, client, 0x02, None) {
            return stream;
        }
        assert!(
            Instant::now() < deadline,
            "{client}: never connected to {address}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn subscribe(stream: &mut TcpStream, topic: &str) {
    let mut body = 1u16.to_be_bytes().to_vec();
    body.extend(string(topic.as_bytes()));
    body.push(1);
    stream.write_all(&packet(0x82, &body)).unwrap();
    let (kind, ack) = read_packet(stream, Duration::from_secs(10)).expect("no SUBACK");
    assert_eq!((kind, ack.last().copied()), (0x90, Some(1)), "SUBACK");
}

fn publish(stream: &mut TcpStream, topic: &str, payload: &str, id: u16) {
    let mut body = string(topic.as_bytes());
    body.extend_from_slice(&id.to_be_bytes());
    body.extend_from_slice(payload.as_bytes());
    stream.write_all(&packet(0x32, &body)).unwrap();
}

/// Whether the PUBACK for `id` comes within `within`.
fn acknowledged(stream: &mut TcpStream, id: u16, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        match read_packet(stream, left.max(Duration::from_millis(1))) {
            Some((0x40, ack)) if ack == id.to_be_bytes() => return true,
            Some(_) => {}
            None => return false,
        }
    }
    false
}

/// Whether a PUBLISH carrying `payload` arrives within `within`; each
/// QoS 1 delivery is acknowledged.
fn delivered(stream: &mut TcpStream, payload: &str, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while let Some(left) = deadline.checked_duration_since(Instant::now()) {
        let Some((kind, body)) = read_packet(stream, left.max(Duration::from_millis(1))) else {
            return false;
        };
        if kind >> 4 != 3 {
            continue;
        }
        let topic = usize::from(u16::from_be_bytes([body[0], body[1]]));
        let mut at = 2 + topic;
        if kind & 0x06 != 0 {
            let id = [body[at], body[at + 1]];
            at += 2;
            stream.write_all(&packet(0x40, &id)).unwrap();
        }
        if body[at..] == *payload.as_bytes() {
            return true;
        }
    }
    false
}

/// As an at-least-once client does: publish, and if no PUBACK comes, connect
/// again and resend. A proposal made while the slot changes hands is dropped,
/// not held (ADR-0008, "Not yet in step 4"); the client's resend is what
/// delivers it. Returns the attempt that was acknowledged.
fn publish_until_acknowledged(address: SocketAddr, client: &str, payload: &str, id: u16) -> u32 {
    for attempt in 1..=10 {
        let mut stream = connect(address, client);
        publish(&mut stream, "t/x", payload, id);
        if acknowledged(&mut stream, id, Duration::from_secs(3)) {
            return attempt;
        }
    }
    panic!("{payload}: never acknowledged in ten attempts");
}

#[test]
fn three_server_processes_acknowledge_only_committed_publishes_and_hand_over() {
    let mut cluster = Cluster::new();
    for id in 0..NODES {
        cluster.start(id);
    }
    let long = Duration::from_secs(15);

    // A QoS 1 publish on node 2, acknowledged and delivered on node 0.
    let mut on0 = connect(cluster.mqtt(0), "sub-0");
    subscribe(&mut on0, "t/x");
    let mut pub2 = connect(cluster.mqtt(2), "pub-2");
    publish(&mut pub2, "t/x", "one", 1);
    assert!(
        acknowledged(&mut pub2, 1, long),
        "no PUBACK for the first publish"
    );
    assert!(
        delivered(&mut on0, "one", long),
        "the first publish was not delivered"
    );

    // What the path cannot keep yet is refused at CONNECT (code 3).
    assert_eq!(
        connect_with(cluster.mqtt(1), "will", 0x06, Some(("w/t", "bye"))).map(|(_, code)| code),
        Some(3),
        "a will was accepted"
    );
    assert_eq!(
        connect_with(cluster.mqtt(1), "keep", 0x00, None).map(|(_, code)| code),
        Some(3),
        "a persistent session was accepted"
    );

    // PUBACK waits for the commit. Node 0 sequences the first slot; with
    // its two replicas stopped it can append but not commit — two of three
    // logs are needed — so a publish on it is neither acknowledged nor
    // delivered. The sequencer is alive throughout: what is missing is the
    // majority, nothing else.
    cluster.stop(1);
    cluster.stop(2);
    let mut pub0 = connect(cluster.mqtt(0), "pub-0");
    publish(&mut pub0, "t/x", "three", 3);
    assert!(
        !acknowledged(&mut pub0, 3, Duration::from_secs(3)),
        "acknowledged without a majority"
    );
    assert!(
        !delivered(&mut on0, "three", Duration::from_secs(1)),
        "delivered without a majority"
    );
    // A replica restarted over its own data brings the majority back: the
    // same publish, on the same connection, commits and is acknowledged.
    cluster.start(1);
    assert!(
        acknowledged(&mut pub0, 3, Duration::from_secs(20)),
        "the publish waiting for a majority was not acknowledged once it had one"
    );
    assert!(
        delivered(&mut on0, "three", long),
        "not delivered once the majority was back"
    );

    // The operator's handoff: the first slot's owner, node 0, hands it
    // over; node 1 says it does not sequence. Publishing goes on. The first
    // publish after a handoff is dropped, not held (D-109): one resend, as
    // an at-least-once client does, and no more.
    cluster.signal(0, "-USR1");
    cluster.signal(1, "-USR1");
    wait_for_log(&cluster, 0, "handing the slot over");
    wait_for_log(&cluster, 1, "no handoff");
    let tries = publish_until_acknowledged(cluster.mqtt(1), "pub-1", "four", 4);
    assert!(
        tries <= 2,
        "acknowledged only on attempt {tries} after the handoff"
    );
    assert!(
        delivered(&mut on0, "four", long),
        "not delivered after the handoff"
    );

    // An incompatible configuration is refused: over node 2's data, with
    // another takeover setting, the process refuses to start…
    let mut other = cluster.spawn(2, None, &[("STRAUM_CLUSTER_TAKEOVER_TICKS", "4")]);
    let status = wait(&mut other.child, Duration::from_secs(20));
    assert!(!status.success(), "started over another cluster's data");
    assert!(
        log(&other.log).contains("belongs to another cluster"),
        "{}",
        log(&other.log)
    );
    // …and from an empty directory it starts, but its links are refused.
    let fresh = cluster.dir.join("data-2-other");
    let stranger = cluster.spawn(2, Some(&fresh), &[("STRAUM_CLUSTER_TAKEOVER_TICKS", "4")]);
    cluster.servers[2] = Some(stranger);
    let deadline = Instant::now() + Duration::from_secs(10);
    while !(cluster.log(0).contains("another configuration")
        || cluster.log(1).contains("another configuration"))
    {
        assert!(
            Instant::now() < deadline,
            "a node with another configuration was not refused"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    cluster.stop(2);

    // Node 2 as it was configured rejoins, and everyone stops cleanly.
    cluster.start(2);
    let mut pub2 = connect(cluster.mqtt(2), "pub-2b");
    publish(&mut pub2, "t/x", "five", 5);
    assert!(
        acknowledged(&mut pub2, 5, long),
        "no PUBACK after node 2 rejoined"
    );
    assert!(
        delivered(&mut on0, "five", long),
        "not delivered after node 2 rejoined"
    );
    for id in 0..NODES {
        cluster.stop(id);
    }
}
