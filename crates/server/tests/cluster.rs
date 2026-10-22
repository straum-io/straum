//! The experimental cluster path end to end (ADR-0008 step 7a, D-106 A):
//! three real `straum-server` processes on loopback, watched through their
//! metrics (no client protocol since D-188 A). One test, its steps in
//! order, so no other test in this file competes for the ports it picks.
//! Unix only: signals drive the handoff and the stops.
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
    // The node tests' blocks (40 of 300 from 20 000, below Linux's ephemeral
    // range): the same for every test binary, so two never overlap.
    let block = u16::try_from(std::process::id() % 40).unwrap();
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert!(n < 300, "this test binary used up its block of ports");
    20_000 + block * 300 + n
}

/// One node's process and where it logs.
struct Server {
    child: Child,
    log: PathBuf,
}

struct Cluster {
    dir: PathBuf,
    links: Vec<u16>,
    metrics: Vec<u16>,
    servers: Vec<Option<Server>>,
}

impl Cluster {
    fn new() -> Self {
        let dir =
            std::env::temp_dir().join(format!("straum-server-cluster-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let links = (0..NODES).map(|_| free_port()).collect();
        let metrics = (0..NODES).map(|_| free_port()).collect();
        Self {
            dir,
            links,
            metrics,
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
                "STRAUM_CLUSTER_METRICS_LISTEN",
                format!("127.0.0.1:{}", self.metrics[usize::from(id)]),
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

    /// One stable gauge of node `id`, scraped now; `None` if the node did
    /// not answer.
    fn gauge(&self, id: u8, name: &str) -> Option<u64> {
        let address = SocketAddr::from(([127, 0, 0, 1], self.metrics[usize::from(id)]));
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(2)).ok()?;
        stream.set_read_timeout(Some(Duration::from_secs(2))).ok()?;
        stream.write_all(b"GET / HTTP/1.1\r\n\r\n").ok()?;
        let mut text = String::new();
        stream.read_to_string(&mut text).ok()?;
        text.lines()
            .find_map(|line| line.strip_prefix(name)?.strip_prefix(' ')?.parse().ok())
    }

    /// Until `holds` is true of node `id`'s gauge `name`, or the deadline.
    fn wait_gauge(&self, id: u8, name: &str, holds: impl Fn(u64) -> bool) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(value) = self.gauge(id, name).filter(|&v| holds(v)) {
                return value;
            }
            assert!(
                Instant::now() < deadline,
                "node {id}: {name} never held ({:?}): {}",
                self.gauge(id, name),
                self.log(id)
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// The processes are killed whether the test passed or not (D-116). On a
/// failure every node's logs, from every life, go to the test's output and
/// the directory is kept, so a failure shows what all three nodes did, not
/// only the one an assertion names (D-118); on success it is removed.
/// Nothing in here panics: a cleanup that fails cannot hide the failure.
impl Drop for Cluster {
    fn drop(&mut self) {
        for server in self.servers.iter_mut().flatten() {
            let _ = server.child.kill();
            let _ = server.child.wait();
        }
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
            return;
        }
        let mut logs: Vec<PathBuf> = std::fs::read_dir(&self.dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok().map(|e| e.path()))
                    .filter(|p| {
                        p.file_name()
                            .is_some_and(|n| n.to_string_lossy().starts_with("log-"))
                    })
                    .collect()
            })
            .unwrap_or_default();
        logs.sort();
        for path in &logs {
            eprintln!("===== {} =====\n{}", path.display(), log(path));
        }
        eprintln!("===== kept: {} =====", self.dir.display());
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

#[test]
fn three_server_processes_commit_hand_over_and_refuse_another_cluster() {
    let mut cluster = Cluster::new();
    for id in 0..NODES {
        cluster.start(id);
    }

    // Node 0 sequences the first slot, and its start commits on all three.
    cluster.wait_gauge(0, "straum_leading", |v| v == 1);
    let first = cluster.wait_gauge(0, "straum_commit", |v| v >= 2);
    for id in 1..NODES {
        cluster.wait_gauge(id, "straum_applied", |v| v >= first);
    }

    // The operator's handoff: the first slot's owner, node 0, hands it
    // over; node 1 says it does not sequence. The next slot commits.
    cluster.signal(0, "-USR1");
    cluster.signal(1, "-USR1");
    wait_for_log(&cluster, 0, "handing the slot over");
    wait_for_log(&cluster, 1, "no handoff");
    cluster.wait_gauge(1, "straum_leading", |v| v == 1);
    let handed = cluster.wait_gauge(1, "straum_commit", |v| v > first);

    // An incompatible configuration is refused: over node 2's data, with
    // another takeover setting, the process refuses to start…
    cluster.stop(2);
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
    // Its links can be refused before it finishes starting; a SIGTERM
    // before its handler is installed kills it, which is not a clean stop
    // (seen under the full validation's load, logs kept by D-118 A).
    wait_for_log(&cluster, 2, "node started");
    cluster.stop(2);

    // Node 2 as it was configured rejoins and reaches the log, and
    // everyone stops cleanly.
    cluster.start(2);
    cluster.wait_gauge(2, "straum_applied", |v| v >= handed);
    for id in 0..NODES {
        cluster.stop(id);
    }
}
