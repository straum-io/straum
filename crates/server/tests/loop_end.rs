//! D-152 A: a `straum-server` whose node loop ends exits non-zero, rather
//! than idling as a process that serves nothing. One node runs alone; its
//! loop is ended by its log failing, the data directory turned read-only
//! before a promise. Unix only: signals and permissions.
#![cfg(unix)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// As in the cluster test (D-108): a block below Linux's ephemeral range
/// chosen by the process id, numbered by a counter.
fn free_port() -> u16 {
    static NEXT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(0);
    let block = u16::try_from(std::process::id() % 40).unwrap();
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    assert!(n < 300, "this test binary used up its block of ports");
    20_000 + block * 300 + n
}

fn spawn(dir: &Path, peers: &str, mqtt: u16, log: &Path) -> Child {
    let file = std::fs::File::create(log).unwrap();
    Command::new(env!("CARGO_BIN_EXE_straum-server"))
        .env_clear()
        .env("STRAUM_EXPERIMENTAL_CLUSTER", "1")
        .env("STRAUM_CLUSTER_NODE_ID", "0")
        .env("STRAUM_CLUSTER_PEERS", peers)
        .env("STRAUM_CLUSTER_MQTT_LISTEN", format!("127.0.0.1:{mqtt}"))
        .env("STRAUM_CLUSTER_DATA_DIR", dir)
        .env("STRAUM_LOG_LEVEL", "info")
        .env("NO_COLOR", "1")
        .stdin(Stdio::null())
        .stdout(file.try_clone().unwrap())
        .stderr(file)
        .spawn()
        .unwrap()
}

fn wait_for_log(log: &Path, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !std::fs::read_to_string(log)
        .unwrap_or_default()
        .contains(what)
    {
        assert!(
            Instant::now() < deadline,
            "no {what:?} in {}",
            log.display()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn exited(child: &mut Child, within: Duration) -> Option<std::process::ExitStatus> {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            return Some(status);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    None
}

/// Every directory under `dir`, read-only or writable again.
fn read_only(dir: &Path, on: bool) {
    for entry in std::fs::read_dir(dir).unwrap().flatten() {
        if entry.file_type().unwrap().is_dir() {
            read_only(&entry.path(), on);
        }
    }
    let mode = if on { 0o555 } else { 0o755 };
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn a_server_whose_node_loop_ends_exits_non_zero() {
    let root = std::env::temp_dir().join(format!("straum-server-loop-end-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let data = root.join("data");
    let peers = (0..3)
        .map(|id| format!("{id}=127.0.0.1:{}", free_port()))
        .collect::<Vec<_>>()
        .join(",");
    let mqtt = free_port();

    // First life: node 0 takes the first slot at once and promises it.
    let first = root.join("log-1");
    let mut child = spawn(&data, &peers, mqtt, &first);
    wait_for_log(&first, "node started");
    let deadline = Instant::now() + Duration::from_secs(10);
    while !data.join("promise").exists() {
        assert!(
            Instant::now() < deadline,
            "node 0 never promised the first slot"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status()
        .unwrap();
    assert!(exited(&mut child, Duration::from_secs(10)).is_some_and(|s| s.success()));

    // Second life: alone, it promises slot 4 after nine heartbeats of
    // silence, and the read-only directory fails that promise.
    let second = root.join("log-2");
    let mut child = spawn(&data, &peers, mqtt, &second);
    wait_for_log(&second, "node started");
    read_only(&data, true);
    let status = exited(&mut child, Duration::from_secs(10));
    read_only(&data, false);
    if status.is_none() {
        let _ = child.kill();
    }
    let log = std::fs::read_to_string(&second).unwrap_or_default();
    let status = status.unwrap_or_else(|| panic!("the server kept running:\n{log}"));
    assert!(!status.success(), "exited with {status}:\n{log}");
    assert!(log.contains("the node loop ended"), "{log}");
    let _ = std::fs::remove_dir_all(&root);
}
