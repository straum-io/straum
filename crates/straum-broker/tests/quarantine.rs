//! The startup warning for quarantined checkpoint files: printed at every
//! start while a quarantine directory exists, never as a `recovery:` line
//! (a quarantine is reported once, by the store open that made it).

#![allow(clippy::unwrap_used)]

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::TempDir;
use straum_client::wait_ready;

/// Starts the broker on `data`, waits until it answers Hello, stops it and
/// returns its stderr.
fn start_and_stop(data: &std::path::Path) -> String {
    let addr = straum_conformance::broker::free_addr().unwrap();
    let log = data.with_extension("stderr.log");
    let mut child = Command::new(env!("CARGO_BIN_EXE_straum-server"))
        .args(["--data-dir", &data.to_string_lossy(), "--listen", &addr])
        .args(["--cluster-id", "q", "--node-id", "1"])
        .stdout(Stdio::null())
        .stderr(std::fs::File::create(&log).unwrap())
        .spawn()
        .unwrap();
    let ready = wait_ready(&addr, Duration::from_secs(60), || {
        matches!(child.try_wait(), Ok(None))
    });
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(ready.is_ok(), "broker not ready: {ready:?}");
    std::fs::read_to_string(&log).unwrap()
}

#[test]
fn quarantined_files_give_a_startup_warning_at_every_start() {
    let dir = TempDir::new("broker-quarantine");
    let data = dir.0.join("data");
    let first = start_and_stop(&data);
    assert!(!first.contains("warning:"), "{first}");
    let q = data.join("store").join("quarantine-0000000000000002");
    std::fs::create_dir(&q).unwrap();
    std::fs::write(q.join("checkpoint-0000000000000002"), b"damaged").unwrap();
    for _ in 0..2 {
        let err = start_and_stop(&data);
        let warnings: Vec<&str> = err
            .lines()
            .filter(|l| l.starts_with("straum-server: warning: 1 quarantined checkpoint file(s)"))
            .collect();
        assert_eq!(warnings.len(), 1, "{err}");
        assert!(err.contains("store recovery: 0 action(s)"), "{err}");
        assert!(!err.contains("straum-server: recovery:"), "{err}");
    }
}
