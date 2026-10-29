#![allow(dead_code)] // each test binary uses a different subset
#![allow(clippy::unwrap_used)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use straum_broker::{Broker, Config};
use straum_client::{Client, ClientConfig};

/// A fresh directory under the system temp dir, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("straum-broker-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn config(dir: &TempDir) -> Config {
    Config {
        data_dir: dir.0.join("data"),
        listen: "127.0.0.1:0".parse().unwrap(),
        cluster_id: "test".into(),
        node_id: 1,
        ..Config::default()
    }
}

pub fn client(b: &Broker) -> Client {
    Client::connect(ClientConfig::new(b.addr.to_string())).unwrap()
}
