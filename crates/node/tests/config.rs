//! The new path's startup checks (ADR-0008 step 7a, D-104 A): an invalid
//! configuration is refused, never corrected.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::net::SocketAddr;
use std::time::Duration;

use node::{ConfigError, Experimental, Node, NodeError};

fn address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

fn experimental() -> Experimental {
    Experimental {
        id: 1,
        voters: (0..3)
            .map(|id| (id, address(7000 + u16::from(id))))
            .collect(),
        listen: address(7001),
        mqtt: address(1884),
        data: std::env::temp_dir().join("straum-config-test"),
        heartbeat: node::experimental::DEFAULT_HEARTBEAT,
        takeover_ticks: node::experimental::DEFAULT_TAKEOVER_TICKS,
        read_bytes: node::experimental::DEFAULT_READ_BYTES,
        retain_bytes: node::DEFAULT_RETAIN_BYTES,
    }
}

#[test]
fn the_experimental_profile_passes_its_own_checks() {
    let config = experimental().node_config().unwrap();
    assert_eq!(config.core.nodes, vec![0, 1, 2]);
    assert_eq!(config.core.owner, 0);
    assert_eq!(config.core.lag_bound, None, "flow control is off in 7a");
    assert_eq!(
        config.core.max_uncommitted,
        cluster::DEFAULT_MAX_UNCOMMITTED
    );
    assert_eq!(config.peers.len(), 2);
}

#[test]
fn every_broken_rule_is_refused() {
    let with = |change: &dyn Fn(&mut Experimental)| {
        let mut e = experimental();
        change(&mut e);
        e.node_config().unwrap_err()
    };
    assert_eq!(
        with(&|e| e.voters.truncate(2)),
        ConfigError::Voters { count: 2 }
    );
    assert_eq!(
        with(&|e| e.voters.push((3, address(7003)))),
        ConfigError::Voters { count: 4 }
    );
    assert_eq!(
        with(&|e| {
            e.voters.truncate(1);
            e.id = 0;
        }),
        ConfigError::Voters { count: 1 }
    );
    assert_eq!(with(&|e| e.voters[2].0 = 1), ConfigError::DuplicateVoter(1));
    assert_eq!(with(&|e| e.id = 9), ConfigError::NotAVoter(9));
    assert_eq!(
        with(&|e| e.heartbeat = Duration::ZERO),
        ConfigError::Heartbeat(Duration::ZERO)
    );
    assert_eq!(
        with(&|e| e.takeover_ticks = 0),
        ConfigError::TakeoverTicks(0)
    );
    assert!(matches!(
        with(&|e| e.read_bytes = 64 << 20),
        ConfigError::ReadBytes { .. }
    ));
    assert!(matches!(
        with(&|e| e.read_bytes = 0),
        ConfigError::ReadBytes { .. }
    ));
}

#[tokio::test]
async fn a_node_refuses_to_start_on_a_broken_configuration() {
    let mut config = experimental().node_config().unwrap();
    config.read_bytes = config.max_frame;
    let refused = Node::start(config).await;
    assert!(
        matches!(
            refused,
            Err(NodeError::Config(ConfigError::ReadBytes { .. }))
        ),
        "{:?}",
        refused.err()
    );
}
