//! Links C1–C4 of the deductive chain of D-28, held by a tool rather than by
//! review. C0 (every acknowledged publish reaches the broker with a token) and
//! C5 (the quorum watermark and the follower reporting after its sync) are not
//! checked here; a count cannot show C0 (D-28), and D-46 decides how they are.
//! C3 is checked as "a single caller", not the full statement of which events
//! may reach it.
//!
//! The chain says why a PUBACK cannot leave before durability: it is created
//! in exactly two places in the broker, the durability mark only moves in one
//! handler, that handler is called only from the broker loop, the log reports
//! only after its sync, and the sync is on exactly for `fsynced` and `quorum`.
//! Each of those is a statement about where code is, so each is checked here
//! by reading the code. A change that adds a third place or a second caller
//! fails this test, and has to come with a new argument for the chain.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

fn source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join(relative);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// The production part of a file: everything before its test module.
fn production(relative: &str) -> String {
    let all = source(relative);
    match all.find("#[cfg(test)]\nmod tests") {
        Some(at) => all[..at].to_owned(),
        None => all,
    }
}

fn count(haystack: &str, needle: &str) -> usize {
    haystack.matches(needle).count()
}

/// C1: a PUBACK for a publish is created in exactly two places.
#[test]
fn a_puback_is_created_in_two_places() {
    let broker = production("broker/src/lib.rs");
    assert_eq!(count(&broker, "Response::PublishAccepted("), 2);
}

/// C2: the durability mark moves only in `Broker::on_durable`.
#[test]
fn the_durability_mark_moves_in_one_place() {
    let broker = production("broker/src/lib.rs");
    assert_eq!(count(&broker, "self.durable_high_water = "), 1);
}

/// C3: the broker's durability handler is called from one place in the
/// runtime, and a durable publish enters through one place.
#[test]
fn the_runtime_reaches_durability_through_one_door() {
    let runtime = production("runtime/src/broker_loop.rs");
    assert_eq!(count(&runtime, "self.broker.on_durable("), 1);
    assert_eq!(count(&runtime, ".handle_durable("), 1);
    for other in [
        "runtime/src/lib.rs",
        "runtime/src/connection.rs",
        "runtime/src/wal.rs",
        "runtime/src/replication.rs",
    ] {
        let file = production(other);
        assert_eq!(count(&file, ".handle_durable("), 0, "{other}");
        assert_eq!(count(&file, "broker.on_durable("), 0, "{other}");
    }
}

/// C4: the log advances its durable sequence only after the sync, and the
/// sync is on for exactly `fsynced` and `quorum`.
#[test]
fn durability_follows_the_sync() {
    let wal = production("storage/src/wal.rs");
    let sync = wal
        .find("            self.sync()?;")
        .expect("the sync call");
    let advance = wal
        .find("        self.durable_sequence = self.sequence;")
        .expect("the durable advance");
    assert!(sync < advance, "the durable sequence moves before the sync");
    let runtime = production("runtime/src/lib.rs");
    assert_eq!(
        count(&runtime, "Durability::Fsynced | Durability::Quorum => true"),
        1
    );
    assert_eq!(
        count(
            &runtime,
            "Durability::Memory | Durability::Written => false"
        ),
        1
    );
}
