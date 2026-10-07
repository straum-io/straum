//! The phase-0 harness over many seeds (ADR-0007).
//!
//! Every seed is a different interleaving of commits, byte-level deliveries,
//! reports, syncs, cuts, reconnects, stalls, crashes, power losses with torn
//! tails, injected sync failures, rewrites and fresh copies. A failing seed
//! is reproduced with `STRAUM_SIM_SEED=<seed> cargo test -p sim`. The default
//! is 30 seeds per mode, sized for `cargo test --workspace` in a debug build;
//! the wide sweep is `STRAUM_SIM_SEEDS=1000 cargo test --release -p sim`
//! (about 10 s on an M1).
//!
//! No violation is tolerated. The one the harness found on its first sweep,
//! D-43, is fixed, and its seed is kept as a regression below.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use sim::{Config, Mode, run};

fn seeds() -> std::ops::Range<u64> {
    match std::env::var("STRAUM_SIM_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        Some(seed) => seed..seed + 1,
        None => {
            0..std::env::var("STRAUM_SIM_SEEDS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(30)
        }
    }
}

/// Runs every seed; any violation fails the run, with its seed and step.
fn sweep(mode: Mode) {
    for seed in seeds() {
        match run(Config {
            seed,
            steps: 400,
            mode,
        }) {
            Ok(report) => assert!(report.counts.commits > 0, "seed {seed} committed nothing"),
            Err(violation) => panic!("{violation}"),
        }
    }
}

#[test]
fn fsynced_holds_every_invariant() {
    sweep(Mode::Fsynced);
}

#[test]
fn quorum_holds_every_invariant() {
    sweep(Mode::Quorum);
}

/// D-43, the regression: on this seed a follower's log sync failed after the
/// batch reached the operating system, the process restarted, the restarted
/// follower reported the recovered records as durable, and a power loss took
/// them. The log now syncs what it recovers before it calls it durable.
#[test]
fn a_restart_no_longer_reports_unsynced_records_as_durable() {
    for mode in [Mode::Fsynced, Mode::Quorum] {
        let outcome = run(Config {
            seed: 3,
            steps: 400,
            mode,
        });
        assert!(outcome.is_ok(), "{:?}", outcome.err());
    }
}

/// The same seed makes the same choices and sees the same outcomes.
#[test]
fn a_seed_is_a_trace() {
    for mode in [Mode::Fsynced, Mode::Quorum] {
        for seed in [1, 1000, 123_456] {
            let config = Config {
                seed,
                steps: 400,
                mode,
            };
            let first = run(config);
            let second = run(config);
            assert_eq!(first, second, "seed {seed} ran two ways");
        }
    }
}

/// The run exercises what it claims to: every kind of event happens across
/// the seeds, so an invariant that held held under them.
#[test]
fn every_kind_of_event_happens() {
    let mut total = sim::Counts::default();
    for seed in 0..20 {
        for mode in [Mode::Fsynced, Mode::Quorum] {
            if let Ok(report) = run(Config {
                seed,
                steps: 400,
                mode,
            }) {
                let c = report.counts;
                total.commits += c.commits;
                total.chunks += c.chunks;
                total.frames += c.frames;
                total.reports += c.reports;
                total.leader_crashes += c.leader_crashes;
                total.leader_power_losses += c.leader_power_losses;
                total.follower_crashes += c.follower_crashes;
                total.follower_power_losses += c.follower_power_losses;
                total.sync_failures += c.sync_failures;
                total.cuts += c.cuts;
                total.connections += c.connections;
                total.stalls += c.stalls;
                total.refusals += c.refusals;
                total.rewrites += c.rewrites;
                total.fresh_copies += c.fresh_copies;
            }
        }
    }
    eprintln!("{total:?}");
    let every = [
        total.commits,
        total.chunks,
        total.frames,
        total.reports,
        total.leader_crashes,
        total.leader_power_losses,
        total.follower_crashes,
        total.follower_power_losses,
        total.sync_failures,
        total.cuts,
        total.connections,
        total.stalls,
        total.refusals,
        total.rewrites,
        total.fresh_copies,
    ];
    assert!(every.iter().all(|n| *n > 0), "{total:?}");
}
