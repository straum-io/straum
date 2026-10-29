//! Deterministic simulation (spike): the store on the simulated disk of
//! `straum_store::sim`, driven by a seeded workload of batches,
//! checkpoints, compaction steps and releases, with one planned fault per
//! seed: a power loss at a seeded call, an I/O error, a corrupted read-back
//! of a new manifest (write-time quarantine) followed by a power loss, or a
//! damaged manifest at rest with a power loss during the open that
//! quarantines it. After the power loss the store is reopened and checked:
//!
//! * open succeeds;
//! * every acknowledged publish reads back with its bytes; an entry past
//!   them holds a payload of a batch that failed;
//! * every file recovery removed or moved is named in `RecoveryReport`,
//!   and every file it names as removed is gone;
//! * a second open reports nothing;
//! * the store accepts a batch.
//!
//! The same seed gives the same trace hash (every call, its arguments,
//! written bytes and crash images). Seeds: `STRAUM_STORE_SIM_SEEDS`
//! (default 48) from `STRAUM_STORE_SIM_SEED` (default 1).
#![cfg(feature = "sim")]
#![allow(clippy::unwrap_used)] // test crate

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use straum_store::sim::{self, Fault, Model, Rng};
use straum_store::{Config, Message, RecoveryAction, RecoveryReport, Store, StoreError, WriteMode};

const DIR: &str = "/sim/store";
const TRACKED: [&[u8]; 2] = [b"orders", b"events"];
const SCRATCH: &[u8] = b"scratch";
const STEPS: u64 = 80;

/// Polls once: a simulated call never pends, so neither does the store.
fn run<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("a store future was pending on the simulated disk"),
    }
}

fn config(seed: u64) -> Config {
    Config {
        segment_max_bytes: 4096,
        write_mode: match seed % 3 {
            0 => WriteMode::Sequential,
            1 => WriteMode::ParallelSync,
            _ => WriteMode::InlineSmall { threshold: 1024 },
        },
        index_segment_max_bytes: 4096,
        seek_page_entries: 2,
        dict_train_bytes: 16 << 10,
        max_block_raw_bytes: 4096,
        wal_file_max_bytes: 8 << 10,
        data_open_files: 2,
        ..Config::default()
    }
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    /// Power loss at a seeded call of the workload: any call, a call on a
    /// checkpoint file, or a sync, a third each.
    Crash,
    /// An I/O error at a seeded call, then a power loss at the end.
    Eio,
    /// A new manifest reads back corrupted, the checkpoint quarantines it,
    /// and power is lost at a seeded call from there.
    WriteQuarantine,
    /// The newest manifest is damaged at rest; power is lost at a seeded
    /// call of the open that quarantines it.
    OpenQuarantine,
}

#[derive(Default)]
struct Run {
    /// (topic, seq) → payload, acknowledged.
    acked: BTreeMap<(Vec<u8>, u64), Vec<u8>>,
    /// Payloads of batches that failed: may or may not be durable.
    maybe: BTreeSet<Vec<u8>>,
    errors: u64,
    checkpoints: u64,
}

fn payload(rng: &mut Rng) -> Vec<u8> {
    let len = 1 + rng.below(1500) as usize;
    // Half compressible, half not, so blocks go both inline and to segments.
    if rng.below(2) == 0 {
        vec![b'a' + rng.below(26) as u8; len]
    } else {
        (0..len).map(|_| rng.next_u64() as u8).collect()
    }
}

/// The seeded workload on the simulated disk as `sim::install` left it,
/// with the faults already planned.
fn workload(seed: u64) -> Run {
    let mut rng = Rng(seed);
    let mut out = Run::default();
    let mut store = match run(Store::open(DIR, config(seed))) {
        Ok(s) => s,
        // A fault planned in the first open: nothing was acknowledged.
        Err(_) => return out,
    };
    let pool: Vec<Vec<u8>> = (0..6).map(|_| payload(&mut rng)).collect();
    let mut now = 1_000;
    for step in 0..STEPS {
        now += 1 + rng.below(40);
        let op = rng.below(20);
        let res: Result<(), StoreError> = match op {
            0..=12 => {
                let n = 1 + rng.below(4) as usize;
                let batch: Vec<(&[u8], Vec<u8>)> = (0..n)
                    .map(|_| {
                        let topic = match rng.below(3) {
                            0 => SCRATCH,
                            t => TRACKED[t as usize - 1],
                        };
                        let p = if rng.below(4) == 0 {
                            pool[rng.below(pool.len() as u64) as usize].clone()
                        } else {
                            payload(&mut rng)
                        };
                        (topic, p)
                    })
                    .collect();
                let msgs: Vec<Message<'_>> = batch
                    .iter()
                    .map(|(t, p)| Message {
                        topic: t,
                        payload: p,
                        key: None,
                        headers: &[],
                    })
                    .collect();
                match run(store.put_batch(&msgs, now)) {
                    Ok(seqs) => {
                        // Scratch entries are released: not tracked.
                        for ((t, p), seq) in batch.iter().zip(seqs) {
                            if *t != SCRATCH {
                                out.acked.insert((t.to_vec(), seq), p.clone());
                            }
                        }
                        Ok(())
                    }
                    Err(e) => {
                        out.maybe.extend(batch.into_iter().map(|(_, p)| p));
                        Err(e)
                    }
                }
            }
            13..=15 => {
                out.checkpoints += 1;
                run(store.checkpoint())
            }
            16..=17 => run(store.compact_step()).map(|_| ()),
            _ => run(store.release_before(SCRATCH, now.saturating_sub(300))).map(|_| ()),
        };
        sim::note(format!("{step} {op} {:?}", res.as_ref().err()).as_bytes());
        if let Err(e) = res {
            if out.errors > 0 || !sim::is_down() {
                // After its first error the store refuses everything.
                assert!(
                    out.errors == 0 || matches!(e, StoreError::Poisoned),
                    "seed {seed}: a call after an error returned {e}"
                );
            }
            out.errors += 1;
        }
    }
    out
}

/// Names `report` says recovery removed or moved: `(removed, moved)`.
fn reported(report: &RecoveryReport) -> (BTreeSet<String>, BTreeSet<String>) {
    let (mut removed, mut moved) = (BTreeSet::new(), BTreeSet::new());
    for a in &report.actions {
        match a {
            RecoveryAction::OrphanSegmentRemoved { segment } => {
                removed.insert(format!("seg-{segment:016x}.dat"));
            }
            RecoveryAction::IndexSegmentRemoved { segment, .. }
            | RecoveryAction::IndexSegmentDropped { segment, .. } => {
                removed.insert(format!("idx-{segment:016x}.dat"));
            }
            RecoveryAction::CheckpointFileRemoved { file } => {
                removed.insert(file.clone());
            }
            RecoveryAction::CheckpointFileQuarantined { file, .. } => {
                moved.insert(file.clone());
            }
            RecoveryAction::CheckpointQuarantineResumed { found, .. } => {
                moved.extend(found.iter().cloned());
            }
            _ => {}
        }
    }
    (removed, moved)
}

/// Opens after a power loss and checks the rules in the module docs.
fn reopen_and_check(seed: u64, r: &Run, before: &BTreeMap<String, Option<u64>>) -> Store {
    let store = match run(Store::open(DIR, config(seed))) {
        Ok(s) => s,
        Err(e) => {
            let q: Vec<_> = before
                .keys()
                .filter(|n| n.starts_with("quarantine-"))
                .map(|n| (n, sim::listing(&Path::new(DIR).join(n))))
                .collect();
            panic!(
                "seed {seed}: open after power loss failed: {e}\nstore: {:?}\nquarantine: {q:?}",
                before
                    .keys()
                    .filter(|n| n.starts_with("checkpoint-") || n.starts_with("objtab-"))
                    .collect::<Vec<_>>()
            )
        }
    };
    let report = store.recovery_report().clone();
    sim::note(format!("{report:?}").as_bytes());
    let after = sim::listing(Path::new(DIR));
    let (removed, moved) = reported(&report);
    for (name, len) in before {
        let gone = !after.contains_key(name);
        if gone {
            assert!(
                removed.contains(name) || moved.contains(name) || name.ends_with(".log.tmp"),
                "seed {seed}: {name} vanished at open without a report entry: {report:?}"
            );
        }
        if name.ends_with(".log.tmp") && gone {
            assert!(
                report.actions.contains(&RecoveryAction::WalTmpRemoved),
                "seed {seed}: {name} removed unreported"
            );
        }
        if let (Some(Some(old)), Some(Some(new))) = (Some(*len), after.get(name))
            && new < &old
        {
            assert!(
                report.actions.iter().any(|a| matches!(
                    a,
                    RecoveryAction::TornTailTruncated { .. } | RecoveryAction::FrameDropped { .. }
                )),
                "seed {seed}: {name} shrank at open unreported"
            );
        }
    }
    for name in &removed {
        assert!(
            !after.contains_key(name),
            "seed {seed}: reported removed, still there: {name}"
        );
    }
    for ((topic, seq), p) in &r.acked {
        let got = run(store.read(topic, *seq));
        assert!(
            matches!(&got, Ok(Some(b)) if b == p),
            "seed {seed}: acknowledged {}:{seq} lost or wrong: {:?}",
            String::from_utf8_lossy(topic),
            got.map(|o| o.map(|b| b.len()))
        );
    }
    for topic in TRACKED {
        let last_acked = r.acked.keys().filter(|k| k.0 == topic).map(|k| k.1).max();
        let Some((_, end)) = store.topic_range(topic) else {
            assert!(last_acked.is_none(), "seed {seed}: topic lost");
            continue;
        };
        let from = last_acked.map_or(0, |s| s + 1);
        for seq in from..end {
            if let Ok(Some(b)) = run(store.read(topic, seq)) {
                assert!(
                    r.maybe.contains(&b),
                    "seed {seed}: entry {seq} was never written"
                );
            }
        }
    }
    store
}

/// One seed end to end; returns the trace hash and a summary.
fn simulate(seed: u64, model: Model) -> ([u8; 32], String) {
    let mut rng = Rng(seed ^ 0xfa17);
    let scenario = match seed % 4 {
        0 => Scenario::Crash,
        1 => Scenario::Eio,
        2 => Scenario::WriteQuarantine,
        _ => Scenario::OpenQuarantine,
    };
    // A dry run without faults lists the calls; the faults are aimed at
    // them. Deterministic: the faulty run makes the same calls up to the
    // first fault.
    sim::install(seed, model);
    workload(seed);
    let sites = sim::sites();
    let checkpointish = |s: &str| {
        [
            "checkpoint-",
            "objtab-",
            "reloc-",
            "blktab-",
            "blkrel-",
            "dict-",
        ]
        .iter()
        .any(|p| s.contains(p))
    };
    // aim: 0 any call, 1 a checkpoint file's call, 2 a sync (power lost
    // with writes not yet durable: dropped or torn in the crash image).
    let pick = |rng: &mut Rng, from: u64, aim: u64| -> u64 {
        let pool: Vec<u64> = sites
            .iter()
            .filter(|(c, s)| {
                *c >= from
                    && match aim {
                        0 => true,
                        1 => checkpointish(s),
                        _ => s.starts_with("sync "),
                    }
            })
            .map(|(c, _)| *c)
            .collect();
        if pool.is_empty() {
            from + rng.below(sites.len() as u64 + 1)
        } else {
            pool[rng.below(pool.len() as u64) as usize]
        }
    };
    sim::install(seed, model);
    let mut site = String::new();
    let r = match scenario {
        Scenario::Crash => {
            let aim = rng.below(3);
            let k = pick(&mut rng, 1, aim);
            site = sites
                .iter()
                .find(|s| s.0 == k)
                .map(|s| s.1.clone())
                .unwrap_or_default();
            sim::fail_at(k, Fault::Crash);
            workload(seed)
        }
        Scenario::Eio => {
            let k = pick(&mut rng, 1, 0);
            site = sites
                .iter()
                .find(|s| s.0 == k)
                .map(|s| s.1.clone())
                .unwrap_or_default();
            sim::fail_at(k, Fault::Eio);
            workload(seed)
        }
        Scenario::WriteQuarantine => {
            // The read-back of a new manifest (a whole-file read of
            // checkpoint-<n>), then a power loss up to 40 calls later.
            let reads: Vec<u64> = sites
                .iter()
                .filter(|(_, s)| s.starts_with("read checkpoint-"))
                .map(|(c, _)| *c)
                .collect();
            if let Some(&k) = reads.get(rng.below(reads.len().max(1) as u64) as usize) {
                sim::fail_at(k, Fault::Corrupt);
                sim::fail_at(k + 1 + rng.below(40), Fault::Crash);
                site = format!("corrupt read-back at {k}");
            }
            workload(seed)
        }
        Scenario::OpenQuarantine => workload(seed),
    };
    sim::power_loss();
    if let Scenario::OpenQuarantine = scenario {
        let names = sim::listing(Path::new(DIR));
        if let Some(m) = names
            .keys()
            .filter(|n| n.starts_with("checkpoint-") && !n.ends_with(".tmp"))
            .max()
        {
            sim::damage(&Path::new(DIR).join(m));
            // The open makes about 20 to 80 calls; past its end the
            // planned crash does not fire.
            let start = sim::calls();
            let k = start + 1 + rng.below(60);
            sim::fail_at(k, Fault::Crash);
            site = format!("damaged {m}, power loss at open call {}", k - start);
            let _ = run(Store::open(DIR, config(seed)));
            sim::power_loss();
        }
    }
    let before = sim::listing(Path::new(DIR));
    let store = reopen_and_check(seed, &r, &before);
    // Kinds of action the first open reported, for the sweep's summary.
    let first_report: BTreeSet<String> = store
        .recovery_report()
        .actions
        .iter()
        .map(|a| {
            let d = format!("{a:?}");
            d.split([' ', '{']).next().unwrap_or_default().to_owned()
        })
        .collect();
    drop(store);
    let mut store = run(Store::open(DIR, config(seed))).unwrap();
    assert!(
        store.recovery_report().is_empty(),
        "seed {seed}: second open repaired again: {:?}",
        store.recovery_report()
    );
    let msg = [Message {
        topic: TRACKED[0],
        payload: b"after",
        key: None,
        headers: &[],
    }];
    run(store.put_batch(&msg, u64::MAX / 2)).unwrap();
    let st = sim::stats();
    let summary = format!(
        "seed {seed} {scenario:?} {model:?} at [{site}] acked {} errors {} checkpoints {} report {:?} | {:?}",
        r.acked.len(),
        r.errors,
        r.checkpoints,
        first_report,
        st
    );
    (sim::trace(), summary)
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Ordered and Unordered alternate every four seeds, so each scenario
/// runs under both.
fn model(seed: u64) -> Model {
    if (seed / 4).is_multiple_of(2) {
        Model::Ordered
    } else {
        Model::Unordered
    }
}

#[test]
fn the_same_seed_gives_the_same_trace() {
    for seed in [3, 6, 7, 190] {
        let (a, _) = simulate(seed, model(seed));
        let (b, _) = simulate(seed, model(seed));
        assert_eq!(a, b, "seed {seed}: two runs differ");
        let (c, _) = simulate(seed + 4, model(seed + 4));
        assert_ne!(a, c, "seeds {seed} and {} gave the same trace", seed + 4);
    }
}

#[test]
fn seeded_faults_keep_every_acknowledged_write() {
    let first = env_u64("STRAUM_STORE_SIM_SEED", 1);
    let n = env_u64("STRAUM_STORE_SIM_SEEDS", 48);
    for seed in first..first + n {
        let (hash, summary) = simulate(seed, model(seed));
        eprintln!("{summary} trace {}", hex(&hash[..8]));
    }
}

/// Finding: the quarantine's recovery assumes that a rename between two
/// directories is atomic across a power loss (ext4 and XFS journal it as one
/// transaction; POSIX does not promise it). Under `Model::Posix` the moved
/// manifest can survive in both directories: open takes the quarantine as
/// completed, uses the manifest still in the store directory, finds its
/// tables in the quarantine and fails with `CorruptCheckpoint`. Seed 190 (a
/// write-time quarantine with a power loss right after the quarantine
/// directory's sync) reaches that state; this pins it until a decision.
#[test]
fn posix_rename_model_reaches_a_manifest_in_two_places() {
    let res = std::panic::catch_unwind(|| simulate(190, Model::Posix));
    let msg = match res {
        Ok(_) => panic!("seed 190 under Model::Posix no longer fails"),
        Err(e) => e.downcast_ref::<String>().cloned().unwrap_or_default(),
    };
    assert!(
        msg.contains("an object table is missing")
            && msg.contains("\"checkpoint-0000000000000004\"")
            && msg.contains("quarantine-0000000000000004"),
        "{msg}"
    );
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}
