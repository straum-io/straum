//! Invariant 5 of unified-stream §5 over crashes (ADR-0008 step 1): batches
//! of `journal` records written through the production log on a simulated
//! disk, power lost at random points with torn tails. After every recovery
//! the records form one unbroken chain, no batch whose commit returned is
//! missing, and a torn batch can only be the last one.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::PathBuf;

use journal::{Chain, Entry, Kind, Verifier};
use storage::simulated::SimDisk;
use storage::{Retain, Wal, WalLimits, replay_on};

fn path() -> PathBuf {
    PathBuf::from("/sim/journal.wal")
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn records(disk: &SimDisk) -> Vec<Vec<u8>> {
    let mut seen = Vec::new();
    replay_on(disk, &path(), WalLimits::default(), &mut |r| {
        seen.push(r.to_vec())
    })
    .unwrap();
    seen
}

/// `zeros`: zeros written ahead of the log (ADR-0009), 128 bytes, less than
/// some batches, so extensions happen too; and power lost sector by sector
/// in any order (`SimDisk::power_loss_sectors`) instead of a torn tail.
fn run(seed: u64, zeros: bool) -> (u32, u32) {
    let limits = WalLimits {
        preallocate_bytes: if zeros { 128 } else { 0 },
        ..WalLimits::default()
    };
    let (mut crashes, mut torn) = (0, 0);
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let disk = SimDisk::new();
    let mut wal = Wal::open_on(disk.clone(), &path(), limits).unwrap().0;
    let mut chain = Chain::genesis();
    // The last lsn of a batch whose synced commit returned, and the chain
    // hash at every such batch: recovery must reproduce the same history,
    // not just reach the same position (invariant 1).
    let mut committed = 0u64;
    let mut synced_at: Vec<(u64, u128)> = Vec::new();
    let mut slot = 1u64;
    for step in 0..200 {
        if rng.below(20) == 0 {
            // A new slot now and then: lsn continues, slot only grows.
            slot += 1;
        }
        let count = rng.below(5) as usize;
        let bodies: Vec<Vec<u8>> = (0..count)
            .map(|i| format!("s{seed}t{step}r{i}").into_bytes())
            .collect();
        let entries: Vec<Entry<'_>> = bodies
            .iter()
            .map(|b| Entry {
                kind: Kind::Publish,
                properties: &[],
                body: b,
            })
            .collect();
        let mut batch = Vec::new();
        chain.batch(slot, &entries, &mut batch).unwrap();
        for record in &batch {
            wal.stage(record).unwrap();
        }
        // Sometimes the sync never happens before the power goes.
        let synced = rng.below(4) != 0;
        if wal.commit_with(synced).is_ok() && synced {
            committed = chain.next_lsn() - 1;
            synced_at.push((committed, chain.hash()));
        }

        if rng.below(8) == 0 {
            drop(wal);
            if zeros {
                disk.power_loss_sectors(rng.next());
            } else {
                disk.power_loss(rng.below(200) as usize);
            }
            let recovered = records(&disk);
            let mut verifier = Verifier::new(Chain::genesis());
            for (at, record) in recovered.iter().enumerate() {
                verifier
                    .feed(record)
                    .unwrap_or_else(|e| panic!("seed {seed} step {step} record {at}: {e}"));
                let so_far = verifier.clone().finish();
                let lsn = so_far.chain.next_lsn() - 1;
                let synced = synced_at.iter().find(|(l, _)| *l == lsn);
                if let (0, Some(&(_, hash))) = (so_far.partial, synced) {
                    assert_eq!(
                        so_far.chain.hash(),
                        hash,
                        "seed {seed} step {step}: batch through {lsn} changed"
                    );
                }
            }
            let tail = verifier.finish();
            crashes += 1;
            torn += u32::from(tail.partial > 0);
            assert!(
                tail.chain.next_lsn() > committed,
                "seed {seed} step {step}: committed through {committed}, recovered through {}",
                tail.chain.next_lsn() - 1
            );
            // Cut a torn batch before appending. Until the slot table can
            // truncate by lsn (step 2), a rewrite that drops records past the
            // last complete batch does it.
            wal = Wal::open_on(disk.clone(), &path(), limits).unwrap().0;
            if tail.partial > 0 {
                let through = tail.chain.next_lsn() - 1;
                wal.compact(0, Retain::NONE, &mut |record| {
                    journal::decode(record).is_ok_and(|r| r.lsn <= through)
                })
                .unwrap();
            }
            chain = tail.chain;
            committed = committed.min(chain.next_lsn() - 1);
            synced_at.retain(|(l, _)| *l < chain.next_lsn());
        }
    }
    // And whatever is in the file at the end verifies.
    drop(wal);
    let mut verifier = Verifier::new(Chain::genesis());
    for record in records(&disk) {
        verifier.feed(&record).unwrap();
    }
    (crashes, torn)
}

#[test]
fn the_chain_survives_power_loss_and_torn_tails() {
    let seeds: u64 = std::env::var("STRAUM_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let (mut crashes, mut torn) = (0, 0);
    for seed in 0..seeds {
        let (c, t) = run(seed, false);
        crashes += c;
        torn += t;
    }
    // Not vacuous: the sweep did lose power, and some of it mid-batch.
    assert!(crashes > 0 && torn > 0, "crashes {crashes}, torn {torn}");
    println!("crashes {crashes}, torn tails {torn}");
}

/// ADR-0009 (D-170 A): the same chain properties with zeros written ahead of
/// the log and power lost sector by sector, in any order, over them.
#[test]
fn the_chain_survives_sector_torn_power_loss_with_zeros_ahead() {
    let seeds: u64 = std::env::var("STRAUM_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let (mut crashes, mut torn) = (0, 0);
    for seed in 0..seeds {
        let (c, t) = run(seed, true);
        crashes += c;
        torn += t;
    }
    assert!(crashes > 0 && torn > 0, "crashes {crashes}, torn {torn}");
    println!("with zeros ahead: crashes {crashes}, torn batches {torn}");
}
