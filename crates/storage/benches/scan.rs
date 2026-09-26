//! Reading a log back: recovery at startup, and the live-set pass that
//! compaction runs before it rewrites.
//!
//! This is not the message hot path, but compaction now runs beside the
//! writer and blocks commits for its duration, so what the scan costs is a
//! stall an operator feels (docs/storage.md §104).

use std::io::Write;
use std::path::PathBuf;

use divan::Bencher;
use storage::{WalLimits, replay};

fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("{error}"))
}

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

/// A log of `records` publish-shaped records, written once and reused by
/// every sample: building it is not the measured work.
fn log(records: usize, payload: usize) -> PathBuf {
    let mut path = std::env::temp_dir();
    path.push(format!(
        "straum-scan-{records}-{payload}-{}.wal",
        std::process::id()
    ));
    let mut file = ok(std::fs::File::create(&path));
    // The magic the scanner expects, then framed records through the same
    // encoder the writer uses.
    ok(file.write_all(&storage::empty_header_for_benchmarks()));

    let mut body = Vec::with_capacity(12 + 8 + payload);
    let mut framed = Vec::new();
    for sequence in 0..records {
        body.clear();
        body.push(1);
        body.push(0b0000_0010);
        body.extend_from_slice(&(sequence as u64).to_be_bytes());
        body.extend_from_slice(&8u16.to_be_bytes());
        body.extend_from_slice(b"bench/to");
        body.resize(body.len() + payload, 0xab);

        framed.clear();
        storage::encode_for_benchmarks(&body, &mut framed);
        ok(file.write_all(&framed));
    }
    ok(file.flush());
    drop(file);
    path
}

/// 64 B payloads: the shape the workload matrix calls the common case, and
/// the one where per-record overhead dominates.
#[divan::bench(args = [10_000, 100_000])]
fn scan_small(bencher: Bencher, records: usize) {
    let path = log(records, 64);
    bencher.bench_local(|| {
        let mut seen = 0usize;
        let recovery = ok(replay(&path, WalLimits::default(), &mut |payload| {
            seen = seen.wrapping_add(payload.len());
        }));
        divan::black_box((recovery.records, seen));
    });
}

/// 1 KiB payloads: the same record count moves sixteen times the bytes, so
/// this separates per-record cost from per-byte cost.
#[divan::bench(args = [10_000])]
fn scan_large(bencher: Bencher, records: usize) {
    let path = log(records, 1024);
    bencher.bench_local(|| {
        let mut seen = 0usize;
        let recovery = ok(replay(&path, WalLimits::default(), &mut |payload| {
            seen = seen.wrapping_add(payload.len());
        }));
        divan::black_box((recovery.records, seen));
    });
}
