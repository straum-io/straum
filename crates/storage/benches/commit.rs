//! Does the batching actually adapt to the workload? (docs/storage.md §94)
//!
//! Not a micro benchmark: each case runs a producer against a real file and
//! reports what the rules ask for — records per sync, bytes per sync, sync
//! latency and commit latency percentiles.

use std::fs;
use std::io::{IoSlice, Write};
use std::path::PathBuf;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use storage::{Commit, Committer, Wal, WalLimits, WalRecord};

fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    result.unwrap_or_else(|error| panic!("{error}"))
}

fn scratch(name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let safe: String = name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect();
    path.push(format!("straum-bench-{safe}-{}.wal", std::process::id()));
    let _ = fs::remove_file(&path);
    path
}

struct Run {
    name: &'static str,
    records: usize,
    payload: usize,
    commits: Vec<(Commit, Instant)>,
    submitted: Vec<Instant>,
    elapsed: Duration,
}

/// Runs one workload. `pace` is called before every record; it is where a
/// workload decides how fast it produces.
fn run(
    name: &'static str,
    records: usize,
    payload: usize,
    mut pace: impl FnMut(usize, &Receiver<Commit>),
) -> Run {
    let path = scratch(name);
    let (wal, _) = ok(Wal::open(&path, WalLimits::default()));
    let mut committer = Committer::new(wal, WalLimits::default(), true, 0);

    let (tx, mut rx) = channel::<WalRecord>();
    let (commit_tx, commit_rx): (Sender<Commit>, Receiver<Commit>) = channel();

    let writer = thread::spawn(move || {
        let mut seen = Vec::with_capacity(records);
        ok(committer.run(&mut rx, |commit| {
            seen.push((*commit, Instant::now()));
            let _ = commit_tx.send(*commit);
        }));
        seen
    });

    let body = Bytes::from(vec![b'x'; payload]);
    let mut submitted = Vec::with_capacity(records);
    let started = Instant::now();
    for index in 0..records {
        pace(index, &commit_rx);
        submitted.push(Instant::now());
        ok(tx.send(WalRecord::local(index as u64 + 1, body.clone())));
    }
    drop(tx);
    let commits = ok(writer.join().map_err(|_| "writer thread panicked"));
    let elapsed = started.elapsed();
    let _ = fs::remove_file(&path);

    Run {
        name,
        records,
        payload,
        commits,
        submitted,
        elapsed,
    }
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted.get(index).copied().unwrap_or(0)
}

fn report(run: &Run) {
    let syncs = run.commits.len();
    let records: usize = run.commits.iter().map(|(commit, _)| commit.records).sum();
    let bytes: usize = run.commits.iter().map(|(commit, _)| commit.bytes).sum();

    let mut sync_nanos: Vec<u64> = run
        .commits
        .iter()
        .map(|(commit, _)| commit.sync.as_nanos() as u64)
        .collect();
    sync_nanos.sort_unstable();

    // Commit latency: from submitting a record to the sync that covered it.
    let mut latencies: Vec<u64> = Vec::with_capacity(run.records);
    let mut next = 0usize;
    for (commit, finished) in &run.commits {
        let last = commit.durable_correlation as usize;
        while next < last {
            if let Some(submitted) = run.submitted.get(next) {
                latencies.push(finished.duration_since(*submitted).as_nanos() as u64);
            }
            next += 1;
        }
    }
    latencies.sort_unstable();

    let per_sync = records as f64 / syncs.max(1) as f64;
    let throughput = run.records as f64 / run.elapsed.as_secs_f64();

    println!(
        "{:<22} {:>9} {:>7} {:>10.1} {:>11} {:>9} {:>9} {:>9} {:>9} {:>11.0}",
        run.name,
        run.records,
        syncs,
        per_sync,
        bytes / syncs.max(1),
        micros(percentile(&sync_nanos, 0.5)),
        micros(percentile(&latencies, 0.5)),
        micros(percentile(&latencies, 0.95)),
        micros(percentile(&latencies, 0.99)),
        throughput,
    );
    let _ = run.payload;
}

fn micros(nanos: u64) -> String {
    format!("{:.1}", nanos as f64 / 1000.0)
}

/// §98: contiguous write, vectored write, or one write per record?
fn write_strategies() {
    let path = scratch("strategies");
    let payload = vec![b'x'; 4096];
    let records = 256;

    for (name, vectored, per_record) in [
        ("contiguous", false, false),
        ("vectored", true, false),
        ("per record", false, true),
    ] {
        let mut file = ok(fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path));

        let started = Instant::now();
        if per_record {
            for _ in 0..records {
                ok(file.write_all(&payload));
            }
        } else if vectored {
            let slices: Vec<IoSlice> = (0..records).map(|_| IoSlice::new(&payload)).collect();
            let mut written = 0usize;
            let mut cursor = slices.as_slice();
            while !cursor.is_empty() {
                let n = ok(file.write_vectored(cursor));
                written += n;
                let mut skip = 0usize;
                let mut consumed = n;
                for slice in cursor {
                    if consumed >= slice.len() {
                        consumed -= slice.len();
                        skip += 1;
                    } else {
                        break;
                    }
                }
                cursor = cursor.get(skip..).unwrap_or_default();
                if n == 0 {
                    break;
                }
            }
            let _ = written;
        } else {
            let mut buffer = Vec::with_capacity(records * payload.len());
            for _ in 0..records {
                buffer.extend_from_slice(&payload);
            }
            ok(file.write_all(&buffer));
        }
        ok(file.sync_data());
        let elapsed = started.elapsed();

        let bytes = (records * payload.len()) as f64;
        println!(
            "{:<22} {:>9.1} µs {:>10.0} MB/s",
            name,
            elapsed.as_secs_f64() * 1e6,
            bytes / elapsed.as_secs_f64() / 1e6
        );
    }
    let _ = fs::remove_file(&path);
}

/// D-157 A: what one `sync_data` costs by group size, on a file that grows
/// (as the WAL's does today: each sync also commits the new size through
/// the filesystem's journal), one extended with `set_len` (sparse: each
/// write still allocates blocks), and one written with zeros and synced
/// beforehand (each sync flushes data only). Not the WAL: raw writes of
/// `RECORD`-byte records, so the layouts differ in nothing else.
/// `fallocate` (unwritten extents, converted on write) is not tested: std
/// has no call for it.
fn sync_cost() {
    const RECORD: usize = 128;
    const SYNCS: usize = 500;
    for records in [1usize, 8, 16, 64, 256] {
        let group = vec![b'x'; records * RECORD];
        let size = (group.len() * SYNCS) as u64;
        for layout in ["growing", "sparse", "prezeroed"] {
            let path = scratch(&format!("sync-{layout}-{records}"));
            let mut file = ok(fs::OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(true)
                .open(&path));
            match layout {
                "sparse" => ok(file.set_len(size)),
                "prezeroed" => {
                    let zeros = vec![0u8; 1 << 20];
                    let mut left = size as usize;
                    while left > 0 {
                        let n = left.min(zeros.len());
                        ok(file.write_all(&zeros[..n]));
                        left -= n;
                    }
                    ok(file.sync_all());
                    ok(std::io::Seek::rewind(&mut file));
                }
                _ => {}
            }
            let mut syncs = Vec::with_capacity(SYNCS);
            let started = Instant::now();
            for _ in 0..SYNCS {
                ok(file.write_all(&group));
                let at = Instant::now();
                ok(file.sync_data());
                syncs.push(at.elapsed().as_nanos() as u64);
            }
            let elapsed = started.elapsed().as_secs_f64();
            syncs.sort_unstable();
            println!(
                "sync_cost layout={layout} records_per_sync={records} syncs={SYNCS} sync_p50_us={} sync_p99_us={} syncs_per_s={:.0} records_per_s={:.0}",
                micros(percentile(&syncs, 0.5)),
                micros(percentile(&syncs, 0.99)),
                SYNCS as f64 / elapsed,
                (SYNCS * records) as f64 / elapsed,
            );
            drop(file);
            let _ = fs::remove_file(&path);
        }
    }
}

fn main() {
    // `cargo test --all-targets` runs this program too, as a debug build and
    // with no arguments, and would do minutes of real fsyncs to measure a
    // build whose numbers mean nothing. Every real run is a release build,
    // so that is the line, not the arguments.
    if cfg!(debug_assertions) {
        eprintln!("commit: built without optimisation; use `cargo bench` or a --release build");
        return;
    }
    println!(
        "{:<22} {:>9} {:>7} {:>10} {:>11} {:>9} {:>9} {:>9} {:>9} {:>11}",
        "workload",
        "records",
        "syncs",
        "rec/sync",
        "bytes/sync",
        "sync p50",
        "lat p50",
        "lat p95",
        "lat p99",
        "records/s"
    );
    println!("{:-<115}", "");

    // §75: one record at a time, nothing else available.
    let idle = run("idle (one at a time)", 200, 256, |index, commits| {
        if index > 0 {
            let _ = commits.recv();
        }
    });
    report(&idle);

    // A paced producer: the batch should stay small while the rate is low.
    for (name, rate) in [("low 1k/s", 1_000u64), ("moderate 50k/s", 50_000)] {
        let start = Instant::now();
        let run = run(name, 2_000, 256, move |index, _| {
            let due = Duration::from_nanos(index as u64 * 1_000_000_000 / rate);
            while start.elapsed() < due {
                std::hint::spin_loop();
            }
        });
        report(&run);
    }

    // §76: as fast as the producer can go; batches should fill up with what
    // accumulates during each sync.
    report(&run("high (unpaced)", 20_000, 256, |_, _| {}));
    report(&run("high 4 KiB payload", 5_000, 4096, |_, _| {}));

    // §94: a burst that lands while a sync is in flight.
    report(&run("burst", 10_000, 1024, |index, _| {
        if index == 0 {
            thread::sleep(Duration::from_millis(5));
        }
    }));

    println!();
    println!("write strategy, 256 × 4 KiB + one sync (docs/storage.md §98)");
    write_strategies();

    println!();
    println!("sync cost by group size and file layout, 128 B records (D-157 A)");
    sync_cost();
}
