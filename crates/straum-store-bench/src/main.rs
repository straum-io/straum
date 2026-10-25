//! Disk benchmark: the store's durable write path against two baselines, on
//! the same disk in the same process:
//!
//! * `raw`: payloads copied into one reusable buffer, one positional write
//!   and one `fdatasync` per batch to a single file
//! * `hashcopy`: `raw` plus BLAKE3 of every payload and CRC32C over the
//!   batch, the CPU work the store adds before its writes
//! * `sequential`, `parallel`, `inline`: `Store::put_batch` with
//!   `WriteMode::Sequential`, `WriteMode::ParallelSync` or
//!   `WriteMode::INLINE_SMALL` (threshold 4096 bytes)
//!
//! ```text
//! straum-store-bench --dir <dir on the measured disk> [--out results.csv]
//!     [--mode raw,hashcopy,sequential,parallel,inline] [--sizes 100,1024,10240] [--batches 1,8,64,512]
//!     [--reps 5] [--min-batches 100] [--max-batches 1000] [--max-bytes 1073741824]
//! ```
//!
//! A run writes unique payloads (no dedup) into a fresh subdirectory until
//! `--max-batches` batches or `--max-bytes` payload bytes, whichever comes
//! first, and never fewer than `--min-batches` batches. The subdirectory is
//! deleted after the run; before it, free space must be at least twice the
//! planned bytes. One CSV row per run, then one `median` row per
//! (mode, size, batch) with the median of each column over the repetitions
//! and the spread of msgs/s across them.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;

use compio::BufResult;
use compio::fs::OpenOptions;
use compio::io::AsyncWriteAtExt;
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

const HEADER: &str = "mode,msg_size,batch,rep,messages,batches,elapsed_ns,msgs_per_s,fsyncs,fsyncs_per_batch,bytes_written,bytes_per_msg,wal_bytes_per_msg,batch_p50_ns,batch_p99_ns,rep_min_msgs_per_s,rep_max_msgs_per_s,spread_pct,dedup_msgs,bytes_saved,meta_bytes_per_msg";
/// Planned bytes per message on top of the payload, for the free-space
/// check: above the store's metadata (WAL records, frame header share,
/// topic index entry) for every mode.
const OVERHEAD_PER_MSG: u64 = 512;
const TOPIC: &[u8] = b"bench";
const MODES: [&str; 5] = ["raw", "hashcopy", "sequential", "parallel", "inline"];

struct Args {
    dir: PathBuf,
    out: Option<PathBuf>,
    modes: Vec<String>,
    sizes: Vec<usize>,
    batches: Vec<usize>,
    reps: usize,
    min_batches: u64,
    max_batches: u64,
    max_bytes: u64,
}

impl Args {
    /// Batches for one run: up to `max_batches` or `max_bytes` of payload,
    /// whichever comes first, never fewer than `min_batches`.
    fn planned_batches(&self, size: usize, batch: usize) -> u64 {
        let per_batch = (size * batch).max(1) as u64;
        self.max_batches
            .min(self.max_bytes.div_ceil(per_batch))
            .max(self.min_batches)
    }
}

#[derive(Clone, Copy, Default)]
struct Run {
    messages: u64,
    batches: u64,
    elapsed_ns: u64,
    fsyncs: u64,
    bytes_written: u64,
    wal_bytes: u64,
    p50_ns: u64,
    p99_ns: u64,
    /// From `Stats`; zero for the raw and hashcopy baselines.
    dedup_msgs: u64,
    bytes_saved: u64,
    meta_bytes: u64,
}

impl Run {
    fn columns(&self) -> [f64; 14] {
        let msgs = self.messages as f64;
        [
            msgs,
            self.batches as f64,
            self.elapsed_ns as f64,
            msgs / (self.elapsed_ns as f64 / 1e9),
            self.fsyncs as f64,
            self.fsyncs as f64 / self.batches as f64,
            self.bytes_written as f64,
            self.bytes_written as f64 / msgs,
            self.wal_bytes as f64 / msgs,
            self.p50_ns as f64,
            self.p99_ns as f64,
            self.dedup_msgs as f64,
            self.bytes_saved as f64,
            self.meta_bytes as f64 / msgs,
        ]
    }
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!(
                "{e}\nusage: straum-store-bench --dir <dir> [--out file.csv] [--mode raw,hashcopy,sequential,parallel,inline] [--sizes 100,1024,10240] [--batches 1,8,64,512] [--reps 5] [--min-batches 100] [--max-batches 1000] [--max-bytes 1073741824]"
            );
            std::process::exit(2);
        }
    };
    if let Err(e) = run(&args) {
        eprintln!("straum-store-bench: {e}");
        std::process::exit(1);
    }
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&args.dir)?;
    let mut out: Box<dyn Write> = match &args.out {
        Some(path) => Box::new(std::fs::File::create(path)?),
        None => Box::new(std::io::stdout()),
    };
    writeln!(out, "{HEADER}")?;
    let rt = compio::runtime::Runtime::new()?;
    eprintln!("compio driver: {:?}", rt.driver_type());
    for mode in &args.modes {
        for &size in &args.sizes {
            for &batch in &args.batches {
                let batches = args.planned_batches(size, batch);
                let planned = batches * batch as u64 * (size as u64 + OVERHEAD_PER_MSG);
                let mut runs = Vec::with_capacity(args.reps);
                for rep in 1..=args.reps {
                    let dir = args.dir.join(format!("run-{mode}-{size}-{batch}-{rep}"));
                    let _ = std::fs::remove_dir_all(&dir);
                    check_free_space(&args.dir, planned)?;
                    std::fs::create_dir_all(&dir)?;
                    let r = rt.block_on(async {
                        match mode.as_str() {
                            "raw" => run_raw(&dir, size, batch, batches, false).await,
                            "hashcopy" => run_raw(&dir, size, batch, batches, true).await,
                            "sequential" => {
                                run_store(&dir, size, batch, batches, WriteMode::Sequential).await
                            }
                            "parallel" => {
                                run_store(&dir, size, batch, batches, WriteMode::ParallelSync).await
                            }
                            _ => {
                                run_store(&dir, size, batch, batches, WriteMode::INLINE_SMALL).await
                            }
                        }
                    })?;
                    std::fs::remove_dir_all(&dir)?;
                    write_row(
                        &mut out,
                        mode,
                        size,
                        batch,
                        &rep.to_string(),
                        &r.columns(),
                        None,
                    )?;
                    runs.push(r);
                }
                let median: Vec<f64> = (0..14)
                    .map(|c| median(runs.iter().map(|r| r.columns()[c]).collect()))
                    .collect();
                let rates: Vec<f64> = runs.iter().map(|r| r.columns()[3]).collect();
                let lo = rates.iter().copied().fold(f64::INFINITY, f64::min);
                let hi = rates.iter().copied().fold(0.0, f64::max);
                let spread = (hi - lo) / median[3] * 100.0;
                write_row(
                    &mut out,
                    mode,
                    size,
                    batch,
                    "median",
                    &median,
                    Some((lo, hi, spread)),
                )?;
                out.flush()?;
                eprintln!(
                    "{mode} size={size} batch={batch}: {batches} batches/run, median {:.0} msg/s, spread {spread:.1}%, {:.2} fsyncs/batch",
                    median[3], median[5]
                );
            }
        }
    }
    Ok(())
}

/// Stops the benchmark if the disk under `dir` has less than twice the
/// bytes a run plans to write. `df -Pk` is POSIX: no new dependency for
/// `statvfs`.
fn check_free_space(dir: &Path, planned: u64) -> Result<(), Box<dyn std::error::Error>> {
    let out = std::process::Command::new("df")
        .arg("-Pk")
        .arg(dir)
        .output()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let avail_kib: u64 = text
        .lines()
        .nth(1)
        .and_then(|l| l.split_whitespace().nth(3))
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| {
            format!(
                "cannot read free space of {} from df: {text}",
                dir.display()
            )
        })?;
    let avail = avail_kib * 1024;
    if avail < 2 * planned {
        return Err(format!(
            "not enough free space in {}: {avail} bytes available, a run plans {planned} bytes and needs twice that",
            dir.display()
        )
        .into());
    }
    Ok(())
}

/// Payload `i`: the index in the first 8 bytes (unique, so no dedup), then
/// a fixed fill.
fn fill_payloads(bufs: &mut [Vec<u8>], size: usize, first: u64) {
    for (i, buf) in bufs.iter_mut().enumerate() {
        buf.clear();
        buf.resize(size, 0xa5);
        let id = (first + i as u64).to_le_bytes();
        let n = size.min(8);
        buf[..n].copy_from_slice(&id[..n]);
    }
}

async fn run_store(
    dir: &Path,
    size: usize,
    batch: usize,
    batches: u64,
    write_mode: WriteMode,
) -> std::io::Result<Run> {
    let messages = batches as usize * batch;
    let config = Config {
        write_mode,
        ..Config::default()
    };
    let mut store = Store::open(dir, config)
        .await
        .map_err(std::io::Error::other)?;
    let mut bufs = vec![Vec::new(); batch];
    let mut lat = Vec::with_capacity(batches as usize);
    let mut sent = 0;
    while sent < messages {
        let n = batch;
        fill_payloads(&mut bufs[..n], size, sent as u64);
        let msgs: Vec<Message> = bufs[..n]
            .iter()
            .map(|p| Message {
                topic: TOPIC,
                payload: p,
            })
            .collect();
        let start = Instant::now();
        store
            .put_batch(std::hint::black_box(&msgs), sent as u64)
            .await
            .map_err(std::io::Error::other)?;
        lat.push(start.elapsed().as_nanos() as u64);
        sent += n;
    }
    let s = store.stats();
    Ok(finish(
        lat,
        messages,
        s.fsyncs,
        s.segment_bytes + s.wal_bytes + s.topic_bytes,
        s.wal_bytes,
        (
            s.messages_deduplicated,
            s.payload_bytes_saved,
            s.metadata_bytes,
        ),
    ))
}

async fn run_raw(
    dir: &Path,
    size: usize,
    batch: usize,
    batches: u64,
    hash: bool,
) -> std::io::Result<Run> {
    let messages = batches as usize * batch;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join("raw.dat"))
        .await?;
    let mut bufs = vec![Vec::new(); batch];
    let mut buf = Vec::with_capacity(batch * size);
    let mut lat = Vec::with_capacity(batches as usize);
    let (mut sent, mut pos, mut fsyncs) = (0, 0u64, 0);
    while sent < messages {
        let n = batch;
        fill_payloads(&mut bufs[..n], size, sent as u64);
        let start = Instant::now();
        if hash {
            let mut crc = 0;
            for p in &bufs[..n] {
                std::hint::black_box(ObjectId::of(p));
                crc = crc32c::crc32c_append(crc, p);
            }
            std::hint::black_box(crc);
        }
        // Concatenated into one write, as the store does for a segment.
        buf.clear();
        for p in &bufs[..n] {
            buf.extend_from_slice(p);
        }
        let len = buf.len() as u64;
        let BufResult(res, b) = (&file).write_all_at(buf, pos).await;
        buf = b;
        res?;
        file.sync_data().await?;
        lat.push(start.elapsed().as_nanos() as u64);
        pos += len;
        fsyncs += 1;
        sent += n;
    }
    Ok(finish(lat, messages, fsyncs, pos, 0, (0, 0, 0)))
}

fn finish(
    mut lat: Vec<u64>,
    messages: usize,
    fsyncs: u64,
    bytes_written: u64,
    wal_bytes: u64,
    (dedup_msgs, bytes_saved, meta_bytes): (u64, u64, u64),
) -> Run {
    let elapsed_ns = lat.iter().sum();
    let batches = lat.len() as u64;
    lat.sort_unstable();
    let pct = |p: f64| {
        lat.get(((lat.len() as f64 - 1.0) * p).round() as usize)
            .copied()
            .unwrap_or(0)
    };
    Run {
        messages: messages as u64,
        batches,
        elapsed_ns,
        fsyncs,
        bytes_written,
        wal_bytes,
        p50_ns: pct(0.50),
        p99_ns: pct(0.99),
        dedup_msgs,
        bytes_saved,
        meta_bytes,
    }
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    let n = v.len();
    if n == 0 {
        return 0.0;
    }
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

fn write_row(
    out: &mut dyn Write,
    mode: &str,
    size: usize,
    batch: usize,
    rep: &str,
    c: &[f64],
    spread: Option<(f64, f64, f64)>,
) -> std::io::Result<()> {
    let spread = match spread {
        Some((lo, hi, pct)) => format!("{lo:.1},{hi:.1},{pct:.1}"),
        None => ",,".to_owned(),
    };
    writeln!(
        out,
        "{mode},{size},{batch},{rep},{:.0},{:.0},{:.0},{:.1},{:.0},{:.3},{:.0},{:.1},{:.1},{:.0},{:.0},{spread},{:.0},{:.0},{:.1}",
        c[0], c[1], c[2], c[3], c[4], c[5], c[6], c[7], c[8], c[9], c[10], c[11], c[12], c[13]
    )
}

fn parse_args() -> Result<Args, String> {
    let mut args = Args {
        dir: PathBuf::new(),
        out: None,
        modes: MODES.map(String::from).to_vec(),
        sizes: vec![100, 1024, 10240],
        batches: vec![1, 8, 64, 512],
        reps: 5,
        min_batches: 100,
        max_batches: 1000,
        max_bytes: 1 << 30,
    };
    let list = |v: &str| -> Result<Vec<usize>, String> {
        v.split(',')
            .map(|x| x.parse().map_err(|_| format!("bad number: {x}")))
            .collect()
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it.next().ok_or_else(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--dir" => args.dir = value.into(),
            "--out" => args.out = Some(value.into()),
            "--mode" => {
                args.modes = value.split(',').map(str::to_owned).collect();
                if let Some(m) = args.modes.iter().find(|m| !MODES.contains(&m.as_str())) {
                    return Err(format!("unknown mode {m}"));
                }
            }
            "--sizes" => args.sizes = list(&value)?,
            "--batches" => args.batches = list(&value)?,
            "--reps" => args.reps = value.parse().map_err(|_| "bad --reps")?,
            "--min-batches" => args.min_batches = value.parse().map_err(|_| "bad --min-batches")?,
            "--max-batches" => args.max_batches = value.parse().map_err(|_| "bad --max-batches")?,
            "--max-bytes" => args.max_bytes = value.parse().map_err(|_| "bad --max-bytes")?,
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if args.dir.as_os_str().is_empty() {
        return Err("--dir is required".into());
    }
    if args.reps == 0 || args.min_batches == 0 || args.batches.contains(&0) {
        return Err("--reps, --min-batches and batch sizes must be positive".into());
    }
    Ok(args)
}
