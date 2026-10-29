//! Disk benchmark: the store's durable write path against two baselines, on
//! the same disk in the same process, and the store's read path:
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
//!     [--payload pattern|random|file:<path>] [--seed 1] [--label <text>] [--rep-offset 0] [--no-header]
//!     [--reads 0] [--read-seconds 5] [--read-cache default,off] [--reads-out reads.csv] [--clk-tck 100]
//! ```
//!
//! A run writes payloads into a fresh subdirectory until `--max-batches`
//! batches or `--max-bytes` payload bytes, whichever comes first, and never
//! fewer than `--min-batches` batches. The subdirectory is deleted after the
//! run; before it, free space must be at least twice the planned bytes. One
//! CSV row per run; with more than one repetition, then one `median` row per
//! (mode, size, batch) with the median of each column over the repetitions
//! and the spread of msgs/s across them.
//!
//! Payloads (`--payload`):
//!
//! * `pattern`: the message index in the first 8 bytes, then a fixed fill
//!   (unique, highly compressible)
//! * `random`: bytes of a xorshift64* stream from `--seed` (unique,
//!   incompressible)
//! * `file:<path>`: consecutive `size`-byte windows of the file. At the end
//!   of the file the windows start again one byte further in, so a payload
//!   repeats only where the file itself repeats.
//!
//! With `--reads N` (store modes only) the store is reopened after the
//! writes and read twice: a sequential scan from the first `topic_seq`, and
//! reads of random `topic_seq`s (xorshift64* from `--seed`). Each workload
//! stops after N reads or `--read-seconds`, whichever comes first, and is
//! repeated for every `--read-cache` setting: `default` (the configuration
//! default) or, with the `blocks` feature, `off` (`block_cache_blocks = 0`).
//! Built without `blocks` (an engine without a block cache) the setting is
//! reported as `none`. Reads come from the page cache as written: dropping
//! it needs root.
//!
//! CPU time is user plus system time of the process from `/proc/self/stat`
//! (the accounting `getrusage(RUSAGE_SELF)` reports, in `--clk-tck` units)
//! over the write loop or one read workload, payload preparation included;
//! peak RSS is `VmHWM` from `/proc/self/status` after the write loop, reset
//! before it through `/proc/self/clear_refs`. Both are Linux only and `NaN`
//! elsewhere.

use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use compio::BufResult;
use compio::fs::OpenOptions;
use compio::io::AsyncWriteAtExt;
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

const HEADER: &str = "label,payload,mode,msg_size,batch,rep,messages,batches,elapsed_ns,msgs_per_s,fsyncs,fsyncs_per_batch,bytes_written,bytes_per_msg,wal_bytes_per_msg,disk_bytes_per_msg,batch_p50_ns,batch_p99_ns,cpu_ns_per_msg,peak_rss_bytes,rep_min_msgs_per_s,rep_max_msgs_per_s,spread_pct,dedup_msgs,bytes_saved,meta_bytes_per_msg,seg_sync_p50_ns,seg_sync_p99_ns,wal_sync_p50_ns,wal_sync_p99_ns,overlap_pct,max_block_raw_bytes";
const READ_HEADER: &str = "label,payload,mode,msg_size,batch,rep,workload,cache,messages,reads,elapsed_ns,msgs_per_s,read_p50_ns,read_p99_ns,cpu_ns_per_read";
/// Columns of `Run::columns`.
const COLUMNS: usize = 22;
/// Planned bytes per message on top of the payload, for the free-space
/// check: above the store's metadata (WAL records, frame header share,
/// topic index entry) for every mode.
const OVERHEAD_PER_MSG: u64 = 512;
const TOPIC: &[u8] = b"bench";
/// The topic of the dedup scan: half of its messages repeat the bodies of
/// random earlier messages of `TOPIC`.
const DEDUP_TOPIC: &[u8] = b"dedup";
/// Cursors of the multiple-consumer workload.
const CONSUMERS: u64 = 4;
const MODES: [&str; 5] = ["raw", "hashcopy", "sequential", "parallel", "inline"];

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadCache {
    Default,
    #[cfg(feature = "blocks")]
    Off,
}

impl ReadCache {
    fn name(self) -> &'static str {
        match self {
            #[cfg(feature = "blocks")]
            Self::Default => "default",
            #[cfg(not(feature = "blocks"))]
            Self::Default => "none",
            #[cfg(feature = "blocks")]
            Self::Off => "off",
        }
    }

    fn config(self, write_mode: WriteMode, args: &Args) -> Config {
        #[cfg_attr(not(feature = "blocks"), allow(unused_mut))]
        let mut config = store_config(write_mode, args);
        #[cfg(feature = "blocks")]
        if self == Self::Off {
            config.block_cache_blocks = 0;
        }
        config
    }
}

/// The store configuration of a run: the write mode and, with the `cap`
/// feature, `--max-block-raw-bytes`.
#[cfg_attr(not(feature = "cap"), allow(unused_variables))]
fn store_config(write_mode: WriteMode, args: &Args) -> Config {
    #[cfg_attr(not(feature = "cap"), allow(unused_mut))]
    let mut config = Config {
        write_mode,
        ..Config::default()
    };
    #[cfg(feature = "cap")]
    if let Some(cap) = args.max_block_raw_bytes {
        config.max_block_raw_bytes = cap;
    }
    config
}

/// InlineSmall's threshold: with `--max-block-raw-bytes` (feature `cap`),
/// the cap, so any capped block can go inline; otherwise the engine's
/// default (4096 bytes).
#[cfg_attr(not(feature = "cap"), allow(unused_variables))]
fn inline_mode(args: &Args) -> WriteMode {
    #[cfg(feature = "cap")]
    if let Some(cap) = args.max_block_raw_bytes {
        return WriteMode::InlineSmall {
            threshold: u32::try_from(cap).unwrap_or(u32::MAX),
        };
    }
    WriteMode::INLINE_SMALL
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Workload {
    Scan,
    Random,
    DedupScan,
    Multi,
}

impl Workload {
    const ALL: [Self; 4] = [Self::Scan, Self::Random, Self::DedupScan, Self::Multi];

    fn name(self) -> &'static str {
        match self {
            Self::Scan => "scan",
            Self::Random => "random",
            Self::DedupScan => "dedup_scan",
            Self::Multi => "multi",
        }
    }
}

enum Payload {
    Pattern,
    Random,
    File {
        path: PathBuf,
        file: std::fs::File,
        len: u64,
    },
}

impl Payload {
    fn name(&self) -> String {
        match self {
            Self::Pattern => "pattern".into(),
            Self::Random => "random".into(),
            Self::File { path, .. } => path
                .file_name()
                .map_or_else(|| "file".into(), |n| n.to_string_lossy().into_owned()),
        }
    }
}

/// Fills one batch of payloads at a time, in message order.
struct Source<'a> {
    kind: &'a Payload,
    size: usize,
    /// Messages produced so far.
    next: u64,
    rng: XorShift,
    /// `file:`: next window offset, and how many times the file wrapped.
    offset: u64,
    pass: u64,
}

impl<'a> Source<'a> {
    fn new(kind: &'a Payload, size: usize, seed: u64) -> Self {
        Self {
            kind,
            size,
            next: 0,
            rng: XorShift::new(seed),
            offset: 0,
            pass: 0,
        }
    }

    fn fill(&mut self, bufs: &mut [Vec<u8>]) -> std::io::Result<()> {
        for buf in bufs.iter_mut() {
            buf.clear();
            buf.resize(self.size, 0xa5);
            match self.kind {
                Payload::Pattern => {
                    let id = self.next.to_le_bytes();
                    let n = self.size.min(8);
                    buf[..n].copy_from_slice(&id[..n]);
                }
                Payload::Random => {
                    for chunk in buf.chunks_mut(8) {
                        let v = self.rng.next().to_le_bytes();
                        chunk.copy_from_slice(&v[..chunk.len()]);
                    }
                }
                Payload::File { file, len, .. } => {
                    let size = self.size as u64;
                    if self.offset + size > *len {
                        self.pass += 1;
                        self.offset = self.pass % size.max(1);
                        if self.offset + size > *len {
                            return Err(std::io::Error::other(
                                "payload file shorter than a message",
                            ));
                        }
                    }
                    file.read_exact_at(buf, self.offset)?;
                    self.offset += size;
                }
            }
            self.next += 1;
        }
        Ok(())
    }
}

/// xorshift64*: a fixed, dependency-free stream for payloads and read order.
struct XorShift(u64);

impl XorShift {
    fn new(seed: u64) -> Self {
        Self(seed.max(1))
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }
}

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
    payload: Payload,
    seed: u64,
    label: String,
    rep_offset: usize,
    header: bool,
    reads: u64,
    read_time: Duration,
    read_caches: Vec<ReadCache>,
    reads_out: Option<PathBuf>,
    clk_tck: u64,
    /// `Config::max_block_raw_bytes` (feature `cap`); `None`: the engine's
    /// default.
    max_block_raw_bytes: Option<u64>,
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
    disk_bytes: u64,
    p50_ns: u64,
    p99_ns: u64,
    cpu_ns: Option<u64>,
    peak_rss: Option<u64>,
    /// From `Stats`; zero for the raw and hashcopy baselines.
    dedup_msgs: u64,
    bytes_saved: u64,
    meta_bytes: u64,
    /// Per-batch sync durations (feature `cap`): segment p50, p99, WAL p50,
    /// p99 (ns), and the percentage of batches whose two syncs overlapped.
    syncs: Option<[f64; 5]>,
}

impl Run {
    fn columns(&self) -> [f64; COLUMNS] {
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
            self.disk_bytes as f64 / msgs,
            self.p50_ns as f64,
            self.p99_ns as f64,
            self.cpu_ns.map_or(f64::NAN, |c| c as f64 / msgs),
            self.peak_rss.map_or(f64::NAN, |r| r as f64),
            self.dedup_msgs as f64,
            self.bytes_saved as f64,
            self.meta_bytes as f64 / msgs,
            self.syncs.map_or(f64::NAN, |s| s[0]),
            self.syncs.map_or(f64::NAN, |s| s[1]),
            self.syncs.map_or(f64::NAN, |s| s[2]),
            self.syncs.map_or(f64::NAN, |s| s[3]),
            self.syncs.map_or(f64::NAN, |s| s[4]),
        ]
    }
}

/// One read workload's result.
struct Reads {
    workload: &'static str,
    cache: &'static str,
    messages: u64,
    reads: u64,
    elapsed_ns: u64,
    p50_ns: u64,
    p99_ns: u64,
    cpu_ns: Option<u64>,
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(e) => {
            eprintln!(
                "{e}\nusage: straum-store-bench --dir <dir> [--out file.csv] [--mode raw,hashcopy,sequential,parallel,inline] [--sizes 100,1024,10240] [--batches 1,8,64,512] [--reps 5] [--min-batches 100] [--max-batches 1000] [--max-bytes 1073741824] [--payload pattern|random|file:<path>] [--seed 1] [--label <text>] [--rep-offset 0] [--no-header] [--reads 0] [--read-seconds 5] [--read-cache default,off] [--reads-out reads.csv] [--clk-tck 100]"
            );
            std::process::exit(2);
        }
    };
    if let Err(e) = run(&args) {
        eprintln!("straum-store-bench: {e}");
        std::process::exit(1);
    }
}

fn open_out(path: Option<&PathBuf>) -> std::io::Result<Box<dyn Write>> {
    Ok(match path {
        Some(path) => Box::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)?,
        ),
        None => Box::new(std::io::stdout()),
    })
}

fn run(args: &Args) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(&args.dir)?;
    let mut out = open_out(args.out.as_ref())?;
    let mut reads_out = match (&args.reads_out, args.reads) {
        (_, 0) => None,
        (path, _) => Some(open_out(path.as_ref())?),
    };
    if args.header {
        writeln!(out, "{HEADER}")?;
        if let Some(r) = reads_out.as_mut() {
            writeln!(r, "{READ_HEADER}")?;
        }
    }
    let rt = compio::runtime::Runtime::new()?;
    eprintln!("compio driver: {:?}", rt.driver_type());
    let payload = args.payload.name();
    let cap = args
        .max_block_raw_bytes
        .map_or_else(String::new, |c| c.to_string());
    for mode in &args.modes {
        for &size in &args.sizes {
            for &batch in &args.batches {
                let batches = args.planned_batches(size, batch);
                let planned = batches * batch as u64 * (size as u64 + OVERHEAD_PER_MSG);
                let mut runs = Vec::with_capacity(args.reps);
                for rep in args.rep_offset + 1..=args.rep_offset + args.reps {
                    let dir = args.dir.join(format!("run-{mode}-{size}-{batch}-{rep}"));
                    let _ = std::fs::remove_dir_all(&dir);
                    check_free_space(&args.dir, planned)?;
                    std::fs::create_dir_all(&dir)?;
                    let write_mode = match mode.as_str() {
                        "sequential" => Some(WriteMode::Sequential),
                        "parallel" => Some(WriteMode::ParallelSync),
                        "inline" => Some(inline_mode(args)),
                        _ => None,
                    };
                    let mut source = Source::new(&args.payload, size, args.seed);
                    let mut r = rt.block_on(async {
                        match (mode.as_str(), write_mode) {
                            (_, Some(m)) => {
                                run_store(&dir, &mut source, batch, batches, m, args).await
                            }
                            ("raw", _) => {
                                run_raw(&dir, &mut source, batch, batches, false, args.clk_tck)
                                    .await
                            }
                            _ => {
                                run_raw(&dir, &mut source, batch, batches, true, args.clk_tck).await
                            }
                        }
                    })?;
                    r.disk_bytes = disk_bytes(&dir)?;
                    if let (Some(m), Some(rout)) = (write_mode, reads_out.as_mut()) {
                        rt.block_on(prepare_dedup(&dir, m, &mut source, batch, args))?;
                        for &cache in &args.read_caches {
                            for workload in Workload::ALL {
                                let rd =
                                    rt.block_on(run_reads(&dir, m, cache, workload, size, args))?;
                                writeln!(
                                    rout,
                                    "{},{payload},{mode},{size},{batch},{rep},{},{},{},{},{},{:.1},{},{},{}",
                                    args.label,
                                    rd.workload,
                                    rd.cache,
                                    rd.messages,
                                    rd.reads,
                                    rd.elapsed_ns,
                                    rd.reads as f64 / (rd.elapsed_ns as f64 / 1e9),
                                    rd.p50_ns,
                                    rd.p99_ns,
                                    rd.cpu_ns.map_or("NaN".into(), |c| format!(
                                        "{:.0}",
                                        c as f64 / rd.reads.max(1) as f64
                                    )),
                                )?;
                            }
                        }
                        rout.flush()?;
                    }
                    std::fs::remove_dir_all(&dir)?;
                    write_row(
                        &mut out,
                        &args.label,
                        &payload,
                        mode,
                        size,
                        batch,
                        &rep.to_string(),
                        &r.columns(),
                        None,
                        &cap,
                    )?;
                    out.flush()?;
                    runs.push(r);
                }
                let median: Vec<f64> = (0..COLUMNS)
                    .map(|c| median(runs.iter().map(|r| r.columns()[c]).collect()))
                    .collect();
                let rates: Vec<f64> = runs.iter().map(|r| r.columns()[3]).collect();
                let lo = rates.iter().copied().fold(f64::INFINITY, f64::min);
                let hi = rates.iter().copied().fold(0.0, f64::max);
                let spread = (hi - lo) / median[3] * 100.0;
                if args.reps > 1 {
                    write_row(
                        &mut out,
                        &args.label,
                        &payload,
                        mode,
                        size,
                        batch,
                        "median",
                        &median,
                        Some((lo, hi, spread)),
                        &cap,
                    )?;
                    out.flush()?;
                }
                eprintln!(
                    "{} {payload} {mode} size={size} batch={batch}: {batches} batches/run, median {:.0} msg/s, spread {spread:.1}%, {:.2} fsyncs/batch",
                    args.label, median[3], median[5]
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

/// Bytes allocated on disk (`st_blocks`) by the files under `dir`.
fn disk_bytes(dir: &Path) -> std::io::Result<u64> {
    let mut total = 0;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d)? {
            let entry = entry?;
            let meta = entry.metadata()?;
            if meta.is_dir() {
                stack.push(entry.path());
            } else {
                total += meta.blocks() * 512;
            }
        }
    }
    Ok(total)
}

/// User plus system CPU time of this process in nanoseconds, from fields 14
/// and 15 of `/proc/self/stat` (clock ticks). `None` without procfs.
fn cpu_ns(clk_tck: u64) -> Option<u64> {
    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    // The command name (field 2) may contain spaces; fields after it follow
    // the last ')'. Field 3 is the first of them.
    let mut fields = stat.get(stat.rfind(')')? + 1..)?.split_whitespace();
    let utime: u64 = fields.nth(11)?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some((utime + stime) * 1_000_000_000 / clk_tck.max(1))
}

/// Resets the peak RSS (`VmHWM`) to the current RSS (Linux 4.0 and later).
fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// `VmHWM` of this process in bytes. `None` without procfs.
fn peak_rss() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

async fn run_store(
    dir: &Path,
    source: &mut Source<'_>,
    batch: usize,
    batches: u64,
    write_mode: WriteMode,
    args: &Args,
) -> std::io::Result<Run> {
    let clk_tck = args.clk_tck;
    let messages = batches as usize * batch;
    let mut store = Store::open(dir, store_config(write_mode, args))
        .await
        .map_err(std::io::Error::other)?;
    let mut bufs = vec![Vec::new(); batch];
    let mut lat = Vec::with_capacity(batches as usize);
    let mut sent = 0;
    #[cfg(feature = "cap")]
    let mut timings = Vec::with_capacity(batches as usize);
    reset_peak_rss();
    let cpu_start = cpu_ns(clk_tck);
    while sent < messages {
        let n = batch;
        source.fill(&mut bufs[..n])?;
        let msgs: Vec<Message> = bufs[..n]
            .iter()
            .map(|p| Message {
                topic: TOPIC,
                payload: p,
                ..Default::default()
            })
            .collect();
        let start = Instant::now();
        store
            .put_batch(std::hint::black_box(&msgs), sent as u64)
            .await
            .map_err(std::io::Error::other)?;
        lat.push(start.elapsed().as_nanos() as u64);
        #[cfg(feature = "cap")]
        timings.push(store.last_commit_timing());
        sent += n;
    }
    let cpu = cpu_ns(clk_tck).zip(cpu_start).map(|(b, a)| b - a);
    let rss = peak_rss();
    let s = store.stats();
    let mut run = finish(
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
    );
    run.cpu_ns = cpu;
    run.peak_rss = rss;
    #[cfg(feature = "cap")]
    {
        let dur = |s: Option<(u64, u64)>| s.map(|(a, b)| b - a);
        let mut seg: Vec<u64> = timings.iter().filter_map(|t| dur(t.segment_sync)).collect();
        let mut wal: Vec<u64> = timings.iter().filter_map(|t| dur(t.wal_sync)).collect();
        seg.sort_unstable();
        wal.sort_unstable();
        let both = timings
            .iter()
            .filter(|t| t.segment_sync.is_some() && t.wal_sync.is_some())
            .count();
        let overlapped = timings.iter().filter(|t| t.overlapped()).count();
        let pct = |v: &[u64], p| {
            if v.is_empty() {
                f64::NAN
            } else {
                percentile(v, p) as f64
            }
        };
        run.syncs = Some([
            pct(&seg, 0.50),
            pct(&seg, 0.99),
            pct(&wal, 0.50),
            pct(&wal, 0.99),
            if both == 0 {
                f64::NAN
            } else {
                overlapped as f64 * 100.0 / both as f64
            },
        ]);
    }
    Ok(run)
}

async fn run_raw(
    dir: &Path,
    source: &mut Source<'_>,
    batch: usize,
    batches: u64,
    hash: bool,
    clk_tck: u64,
) -> std::io::Result<Run> {
    let messages = batches as usize * batch;
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.join("raw.dat"))
        .await?;
    let mut bufs = vec![Vec::new(); batch];
    let mut buf = Vec::with_capacity(batch * source.size);
    let mut lat = Vec::with_capacity(batches as usize);
    let (mut sent, mut pos, mut fsyncs) = (0, 0u64, 0);
    reset_peak_rss();
    let cpu_start = cpu_ns(clk_tck);
    while sent < messages {
        let n = batch;
        source.fill(&mut bufs[..n])?;
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
    let cpu = cpu_ns(clk_tck).zip(cpu_start).map(|(b, a)| b - a);
    let mut run = finish(lat, messages, fsyncs, pos, 0, (0, 0, 0));
    run.cpu_ns = cpu;
    run.peak_rss = peak_rss();
    Ok(run)
}

/// Writes `DEDUP_TOPIC` next to the measured topic, untimed: as many
/// messages as a read workload makes at most, in batches of `batch`; each
/// with probability one half repeats the body of a random earlier message
/// of `TOPIC` (stored once, in an old block), otherwise a fresh payload of
/// the run's source. Its sequential scan touches old blocks at random.
async fn prepare_dedup(
    dir: &Path,
    write_mode: WriteMode,
    source: &mut Source<'_>,
    batch: usize,
    args: &Args,
) -> std::io::Result<()> {
    let mut store = Store::open(dir, store_config(write_mode, args))
        .await
        .map_err(std::io::Error::other)?;
    let (first, next) = store
        .topic_range(TOPIC)
        .ok_or_else(|| std::io::Error::other("no topic to repeat"))?;
    let n = args.reads.min(next - first) as usize;
    let mut rng = XorShift::new(args.seed ^ 0xd5d5);
    let mut fresh = vec![Vec::new(); 1];
    let mut written = 0;
    while written < n {
        let mut bodies = Vec::with_capacity(batch);
        for _ in 0..batch.min(n - written) {
            if rng.next().is_multiple_of(2) {
                let seq = first + rng.below(next - first);
                let body = store
                    .read(TOPIC, seq)
                    .await
                    .map_err(std::io::Error::other)?
                    .ok_or_else(|| std::io::Error::other("a repeated message is gone"))?;
                bodies.push(body);
            } else {
                source.fill(&mut fresh)?;
                bodies.push(fresh[0].clone());
            }
        }
        let msgs: Vec<Message> = bodies
            .iter()
            .map(|p| Message {
                topic: DEDUP_TOPIC,
                payload: p,
                ..Default::default()
            })
            .collect();
        store
            .put_batch(&msgs, written as u64)
            .await
            .map_err(std::io::Error::other)?;
        written += msgs.len();
    }
    Ok(())
}

/// Reopens the store in `dir` and runs one read workload, up to `--reads`
/// reads or `--read-seconds`: `scan` reads `TOPIC` in `topic_seq` order;
/// `random` reads random `topic_seq`s; `dedup_scan` reads `DEDUP_TOPIC` in
/// order; `multi` reads `TOPIC` with `CONSUMERS` cursors starting at even
/// intervals, one read each in turn. Every read must return a payload of
/// `size` bytes.
async fn run_reads(
    dir: &Path,
    write_mode: WriteMode,
    cache: ReadCache,
    workload: Workload,
    size: usize,
    args: &Args,
) -> std::io::Result<Reads> {
    let store = Store::open(dir, cache.config(write_mode, args))
        .await
        .map_err(std::io::Error::other)?;
    let topic = if workload == Workload::DedupScan {
        DEDUP_TOPIC
    } else {
        TOPIC
    };
    let (first, next) = store
        .topic_range(topic)
        .ok_or_else(|| std::io::Error::other("no topic to read"))?;
    let messages = next - first;
    let mut rng = XorShift::new(args.seed);
    let mut cursors: Vec<u64> = (0..CONSUMERS)
        .map(|k| first + messages * k / CONSUMERS)
        .collect();
    let mut lat = Vec::with_capacity(args.reads.min(messages) as usize);
    let cpu_start = cpu_ns(args.clk_tck);
    let began = Instant::now();
    for i in 0..args.reads.min(messages) {
        if began.elapsed() >= args.read_time {
            break;
        }
        let seq = match workload {
            Workload::Random => first + rng.below(messages),
            Workload::Multi => {
                let c = &mut cursors[(i % CONSUMERS) as usize];
                let seq = *c;
                *c = if *c + 1 >= next { first } else { *c + 1 };
                seq
            }
            Workload::Scan | Workload::DedupScan => first + i,
        };
        let start = Instant::now();
        let got = store
            .read(topic, std::hint::black_box(seq))
            .await
            .map_err(std::io::Error::other)?;
        lat.push(start.elapsed().as_nanos() as u64);
        if got.as_ref().map(Vec::len) != Some(size) {
            return Err(std::io::Error::other(format!(
                "read of {seq} returned {:?} bytes, expected {size}",
                got.map(|g| g.len())
            )));
        }
    }
    let cpu = cpu_ns(args.clk_tck).zip(cpu_start).map(|(b, a)| b - a);
    let reads = lat.len() as u64;
    let elapsed_ns = lat.iter().sum();
    lat.sort_unstable();
    Ok(Reads {
        workload: workload.name(),
        cache: cache.name(),
        messages,
        reads,
        elapsed_ns,
        p50_ns: percentile(&lat, 0.50),
        p99_ns: percentile(&lat, 0.99),
        cpu_ns: cpu,
    })
}

/// Nearest-rank percentile of sorted `v`; 0 for an empty one.
fn percentile(v: &[u64], p: f64) -> u64 {
    v.get(((v.len() as f64 - 1.0) * p).round() as usize)
        .copied()
        .unwrap_or(0)
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
    Run {
        messages: messages as u64,
        batches,
        elapsed_ns,
        fsyncs,
        bytes_written,
        wal_bytes,
        p50_ns: percentile(&lat, 0.50),
        p99_ns: percentile(&lat, 0.99),
        dedup_msgs,
        bytes_saved,
        meta_bytes,
        ..Run::default()
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

#[allow(clippy::too_many_arguments)]
fn write_row(
    out: &mut dyn Write,
    label: &str,
    payload: &str,
    mode: &str,
    size: usize,
    batch: usize,
    rep: &str,
    c: &[f64],
    spread: Option<(f64, f64, f64)>,
    cap: &str,
) -> std::io::Result<()> {
    let spread = match spread {
        Some((lo, hi, pct)) => format!("{lo:.1},{hi:.1},{pct:.1}"),
        None => ",,".to_owned(),
    };
    writeln!(
        out,
        "{label},{payload},{mode},{size},{batch},{rep},{:.0},{:.0},{:.0},{:.1},{:.0},{:.3},{:.0},{:.1},{:.1},{:.1},{:.0},{:.0},{:.0},{:.0},{spread},{:.0},{:.0},{:.1},{:.0},{:.0},{:.0},{:.0},{:.1},{cap}",
        c[0],
        c[1],
        c[2],
        c[3],
        c[4],
        c[5],
        c[6],
        c[7],
        c[8],
        c[9],
        c[10],
        c[11],
        c[12],
        c[13],
        c[14],
        c[15],
        c[16],
        c[17],
        c[18],
        c[19],
        c[20],
        c[21]
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
        payload: Payload::Pattern,
        seed: 1,
        label: String::new(),
        rep_offset: 0,
        header: true,
        reads: 0,
        read_time: Duration::from_secs(5),
        read_caches: vec![ReadCache::Default],
        reads_out: None,
        clk_tck: 100,
        max_block_raw_bytes: None,
    };
    let list = |v: &str| -> Result<Vec<usize>, String> {
        v.split(',')
            .map(|x| x.parse().map_err(|_| format!("bad number: {x}")))
            .collect()
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        if flag == "--no-header" {
            args.header = false;
            continue;
        }
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
            "--payload" => {
                args.payload = match value.as_str() {
                    "pattern" => Payload::Pattern,
                    "random" => Payload::Random,
                    v => {
                        let path = PathBuf::from(
                            v.strip_prefix("file:")
                                .ok_or_else(|| format!("unknown payload {v}"))?,
                        );
                        let file = std::fs::File::open(&path)
                            .map_err(|e| format!("{}: {e}", path.display()))?;
                        let len = file.metadata().map_err(|e| e.to_string())?.len();
                        Payload::File { path, file, len }
                    }
                }
            }
            "--seed" => args.seed = value.parse().map_err(|_| "bad --seed")?,
            "--label" => args.label = value,
            "--rep-offset" => args.rep_offset = value.parse().map_err(|_| "bad --rep-offset")?,
            "--reads" => args.reads = value.parse().map_err(|_| "bad --reads")?,
            "--read-seconds" => {
                args.read_time =
                    Duration::from_secs_f64(value.parse().map_err(|_| "bad --read-seconds")?)
            }
            "--read-cache" => {
                args.read_caches = value
                    .split(',')
                    .map(|c| match c {
                        "default" => Ok(ReadCache::Default),
                        #[cfg(feature = "blocks")]
                        "off" => Ok(ReadCache::Off),
                        _ => Err(format!("unknown or unsupported --read-cache {c}")),
                    })
                    .collect::<Result<_, _>>()?
            }
            "--reads-out" => args.reads_out = Some(value.into()),
            "--clk-tck" => args.clk_tck = value.parse().map_err(|_| "bad --clk-tck")?,
            #[cfg(feature = "cap")]
            "--max-block-raw-bytes" => {
                args.max_block_raw_bytes =
                    Some(value.parse().map_err(|_| "bad --max-block-raw-bytes")?)
            }
            _ => return Err(format!("unknown flag {flag}")),
        }
    }
    if args.dir.as_os_str().is_empty() {
        return Err("--dir is required".into());
    }
    if args.reps == 0
        || args.min_batches == 0
        || args.batches.contains(&0)
        || args.sizes.contains(&0)
    {
        return Err("--reps, --min-batches, sizes and batch sizes must be positive".into());
    }
    if let Payload::File { len, path, .. } = &args.payload
        && args.sizes.iter().any(|&s| s as u64 + s as u64 > *len)
    {
        return Err(format!(
            "{} is too short: it needs at least twice the largest message size",
            path.display()
        ));
    }
    Ok(args)
}
