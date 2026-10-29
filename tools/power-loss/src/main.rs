//! Writer and verifier processes of the power-loss harness. The driver,
//! `scripts/power-loss/run.sh`, mounts LazyFS, runs a writer, injects a
//! fault, remounts and runs a verifier; this binary holds every piece that
//! needs the store or the client:
//!
//! ```text
//! plan          --scenario S --seed N --iterations K --store-root P
//! store-write   --dir D --mode M --seed N [--compact-every C]
//!               [--arm-at-compaction K --fifo F --fault CMD]
//!               [--dict-train-bytes B [--arm-at-train --fifo F --fault CMD]]
//! store-verify  --dir D --mode M --logs DIR [--dict-train-bytes B]
//! broker-produce --addr A --base N --seed N
//! broker-verify --broker BIN --data-dir D --mode M --logs DIR --stderr FILE
//! free-addr     (prints `RESULT ok 127.0.0.1:<free port>`)
//! ```
//!
//! Writers print one line per event to stdout, which the driver keeps
//! outside the LazyFS mount: `TRY` before a batch is handed to the store or
//! broker, `ACK` only after it was acknowledged. Message contents are pure
//! functions of the writer's seed and the line's numbers, so a verifier
//! regenerates the exact bytes of every acknowledged message from the logs.
//! Verifiers exit 0 when every check passed, 1 otherwise, and print one
//! `RESULT` line.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::time::Duration;

use straum_client::proto::{ErrorCode, Header, Message as ClientMessage};
use straum_client::{Client, ClientConfig, ProduceOptions};
use straum_store::{Config, Message, ObjectId, Store, WriteMode};

/// Released and compacted by the writer; its messages are not tracked.
const SCRATCH: &[u8] = b"scratch";
const ORDERS: &[u8] = b"orders";
const EVENTS: &[u8] = b"events";
const BROKER_TOPIC: &str = "powerloss";

/// xorshift64* seeded through splitmix64: the only source of randomness.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(splitmix(seed) | 1)
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

fn splitmix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

fn mix(a: u64, b: u64) -> u64 {
    splitmix(a ^ splitmix(b))
}

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        eprintln!(
            "usage: straum-power-loss <plan|store-write|store-verify|broker-produce|broker-verify|free-addr> --key value ..."
        );
        return ExitCode::from(2);
    };
    let rest: Vec<String> = args.collect();
    let opts: HashMap<&str, &str> = rest
        .chunks(2)
        .filter_map(|p| Some((p[0].strip_prefix("--")?, p.get(1)?.as_str())))
        .collect();
    let res = match cmd.as_str() {
        "plan" => plan(&opts),
        "store-write" => store_write(&opts),
        "store-verify" => store_verify(&opts),
        "broker-produce" => broker_produce(&opts),
        "broker-verify" => broker_verify(&opts),
        "free-addr" => straum_conformance::broker::free_addr(),
        _ => Err(format!("unknown command {cmd}")),
    };
    match res {
        Ok(summary) => {
            println!("RESULT ok {summary}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            println!("RESULT FAIL {e}");
            ExitCode::FAILURE
        }
    }
}

fn opt<'a>(opts: &HashMap<&str, &'a str>, key: &str) -> Result<&'a str, String> {
    opts.get(key).copied().ok_or(format!("--{key} is required"))
}

fn num(opts: &HashMap<&str, &str>, key: &str) -> Result<u64, String> {
    opt(opts, key)?
        .parse()
        .map_err(|_| format!("--{key}: not a number"))
}

// ---------------------------------------------------------------- plan

/// LazyFS crash points: `lazyfs::crash` kills LazyFS at the first matching
/// operation after the command arrives, dropping its cache.
const CRASH_POINTS: [(&str, &str); 8] = [
    ("write", r"wal-.*\.log$"),
    ("fsync", r"wal-.*\.log$"),
    ("write", "seg-"),
    ("fsync", "seg-"),
    ("write", "idx-"),
    ("create", "seg-"),
    ("unlink", "seg-"),
    // A new WAL file, created when the previous one is full.
    ("create", "wal-"),
];

/// One line per iteration: `<i> <delay_ms> <writer_seed> <kind> <command>
/// <arm_at_compaction>`. Each iteration draws from its own generator, so an
/// iteration is reproduced from the seed and its number alone.
fn plan(opts: &HashMap<&str, &str>) -> Result<String, String> {
    let scenario = opt(opts, "scenario")?;
    let seed = num(opts, "seed")?;
    let iterations = num(opts, "iterations")?;
    let root = opt(opts, "store-root")?;
    for i in 0..iterations {
        let mut rng = Rng::new(mix(seed, i));
        let writer_seed = rng.next();
        let delay = rng.below(300);
        // The writer arms these faults itself, at a compaction or at the
        // batch that trains the dictionary.
        let compaction = matches!(scenario, "compaction" | "dictionary");
        let kind = match rng.below(3) {
            0 if !compaction => "clear",
            0 | 1 => "crash",
            _ => "torn",
        };
        let command = match kind {
            "clear" => "lazyfs::clear-cache".to_owned(),
            "crash" => {
                let points = match scenario {
                    // Operations a compaction or a training batch issues.
                    "compaction" | "dictionary" => &CRASH_POINTS[..4],
                    // The broker never compacts: no segment is unlinked.
                    "broker" => &CRASH_POINTS[..6],
                    _ => &CRASH_POINTS[..],
                };
                let (op, rgx) = points[rng.below(points.len() as u64) as usize];
                let timing = ["before", "after"][rng.below(2) as usize];
                format!("lazyfs::crash::timing={timing}::op={op}::from_rgx={rgx}")
            }
            _ => {
                // Torn WAL writes twice as often as torn index writes; a
                // compaction writes no index. `@WAL@` is the WAL file being
                // appended to when the fault is armed; `@IDX@` the index
                // segment the writer will append to, named by run.sh
                // before the writer starts.
                let file = if compaction || rng.below(3) > 0 {
                    "@WAL@"
                } else {
                    "@IDX@"
                };
                let parts = 2 + rng.below(3);
                let persist: Vec<String> = (1..=parts)
                    .filter(|_| rng.below(2) == 0)
                    .map(|p| p.to_string())
                    .collect();
                let persist = if persist.is_empty() {
                    (1 + rng.below(parts)).to_string()
                } else {
                    persist.join(",")
                };
                format!("lazyfs::torn-op::file={root}/{file}::parts={parts}::persist={persist}")
            }
        };
        let arm_at = 1 + rng.below(4);
        println!("{i} {delay} {writer_seed} {kind} {command} {arm_at}");
    }
    Ok(format!("{iterations} iterations"))
}

// ---------------------------------------------------------------- store

fn store_config(mode: &str, opts: &HashMap<&str, &str>) -> Result<Config, String> {
    let write_mode = match mode {
        "sequential" => WriteMode::Sequential,
        "parallel" => WriteMode::ParallelSync,
        // Below most payloads (up to ~3 KiB): inline and segment objects mix.
        "inline" => WriteMode::InlineSmall { threshold: 1024 },
        _ => return Err(format!("unknown write mode {mode}")),
    };
    let dict_train_bytes = match opts.get("dict-train-bytes") {
        Some(_) => num(opts, "dict-train-bytes")?,
        None => Config::default().dict_train_bytes,
    };
    Ok(Config {
        // Small segments and WAL files: rotation, sealing mid-batch and
        // compaction happen within one iteration.
        segment_max_bytes: 64 << 10,
        wal_file_max_bytes: 64 << 10,
        write_mode,
        dict_train_bytes,
        ..Config::default()
    })
}

fn batch_len(seed: u64, round: u64) -> u64 {
    1 + Rng::new(mix(seed, round)).below(16)
}

/// Message `j` of `round` of the writer started with `seed`.
fn store_message(seed: u64, round: u64, j: u64) -> (&'static [u8], Vec<u8>) {
    let mut rng = Rng::new(mix(mix(seed, round), j + 1));
    let topic = [SCRATCH, ORDERS, EVENTS][rng.below(3) as usize];
    let payload = if rng.below(3) == 0 {
        // Eight shared bodies: deduplication across batches and crashes.
        let i = rng.below(8) as u8;
        vec![i; 1 + 97 * usize::from(i)]
    } else {
        let len = 16 + rng.below(3000);
        (0..len).map(|_| rng.next() as u8).collect()
    };
    (topic, payload)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Writes batches until killed. `--arm-at-compaction K` writes `--fault`
/// to the LazyFS fault FIFO just before the K-th compaction.
#[expect(
    clippy::unwrap_used,
    reason = "a store error after the fault ends the writer; the verifier judges the state"
)]
fn store_write(opts: &HashMap<&str, &str>) -> Result<String, String> {
    let dir = opt(opts, "dir")?;
    let mode = opt(opts, "mode")?;
    let seed = num(opts, "seed")?;
    let compact_every = opts
        .get("compact-every")
        .map_or(Ok(3), |_| num(opts, "compact-every"))?;
    let arm_at = opts
        .get("arm-at-compaction")
        .map_or(Ok(0), |_| num(opts, "arm-at-compaction"))?;
    let arm_at_train = opts.contains_key("arm-at-train");
    let config = store_config(mode, opts)?;
    println!("SEED {seed} END");
    let rt = compio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async {
        let mut store = Store::open(dir, config).await.unwrap();
        println!("READY END");
        let mut compactions = 0;
        let mut round = 0;
        loop {
            round += 1;
            let msgs: Vec<_> = (0..batch_len(seed, round))
                .map(|j| store_message(seed, round, j))
                .collect();
            let batch: Vec<Message> = msgs
                .iter()
                .map(|(topic, payload)| Message {
                    topic,
                    payload,
                    ..Default::default()
                })
                .collect();
            let now = now_ms();
            if store.trains_next_batch() {
                // The fault, if armed here, hits the batch that trains.
                if arm_at_train {
                    arm(opts);
                    println!("ARMED {round} END");
                }
                println!("TRAIN {round} END");
            }
            println!("TRY {round} END");
            let seqs = store.put_batch(&batch, now).await.unwrap();
            // An ACK line is a durability promise: printed only after
            // put_batch returned. " END" marks a line not cut by a kill.
            for (j, ((topic, _), seq)) in msgs.iter().zip(seqs).enumerate() {
                if *topic != SCRATCH {
                    let topic = std::str::from_utf8(topic).unwrap();
                    println!("ACK {round} {j} {topic} {seq} END");
                }
            }
            if round % 2 == 1 {
                store.release_before(SCRATCH, now).await.unwrap();
            }
            if round % compact_every == 0 {
                compactions += 1;
                if compactions == arm_at {
                    arm(opts);
                    println!("ARMED {round} END");
                }
                println!("COMPACT-START {round} END");
                let stats = store.compact().await.unwrap();
                println!("COMPACTED {} END", stats.segments_rewritten);
            }
        }
    });
    Ok(String::new())
}

/// The newest WAL file of the store in `dir`: the one being appended to.
fn last_wal_file(dir: &Path) -> Option<String> {
    std::fs::read_dir(dir)
        .ok()?
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter(|n| wal_file_id(n).is_some())
        .max()
}

/// The first frame seq of a WAL file `wal-<16 hex digits>.log`.
fn wal_file_id(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    (hex.len() == 16).then(|| u64::from_str_radix(hex, 16).ok())?
}

/// Writes `--fault` to the LazyFS fault FIFO `--fifo`; `@WAL@` in it
/// becomes the WAL file being appended to.
#[expect(
    clippy::unwrap_used,
    reason = "a harness error ends the writer; run.sh reports the iteration"
)]
fn arm(opts: &HashMap<&str, &str>) {
    let fifo = opt(opts, "fifo").unwrap();
    let wal = last_wal_file(Path::new(opt(opts, "dir").unwrap())).unwrap();
    let fault = opt(opts, "fault").unwrap().replace("@WAL@", &wal);
    std::fs::OpenOptions::new()
        .write(true)
        .open(fifo)
        .and_then(|mut f| writeln!(f, "{fault}"))
        .unwrap();
    // LazyFS reads the FIFO on its own thread.
    std::thread::sleep(Duration::from_millis(200));
}

/// What the writer logs of one scenario say: every acknowledged message and
/// the rounds that may be on disk without an acknowledgement.
#[derive(Default)]
struct StoreLog {
    /// (topic, seq) → (writer seed, round, j).
    acked: HashMap<(Vec<u8>, u64), (u64, u64, u64)>,
    /// The last round each writer handed to the store: at most one batch
    /// per writer can be durable without its ACK lines.
    unacked: Vec<(u64, u64)>,
    /// The round whose batch trained the dictionary, and the last round
    /// acknowledged, of the last writer log that has a `TRAIN` line.
    train: Option<(u64, u64)>,
}

/// `iter-*/<name>` under `logs`, in iteration order.
fn log_files(logs: &str, name: &str) -> Result<Vec<PathBuf>, String> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(logs)
        .map_err(|e| format!("{logs}: {e}"))?
        .filter_map(Result::ok)
        .map(|e| e.path().join(name))
        .filter(|p| p.is_file())
        .collect();
    files.sort();
    Ok(files)
}

/// Complete lines of a log: a line cut by the kill has no " END".
fn log_lines(path: &Path) -> Result<Vec<Vec<String>>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(text
        .lines()
        .filter_map(|l| l.strip_suffix(" END"))
        .map(|l| l.split(' ').map(str::to_owned).collect())
        .collect())
}

fn parse_store_logs(logs: &str) -> Result<StoreLog, String> {
    let mut log = StoreLog::default();
    for path in log_files(logs, "writer.log")? {
        let mut seed = None;
        let mut last_try = None;
        let (mut trained, mut last_ack) = (None, 0);
        for f in log_lines(&path)? {
            let n = |i: usize| -> Result<u64, String> {
                f.get(i)
                    .and_then(|v| v.parse().ok())
                    .ok_or(format!("{}: malformed line {f:?}", path.display()))
            };
            match f[0].as_str() {
                "SEED" => seed = Some(n(1)?),
                "TRY" => last_try = Some(n(1)?),
                "TRAIN" => trained = Some(n(1)?),
                "ACK" => {
                    let seed = seed.ok_or("ACK before SEED")?;
                    let (round, j, seq) = (n(1)?, n(2)?, n(4)?);
                    last_ack = last_ack.max(round);
                    let key = (f[3].clone().into_bytes(), seq);
                    if let Some(prev) = log.acked.insert(key, (seed, round, j))
                        && store_message(prev.0, prev.1, prev.2).1
                            != store_message(seed, round, j).1
                    {
                        return Err(format!(
                            "{}: {}/{seq} acknowledged twice with different bytes (a seq reassigned after its acknowledgement)",
                            path.display(),
                            f[3]
                        ));
                    }
                }
                _ => {}
            }
        }
        if let (Some(seed), Some(round)) = (seed, last_try) {
            log.unacked.push((seed, round));
        }
        if let Some(round) = trained {
            log.train = Some((round, last_ack));
        }
    }
    Ok(log)
}

/// Every file under a store directory: relative path → (length, BLAKE3),
/// plus the WAL files' bytes, to check what recovery cut.
struct Snapshot {
    files: BTreeMap<String, (u64, [u8; 32])>,
    wal: HashMap<String, Vec<u8>>,
}

fn snapshot(dir: &Path) -> Result<Snapshot, String> {
    let mut files = BTreeMap::new();
    let mut wal = HashMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let entries = match std::fs::read_dir(&d) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
            Err(e) => return Err(format!("{}: {e}", d.display())),
        };
        for entry in entries {
            let path = entry.map_err(|e| e.to_string())?.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let bytes = std::fs::read(&path).map_err(|e| format!("{}: {e}", path.display()))?;
            let rel = path
                .strip_prefix(dir)
                .map_err(|e| e.to_string())?
                .to_string_lossy()
                .into_owned();
            files.insert(
                rel.clone(),
                (bytes.len() as u64, *blake3::hash(&bytes).as_bytes()),
            );
            if wal_file_id(&rel).is_some() {
                wal.insert(rel, bytes);
            }
        }
    }
    Ok(Snapshot { files, wal })
}

/// `name { field: value, ... }` of a recovery action in Debug form.
fn action_name(a: &str) -> &str {
    a.split([' ', '{']).next().unwrap_or("")
}

fn action_field<'a>(a: &'a str, field: &str) -> Option<&'a str> {
    let start = a.find(&format!("{field}: "))? + field.len() + 2;
    let rest = &a[start..];
    let end = rest.find([',', '}']).unwrap_or(rest.len());
    Some(rest[..end].trim().trim_matches('"'))
}

/// The report holds only the changes `docs/guarantees.md` §3 lists, and
/// `FrameDropped` only in ParallelSync.
fn check_report(actions: &[String], mode: &str) -> Result<(), String> {
    const ALLOWED: [&str; 7] = [
        "WalTmpRemoved",
        "TornTailTruncated",
        "FrameDropped",
        "OrphanSegmentRemoved",
        "IndexSegmentRemoved",
        "StaleIndexRecordsFenced",
        "TopicIndexRepaired",
    ];
    let mut cuts = 0;
    for a in actions {
        let name = action_name(a);
        if !ALLOWED.contains(&name) {
            return Err(format!("undocumented recovery action: {a}"));
        }
        if name == "FrameDropped" && mode != "parallel" {
            return Err(format!("FrameDropped outside ParallelSync: {a}"));
        }
        if name == "TornTailTruncated" || name == "FrameDropped" {
            cuts += 1;
        }
    }
    if cuts > 1 {
        return Err(format!(
            "more than one WAL cut in one recovery: {actions:?}"
        ));
    }
    Ok(())
}

/// The id of an index segment file `idx-<16 hex digits>.dat`.
fn index_segment_id(name: &str) -> Option<u64> {
    let hex = name.strip_prefix("idx-")?.strip_suffix(".dat")?;
    (hex.len() == 16).then(|| u64::from_str_radix(hex, 16).ok())?
}

/// Every difference between the snapshots before and after recovery is
/// listed in the report, and every report entry is a difference on disk.
/// Anything else is a silent repair. Index segments: recovery never
/// changes an existing one, it may only delete it (`IndexSegmentRemoved`);
/// a new one holds a fence (`StaleIndexRecordsFenced`) or the blocks and
/// seek pages of `TopicIndexRepaired`, which all go to new segments.
fn check_changes(before: &Snapshot, after: &Snapshot, actions: &[String]) -> Result<(), String> {
    let mut explained = vec![false; actions.len()];
    let mut explain = |pred: &dyn Fn(&str) -> bool, what: &str| -> Result<(), String> {
        let i = actions
            .iter()
            .enumerate()
            .position(|(i, a)| !explained[i] && pred(a))
            .ok_or(format!(
                "silent repair: {what} changed with no report entry"
            ))?;
        explained[i] = true;
        Ok(())
    };
    let keys: HashSet<&String> = before.files.keys().chain(after.files.keys()).collect();
    let mut new_index = Vec::new();
    for rel in keys {
        let (b, a) = (before.files.get(rel), after.files.get(rel));
        if b == a {
            continue;
        }
        let name = rel.rsplit('/').next().unwrap_or(rel);
        match (rel.as_str(), b, a) {
            (_, Some(b), Some(a)) if wal_file_id(rel).is_some() => {
                let cut = a.0 as usize;
                let bytes = before.wal.get(rel).map_or(&[][..], Vec::as_slice);
                if a.0 >= b.0 || *blake3::hash(&bytes[..cut]).as_bytes() != a.1 {
                    return Err(format!(
                        "silent repair: {rel} changed other than by a cut ({} → {} bytes)",
                        b.0, a.0
                    ));
                }
                let file = wal_file_id(rel).unwrap_or_default().to_string();
                let removed = (b.0 - a.0).to_string();
                let offset = a.0.to_string();
                explain(
                    &|x| {
                        matches!(action_name(x), "TornTailTruncated" | "FrameDropped")
                            && action_field(x, "file") == Some(file.as_str())
                            && action_field(x, "offset") == Some(offset.as_str())
                            && action_field(x, "bytes_removed") == Some(removed.as_str())
                    },
                    &format!("{rel} (cut)"),
                )?;
            }
            (_, Some(_), None) if name.strip_suffix(".tmp").and_then(wal_file_id).is_some() => {
                explain(&|x| action_name(x) == "WalTmpRemoved", rel)?;
            }
            (_, Some(_), None) if name.starts_with("seg-") => {
                let id = name
                    .strip_prefix("seg-")
                    .and_then(|n| n.strip_suffix(".dat"))
                    .and_then(|n| u64::from_str_radix(n, 16).ok())
                    .ok_or(format!("unexpected file {rel}"))?
                    .to_string();
                explain(
                    &|x| {
                        action_name(x) == "OrphanSegmentRemoved"
                            && action_field(x, "segment") == Some(id.as_str())
                    },
                    rel,
                )?;
            }
            (_, Some(b), None) if index_segment_id(name).is_some() => {
                let id = index_segment_id(name).unwrap_or_default().to_string();
                let bytes = b.0.to_string();
                explain(
                    &|x| {
                        action_name(x) == "IndexSegmentRemoved"
                            && action_field(x, "segment") == Some(id.as_str())
                            && action_field(x, "bytes") == Some(bytes.as_str())
                    },
                    rel,
                )?;
            }
            (_, None, Some(_)) if index_segment_id(name).is_some() => {
                new_index.push((rel.clone(), index_segment_id(name).unwrap_or_default()));
            }
            _ => return Err(format!("silent repair: {rel} changed ({b:?} → {a:?})")),
        }
    }
    // A new index segment is a fence or holds repaired records; repaired
    // records need a new segment.
    let repaired: Vec<usize> = (0..actions.len())
        .filter(|&i| action_name(&actions[i]) == "TopicIndexRepaired")
        .collect();
    for (rel, id) in &new_index {
        let id = id.to_string();
        let fence = actions.iter().position(|x| {
            action_name(x) == "StaleIndexRecordsFenced"
                && action_field(x, "segment") == Some(id.as_str())
        });
        match fence {
            Some(i) => explained[i] = true,
            None if !repaired.is_empty() => {}
            None => return Err(format!("silent repair: {rel} created with no report entry")),
        }
    }
    if !new_index.is_empty() {
        for i in repaired {
            explained[i] = true;
        }
    }
    if let Some(i) = explained.iter().position(|e| !e) {
        return Err(format!(
            "report entry with no matching change on disk: {}",
            actions[i]
        ));
    }
    Ok(())
}

fn store_verify(opts: &HashMap<&str, &str>) -> Result<String, String> {
    let dir = PathBuf::from(opt(opts, "dir")?);
    let mode = opt(opts, "mode")?;
    let config = store_config(mode, opts)?;
    let log = parse_store_logs(opt(opts, "logs")?)?;
    if last_wal_file(&dir).is_none() {
        return if log.acked.is_empty() {
            Ok("acked=0 (no store yet)".to_owned())
        } else {
            Err(format!(
                "no WAL file with {} acknowledged messages",
                log.acked.len()
            ))
        };
    }
    // Bodies of messages that may be on disk without an acknowledgement.
    let mut candidates: HashMap<ObjectId, HashSet<&[u8]>> = HashMap::new();
    for &(seed, round) in &log.unacked {
        for j in 0..batch_len(seed, round) {
            let (topic, payload) = store_message(seed, round, j);
            candidates
                .entry(ObjectId::of(&payload))
                .or_default()
                .insert(topic);
        }
    }
    let before = snapshot(&dir)?;
    let rt = compio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    rt.block_on(async {
        let store = Store::open(&dir, config)
            .await
            .map_err(|e| format!("recovery refused to open the store: {e}"))?;
        let actions: Vec<String> = store
            .recovery_report()
            .actions
            .iter()
            .map(|a| format!("{a:?}"))
            .collect();
        for a in &actions {
            println!("RECOVERY {a}");
        }
        check_report(&actions, mode)?;

        // Every acknowledged message, with its exact bytes.
        for ((topic, seq), &(seed, round, j)) in &log.acked {
            let want = store_message(seed, round, j).1;
            let t = String::from_utf8_lossy(topic);
            match store.read(topic, *seq).await {
                Ok(Some(got)) if got == want => {}
                Ok(Some(_)) => return Err(format!("acknowledged {t}/{seq}: other bytes")),
                Ok(None) => return Err(format!("acknowledged {t}/{seq}: lost")),
                Err(e) => return Err(format!("acknowledged {t}/{seq}: {e}")),
            }
        }

        // Every live entry resolves to its object with matching bytes,
        // refcounts equal the live entries per object, and a tracked entry
        // that was never acknowledged holds a body that writer did send.
        let mut counts: HashMap<ObjectId, u64> = HashMap::new();
        let mut unacked_present = 0;
        let topics: Vec<Vec<u8>> = store.topics().map(<[u8]>::to_vec).collect();
        for topic in &topics {
            let t = String::from_utf8_lossy(topic);
            let (first, next) = store.topic_range(topic).ok_or("topic without a range")?;
            for seq in first..next {
                let id = store
                    .entry(topic, seq)
                    .await
                    .map_err(|e| format!("{t}/{seq}: {e}"))?
                    .ok_or(format!("{t}/{seq}: no entry"))?;
                let bytes = store
                    .read(topic, seq)
                    .await
                    .map_err(|e| format!("{t}/{seq}: {e}"))?
                    .ok_or(format!("{t}/{seq}: live entry unreadable"))?;
                if ObjectId::of(&bytes) != id {
                    return Err(format!("{t}/{seq}: bytes do not match the entry's id"));
                }
                *counts.entry(id).or_default() += 1;
                if topic != SCRATCH && !log.acked.contains_key(&(topic.clone(), seq)) {
                    if !candidates.get(&id).is_some_and(|ts| ts.contains(topic.as_slice())) {
                        return Err(format!(
                            "{t}/{seq}: neither acknowledged nor in a batch the writer had in flight"
                        ));
                    }
                    unacked_present += 1;
                }
            }
        }
        for (id, n) in &counts {
            if store.refcount(id) != Some(*n) {
                return Err(format!("refcount of {id:?}: {:?}, live entries {n}", store.refcount(id)));
            }
        }
        // An acknowledged training batch, or one after it, makes the
        // dictionary durable: recovery must find it in the WAL.
        if let Some((train, last_ack)) = log.train
            && last_ack >= train
            && store.dictionary_bytes().is_none()
        {
            return Err(format!(
                "round {last_ack} acknowledged after the training round {train}, but no dictionary after recovery"
            ));
        }
        drop(store);

        let after = snapshot(&dir)?;
        check_changes(&before, &after, &actions)?;

        // Recovery leaves nothing for a second recovery to do.
        let again = Store::open(&dir, config)
            .await
            .map_err(|e| format!("second open after recovery: {e}"))?;
        if !again.recovery_report().is_empty() {
            return Err(format!(
                "second open after recovery changed the store again: {:?}",
                again.recovery_report().actions
            ));
        }
        Ok(format!(
            "acked={} unacked_present={unacked_present} actions={} [{}]",
            log.acked.len(),
            actions.len(),
            actions.iter().map(|a| action_name(a)).collect::<Vec<_>>().join(",")
        ))
    })
}

// ---------------------------------------------------------------- broker

/// Message `n` of the run: key, headers and body follow from `n`.
fn broker_message(n: u64) -> ClientMessage {
    let key = (!n.is_multiple_of(3)).then(|| format!("key-{}", n % 7).into_bytes());
    let headers = (0..n % 3)
        .map(|i| Header {
            name: format!("h{i}").into_bytes(),
            value: n.to_le_bytes()[..(i as usize + 1)].to_vec(),
        })
        .collect();
    // Repeated bodies exercise deduplication across crashes.
    let body = if n.is_multiple_of(5) {
        b"repeated body".to_vec()
    } else {
        format!("body {n} {}", "x".repeat((n % 64) as usize)).into_bytes()
    };
    ClientMessage { key, headers, body }
}

/// Two producers with durable acks until the broker goes away.
fn broker_produce(opts: &HashMap<&str, &str>) -> Result<String, String> {
    let addr = opt(opts, "addr")?;
    let base = num(opts, "base")?;
    let mut rng = Rng::new(num(opts, "seed")?);
    let mut config = ClientConfig::new(addr);
    // A retried Produce may be stored twice (guarantees.md §1): no retries.
    config.max_attempts = 1;
    config.request_timeout = Some(Duration::from_secs(30));
    straum_client::wait_ready(addr, Duration::from_secs(120), || true)
        .map_err(|e| format!("broker at {addr} not ready: {e}"))?;
    let mut c = Client::connect(config.clone()).map_err(|e| e.to_string())?;
    match c.create_topic(BROKER_TOPIC, 1, 0) {
        Ok(()) => println!("TOPIC END"),
        Err(e) if e.code() == Some(ErrorCode::TopicExists) => {}
        Err(e) => return Err(format!("create topic: {e}")),
    }
    println!("READY END");
    let producers: Vec<_> = (0..2u64)
        .map(|t| {
            let config = config.clone();
            let base = base + t * 1_000_000;
            let batch = 1 + rng.below(8);
            std::thread::spawn(move || {
                let Ok(mut c) = Client::connect(config) else {
                    return;
                };
                for k in 0.. {
                    let first = base + k * batch;
                    let msgs = (first..first + batch).map(broker_message).collect();
                    println!("TRY {first} {batch} END");
                    let opts = ProduceOptions {
                        producer_id: t,
                        producer_seq: k,
                        ..ProduceOptions::default()
                    };
                    match c.produce(BROKER_TOPIC, 0, msgs, opts) {
                        Ok(ok) => println!(
                            "ACK {} {} {} {first} END",
                            ok.base_seq, ok.count, ok.timestamp_ms
                        ),
                        Err(_) => return,
                    }
                    // Paced: unpaced, the producers acknowledge ~6000
                    // messages per iteration (3000 at 2 ms) and verifying
                    // all of them after every fault makes 200 iterations
                    // take hours.
                    std::thread::sleep(Duration::from_millis(10));
                }
            })
        })
        .collect();
    for p in producers {
        let _ = p.join();
    }
    Ok(String::new())
}

#[derive(Default)]
struct BrokerLog {
    topic_acked: bool,
    /// seq → (n, timestamp).
    acked: HashMap<u64, (u64, u64)>,
    /// Requests sent and never acknowledged: (first n, count).
    unacked: Vec<(u64, u64)>,
}

fn parse_broker_logs(logs: &str) -> Result<BrokerLog, String> {
    let mut log = BrokerLog::default();
    for path in log_files(logs, "producer.log")? {
        let mut tried: HashMap<u64, u64> = HashMap::new();
        for f in log_lines(&path)? {
            let n = |i: usize| -> Result<u64, String> {
                f.get(i)
                    .and_then(|v| v.parse().ok())
                    .ok_or(format!("{}: malformed line {f:?}", path.display()))
            };
            match f[0].as_str() {
                "TOPIC" => log.topic_acked = true,
                "TRY" => {
                    tried.insert(n(1)?, n(2)?);
                }
                "ACK" => {
                    let (base_seq, count, ts, first) = (n(1)?, n(2)?, n(3)?, n(4)?);
                    tried.remove(&first);
                    for i in 0..count {
                        if let Some(prev) = log.acked.insert(base_seq + i, (first + i, ts))
                            && broker_message(prev.0) != broker_message(first + i)
                        {
                            return Err(format!(
                                "{}: seq {} acknowledged twice with different messages",
                                path.display(),
                                base_seq + i
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        log.unacked.extend(tried);
    }
    Ok(log)
}

/// Starts the broker on the data directory, checks every acknowledged
/// message through Fetch, stops it, and checks the store's recovery report
/// (printed by the broker) against the changes on disk.
fn broker_verify(opts: &HashMap<&str, &str>) -> Result<String, String> {
    let data = PathBuf::from(opt(opts, "data-dir")?);
    let mode = opt(opts, "mode")?;
    let log = parse_broker_logs(opt(opts, "logs")?)?;
    let store_dir = data.join("store");
    if last_wal_file(&store_dir).is_none() {
        return if log.acked.is_empty() && !log.topic_acked {
            Ok("acked=0 (no store yet)".to_owned())
        } else {
            Err("no WAL file in store/ after acknowledgements".to_owned())
        };
    }
    let before = snapshot(&store_dir)?;
    let stderr_path = opt(opts, "stderr")?;
    let stderr = std::fs::File::create(stderr_path).map_err(|e| format!("{stderr_path}: {e}"))?;
    let addr = straum_conformance::broker::free_addr()?;
    let mut child = Command::new(opt(opts, "broker")?)
        .args(["--data-dir", &data.to_string_lossy()])
        .args([
            "--listen",
            &addr,
            "--cluster-id",
            "powerloss",
            "--node-id",
            "1",
        ])
        .args([
            "--store-write-mode",
            mode,
            "--store-segment-max-bytes",
            "65536",
        ])
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .map_err(|e| format!("start broker: {e}"))?;
    let res = straum_client::wait_ready(&addr, Duration::from_secs(120), || {
        matches!(child.try_wait(), Ok(None))
    })
    .map_err(|e| format!("broker not ready (recovery refused?): {e}"))
    .and_then(|ok| match ok.cluster_id.as_str() {
        "powerloss" => broker_check(&addr, &log),
        other => Err(format!(
            "{addr} answered for cluster {other:?}, not this broker"
        )),
    });
    let _ = child.kill();
    let _ = child.wait();
    let summary = res?;
    // The store's recovery report, printed by the broker before it listens.
    let actions: Vec<String> = std::fs::read_to_string(stderr_path)
        .map_err(|e| format!("{stderr_path}: {e}"))?
        .lines()
        .filter_map(|l| l.strip_prefix("straum-server: recovery: "))
        .map(str::to_owned)
        .collect();
    for a in &actions {
        println!("RECOVERY {a}");
    }
    check_report(&actions, mode)?;
    let after = snapshot(&store_dir)?;
    check_changes(&before, &after, &actions)?;
    Ok(format!(
        "{summary} actions={} [{}]",
        actions.len(),
        actions
            .iter()
            .map(|a| action_name(a))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

/// Fetches the topic from the broker at `addr` and checks every
/// acknowledged message.
fn broker_check(addr: &str, log: &BrokerLog) -> Result<String, String> {
    let mut config = ClientConfig::new(addr);
    config.max_attempts = 1;
    let mut c = Client::connect(config).map_err(|e| format!("connect: {e}"))?;
    let mut seq = 0;
    let mut seen = 0usize;
    let mut unacked_present = 0;
    let mut last_ts = 0;
    loop {
        let f = match c.fetch(BROKER_TOPIC, 0, seq, 4 << 20, 0) {
            Ok(f) => f,
            Err(e) if !log.topic_acked && log.acked.is_empty() => {
                return Ok(format!("acked=0 (topic never acknowledged: {e})"));
            }
            Err(e) => return Err(format!("fetch from seq {seq}: {e}")),
        };
        let Some(last) = f.messages.last() else { break };
        let next = last.seq + 1;
        for m in &f.messages {
            if m.seq != seq {
                return Err(format!("seq {seq} missing: fetch returned {}", m.seq));
            }
            if m.timestamp_ms < last_ts {
                return Err(format!(
                    "seq {}: timestamp {} below {last_ts}",
                    m.seq, m.timestamp_ms
                ));
            }
            last_ts = m.timestamp_ms;
            match log.acked.get(&m.seq) {
                Some(&(n, ts)) => {
                    if m.message != broker_message(n) || m.timestamp_ms != ts {
                        return Err(format!(
                            "acknowledged seq {}: other message or timestamp",
                            m.seq
                        ));
                    }
                    seen += 1;
                }
                None => {
                    let sent = log.unacked.iter().any(|&(first, count)| {
                        (first..first + count).any(|n| broker_message(n) == m.message)
                    });
                    if !sent {
                        return Err(format!(
                            "seq {}: neither acknowledged nor in a request the producers had in flight",
                            m.seq
                        ));
                    }
                    unacked_present += 1;
                }
            }
            seq += 1;
        }
        seq = next;
    }
    if seen != log.acked.len() {
        let lost = log.acked.keys().filter(|&&s| s >= seq).min();
        return Err(format!(
            "{} acknowledged messages lost; first lost seq {lost:?}, fetch ended at {seq}",
            log.acked.len() - seen
        ));
    }
    Ok(format!(
        "acked={} unacked_present={unacked_present}",
        log.acked.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(files: &[(&str, &[u8])]) -> Snapshot {
        Snapshot {
            files: files
                .iter()
                .map(|(p, b)| {
                    (
                        (*p).to_owned(),
                        (b.len() as u64, *blake3::hash(b).as_bytes()),
                    )
                })
                .collect(),
            wal: files
                .iter()
                .filter(|(p, _)| wal_file_id(p).is_some())
                .map(|(p, b)| ((*p).to_owned(), b.to_vec()))
                .collect(),
        }
    }

    const W0: &str = "wal-0000000000000000.log";
    const IDX0: &str = "idx-0000000000000000.dat";
    const IDX1: &str = "idx-0000000000000001.dat";

    #[test]
    fn changes_must_match_the_report() {
        let before = snap(&[
            (W0, b"0123456789"),
            ("seg-000000000000000a.dat", b"x"),
            (IDX0, b"abc"),
        ]);
        let cut = snap(&[
            (W0, b"012345"),
            ("seg-000000000000000a.dat", b"x"),
            (IDX0, b"abc"),
        ]);
        let torn =
            "TornTailTruncated { frame_seq: 3, file: 0, offset: 6, bytes_removed: 4 }".to_owned();
        assert!(check_changes(&before, &cut, std::slice::from_ref(&torn)).is_ok());
        // Cut without an entry, entry without a cut, wrong offset.
        assert!(check_changes(&before, &cut, &[]).is_err());
        assert!(check_changes(&before, &before, std::slice::from_ref(&torn)).is_err());
        let wrong =
            "TornTailTruncated { frame_seq: 3, file: 0, offset: 5, bytes_removed: 5 }".to_owned();
        assert!(check_changes(&before, &cut, &[wrong]).is_err());
        // A WAL rewritten, not cut.
        let rewritten = snap(&[
            (W0, b"01234X"),
            ("seg-000000000000000a.dat", b"x"),
            (IDX0, b"abc"),
        ]);
        assert!(check_changes(&before, &rewritten, &[torn]).is_err());

        let no_seg = snap(&[(W0, b"0123456789"), (IDX0, b"abc")]);
        let orphan = "OrphanSegmentRemoved { segment: 10 }".to_owned();
        assert!(check_changes(&before, &no_seg, &[orphan]).is_ok());
        assert!(check_changes(&before, &no_seg, &[]).is_err());
        // A segment changed in place is never a documented repair.
        let seg_changed = snap(&[
            (W0, b"0123456789"),
            ("seg-000000000000000a.dat", b"y"),
            (IDX0, b"abc"),
        ]);
        assert!(check_changes(&before, &seg_changed, &[]).is_err());
    }

    #[test]
    fn a_cut_names_its_wal_file() {
        const W1: &str = "wal-0000000000000005.log";
        let before = snap(&[(W0, b"0123456789"), (W1, b"abcdef")]);
        let cut = snap(&[(W0, b"0123456789"), (W1, b"abc")]);
        let torn = |file| {
            format!(
                "TornTailTruncated {{ frame_seq: 7, file: {file}, offset: 3, bytes_removed: 3 }}"
            )
        };
        assert!(check_changes(&before, &cut, &[torn(5)]).is_ok());
        assert!(check_changes(&before, &cut, &[torn(0)]).is_err());
        let tmp = snap(&[
            (W0, b"0123456789"),
            (W1, b"abcdef"),
            ("wal-0000000000000009.log.tmp", b"h"),
        ]);
        let removed = "WalTmpRemoved".to_owned();
        assert!(check_changes(&tmp, &before, std::slice::from_ref(&removed)).is_ok());
        assert!(check_changes(&tmp, &before, &[]).is_err());
    }

    #[test]
    fn index_segments_may_only_be_deleted_or_added_with_a_report_entry() {
        let before = snap(&[(W0, b"0123456789"), (IDX0, b"abc")]);
        let repaired =
            r#"TopicIndexRepaired { topic: [111], blocks_rewritten: 1, pages_rewritten: 0 }"#
                .to_owned();
        // An existing segment changed in place: never a repair.
        let changed = snap(&[(W0, b"0123456789"), (IDX0, b"abd")]);
        assert!(check_changes(&before, &changed, std::slice::from_ref(&repaired)).is_err());
        // Deleted, with its size reported.
        let gone = snap(&[(W0, b"0123456789"), (IDX1, b"hdr+blocks")]);
        let removed = "IndexSegmentRemoved { segment: 0, bytes: 3 }".to_owned();
        let actions = [removed.clone(), repaired.clone()];
        assert!(check_changes(&before, &gone, &actions).is_ok());
        let wrong = "IndexSegmentRemoved { segment: 0, bytes: 4 }".to_owned();
        assert!(check_changes(&before, &gone, &[wrong, repaired.clone()]).is_err());
        // A new segment needs a fence or repaired records, and repaired
        // records need a new segment.
        let added = snap(&[(W0, b"0123456789"), (IDX0, b"abc"), (IDX1, b"h")]);
        assert!(check_changes(&before, &added, &[]).is_err());
        assert!(check_changes(&before, &added, std::slice::from_ref(&repaired)).is_ok());
        assert!(check_changes(&before, &before, std::slice::from_ref(&repaired)).is_err());
        let fenced = "StaleIndexRecordsFenced { records: 2, segment: 1 }".to_owned();
        assert!(check_changes(&before, &added, std::slice::from_ref(&fenced)).is_ok());
        let elsewhere = "StaleIndexRecordsFenced { records: 2, segment: 7 }".to_owned();
        assert!(check_changes(&before, &added, &[elsewhere]).is_err());
        assert!(check_changes(&before, &added, &[fenced, repaired]).is_ok());
    }

    #[test]
    fn report_rules() {
        let dropped = "FrameDropped { frame_seq: 1, file: 0, offset: 2, mode: ParallelSync, object: x, reason: ObjectMissing, bytes_removed: 3 }".to_owned();
        assert!(check_report(std::slice::from_ref(&dropped), "parallel").is_ok());
        assert!(check_report(std::slice::from_ref(&dropped), "sequential").is_err());
        let torn =
            "TornTailTruncated { frame_seq: 1, file: 0, offset: 2, bytes_removed: 3 }".to_owned();
        assert!(check_report(&[torn, dropped], "parallel").is_err());
        assert!(check_report(&["Rewritten".to_owned()], "parallel").is_err());
    }

    #[test]
    fn messages_are_reproducible() {
        assert_eq!(store_message(7, 3, 2), store_message(7, 3, 2));
        assert_ne!(store_message(7, 3, 2).1, store_message(7, 3, 3).1);
        assert_eq!(batch_len(7, 3), batch_len(7, 3));
    }
}
