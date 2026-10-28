//! Writer and verifier processes of the power-loss harness. The driver,
//! `scripts/power-loss/run.sh`, mounts LazyFS, runs a writer, injects a
//! fault, remounts and runs a verifier; this binary holds every piece that
//! needs the store or the client:
//!
//! ```text
//! plan          --scenario S --seed N --iterations K --store-root P
//! store-write   --dir D --mode M --seed N [--compact-every C]
//!               [--arm-at-compaction K --fifo F --fault CMD]
//! store-verify  --dir D --mode M --logs DIR
//! broker-produce --addr A --base N --seed N
//! broker-verify --broker BIN --data-dir D --mode M --logs DIR --stderr FILE
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
use std::io::{BufRead, BufReader, Write};
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
            "usage: straum-power-loss <plan|store-write|store-verify|broker-produce|broker-verify> --key value ..."
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
const CRASH_POINTS: [(&str, &str); 7] = [
    ("write", r"wal\.log"),
    ("fsync", r"wal\.log"),
    ("write", "seg-"),
    ("fsync", "seg-"),
    ("write", r"\.idx"),
    ("create", "seg-"),
    ("unlink", "seg-"),
];

/// One line per iteration: `<i> <delay_ms> <writer_seed> <kind> <command>
/// <arm_at_compaction>`. Each iteration draws from its own generator, so an
/// iteration is reproduced from the seed and its number alone.
fn plan(opts: &HashMap<&str, &str>) -> Result<String, String> {
    let scenario = opt(opts, "scenario")?;
    let seed = num(opts, "seed")?;
    let iterations = num(opts, "iterations")?;
    let root = opt(opts, "store-root")?;
    let index = match scenario {
        "broker" => hex(format!("t/{BROKER_TOPIC}/0").as_bytes()),
        _ => hex(ORDERS),
    };
    for i in 0..iterations {
        let mut rng = Rng::new(mix(seed, i));
        let writer_seed = rng.next();
        let delay = rng.below(300);
        let compaction = scenario == "compaction";
        let kind = match rng.below(3) {
            0 if !compaction => "clear",
            0 | 1 => "crash",
            _ => "torn",
        };
        let command = match kind {
            "clear" => "lazyfs::clear-cache".to_owned(),
            "crash" => {
                let points = match scenario {
                    // Operations a compaction issues.
                    "compaction" => &CRASH_POINTS[..4],
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
                // compaction writes no index.
                let file = if compaction || rng.below(3) > 0 {
                    "wal.log".to_owned()
                } else {
                    format!("topics/{index}.idx")
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

fn store_config(mode: &str) -> Result<Config, String> {
    let write_mode = match mode {
        "sequential" => WriteMode::Sequential,
        "parallel" => WriteMode::ParallelSync,
        // Below most payloads (up to ~3 KiB): inline and segment objects mix.
        "inline" => WriteMode::InlineSmall { threshold: 1024 },
        _ => return Err(format!("unknown write mode {mode}")),
    };
    Ok(Config {
        // Small segments: rotation, sealing mid-batch and compaction happen
        // within one iteration.
        segment_max_bytes: 64 << 10,
        write_mode,
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
    let config = store_config(mode)?;
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
                    let fifo = opt(opts, "fifo").unwrap();
                    let fault = opt(opts, "fault").unwrap();
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(fifo)
                        .and_then(|mut f| writeln!(f, "{fault}"))
                        .unwrap();
                    println!("ARMED {round} END");
                    // LazyFS reads the FIFO on its own thread.
                    std::thread::sleep(Duration::from_millis(200));
                }
                println!("COMPACT-START {round} END");
                let stats = store.compact().await.unwrap();
                println!("COMPACTED {} END", stats.segments_rewritten);
            }
        }
    });
    Ok(String::new())
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
        for f in log_lines(&path)? {
            let n = |i: usize| -> Result<u64, String> {
                f.get(i)
                    .and_then(|v| v.parse().ok())
                    .ok_or(format!("{}: malformed line {f:?}", path.display()))
            };
            match f[0].as_str() {
                "SEED" => seed = Some(n(1)?),
                "TRY" => last_try = Some(n(1)?),
                "ACK" => {
                    let seed = seed.ok_or("ACK before SEED")?;
                    let (round, j, seq) = (n(1)?, n(2)?, n(4)?);
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
    }
    Ok(log)
}

/// Every file under a store directory: relative path → (length, BLAKE3),
/// plus the WAL's bytes, to check what recovery cut.
struct Snapshot {
    files: BTreeMap<String, (u64, [u8; 32])>,
    wal: Vec<u8>,
}

fn snapshot(dir: &Path) -> Result<Snapshot, String> {
    let mut files = BTreeMap::new();
    let mut wal = Vec::new();
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
            if rel == "wal.log" {
                wal = bytes;
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
    const ALLOWED: [&str; 6] = [
        "WalTmpRemoved",
        "TornTailTruncated",
        "FrameDropped",
        "OrphanSegmentRemoved",
        "UnknownTopicIndexRemoved",
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

/// Every difference between the snapshots before and after recovery is
/// listed in the report, and every report entry is a difference on disk.
/// Anything else is a silent repair.
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
    for rel in keys {
        let (b, a) = (before.files.get(rel), after.files.get(rel));
        if b == a {
            continue;
        }
        let name = rel.rsplit('/').next().unwrap_or(rel);
        match (rel.as_str(), b, a) {
            ("wal.log", Some(b), Some(a)) => {
                let cut = a.0 as usize;
                if a.0 >= b.0 || *blake3::hash(&before.wal[..cut]).as_bytes() != a.1 {
                    return Err(format!(
                        "silent repair: wal.log changed other than by a cut ({} → {} bytes)",
                        b.0, a.0
                    ));
                }
                let removed = (b.0 - a.0).to_string();
                let offset = a.0.to_string();
                explain(
                    &|x| {
                        matches!(action_name(x), "TornTailTruncated" | "FrameDropped")
                            && action_field(x, "offset") == Some(offset.as_str())
                            && action_field(x, "bytes_removed") == Some(removed.as_str())
                    },
                    "wal.log (cut)",
                )?;
            }
            ("wal.log.tmp", Some(_), None) => {
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
            (_, Some(_), None) if rel.starts_with("topics/") => {
                explain(
                    &|x| {
                        action_name(x) == "UnknownTopicIndexRemoved"
                            && action_field(x, "path").is_some_and(|p| p.ends_with(rel.as_str()))
                    },
                    rel,
                )?;
            }
            (_, _, Some(_)) if rel.starts_with("topics/") => {
                explain(
                    &|x| {
                        action_name(x) == "TopicIndexRepaired"
                            && action_field(x, "path").is_some_and(|p| p.ends_with(rel.as_str()))
                    },
                    rel,
                )?;
            }
            _ => return Err(format!("silent repair: {rel} changed ({b:?} → {a:?})")),
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
    let config = store_config(mode)?;
    let log = parse_store_logs(opt(opts, "logs")?)?;
    if !dir.join("wal.log").exists() {
        return if log.acked.is_empty() {
            Ok("acked=0 (no store yet)".to_owned())
        } else {
            Err(format!(
                "wal.log missing with {} acknowledged messages",
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
                let id = store.entry(topic, seq).ok_or(format!("{t}/{seq}: no entry"))?;
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
    if !store_dir.join("wal.log").exists() {
        return if log.acked.is_empty() && !log.topic_acked {
            Ok("acked=0 (no store yet)".to_owned())
        } else {
            Err("store/wal.log missing after acknowledgements".to_owned())
        };
    }
    let before = snapshot(&store_dir)?;
    let mut child = Command::new(opt(opts, "broker")?)
        .args(["--data-dir", &data.to_string_lossy()])
        .args([
            "--listen",
            "127.0.0.1:0",
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
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("start broker: {e}"))?;
    let res = broker_check(&mut child, opt(opts, "stderr")?, &log);
    let _ = child.kill();
    let _ = child.wait();
    let (actions, summary) = res?;
    check_report(&actions, mode)?;
    let after = snapshot(&store_dir)?;
    check_changes(&before, &after, &actions)?;
    Ok(summary)
}

fn broker_check(
    child: &mut std::process::Child,
    stderr_path: &str,
    log: &BrokerLog,
) -> Result<(Vec<String>, String), String> {
    let mut out = std::fs::File::create(stderr_path).map_err(|e| format!("{stderr_path}: {e}"))?;
    let mut lines = BufReader::new(child.stderr.take().ok_or("no stderr")?).lines();
    let mut actions = Vec::new();
    let addr = loop {
        let line = lines
            .next()
            .ok_or("broker exited before listening (recovery refused?)")?
            .map_err(|e| e.to_string())?;
        let _ = writeln!(out, "{line}");
        if let Some(a) = line.strip_prefix("straum-broker: recovery: ") {
            println!("RECOVERY {a}");
            actions.push(a.to_owned());
        }
        if let Some(a) = line.strip_prefix("straum-broker: listening on ") {
            break a.to_owned();
        }
    };
    std::thread::spawn(move || {
        for line in lines.map_while(Result::ok) {
            let _ = writeln!(out, "{line}");
        }
    });
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
                return Ok((actions, format!("acked=0 (topic never acknowledged: {e})")));
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
    let summary = format!(
        "acked={} unacked_present={unacked_present} actions={} [{}]",
        log.acked.len(),
        actions.len(),
        actions
            .iter()
            .map(|a| action_name(a))
            .collect::<Vec<_>>()
            .join(",")
    );
    Ok((actions, summary))
}

fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
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
                .find(|(p, _)| *p == "wal.log")
                .map(|(_, b)| b.to_vec())
                .unwrap_or_default(),
        }
    }

    #[test]
    fn changes_must_match_the_report() {
        let before = snap(&[
            ("wal.log", b"0123456789"),
            ("seg-000000000000000a.dat", b"x"),
            ("topics/6f.idx", b"abc"),
        ]);
        let cut = snap(&[
            ("wal.log", b"012345"),
            ("seg-000000000000000a.dat", b"x"),
            ("topics/6f.idx", b"abc"),
        ]);
        let torn = "TornTailTruncated { frame_seq: 3, offset: 6, bytes_removed: 4 }".to_owned();
        assert!(check_changes(&before, &cut, std::slice::from_ref(&torn)).is_ok());
        // Cut without an entry, entry without a cut, wrong offset.
        assert!(check_changes(&before, &cut, &[]).is_err());
        assert!(check_changes(&before, &before, std::slice::from_ref(&torn)).is_err());
        let wrong = "TornTailTruncated { frame_seq: 3, offset: 5, bytes_removed: 5 }".to_owned();
        assert!(check_changes(&before, &cut, &[wrong]).is_err());
        // A WAL rewritten, not cut.
        let rewritten = snap(&[
            ("wal.log", b"01234X"),
            ("seg-000000000000000a.dat", b"x"),
            ("topics/6f.idx", b"abc"),
        ]);
        assert!(check_changes(&before, &rewritten, &[torn]).is_err());

        let no_seg = snap(&[("wal.log", b"0123456789"), ("topics/6f.idx", b"abc")]);
        let orphan = "OrphanSegmentRemoved { segment: 10 }".to_owned();
        assert!(check_changes(&before, &no_seg, &[orphan]).is_ok());
        assert!(check_changes(&before, &no_seg, &[]).is_err());
        // A segment changed in place is never a documented repair.
        let seg_changed = snap(&[
            ("wal.log", b"0123456789"),
            ("seg-000000000000000a.dat", b"y"),
            ("topics/6f.idx", b"abc"),
        ]);
        assert!(check_changes(&before, &seg_changed, &[]).is_err());

        let idx = snap(&[
            ("wal.log", b"0123456789"),
            ("seg-000000000000000a.dat", b"x"),
            ("topics/6f.idx", b"abd"),
        ]);
        let repaired = r#"TopicIndexRepaired { path: "/m/store/topics/6f.idx", entries_kept: 1, bytes_truncated: 1, entries_rewritten: 1 }"#.to_owned();
        assert!(check_changes(&before, &idx, &[repaired]).is_ok());
    }

    #[test]
    fn report_rules() {
        let dropped = "FrameDropped { frame_seq: 1, offset: 2, mode: ParallelSync, object: x, reason: ObjectMissing, bytes_removed: 3 }".to_owned();
        assert!(check_report(std::slice::from_ref(&dropped), "parallel").is_ok());
        assert!(check_report(std::slice::from_ref(&dropped), "sequential").is_err());
        let torn = "TornTailTruncated { frame_seq: 1, offset: 2, bytes_removed: 3 }".to_owned();
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
