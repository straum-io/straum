//! Hit ratio and memory of the recent-id cache (ADR-0014 step 6a): counts
//! only, no timing, so it may run on a busy machine (store-rules.md §5).
//!
//! The store's id lookup is simulated exactly: memtable, then the cache,
//! then the id runs; a new id goes to the memtable and the cache; a
//! checkpoint empties the memtable every `interval` new ids; a hit in the
//! runs enters the cache. Every repeat is counted by where it was found;
//! a repeat found only in the runs costs one page read.
//!
//! Usage: recent-ids <loghub dir> [--max-lines N]
//! The directory holds openstack_normal1.log, openstack_normal2.log,
//! openstack_abnormal.log, SSH.log and BGL.log (the files of ADR-0014's
//! algorithm selection). Timestamps are stripped as `tools/dedup-survey`
//! does. Synthetic streams cover reuse distances from 512 ids (one batch of
//! `straum-store-bench`'s default 512 messages) to the whole history. The
//! cache's bytes are computed from its maps' capacity (`RecentIds::bytes`);
//! the peak over the run is what a budget share must hold.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use straum_store::{ObjectId, RecentIds};

#[derive(Clone, Copy)]
enum Format {
    OpenStack,
    Ssh,
    Bgl,
}

/// The line without its leading timestamp (as `tools/dedup-survey`).
fn strip_timestamp(format: Format, line: &[u8]) -> Option<Vec<u8>> {
    fn token(s: &[u8]) -> Option<(&[u8], &[u8])> {
        let i = s.iter().position(|&b| b == b' ')?;
        Some((&s[..i], &s[i + 1..]))
    }
    fn shape(s: &[u8], p: &[u8]) -> bool {
        s.len() == p.len()
            && s.iter().zip(p).all(|(&c, &p)| {
                if p == b'd' {
                    c.is_ascii_digit()
                } else {
                    c == p
                }
            })
    }
    match format {
        Format::OpenStack => {
            let (_, rest) = token(line)?;
            let (date, rest) = token(rest)?;
            let (time, rest) = token(rest)?;
            (shape(date, b"dddd-dd-dd") && shape(time, b"dd:dd:dd.ddd")).then(|| rest.to_vec())
        }
        Format::Ssh => {
            let month = line.get(..3)?;
            let ok = month[0].is_ascii_uppercase() && month[1..].iter().all(u8::is_ascii_lowercase);
            let mut rest = line.get(3..)?;
            let spaces = rest.iter().take_while(|&&b| b == b' ').count();
            if !ok || spaces == 0 {
                return None;
            }
            rest = &rest[spaces..];
            let (day, rest) = token(rest)?;
            let (time, rest) = token(rest)?;
            let day_ok = (1..=2).contains(&day.len()) && day.iter().all(u8::is_ascii_digit);
            (day_ok && shape(time, b"dd:dd:dd")).then(|| rest.to_vec())
        }
        Format::Bgl => {
            let (label, rest) = token(line)?;
            let (epoch, rest) = token(rest)?;
            let (date, rest) = token(rest)?;
            let (_node, rest) = token(rest)?;
            let (time, rest) = token(rest)?;
            let ok = !epoch.is_empty()
                && epoch.iter().all(u8::is_ascii_digit)
                && shape(date, b"dddd.dd.dd")
                && shape(time, b"dddd-dd-dd-dd.dd.dd.dddddd");
            ok.then(|| [label, b" ", rest].concat())
        }
    }
}

fn dataset(dir: &Path, files: &[&str], format: Format, max: usize) -> Vec<ObjectId> {
    let mut ids = Vec::new();
    for f in files {
        let data = std::fs::read(dir.join(f)).unwrap_or_else(|e| panic!("{f}: {e}"));
        for line in data.split(|&b| b == b'\n').filter(|l| !l.is_empty()) {
            if ids.len() == max {
                return ids;
            }
            let body = strip_timestamp(format, line).unwrap_or_else(|| line.to_vec());
            ids.push(ObjectId::of(&body));
        }
    }
    ids
}

fn splitmix(x: &mut u64) -> u64 {
    *x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// `n` ids: half new, half a repeat of the id `distance` positions back
/// (`None`: of a uniformly chosen earlier one).
fn synthetic(n: usize, distance: Option<usize>, seed: u64) -> Vec<ObjectId> {
    let mut x = seed;
    let mut out: Vec<ObjectId> = Vec::with_capacity(n);
    let mut fresh = 0u64;
    for i in 0..n {
        let r = splitmix(&mut x);
        let back = match distance {
            Some(d) if r & 1 == 1 && i >= d => Some(i - d),
            None if r & 1 == 1 && i > 0 => Some((r >> 1) as usize % i),
            _ => None,
        };
        out.push(match back {
            Some(j) => out[j],
            None => {
                fresh += 1;
                ObjectId::of(&fresh.to_le_bytes())
            }
        });
    }
    out
}

#[derive(Default)]
struct Counts {
    lookups: u64,
    new: u64,
    memtable: u64,
    cache: u64,
    runs: u64,
}

fn simulate(ids: &[ObjectId], capacity: usize, interval: usize) -> (Counts, usize, usize) {
    let mut c = Counts::default();
    let mut memtable: HashMap<ObjectId, u64> = HashMap::new();
    let mut runs: HashSet<ObjectId> = HashSet::new();
    let mut cache = RecentIds::new(capacity);
    let mut peak_bytes = 0;
    for id in ids {
        c.lookups += 1;
        if memtable.contains_key(id) {
            c.memtable += 1;
        } else if cache.get(id).is_some() {
            c.cache += 1;
        } else if runs.contains(id) {
            c.runs += 1;
            cache.insert(*id, 0);
        } else {
            c.new += 1;
            memtable.insert(*id, c.new);
            cache.insert(*id, c.new);
            if memtable.len() >= interval {
                runs.extend(memtable.drain().map(|(id, _)| id));
            }
        }
        peak_bytes = peak_bytes.max(cache.bytes());
    }
    (c, cache.len(), peak_bytes)
}

fn report(name: &str, ids: &[ObjectId], intervals: &[usize], capacities: &[usize]) {
    for &interval in intervals {
        for &capacity in capacities {
            let (c, len, bytes) = simulate(ids, capacity, interval);
            let repeats = c.memtable + c.cache + c.runs;
            let pct = |x: u64| 100.0 * x as f64 / repeats.max(1) as f64;
            println!(
                "{name}\tinterval={interval}\tcapacity={capacity}\tlookups={}\tnew={}\trepeats={repeats}\tmemtable={:.2}%\tcache={:.2}%\truns(page read)={:.2}%\tcache_entries_at_end={len}\tcache_peak_bytes={bytes}",
                c.lookups,
                c.new,
                pct(c.memtable),
                pct(c.cache),
                pct(c.runs),
            );
        }
    }
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let Some(dir) = args.get(1) else {
        eprintln!("usage: recent-ids <loghub dir> [--max-lines N]");
        std::process::exit(2);
    };
    let max = args
        .iter()
        .position(|a| a == "--max-lines")
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(usize::MAX);
    let dir = Path::new(dir);
    // Checkpoint every 100 000, 600 000 (the memtable's 64 MiB share at
    // about 64 bytes per id plus a 48–56-byte dense-table entry) and
    // 1 000 000 new ids.
    let intervals = [100_000, 600_000, 1_000_000];
    let capacities = [0, 1 << 14, 1 << 16, 1 << 18, 1 << 20, 1 << 22];
    let sets: [(&str, &[&str], Format); 3] = [
        (
            "openstack",
            &[
                "openstack_normal1.log",
                "openstack_normal2.log",
                "openstack_abnormal.log",
            ],
            Format::OpenStack,
        ),
        ("ssh", &["SSH.log"], Format::Ssh),
        ("bgl", &["BGL.log"], Format::Bgl),
    ];
    for (name, files, format) in sets {
        let ids = dataset(dir, files, format, max);
        report(name, &ids, &intervals, &capacities);
    }
    for (name, distance) in [
        ("synthetic-d512", Some(512)),
        ("synthetic-d10k", Some(10_000)),
        ("synthetic-d1M", Some(1_000_000)),
        ("synthetic-uniform", None),
    ] {
        let ids = synthetic(4_000_000, distance, 7);
        report(name, &ids, &intervals, &capacities);
    }
}
