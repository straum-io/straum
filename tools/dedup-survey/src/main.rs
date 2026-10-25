//! Duplicate survey: how much of a real log straum-store deduplicates, and
//! how much disk it uses against a plain log and a zstd-compressed plain
//! log.
//!
//! ```text
//! dedup-survey --work <empty dir> --out <file.csv> \
//!     --dataset <name>:<format>:<file>[,<file>...] [--dataset ...]
//! ```
//!
//! `format` is `openstack`, `ssh` or `bgl` and selects the timestamp
//! stripping of the secondary measurement (see `strip_timestamp`). Each log
//! line, without its newline, is one message; one topic per dataset;
//! batches of 64; write mode Sequential; the timestamp passed to the store
//! is the index of the batch's first line (`put_batch` takes one per batch).
//! Every dataset is surveyed twice: `raw` (the lines as they are) and
//! `stripped` (leading timestamp removed). Stores are created under
//! `<work>/stores/`.

use std::path::{Path, PathBuf};

use straum_store::{Config, Message, Store};

const HEADER: &str = "dataset,variant,files,messages,distinct_payloads,dup_messages,dup_share,payload_bytes,store_bytes,segment_bytes,wal_bytes,index_bytes,plain_bytes,zstd_plain_bytes,store_over_plain,store_over_zstd,unstripped_lines";
const BATCH: usize = 64;
const ZSTD_LEVEL: i32 = 3;
const ZSTD_CHUNK: usize = 1 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Format {
    OpenStack,
    Ssh,
    Bgl,
}

struct Dataset {
    name: String,
    format: Format,
    files: Vec<PathBuf>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("dedup-survey: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (work, out, datasets) = parse_args()?;
    let mut csv = String::from(HEADER);
    csv.push('\n');
    let rt = compio::runtime::Runtime::new()?;
    for ds in &datasets {
        let mut data = Vec::new();
        for f in &ds.files {
            data.extend_from_slice(&std::fs::read(f)?);
            if data.last().is_some_and(|&b| b != b'\n') {
                data.push(b'\n');
            }
        }
        let raw: Vec<&[u8]> = lines(&data).collect();
        let mut unstripped = 0u64;
        let stripped_owned: Vec<Vec<u8>> = raw
            .iter()
            .map(|l| {
                strip_timestamp(ds.format, l).unwrap_or_else(|| {
                    unstripped += 1;
                    l.to_vec()
                })
            })
            .collect();
        let stripped: Vec<&[u8]> = stripped_owned.iter().map(Vec::as_slice).collect();
        for (variant, msgs, unmatched) in [("raw", &raw, 0), ("stripped", &stripped, unstripped)] {
            let dir = work.join("stores").join(format!("{}-{variant}", ds.name));
            let r = rt.block_on(survey(&dir, ds.name.as_bytes(), msgs))?;
            let plain = plain_bytes(msgs);
            let zstd_plain = zstd_plain_bytes(msgs)?;
            let store = r.segment_bytes + r.wal_bytes + r.index_bytes;
            csv.push_str(&format!(
                "{},{variant},{},{},{},{},{:.6},{},{store},{},{},{},{plain},{zstd_plain},{:.4},{:.4},{unmatched}\n",
                ds.name,
                ds.files.len(),
                r.messages,
                r.distinct,
                r.duplicates,
                r.duplicates as f64 / r.messages as f64,
                r.payload_bytes,
                r.segment_bytes,
                r.wal_bytes,
                r.index_bytes,
                store as f64 / plain as f64,
                store as f64 / zstd_plain as f64,
            ));
            eprintln!(
                "{} {variant}: {} messages, {:.2}% duplicates, store/plain {:.3}, store/zstd {:.3}",
                ds.name,
                r.messages,
                100.0 * r.duplicates as f64 / r.messages as f64,
                store as f64 / plain as f64,
                store as f64 / zstd_plain as f64
            );
        }
    }
    std::fs::write(&out, csv)?;
    Ok(())
}

/// Lines without their `\n`; a trailing `\r` is kept (part of the line's
/// bytes). The empty piece after the last newline is not a line.
fn lines(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    let data = data.strip_suffix(b"\n").unwrap_or(data);
    data.split(|&b| b == b'\n')
        .filter(move |_| !data.is_empty())
}

struct Survey {
    messages: u64,
    distinct: u64,
    duplicates: u64,
    payload_bytes: u64,
    segment_bytes: u64,
    wal_bytes: u64,
    index_bytes: u64,
}

async fn survey(
    dir: &Path,
    topic: &[u8],
    msgs: &[&[u8]],
) -> Result<Survey, Box<dyn std::error::Error>> {
    if dir.exists() {
        return Err(format!("{} already exists; use an empty --work dir", dir.display()).into());
    }
    let mut store = Store::open(dir, Config::default()).await?;
    let mut batch = Vec::with_capacity(BATCH);
    for (i, chunk) in msgs.chunks(BATCH).enumerate() {
        batch.clear();
        batch.extend(chunk.iter().map(|&payload| Message { topic, payload }));
        store.put_batch(&batch, (i * BATCH) as u64).await?;
    }
    let s = store.stats();
    let distinct = store.object_count() as u64;
    drop(store);
    let (segment_bytes, wal_bytes, index_bytes) = disk_usage(dir)?;
    Ok(Survey {
        messages: s.messages_accepted,
        distinct,
        duplicates: s.messages_deduplicated,
        payload_bytes: s.payload_bytes_received,
        segment_bytes,
        wal_bytes,
        index_bytes,
    })
}

/// File lengths in a store directory: segments, WAL, topic indexes.
fn disk_usage(dir: &Path) -> std::io::Result<(u64, u64, u64)> {
    let (mut seg, mut wal, mut idx) = (0, 0, 0);
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("seg-") {
            seg += entry.metadata()?.len();
        } else if name == "wal.log" {
            wal += entry.metadata()?.len();
        }
    }
    for entry in std::fs::read_dir(dir.join("topics"))? {
        idx += entry?.metadata()?.len();
    }
    Ok((seg, wal, idx))
}

/// Plain log: every line as `len u32 | bytes`, concatenated.
fn plain_bytes(msgs: &[&[u8]]) -> u64 {
    msgs.iter().map(|m| 4 + m.len() as u64).sum()
}

/// The plain log compressed with zstd level 3 in independent 1 MiB chunks.
fn zstd_plain_bytes(msgs: &[&[u8]]) -> std::io::Result<u64> {
    let mut total = 0;
    let mut chunk = Vec::with_capacity(2 * ZSTD_CHUNK);
    let mut flush = |chunk: &mut Vec<u8>, all: bool| -> std::io::Result<()> {
        while chunk.len() >= ZSTD_CHUNK || (all && !chunk.is_empty()) {
            let n = chunk.len().min(ZSTD_CHUNK);
            total += zstd::bulk::compress(&chunk[..n], ZSTD_LEVEL)?.len() as u64;
            chunk.drain(..n);
        }
        Ok(())
    };
    for m in msgs {
        chunk.extend_from_slice(&(m.len() as u32).to_le_bytes());
        chunk.extend_from_slice(m);
        flush(&mut chunk, false)?;
    }
    flush(&mut chunk, true)?;
    Ok(total)
}

/// The line without its leading timestamp, or `None` if the line does not
/// have the dataset's layout. Equivalent regexes (matched by hand, no regex
/// dependency):
///
/// * openstack: `^\S+ \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} ` → `` (log
///   file name, date, time)
/// * ssh: `^[A-Z][a-z]{2} +\d{1,2} \d{2}:\d{2}:\d{2} ` → `` (syslog time)
/// * bgl: `^(\S+) \d+ \d{4}\.\d{2}\.\d{2} \S+ \d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d{6} `
///   → `$1 ` (alert label kept; epoch, date, first node field and time
///   removed; the line repeats the node right after the time, so no node
///   information is lost)
fn strip_timestamp(format: Format, line: &[u8]) -> Option<Vec<u8>> {
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

/// Splits off the first space-terminated token.
fn token(s: &[u8]) -> Option<(&[u8], &[u8])> {
    let i = s.iter().position(|&b| b == b' ')?;
    Some((&s[..i], &s[i + 1..]))
}

/// `pattern`: `d` matches an ASCII digit, any other byte matches itself.
fn shape(s: &[u8], pattern: &[u8]) -> bool {
    s.len() == pattern.len()
        && s.iter().zip(pattern).all(|(&c, &p)| {
            if p == b'd' {
                c.is_ascii_digit()
            } else {
                c == p
            }
        })
}

fn parse_args() -> Result<(PathBuf, PathBuf, Vec<Dataset>), String> {
    let usage = "usage: dedup-survey --work <dir> --out <file.csv> --dataset <name>:<openstack|ssh|bgl>:<file>[,<file>...] ...";
    let (mut work, mut out, mut datasets) = (None, None, Vec::new());
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let value = it
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{usage}"))?;
        match flag.as_str() {
            "--work" => work = Some(PathBuf::from(value)),
            "--out" => out = Some(PathBuf::from(value)),
            "--dataset" => {
                let mut parts = value.splitn(3, ':');
                let (Some(name), Some(format), Some(files)) =
                    (parts.next(), parts.next(), parts.next())
                else {
                    return Err(format!("bad --dataset {value}\n{usage}"));
                };
                let format = match format {
                    "openstack" => Format::OpenStack,
                    "ssh" => Format::Ssh,
                    "bgl" => Format::Bgl,
                    f => return Err(format!("unknown format {f}\n{usage}")),
                };
                let files = files.split(',').map(PathBuf::from).collect();
                datasets.push(Dataset {
                    name: name.to_owned(),
                    format,
                    files,
                });
            }
            _ => return Err(format!("unknown flag {flag}\n{usage}")),
        }
    }
    match (work, out) {
        (Some(w), Some(o)) if !datasets.is_empty() => Ok((w, o, datasets)),
        _ => Err(usage.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Synthetic lines in each dataset's layout (no dataset content).
    #[test]
    fn strips_openstack_time() {
        let l = b"nova-api.log.1 2017-01-02 03:04:05.678 123 INFO x y";
        assert_eq!(
            strip_timestamp(Format::OpenStack, l),
            Some(b"123 INFO x y".to_vec())
        );
        assert_eq!(
            strip_timestamp(Format::OpenStack, b"a 2017-1-02 03:04:05.678 x"),
            None
        );
    }

    #[test]
    fn strips_ssh_time() {
        let l = b"Dec  1 06:55:46 host sshd[1]: msg";
        assert_eq!(
            strip_timestamp(Format::Ssh, l),
            Some(b"host sshd[1]: msg".to_vec())
        );
        let l = b"Jan 10 16:05:01 host x";
        assert_eq!(strip_timestamp(Format::Ssh, l), Some(b"host x".to_vec()));
        assert_eq!(strip_timestamp(Format::Ssh, b"dec 10 06:55:46 h x"), None);
    }

    #[test]
    fn strips_bgl_time() {
        let l = b"- 1100000000 2004.11.09 R00-M0-N0 2004-11-09-12.00.00.123456 R00-M0-N0 RAS KERNEL INFO msg";
        assert_eq!(
            strip_timestamp(Format::Bgl, l),
            Some(b"- R00-M0-N0 RAS KERNEL INFO msg".to_vec())
        );
        assert_eq!(strip_timestamp(Format::Bgl, b"- x 2004.11.09 n t m"), None);
    }

    #[test]
    fn lines_and_baselines() {
        let data = b"a\nbb\n\nccc\n";
        let l: Vec<&[u8]> = lines(data).collect();
        assert_eq!(l, vec![&b"a"[..], b"bb", b"", b"ccc"]);
        assert_eq!(plain_bytes(&l), 4 * 4 + 6);
        assert_eq!(lines(b"").count(), 0);
        assert_eq!(lines(b"x").count(), 1);
        let many: Vec<&[u8]> = vec![&b"same line repeated"[..]; 100_000];
        let z = zstd_plain_bytes(&many).unwrap();
        assert!(z > 0 && z < plain_bytes(&many) / 50, "{z}");
    }
}
