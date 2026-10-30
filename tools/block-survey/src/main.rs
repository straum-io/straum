//! Block compression survey: how small the unique payloads of a log get when
//! compressed with zstd in independent blocks, by block size, zstd level and
//! dictionary.
//!
//! ```text
//! block-survey --out <file.csv> --dataset <name>:<file>[,<file>...] [--dataset ...]
//! ```
//!
//! A message is one line without its newline, as in dedup-survey. A unique
//! payload is the first occurrence of a distinct line. A block holds objects
//! encoded as `varint length | bytes`, and is one of:
//!
//! * `bytes:<n>`: consecutive unique payloads; the block is closed before
//!   the next object would take it past n bytes, and holds at least one
//!   object;
//! * `batch:<n>`: the message stream cut into batches of n messages (one
//!   group commit each); the block holds the payloads first seen in that
//!   batch, and a batch with none makes no block.
//!
//! Every block is compressed on its own at zstd levels 1 and 3, without and
//! with a dictionary trained on the first 1% of the messages (at most 10 MiB
//! of samples, 64 KiB dictionary). The dictionary's length is counted once
//! per dataset. Times are wall-clock times of the compress and decompress
//! calls only, on one thread; each block is decompressed and compared with
//! its raw bytes.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const HEADER: &str = "dataset,messages,unique,unique_payload_bytes,block,level,dict,blocks,raw_bytes,compressed_bytes,dict_bytes,total_bytes,bytes_per_msg,ratio_to_baseline,compress_ms_per_mb,decompress_ms_per_mb,dict_train_ms";
const BLOCKS: [Block; 9] = [
    Block::Bytes(4 << 10),
    Block::Bytes(16 << 10),
    Block::Bytes(64 << 10),
    Block::Bytes(256 << 10),
    Block::Bytes(1 << 20),
    Block::Batch(1),
    Block::Batch(8),
    Block::Batch(64),
    Block::Batch(512),
];
const LEVELS: [i32; 2] = [1, 3];
const BASELINE: (Block, i32, bool) = (Block::Bytes(1 << 20), 3, false);
const DICT_SAMPLE_SHARE: usize = 100;
const DICT_SAMPLE_CAP: usize = 10 << 20;
const DICT_SIZE: usize = 64 << 10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Block {
    Bytes(usize),
    Batch(usize),
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Block::Bytes(n) => write!(f, "bytes:{n}"),
            Block::Batch(n) => write!(f, "batch:{n}"),
        }
    }
}

struct Dataset {
    name: String,
    files: Vec<PathBuf>,
}

struct Row {
    block: Block,
    level: i32,
    dict: bool,
    blocks: u64,
    raw: u64,
    compressed: u64,
    dict_bytes: u64,
    compress: Duration,
    decompress: Duration,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("block-survey: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (out, datasets) = parse_args()?;
    let mut csv = String::from(HEADER);
    csv.push('\n');
    for ds in &datasets {
        let mut data = Vec::new();
        for f in &ds.files {
            data.extend_from_slice(&std::fs::read(f)?);
            if data.last().is_some_and(|&b| b != b'\n') {
                data.push(b'\n');
            }
        }
        let msgs: Vec<&[u8]> = lines(&data).collect();
        let mut seen = HashSet::with_capacity(msgs.len());
        let first: Vec<bool> = msgs.iter().map(|m| seen.insert(*m)).collect();
        drop(seen);
        let unique: Vec<&[u8]> = msgs
            .iter()
            .zip(&first)
            .filter_map(|(m, &f)| f.then_some(*m))
            .collect();
        let unique_bytes: u64 = unique.iter().map(|m| m.len() as u64).sum();

        let samples_wanted = msgs.len().div_ceil(DICT_SAMPLE_SHARE);
        let mut sample_bytes = 0;
        let samples: Vec<&[u8]> = msgs[..samples_wanted]
            .iter()
            .take_while(|m| {
                sample_bytes += m.len();
                sample_bytes <= DICT_SAMPLE_CAP
            })
            .copied()
            .collect();
        let t = Instant::now();
        let dict = zstd::dict::from_samples(&samples, DICT_SIZE)?;
        let train = t.elapsed();
        eprintln!(
            "{}: {} messages, {} unique, dictionary {} bytes from {} samples in {:?}",
            ds.name,
            msgs.len(),
            unique.len(),
            dict.len(),
            samples.len(),
            train
        );

        let mut rows = Vec::new();
        for block in BLOCKS {
            for level in LEVELS {
                for use_dict in [false, true] {
                    let d = use_dict.then_some(dict.as_slice());
                    let row = measure(block, level, d, &msgs, &first, &unique)?;
                    eprintln!(
                        "{} {block} level {level} dict {use_dict}: {} bytes",
                        ds.name,
                        row.compressed + row.dict_bytes
                    );
                    rows.push(row);
                }
            }
        }
        let total = |r: &Row| r.compressed + r.dict_bytes;
        let baseline = rows
            .iter()
            .find(|r| (r.block, r.level, r.dict) == BASELINE)
            .map(total)
            .ok_or("baseline case missing")?;
        for r in &rows {
            let mb = r.raw as f64 / 1e6;
            csv.push_str(&format!(
                "{},{},{},{unique_bytes},{},{},{},{},{},{},{},{},{:.3},{:.4},{:.3},{:.3},{:.1}\n",
                ds.name,
                msgs.len(),
                unique.len(),
                r.block,
                r.level,
                r.dict,
                r.blocks,
                r.raw,
                r.compressed,
                r.dict_bytes,
                total(r),
                total(r) as f64 / msgs.len() as f64,
                total(r) as f64 / baseline as f64,
                r.compress.as_secs_f64() * 1e3 / mb,
                r.decompress.as_secs_f64() * 1e3 / mb,
                train.as_secs_f64() * 1e3,
            ));
        }
    }
    std::fs::write(&out, csv)?;
    Ok(())
}

/// Compresses every block of one case and decompresses it again.
fn measure(
    block: Block,
    level: i32,
    dict: Option<&[u8]>,
    msgs: &[&[u8]],
    first: &[bool],
    unique: &[&[u8]],
) -> std::io::Result<Row> {
    let (mut c, mut d) = match dict {
        Some(dict) => (
            zstd::bulk::Compressor::with_dictionary(level, dict)?,
            zstd::bulk::Decompressor::with_dictionary(dict)?,
        ),
        None => (
            zstd::bulk::Compressor::new(level)?,
            zstd::bulk::Decompressor::new()?,
        ),
    };
    let mut row = Row {
        block,
        level,
        dict: dict.is_some(),
        blocks: 0,
        raw: 0,
        compressed: 0,
        dict_bytes: dict.map_or(0, |d| d.len() as u64),
        compress: Duration::ZERO,
        decompress: Duration::ZERO,
    };
    let (mut packed, mut back) = (Vec::new(), Vec::new());
    let mut one = |raw: &[u8]| -> std::io::Result<()> {
        packed.clear();
        packed.reserve(zstd::compress_bound(raw.len()));
        let t = Instant::now();
        let n = c.compress_to_buffer(raw, &mut packed)?;
        row.compress += t.elapsed();
        back.clear();
        back.reserve(raw.len());
        let t = Instant::now();
        d.decompress_to_buffer(&packed[..n], &mut back)?;
        row.decompress += t.elapsed();
        if back != raw {
            return Err(std::io::Error::other("round trip changed a block"));
        }
        row.blocks += 1;
        row.raw += raw.len() as u64;
        row.compressed += n as u64;
        Ok(())
    };
    let mut buf = Vec::new();
    match block {
        Block::Bytes(limit) => {
            for m in unique {
                let len = varint_len(m.len() as u64) + m.len();
                if !buf.is_empty() && buf.len() + len > limit {
                    one(&buf)?;
                    buf.clear();
                }
                push_object(&mut buf, m);
            }
            if !buf.is_empty() {
                one(&buf)?;
            }
        }
        Block::Batch(n) => {
            for (batch, new) in msgs.chunks(n).zip(first.chunks(n)) {
                buf.clear();
                for (m, _) in batch.iter().zip(new).filter(|(_, f)| **f) {
                    push_object(&mut buf, m);
                }
                if !buf.is_empty() {
                    one(&buf)?;
                }
            }
        }
    }
    Ok(row)
}

fn push_object(buf: &mut Vec<u8>, m: &[u8]) {
    push_varint(buf, m.len() as u64);
    buf.extend_from_slice(m);
}

/// Unsigned LEB128, as in the WAL.
fn push_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push(v as u8 | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

fn varint_len(v: u64) -> usize {
    (64 - v.leading_zeros() as usize).max(1).div_ceil(7)
}

/// Lines without their `\n`; a trailing `\r` is kept (part of the line's
/// bytes). The empty piece after the last newline is not a line.
fn lines(data: &[u8]) -> impl Iterator<Item = &[u8]> {
    let data = data.strip_suffix(b"\n").unwrap_or(data);
    data.split(|&b| b == b'\n')
        .filter(move |_| !data.is_empty())
}

fn parse_args() -> Result<(PathBuf, Vec<Dataset>), String> {
    let usage =
        "usage: block-survey --out <file.csv> --dataset <name>:<file>[,<file>...] [--dataset ...]";
    let (mut out, mut datasets) = (None, Vec::new());
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        let v = args.next().ok_or(usage)?;
        match a.as_str() {
            "--out" => out = Some(PathBuf::from(v)),
            "--dataset" => {
                let (name, files) = v.split_once(':').ok_or(usage)?;
                datasets.push(Dataset {
                    name: name.to_string(),
                    files: files.split(',').map(PathBuf::from).collect(),
                });
            }
            _ => return Err(usage.into()),
        }
    }
    if datasets.is_empty() {
        return Err(usage.into());
    }
    Ok((out.ok_or(usage)?, datasets))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_len_matches_encoding() {
        for v in [0u64, 1, 127, 128, 16_383, 16_384, u32::MAX as u64, u64::MAX] {
            let mut buf = Vec::new();
            push_varint(&mut buf, v);
            assert_eq!(varint_len(v), buf.len(), "{v}");
        }
    }

    #[test]
    fn byte_blocks_respect_the_limit_and_keep_every_object() {
        let objs: Vec<Vec<u8>> = (0..50u8).map(|i| vec![i; 30 + i as usize]).collect();
        let unique: Vec<&[u8]> = objs.iter().map(Vec::as_slice).collect();
        let row = measure(Block::Bytes(256), 1, None, &unique, &[true; 50], &unique).unwrap();
        let expected: u64 = unique.iter().map(|m| 1 + m.len() as u64).sum();
        assert_eq!(row.raw, expected);
        assert!(row.blocks >= expected / 256);
    }

    #[test]
    fn batch_blocks_hold_only_first_occurrences() {
        let msgs: Vec<&[u8]> = vec![b"a", b"b", b"a", b"c", b"b", b"d"];
        let first = [true, true, false, true, false, true];
        let unique: Vec<&[u8]> = vec![b"a", b"b", b"c", b"d"];
        let row = measure(Block::Batch(3), 1, None, &msgs, &first, &unique).unwrap();
        assert_eq!((row.blocks, row.raw), (2, 8));
        let row = measure(Block::Batch(1), 1, None, &msgs, &first, &unique).unwrap();
        assert_eq!(row.blocks, 4);
    }
}
