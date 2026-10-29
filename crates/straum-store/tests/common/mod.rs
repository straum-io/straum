#![allow(dead_code)] // each test binary uses a different subset
#![allow(clippy::unwrap_used)]

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The first WAL file, the only one below `Config::wal_file_max_bytes`.
pub const WAL0: &str = "wal-0000000000000000.log";

/// A fresh directory under the system temp dir, removed on drop.
pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("straum-store-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Byte offsets where each WAL frame starts (docs/format.md: a 24-byte file
/// header, then frames of `21 + len` bytes with `len u32` first).
pub fn wal_frame_starts(wal: &[u8]) -> Vec<usize> {
    let mut starts = Vec::new();
    let mut pos = 24;
    while pos + 21 <= wal.len() {
        starts.push(pos);
        let len = u32::from_le_bytes(wal[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 21 + len;
    }
    assert_eq!(pos, wal.len(), "WAL does not end on a frame boundary");
    starts
}

/// Every file under `dir` with its bytes, to prove a directory unchanged.
pub fn snapshot(dir: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    let mut files = std::collections::BTreeMap::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.insert(path.clone(), std::fs::read(&path).unwrap());
            }
        }
    }
    files
}

/// Deterministic xorshift64*, so failures reproduce from the printed seed.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

/// An index record found in an index segment (docs/format.md: `kind u8 |
/// len varint | topic varint | number varint | stamp varint | body | crc
/// u32` after a 24-byte header).
#[derive(Clone, Debug)]
pub struct IndexRecord {
    pub path: PathBuf,
    pub offset: usize,
    pub size: usize,
    pub kind: u8,
    pub topic: u64,
    pub number: u64,
    pub stamp: u64,
    /// The body's byte range in the file.
    pub body: std::ops::Range<usize>,
}

pub const INDEX_BLOCK_RECORD: u8 = 1;
pub const INDEX_SEEK_PAGE: u8 = 2;
pub const INDEX_ATTRS_RECORD: u8 = 3;

fn varint(b: &[u8], pos: &mut usize) -> u64 {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let x = b[*pos];
        *pos += 1;
        v |= u64::from(x & 0x7f) << shift;
        if x & 0x80 == 0 {
            break;
        }
    }
    v
}

/// The store's index segments in id order.
pub fn index_segments(dir: &Path) -> Vec<PathBuf> {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap().to_str().unwrap().starts_with("idx-"))
        .collect();
    paths.sort();
    paths
}

/// Every intact record of every index segment, in file order; a segment is
/// read up to its first damaged or unwritten record.
pub fn index_records(dir: &Path) -> Vec<IndexRecord> {
    let mut out = Vec::new();
    for path in index_segments(dir) {
        let bytes = std::fs::read(&path).unwrap();
        let mut at = 24;
        while at + 6 < bytes.len() {
            let mut pos = at + 1;
            let len = varint(&bytes, &mut pos) as usize;
            let topic = varint(&bytes, &mut pos);
            let number = varint(&bytes, &mut pos);
            let stamp = varint(&bytes, &mut pos);
            let end = pos + len;
            if end + 4 > bytes.len()
                || crc32c::crc32c(&bytes[at..end]).to_le_bytes() != bytes[end..end + 4]
            {
                break;
            }
            out.push(IndexRecord {
                path: path.clone(),
                offset: at,
                size: end + 4 - at,
                kind: bytes[at],
                topic,
                number,
                stamp,
                body: pos..end,
            });
            at = end + 4;
        }
    }
    out
}

/// The record of `kind` with `number` (topic 0 unless given).
pub fn find_index_record(dir: &Path, kind: u8, topic: u64, number: u64) -> IndexRecord {
    index_records(dir)
        .into_iter()
        .find(|r| r.kind == kind && r.topic == topic && r.number == number)
        .unwrap()
}

/// Recomputes a record's CRC after its bytes were changed in place.
pub fn reseal_index_record(bytes: &mut [u8], offset: usize, size: usize) {
    let end = offset + size - 4;
    let crc = crc32c::crc32c(&bytes[offset..end]);
    bytes[end..offset + size].copy_from_slice(&crc.to_le_bytes());
}

/// Applies `f` to the bytes of `path`.
pub fn edit(path: &Path, f: impl FnOnce(&mut Vec<u8>)) {
    let mut bytes = std::fs::read(path).unwrap();
    f(&mut bytes);
    std::fs::write(path, bytes).unwrap();
}
