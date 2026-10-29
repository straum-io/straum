//! Shared code of the fuzz targets: the protocol check, the store
//! directory archive, the base store and the recovery invariants.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use straum_proto::{
    FRAME_HEADER, FrameHeader, Request, Response, encode_request, encode_response, split_frame,
};
use straum_store::{Config, Message, RecoveryAction, RecoveryReport, Store, WriteMode};

// ---------------------------------------------------------------- protocol

/// Frame splitting, header parsing and every request and response decoder.
/// A decoded value must encode back to the exact input bytes: the encoding
/// of a value is unique, so a decoder that accepts anything else is lenient.
pub fn proto_frame(data: &[u8]) {
    if let Some(head) = data.first_chunk::<FRAME_HEADER>() {
        let h = FrameHeader::parse(head);
        assert_eq!(h.frame_size(), 4 + u64::from(h.length));
        assert_eq!(h.body_len().is_some(), h.length >= 10);
    }
    match split_frame(data) {
        Some((h, body)) => {
            assert_eq!(h.frame_size(), data.len() as u64);
            assert_eq!(h.body_len(), Some(body.len()));
            decode_all(h.msg_type, h.correlation_id, body, Some(data));
        }
        None => {
            if let Some(head) = data.first_chunk::<FRAME_HEADER>() {
                let h = FrameHeader::parse(head);
                assert_ne!(h.body_len(), Some(data.len() - FRAME_HEADER));
            }
        }
    }
    // Every decoder on every body, whatever the length field says.
    if let Some((t, body)) = data.split_first_chunk::<2>() {
        decode_all(u16::from_le_bytes(*t), 7, body, None);
    }
}

fn decode_all(msg_type: u16, correlation_id: u64, body: &[u8], frame: Option<&[u8]>) {
    if let Ok(req) = Request::decode(msg_type, body) {
        assert_eq!(req.msg_type(), msg_type);
        let mut out = Vec::new();
        encode_request(&mut out, correlation_id, &req);
        assert_eq!(&out[FRAME_HEADER..], body, "request {}", req.name());
        if let Some(f) = frame {
            assert_eq!(out, f);
        }
    }
    if let Ok(resp) = Response::decode(msg_type, body) {
        assert_eq!(resp.msg_type(), msg_type);
        let mut out = Vec::new();
        encode_response(&mut out, correlation_id, &resp);
        assert_eq!(&out[FRAME_HEADER..], body, "response {}", resp.name());
        if let Some(f) = frame {
            assert_eq!(out, f);
        }
    }
}

// ---------------------------------------------------------------- archive

/// A store directory as fuzz input: `flags u8`, then entries `name_len u8 |
/// name | len u32 | bytes` (a length past the end takes the rest). Flag bit 0
/// recomputes every CRC of WAL files, manifests, object tables and
/// relocation runs, so mutations reach the parsers behind the checksums.
pub struct Archive {
    pub reseal: bool,
    pub files: Vec<(String, Vec<u8>)>,
}

const MAX_FILES: usize = 32;

impl Archive {
    pub fn parse(data: &[u8]) -> Self {
        let (reseal, mut rest) = match data.split_first() {
            Some((f, rest)) => (f & 1 != 0, rest),
            None => (false, data),
        };
        let mut files: Vec<(String, Vec<u8>)> = Vec::new();
        while files.len() < MAX_FILES {
            let Some((&n, r)) = rest.split_first() else {
                break;
            };
            let Some(name) = r.get(..n as usize) else {
                break;
            };
            let r = &r[n as usize..];
            let Some((len, r)) = r.split_first_chunk::<4>() else {
                break;
            };
            let len = (u32::from_le_bytes(*len) as usize).min(r.len());
            let (bytes, r) = r.split_at(len);
            rest = r;
            let Some(name) = valid_name(name) else {
                continue;
            };
            files.retain(|(n, _)| *n != name);
            files.push((name, bytes.to_vec()));
        }
        if reseal {
            for (name, bytes) in &mut files {
                reseal_file(name, bytes);
            }
        }
        Self { reseal, files }
    }

    pub fn encode(reseal: bool, files: &[(String, Vec<u8>)]) -> Vec<u8> {
        let mut out = vec![u8::from(reseal)];
        for (name, bytes) in files {
            out.push(name.len() as u8);
            out.extend_from_slice(name.as_bytes());
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(bytes);
        }
        out
    }
}

/// A relative path of one or two components of `[A-Za-z0-9._+-]`, none
/// `.` or `..`: a file in the store directory or in a directory of it.
fn valid_name(name: &[u8]) -> Option<String> {
    let name = std::str::from_utf8(name).ok()?;
    let parts: Vec<&str> = name.split('/').collect();
    let ok = parts.len() <= 2
        && parts.iter().all(|p| {
            !p.is_empty()
                && *p != "."
                && *p != ".."
                && p.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._+-".contains(&b))
        });
    ok.then(|| name.to_owned())
}

fn put_crc(bytes: &mut [u8], at: usize, crc: u32) {
    bytes[at..at + 4].copy_from_slice(&crc.to_le_bytes());
}

/// Recomputes the CRCs of a file by its name (docs/format.md of
/// straum-store): WAL file header and frames, manifest, table and run
/// header and pages. Other files are left as they are.
pub fn reseal_file(name: &str, b: &mut [u8]) {
    let base = name.rsplit('/').next().unwrap_or(name);
    if base.starts_with("wal-") {
        if b.len() < 24 {
            return;
        }
        put_crc(b, 20, crc32c::crc32c(&b[..20]));
        let mut pos = 24;
        while pos + 21 <= b.len() {
            let len = u32::from_le_bytes([b[pos], b[pos + 1], b[pos + 2], b[pos + 3]]) as usize;
            let Some(end) = (pos + 21).checked_add(len).filter(|e| *e <= b.len()) else {
                break;
            };
            let crc = crc32c::crc32c_append(crc32c::crc32c(&b[pos..pos + 17]), &b[pos + 21..end]);
            put_crc(b, pos + 17, crc);
            pos = end;
        }
    } else if base.starts_with("checkpoint-") {
        if let Some(end) = b.len().checked_sub(4) {
            put_crc(b, end, crc32c::crc32c(&b[..end]));
        }
    } else if base.starts_with("objtab-") || base.starts_with("reloc-") {
        if b.len() < 24 {
            return;
        }
        put_crc(b, 20, crc32c::crc32c(&b[..20]));
        let entry = if b.starts_with(b"STRMOBJ\0") { 44 } else { 20 };
        let page = 4092 / entry * entry + 4;
        let mut pos = 24;
        for k in 0u64.. {
            let end = (pos + page).min(b.len());
            if end < pos + 4 {
                break;
            }
            let crc = crc32c::crc32c_append(crc32c::crc32c(&k.to_le_bytes()), &b[pos..end - 4]);
            put_crc(b, end - 4, crc);
            pos = end;
        }
    }
}

// ---------------------------------------------------------------- stores

/// The configuration of every store here: small blocks, segments, WAL
/// files, index segments and seek pages, so a small store has several of
/// each, and a dictionary.
pub fn config(write_mode: WriteMode) -> Config {
    Config {
        write_mode,
        segment_max_bytes: 4096,
        max_block_raw_bytes: 512,
        dict_train_bytes: 4096,
        wal_file_max_bytes: 16 << 10,
        index_segment_max_bytes: 2048,
        seek_page_entries: 2,
        block_cache_bytes: 1 << 20,
        ..Config::default()
    }
}

fn runtime() -> &'static compio::runtime::Runtime {
    thread_local! {
        static RT: &'static compio::runtime::Runtime =
            Box::leak(Box::new(compio::runtime::Runtime::new().expect("compio runtime")));
    }
    RT.with(|rt| *rt)
}

fn block_on<F: Future>(f: F) -> F::Output {
    runtime().block_on(f)
}

fn payload(i: u64) -> Vec<u8> {
    // Every seventh repeats an earlier body: deduplicated.
    let i = if i % 7 == 6 && i >= 20 { i - 20 } else { i };
    format!("event {i} {}", "z".repeat((i % 50) as usize)).into_bytes()
}

/// A store written through the public API, deterministically: two topics,
/// keys and headers, a dictionary, releases, compaction and two
/// checkpoints. Returns every file, and the checkpoint files after the
/// first checkpoint.
pub struct Built {
    pub files: Vec<(String, Vec<u8>)>,
    pub first_checkpoint: Vec<(String, Vec<u8>)>,
}

pub fn build_store(dir: &Path, mode: WriteMode) -> Built {
    reset_dir(dir);
    block_on(async {
        let mut store = Store::open(dir, config(mode)).await.expect("open");
        let mut first_checkpoint = Vec::new();
        for b in 0..12u64 {
            let bodies: Vec<Vec<u8>> = (b * 30..(b + 1) * 30).map(payload).collect();
            let keys: Vec<Vec<u8>> = (0..30u64)
                .map(|i| format!("k{}", i % 4).into_bytes())
                .collect();
            let headers: &[(&[u8], &[u8])] = &[(b"h", b"v"), (b"trace", b"")];
            let batch: Vec<Message> = bodies
                .iter()
                .zip(&keys)
                .enumerate()
                .map(|(i, (p, k))| Message {
                    topic: if i % 3 == 0 { b"b" } else { b"a" },
                    payload: p,
                    key: (i % 2 == 0).then_some(k.as_slice()),
                    headers: if i % 5 == 0 { headers } else { &[] },
                })
                .collect();
            store.put_batch(&batch, b * 1000).await.expect("put");
            if b == 4 {
                store.release_before(b"a", 2500).await.expect("release");
                store.compact().await.expect("compact");
                store.checkpoint().await.expect("checkpoint");
                first_checkpoint = read_dir_files(dir)
                    .into_iter()
                    .filter(|(n, _)| is_checkpoint_file(n))
                    .collect();
            }
            if b == 8 {
                store.release_before(b"b", 7000).await.expect("release");
                store.compact().await.expect("compact");
                store.checkpoint().await.expect("checkpoint");
            }
        }
        drop(store);
        Built {
            files: read_dir_files(dir),
            first_checkpoint,
        }
    })
}

pub fn is_checkpoint_file(name: &str) -> bool {
    ["checkpoint-", "objtab-", "reloc-", "quarantine-"]
        .iter()
        .any(|p| name.starts_with(p))
}

/// The base store of the fuzz targets, built once per process.
pub fn base_store() -> &'static Built {
    static BASE: OnceLock<Built> = OnceLock::new();
    BASE.get_or_init(|| {
        let dir = scratch().with_extension("base");
        let built = build_store(&dir, WriteMode::Sequential);
        reset_dir(&dir);
        let _ = std::fs::remove_dir(&dir);
        built
    })
}

/// A directory of this thread for the store under test (`STRAUM_FUZZ_DIR`,
/// else the system temp dir).
pub fn scratch() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    thread_local! {
        static N: u64 = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    let root = std::env::var_os("STRAUM_FUZZ_DIR").map_or_else(std::env::temp_dir, PathBuf::from);
    root.join(format!(
        "straum-fuzz-{}-{}",
        std::process::id(),
        N.with(|n| *n)
    ))
}

fn reset_dir(dir: &Path) {
    if dir.exists() {
        std::fs::remove_dir_all(dir).expect("remove scratch dir");
    }
    std::fs::create_dir_all(dir).expect("create scratch dir");
}

/// Every file and directory under `dir` by relative path; `None` for a
/// directory.
type Snapshot = BTreeMap<String, Option<Vec<u8>>>;

fn snapshot(dir: &Path) -> Snapshot {
    let mut out = BTreeMap::new();
    let mut stack = vec![(dir.to_path_buf(), String::new())];
    while let Some((d, prefix)) = stack.pop() {
        for entry in std::fs::read_dir(&d).expect("read dir") {
            let entry = entry.expect("dir entry");
            let rel = format!("{prefix}{}", entry.file_name().to_string_lossy());
            if entry.file_type().expect("file type").is_dir() {
                out.insert(rel.clone(), None);
                stack.push((entry.path(), format!("{rel}/")));
            } else {
                out.insert(rel, Some(std::fs::read(entry.path()).expect("read file")));
            }
        }
    }
    out
}

fn read_dir_files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    snapshot(dir)
        .into_iter()
        .filter_map(|(n, b)| b.map(|b| (n, b)))
        .collect()
}

/// `base` overlaid with the archive in `data`, opened twice. Invariants:
/// no panic; an open that fails changes no file; an open that succeeds
/// changes only what its `RecoveryReport` lists; the reopen after it
/// reports and changes nothing.
pub fn store_open(base: &[(String, Vec<u8>)], data: &[u8]) {
    let archive = Archive::parse(data);
    let dir = scratch();
    reset_dir(&dir);
    for (name, bytes) in base.iter().chain(&archive.files) {
        let path = dir.join(name);
        if let Some(parent) = path.parent() {
            // A file of the archive may already hold the parent's name.
            if std::fs::create_dir_all(parent).is_err() {
                continue;
            }
        }
        let _ = std::fs::write(&path, bytes);
    }
    check_dir(&dir);
    reset_dir(&dir);
}

/// Opens the store in `dir` twice and checks the invariants of
/// `store_open`.
pub fn check_dir(dir: &Path) {
    let before = snapshot(dir);
    let first = block_on(open_report(dir));
    if std::env::var_os("STRAUM_FUZZ_VERBOSE").is_some() {
        eprintln!("{first:?}");
    }
    let after = snapshot(dir);
    match &first {
        Err(e) => assert_unchanged(
            &before,
            &after,
            &format!("open failed ({e}) and changed files"),
        ),
        Ok(report) => check_explained(&before, &after, report),
    }
    if first.is_ok() {
        let second = block_on(open_report(dir));
        let again = snapshot(dir);
        match second {
            Err(e) => assert_unchanged(
                &after,
                &again,
                &format!("reopen failed ({e}) and changed files"),
            ),
            Ok(report) => {
                // F1 (open, lane A): dropping the last ParallelSync frame
                // leaves the frame before it last, unchecked until the next
                // open drops it too.
                let dropped = |r: &RecoveryReport| {
                    r.actions
                        .iter()
                        .any(|a| matches!(a, RecoveryAction::FrameDropped { .. }))
                };
                if known("F1") && first.as_ref().is_ok_and(dropped) && dropped(&report) {
                    return;
                }
                assert!(
                    report.is_empty(),
                    "reopen after a successful open repaired again: {:?}\nfirst open: {:?}",
                    report.actions,
                    first
                );
                assert_unchanged(&after, &again, "reopen with an empty report changed files");
            }
        }
    }
}

/// A finding already handed over, named in `STRAUM_FUZZ_KNOWN`
/// (comma-separated), is passed over so a run can look for others.
fn known(id: &str) -> bool {
    std::env::var("STRAUM_FUZZ_KNOWN").is_ok_and(|v| v.split(',').any(|k| k == id))
}

async fn open_report(dir: &Path) -> Result<RecoveryReport, straum_store::StoreError> {
    let store = Store::open(dir, config(WriteMode::Sequential)).await?;
    Ok(store.recovery_report().clone())
}

fn diff(before: &Snapshot, after: &Snapshot) -> Vec<String> {
    let mut changed: Vec<String> = before
        .iter()
        .filter(|(k, v)| after.get(*k) != Some(*v))
        .map(|(k, _)| k.clone())
        .collect();
    changed.extend(after.keys().filter(|k| !before.contains_key(*k)).cloned());
    changed
}

fn assert_unchanged(before: &Snapshot, after: &Snapshot, what: &str) {
    let changed = diff(before, after);
    assert!(changed.is_empty(), "{what}: {changed:?}");
}

/// Every changed path must be one an action of `report` names, changed the
/// way that action says.
fn check_explained(before: &Snapshot, after: &Snapshot, report: &RecoveryReport) {
    let had_wal = before
        .keys()
        .any(|k| k.starts_with("wal-") && k.ends_with(".log"));
    let index_repaired = report.actions.iter().any(|a| {
        matches!(
            a,
            RecoveryAction::TopicIndexRepaired { .. }
                | RecoveryAction::StaleIndexRecordsFenced { .. }
                | RecoveryAction::IndexSegmentDropped { .. }
        )
    });
    for path in diff(before, after) {
        let (old, new) = (before.get(&path), after.get(&path));
        let ok = report.actions.iter().any(|a| match a {
            RecoveryAction::WalTmpRemoved => {
                new.is_none() && path.starts_with("wal-") && path.ends_with(".log.tmp")
            }
            RecoveryAction::TornTailTruncated {
                file,
                offset,
                bytes_removed,
                ..
            }
            | RecoveryAction::FrameDropped {
                file,
                offset,
                bytes_removed,
                ..
            } => {
                path == format!("wal-{file:016x}.log")
                    && matches!((old, new), (Some(Some(o)), Some(Some(n)))
                        if n.len() as u64 == *offset
                            && o.len() as u64 == offset + bytes_removed
                            && o.starts_with(n))
            }
            RecoveryAction::OrphanSegmentRemoved { segment } => {
                new.is_none() && path == format!("seg-{segment:016x}.dat")
            }
            RecoveryAction::IndexSegmentRemoved { segment, .. }
            | RecoveryAction::IndexSegmentDropped { segment, .. } => {
                new.is_none() && path == format!("idx-{segment:016x}.dat")
            }
            RecoveryAction::CheckpointFileRemoved { file } => new.is_none() && path == *file,
            RecoveryAction::CheckpointManifestDamaged { quarantine, .. } => {
                old.is_none() && new == Some(&None) && path == *quarantine
            }
            RecoveryAction::CheckpointFileQuarantined { file, quarantine } => {
                (path == *file && new.is_none())
                    || (path == format!("{quarantine}/{file}")
                        && old.is_none()
                        && new.is_some()
                        && new == before.get(file))
            }
            _ => false,
        });
        // The index is derived: repairs append to index segments or add new
        // ones, never rewrite what is there.
        let index_ok = index_repaired
            && path.starts_with("idx-")
            && match (old, new) {
                (None, Some(Some(_))) => true,
                (Some(Some(o)), Some(Some(n))) => n.starts_with(o),
                _ => false,
            };
        // A new store: the directory held no WAL file, open created the first.
        let fresh = !had_wal && path == "wal-0000000000000000.log" && old.is_none();
        assert!(
            ok || index_ok || fresh,
            "open changed {path} (before: {:?} bytes, after: {:?} bytes) without reporting it: {:?}",
            old.map(|o| o.as_ref().map(Vec::len)),
            new.map(|n| n.as_ref().map(Vec::len)),
            report.actions
        );
    }
}

/// The base of `store_wal`: the data segments only.
pub fn wal_base(built: &Built) -> Vec<(String, Vec<u8>)> {
    built
        .files
        .iter()
        .filter(|(n, _)| n.starts_with("seg-"))
        .cloned()
        .collect()
}

/// The base of `store_checkpoint`: everything but the checkpoint files.
pub fn checkpoint_base(built: &Built) -> Vec<(String, Vec<u8>)> {
    built
        .files
        .iter()
        .filter(|(n, _)| !is_checkpoint_file(n))
        .cloned()
        .collect()
}

// ---------------------------------------------------------------- seeds

/// The seed corpora as `(target, name, bytes)`: the protocol's golden
/// frames (`conformance/frames`), and store files written through the
/// store's API as archives, intact and in the damaged states recovery
/// handles.
pub fn seeds() -> Vec<(&'static str, String, Vec<u8>)> {
    let mut out = Vec::new();
    let frames = Path::new(env!("CARGO_MANIFEST_DIR")).join("../conformance/frames");
    for entry in std::fs::read_dir(&frames).expect("conformance/frames") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|e| e == "bin") {
            let name = path
                .file_name()
                .expect("name")
                .to_string_lossy()
                .into_owned();
            out.push((
                "proto_frame",
                name,
                std::fs::read(&path).expect("read frame"),
            ));
        }
    }

    let base = base_store();
    let pick = |files: &[(String, Vec<u8>)], f: fn(&str) -> bool| -> Vec<(String, Vec<u8>)> {
        files.iter().filter(|(n, _)| f(n)).cloned().collect()
    };
    let is_wal = |n: &str| n.starts_with("wal-");
    let wal = pick(&base.files, is_wal);
    out.push((
        "store_wal",
        "sequential".into(),
        Archive::encode(true, &wal),
    ));
    out.push((
        "store_wal",
        "sequential-raw".into(),
        Archive::encode(false, &wal),
    ));
    out.push((
        "store_wal",
        "first-file".into(),
        Archive::encode(true, &wal[..1]),
    ));
    for (name, mode) in [
        ("parallel-sync", WriteMode::ParallelSync),
        ("inline-small", WriteMode::INLINE_SMALL),
    ] {
        let dir = scratch().with_extension("seed");
        let built = build_store(&dir, mode);
        let _ = std::fs::remove_dir_all(&dir);
        out.push((
            "store_wal",
            name.into(),
            Archive::encode(true, &pick(&built.files, is_wal)),
        ));
    }

    let last = pick(&base.files, is_checkpoint_file);
    let first = &base.first_checkpoint;
    let mut both = first.clone();
    both.extend(last.iter().cloned());
    // checkpoint-2 damaged: open falls back to checkpoint-1 and quarantines 2.
    let mut damaged = both.clone();
    for (n, b) in &mut damaged {
        if n.starts_with("checkpoint-") && !first.iter().any(|(f, _)| f == n) {
            b[24] ^= 0xff;
        }
    }
    // An interrupted quarantine of the damaged checkpoint-2: its run moved.
    let mut interrupted = damaged.clone();
    for (n, _) in &mut interrupted {
        if n.starts_with("reloc-") {
            *n = format!("quarantine-0000000000000002/{n}");
        }
    }
    let mut tmp = last.clone();
    if let Some((_, b)) = last.iter().find(|(n, _)| n.starts_with("checkpoint-")) {
        tmp.push(("checkpoint-0000000000000003.tmp".into(), b.clone()));
    }
    for (name, reseal, files) in [
        ("last", true, &last),
        ("first", true, first),
        ("two-manifests", true, &both),
        ("damaged-newest", false, &damaged),
        ("interrupted-quarantine", false, &interrupted),
        ("tmp-manifest", true, &tmp),
    ] {
        out.push((
            "store_checkpoint",
            name.into(),
            Archive::encode(reseal, files),
        ));
    }
    out
}
