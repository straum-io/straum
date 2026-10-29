//! Deterministic simulation of the disk and the clock (feature `sim`, a
//! spike). One simulated disk per thread, created by [`install`] from a
//! seed. Every call the store makes is numbered and hashed into a trace; a
//! fault can be planned at any call number.
//!
//! Crash model ([`power_loss`]):
//! * file data: what the last successful `sync_data`/`sync_all` of the file
//!   made durable, plus each later write kept whole, dropped, or torn to a
//!   seeded prefix (independently, in order); a later `set_len` kept or
//!   dropped;
//! * directory entries: every create, remove and rename is durable once
//!   each directory it touches was synced after it; of the others, a seeded
//!   prefix in call order survives, or each one independently ([`Model`]);
//! * a failed file sync (planned `Fault::Eio`) drops that file's unsynced
//!   writes from what can become durable while reads still see them, as
//!   Linux does after a writeback error.
//!
//! All calls complete immediately: no future of this module is ever
//! pending, so no executor or I/O driver takes part.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Fault {
    /// Power is cut before the call: it and every later call fail with an
    /// error and change nothing, until [`power_loss`].
    Crash,
    /// The call fails with an I/O error and changes nothing (a file sync
    /// also loses the file's unsynced writes); later calls work.
    Eio,
    /// A read returns its bytes with one byte flipped; other calls are
    /// unaffected.
    Corrupt,
}

/// How directory entries not yet made durable by a directory sync survive
/// a power loss.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Model {
    /// A seeded prefix of them, in call order; a rename is atomic, also
    /// between two directories (a journaled file system such as ext4).
    Ordered,
    /// Each one independently, half the time; a rename is atomic.
    Unordered,
    /// As `Unordered`, but a rename between two directories is a removal
    /// from one and an entry in the other, each durable only with its own
    /// directory's sync (POSIX promises no more).
    Posix,
}

/// splitmix64.
#[derive(Clone)]
pub struct Rng(pub u64);

impl Rng {
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` (`n > 0`; modulo bias is irrelevant here).
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n.max(1)
    }
}

enum Pending {
    Write(u64, Vec<u8>),
    SetLen(u64),
}

#[derive(Default)]
struct Inode {
    /// What reads see.
    data: Vec<u8>,
    /// What survives a power loss for certain.
    durable: Vec<u8>,
    /// Changes since the last sync, in order.
    pending: Vec<Pending>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Node {
    File(usize),
    Dir,
}

enum Meta {
    Create(PathBuf, Node),
    Remove(PathBuf),
    Rename(PathBuf, PathBuf),
}

struct MetaOp {
    meta: Meta,
    /// Directories touched by the operation and not synced since.
    unsynced: Vec<PathBuf>,
}

struct Disk {
    rng: Rng,
    model: Model,
    inodes: Vec<Inode>,
    live: BTreeMap<PathBuf, Node>,
    /// The namespace before the first operation of `log`.
    base: BTreeMap<PathBuf, Node>,
    log: Vec<MetaOp>,
    calls: u64,
    faults: BTreeMap<u64, Fault>,
    /// The fault of the current call, if any.
    hit: Option<Fault>,
    down: bool,
    trace: blake3::Hasher,
    /// `(call, "kind name")` of every call.
    sites: Vec<(u64, String)>,
    clock_ns: u64,
    epoch: Instant,
    stats: Stats,
}

/// Counts of what [`power_loss`] did, over the disk's life.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    pub power_losses: u64,
    pub writes_dropped: u64,
    pub writes_torn: u64,
    pub writes_kept: u64,
    pub entries_lost: u64,
    pub entries_kept: u64,
    pub sync_writes_lost: u64,
}

thread_local! {
    static DISK: RefCell<Option<Disk>> = const { RefCell::new(None) };
}

fn with<R>(f: impl FnOnce(&mut Disk) -> R) -> R {
    DISK.with_borrow_mut(|d| match d.as_mut() {
        Some(d) => f(d),
        None => panic!("straum_store::sim::install was not called on this thread"),
    })
}

/// A new, empty simulated disk for this thread, seeded.
pub fn install(seed: u64, model: Model) {
    let mut trace = blake3::Hasher::new();
    trace.update(&seed.to_le_bytes());
    let mut live = BTreeMap::new();
    live.insert(PathBuf::from("/"), Node::Dir);
    DISK.with_borrow_mut(|d| {
        *d = Some(Disk {
            rng: Rng(seed ^ 0x5eed),
            model,
            inodes: Vec::new(),
            base: live.clone(),
            live,
            log: Vec::new(),
            calls: 0,
            faults: BTreeMap::new(),
            hit: None,
            down: false,
            trace,
            sites: Vec::new(),
            clock_ns: 0,
            epoch: Instant::now(),
            stats: Stats::default(),
        });
    });
}

/// Plans `fault` at call number `call` (the first call is 1).
pub fn fail_at(call: u64, fault: Fault) {
    with(|d| d.faults.insert(call, fault));
}

/// Calls made so far.
pub fn calls() -> u64 {
    with(|d| d.calls)
}

/// Whether a planned crash has happened and [`power_loss`] not yet run.
pub fn is_down() -> bool {
    with(|d| d.down)
}

/// Adds the caller's events to the trace.
pub fn note(bytes: &[u8]) {
    with(|d| {
        d.trace.update(&(bytes.len() as u64).to_le_bytes());
        d.trace.update(bytes);
    });
}

pub fn trace() -> [u8; 32] {
    with(|d| *d.trace.finalize().as_bytes())
}

/// `(call, "kind name")` of every call so far.
pub fn sites() -> Vec<(u64, String)> {
    with(|d| d.sites.clone())
}

/// Flips one seeded byte of file `path`, durably (damage at rest).
pub fn damage(path: &Path) {
    with(|d| {
        if let Some(Node::File(i)) = d.live.get(path).copied()
            && !d.inodes[i].durable.is_empty()
        {
            let at = d.rng.below(d.inodes[i].durable.len() as u64) as usize;
            d.inodes[i].durable[at] ^= 0x20;
            let inode = &mut d.inodes[i];
            inode.data.clone_from(&inode.durable);
            d.trace.update(b"damage");
            d.trace.update(&(at as u64).to_le_bytes());
        }
    });
}

pub fn stats() -> Stats {
    with(|d| d.stats)
}

/// Names directly in `dir`, and their lengths (`None` for a directory).
pub fn listing(dir: &Path) -> BTreeMap<String, Option<u64>> {
    with(|d| {
        d.children(dir)
            .into_iter()
            .map(|(p, n)| {
                let len = match n {
                    Node::File(i) => Some(d.inodes[i].data.len() as u64),
                    Node::Dir => None,
                };
                (name(&p), len)
            })
            .collect()
    })
}

/// Cuts the power and boots again: the disk becomes a seeded crash image
/// (module docs), planned faults are cleared and calls work again.
pub fn power_loss() {
    with(|d| {
        d.stats.power_losses += 1;
        let unsynced = d.log.iter().filter(|o| !o.unsynced.is_empty()).count() as u64;
        let cut = d.rng.below(unsynced + 1);
        let mut ns = std::mem::take(&mut d.base);
        let mut k = 0;
        for op in std::mem::take(&mut d.log) {
            if !op.unsynced.is_empty() {
                k += 1;
                let lost = match d.model {
                    Model::Ordered => k > cut,
                    Model::Unordered | Model::Posix => d.rng.below(2) == 0,
                };
                if lost {
                    d.stats.entries_lost += 1;
                    continue;
                }
                d.stats.entries_kept += 1;
            }
            apply(&mut ns, op.meta);
        }
        for i in 0..d.inodes.len() {
            let mut img = std::mem::take(&mut d.inodes[i].durable);
            for p in std::mem::take(&mut d.inodes[i].pending) {
                match p {
                    Pending::Write(at, bytes) => {
                        let n = match d.rng.below(4) {
                            0 => {
                                d.stats.writes_dropped += 1;
                                0
                            }
                            1 => {
                                d.stats.writes_torn += 1;
                                d.rng.below(bytes.len() as u64 + 1) as usize
                            }
                            _ => {
                                d.stats.writes_kept += 1;
                                bytes.len()
                            }
                        };
                        if n > 0 {
                            write_at(&mut img, at, &bytes[..n]);
                        }
                    }
                    Pending::SetLen(len) => {
                        if d.rng.below(2) == 0 {
                            img.resize(len as usize, 0);
                        }
                    }
                }
            }
            d.inodes[i].data.clone_from(&img);
            d.inodes[i].durable = img;
        }
        d.live = ns.clone();
        d.base = ns;
        d.faults.clear();
        d.hit = None;
        d.down = false;
        // The image goes into the trace: the same seed gives the same disk.
        let image: Vec<(PathBuf, Node)> = d.live.iter().map(|(p, n)| (p.clone(), *n)).collect();
        for (p, n) in image {
            d.trace.update(p.as_os_str().as_encoded_bytes());
            if let Node::File(i) = n {
                d.trace.update(blake3::hash(&d.inodes[i].data).as_bytes());
            }
        }
    });
}

fn apply(ns: &mut BTreeMap<PathBuf, Node>, meta: Meta) {
    match meta {
        Meta::Create(p, n) => {
            ns.insert(p, n);
        }
        Meta::Remove(p) => {
            ns.remove(&p);
        }
        Meta::Rename(from, to) => {
            if let Some(n) = ns.remove(&from) {
                ns.insert(to, n);
            }
        }
    }
}

fn write_at(data: &mut Vec<u8>, at: u64, bytes: &[u8]) {
    let at = at as usize;
    if data.len() < at + bytes.len() {
        data.resize(at + bytes.len(), 0);
    }
    data[at..at + bytes.len()].copy_from_slice(bytes);
}

fn name(p: &Path) -> String {
    p.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn parent(p: &Path) -> PathBuf {
    p.parent()
        .map_or_else(|| PathBuf::from("/"), Path::to_path_buf)
}

fn not_found() -> io::Error {
    io::Error::from(io::ErrorKind::NotFound)
}

impl Disk {
    /// Numbers and traces a call, and applies a planned fault.
    fn begin(&mut self, kind: &str, path: &Path, a: u64, b: u64) -> io::Result<()> {
        self.calls += 1;
        self.hit = None;
        self.sites
            .push((self.calls, format!("{kind} {}", name(path))));
        self.trace.update(kind.as_bytes());
        self.trace.update(path.as_os_str().as_encoded_bytes());
        self.trace.update(&a.to_le_bytes());
        self.trace.update(&b.to_le_bytes());
        if !self.down
            && let Some(f) = self.faults.remove(&self.calls)
        {
            self.hit = Some(f);
            if f == Fault::Crash {
                self.down = true;
            }
        }
        if self.down {
            return Err(io::Error::other("simulated power loss"));
        }
        if self.hit == Some(Fault::Eio) {
            return Err(io::Error::other("simulated I/O error"));
        }
        Ok(())
    }

    fn corrupt(&mut self, mut b: Vec<u8>) -> Vec<u8> {
        if self.hit == Some(Fault::Corrupt) && !b.is_empty() {
            let at = self.rng.below(b.len() as u64) as usize;
            b[at] ^= 0x20;
        }
        b
    }

    fn children(&self, dir: &Path) -> Vec<(PathBuf, Node)> {
        self.live
            .iter()
            .filter(|(p, _)| p.parent() == Some(dir))
            .map(|(p, n)| (p.clone(), *n))
            .collect()
    }

    fn log(&mut self, meta: Meta, mut dirs: Vec<PathBuf>) {
        dirs.dedup();
        self.log.push(MetaOp {
            meta,
            unsynced: dirs,
        });
    }

    fn create(&mut self, path: &Path, node: Node) -> io::Result<()> {
        if self.live.contains_key(path) {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        if self.live.get(&parent(path)) != Some(&Node::Dir) {
            return Err(not_found());
        }
        self.live.insert(path.to_path_buf(), node);
        self.log(Meta::Create(path.to_path_buf(), node), vec![parent(path)]);
        Ok(())
    }

    fn sync_dir(&mut self, dir: &Path) {
        for op in &mut self.log {
            op.unsynced.retain(|d| d != dir);
        }
        // Durable operations at the front move into the base namespace.
        let done = self
            .log
            .iter()
            .take_while(|o| o.unsynced.is_empty())
            .count();
        for op in self.log.drain(..done) {
            apply(&mut self.base, op.meta);
        }
    }

    fn sync_file(&mut self, i: usize) {
        let inode = &mut self.inodes[i];
        for p in std::mem::take(&mut inode.pending) {
            match p {
                Pending::Write(at, bytes) => write_at(&mut inode.durable, at, &bytes),
                Pending::SetLen(len) => inode.durable.resize(len as usize, 0),
            }
        }
    }
}

/// The calls the store makes, with the signatures of `crate::fs`.
pub mod fs {
    use std::ffi::OsString;
    use std::io;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use super::{
        Disk, Fault, Inode, Meta, Model, Node, Pending, not_found, parent, with, write_at,
    };

    #[derive(Clone, Debug)]
    pub struct File {
        node: Node,
        path: PathBuf,
    }

    pub struct Metadata {
        len: u64,
    }

    impl Metadata {
        #[expect(
            clippy::len_without_is_empty,
            reason = "the one call compio's Metadata gets"
        )]
        pub fn len(&self) -> u64 {
            self.len
        }
    }

    #[derive(Default)]
    pub struct OpenOptions {
        create_new: bool,
    }

    impl OpenOptions {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn read(&mut self, _: bool) -> &mut Self {
            self
        }

        pub fn write(&mut self, _: bool) -> &mut Self {
            self
        }

        pub fn create_new(&mut self, v: bool) -> &mut Self {
            self.create_new = v;
            self
        }

        pub async fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
            let path = path.as_ref();
            let create_new = self.create_new;
            with(|d| {
                d.begin("open", path, u64::from(create_new), 0)?;
                let node = match d.live.get(path) {
                    Some(_) if create_new => {
                        return Err(io::Error::from(io::ErrorKind::AlreadyExists));
                    }
                    Some(n) => *n,
                    None if create_new => {
                        d.inodes.push(Inode::default());
                        let n = Node::File(d.inodes.len() - 1);
                        d.create(path, n)?;
                        n
                    }
                    None => return Err(not_found()),
                };
                Ok(File {
                    node,
                    path: path.to_path_buf(),
                })
            })
        }
    }

    impl File {
        pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
            OpenOptions::new().read(true).open(path).await
        }

        fn inode(&self) -> io::Result<usize> {
            match self.node {
                Node::File(i) => Ok(i),
                Node::Dir => Err(io::Error::other("is a directory")),
            }
        }

        pub async fn sync_data(&self) -> io::Result<()> {
            self.sync_all().await
        }

        pub async fn sync_all(&self) -> io::Result<()> {
            with(|d| {
                let res = d.begin("sync", &self.path, 0, 0);
                match (self.node, res) {
                    (Node::Dir, Ok(())) => d.sync_dir(&self.path),
                    (Node::File(i), Ok(())) => d.sync_file(i),
                    (Node::File(i), Err(e)) => {
                        if d.hit == Some(Fault::Eio) {
                            let lost = std::mem::take(&mut d.inodes[i].pending);
                            d.stats.sync_writes_lost += lost.len() as u64;
                        }
                        return Err(e);
                    }
                    (Node::Dir, Err(e)) => return Err(e),
                }
                Ok(())
            })
        }

        pub async fn set_len(&self, len: u64) -> io::Result<()> {
            let i = self.inode()?;
            with(|d| {
                d.begin("set_len", &self.path, len, 0)?;
                let inode = &mut d.inodes[i];
                inode.data.resize(len as usize, 0);
                inode.pending.push(Pending::SetLen(len));
                Ok(())
            })
        }
    }

    pub async fn read_exact_at(file: &File, len: usize, pos: u64) -> (io::Result<()>, Vec<u8>) {
        let res = file.inode().and_then(|i| {
            with(|d| {
                d.begin("read_at", &file.path, pos, len as u64)?;
                let data = &d.inodes[i].data;
                let at = pos as usize;
                match data.get(at..at + len) {
                    Some(b) => Ok(d.corrupt(b.to_vec())),
                    None => Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                }
            })
        });
        match res {
            Ok(b) => (Ok(()), b),
            Err(e) => (Err(e), Vec::new()),
        }
    }

    pub async fn write_all(file: &File, buf: Vec<u8>, pos: u64) -> (io::Result<()>, Vec<u8>) {
        let res = file.inode().and_then(|i| {
            with(|d| {
                d.begin("write_at", &file.path, pos, buf.len() as u64)?;
                d.trace.update(blake3::hash(&buf).as_bytes());
                let inode = &mut d.inodes[i];
                write_at(&mut inode.data, pos, &buf);
                inode.pending.push(Pending::Write(pos, buf.clone()));
                Ok(())
            })
        });
        (res, buf)
    }

    pub async fn read(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
        let path = path.as_ref();
        with(|d| {
            d.begin("read", path, 0, 0)?;
            match d.live.get(path) {
                Some(Node::File(i)) => {
                    let b = d.inodes[*i].data.clone();
                    Ok(d.corrupt(b))
                }
                Some(Node::Dir) => Err(io::Error::other("is a directory")),
                None => Err(not_found()),
            }
        })
    }

    pub async fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
        let path = path.as_ref();
        with(|d| {
            d.begin("metadata", path, 0, 0)?;
            match d.live.get(path) {
                Some(Node::File(i)) => Ok(Metadata {
                    len: d.inodes[*i].data.len() as u64,
                }),
                Some(Node::Dir) => Ok(Metadata { len: 0 }),
                None => Err(not_found()),
            }
        })
    }

    pub async fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> io::Result<()> {
        let (from, to) = (from.as_ref(), to.as_ref());
        with(|d| {
            d.begin("rename", from, 0, 0)?;
            d.trace.update(to.as_os_str().as_encoded_bytes());
            if d.live.get(&parent(to)) != Some(&Node::Dir) {
                return Err(not_found());
            }
            let n = d.live.remove(from).ok_or_else(not_found)?;
            d.live.insert(to.to_path_buf(), n);
            if d.model == Model::Posix && parent(from) != parent(to) {
                d.log(Meta::Remove(from.to_path_buf()), vec![parent(from)]);
                d.log(Meta::Create(to.to_path_buf(), n), vec![parent(to)]);
            } else {
                d.log(
                    Meta::Rename(from.to_path_buf(), to.to_path_buf()),
                    vec![parent(from), parent(to)],
                );
            }
            Ok(())
        })
    }

    pub async fn remove_file(path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        with(|d| {
            d.begin("remove", path, 0, 0)?;
            match d.live.get(path) {
                Some(Node::File(_)) => {}
                Some(Node::Dir) => return Err(io::Error::other("is a directory")),
                None => return Err(not_found()),
            }
            d.live.remove(path);
            d.log(Meta::Remove(path.to_path_buf()), vec![parent(path)]);
            Ok(())
        })
    }

    pub async fn create_dir_all(path: impl AsRef<Path>) -> io::Result<()> {
        let path = path.as_ref();
        with(|d| {
            d.begin("mkdir_all", path, 0, 0)?;
            let mut todo: Vec<&Path> = path.ancestors().collect();
            todo.reverse();
            for p in todo {
                match d.live.get(p) {
                    Some(Node::Dir) => {}
                    Some(Node::File(_)) => return Err(io::Error::other("not a directory")),
                    None => d.create(p, Node::Dir)?,
                }
            }
            Ok(())
        })
    }

    pub fn create_dir(path: &Path) -> io::Result<()> {
        with(|d| {
            d.begin("mkdir", path, 0, 0)?;
            d.create(path, Node::Dir)
        })
    }

    pub fn list(dir: &Path) -> io::Result<Vec<OsString>> {
        with(|d| {
            d.begin("list", dir, 0, 0)?;
            if d.live.get(dir) != Some(&Node::Dir) {
                return Err(not_found());
            }
            Ok(d.children(dir)
                .into_iter()
                .filter_map(|(p, _)| p.file_name().map(OsString::from))
                .collect())
        })
    }

    pub fn is_dir(path: &Path) -> bool {
        with(|d| d.live.get(path) == Some(&Node::Dir))
    }

    pub fn exists(path: &Path) -> bool {
        with(|d| d.live.contains_key(path))
    }

    pub fn name_taken(path: &Path) -> bool {
        exists(path)
    }

    /// A seeded clock: each read advances it by 1 to 1000 µs.
    pub fn now() -> Instant {
        with(|d: &mut Disk| {
            d.clock_ns += 1_000 * (1 + d.rng.below(1000));
            d.epoch + Duration::from_nanos(d.clock_ns)
        })
    }
}
