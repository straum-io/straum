# Power-loss harness

Checks that every acknowledged message survives a power loss, for the
store alone and for the broker, in each write mode. The store and the
broker run on a [LazyFS](https://github.com/dsrhaslab/lazyfs) mount: LazyFS
keeps written data in its own cache until the program calls `fsync` or
`fdatasync`, and a fault drops that cache, which is what a power loss does
to the page cache.

`run.sh` drives the run; `tools/power-loss` is the binary that writes and
verifies. Neither changes the store or the broker.

## Requirements

Linux with FUSE 3 and LazyFS 0.3.1 (commit
`045a0b3a1126725e693934e29d3ba15e08cc39ec`) built from source. The script
checks this, prints the install commands below when something is missing,
and exits with status 2. It never installs anything or uses sudo.

```bash
sudo apt install fuse3 libfuse3-dev g++ cmake git pkg-config
git clone https://github.com/dsrhaslab/lazyfs.git ~/lazyfs
git -C ~/lazyfs checkout 045a0b3a1126725e693934e29d3ba15e08cc39ec   # tag 0.3.1
(cd ~/lazyfs/libs/libpcache && ./build.sh)
(cd ~/lazyfs/lazyfs && ./build.sh)
```

LazyFS is mounted by the user running the script, without `allow_other`,
so `/etc/fuse.conf` needs no change. The script raises its own soft limit
on open files to the hard limit.

## Running

```bash
scripts/power-loss/run.sh --out ~/power-loss/$(date -u +%Y%m%dT%H%M%SZ)
scripts/power-loss/run.sh --out /tmp/pl-smoke --quick      # 5 iterations each
```

| Option | Default | |
|---|---|---|
| `--out DIR` | required | every file of the run; LazyFS's root and mount are inside |
| `--iterations N` | 200 | per scenario and mode |
| `--quick` | | 5 iterations |
| `--seed N` | 24301 | printed in `env.txt` and in every iteration line |
| `--scenarios` | `store,broker,compaction,dictionary,checkpoint` | |
| `--modes` | `sequential,parallel,inline` | |
| `--lazyfs DIR` | `$LAZYFS_DIR` or `~/lazyfs` | the LazyFS checkout |
| `--keep` | | keep the store data of passing runs |

Exit status: 0 only if every iteration passed, 1 if any failed, 2 if a
precondition is missing. A scenario stops at its first failed iteration
and keeps its data; the other scenarios still run.

Output in `--out`:

```text
env.txt            kernel, OS, CPU, file system under LazyFS, LazyFS and
                   libfuse versions, Straum commit and the number of
                   uncommitted changes, rustc, seed
iterations.log     one line per iteration: PASS/FAIL, fault, outcome, result
summary.txt        the table of all scenarios and the overall result
<scenario>-<mode>/
  plan.txt         the iterations drawn from the seed
  iter-NNNNNN/     writer.log (TRY/ACK lines), writer.err, verify.log,
                   lazyfs.log; broker: broker.log, producer.log,
                   verify-broker.log; checkpoint: damage.log, and for an
                   interrupted recovery store-open.log, lazyfs-rc.log
  root/            the store's files after the last recovery
  pre-recovery/    a copy of root/ taken after the last fault (checkpoint:
                   and the damage and interrupted recovery), before the
                   verifier's recovery (both removed after a passing run)
```

## In a container (macOS development)

On the Mac the harness runs on the `straum-test` Podman machine, in an
image built from `Containerfile` next to this file: the base image of
`scripts/container/linux.sh` plus FUSE 3 and LazyFS at the pinned commit.
The image is built without privileges; only the run gets `--device
/dev/fuse --cap-add SYS_ADMIN`, and it runs nothing but this script
(Straum `rules.md` §13). Source changes, a planted bug included, are made
on the host before the run.

```bash
scripts/container/linux.sh --session -- true    # starts straum-test (unprivileged)
podman --connection straum-test build -t localhost/straum-power-loss:0.3.1 \
    -f scripts/power-loss/Containerfile scripts/power-loss
podman --connection straum-test run --rm \
    --device /dev/fuse --cap-add SYS_ADMIN \
    -e STRAUM_COMMIT="$(git rev-parse HEAD)" \
    -e STRAUM_UNCOMMITTED="$(git status --porcelain | wc -l | tr -d ' ')" \
    -v "$PWD":/src -w /src \
    -v straum-container-registry:/usr/local/cargo/registry \
    -v "straum-container-target-$(printf '%s' "$PWD" | cksum | cut -d ' ' -f 1)":/src/target \
    localhost/straum-power-loss:0.3.1 \
    scripts/power-loss/run.sh --quick --out /src/target/power-loss
scripts/container/linux.sh --end-session         # back to the default machine
```

Inside the container git cannot read a worktree's metadata, so the host
passes the commit and its number of uncommitted changes (`git status
--porcelain` lines) as `STRAUM_COMMIT` and `STRAUM_UNCOMMITTED`; `env.txt`
marks them `from the host`. Without them run.sh asks git, and prints
`unknown` if git fails.

The privileged container's command is `run.sh` itself, not
`scripts/container/linux.sh`, whose wrapper first runs `rustup component
add`. The volumes are those of `linux.sh` (the registry, and this tree's
target volume), so the output lands in the tree's target volume; copy it
out with `podman --connection straum-test cp`. Container runs show whether
the harness works; they are not the owner's hardware.

## An iteration

1. LazyFS is mounted over `<scenario>-<mode>/root`.
2. A writer runs: `store-write` (the store), or the broker with
   `broker-produce` (two producers, durable acks). Each prints `TRY` before
   a batch or request and `ACK` after it was acknowledged, to a file outside
   the mount.
3. After the writer is ready and a random delay (0–299 ms), one fault:
   - `clear`: the writers are stopped (SIGSTOP), `lazyfs::clear-cache`
     drops every unsynced write, the writers are killed and LazyFS is
     unmounted;
   - `crash`: `lazyfs::crash` kills LazyFS before or after the next
     `write`, `fsync`, `create` or `unlink` of a WAL, segment or index
     segment file;
   - `torn`: `lazyfs::torn-op` splits the next write to the WAL file
     being appended to when the fault is sent (`wal-<seq>.log`) or to
     the index segment the writer will append to (`idx-<id>.dat`, the next
     id after those on disk when the writer starts) into 2–4 parts,
     persists a random subset and kills LazyFS.

   If a `crash` or `torn` fault's operation does not come within 10 s
   (60 s in the compaction scenario), LazyFS is killed instead, which is
   also a power loss; the line says `outcome=fallback`.
4. LazyFS is mounted again and the verifier opens the store (or starts the
   broker on it). The iteration passes if all of these hold:
   - every acknowledged message so far is read back with its exact bytes
     (the broker: key, headers, body and timestamp, through Fetch);
   - every message present without an acknowledgement belongs to a batch
     or request the writer had in flight; no seq acknowledged once holds
     other bytes later;
   - every live entry matches its BLAKE3 id and reference counts match the
     live entries (store); seqs are contiguous and timestamps do not
     decrease (broker);
   - the recovery report holds only the entries `docs/guarantees.md` §3
     lists, `FrameDropped` only in `parallel`, at most one WAL cut;
   - every change recovery made to the store directory is in the report
     and every report entry is a change on disk: a WAL change must be a cut
     of the reported WAL file at the reported offset, a deleted
     `wal-*.log.tmp` needs `WalTmpRemoved`, a segment may only be deleted, an existing
     index segment only deleted (`IndexSegmentRemoved` with its size), and
     a new index segment must hold a fence (`StaleIndexRecordsFenced`) or
     repaired records (`TopicIndexRepaired`). Anything else is a silent
     repair;
   - a second open finds nothing to change.

The store directory carries over from one iteration to the next, so later
iterations also recover stores that were recovered before.

## Scenarios

| Scenario | Writer | Fault |
|---|---|---|
| `store` | batches of 1–16 messages to three topics, one released and compacted every 3rd round; segments and WAL files of 64 KiB; `inline` stores bodies up to 1024 bytes in the WAL (the broker's `inline`: 4096), so both kinds of object occur | `clear`, `crash` or `torn` at a random moment |
| `broker` | `straum-server` (crate `straum-broker`) with `store_write_mode` set to the mode and 64 KiB segments; two producers with keys, headers and repeated bodies, 10 ms apart between requests | as `store` |
| `compaction` | as `store`, compacting after every round | the writer sends a `crash` or `torn` (WAL) fault just before its 1st–4th compaction; the summary counts the iterations whose writer died inside the compaction |
| `dictionary` | as `store`, on a fresh store per iteration with `dict_train_bytes` 64 KiB | the writer sends a `crash` or `torn` (WAL) fault just before the batch that trains the dictionary (`Store::trains_next_batch`) |
| `checkpoint` | as `store`, with `Store::checkpoint` after every 4th round | `clear` at a random moment; or the writer sends a `crash` or `torn` (manifest) fault just before its 1st–4th checkpoint; then, in some iterations, the newest manifest is damaged and the first recovery may be interrupted (below) |

The summary's `in-op` column counts the iterations whose writer died
inside the compaction (`compaction`), inside the training batch
(`dictionary`) or inside `Store::checkpoint` (`checkpoint`).

### Dictionary

The store trains a zstd dictionary once, inside the `put_batch` that
follows `Config::dict_train_bytes` raw bytes stored without one; the
dictionary goes into that batch's WAL frame. Each iteration starts a
fresh store, so every iteration has a training batch, and the writer
prints `TRAIN <round>` and arms the fault just before it. The verifier
uses only that iteration's writer log. Its checks are those of `store`,
plus: when the training round or a later one was acknowledged, the
dictionary is in the store after recovery. The iteration line says
`where=in-training` when the writer died before the training batch was
acknowledged, `after-training` or `before-training` otherwise.

### Checkpoint

A checkpoint writes an object table (`objtab-*.tab`), a relocation run
(`reloc-*.run`) when compaction moved covered objects, the manifest
through `checkpoint-<n>.tmp` and a rename, and deletes the previous
manifest (`crates/straum-store/docs/format.md` "Checkpoint"). The `crash`
points are `create`, `write` and `fsync` of the table, the run and the tmp
manifest, its `rename`, and the `unlink` of the previous manifest, each
`before` or `after`. `torn` splits the write of the tmp manifest (the
writer names `checkpoint-<n>.tmp`, `n` the next checkpoint number on
disk).

A manifest is synced before its rename, so no LazyFS fault can damage
one. To run the fallback and the quarantine (D-257 A), the harness damages
the newest manifest itself, on the unmounted root after the power loss: it
cuts it to a random length or flips one bit (`damage.log`). This happens
after every crash before the previous manifest's `unlink` (two manifests
on disk: open must fall back to the older one) and in a quarter of the
other iterations. Half of those then run a first recovery on LazyFS
(`store-open`) and kill it before or after its first rename into the
quarantine directory (`rc-before`, `rc-after`), leaving an interrupted or
a completed quarantine for the verifier's open. The iteration line says
`where=in-checkpoint` or `outside-checkpoint`, and
`damaged=<manifest>_<how> rc=<when>:<outcome>`.

The verifier's checks are those of `store`, plus, from the files before
its recovery:

- each manifest whose quarantine directory exists without it is
  completed (`CheckpointQuarantineResumed`, `found` the files in that
  directory); the damaged manifest, if newer than every intact one, is
  quarantined (`CheckpointManifestDamaged`, the directory
  `quarantine-<n>` or the next free `.k`); no other manifest is reported
  damaged: one would have been renamed before it was durable;
- `fallback` in those entries and `Store::checkpoint_number` are the
  newest other manifest;
- each `CheckpointFileQuarantined` is a move of that file, unchanged, into
  its directory, every quarantined manifest is in it, and
  `Store::quarantined_files` matches the files in quarantine directories;
- each deleted checkpoint file is a `CheckpointFileRemoved`;
- the open succeeds: a state these rules explain never makes it fail.

## Reproducing a failure

`iterations.log` names the seed and the iteration. The seed reproduces
the plan (fault, delay, writer seed), not the exact moment the fault hits:
that depends on timing. A failed scenario keeps its directory:
`pre-recovery/` is the store as the power loss left it, before the
verifier's recovery changed it, and `root/` is the state after that
recovery. To repeat the check on a copy, without LazyFS:

```bash
cp -a <scenario>-<mode>/pre-recovery /tmp/repro
straum-power-loss store-verify --dir /tmp/repro/store --mode <mode> --logs <scenario>-<mode> \
    [--damaged <the manifest in damage.log>]
```

## What LazyFS does not model

- File creation, renames and unlinks reach the underlying file system at
  once (LazyFS caches file data only); a missing directory `fsync` goes
  undetected.
- A drive that acknowledges a flush and loses or reorders the data.
- Unsynced writes reaching the disk partly and in any order: `clear` drops
  all of them, `torn` persists parts of one write.
- A fault during recovery itself, except the one rename into a
  quarantine that the `checkpoint` scenario interrupts.
- A synced file damaged later: the `checkpoint` scenario damages a
  manifest itself, outside LazyFS.
- Concurrency inside LazyFS: it runs single-threaded (`-s`), so the two
  syncs of ParallelSync reach it one after the other.
