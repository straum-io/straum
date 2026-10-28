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
on open files to the hard limit (the store keeps every segment open).

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
| `--scenarios` | `store,broker,compaction,dictionary` | |
| `--modes` | `sequential,parallel,inline` | |
| `--lazyfs DIR` | `$LAZYFS_DIR` or `~/lazyfs` | the LazyFS checkout |
| `--keep` | | keep the store data of passing runs |

Exit status: 0 only if every iteration passed, 1 if any failed, 2 if a
precondition is missing. A scenario stops at its first failed iteration
and keeps its data; the other scenarios still run.

Output in `--out`:

```text
env.txt            kernel, OS, CPU, file system under LazyFS, LazyFS and
                   libfuse versions, Straum commit, rustc, seed
iterations.log     one line per iteration: PASS/FAIL, fault, outcome, result
summary.txt        the table of all scenarios and the overall result
<scenario>-<mode>/
  plan.txt         the iterations drawn from the seed
  iter-NNNNNN/     writer.log (TRY/ACK lines), writer.err, verify.log,
                   lazyfs.log; broker: broker.log, producer.log,
                   verify-broker.log
  root/            the store's files after the last recovery
  pre-recovery/    a copy of root/ taken after the last fault, before
                   recovery (both removed after a passing run)
```

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
     file;
   - `torn`: `lazyfs::torn-op` splits the next write to `wal.log` or to a
     topic index into 2–4 parts, persists a random subset and kills LazyFS.

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
     at the reported offset, a segment may only be deleted, an index only
     deleted or rewritten. Anything else is a silent repair;
   - a second open finds nothing to change.

The store directory carries over from one iteration to the next, so later
iterations also recover stores that were recovered before.

## Scenarios

| Scenario | Writer | Fault |
|---|---|---|
| `store` | batches of 1–16 messages to three topics, one released and compacted every 3rd round; segments of 64 KiB; `inline` stores bodies up to 1024 bytes in the WAL (the broker's `inline`: 4096), so both kinds of object occur | `clear`, `crash` or `torn` at a random moment |
| `broker` | `straum-broker` with `store_write_mode` set to the mode and 64 KiB segments; two producers with keys, headers and repeated bodies, 10 ms apart between requests | as `store` |
| `compaction` | as `store`, compacting after every round | the writer sends a `crash` or `torn` (WAL) fault just before its 1st–4th compaction; the summary counts the iterations whose writer died inside the compaction |
| `dictionary` | — | not applicable: main has no dictionary training |

### Dictionary

Block compression (branch `store-block-compression`) trains a zstd
dictionary once per store, inside the `put_batch` that crosses
`Config::dict_train_bytes`. When that lands, the scenario is: a fresh store
per iteration with a small `dict_train_bytes` (64 KiB), a writer like
`store-write` that prints `TRAIN` before the batch that will train, and the
fault armed from that line; the checks are those of `store`, plus that the
dictionary is in the WAL after recovery whenever a batch after the training
batch was acknowledged. Until the writer has that option, `run.sh` reports
the scenario as failed once `dict_train_bytes` exists in the store, so a
run after the merge cannot pass without it.

## Reproducing a failure

`iterations.log` names the seed and the iteration. The seed reproduces
the plan (fault, delay, writer seed), not the exact moment the fault hits:
that depends on timing. A failed scenario keeps its directory:
`pre-recovery/` is the store as the power loss left it, before the
verifier's recovery changed it, and `root/` is the state after that
recovery. To repeat the check on a copy, without LazyFS:

```bash
cp -a <scenario>-<mode>/pre-recovery /tmp/repro
straum-power-loss store-verify --dir /tmp/repro/store --mode <mode> --logs <scenario>-<mode>
```

## What LazyFS does not model

- File creation, renames and unlinks reach the underlying file system at
  once (LazyFS caches file data only); a missing directory `fsync` goes
  undetected.
- A drive that acknowledges a flush and loses or reorders the data.
- Unsynced writes reaching the disk partly and in any order: `clear` drops
  all of them, `torn` persists parts of one write.
- A fault during recovery itself.
- Concurrency inside LazyFS: it runs single-threaded (`-s`), so the two
  syncs of ParallelSync reach it one after the other.
