# Fuzz targets

A cargo-fuzz crate with a workspace of its own: the main workspace and
`scripts/validate.sh` never build it. Needs a nightly toolchain and
`cargo install cargo-fuzz`.

| Target | Input | Checks |
|---|---|---|
| `proto_frame` | bytes | `split_frame`, `FrameHeader`, every `Request` and `Response` decoder; a decoded value encodes back to the same bytes |
| `proto_json` | text | the JSON reader of fixtures and scenarios: `parse`, `request_from_json`, `response_from_json`, `unhex`; an accepted message survives the wire and JSON round trips |
| `store_wal` | archive of store files, mostly WAL files | `Store::open` on the base store's data segments plus the input |
| `store_checkpoint` | archive of checkpoint files and quarantine directories | `Store::open` on the base store without its checkpoint files plus the input |

The base store is written through the store's API at start-up
(`build_store` in `src/lib.rs`), deterministically. An archive is `flags u8`,
then `name_len u8 | name | len u32 | bytes` per file; flag bit 0 recomputes
the CRCs of WAL files and of every checkpoint file, so
mutations get past the checksums.

Store invariants, checked after every open: no panic; a failed open changes
no file; a successful open changes only what its `RecoveryReport` lists, in
the way the action says; a second open then succeeds or fails without
changing anything, and if it succeeds it reports nothing.

## Running

```sh
# macOS Command Line Tools: libFuzzer's C++ headers are only in the SDK.
export CXXFLAGS="-isystem $(xcrun --show-sdk-path)/usr/include/c++/v1"
cd fuzz
cargo run --release --bin gen_corpus          # seeds into corpus/<target>/
cargo +nightly fuzz run -O store_wal -- \
  -max_total_time=900 -rss_limit_mb=2048 -malloc_limit_mb=1024 \
  -max_len=131072 -timeout=20
```

`STRAUM_FUZZ_KNOWN=F1` passes over a finding already reported, so a run can
look for others; `STRAUM_FUZZ_DIR` sets where the store under test is
written (default: the system temp dir); `STRAUM_FUZZ_VERBOSE=1` prints each
open's result.

## Reproducers

`regressions/<target>/*.hex` are fixed findings, `regressions/open/` open
ones, as hex text. `cargo test --release` (stable) replays the fixed ones,
checks every seed, that the base store is deterministic and that the CRC
recompute matches every checksum the store writes; `-- --include-ignored`
also replays the open ones and runs `tests/findings.rs`, the open store
findings built through the API. A saved store input names its format
version and would only give `UnsupportedFormat` after a format change, so
the replay fails on one from another version; the API-built form does not
go stale.
Protocol findings are regression tests in `crates/straum-proto/tests`.

To save a crash: `xxd -p -c 64 artifacts/<target>/crash-<hash> >
regressions/open/<target>/<name>.hex`.

## Findings

Open, reported to the store's owners:

- **F1** (`store_wal`): an acknowledged ParallelSync frame is dropped on
  the second open. The first open drops the last frame, whose block is
  damaged (`FrameDropped`); the frame before it was followed by that frame,
  so it was acknowledged, but its blocks are checked for presence and
  bounds only. It is now last, and the reopen checks it in full and drops
  it as never acknowledged. `tests/findings.rs` builds it through the API
  (the fuzzer's input was in format 18 and no longer opens).
- **F2** (seen in `store_checkpoint`): the store's file name parsers use
  `u64::from_str_radix`, which takes a `+` sign and upper-case hex, so
  `checkpoint-+000000000000002` is read as manifest 2 and the path rebuilt
  from the number (`checkpoint-0000000000000002`) names another file: open
  fails with an I/O `NotFound`, no file changed. On a case-insensitive file
  system, an upper-case manifest name is moved under that name while the
  report gives the lower-case one. Archives therefore hold lower-case names
  only.

Fixed: `straum_proto::json::unhex` took a leading `+`
(`regressions/proto_json/`, and `unhex_takes_hex_digits_only` in
`crates/straum-proto/tests/codec.rs`).
