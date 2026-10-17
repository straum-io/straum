# ADR-0002: Unsafe policy

- Status: accepted (item 3 by D-40)
- Date: 2026-10-04
- Decided by: Stanislav
- Source: docs/architecture/unified-stream.md §4, §4.1

## Context

Until now `unsafe_code = "forbid"` held workspace-wide, with a rule that it
could be lifted "for one module" after a Linux benchmark. Rust cannot do
that: `forbid` set in `[workspace.lints]` cannot be relaxed by an
`#[allow]` inside a crate that inherits it, so "one module" could never be
exempted without changing how the crate takes its lints. The unified stream
names candidates that do need unsafe — mmap of sealed segments, io_uring
with owned buffers, aligned buffers for O_DIRECT — and states the policy
under which they may arrive.

## Decision

1. `unsafe_code = "forbid"` is the default for every crate, through
   `[workspace.lints]`. The workspace also sets `unsafe_op_in_unsafe_fn =
   "deny"` and the clippy lints `undocumented_unsafe_blocks` and
   `multiple_unsafe_ops_per_block` to `deny`. While no crate is exempt they
   change nothing; they are there so that the first exempt crate is held to
   them without anybody having to remember.
2. Unsafe is allowed only where it is justified and provably sound — no safe
   caller of the public API can cause undefined behaviour — and all of the
   conditions of unified-stream §4.1 hold: a Linux profile shows a gain the
   safe version does not give, the safe version stays as a baseline behind a
   feature flag and is benchmarked against it; the unsafe lives in a small
   crate of its own (`straum-sys-*`) with a safe public API; every block has
   `// SAFETY:`, every `unsafe fn` a `# Safety` section; Miri where it can
   run, sanitizers and fuzzing where it cannot, and the failure scenarios in
   the simulation harness; an ADR per use, with Stanislav's final consent.
3. **How the first exempt crate is set up** (accepted, D-40). It does not write
   `[lints] workspace = true`, because that would inherit `forbid`. It
   declares its own table, which repeats every workspace lint except
   `unsafe_code`:

   ```toml
   [lints.rust]
   unsafe_code = "deny"             # allowed only where a block says #[allow] with SAFETY
   unsafe_op_in_unsafe_fn = "deny"

   [lints.clippy]
   unwrap_used = "deny"
   expect_used = "deny"
   dbg_macro = "deny"
   todo = "deny"
   undocumented_unsafe_blocks = "deny"
   multiple_unsafe_ops_per_block = "deny"
   ```

   `deny` rather than `allow` for `unsafe_code`, so that every unsafe
   region is a visible `#[allow(unsafe_code)]` on the smallest item, next to
   its `// SAFETY:`. When the first such crate is added,
   `scripts/check-boundaries.sh` gains a check that only crates named
   `straum-sys-*` may lack `workspace = true` in `[lints]`, and that each of
   them repeats the clippy and rust lints above. The crate's
   ADR names it and links the measurement.
4. engineering-rules.md §20 and §28 ("Unsafe") stay in force where they agree;
   for the full policy this ADR and unified-stream §4.1 decide.

## Consequences

- No crate is exempt today. The workspace lint change is configuration only;
  it compiles cleanly and changes no code.
- The first exemption is a change to the boundary script and to the crate's
  manifest as above, plus its own ADR.
- rules.md §14 no longer carries its own unsafe rule; it points here.

## Scope: third-party dependencies (D-50, 2026-10-05)

The policy in unified-stream §4.1 governs unsafe in Straum's own code. A
third-party crate that contains unsafe internally is judged as a dependency,
under rules.md §2: what it is for, what it costs, its transitive
dependencies and its maintenance. The first such crate admitted on these
terms is `xxhash-rust` (xxh3-128 for the log's `prev_hash`, D-50 A). It
contains unsafe internally (34 occurrences in 0.8.19: unaligned reads and
SIMD in `xxh3` and `utils`), checked on 2026-10-05 from the crate source.
