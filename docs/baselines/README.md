# Baselines

> **History.** The baselines recorded here are of the old broker (removed at step 8, D-111) unless a section says otherwise; the new path has no Linux baseline yet.

One file per benchmark machine: `docs/baselines/<machine-id>.md`.

Each entry records a commit hash, the environment block from
`docs/benchmarks.md` §8, and the accepted numbers. Baselines from different
machines are never compared with each other.

Entries are added at release points and when a structural decision was made on
the strength of a measurement. A later regression is investigated against the
entry, not against memory of "how fast it used to be".

Template:

```text
## <date> — <commit>

Machine:   <cpu, cores used, smt, governor, kernel>
Toolchain: rustc <version>, profile release (lto=thin, codegen-units=1), RUSTFLAGS=<...>
Sysctls:   <relevant values from docs/linux-tuning.md>

| benchmark | workload | median | spread | alloc/op |
|---|---|---|---|---|
| straum-broker/publish_route | 64 B, 100 subs, 1 thread | ... | ... | ... |
```
