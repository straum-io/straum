# straum

A replicated MQTT 3.1.1 / MQTT 5 broker in Rust: sans-IO cores, a WAL with
group commit and compaction, and log replication. The architecture is
`docs/architecture/unified-stream.md`; what runs today is
`docs/architecture/current-implementation.md`; decisions are in `docs/adr/`
and `docs/decisions/`. The development rules (`rules.md`,
`engineering-rules.md`) are kept in the `straum-io/straum-dev-plan`
repository.
