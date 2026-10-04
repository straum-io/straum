# straum

A replicated MQTT 3.1.1 / MQTT 5 broker in Rust: sans-IO cores, a WAL with
group commit and compaction, and log replication. The architecture is
`docs/architecture/unified-stream.md`; what runs today is
`docs/architecture/current-implementation.md`; the working rules are
`CLAUDE.md`; decisions are in `docs/adr/`.
