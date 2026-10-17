# straum

A replicated MQTT 3.1.1 / MQTT 5 broker in Rust: sans-IO cores, a WAL with
group commit and compaction, and log replication. The client contract the
core pins in its tests is `docs/configuration.md`. The architecture, ADRs,
decision log and the other engineering documents are kept in the
`straum-io/straum-dev-plan` repository under `plugins/straum-engineering/repos/straum/`,
with the same `docs/` paths that comments in this repository cite.
