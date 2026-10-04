# ADR-0004: Sans-IO cores, the runtime as a driver

- Status: accepted
- Date: 2026-10-04
- Decided by: Stanislav
- Source: docs/architecture/unified-stream.md §4, §5

## Context

The codec (`mqtt`), the broker (`broker`) and the replication state machines
(`replication`) are synchronous and free of I/O, and the boundary script
forbids tokio in them. The runtime (`runtime`) is tokio and holds what is
left: sockets, tasks, channels, timers, the WAL writer thread. Work under
D-28 has started moving the remaining decisions out of the runtime: storage
behind a `Disk` boundary with a simulated disk (stage 1), the leader's link
decisions into `replication::Link` and `Redial` (stage 2; both staged, not yet
committed when this ADR was written — D-39). The defects found
recently (D-21, D-22, D-27, D-33) all lived in the runtime's glue.

## Decision

- The sans-IO cores stay. Protocol logic is never interleaved with
  `.await`; that is the condition the deterministic simulation rests on.
- The runtime is a driver only, in the model of quinn-proto and quinn: it
  moves bytes, waits, and feeds events to state machines that decide. tokio
  is allowed as the driver and is not required.
- tokio-uring is not used. io_uring, if the Linux profile asks for it, comes
  through the `io-uring` crate from an event loop of Straum's own,
  thread-per-core, under the unsafe policy (ADR-0002).
- The crate boundaries keep enforcing this: no tokio in the cores.

## Consequences

- Each phase moves any decision still made in the runtime into a state
  machine before building on it; what remains in the runtime is input and
  output.
- The simulation harness drives the same state machines the runtime drives,
  with controlled clock, network and disk.
- A future driver other than tokio is a runtime change, not a core change.
