//! Deterministic simulation of the cluster path (ADR-0007, ADR-0008). Every
//! simulation lives in `tests/`: each drives the production cores with
//! simulated time, network and disk, from one seed. The leader/follower
//! simulation of the old path went with it at step 8 (D-111).
