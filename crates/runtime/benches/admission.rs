//! The cost of admission (D-7) on its own: what a durable publish now pays
//! for its permit, without a socket, a log or a scheduler in the measurement.
//!
//! The end-to-end comparison (`e2e/benches/durability.rs`) cannot resolve a
//! cost this small on a development machine: its noise floor is ten percent
//! (CLAUDE.md §11). This is the deterministic half of D-6 B.
//!
//! Each iteration runs the cycle the broker actually performs for `PUBLISHES`
//! publishes: the reader takes a permit (`Arc` clone, acquire, `forget`), and
//! the broker loop returns permits when acknowledgements go — one at a time
//! for a record that was durable before its publish arrived, a whole release
//! at once when a commit or a watermark covers many. `baseline` is the same
//! loop with no semaphore, so the difference is admission and nothing else.

use std::sync::Arc;

use tokio::sync::Semaphore;

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const PUBLISHES: usize = 1024;

fn runtime() -> tokio::runtime::Runtime {
    match tokio::runtime::Builder::new_current_thread().build() {
        Ok(runtime) => runtime,
        Err(error) => panic!("no runtime: {error}"),
    }
}

/// The loop with nothing in it, for the subtraction.
#[divan::bench]
fn baseline(bencher: divan::Bencher) {
    let runtime = runtime();
    bencher
        .counter(divan::counter::ItemsCount::new(PUBLISHES))
        .bench_local(|| {
            runtime.block_on(async {
                let mut held = 0usize;
                for _ in 0..PUBLISHES {
                    held = divan::black_box(held + 1);
                }
                held
            })
        });
}

/// A permit per publish, released `released_together` at a time: 1 is the
/// already-durable path, 50 is a group commit of the size `fsynced` showed
/// on this machine (docs/performance.md, D-6 B).
#[divan::bench(args = [1, 50])]
fn permit_cycle(bencher: divan::Bencher, released_together: usize) {
    let runtime = runtime();
    let admission = Arc::new(Semaphore::new(65_536));
    bencher
        .counter(divan::counter::ItemsCount::new(PUBLISHES))
        .bench_local(|| {
            runtime.block_on(async {
                let mut held = 0usize;
                for _ in 0..PUBLISHES {
                    match Arc::clone(&admission).acquire_owned().await {
                        Ok(permit) => permit.forget(),
                        Err(error) => panic!("closed: {error}"),
                    }
                    held += 1;
                    if held == released_together {
                        admission.add_permits(held);
                        held = 0;
                    }
                }
                if held > 0 {
                    admission.add_permits(held);
                }
                divan::black_box(admission.available_permits())
            })
        });
}
