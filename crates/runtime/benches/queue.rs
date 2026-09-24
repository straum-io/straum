//! The session queue, on its own: the required queue benchmarks of
//! docs/benchmarks.md §3.
//!
//! These measure the channel and nothing else — one thread, no scheduler. The
//! difference between this floor and what a real hop costs is wakeups and task
//! switching, which belong to the runtime, not to the queue.

use bytes::Bytes;
use domain::{DeliveryId, Topic};
use protocol::{Delivery, Outbound};
use tokio::sync::mpsc::{self, error::TrySendError};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const CAPACITY: usize = 1024;
const BATCH: usize = 64;

fn delivery() -> Outbound {
    let topic = match Topic::new(Bytes::from_static(
        b"sensors/eu-west/rack12/node7/temperature",
    )) {
        Ok(topic) => topic,
        Err(error) => panic!("invalid benchmark topic: {error}"),
    };
    Outbound::Deliver(Delivery {
        id: Some(DeliveryId(1)),
        topic,
        payload: Bytes::from_static(&[b'x'; 64]),
    })
}

/// The broker loop's side: hand one batch to a session that is keeping up.
#[divan::bench]
fn enqueue(bencher: divan::Bencher) {
    let item = delivery();
    bencher
        .with_inputs(|| mpsc::channel::<Outbound>(CAPACITY))
        .bench_local_values(|(tx, rx)| {
            for _ in 0..BATCH {
                let _ = tx.try_send(divan::black_box(item.clone()));
            }
            (tx, rx)
        });
}

/// The writer's side: take a batch out of a queue that already has one.
#[divan::bench]
fn dequeue(bencher: divan::Bencher) {
    let item = delivery();
    bencher
        .with_inputs(|| {
            let (tx, rx) = mpsc::channel::<Outbound>(CAPACITY);
            for _ in 0..BATCH {
                let _ = tx.try_send(item.clone());
            }
            (tx, rx)
        })
        .bench_local_values(|(tx, mut rx)| {
            let mut taken = 0;
            while rx.try_recv().is_ok() {
                taken += 1;
            }
            divan::black_box(taken);
            (tx, rx)
        });
}

/// Steady state: a session that is served as fast as it is filled.
#[divan::bench]
fn enqueue_dequeue(bencher: divan::Bencher) {
    let item = delivery();
    bencher
        .with_inputs(|| mpsc::channel::<Outbound>(CAPACITY))
        .bench_local_values(|(tx, mut rx)| {
            for _ in 0..BATCH {
                let _ = tx.try_send(item.clone());
            }
            while rx.try_recv().is_ok() {}
            (tx, rx)
        });
}

/// A session that stopped reading: every send is refused, and every refusal
/// is a trip through the overflow policy (CLAUDE.md §8).
#[divan::bench]
fn full_queue(bencher: divan::Bencher) {
    let item = delivery();
    bencher
        .with_inputs(|| {
            let (tx, rx) = mpsc::channel::<Outbound>(CAPACITY);
            while tx.try_send(item.clone()).is_ok() {}
            (tx, rx)
        })
        .bench_local_values(|(tx, rx)| {
            for _ in 0..BATCH {
                match tx.try_send(divan::black_box(item.clone())) {
                    Err(TrySendError::Full(rejected)) => {
                        divan::black_box(rejected);
                    }
                    _ => panic!("the queue was supposed to be full"),
                }
            }
            (tx, rx)
        });
}
