//! The apply function per committed record (ADR-0008 step 4 baseline):
//! one publish routed to N subscribed sessions, at most once and at least
//! once, and one acknowledgement.

use apply::{Effect, Limits, Options, Origin, Proposal, State};
use bytes::Bytes;
use domain::{DeliveryGuarantee, Topic, TopicFilter};
use journal::{Chain, Entry, decode};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

fn record(chain: &mut Chain, proposal: &Proposal) -> Vec<Vec<u8>> {
    let mut body = Vec::new();
    if proposal.encode(&mut body).is_err() {
        panic!("benchmark proposal does not encode");
    }
    let mut records = Vec::new();
    let entry = Entry {
        kind: proposal.kind(),
        properties: proposal.properties(),
        body: &body,
    };
    if chain.batch(1, &[entry], &mut records).is_err() {
        panic!("benchmark batch");
    }
    records
}

/// A state with `sessions` sessions subscribed to the topic, and the
/// records of one publish to it.
fn setup(sessions: u64, guarantee: DeliveryGuarantee) -> (State, Vec<Vec<u8>>) {
    let topic = match Topic::new(Bytes::from_static(b"sensors/eu-west/rack12/temperature")) {
        Ok(t) => t,
        Err(_) => panic!("topic"),
    };
    let filter = match TopicFilter::new(Bytes::from_static(b"sensors/+/rack12/#")) {
        Ok(f) => f,
        Err(_) => panic!("filter"),
    };
    let mut chain = Chain::genesis();
    let mut state = State::new(Limits::default());
    let mut out = Vec::new();
    for session in 0..sessions {
        let subscribe = Proposal::Subscribe {
            origin: Origin { node: 1, session },
            request: session,
            filter: filter.clone(),
            guarantee,
            options: Options::default(),
        };
        for r in record(&mut chain, &subscribe) {
            if let Ok(rec) = decode(&r) {
                let _ = state.apply(&rec, &mut out);
            }
        }
    }
    let publish = Proposal::Publish {
        origin: Origin {
            node: 2,
            session: 0,
        },
        request: 0,
        topic,
        payload: Bytes::from(vec![b'x'; 64]),
        properties: Bytes::new(),
        guarantee,
        retain: false,
    };
    (state, record(&mut chain, &publish))
}

fn bench(bencher: divan::Bencher, sessions: u64, guarantee: DeliveryGuarantee) {
    bencher
        .with_inputs(|| setup(sessions, guarantee))
        .bench_local_values(|(mut state, records)| {
            let mut out: Vec<Effect> = Vec::with_capacity(sessions as usize + 2);
            for r in &records {
                if let Ok(rec) = decode(r) {
                    let _ = state.apply(&rec, &mut out);
                }
            }
            out
        });
}

#[divan::bench(args = [1, 10, 100])]
fn apply_publish_at_most_once(bencher: divan::Bencher, sessions: u64) {
    bench(bencher, sessions, DeliveryGuarantee::AtMostOnce);
}

#[divan::bench(args = [1, 10, 100])]
fn apply_publish_at_least_once(bencher: divan::Bencher, sessions: u64) {
    bench(bencher, sessions, DeliveryGuarantee::AtLeastOnce);
}
