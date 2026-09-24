//! Topic filter matching, the component level of docs/benchmarks.md §1.

use bytes::Bytes;
use domain::{Topic, TopicFilter};

#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

fn main() {
    divan::main();
}

const TOPIC: &str = "sensors/eu-west/rack12/node7/temperature";

fn topic() -> Topic {
    match Topic::new(Bytes::from_static(TOPIC.as_bytes())) {
        Ok(topic) => topic,
        Err(error) => panic!("invalid benchmark topic: {error}"),
    }
}

fn filter(filter: &'static str) -> TopicFilter {
    match TopicFilter::new(Bytes::from_static(filter.as_bytes())) {
        Ok(filter) => filter,
        Err(error) => panic!("invalid benchmark filter: {error}"),
    }
}

fn bench(bencher: divan::Bencher, pattern: &'static str) {
    let topic = topic();
    let filter = filter(pattern);
    bencher.bench(|| divan::black_box(&filter).matches(divan::black_box(&topic)));
}

#[divan::bench]
fn exact_match(bencher: divan::Bencher) {
    bench(bencher, TOPIC);
}

#[divan::bench]
fn single_level_wildcard(bencher: divan::Bencher) {
    bench(bencher, "sensors/+/rack12/+/temperature");
}

#[divan::bench]
fn multi_level_wildcard(bencher: divan::Bencher) {
    bench(bencher, "sensors/eu-west/#");
}
