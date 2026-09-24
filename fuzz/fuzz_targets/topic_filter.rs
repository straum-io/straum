//! Topic and filter construction from arbitrary bytes, and matching between
//! whatever survives validation.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use domain::{Topic, TopicFilter};

fuzz_target!(|input: (Vec<u8>, Vec<u8>)| {
    let topic = Topic::new(Bytes::from(input.0));
    let filter = TopicFilter::new(Bytes::from(input.1));

    if let (Ok(topic), Ok(filter)) = (topic, filter) {
        let _ = filter.matches(&topic);
    }
});
