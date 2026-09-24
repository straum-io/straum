//! Arbitrary bytes through the decoder: no panic, no unbounded allocation,
//! and every successful decode consumes input.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt::{MqttLimits, decode};

fuzz_target!(|data: &[u8]| {
    let limits = MqttLimits::default();
    let mut buf = BytesMut::from(data);

    loop {
        let before = buf.len();
        match decode(&mut buf, &limits) {
            Ok(Some(_)) => assert!(buf.len() < before, "decoder reported progress without it"),
            Ok(None) | Err(_) => break,
        }
    }
});
