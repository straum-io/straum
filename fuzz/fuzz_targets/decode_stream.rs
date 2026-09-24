//! Feeding the same bytes in arbitrary chunks must decode to the same
//! packets as feeding them at once: the decoder carries no hidden state.
#![no_main]

use bytes::BytesMut;
use libfuzzer_sys::fuzz_target;
use mqtt::{MqttError, MqttLimits, Packet, decode};

#[derive(Debug, arbitrary::Arbitrary)]
struct Input {
    data: Vec<u8>,
    chunks: Vec<u8>,
}

/// Decodes what it can. `Some` is the error that stopped it.
fn drain(buf: &mut BytesMut, out: &mut Vec<Packet>) -> Option<MqttError> {
    let limits = MqttLimits::default();
    loop {
        match decode(buf, &limits) {
            Ok(Some(packet)) => out.push(packet),
            Ok(None) => return None,
            Err(error) => return Some(error),
        }
    }
}

fuzz_target!(|input: Input| {
    let mut whole = Vec::new();
    let whole_error = drain(&mut BytesMut::from(input.data.as_slice()), &mut whole);

    let mut streamed = Vec::new();
    let mut streamed_error = None;
    let mut buf = BytesMut::new();
    let mut rest = input.data.as_slice();
    let mut chunks = input.chunks.iter().copied().cycle();

    while !rest.is_empty() {
        let size = usize::from(chunks.next().unwrap_or(1)).clamp(1, rest.len());
        let (head, tail) = rest.split_at(size);
        buf.extend_from_slice(head);
        rest = tail;

        streamed_error = drain(&mut buf, &mut streamed);
        if streamed_error.is_some() {
            break;
        }
    }

    assert_eq!(whole, streamed);
    assert_eq!(whole_error, streamed_error);
});
