//! The MQTT 3.1.1 and 5 codec (ADR-0008 step 3a) on arbitrary bytes:
//! chunked input decodes like whole input, and whatever decodes encodes back
//! to bytes that decode to the same packet.
#![no_main]

use bytes::BytesMut;
use codec::{DecodeError, Limits, Packet, Sender, Version, decode, encode};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, arbitrary::Arbitrary)]
struct Input {
    data: Vec<u8>,
    chunks: Vec<u8>,
    /// `None`: before CONNECT, so the first packet says the version.
    version: Option<bool>,
    from_server: bool,
}

/// Decodes what it can, following the version a CONNECT sets.
fn drain(
    buf: &mut BytesMut,
    version: &mut Option<Version>,
    from: Sender,
    out: &mut Vec<Packet>,
) -> Option<DecodeError> {
    loop {
        match decode(buf, *version, from, &Limits::default()) {
            Ok(Some(packet)) => {
                if let Packet::Connect(connect) = &packet {
                    *version = Some(connect.version);
                }
                out.push(packet);
            }
            Ok(None) => return None,
            Err(error) => return Some(error),
        }
    }
}

fuzz_target!(|input: Input| {
    let start = input
        .version
        .map(|v5| if v5 { Version::V5 } else { Version::V311 });
    let from = if input.from_server { Sender::Server } else { Sender::Client };

    let mut version = start;
    let mut whole = Vec::new();
    let whole_error = drain(
        &mut BytesMut::from(input.data.as_slice()),
        &mut version,
        from,
        &mut whole,
    );

    let mut version = start;
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
        streamed_error = drain(&mut buf, &mut version, from, &mut streamed);
        if streamed_error.is_some() {
            break;
        }
    }
    assert_eq!(whole, streamed);
    assert_eq!(whole_error, streamed_error);

    // Re-encode: the decoder accepted it, so the encoder must too, and the
    // bytes must mean the same packet.
    let mut version = start;
    for packet in whole {
        let (known, v) = match &packet {
            Packet::Connect(connect) => (None, connect.version),
            _ => (version, version.expect("only a CONNECT decodes before a version")),
        };
        let mut bytes = BytesMut::new();
        encode(&packet, v, from, &mut bytes).expect("a decoded packet encodes");
        let again = decode(&mut bytes, known, from, &Limits::default());
        assert_eq!(again, Ok(Some(packet)));
        version = Some(v);
    }
});
