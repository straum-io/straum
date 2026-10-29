//! Round trips of every sample, boundary checks, and decoding of
//! arbitrary bytes (an error or a value, never a panic).

mod common;

use common::{Frame, encode, samples};
use proptest::prelude::*;
use straum_proto::*;

#[test]
fn every_sample_round_trips() {
    let all = samples();
    let mut requests = std::collections::HashSet::new();
    let mut responses = std::collections::HashSet::new();
    for s in &all {
        let bytes = encode(s);
        let (h, body) = split_frame(&bytes).unwrap();
        assert_eq!(h.correlation_id, s.correlation_id, "{}", s.name);
        assert_eq!(h.frame_size(), bytes.len() as u64, "{}", s.name);
        match &s.frame {
            Frame::Request(r) => {
                assert_eq!(h.msg_type, r.msg_type());
                assert_eq!(&Request::decode(h.msg_type, body).unwrap(), r, "{}", s.name);
                requests.insert(r.name());
            }
            Frame::Response(r) => {
                assert_eq!(h.msg_type, r.msg_type());
                assert_eq!(
                    &Response::decode(h.msg_type, body).unwrap(),
                    r,
                    "{}",
                    s.name
                );
                responses.insert(r.name());
            }
        }
        let json = match &s.frame {
            Frame::Request(r) => json::request_to_json(r),
            Frame::Response(r) => json::response_to_json(r),
        };
        let text = json.to_pretty();
        let back = json::parse(&text).unwrap();
        assert_eq!(back, json, "{}: JSON text round trip", s.name);
        match &s.frame {
            Frame::Request(r) => {
                assert_eq!(&json::request_from_json(&back).unwrap(), r, "{}", s.name)
            }
            Frame::Response(r) => {
                assert_eq!(&json::response_from_json(&back).unwrap(), r, "{}", s.name)
            }
        }
    }
    assert_eq!(requests.len(), 10, "every request type has a sample");
    assert_eq!(responses.len(), 11, "every response type has a sample");
    let names: std::collections::HashSet<_> = all.iter().map(|s| s.name).collect();
    assert_eq!(names.len(), all.len(), "sample names are unique");
}

#[test]
fn an_empty_message_costs_one_byte_besides_its_body() {
    let mut with = Vec::new();
    let one = |m: Message| {
        let mut out = Vec::new();
        encode_request(
            &mut out,
            0,
            &Request::Produce(Produce {
                topic: "t".into(),
                partition: 0,
                producer_id: 0,
                producer_seq: 0,
                ack_level: 1,
                expected_leader_epoch: 0,
                timeout_ms: 0,
                messages: vec![m],
            }),
        );
        out.len()
    };
    with.push(one(Message::body(&b"abc"[..])));
    with.push(one(Message::body(&b""[..])));
    assert_eq!(with[0] - with[1], 3);
    // flags (1) + body length (4) + body.
    let base = one(Message::body(&b""[..]));
    let mut req = Vec::new();
    encode_request(
        &mut req,
        0,
        &Request::Produce(Produce {
            topic: "t".into(),
            partition: 0,
            producer_id: 0,
            producer_seq: 0,
            ack_level: 1,
            expected_leader_epoch: 0,
            timeout_ms: 0,
            messages: vec![],
        }),
    );
    assert_eq!(base - req.len(), 1 + 4);
}

#[test]
fn malformed_bodies_are_errors() {
    use types::*;
    // A message with reserved flags, and with the headers flag but no headers.
    let mut body = Vec::new();
    Request::Produce(Produce {
        topic: "t".into(),
        partition: 0,
        producer_id: 0,
        producer_seq: 0,
        ack_level: 1,
        expected_leader_epoch: 0,
        timeout_ms: 0,
        messages: vec![Message::body(&b"x"[..])],
    })
    .encode_body(&mut body);
    let flags_at = body.len() - 1 - 4 - 1;
    for (bad, want) in [
        (4u8, "reserved message flags"),
        (2, "headers flag with no headers"),
    ] {
        let mut b = body.clone();
        b[flags_at] = bad;
        if bad == 2 {
            b.splice(flags_at + 1..flags_at + 1, [0, 0]);
        }
        assert_eq!(
            Request::decode(PRODUCE, &b),
            Err(DecodeError::InvalidValue(want))
        );
    }
    let mut trailing = body.clone();
    trailing.push(0);
    assert_eq!(
        Request::decode(PRODUCE, &trailing),
        Err(DecodeError::TrailingBytes)
    );
    assert_eq!(
        Request::decode(PRODUCE, &body[..body.len() - 1]),
        Err(DecodeError::Truncated)
    );
    // A count far beyond the bytes present allocates nothing and fails.
    let huge = [0xff, 0xff, 0xff, 0xff];
    assert_eq!(
        Request::decode(METADATA, &huge),
        Err(DecodeError::Truncated)
    );
    assert_eq!(
        Request::decode(HELLO, &[0, 0, 0, 0, 1, 0, 0xff]),
        Err(DecodeError::InvalidUtf8)
    );
    assert_eq!(
        Request::decode(0x7777, &[]),
        Err(DecodeError::UnknownType(0x7777))
    );
    assert_eq!(
        Response::decode(PING, &[0; 8]),
        Err(DecodeError::UnknownType(PING)),
        "a request type is not a response"
    );
    assert_eq!(
        split_frame(&[9, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]),
        None,
        "length below 10"
    );
}

#[test]
fn error_codes_are_stable() {
    for (i, c) in ErrorCode::ALL.iter().enumerate() {
        assert_eq!(*c as u16, i as u16 + 1);
        assert_eq!(ErrorCode::from_name(c.name()), Some(*c));
    }
    assert_eq!(ErrorCode::from_u16(0), None);
    assert_eq!(ErrorCode::from_u16(16), None);
    let retryable: Vec<_> = ErrorCode::ALL
        .into_iter()
        .filter(|c| c.is_retryable())
        .collect();
    assert_eq!(
        retryable,
        [
            ErrorCode::Overloaded,
            ErrorCode::NotLeader,
            ErrorCode::FencedLeaderEpoch
        ]
    );
}

#[test]
fn partitioner_is_crc32c_mod_count() {
    assert_eq!(
        crc32c::crc32c(b"123456789"),
        0xE306_9283,
        "CRC-32/ISCSI check value"
    );
    assert_eq!(partition_for_key(b"", 7), 0);
    assert_eq!(partition_for_key(b"123456789", 16), 0xE306_9283 % 16);
    assert_eq!(partition_for_key(b"anything", 1), 0);
    assert_eq!(partition_for_key(b"anything", 0), 0);
}

#[test]
fn json_parser_rejects_what_it_does_not_support() {
    for bad in [
        "",
        "1.5",
        "[1,]",
        "{\"a\" 1}",
        "\"\\x\"",
        "nul",
        "[1] 2",
        &"[".repeat(100),
    ] {
        assert!(json::parse(bad).is_err(), "{bad:?}");
    }
    assert_eq!(
        json::parse(" {\"a\": [-1, true, null, \"\\u00e9\"]} ")
            .unwrap()
            .get("a")
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        4
    );
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 2048, ..ProptestConfig::default() })]

    /// Every request and response type, and unknown ones, on arbitrary
    /// bodies.
    #[test]
    fn decoding_arbitrary_bytes_never_panics(t in prop_oneof![1u16..=0x000b, 0x8001u16..=0x800b, Just(0xffffu16), any::<u16>()], body in prop::collection::vec(any::<u8>(), 0..256)) {
        let _ = Request::decode(t, &body);
        let _ = Response::decode(t, &body);
        let _ = split_frame(&body);
    }

    /// Mutations of valid frames: one changed byte, or a cut.
    #[test]
    fn decoding_damaged_frames_never_panics(i in 0usize..64, at in any::<prop::sample::Index>(), byte in any::<u8>(), cut in any::<bool>()) {
        let all = samples();
        let s = &all[i % all.len()];
        let mut bytes = encode(s);
        let at = at.index(bytes.len());
        if cut { bytes.truncate(at) } else { bytes[at] = byte }
        if bytes.len() >= FRAME_HEADER {
            let h = FrameHeader::parse(bytes[..FRAME_HEADER].try_into().unwrap());
            let _ = Request::decode(h.msg_type, &bytes[FRAME_HEADER..]);
            let _ = Response::decode(h.msg_type, &bytes[FRAME_HEADER..]);
        }
    }

    #[test]
    fn arbitrary_messages_round_trip(key in prop::option::of(prop::collection::vec(any::<u8>(), 0..40)), headers in prop::collection::vec((prop::collection::vec(any::<u8>(), 0..8), prop::collection::vec(any::<u8>(), 0..8)), 0..5), body in prop::collection::vec(any::<u8>(), 0..64), seq in any::<u64>()) {
        let message = Message { key, headers: headers.into_iter().map(|(name, value)| Header { name, value }).collect(), body };
        let resp = Response::FetchOk(FetchOk { leader_epoch: 1, start_seq: 0, next_seq: seq, messages: vec![FetchedMessage { seq, timestamp_ms: seq, message }] });
        let mut out = Vec::new();
        encode_response(&mut out, seq, &resp);
        let (h, b) = split_frame(&out).unwrap();
        prop_assert_eq!(Response::decode(h.msg_type, b).unwrap(), resp);
    }
}

/// Found by fuzzing (`fuzz/`, target `proto_json`): `from_str_radix`
/// takes a leading `+`, so `unhex` read `"+0"` as the byte 0.
#[test]
fn unhex_takes_hex_digits_only() {
    for s in ["+0", "+f", "0+0f", "-1", " 0"] {
        assert_eq!(json::unhex(s), None, "{s:?}");
    }
    assert_eq!(json::unhex("00fFa0"), Some(vec![0, 0xff, 0xa0]));
}
