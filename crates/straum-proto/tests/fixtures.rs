//! The golden frames (`conformance/frames/`) and partitioner vectors
//! (`conformance/partitioner/vectors.json`) are generated from this crate.
//! This test fails if the committed files differ from what it generates;
//! `STRAUM_UPDATE_FIXTURES=1 cargo test -p straum-proto --test fixtures committed_fixtures_are_up_to_date`
//! rewrites them.

mod common;

use std::path::{Path, PathBuf};

use common::{Frame, encode, samples};
use straum_proto::json::{self, Value};
use straum_proto::partition_for_key;

fn conformance() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../conformance")
}

fn obj(fields: Vec<(&str, Value)>) -> Value {
    Value::Obj(fields.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
}

/// Every file the generator owns: (path relative to conformance/, bytes).
fn generate() -> Vec<(String, Vec<u8>)> {
    let mut files = Vec::new();
    let mut index = Vec::new();
    for s in samples() {
        let bytes = encode(&s);
        let (direction, message) = match &s.frame {
            Frame::Request(r) => ("request", json::request_to_json(r)),
            Frame::Response(r) => ("response", json::response_to_json(r)),
        };
        let type_code = u16::from_le_bytes([bytes[4], bytes[5]]);
        let desc = obj(vec![
            ("name", Value::Str(s.name.into())),
            ("note", Value::Str(s.note.into())),
            ("direction", Value::Str(direction.into())),
            ("type_code", Value::Num(type_code.into())),
            ("correlation_id", Value::Str(s.correlation_id.to_string())),
            ("frame_bytes", Value::Num(bytes.len() as i64)),
            ("message", message),
        ]);
        files.push((format!("frames/{}.bin", s.name), bytes));
        files.push((
            format!("frames/{}.json", s.name),
            desc.to_pretty().into_bytes(),
        ));
        index.push(Value::Str(s.name.into()));
    }
    files.push((
        "frames/index.json".into(),
        obj(vec![("frames", Value::Arr(index))])
            .to_pretty()
            .into_bytes(),
    ));

    let mut keys: Vec<(&str, Vec<u8>)> = vec![
        ("empty", vec![]),
        ("a", b"a".to_vec()),
        ("check value", b"123456789".to_vec()),
        ("user-42", b"user-42".to_vec()),
        ("utf-8", "κλειδί-ü".as_bytes().to_vec()),
        ("binary", vec![0, 0xff, 0x80, 0x7f]),
        ("1 KiB", vec![b'k'; 1024]),
    ];
    for i in 0..9 {
        keys.push(("order", format!("order-{i}").into_bytes()));
    }
    let vectors = keys
        .iter()
        .map(|(what, k)| {
            obj(vec![
                ("note", Value::Str((*what).into())),
                ("key", Value::Str(json::hex(k))),
                ("crc32c", Value::Num(crc32c::crc32c(k).into())),
                (
                    "partitions",
                    Value::Arr(
                        (1..=16)
                            .map(|n| Value::Num(partition_for_key(k, n).into()))
                            .collect(),
                    ),
                ),
            ])
        })
        .collect();
    let doc = obj(vec![
        (
            "function",
            Value::Str(
                "partition = crc32c(key) mod partition_count (docs/protocol.md section 7)".into(),
            ),
        ),
        (
            "partitions",
            Value::Str("element i is the partition for partition_count = i + 1".into()),
        ),
        ("vectors", Value::Arr(vectors)),
    ]);
    files.push((
        "partitioner/vectors.json".into(),
        doc.to_pretty().into_bytes(),
    ));
    files
}

#[test]
fn committed_fixtures_are_up_to_date() {
    let root = conformance();
    let files = generate();
    if std::env::var_os("STRAUM_UPDATE_FIXTURES").is_some() {
        let _ = std::fs::remove_dir_all(root.join("frames"));
        for (path, bytes) in &files {
            let p = root.join(path);
            std::fs::create_dir_all(p.parent().unwrap()).unwrap();
            std::fs::write(p, bytes).unwrap();
        }
    }
    let mut stale = Vec::new();
    for (path, bytes) in &files {
        if std::fs::read(root.join(path)).ok().as_ref() != Some(bytes) {
            stale.push(path.clone());
        }
    }
    let generated: std::collections::HashSet<_> = files.iter().map(|(p, _)| p.clone()).collect();
    for entry in std::fs::read_dir(root.join("frames")).unwrap() {
        let name = format!("frames/{}", entry.unwrap().file_name().to_string_lossy());
        if !generated.contains(&name) {
            stale.push(format!("{name} (not generated)"));
        }
    }
    assert!(
        stale.is_empty(),
        "conformance fixtures out of date: {stale:?}; run STRAUM_UPDATE_FIXTURES=1 cargo test -p straum-proto --test fixtures committed_fixtures_are_up_to_date"
    );
}

/// What another language's decoder does with the fixtures: decode every
/// `.bin` and compare with its `.json`.
#[test]
fn fixtures_decode_to_their_descriptions() {
    let root = conformance().join("frames");
    let index = json::parse(&std::fs::read_to_string(root.join("index.json")).unwrap()).unwrap();
    for name in index.get("frames").unwrap().as_array().unwrap() {
        let name = name.as_str().unwrap();
        let bytes = std::fs::read(root.join(format!("{name}.bin"))).unwrap();
        let desc =
            json::parse(&std::fs::read_to_string(root.join(format!("{name}.json"))).unwrap())
                .unwrap();
        let (h, body) = straum_proto::split_frame(&bytes).unwrap();
        assert_eq!(
            desc.get("correlation_id").unwrap().as_str().unwrap(),
            h.correlation_id.to_string()
        );
        let message = desc.get("message").unwrap();
        match desc.get("direction").unwrap().as_str().unwrap() {
            "request" => assert_eq!(
                json::request_to_json(&straum_proto::Request::decode(h.msg_type, body).unwrap()),
                *message,
                "{name}"
            ),
            _ => assert_eq!(
                json::response_to_json(&straum_proto::Response::decode(h.msg_type, body).unwrap()),
                *message,
                "{name}"
            ),
        }
    }
}
