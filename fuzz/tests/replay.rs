//! Replays the seed corpora and every saved reproducer under
//! `regressions/<target>/` (hex text, whitespace ignored) through the fuzz
//! targets' checks, so the harness and its findings run on stable.

use std::path::Path;

use straum_fuzz::{base_store, checkpoint_base, proto_frame, seeds, store_open, wal_base};

fn run(target: &str, data: &[u8]) {
    match target {
        "proto_frame" => proto_frame(data),
        "store_wal" => store_open(&wal_base(base_store()), data),
        "store_checkpoint" => store_open(&checkpoint_base(base_store()), data),
        t => panic!("unknown target {t}"),
    }
}

fn unhex(text: &str) -> Vec<u8> {
    let digits: Vec<u8> = text.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    digits
        .chunks(2)
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn seeds_hold_the_invariants() {
    for (target, name, bytes) in seeds() {
        eprintln!("{target}/{name}");
        run(target, &bytes);
    }
}

#[test]
fn base_store_is_deterministic() {
    let dir = straum_fuzz::scratch().with_extension("again");
    let again = straum_fuzz::build_store(&dir, straum_store::WriteMode::Sequential);
    let _ = std::fs::remove_dir_all(&dir);
    assert_eq!(again.files, base_store().files);
}

fn replay_dir(root: &Path) {
    for target in ["proto_frame", "store_wal", "store_checkpoint"] {
        let Ok(dir) = std::fs::read_dir(root.join(target)) else {
            continue;
        };
        for entry in dir {
            let path = entry.unwrap().path();
            eprintln!("{}", path.display());
            run(target, &unhex(&std::fs::read_to_string(&path).unwrap()));
        }
    }
}

#[test]
fn regressions_hold_the_invariants() {
    replay_dir(&Path::new(env!("CARGO_MANIFEST_DIR")).join("regressions"));
}

/// Findings handed over and not yet fixed; move a file up to
/// `regressions/<target>/` once its fix lands.
#[test]
#[ignore = "open findings, handed to lane A"]
fn open_findings_hold_the_invariants() {
    replay_dir(&Path::new(env!("CARGO_MANIFEST_DIR")).join("regressions/open"));
}
