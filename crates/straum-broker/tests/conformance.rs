//! The broker scenarios of `conformance/scenarios/`, each against a fresh
//! process of this crate's binary.

#![allow(clippy::unwrap_used)]

use std::path::Path;

#[test]
#[ignore = "long: every scenario against a fresh broker process"]
fn broker_scenarios_pass() {
    let bin = Path::new(env!("CARGO_BIN_EXE_straum-server"));
    let scenarios =
        straum_conformance::load_dir(&straum_conformance::conformance_dir().join("scenarios"))
            .unwrap();
    assert!(scenarios.len() >= 10, "{} scenarios", scenarios.len());
    let failed: Vec<String> = scenarios
        .iter()
        .filter_map(|(path, s)| {
            straum_conformance::broker::run(bin, s)
                .err()
                .map(|e| format!("{}: {e}", path.display()))
        })
        .collect();
    assert!(failed.is_empty(), "{failed:#?}");
}
