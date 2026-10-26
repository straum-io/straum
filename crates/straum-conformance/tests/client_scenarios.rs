//! The client scenarios of `conformance/client-scenarios/` with
//! straum-client against the fake server, and the fake server's own
//! refusals: a client that breaks a rule fails its scenario.

#![allow(clippy::unwrap_used)]

use straum_conformance::{client, conformance_dir, load_dir};
use straum_proto::json;

#[test]
fn client_scenarios_pass() {
    let scenarios = load_dir(&conformance_dir().join("client-scenarios")).unwrap();
    assert!(scenarios.len() >= 8);
    let failed: Vec<String> = scenarios
        .iter()
        .filter_map(|(path, s)| {
            client::run(s)
                .err()
                .map(|e| format!("{}: {e}", path.display()))
        })
        .collect();
    assert!(failed.is_empty(), "{failed:#?}");
}

/// Edits of a passing scenario that the client cannot satisfy: the runner
/// must report them, so a passing suite means something.
#[test]
fn broken_expectations_fail() {
    let path = conformance_dir().join("client-scenarios/not_leader_then_metadata.json");
    let text = std::fs::read_to_string(path).unwrap();
    for (from, to, why) in [
        (
            "\"producer_seq\": \"5\",\n          \"ack_level\": 1,\n          \"expected_leader_epoch\": 3",
            "\"producer_seq\": \"6\",\n          \"ack_level\": 1,\n          \"expected_leader_epoch\": 3",
            "a retry with another producer_seq",
        ),
        (
            "\"base_seq\": \"11\",\n      \"leader_epoch\": 3",
            "\"base_seq\": \"99\",\n      \"leader_epoch\": 3",
            "a wrong result",
        ),
    ] {
        assert!(text.contains(from), "fixture text changed: {why}");
        let s = json::parse(&text.replace(from, to)).unwrap();
        assert!(client::run(&s).is_err(), "{why} must fail");
    }
    let extra = json::parse(
        &std::fs::read_to_string(conformance_dir().join("client-scenarios/produce_ok.json"))
            .unwrap(),
    )
    .unwrap();
    let json::Value::Obj(mut fields) = extra else {
        panic!()
    };
    for (k, v) in &mut fields {
        if k == "server"
            && let json::Value::Arr(steps) = v
        {
            // The script ends before the Produce: the client's request is extra.
            steps.pop();
        }
    }
    assert!(
        client::run(&json::Value::Obj(fields)).is_err(),
        "an unexpected request must fail"
    );
}
