//! D-145 A: the record kinds `docs/configuration.md` declares stable are
//! exactly the ones written and applied today — a proposal `apply` writes,
//! or the framing every batch and slot change carries — and every reserved
//! kind is refused by `apply` as not yet built.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use std::collections::BTreeSet;

use apply::{ApplyError, Options, Origin, Proposal, State};
use bytes::Bytes;
use domain::{DeliveryGuarantee, Topic, TopicFilter};
use journal::{Chain, Entry, Kind, decode};

/// One proposal of every kind `apply` writes. A new variant fails to
/// compile here until it has a sample (`written`).
fn samples() -> Vec<Proposal> {
    let origin = Origin {
        node: 1,
        session: 1,
    };
    vec![
        Proposal::Publish {
            origin,
            request: 1,
            topic: Topic::new(Bytes::from_static(b"t")).unwrap(),
            payload: Bytes::from_static(b"p"),
            properties: Bytes::new(),
            guarantee: DeliveryGuarantee::AtLeastOnce,
            retain: false,
        },
        Proposal::Subscribe {
            origin,
            request: 2,
            filter: TopicFilter::new(Bytes::from_static(b"t")).unwrap(),
            guarantee: DeliveryGuarantee::AtLeastOnce,
            options: Options::default(),
        },
        Proposal::Unsubscribe {
            origin,
            request: 3,
            filter: TopicFilter::new(Bytes::from_static(b"t")).unwrap(),
        },
        Proposal::Ack {
            origin,
            delivery: 0,
        },
        Proposal::SessionGone { origin },
        Proposal::NodeReset { node: 1, below: 2 },
    ]
}

fn written(proposal: &Proposal) {
    match proposal {
        Proposal::Publish { .. }
        | Proposal::Subscribe { .. }
        | Proposal::Unsubscribe { .. }
        | Proposal::Ack { .. }
        | Proposal::SessionGone { .. }
        | Proposal::NodeReset { .. } => {}
    }
}

fn block<'a>(doc: &'a str, marker: &str) -> Vec<(&'a str, u8)> {
    doc.split(&format!("<!-- {marker}:begin -->"))
        .nth(1)
        .and_then(|rest| rest.split(&format!("<!-- {marker}:end -->")).next())
        .expect("a kinds block")
        .lines()
        .filter_map(|l| l.split_once(" = "))
        .map(|(name, n)| (name.trim(), n.trim().parse().expect("a kind's number")))
        .collect()
}

/// One batch holding `entry`: the batch header, then the entry.
fn batch(chain: &mut Chain, kind: Kind, body: &[u8], properties: &[u8]) -> Vec<Vec<u8>> {
    let mut records = Vec::new();
    chain
        .batch(
            1,
            &[Entry {
                kind,
                properties,
                body,
            }],
            &mut records,
        )
        .unwrap();
    records
}

#[test]
fn the_stable_kinds_are_written_and_applied_and_the_reserved_are_refused() {
    let doc = include_str!("../../../docs/configuration.md");
    let stable = block(doc, "stable-kinds");
    let reserved = block(doc, "reserved-kinds");

    // Written: every sample, and the framing, applied in one log.
    let mut chain = Chain::genesis();
    let mut state = State::new(apply::Limits::default());
    let mut out = Vec::new();
    let mut applied = BTreeSet::new();
    for proposal in samples() {
        written(&proposal);
        let mut body = Vec::new();
        proposal.encode(&mut body).unwrap();
        for record in batch(&mut chain, proposal.kind(), &body, proposal.properties()) {
            let record = decode(&record).unwrap();
            state.apply(&record, &mut out).unwrap();
            applied.insert(format!("{:?}", record.kind));
        }
    }
    for kind in [Kind::SlotStart, Kind::SlotEnd] {
        for record in batch(&mut chain, kind, &[0; 8], &[]) {
            let record = decode(&record).unwrap();
            state.apply(&record, &mut out).unwrap();
            applied.insert(format!("{:?}", record.kind));
        }
    }
    let declared: BTreeSet<String> = stable.iter().map(|(n, _)| (*n).to_owned()).collect();
    assert_eq!(
        declared, applied,
        "stable kinds against those written and applied"
    );
    for (name, n) in &stable {
        assert_eq!(format!("{:?}", Kind::from_u8(*n).unwrap()), *name);
    }

    // Reserved: each refused as not yet built.
    for (name, n) in &reserved {
        let kind = Kind::from_u8(*n).unwrap_or_else(|| panic!("reserved {n} decodes to no kind"));
        let records = batch(&mut chain, kind, b"", &[]);
        state
            .apply(&decode(&records[0]).unwrap(), &mut out)
            .unwrap();
        assert_eq!(
            state.apply(&decode(&records[1]).unwrap(), &mut out),
            Err(ApplyError::NotYet(kind)),
            "{name}"
        );
        // The refused record is not applied; the next batch starts over.
        chain = Chain::genesis();
        state = State::new(apply::Limits::default());
    }
}
