//! D-141 A: the reason codes `docs/configuration.md` declares stable are
//! exactly the ones the gateway names in its source. A malformed packet's
//! reason comes from `codec` and is not among them.

use std::collections::BTreeSet;

#[test]
fn the_stable_reason_codes_are_the_ones_sent() {
    let doc = include_str!("../../../docs/configuration.md");
    let block = doc
        .split("<!-- stable-reasons:begin -->")
        .nth(1)
        .and_then(|rest| rest.split("<!-- stable-reasons:end -->").next())
        .expect("the stable-reasons block");
    let declared: BTreeSet<&str> = block
        .lines()
        .map(str::trim)
        .filter(|l| l.chars().next().is_some_and(|c| c.is_ascii_uppercase()))
        .collect();
    let source = include_str!("../src/lib.rs");
    let named: BTreeSet<&str> = source
        .split("ReasonCode::")
        .skip(1)
        .map(|rest| {
            let end = rest
                .find(|c: char| !c.is_ascii_alphanumeric())
                .unwrap_or(rest.len());
            &rest[..end]
        })
        .collect();
    assert_eq!(declared, named);
}
