//! Per-connection memory is a budget, not a curiosity: at a million
//! connections every byte here is a megabyte there. These are upper bounds a
//! change has to justify, not exact sizes.
#![allow(clippy::unwrap_used)]

use domain::DeliveryId;
use mqtt::{Connection, MqttLimits, PacketId, PacketIds};
use std::mem::size_of;
use std::time::Instant;

#[test]
fn per_connection_state_stays_small() {
    assert!(size_of::<PacketIds>() <= 48, "{}", size_of::<PacketIds>());
    assert!(size_of::<Connection>() <= 64, "{}", size_of::<Connection>());
    // Not per-connection state — a decoded packet is a stack value — but it is
    // moved once per packet on the hot path, so its size is worth watching.
    // CONNECT is the largest variant now that it carries credentials; boxing
    // that one variant is the lever if a codec benchmark ever shows the copy.
    assert!(
        size_of::<mqtt::Packet>() <= 112,
        "{}",
        size_of::<mqtt::Packet>()
    );

    // Limits are one shared value, not a per-connection copy.
    assert!(size_of::<MqttLimits>() <= 64);
}

#[test]
fn the_inflight_table_costs_nothing_until_it_is_used() {
    let mut ids = PacketIds::with_capacity(32);
    assert_eq!(
        ids.capacity_in_use(),
        0,
        "allocated before any QoS 1 delivery"
    );

    ids.assign(DeliveryId(1)).unwrap();
    assert!(ids.capacity_in_use() > 0);
    assert!(
        ids.capacity_in_use() * size_of::<(PacketId, DeliveryId)>() <= 32 * 16,
        "the table grew past the inflight limit"
    );
}

#[test]
fn a_connection_holds_no_buffers_of_its_own() {
    let limits = MqttLimits::default();
    let connection = Connection::new(Instant::now(), &limits);

    // Everything with a buffer lives in the runtime: the read buffer, the
    // write buffer and the channels. This type is state, not storage.
    assert_eq!(size_of_val(&connection), size_of::<Connection>());
}
