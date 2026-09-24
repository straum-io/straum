//! How hard the broker tries to deliver a message.

/// Ordered: `AtLeastOnce` is the stronger guarantee, so `max` picks the
/// stronger of two overlapping subscriptions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DeliveryGuarantee {
    /// Sent once, never repeated, may be dropped under pressure.
    AtMostOnce,
    /// Held until the receiver acknowledges it.
    AtLeastOnce,
}

/// Identifies one at-least-once delivery to one session until it is
/// acknowledged. Assigned by the broker; protocols map it to whatever their
/// wire format uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeliveryId(pub u64);

/// How far a message has to get before the broker admits to having it.
///
/// Orthogonal to [`DeliveryGuarantee`]: that one is about redelivery to a
/// subscriber, this one is about surviving a crash. A message can be
/// at-least-once and `Memory`, or at-most-once and `Fsynced`. The semantics
/// of each level are in `docs/storage.md` §80.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Durability {
    /// In broker memory. No persistence guarantee.
    Memory,
    /// Handed to the operating system. May not survive machine or storage
    /// failure.
    Written,
    /// Covered by a completed durability operation. Only this advances the
    /// durable acknowledgement boundary.
    Fsynced,
}
