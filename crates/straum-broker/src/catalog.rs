//! Topics, their configuration and the placement table.
//!
//! **Topic configuration lives in the WAL.** CreateTopic stores a
//! `TopicRecord` as an entry of the reserved store topic `m/topics`, with
//! the same durable group commit as messages. At startup the broker reads
//! that topic from its first entry, so replaying the WAL on another node
//! gives the same topics in the same order. No clock or local condition
//! takes part.
//!
//! **Partitions map to store topics** one to one: partition `p` of topic
//! `name` is the store topic `t/<name>/<p>`. Its seq space is the store
//! topic's `topic_seq`. Topic names allow no `/`, so the store names cannot
//! collide.
//!
//! **Placement** is an explicit table `(topic, partition) → (leader,
//! replicas, shard, leader_epoch)`; every lookup of a partition goes through
//! it. Version 0 derives every entry the same way (this node, replicas
//! `[this node]`, shard 0, epoch 1) and does not persist it: placement
//! belongs to the controller of a later version.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use straum_proto::ErrorCode;

use crate::sync::Event;

/// The store topic holding `TopicRecord`s.
pub const META_TOPIC: &[u8] = b"m/topics";
const RECORD_VERSION: u8 = 1;
const KIND_CREATE_TOPIC: u8 = 1;
pub const MAX_NAME: usize = 80;
/// Version 0 assigns every partition this leader epoch.
pub const LEADER_EPOCH: u32 = 1;

pub fn store_topic(name: &str, partition: u32) -> Vec<u8> {
    format!("t/{name}/{partition}").into_bytes()
}

/// docs/protocol.md "Topic names": `MalformedRequest` for an invalid name,
/// `Forbidden` for a reserved one.
pub fn check_name(name: &str) -> Result<(), ErrorCode> {
    let valid = !name.is_empty()
        && name.len() <= MAX_NAME
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if !valid {
        return Err(ErrorCode::MalformedRequest);
    }
    if name.starts_with("__") {
        return Err(ErrorCode::Forbidden);
    }
    Ok(())
}

/// A topic's configuration as recorded in `m/topics`:
/// `version u8 = 1 | kind u8 = 1 (create) | name (u16 length, bytes) |
/// partitions u32 | replication_factor u16 | retention_ms u64`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopicRecord {
    pub name: String,
    pub partitions: u32,
    pub replication_factor: u16,
    pub retention_ms: u64,
}

impl TopicRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut b = vec![RECORD_VERSION, KIND_CREATE_TOPIC];
        b.extend_from_slice(&(self.name.len() as u16).to_le_bytes());
        b.extend_from_slice(self.name.as_bytes());
        b.extend_from_slice(&self.partitions.to_le_bytes());
        b.extend_from_slice(&self.replication_factor.to_le_bytes());
        b.extend_from_slice(&self.retention_ms.to_le_bytes());
        b
    }

    pub fn decode(b: &[u8]) -> Option<Self> {
        if b.get(..2)? != [RECORD_VERSION, KIND_CREATE_TOPIC] {
            return None;
        }
        let len = u16::from_le_bytes(b.get(2..4)?.try_into().ok()?) as usize;
        let name = String::from_utf8(b.get(4..4 + len)?.to_vec()).ok()?;
        let rest = b.get(4 + len..)?;
        if rest.len() != 14 {
            return None;
        }
        Some(Self {
            name,
            partitions: u32::from_le_bytes(rest[..4].try_into().ok()?),
            replication_factor: u16::from_le_bytes(rest[4..6].try_into().ok()?),
            retention_ms: u64::from_le_bytes(rest[6..14].try_into().ok()?),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Placement {
    pub leader: u32,
    pub replicas: Vec<u32>,
    pub shard: u32,
    pub leader_epoch: u32,
}

/// A partition's state on this node.
pub struct Partition {
    pub store_topic: Vec<u8>,
    /// Mirrors the store: updated after every commit of this partition.
    pub next_seq: Cell<u64>,
    pub start_seq: Cell<u64>,
    /// Notified after every commit of this partition, and at shutdown.
    pub committed: Event,
}

pub struct PlacementEntry {
    pub placement: Placement,
    pub partition: Rc<Partition>,
}

pub struct Catalog {
    node_id: u32,
    /// By name, so listings are sorted.
    topics: BTreeMap<String, TopicRecord>,
    placement: HashMap<(String, u32), PlacementEntry>,
}

impl Catalog {
    pub fn new(node_id: u32) -> Self {
        Self {
            node_id,
            topics: BTreeMap::new(),
            placement: HashMap::new(),
        }
    }

    pub fn topic(&self, name: &str) -> Option<&TopicRecord> {
        self.topics.get(name)
    }

    /// Topics whose names sort after `cursor` (all for an empty one), by name.
    pub fn topics_after(&self, cursor: &str) -> impl Iterator<Item = &TopicRecord> {
        use std::ops::Bound::{Excluded, Unbounded};
        self.topics
            .range::<str, _>((Excluded(cursor), Unbounded))
            .map(|(_, t)| t)
    }

    /// Adds a recorded topic and places its partitions on this node.
    /// `seqs(store_topic)` gives a partition's `(start_seq, next_seq)`.
    pub fn add(&mut self, record: TopicRecord, seqs: impl Fn(&[u8]) -> (u64, u64)) {
        for p in 0..record.partitions {
            let store_topic = store_topic(&record.name, p);
            let (start, next) = seqs(&store_topic);
            let partition = Rc::new(Partition {
                store_topic,
                next_seq: Cell::new(next),
                start_seq: Cell::new(start),
                committed: Event::default(),
            });
            let placement = Placement {
                leader: self.node_id,
                replicas: vec![self.node_id],
                shard: 0,
                leader_epoch: LEADER_EPOCH,
            };
            self.placement.insert(
                (record.name.clone(), p),
                PlacementEntry {
                    placement,
                    partition,
                },
            );
        }
        self.topics.insert(record.name.clone(), record);
    }

    /// The placement-table lookup every request on a partition goes
    /// through.
    pub fn lookup(&self, topic: &str, partition: u32) -> Result<&PlacementEntry, ErrorCode> {
        if !self.topics.contains_key(topic) {
            return Err(ErrorCode::UnknownTopic);
        }
        self.placement
            .get(&(topic.to_owned(), partition))
            .ok_or(ErrorCode::UnknownPartition)
    }

    pub fn partitions(&self) -> impl Iterator<Item = &Rc<Partition>> {
        self.placement.values().map(|e| &e.partition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_round_trip() {
        let r = TopicRecord {
            name: "orders".into(),
            partitions: 1,
            replication_factor: 1,
            retention_ms: 86_400_000,
        };
        assert_eq!(TopicRecord::decode(&r.encode()), Some(r.clone()));
        let mut b = r.encode();
        b.push(0);
        assert_eq!(TopicRecord::decode(&b), None, "trailing byte");
        assert_eq!(TopicRecord::decode(&b[..5]), None);
        assert_eq!(TopicRecord::decode(&[2, 1]), None, "unknown version");
    }

    #[test]
    fn names_and_lookups() {
        assert_eq!(check_name("orders.v1_x-2"), Ok(()));
        assert_eq!(check_name(""), Err(ErrorCode::MalformedRequest));
        assert_eq!(check_name("a/b"), Err(ErrorCode::MalformedRequest));
        assert_eq!(
            check_name(&"a".repeat(81)),
            Err(ErrorCode::MalformedRequest)
        );
        assert_eq!(check_name("__internal"), Err(ErrorCode::Forbidden));
        let mut c = Catalog::new(3);
        c.add(
            TopicRecord {
                name: "t".into(),
                partitions: 1,
                replication_factor: 1,
                retention_ms: 0,
            },
            |_| (0, 5),
        );
        let e = c.lookup("t", 0).unwrap();
        assert_eq!(
            e.placement,
            Placement {
                leader: 3,
                replicas: vec![3],
                shard: 0,
                leader_epoch: 1
            }
        );
        assert_eq!(e.partition.next_seq.get(), 5);
        assert_eq!(e.partition.store_topic, b"t/t/0");
        assert!(matches!(c.lookup("t", 1), Err(ErrorCode::UnknownPartition)));
        assert!(matches!(c.lookup("u", 0), Err(ErrorCode::UnknownTopic)));
    }
}
