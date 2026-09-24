//! The routing index: topic in, subscribers out.
//!
//! A trie over topic levels rather than a scan over filters. The scan costs
//! one topic match per subscription — 40–95 ns each by the `domain`
//! benchmark, which at a thousand subscriptions is 60 µs per message and puts
//! the ceiling at sixteen thousand messages per second. The trie walks the
//! topic once, whatever the subscription count.
//!
//! Children are kept sorted and found by binary search: no hashing on the
//! publish path, and no linear scan of a wide level either.

use bytes::Bytes;
use domain::{DeliveryGuarantee, Topic, TopicFilter};

const SEPARATOR: u8 = b'/';
const SINGLE: &[u8] = b"+";
const MULTI: &[u8] = b"#";

/// A session's subscription as the index stores it: a slot, not an identity.
/// Slots are dense, so routing never hashes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Subscriber {
    pub slot: usize,
    pub guarantee: DeliveryGuarantee,
}

#[derive(Debug, Default)]
struct Node {
    /// Sorted by level, searched by bisection.
    children: Vec<(Bytes, Node)>,
    /// The `+` branch.
    single: Option<Box<Node>>,
    /// Subscribers of a filter ending in `#` here: they match this level and
    /// everything below it.
    multi: Vec<Subscriber>,
    /// Subscribers of a filter ending exactly here.
    exact: Vec<Subscriber>,
}

#[derive(Debug, Default)]
pub struct Index {
    root: Node,
}

/// The remaining levels of a topic. Copy, so a walk can branch without
/// allocating anything.
#[derive(Clone, Copy)]
struct Levels<'a>(Option<&'a [u8]>);

impl<'a> Levels<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self(Some(bytes))
    }

    fn split(self) -> Option<(&'a [u8], Self)> {
        let bytes = self.0?;
        match bytes.iter().position(|byte| *byte == SEPARATOR) {
            Some(at) => {
                let head = bytes.get(..at)?;
                let tail = bytes.get(at.saturating_add(1)..)?;
                Some((head, Self(Some(tail))))
            }
            None => Some((bytes, Self(None))),
        }
    }
}

impl Index {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds or upgrades one subscription. A session that subscribes twice to
    /// the same filter keeps one entry, at the stronger guarantee.
    pub fn subscribe(&mut self, filter: &TopicFilter, subscriber: Subscriber) {
        let mut node = &mut self.root;
        let mut levels = Levels::new(filter.as_bytes());

        while let Some((level, rest)) = levels.split() {
            if level == MULTI {
                upsert(&mut node.multi, subscriber);
                return;
            }
            node = if level == SINGLE {
                node.single.get_or_insert_with(Box::default)
            } else {
                child(node, level)
            };
            levels = rest;
        }

        upsert(&mut node.exact, subscriber);
    }

    /// Removes one subscription. Empty nodes are left in place: a session that
    /// resubscribes is common, and pruning costs more than the empty vectors.
    pub fn unsubscribe(&mut self, filter: &TopicFilter, slot: usize) {
        let mut node = &mut self.root;
        let mut levels = Levels::new(filter.as_bytes());

        while let Some((level, rest)) = levels.split() {
            if level == MULTI {
                remove(&mut node.multi, slot);
                return;
            }
            node = if level == SINGLE {
                match node.single.as_mut() {
                    Some(single) => single,
                    None => return,
                }
            } else {
                match find(node, level) {
                    Some(index) => match node.children.get_mut(index) {
                        Some((_, child)) => child,
                        None => return,
                    },
                    None => return,
                }
            };
            levels = rest;
        }

        remove(&mut node.exact, slot);
    }

    /// Calls `visit` once per matching subscription, in no particular order.
    /// A session subscribed through several filters is visited several times;
    /// deduplication is the caller's, because only the caller knows what a
    /// session is.
    pub fn matches(&self, topic: &Topic, visit: &mut impl FnMut(Subscriber)) {
        let bytes = topic.as_bytes();
        // `$`-topics are invisible to a wildcard at the first level, and only
        // at the first level (3.1.1 §4.7.2).
        let reserved = bytes.first() == Some(&b'$');
        walk(&self.root, Levels::new(bytes), !reserved, visit);
    }
}

fn walk(node: &Node, levels: Levels<'_>, wildcards: bool, visit: &mut impl FnMut(Subscriber)) {
    if wildcards {
        for subscriber in &node.multi {
            visit(*subscriber);
        }
    }

    let Some((level, rest)) = levels.split() else {
        for subscriber in &node.exact {
            visit(*subscriber);
        }
        return;
    };

    if let Some(index) = find(node, level) {
        if let Some((_, child)) = node.children.get(index) {
            walk(child, rest, true, visit);
        }
    }
    if wildcards {
        if let Some(single) = node.single.as_deref() {
            walk(single, rest, true, visit);
        }
    }
}

fn find(node: &Node, level: &[u8]) -> Option<usize> {
    node.children
        .binary_search_by(|(key, _)| key.as_ref().cmp(level))
        .ok()
}

fn child<'a>(node: &'a mut Node, level: &[u8]) -> &'a mut Node {
    let found = node
        .children
        .binary_search_by(|(key, _)| key.as_ref().cmp(level));
    let index = match found {
        Ok(index) => index,
        Err(index) => {
            node.children
                .insert(index, (Bytes::copy_from_slice(level), Node::default()));
            index
        }
    };
    match node.children.get_mut(index) {
        Some((_, child)) => child,
        // The entry was just found or inserted at this index.
        None => unreachable!(),
    }
}

fn upsert(subscribers: &mut Vec<Subscriber>, subscriber: Subscriber) {
    match subscribers
        .iter_mut()
        .find(|held| held.slot == subscriber.slot)
    {
        Some(held) => held.guarantee = held.guarantee.max(subscriber.guarantee),
        None => subscribers.push(subscriber),
    }
}

fn remove(subscribers: &mut Vec<Subscriber>, slot: usize) {
    if let Some(index) = subscribers.iter().position(|held| held.slot == slot) {
        subscribers.swap_remove(index);
    }
}
