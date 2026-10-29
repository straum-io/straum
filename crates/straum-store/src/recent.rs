//! The recent-id cache (ADR-0014 §1, step 6a): id → newest live number for
//! ids written or deduplicated recently, so a repeat costs no id-run page
//! read in the common case. Two generations: new entries go to the young
//! map; when it holds half the capacity it becomes the old map (the
//! previous old one is dropped), and a hit in the old map moves the entry
//! back to the young one. At most `capacity` entries, and no per-entry
//! list to maintain.

use std::collections::HashMap;

use crate::codec::ObjectId;

pub struct RecentIds {
    half: usize,
    young: HashMap<ObjectId, u64>,
    old: HashMap<ObjectId, u64>,
}

impl RecentIds {
    /// A cache of at most `capacity` entries; 0 or 1 keeps none.
    pub fn new(capacity: usize) -> Self {
        Self {
            half: capacity / 2,
            young: HashMap::new(),
            old: HashMap::new(),
        }
    }

    /// The number cached for `id`, refreshing it.
    pub fn get(&mut self, id: &ObjectId) -> Option<u64> {
        if let Some(&n) = self.young.get(id) {
            return Some(n);
        }
        let n = self.old.remove(id)?;
        self.insert(*id, n);
        Some(n)
    }

    pub fn insert(&mut self, id: ObjectId, number: u64) {
        if self.half == 0 {
            return;
        }
        self.young.insert(id, number);
        if self.young.len() >= self.half {
            self.old = std::mem::take(&mut self.young);
        }
    }

    /// Drops `id` if it is cached under `number` (compaction reclaimed it).
    pub fn remove(&mut self, id: &ObjectId, number: u64) {
        for map in [&mut self.young, &mut self.old] {
            if map.get(id) == Some(&number) {
                map.remove(id);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.young.len() + self.old.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Heap bytes the maps hold: every bucket's entry and its control byte
    /// (hashbrown), whether filled or not.
    pub fn bytes(&self) -> usize {
        let bucket = size_of::<(ObjectId, u64)>() + 1;
        (buckets(&self.young) + buckets(&self.old)) * bucket
    }
}

/// Buckets behind a map's capacity (a power of two, 8/7 of the capacity).
fn buckets(m: &HashMap<ObjectId, u64>) -> usize {
    match m.capacity() {
        0 => 0,
        c if c < 8 => (c + 1).next_power_of_two(),
        c => (c * 8 / 7).next_power_of_two(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(i: u64) -> ObjectId {
        ObjectId::of(&i.to_le_bytes())
    }

    #[test]
    fn holds_at_most_its_capacity_and_keeps_what_is_used() {
        let mut c = RecentIds::new(100);
        for i in 0..1000 {
            c.insert(id(i), i);
            assert!(c.len() <= 100);
            // Entry 0 stays: it is used again and again.
            assert_eq!(c.get(&id(0)), Some(0), "{i}");
        }
        assert_eq!(c.get(&id(0)), Some(0));
        assert_eq!(c.get(&id(5)), None, "an old unused entry is dropped");
        assert_eq!(c.get(&id(999)), Some(999));
        c.remove(&id(999), 7);
        assert_eq!(c.get(&id(999)), Some(999), "another number: kept");
        c.remove(&id(999), 999);
        assert_eq!(c.get(&id(999)), None);
        let mut none = RecentIds::new(1);
        none.insert(id(1), 1);
        assert!(none.is_empty());
    }
}
