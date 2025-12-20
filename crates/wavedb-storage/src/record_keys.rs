//! The record cache's ordered keys ([RFC 0078] §7): per slot, the keys of
//! an ordered lane its index names, sorted — the index, a hash table,
//! cannot list them in order — with the bytes they take kept as they
//! change, since eviction asks for the cache's footprint once per chunk it
//! drops and a sum over every key would make each ask a walk of them all.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::BTreeSet;

/// A key's `Vec` and its share of a B-tree node, about.
const PER_KEY: usize = 40;

/// What one held key costs.
const fn cost(key: &[u8]) -> usize {
    key.len() + PER_KEY
}

/// The keys (see the module docs).
#[derive(Debug)]
pub struct OrderedKeys {
    /// Per slot; empty for a slot of anchors.
    sets: Vec<BTreeSet<Box<[u8]>>>,
    /// What every key held costs, by [`cost`].
    bytes: usize,
}

impl OrderedKeys {
    /// None held, for `types` registered slots.
    pub fn new(types: usize) -> Self {
        Self {
            sets: vec![BTreeSet::new(); types],
            bytes: 0,
        }
    }

    /// Hold `key` in `slot`, if it isn't already.
    pub fn insert(&mut self, slot: usize, key: &[u8]) {
        if let Some(keys) = self.sets.get_mut(slot)
            && !keys.contains(key)
        {
            self.bytes += cost(key);
            keys.insert(key.into());
        }
    }

    /// Stop holding `key` in `slot`.
    pub fn remove(&mut self, slot: usize, key: &[u8]) {
        if let Some(keys) = self.sets.get_mut(slot)
            && keys.remove(key)
        {
            self.bytes = self.bytes.saturating_sub(cost(key));
        }
    }

    /// `slot`'s keys.
    pub fn of(&self, slot: usize) -> Option<&BTreeSet<Box<[u8]>>> {
        self.sets.get(slot)
    }

    /// Bytes the keys take, about.
    pub const fn footprint(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{OrderedKeys, cost};

    #[test]
    fn the_footprint_follows_inserts_and_removes() {
        let mut keys = OrderedKeys::new(2);
        keys.insert(0, b"abc");
        keys.insert(0, b"abc");
        keys.insert(1, b"de");
        assert_eq!(keys.footprint(), cost(b"abc") + cost(b"de"));
        keys.remove(0, b"abc");
        keys.remove(0, b"abc");
        keys.remove(1, b"zz");
        assert_eq!(keys.footprint(), cost(b"de"));
        keys.remove(1, b"de");
        assert_eq!(keys.footprint(), 0);
    }

    #[test]
    fn a_slot_past_the_registered_ones_holds_nothing() {
        let mut keys = OrderedKeys::new(1);
        keys.insert(3, b"abc");
        assert_eq!(keys.footprint(), 0);
        assert!(keys.of(3).is_none());
    }
}
