//! The per-`STRUCT_HASH` page directory and its **extendible hashing**
//! addressing ([RFC 0074]).
//!
//! Each `STRUCT_HASH` owns a `Vec<BlockDescriptor>` of `2^d` **entries**,
//! indexed by the low `d` bits of [`hash_of`] — `d`, the global depth, is
//! never stored: it is the length's `trailing_zeros`. Several entries may
//! point at one page: a page of **local depth** ℓ (in its descriptor) is
//! named by the `2^(d−ℓ)` entries `j ≡ c (mod 2^ℓ)`, and its **bucket** —
//! the key every per-page map uses — is the lowest of them, `c < 2^ℓ`.
//!
//! A page over target splits by itself ([`Directory::split`]): its records
//! partition by hash bit ℓ, and both halves take ℓ + 1. When ℓ = d the
//! directory doubles first, by copying itself — no record moves.
//!
//! ## The id hash
//!
//! `hash_of` is **SeaHash** over the `Id`'s 16 little-endian bytes. The seed is a
//! per-database random `[u64; 4]` (persisted in the database's `MANIFEST`), which gives the
//! DoS resistance (an attacker can't precompute bucket collisions). SeaHash is
//! portable across architecture and endianness, so the directory — rebuilt from
//! the headers on open — routes every record to the same bucket even if the data
//! files are opened on a different machine.
//!
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md
use crate::block::BlockDescriptor;
use seahash::hash_seeded;

/// The deepest a page splits to: past it a page stays over target, which
/// bounds the directory against records sharing their low hash bits.
pub const D_MAX: u8 = 32;

/// Entries a directory may hold per page before a split that would double
/// it waits: 128 bytes of RAM a page at most ([RFC 0074]).
///
/// [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md
pub const DIR_RATIO: usize = 16;

/// A SeaHash of a 128-bit `Id` under a per-database seed.
///
/// Portable across every machine, architecture, and endianness (the `seahash`
/// crate guarantees it), so data files reopened on a different machine route
/// every record to the same bucket. The `Id` is fed as its 16
/// little-endian bytes; the per-database `seed` randomises bucket placement so
/// collisions can't be precomputed.
#[must_use]
pub const fn hash_of(id: u128, seed: [u64; 4]) -> u64 {
    hash_seeded(&id.to_le_bytes(), seed[0], seed[1], seed[2], seed[3])
}

/// The bucket entry `entry` names: the lowest entry of its page.
#[must_use]
pub const fn canonical(entry: usize, desc: BlockDescriptor) -> usize {
    entry & ((1usize << desc.depth()) - 1)
}

/// Whether entry `entry` is its page's bucket rather than an alias.
#[must_use]
pub const fn is_bucket(entry: usize, desc: BlockDescriptor) -> bool {
    entry >> desc.depth() == 0
}

/// A per-`STRUCT_HASH` page directory: `2^d` [`BlockDescriptor`] entries
/// plus the per-database hash seed.
///
/// Pure addressing — the type's compression state
/// ([`DictState`](crate::dictionary::DictState)) lives beside it in the type's
/// `StructStorage` slot, not inside the directory.
#[derive(Debug, Clone)]
pub struct Directory {
    /// `2^d` entries; a page of depth ℓ fills `2^(d−ℓ)` of them.
    pub(crate) slots: Vec<BlockDescriptor>,
    /// How many pages (buckets) the entries name.
    pages: usize,
    /// Per-database hash seed (from the `MANIFEST`).
    seed: [u64; 4],
}

impl Directory {
    /// A directory with a single empty bucket.
    #[must_use]
    pub fn new(seed: [u64; 4]) -> Self {
        Self {
            slots: vec![BlockDescriptor::EMPTY],
            pages: 1,
            seed,
        }
    }

    /// Rebuild a directory from already-known entries (a reconstruction,
    /// which checked their shape: a power of two long, every page's depth
    /// within it and its aliases alike).
    #[must_use]
    pub fn from_slots(slots: Vec<BlockDescriptor>, seed: [u64; 4]) -> Self {
        debug_assert!(slots.len().is_power_of_two(), "2^d entries");
        let pages = slots
            .iter()
            .enumerate()
            .filter(|&(j, &d)| is_bucket(j, d))
            .count();
        Self { slots, pages, seed }
    }

    /// Number of entries (`2^d`).
    #[must_use]
    pub const fn len(&self) -> usize {
        self.slots.len()
    }

    /// Always `false` — a directory always has at least one bucket.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }

    /// The global depth `d`.
    #[must_use]
    pub const fn depth(&self) -> u8 {
        self.slots.len().trailing_zeros() as u8
    }

    /// Number of pages (buckets).
    #[must_use]
    pub const fn pages(&self) -> usize {
        self.pages
    }

    /// The per-database hash seed.
    #[must_use]
    pub const fn seed(&self) -> [u64; 4] {
        self.seed
    }

    /// Hash an `Id` (its raw `u128`) under this directory's seed.
    #[must_use]
    pub const fn hash(&self, id: u128) -> u64 {
        hash_of(id, self.seed)
    }

    /// The bucket whose entries contain `entry`.
    #[must_use]
    pub fn bucket_at(&self, entry: usize) -> usize {
        canonical(entry, self.slots[entry])
    }

    /// The bucket an `Id` routes to.
    #[must_use]
    pub fn bucket_of(&self, id: u128) -> usize {
        let entry = (self.hash(id) & (self.slots.len() as u64 - 1)) as usize;
        self.bucket_at(entry)
    }

    /// The descriptor of `bucket` (or of any entry naming its page).
    #[must_use]
    pub fn descriptor(&self, bucket: usize) -> BlockDescriptor {
        self.slots[bucket]
    }

    /// Point `bucket`'s page at `desc` — every entry naming it. The page
    /// keeps its depth, whatever `desc` carries: depth is the directory's
    /// to change, by [`split`](Self::split).
    pub fn set_descriptor(&mut self, bucket: usize, desc: BlockDescriptor) {
        let bucket = self.bucket_at(bucket);
        let depth = self.slots[bucket].depth();
        let desc = desc.with_depth(depth);
        for entry in (bucket..self.slots.len()).step_by(1 << depth) {
            self.slots[entry] = desc;
        }
    }

    /// Whether `bucket` may split now: below [`D_MAX`], and — when the
    /// split would double the directory — the directory under
    /// [`DIR_RATIO`] entries a page.
    #[must_use]
    pub fn can_split(&self, bucket: usize) -> bool {
        let depth = self.slots[bucket].depth();
        depth < D_MAX
            && (depth < self.depth()
                || self.slots.len() < DIR_RATIO * self.pages)
    }

    /// Split `bucket` by hash bit ℓ (its depth), doubling the directory
    /// first when ℓ = d, and return the new bucket `bucket + 2^ℓ`. The
    /// source keeps its descriptor — its old run, freed when the round
    /// installs the halves — and the new bucket starts empty; both take
    /// depth ℓ + 1.
    pub fn split(&mut self, bucket: usize) -> usize {
        let depth = self.slots[bucket].depth();
        if depth == self.depth() {
            self.slots.extend_from_within(..);
        }
        let deeper = depth + 1;
        let new = bucket + (1 << depth);
        let source = self.slots[bucket].with_depth(deeper);
        let target = BlockDescriptor::EMPTY.with_depth(deeper);
        for entry in (bucket..self.slots.len()).step_by(1 << deeper) {
            self.slots[entry] = source;
            self.slots[entry + (1 << depth)] = target;
        }
        self.pages += 1;
        new
    }

    /// Every bucket and its page's descriptor, once per page.
    pub fn buckets(
        &self,
    ) -> impl Iterator<Item = (usize, BlockDescriptor)> + '_ {
        self.slots
            .iter()
            .enumerate()
            .filter(|&(j, &d)| is_bucket(j, d))
            .map(|(j, &d)| (j, d))
    }

    /// Every entry, aliases included, in entry order.
    #[must_use]
    pub fn slots(&self) -> &[BlockDescriptor] {
        &self.slots
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::{D_MAX, DIR_RATIO, Directory, hash_of};
    use crate::block::BlockDescriptor;

    const SEED: [u64; 4] = [0x1234, 0x5678, 0x9abc, 0xdef0];

    #[test]
    fn hash_is_deterministic() {
        assert_eq!(hash_of(42, SEED), hash_of(42, SEED));
        assert_ne!(hash_of(42, SEED), hash_of(43, SEED));
        // Seed changes the result.
        assert_ne!(hash_of(42, SEED), hash_of(42, [9, 9, 9, 9]));
    }

    #[test]
    fn hash_covers_all_low_buckets() {
        // Sequential ids must spread across the low bucket bits, not concentrate
        // (the failure mode of a weak hash). With 4096 ids over 256 low-byte
        // values, a good hash hits every value; a degenerate one leaves gaps.
        let mut seen = std::collections::HashSet::new();
        for id in 0u128..4096 {
            seen.insert(hash_of(id, SEED) & 0xFF);
        }
        assert_eq!(
            seen.len(),
            256,
            "hash left low-byte buckets empty: {}",
            seen.len()
        );
    }

    /// A split moves only the records of the page it splits, and those by
    /// hash bit ℓ; every other id keeps its bucket.
    #[test]
    fn a_split_moves_only_its_own_records() {
        let mut dir = Directory::new(SEED);
        for round in 0..40u128 {
            let before: BTreeMap<u128, usize> =
                (0..2000).map(|id| (id, dir.bucket_of(id))).collect();
            let source = dir.bucket_of(round * 7919);
            let depth = dir.descriptor(source).depth();
            let new = dir.split(source);
            assert_eq!(new, source + (1 << depth));
            for (id, was) in before {
                let now = dir.bucket_of(id);
                if was != source {
                    assert_eq!(now, was, "an unrelated id moved");
                } else if (dir.hash(id) >> depth) & 1 == 0 {
                    assert_eq!(now, source);
                } else {
                    assert_eq!(now, new);
                }
            }
        }
    }

    /// A doubling copies the entries: every id keeps its bucket.
    #[test]
    fn a_doubling_moves_no_record() {
        let mut dir = Directory::new(SEED);
        dir.set_descriptor(0, BlockDescriptor::new(10, 4));
        let new = dir.split(0);
        assert_eq!((dir.len(), dir.depth(), new), (2, 1, 1));
        dir.split(0);
        assert_eq!((dir.len(), dir.pages()), (4, 3));
        // Bucket 1 (depth 1) now has two entries, 1 and 3.
        assert_eq!(dir.bucket_at(3), 1);
        assert_eq!(dir.descriptor(0).run().start, 10, "the source's run");
        assert!(!dir.descriptor(2).is_allocated(), "the new half is empty");
    }

    /// Setting a descriptor writes every alias and keeps the page's depth.
    #[test]
    fn set_descriptor_writes_every_alias() {
        let mut dir = Directory::new(SEED);
        dir.split(0);
        dir.split(0); // entries 0..4: 0 (ℓ2), 1 (ℓ1), 2 (ℓ2), 3 → 1
        dir.set_descriptor(1, BlockDescriptor::new(77, 3));
        assert_eq!(dir.slots()[1], dir.slots()[3]);
        assert_eq!(dir.slots()[3].run().start, 77);
        assert_eq!(dir.slots()[3].depth(), 1);
        let buckets: Vec<usize> = dir.buckets().map(|(c, _)| c).collect();
        assert_eq!(buckets, [0, 1, 2], "once per page");
    }

    /// Nothing splits past `D_MAX`, and a doubling waits while the
    /// directory holds `DIR_RATIO` entries a page.
    #[test]
    fn splits_stop_at_the_caps() {
        let mut dir = Directory::new(SEED);
        // Split bucket 0 only: one deep page, the rest shallow.
        while dir.can_split(0) {
            dir.split(0);
        }
        let depth = dir.descriptor(0).depth();
        assert!(dir.len() >= DIR_RATIO * dir.pages());
        assert!(depth < D_MAX, "the ratio binds first: {depth}");
        // A shallow page still splits without doubling.
        assert!(dir.can_split(1));
        let len = dir.len();
        dir.split(1);
        assert_eq!(dir.len(), len);
    }

    /// Rebuilt from its entries, a directory counts its pages again.
    #[test]
    fn from_slots_counts_pages() {
        let mut dir = Directory::new(SEED);
        for b in [0, 0, 1, 0] {
            dir.split(b);
        }
        let again = Directory::from_slots(dir.slots().to_vec(), SEED);
        assert_eq!(again.pages(), dir.pages());
        assert_eq!(again.slots(), dir.slots());
        let routed: BTreeSet<usize> =
            (0u128..500).map(|id| again.bucket_of(id)).collect();
        let named: BTreeSet<usize> = dir.buckets().map(|(c, _)| c).collect();
        assert_eq!(routed, named, "every page reachable, no other");
    }
}
