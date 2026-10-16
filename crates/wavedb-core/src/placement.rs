//! Shard placement ([RFC 0067] §1): which shard of a database holds an owner.
//!
//! A pure function of what an operation belongs to — a Pivot, or a Unique
//! type under a tenant — and that is the whole placement layer: no table to
//! consult, no split to schedule, nothing to migrate, and no balancer.
//! Uniformity comes from there being millions of owners (one Pivot per
//! holder) rather than from anyone measuring load.
//!
//! `shard = seahash(owner bytes) % shards`. The byte layout of both owners is
//! frozen and pinned by golden vectors below: once the shard decides which
//! `data.bin` holds a record, changing this function puts every record in
//! the wrong shard, exactly as changing a `STRUCT_HASH` would. SeaHash is the
//! project's identity hash, pinned `=4.1.0`, which covers the rest.
//!
//! The hash before the remainder is arithmetic, not taste: both raw keys
//! collapse modulo a power of two. A Pivot's key is `key_nanos()` =
//! `ms · 10^6 + counter`, and `10^6` is a multiple of 64, so the bare key
//! modulo 64 is the process counter's — a loop minting a few ids per
//! collection would use a fraction of the shards.
//!
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md

use crate::{LocalId, U48};

/// Shards a database is created with unless told otherwise (RFC 0067).
pub const DEFAULT_SHARDS: usize = 64;

/// What an operation belongs to.
///
/// **Placement is a property of the operation, not of each id it writes.** A
/// Unique save is the clearest proof: one atomic batch writes the superseded
/// version at an archive slot (`KEY` = its instant) and the live record at
/// the anchor (`KEY` = the `STRUCT_HASH`) — two ids with nothing in common,
/// which `hash(id)` would split across two shards with no owner to be atomic
/// in. A collection is the same: its B+tree nodes and chain segments carry
/// ids of their own, and they belong to the Pivot because the operation does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Owner {
    /// A NonUnique collection, named by its Pivot.
    Collection(LocalId),
    /// A Unique type under a tenant. It has no Pivot — the anchor is
    /// `KEY = STRUCT_HASH` under that tenant — so the pair is the identity,
    /// and the live record and its whole version history share its shard.
    Unique { tenant: U48, struct_hash: u64 },
}

impl Owner {
    /// The shard this owner lives in, out of `shards`. `shards` of 0 or 1 is
    /// one shard.
    #[must_use]
    pub fn shard(self, shards: usize) -> usize {
        let Ok(n) = u64::try_from(shards) else {
            return 0;
        };
        if n <= 1 {
            return 0;
        }
        // The remainder is `< shards`, so it fits the type `shards` came
        // from; the fallback is unreachable and says 0 rather than panic.
        usize::try_from(self.placement_hash() % n).unwrap_or(0)
    }

    /// SeaHash over the owner's frozen bytes.
    #[must_use]
    pub fn placement_hash(self) -> u64 {
        let mut bytes = [0u8; 14];
        let len = match self {
            // The Pivot's `LocalId`: `key` (8 LE) then `FLAG|SALT` (2 LE).
            Self::Collection(pivot) => {
                let lower = (u16::from(pivot.flag()) << 15) | pivot.salt();
                bytes[..8].copy_from_slice(&pivot.key().to_le_bytes());
                bytes[8..10].copy_from_slice(&lower.to_le_bytes());
                10
            }
            // The tenant (6 LE) then the **full** `STRUCT_HASH` (8 LE).
            Self::Unique {
                tenant,
                struct_hash,
            } => {
                bytes[..6].copy_from_slice(&tenant.get().to_le_bytes()[..6]);
                bytes[6..14].copy_from_slice(&struct_hash.to_le_bytes());
                14
            }
        };
        seahash::hash(&bytes[..len])
    }
}

/// The shard owning the collection `pivot`, out of `shards`.
#[must_use]
pub fn shard_of(pivot: LocalId, shards: usize) -> usize {
    Owner::Collection(pivot).shard(shards)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_SHARDS, Owner, shard_of};
    use crate::{LocalId, U48};

    fn pivot(key: u64) -> LocalId {
        LocalId::new(key, false, 0x1234)
    }

    fn unique(tenant: u32, struct_hash: u64) -> Owner {
        Owner::Unique {
            tenant: U48::from(tenant),
            struct_hash,
        }
    }

    /// The placement is an on-disk address: these numbers may never change.
    /// A failure here means records already written sit in other shards.
    #[test]
    fn golden_vectors_are_frozen() {
        let collection = Owner::Collection(pivot(1_787_000_000_000_000_123));
        let user = unique(7, 0x1122_3344_5566_7788);
        assert_eq!(collection.placement_hash(), GOLDEN_COLLECTION);
        assert_eq!(user.placement_hash(), GOLDEN_UNIQUE);
        assert_eq!(collection.shard(DEFAULT_SHARDS), GOLDEN_COLLECTION_64);
        assert_eq!(user.shard(DEFAULT_SHARDS), GOLDEN_UNIQUE_64);
    }

    const GOLDEN_COLLECTION: u64 = 0xf006_9e73_20f5_e727;
    const GOLDEN_UNIQUE: u64 = 0x9def_d228_0940_9671;
    const GOLDEN_COLLECTION_64: usize = 39;
    const GOLDEN_UNIQUE_64: usize = 49;

    /// The bytes are the layout RFC 0067 names, spelled out independently of
    /// the codec: a change to `LocalId`'s or `U48`'s wire form cannot move a
    /// shard.
    #[test]
    fn the_hashed_bytes_are_the_documented_layout() {
        let p = LocalId::new(0x0102_0304_0506_0708, true, 0x0A0B);
        let mut expect = 0x0102_0304_0506_0708u64.to_le_bytes().to_vec();
        expect.extend_from_slice(&(0x8000u16 | 0x0A0B).to_le_bytes());
        assert_eq!(
            Owner::Collection(p).placement_hash(),
            seahash::hash(&expect)
        );

        let mut expect = 0x0000_0605_0403_0201u64.to_le_bytes()[..6].to_vec();
        expect.extend_from_slice(&0xFFEE_DDCC_BBAA_9988u64.to_le_bytes());
        let owner = Owner::Unique {
            tenant: U48::from_truncated(0x0605_0403_0201),
            struct_hash: 0xFFEE_DDCC_BBAA_9988,
        };
        assert_eq!(owner.placement_hash(), seahash::hash(&expect));
    }

    #[test]
    fn an_owner_always_lands_on_one_shard_in_range() {
        for shards in [1, 2, 7, 8, 64] {
            for n in 0..200u64 {
                let s = shard_of(pivot(n), shards);
                assert!(s < shards);
                assert_eq!(s, shard_of(pivot(n), shards));
            }
        }
        assert_eq!(shard_of(pivot(5), 0), 0, "zero must not divide");
    }

    /// Both components of a Unique owner matter: a route that ignored either
    /// would put every tenant's `User` on one shard, or every type of one
    /// tenant there. The whole `STRUCT_HASH` counts, not its low 15 bits.
    #[test]
    fn tenant_and_full_type_hash_both_move_the_shard() {
        fn distinct(it: impl Iterator<Item = usize>) -> usize {
            it.collect::<std::collections::HashSet<_>>().len()
        }
        assert!(distinct((0..64u32).map(|t| unique(t, 0xAAAA).shard(64))) > 30);
        // Types that share their low 15 bits still spread.
        let shared_salt = (0..64u64).map(|h| unique(7, (h << 40) | 0x1234));
        assert!(distinct(shared_salt.map(|o| o.shard(64))) > 30);
    }

    /// The counter pattern RFC 0067 names: Pivots minted `k` ids apart within
    /// one millisecond hit every shard, not `64 / gcd(k, 64)` of them.
    #[test]
    fn pivots_minted_in_a_burst_use_every_shard() {
        let mut counts = vec![0usize; DEFAULT_SHARDS];
        for n in 0..64_000u64 {
            // `key_nanos()`: ms · 1e6 + a counter that moves 4 per collection.
            let key = 1_787_000_000_000_000_000
                + (n / 250) * 1_000_000
                + 4 * (n % 250);
            counts[shard_of(pivot(key), DEFAULT_SHARDS)] += 1;
        }
        let (lo, hi) =
            (*counts.iter().min().unwrap(), *counts.iter().max().unwrap());
        // 1000 each on average; ±20% catches a hash that does not mix.
        assert!(lo > 800 && hi < 1200, "{counts:?}");
    }

    #[test]
    fn unique_owners_spread_over_the_shards() {
        let mut counts = vec![0usize; 8];
        for t in 0..8_000u32 {
            counts[unique(t, 0x1234_5678_9ABC_DEF0).shard(8)] += 1;
        }
        let (lo, hi) =
            (*counts.iter().min().unwrap(), *counts.iter().max().unwrap());
        assert!(lo > 800 && hi < 1200, "{counts:?}");
    }
}
