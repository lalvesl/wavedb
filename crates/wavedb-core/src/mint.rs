//! Identity and instant minting: type salts, derived archive addresses,
//! and the monotone instant clock.
//!
//! Every time-keyed identity and version instant is minted here. Beyond
//! [`key_nanos`]'s process-unique fused counter, [`mint_instant`] enforces
//! two floors: the caller's **persisted watermark** (a collection's
//! recency/dead maxima — so a rewound wall clock can never author below an
//! instant a catch-up cursor may already have passed) and a process-wide
//! **last-minted watermark** (so two tasks reading one floor concurrently
//! can never mint the same instant).
//!
//! [`key_nanos`]: wavedb_platform::time::key_nanos

use core::sync::atomic::{AtomicU64, Ordering};

use crate::id::Id;
use crate::u48::U48;

/// The 15-bit type discriminator every record id of a type carries.
///
/// It keeps different types' ids apart in a **flat** keyspace (the browser's
/// IndexedDB store) that lacks the native engine's per-`STRUCT_HASH`
/// partitions, and it makes archive addresses derivable without storing them.
pub const fn type_salt(hash: u64) -> u16 {
    (hash & 0x7FFF) as u16
}

/// The derived address of the archived version authored at `instant`:
/// the instant as `KEY`, the type's salt, and the shape's anchor `FLAG`
/// **flipped** (see [`Shape::anchor_flag`]). Both writers of a chain link
/// — the predecessor's `Next` and the eventual archive `Put` — evaluate
/// this same function, so they agree by construction.
///
/// [`Shape::anchor_flag`]: crate::traits::Shape::anchor_flag
pub fn archive_id(
    hash: u64,
    shape: crate::traits::Shape,
    instant: u64,
    tenant: U48,
) -> Id {
    Id::new(instant, tenant, !shape.anchor_flag(), type_salt(hash))
}

/// Mint an instant strictly above `floor` and above every instant this
/// process already minted: `max(key_nanos(), floor + 1, last + 1)`.
pub fn mint_instant(floor: u64) -> u64 {
    static LAST: AtomicU64 = AtomicU64::new(0);
    let mut minted = 0;
    // The closure always returns `Some`, so the update cannot fail; on a
    // contended retry it re-derives from the fresher `last`.
    let _ = LAST.try_update(Ordering::Relaxed, Ordering::Relaxed, |last| {
        minted = wavedb_platform::time::key_nanos()
            .max(floor.saturating_add(1))
            .max(last.saturating_add(1));
        Some(minted)
    });
    minted
}

/// Mint a fresh time-keyed anchor id under `tenant` for the type `hash`,
/// its `KEY` an instant above `floor` (a collection's persisted watermark;
/// `0` when none applies — pivot records): `FLAG = 0` (the live namespace
/// of time-keyed shapes), `SALT` = the type's discriminator.
pub fn mint_floored_id(tenant: U48, hash: u64, floor: u64) -> Id {
    Id::new(mint_instant(floor), tenant, false, type_salt(hash))
}

/// SeaHash (the pinned identity hash, default keys) over a
/// `#[wavedb::key(...)]` record's key-field wire bytes.
///
/// Every process derives the same anchor from the same field values —
/// what makes a natural key an *address*. Generated `natural_key` impls
/// call this; nothing else should.
#[must_use]
pub fn natural_key_hash(bytes: &[u8]) -> u64 {
    seahash::hash(bytes)
}

/// The content-derived live anchor of a keyed record under `tenant`: the
/// natural key as `KEY`, `FLAG = 0` (keyed types are collection members —
/// the same live namespace as time-keyed anchors; archives take the
/// flipped bit as ever), `SALT` = the type's discriminator.
pub fn keyed_id(tenant: U48, hash: u64, key: u64) -> Id {
    Id::new(key, tenant, false, type_salt(hash))
}

#[cfg(test)]
mod tests {
    use super::mint_instant;

    #[test]
    fn minted_instants_are_strictly_increasing_and_respect_the_floor() {
        let a = mint_instant(0);
        let b = mint_instant(0);
        assert!(b > a, "process watermark must order back-to-back mints");

        // A floor far in the future (a mirror imposed a node instant from a
        // faster clock, or the wall clock rewound): minting must land above
        // it, and the next unfloored mint must not fall back below.
        let future = a + 500_000_000_000;
        let c = mint_instant(future);
        assert!(c > future, "floored mint must clear the floor");
        let d = mint_instant(0);
        assert!(d > c, "the watermark must hold after a floored mint");
    }
}
