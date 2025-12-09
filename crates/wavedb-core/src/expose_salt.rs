//! The registry's **15-bit salt guard** — split from [`crate::expose`] for the
//! file budget, along the seam the macro side already uses (`expose.rs` /
//! `expose_collision.rs`).
//!
//! [`type_salt`] is the low 15 bits of a `STRUCT_HASH`, and it is the
//! discriminator that keeps kinds of value apart where the full hash is not
//! available: the archive-slot address, and the browser's flat `Id → bytes`
//! keyspace. Two occupants sharing it lose that separation.
//!
//! The occupants are not just the declared record types. A NonUnique type
//! also reserves five ordered lanes — its lists, recency, the removal log,
//! its secondary and fuzzy chains (RFC 0078 §7) — each a `STRUCT_HASH` of its
//! own and so a salt of its own. A registry of `n` such types puts `6n`
//! values in a 32768-slot space, and one key space per kind of thing stored
//! rests on all of them being distinct. So the guard compares occupant sets,
//! not entries.
//!
//! [`type_salt`]: crate::mint::type_salt

use crate::mint::type_salt;

/// The 15-bit identity guard the `expose_*` macros instantiate — one call per
/// declared pair (and one per entry, against its own lanes), at compile time.
///
/// `DISTINCT` is the const-evaluated verdict: `true` when the compared
/// occupant sets share no [`type_salt`], `false` when they do. Only the
/// `false` arm is deprecated, so a clash costs the build a **warning naming
/// the entry**, while a clean registry is silent.
///
/// Sharing the salt is legal — the full 64-bit head still tells the types
/// apart on read (a full-`STRUCT_HASH` clash is the hard error, asserted
/// alongside this call). It is only a smell worth surfacing. Rename a field or
/// the type to reshuffle the hash, or keep it knowingly; a lane's hash is
/// derived from its type's, so renaming the type moves its lanes with it.
///
/// [`type_salt`]: crate::mint::type_salt
pub struct SaltGuard<const DISTINCT: bool>;

impl SaltGuard<true> {
    /// The clean arm — the salts differ, nothing to report.
    pub const fn check() {}
}

impl SaltGuard<false> {
    /// The clashing arm; its deprecation **is** the warning.
    #[deprecated(
        note = "this exposed type shares the low 15 bits of a STRUCT_HASH \
                (`type_salt`) with another entry in the same exposure list, \
                or with one of the reserved chain lanes a collection rides: \
                the two share archive slots and lose their separation in the \
                browser's flat keyspace. Rename the type or a field to \
                reshuffle the hash, or keep it knowingly."
    )]
    pub const fn check() {}
}

/// `true` when nothing in `left` shares a [`type_salt`] with anything in
/// `right`. Both sides are `STRUCT_HASH`es — a record type's or a reserved
/// lane's, which occupy the same 15-bit space.
///
/// [`type_salt`]: crate::mint::type_salt
const fn apart(left: &[u64], right: &[u64]) -> bool {
    let mut i = 0;
    while i < left.len() {
        let mut j = 0;
        while j < right.len() {
            if type_salt(left[i]) == type_salt(right[j]) {
                return false;
            }
            j += 1;
        }
        i += 1;
    }
    true
}

/// `true` when two registry entries occupy no salt in common — the verdict
/// [`SaltGuard`] is instantiated with.
///
/// Each entry contributes its record type's salt **plus one per lane** its
/// collection rides ([`LANE_HASHES`]). Comparing only the record hashes would
/// leave three quarters of a NonUnique registry's occupants unchecked.
///
/// [`LANE_HASHES`]: crate::traits::WaveDbStruct::LANE_HASHES
#[must_use]
pub const fn salts_distinct(
    a: u64,
    a_lanes: &[u64],
    b: u64,
    b_lanes: &[u64],
) -> bool {
    apart(&[a], &[b])
        && apart(&[a], b_lanes)
        && apart(a_lanes, &[b])
        && apart(a_lanes, b_lanes)
}

/// [`salts_distinct`] for one entry against **itself**: its record salt
/// against each of its own lanes', and the lanes' against each other.
///
/// A type collides with its own lane without any second entry involved, so
/// this holds even for a one-item registry.
#[must_use]
pub const fn salts_self_distinct(hash: u64, lanes: &[u64]) -> bool {
    if !apart(&[hash], lanes) {
        return false;
    }
    let mut i = 1;
    while i < lanes.len() {
        if !apart(&[lanes[i]], lanes.split_at(i).0) {
            return false;
        }
        i += 1;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::{salts_distinct, salts_self_distinct};

    /// Two hashes agreeing on their low 15 bits and nothing else — the exact
    /// shape of a `type_salt` clash between values that are otherwise
    /// unrelated (which is what a lane hash is to its type's).
    const fn twin(base: u64, salt: u64) -> u64 {
        (base << 15) | (salt & 0x7FFF)
    }

    #[test]
    fn the_salt_check_sees_lanes_and_not_only_record_hashes() {
        let (a, b) = (twin(1, 100), twin(2, 200));
        assert!(
            salts_distinct(a, &[twin(3, 300)], b, &[twin(4, 400)]),
            "four distinct salts must pass"
        );
        // Each of the three cross-comparisons the old check could not make.
        assert!(
            !salts_distinct(a, &[], b, &[twin(9, 100)]),
            "a record salt clashing with the other entry's lane"
        );
        assert!(
            !salts_distinct(a, &[twin(9, 200)], b, &[]),
            "a lane clashing with the other entry's record salt"
        );
        assert!(
            !salts_distinct(a, &[twin(7, 500)], b, &[twin(8, 500)]),
            "two lanes of different entries clashing"
        );
        // And the one it could: the record hashes themselves.
        assert!(!salts_distinct(a, &[], twin(9, 100), &[]));
    }

    #[test]
    fn a_type_is_checked_against_its_own_lanes() {
        let h = twin(1, 100);
        assert!(salts_self_distinct(
            h,
            &[twin(2, 1), twin(3, 2), twin(4, 3)]
        ));
        assert!(
            !salts_self_distinct(h, &[twin(2, 1), twin(3, 100)]),
            "a lane clashing with its own type's salt"
        );
        assert!(
            !salts_self_distinct(h, &[twin(2, 7), twin(3, 7)]),
            "two lanes of one type clashing with each other"
        );
        assert!(
            salts_self_distinct(h, &[]),
            "a Unique type declares no lanes"
        );
    }
}
