//! `STRUCT_HASH` — a type's compile-time identity.
//!
//! The hash folds in the struct name, its shape, and every field's name and type,
//! so **any schema change yields a new hash** — a changed struct is simply a
//! different type. It is computed here, at macro-expansion time, and baked into the
//! generated code as a `u64` literal.
//!
//! ## Hashing
//!
//! `STRUCT_HASH` must be **bit-identical on every machine, architecture, and
//! build** so a client and a server always agree on a type's identity. It is
//! **SeaHash** (the `seahash` crate), portable across architecture and endianness
//! for a given seed, fast, and well diffused. The crate is **pinned to an exact
//! version** (`=` in `Cargo.toml`): the algorithm is identity-load-bearing, so an
//! unreviewed bump that changed it would silently change every `STRUCT_HASH` and
//! break schema identity.

use seahash::hash_seeded;

/// Fixed four-lane seed for `STRUCT_HASH`. Domain-separates this hash space from
/// every other SeaHash use (e.g. page routing) and is **load-bearing**: changing
/// any lane changes every type's identity.
const STRUCT_SEED: [u64; 4] = [
    0x5741_5645_4442_5f53, // "WAVEDB_S"
    0x5452_5543_545f_4841, // "TRUCT_HA"
    0x5348_5f76_3100_0000, // "SH_v1"
    0x9e37_79b9_7f4a_7c15, // golden-ratio mixing constant
];

/// Portable SeaHash over `bytes` under the fixed `STRUCT_SEED`.
const fn seahash(bytes: &[u8]) -> u64 {
    hash_seeded(
        bytes,
        STRUCT_SEED[0],
        STRUCT_SEED[1],
        STRUCT_SEED[2],
        STRUCT_SEED[3],
    )
}

/// Build the canonical identity string and hash it.
///
/// Canonical form: `name + '|' + shape + per field ('|' + field_name + ':' +
/// field_type)`. Field types are pre-normalised (whitespace stripped) by the
/// caller so the same declared type always renders identically.
#[must_use]
pub fn compute(name: &str, shape: &str, fields: &[(String, String)]) -> u64 {
    let mut canonical = String::with_capacity(name.len() + shape.len() + 16);
    canonical.push_str(name);
    canonical.push('|');
    canonical.push_str(shape);
    for (field_name, field_type) in fields {
        canonical.push('|');
        canonical.push_str(field_name);
        canonical.push(':');
        canonical.push_str(field_type);
    }
    seahash(canonical.as_bytes())
}

/// A reserved lane's `STRUCT_HASH` for the type hashing to `struct_hash` — the
/// **same derivation** `wavedb_core::index::Lane::hash` performs at runtime.
///
/// Computed here rather than called there because a `static StructStorage`
/// needs a `const` initialiser and SeaHash is not a `const fn`. Note the plain
/// `seahash::hash`: the lane derivation uses the crate's default seeds, not
/// this module's `STRUCT_SEED`, so it must not go through [`seahash`].
///
/// The two must agree exactly — see `lane_hashes_match_the_engines` in
/// `examples/schema-smoke`, which pins it against a real generated type.
pub fn lane_hash(tag: &[u8], struct_hash: u64) -> u64 {
    let mut bytes = tag.to_vec();
    bytes.extend_from_slice(&struct_hash.to_le_bytes());
    seahash::hash(&bytes)
}
#[cfg(test)]
mod tests {
    use super::compute;

    fn fields(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
            .collect()
    }

    #[test]
    fn deterministic() {
        let a = compute("AboutUser", "Unique", &fields(&[("name", "String")]));
        let b = compute("AboutUser", "Unique", &fields(&[("name", "String")]));
        assert_eq!(a, b);
    }

    #[test]
    fn shape_change_changes_hash() {
        let f = fields(&[("x", "u64")]);
        assert_ne!(compute("T", "Unique", &f), compute("T", "NonUnique", &f));
    }

    #[test]
    fn name_change_changes_hash() {
        let f = fields(&[("x", "u64")]);
        assert_ne!(compute("A", "Unique", &f), compute("B", "Unique", &f));
    }

    #[test]
    fn field_name_change_changes_hash() {
        assert_ne!(
            compute("T", "Unique", &fields(&[("a", "u64")])),
            compute("T", "Unique", &fields(&[("b", "u64")])),
        );
    }

    #[test]
    fn field_type_change_changes_hash() {
        assert_ne!(
            compute("T", "Unique", &fields(&[("a", "u64")])),
            compute("T", "Unique", &fields(&[("a", "u32")])),
        );
    }

    #[test]
    fn field_order_change_changes_hash() {
        assert_ne!(
            compute("T", "Unique", &fields(&[("a", "u64"), ("b", "u32")])),
            compute("T", "Unique", &fields(&[("b", "u32"), ("a", "u64")])),
        );
    }

    #[test]
    fn known_value_is_stable() {
        // Pin a concrete output so an accidental algorithm change is caught.
        let h = compute("AboutUser", "Unique", &fields(&[("name", "String")]));
        assert_eq!(h, super::seahash(b"AboutUser|Unique|name:String"));
    }
}
