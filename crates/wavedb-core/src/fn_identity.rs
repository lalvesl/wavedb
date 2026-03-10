//! A `#[server]` function's composed identity.
//!
//! [`compose`] folds the function's name hash with each argument's and the
//! return's **type tag**, so evolving any argument type's schema
//! transitively renames the function (a mixed-build caller fails the header
//! gate instead of mis-decoding).
//!
//! Two pieces:
//!
//! - [`FnArgTag`] — the `const` 64-bit tag of a type as a function
//!   argument/return. `#[wavedb]` structs tag as their `STRUCT_HASH`
//!   (schema evolution propagates); builtins carry fixed tags; containers
//!   compose their element's.
//! - [`compose`] — SeaHash over the little-endian words
//!   `[FN_DOMAIN, seed, parts…]`, computed in `const` context: the hash must
//!   be derivable from other crates' `STRUCT_HASH` consts, which is why the
//!   vendored `seahash` (`crates/seahash`) is `const fn`. One identity hash
//!   for types and functions alike; pre-release layout policy applies (a
//!   change renames every function, caught at the boundary, no migration).

use crate::id::Id;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::permission::PermissionRef;
use crate::u48::U48;

/// Domain separator so a composed fn hash can't collide with a plain
/// name-string hash by construction.
const FN_DOMAIN: u64 = 0xF7A5_11D3_0C4B_9E21;

/// Domain separator for container tags, so `container(k, e)` and
/// `compose(k, &[e])` never meet.
const CONTAINER_DOMAIN: u64 = 0xC017_A1E5_5D0C_7A9B;

/// SeaHash's published default seeds, so a fold equals `seahash::hash` of
/// its words' little-endian bytes (pinned by a test below).
const SEEDS: [u64; 4] = [
    0x16f1_1fe8_9b0d_677c,
    0xb480_a793_d8e6_c86c,
    0x6fe2_e5aa_f078_ebc9,
    0x14f9_94a4_c525_9381,
];

/// SeaHash over the words `[domain, seed, parts…]`, in `const` context.
const fn fold(domain: u64, seed: u64, parts: &[u64]) -> u64 {
    let mut state = seahash::State::new(SEEDS[0], SEEDS[1], SEEDS[2], SEEDS[3]);
    state.push(domain);
    state.push(seed);
    let mut i = 0;
    while i < parts.len() {
        state.push(parts[i]);
        i += 1;
    }
    state.finalize()
}

/// Fold `parts` (argument tags in order, then the return tag) into `seed`
/// (the function's name hash). Order-sensitive: swapping two argument types
/// yields a different identity.
#[must_use]
pub const fn compose(seed: u64, parts: &[u64]) -> u64 {
    fold(FN_DOMAIN, seed, parts)
}

/// Tag a container's element under a kind marker (`Vec<T>` ≠ `Option<T>` of
/// the same `T`).
#[must_use]
pub const fn container(kind: u64, element: u64) -> u64 {
    fold(CONTAINER_DOMAIN, kind, &[element])
}

/// The `const` identity of a type in a `#[server]` signature.
///
/// `#[wavedb]` structs get `TAG = STRUCT_HASH` (macro-implemented), so a
/// schema change to an argument renames every function using it. Builtins
/// carry fixed tags; containers compose. A type without an impl cannot be a
/// server-function argument or return — a compile error, not a wire risk.
pub trait FnArgTag {
    /// This type's 64-bit signature tag.
    const TAG: u64;
}

/// Fixed tags for the wire builtins (arbitrary distinct constants — their
/// exact values only need to be stable and unequal).
macro_rules! fixed_tag {
    ($($ty:ty => $tag:expr),+ $(,)?) => {
        $(impl FnArgTag for $ty { const TAG: u64 = $tag; })+
    };
}

fixed_tag! {
    ()     => 0xB111_0000_0000_0001,
    bool   => 0xB111_0000_0000_0002,
    char   => 0xB111_0000_0000_0003,
    u8     => 0xB111_0000_0000_0010,
    u16    => 0xB111_0000_0000_0011,
    u32    => 0xB111_0000_0000_0012,
    u64    => 0xB111_0000_0000_0013,
    u128   => 0xB111_0000_0000_0014,
    i8     => 0xB111_0000_0000_0020,
    i16    => 0xB111_0000_0000_0021,
    i32    => 0xB111_0000_0000_0022,
    i64    => 0xB111_0000_0000_0023,
    i128   => 0xB111_0000_0000_0024,
    f32    => 0xB111_0000_0000_0030,
    f64    => 0xB111_0000_0000_0031,
    String => 0xB111_0000_0000_0040,
}

// WaveDB's own wire values are valid signature types with fixed tags.
fixed_tag! {
    Id            => 0xB111_0000_0000_0100,
    LocalId       => 0xB111_0000_0000_0101,
    U48           => 0xB111_0000_0000_0102,
    Metadata      => 0xB111_0000_0000_0103,
    PermissionRef => 0xB111_0000_0000_0104,
}

/// Container kind markers.
const VEC_KIND: u64 = 0xC017_0000_0000_0001;
const OPTION_KIND: u64 = 0xC017_0000_0000_0002;
const ARRAY_KIND: u64 = 0xC017_0000_0000_0003;
/// A stream-returning fn's return kind (`impl Stream<Item = Result<T>>`
/// composes as `container(STREAM_KIND, T::TAG)` — `#[server]` emits it).
pub const STREAM_KIND: u64 = 0xC017_0000_0000_0004;

impl<T: FnArgTag> FnArgTag for Vec<T> {
    const TAG: u64 = container(VEC_KIND, T::TAG);
}

/// A [`Wire<T>`](crate::wire::Wire) is `T`'s bytes exactly, so it is `T`'s
/// signature: a reply of read records is the same contract as one of values.
impl<T: FnArgTag> FnArgTag for crate::wire::Wire<T> {
    const TAG: u64 = T::TAG;
}

impl<T: FnArgTag> FnArgTag for Option<T> {
    const TAG: u64 = container(OPTION_KIND, T::TAG);
}

impl<T: FnArgTag, const N: usize> FnArgTag for [T; N] {
    const TAG: u64 = compose(ARRAY_KIND, &[T::TAG, N as u64]);
}

/// Tuple tags compose their members in order under a length seed.
macro_rules! tuple_tag {
    ($len:expr => $($name:ident)+) => {
        impl<$($name: FnArgTag),+> FnArgTag for ($($name,)+) {
            const TAG: u64 =
                compose(0xD0_0F_0000_0000_0000 + $len, &[$($name::TAG),+]);
        }
    };
}

tuple_tag!(2 => A B);
tuple_tag!(3 => A B C);
tuple_tag!(4 => A B C D);

#[cfg(test)]
mod tests {
    use super::{FnArgTag, compose, container};

    #[test]
    fn compose_is_order_sensitive_and_deterministic() {
        let a = compose(1, &[10, 20]);
        assert_eq!(a, compose(1, &[10, 20]), "deterministic");
        assert_ne!(a, compose(1, &[20, 10]), "argument order matters");
        assert_ne!(a, compose(2, &[10, 20]), "the name seed matters");
        assert_ne!(a, compose(1, &[10]), "arity matters");
    }

    #[test]
    fn container_tags_separate_kinds_and_elements() {
        assert_ne!(<Vec<u64>>::TAG, <Option<u64>>::TAG);
        assert_ne!(<Vec<u64>>::TAG, <Vec<u32>>::TAG);
        assert_ne!(<Vec<Vec<u64>>>::TAG, <Vec<u64>>::TAG, "nesting folds in");
        assert_eq!(container(1, 2), container(1, 2));
    }

    /// A fold is SeaHash itself: the words' little-endian bytes, hashed.
    #[test]
    fn compose_is_seahash_over_its_words() {
        let words = [super::FN_DOMAIN, 7, 10, 20];
        let bytes: Vec<u8> =
            words.iter().flat_map(|w| w.to_le_bytes()).collect();
        assert_eq!(compose(7, &[10, 20]), seahash::hash(&bytes));
        assert_ne!(container(7, 10), compose(7, &[10]), "domains separate");
    }

    #[test]
    fn tuples_and_arrays_fold_members() {
        assert_ne!(<(u64, String)>::TAG, <(String, u64)>::TAG);
        assert_ne!(<[u8; 4]>::TAG, <[u8; 5]>::TAG, "length folds in");
        assert_ne!(<(u64, u64)>::TAG, <(u64, u64, u64)>::TAG);
    }
}
