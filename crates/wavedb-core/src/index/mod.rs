//! The `Store`-generic index layer.
//!
//! Order-preserving [`IndexKey`] encoding, the [`Bound`] search range, the
//! [`Pivot`] a collection is named by, the [`Chain`] every index is — a key
//! range of an ordered lane (RFC 0078 §7) — and [`IdStreamExt`] set algebra
//! over `Id` streams.
//!
//! Everything here depends only on [`Store`](crate::Store), so the same code
//! compiles for the node (`PageStore` in `wavedb-storage`) and the browser
//! (IndexedDB). Pages, blocks, and the settle are backend internals and are
//! never named here.

mod chain;
mod chain_scan;
mod key;
mod lane;
#[cfg(test)]
pub(crate) mod mem_store;
mod sec_key;
mod stream;

pub use chain::{Chain, PREFIX};
pub use chain_scan::BATCH;
pub use key::IndexKey;
pub use lane::Lane;
pub use sec_key::SecKey;
pub use stream::{Except, IdStreamExt, Intersect, Union};

use crate::permission::PermissionRef;
use crate::wire::WaveWire;

// ---- Bound: a search range over the encoded key space -----------------------

/// A search bound over the order-preserving key space.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bound {
    /// Every key in the tree.
    All,
    /// Keys byte-equal to this encoding.
    Exact(Vec<u8>),
    /// Half-open `[lo, hi)`.
    Range { lo: Vec<u8>, hi: Vec<u8> },
    /// Keys that start with this byte prefix.
    Prefix(Vec<u8>),
}

impl Bound {
    /// Does an encoded key fall within this bound? (`memcmp` semantics.)
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::All => true,
            Self::Exact(k) => key == k.as_slice(),
            Self::Range { lo, hi } => {
                key >= lo.as_slice() && key < hi.as_slice()
            }
            Self::Prefix(p) => key.starts_with(p),
        }
    }

    /// The least encoded key this bound can match — where an ordered scan
    /// of it starts (RFC 0075). Every key it matches is `≥` this, so a scan
    /// from here taking while [`contains`](Self::contains) holds is exactly
    /// the bound.
    #[must_use]
    pub fn least(&self) -> &[u8] {
        match self {
            Self::All => &[],
            Self::Exact(k) | Self::Prefix(k) => k,
            Self::Range { lo, .. } => lo,
        }
    }
}

// ---- Pivot: what names a collection ----------------------------------------

/// What names a collection.
///
/// `#[wavedb]` generates one per NonUnique type; this trait is the portable
/// shape the engine reads. Its id is the collection's identity — every chain
/// of the collection is the key range under it (RFC 0078 §7), so the record
/// holds no roots and no chain write ever rewrites it. What it holds is the
/// collection's default access rule.
pub trait Pivot: WaveWire + Sized + Default {
    /// Identity stamped at the head of the stored pivot record (`[STRUCT_HASH]
    /// [wire]`), routing all pivots of one collection type into one storage
    /// directory. The macro derives it from the generated pivot's own shape.
    const STRUCT_HASH: u64;

    /// One chain per `#[wavedb::pivot(...)]` secondary index, in declaration
    /// order ([RFC 0075]) — `[field][anchor]` keys and no payload, the
    /// recency chain's shape keyed by a field instead of an instant. Its
    /// roots are permanent, so a secondary write never rewrites the `Pivot`.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md
    fn secondaries(&self) -> &[ChainRoots];
    /// The **record chain**'s roots: the collection's living records stored
    /// inline in modification order, with a sparse index above them (RFC 0050).
    ///
    /// This *is* the membership set and the modification log — it replaced the
    /// `current` and `recency` B+trees the collection used to carry. Liveness
    /// is read off each record's own `Metadata`, not off a tree.
    fn recency(&self) -> ChainRoots;
    /// The **removal log**'s endpoints — the same segment chain shape with no
    /// index, since nothing ever searches it. It replaced the `dead` B+tree.
    fn removals(&self) -> LogRoots;
    /// One **declared list**'s roots per `#[wavedb::list(...)]`, in declaration
    /// order ([RFC 0051]) — a second chain of the same records, kept sorted by
    /// the declared property instead of by modification instant.
    ///
    /// Empty for a collection that declares none, which is the default: the
    /// built-in chain is the list every collection gets.
    ///
    /// [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
    fn lists(&self) -> &[ChainRoots] {
        &[]
    }
    /// One **fuzzy index**'s chain per `#[wavedb::fuzzy]` declaration, in
    /// declaration order ([RFC 0056]) — an n-gram posting `Chain<()>` in
    /// its own lane (RFC 0075).
    ///
    /// A posting chain, unlike a list, holds no record bytes: a gram, a length
    /// and an anchor. That is what lets a save whose indexed field did not
    /// change write nothing here at all.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    fn fuzzy(&self) -> &[ChainRoots] {
        &[]
    }
    /// Collection-default access rule: seeds new inserts and gates
    /// collection-scope ops (`Insert`, `All`). Each record's
    /// `Metadata.permission` overrides it (authoritative per record).
    /// `None` = tenant-only.
    fn permission(&self) -> Option<&PermissionRef>;
    /// A copy of this pivot with every root replaced and everything else
    /// (permission) preserved — what the engine writes back when a root moved.
    ///
    /// In practice that is rare: a chain's endpoints move at most once in its
    /// life (its first split) and its index root never moves.
    /// `roots.secondaries` and `roots.lists` must hold exactly as many entries
    /// as [`secondaries`](Self::secondaries) and [`lists`](Self::lists) return.
    #[must_use]
    fn replace_roots(&self, roots: Roots<'_>) -> Self;
}

#[cfg(test)]
mod tests {
    use super::Bound;

    #[test]
    fn bound_contains() {
        assert!(Bound::All.contains(&[1, 2, 3]));
        assert!(Bound::Exact(vec![1, 2]).contains(&[1, 2]));
        assert!(!Bound::Exact(vec![1, 2]).contains(&[1, 3]));
        let r = Bound::Range {
            lo: vec![1],
            hi: vec![3],
        };
        assert!(r.contains(&[1]));
        assert!(r.contains(&[2]));
        assert!(!r.contains(&[3])); // half-open
        assert!(Bound::Prefix(vec![0xAB]).contains(&[0xAB, 0xCD]));
        assert!(!Bound::Prefix(vec![0xAB]).contains(&[0xAC]));
    }
}
