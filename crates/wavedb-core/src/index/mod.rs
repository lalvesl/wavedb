//! The `Store`-generic index layer.
//!
//! Order-preserving [`IndexKey`] encoding, the [`Bound`] search range, the
//! [`Pivot`] roots holder, the concrete [`BpTree`], and [`IdStreamExt`] set
//! algebra over `Id` streams.
//!
//! Everything here depends only on [`Store`](crate::Store) (`get` + `apply`), so the same code
//! compiles for the node (`PageStore` in `wavedb-storage`) and the browser
//! (IndexedDB). Pages, blocks, and the journal are backend internals and are
//! never named here.

mod chain;
mod chain_insert;
mod chain_io;
mod chain_remove;
mod chain_scan;
mod key;
#[cfg(test)]
pub(crate) mod mem_store;
mod node;
mod node_key;
mod segment;
mod sparse;
mod sparse_tree;
mod sparse_write;
mod stream;
mod tree;
mod tree_delete;
mod tree_insert;

pub use chain::{Chain, DEFAULT_DEAD_MIN, DEFAULT_SEGMENT_MIN};
pub use key::IndexKey;
pub use node::BPTREE_NODE_STRUCT_HASH;
pub use node_key::{NodeKey, SecKey};
pub use segment::{Lane, Segment, mint_lane_id};
pub use sparse::{Branch, Slot, SparseNode, Step};
pub use sparse_tree::{DEFAULT_SPARSE_CAP, SparseTree};
pub use stream::{Except, IdStreamExt, Intersect, Union};
pub use tree::{BpTree, DEFAULT_INTERNAL_CAP, DEFAULT_LEAF_CAP};

use crate::local_id::LocalId;
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

    /// The inclusive `CREATED_AT` (`u64`) range this bound can match, when it is
    /// expressible — the tree's descent pruning. `None` = no pruning possible
    /// ([`Bound::All`], or a key that isn't an 8-byte big-endian `CREATED_AT`).
    /// A returned `(lo, hi)` with `lo > hi` matches nothing.
    #[must_use]
    pub(crate) fn created_at_range(&self) -> Option<(u64, u64)> {
        let as_u64 = |b: &[u8]| -> Option<u64> {
            Some(u64::from_be_bytes(b.try_into().ok()?))
        };
        match self {
            Self::All => None,
            Self::Exact(k) => {
                let k = as_u64(k)?;
                Some((k, k))
            }
            Self::Range { lo, hi } => {
                let (lo, hi) = (as_u64(lo)?, as_u64(hi)?);
                // Half-open [lo, hi) → inclusive. hi == 0 matches nothing;
                // wrapping would turn it into the full range, so signal the
                // empty range explicitly with lo > hi.
                if hi == 0 {
                    return Some((1, 0));
                }
                Some((lo, hi - 1))
            }
            Self::Prefix(p) => {
                if p.len() > 8 {
                    return None;
                }
                let mut lo = [0x00u8; 8];
                let mut hi = [0xFFu8; 8];
                lo[..p.len()].copy_from_slice(p);
                hi[..p.len()].copy_from_slice(p);
                Some((u64::from_be_bytes(lo), u64::from_be_bytes(hi)))
            }
        }
    }
}

// ---- Chain roots ------------------------------------------------------------

/// The ids naming one **indexed** chain: both endpoints, and its sparse index's
/// root.
///
/// All three are **permanent** (RFC 0050): a split hands its new id to the
/// interior side so an endpoint keeps its own, and an index root that overflows
/// keeps its id by moving its contents into a fresh child. So a `Pivot` holding
/// these is written once at creation and then essentially never — the opposite
/// of a `BpTree` root, which moves on every root split and rewrites the `Pivot`
/// with it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, WaveWire)]
pub struct ChainRoots {
    /// The segment holding the least keys.
    pub head: LocalId,
    /// The segment holding the greatest keys — the growth end.
    pub tail: LocalId,
    /// Root of the sparse index above the chain.
    pub index: LocalId,
}

/// The ids naming one **index-less** chain — the removal log, which nothing
/// searches, so there is no index root to name (RFC 0050).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, WaveWire)]
pub struct LogRoots {
    /// The segment holding the earliest removals.
    pub head: LocalId,
    /// The segment holding the most recent — where an append lands and a
    /// catch-up starts.
    pub tail: LocalId,
}

/// Every root a collection holds, gathered so [`Pivot::replace_roots`] takes
/// one argument instead of a growing positional list.
///
/// A struct rather than parameters because the members are three slices and two
/// `LocalId`-shaped records: positionally, `secondaries` and `lists` are one
/// transposition away from silently swapping. Every `Pivot` implementation is
/// generated or hand-written to mirror the macro, so a field added here is a
/// compile error everywhere it matters and nowhere it doesn't.
pub struct Roots<'a> {
    /// One root per `#[wavedb::pivot(...)]` secondary index.
    pub secondaries: &'a [LocalId],
    /// The built-in modification-ordered record chain.
    pub recency: ChainRoots,
    /// The removal log.
    pub removals: LogRoots,
    /// One per `#[wavedb::list(...)]` declaration, declaration order
    /// ([RFC 0051]).
    ///
    /// [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
    pub lists: &'a [ChainRoots],
    /// One root per `#[wavedb::fuzzy]` declaration, declaration order
    /// ([RFC 0056]) — an n-gram posting tree.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    pub fuzzy: &'a [LocalId],
}

// ---- Pivot: the collection's roots holder -----------------------------------

/// The collection's roots holder.
///
/// `#[wavedb]` generates one per NonUnique type; this trait is the portable shape
/// the engine reads. Root pointers are [`LocalId`] (tenant-scoped tree ⇒ `TENANT`
/// derivable). No element counter — the `Pivot` is rewritten only when a `BpTree`
/// root moves or its default permission changes (a rare admin op).
pub trait Pivot: WaveWire + Sized {
    /// Identity stamped at the head of the stored pivot record (`[STRUCT_HASH]
    /// [wire]`), routing all pivots of one collection type into one storage
    /// directory. The macro derives it from the generated pivot's own shape.
    const STRUCT_HASH: u64;

    /// One root per `#[wavedb::pivot(...)]` secondary index — the only
    /// B+trees a collection still holds (RFC 0050 phase 5c retired the rest).
    fn secondaries(&self) -> &[LocalId];
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
    /// One **fuzzy index**'s root per `#[wavedb::fuzzy]` declaration, in
    /// declaration order ([RFC 0056]) — an n-gram posting `BpTree<SecKey>`.
    ///
    /// A posting tree, unlike a list, holds no record bytes: a gram, a length
    /// and an anchor. That is what lets a save whose indexed field did not
    /// change write nothing here at all.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    fn fuzzy(&self) -> &[LocalId] {
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
    /// In practice that is now rare: a chain's endpoints move at most once in
    /// its life (its first split) and its index root never moves, so only a
    /// secondary tree's root split still triggers a `Pivot` rewrite.
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

    #[test]
    fn created_at_range_matches_contains_semantics() {
        let key = |v: u64| v.to_be_bytes().to_vec();

        assert_eq!(Bound::All.created_at_range(), None);
        assert_eq!(Bound::Exact(key(9)).created_at_range(), Some((9, 9)));
        // Non-8-byte keys carry no CREATED_AT meaning — no pruning.
        assert_eq!(Bound::Exact(vec![1, 2]).created_at_range(), None);

        let r = Bound::Range {
            lo: key(10),
            hi: key(20),
        };
        assert_eq!(r.created_at_range(), Some((10, 19))); // half-open → inclusive

        // hi == 0 matches nothing: lo > hi signals the empty range.
        let empty = Bound::Range {
            lo: key(0),
            hi: key(0),
        };
        let (lo, hi) = empty.created_at_range().unwrap();
        assert!(lo > hi);

        // A prefix covers the padded [p00.., pFF..] block.
        let p = Bound::Prefix(vec![0xAB]);
        assert_eq!(
            p.created_at_range(),
            Some((0xAB00_0000_0000_0000, 0xABFF_FFFF_FFFF_FFFF))
        );
    }
}
