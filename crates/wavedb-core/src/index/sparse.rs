//! The sparse index node ([RFC 0052]) — one entry per **chain segment**, not per
//! record, carrying that segment's element count.
//!
//! It was built beside the dense B+tree rather than as a count bolted onto
//! it — a count is mutable payload, and every child here carries its own least
//! key, which is what lets every child carry a count too — and outlived it:
//! since RFC 0075 every index is a chain, and this is the only tree left.
//!
//! ## Value format
//!
//! ```text
//! [ Lane::Index hash (8 B LE) ][ kind (u8) ][ WaveWire payload ]
//! ```
//!
//! Same discipline as a chain segment: the leading 8 bytes route the value in the
//! storage backend, the kind byte picks the variant, the payload is plain
//! `WaveWire`. Index nodes take their own lane because they are *navigational*
//! while segments are *streaming* ([RFC 0053]).
//!
//! [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md
//! [RFC 0053]: https://github.com/wavedb/wavedb/blob/main/rfcs/0053-tenant-fair-cache-retention-PLANNED.md

use crate::error::{Error, Result};
use crate::local_id::LocalId;
use crate::wire::{WaveWire, from_wire, to_wire};

use super::sec_key::SecKey;

/// Bytes before the kind byte: the lane's `STRUCT_HASH` tag.
const LANE_PREFIX: usize = 8;

/// `kind` byte values.
const KIND_LEAF: u8 = 0;
const KIND_INTERNAL: u8 = 1;

/// A leaf entry: one chain segment, filed under the least key it holds.
///
/// The same shape as [`Branch`] one level up — least key, pointer, count — which
/// is what lets one descent routine serve both levels.
///
/// > **Corrected 2026-07-30 (phase 3).** Phase 2 stored only a `SecKey` here and
/// > reused its trailing `rec` as the segment id, saving ten bytes an entry. That
/// > is wrong: `SecKey` orders by `field` **then** `rec`, so when a search key and
/// > a separator share a `field` the comparison falls through to the trailing
/// > pointer — and a segment id has no relationship to a record anchor, making the
/// > descent land on the wrong side of the boundary about half the time. It breaks
/// > wherever equal fields are reachable: an exact hit on a segment's own least
/// > instant in the built-in chain, and *routinely* in a declared list over a
/// > low-cardinality column, where a whole run of segments shares one value. The
/// > separator must be the segment's least **record** key, verbatim, with the
/// > segment id carried beside it.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct Slot {
    /// The least key the segment holds, exactly as the segment holds it.
    pub first: SecKey,
    /// The segment this entry names.
    pub seg: LocalId,
    /// How many entries the segment holds.
    pub count: u64,
}

/// An internal entry: one child subtree, filed under the least key anywhere in
/// it, with the subtree's total element count.
///
/// Every child carries its own least key — no `leftmost`-plus-separators split —
/// so a count belongs to every child, and an offset descent needs no arithmetic
/// the node does not hold.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct Branch {
    /// The least key anywhere in this subtree.
    pub first: SecKey,
    /// The child node.
    pub node: LocalId,
    /// Total elements under this child, summed over the segments it covers.
    pub count: u64,
}

/// A sparse-index node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparseNode {
    /// Segments, ascending by their least key.
    Leaf(Vec<Slot>),
    /// Child subtrees, ascending by their least key.
    Internal(Vec<Branch>),
}

/// Where a descent by key or by offset lands one level down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// A segment, and — for an offset descent — how far into it to start.
    Segment {
        /// The segment this level resolved to.
        slot: Slot,
        /// Elements to skip inside the segment (`0` for a key descent).
        offset: u64,
    },
    /// A child node to descend into, with the offset rebased on it.
    Child {
        /// The child node.
        node: LocalId,
        /// The offset relative to that child's own first element.
        offset: u64,
    },
}

impl SparseNode {
    /// Total elements this node covers.
    #[must_use]
    pub fn total(&self) -> u64 {
        match self {
            Self::Leaf(slots) => slots.iter().map(|s| s.count).sum(),
            Self::Internal(branches) => branches.iter().map(|b| b.count).sum(),
        }
    }

    /// How many entries the node itself holds — what its split policy bounds.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Leaf(slots) => slots.len(),
            Self::Internal(branches) => branches.len(),
        }
    }

    /// `true` when this node covers nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The least key this node covers — the separator its parent files it under.
    ///
    /// `None` for an empty node, which is also the signal that a parent should
    /// drop the entry naming it rather than file it under no key at all.
    #[must_use]
    pub fn first_key(&self) -> Option<&SecKey> {
        match self {
            Self::Leaf(slots) => slots.first().map(|s| &s.first),
            Self::Internal(branches) => branches.first().map(|b| &b.first),
        }
    }

    /// Split in half, keeping the lower half here and returning the upper half
    /// with its least key — what a parent needs to file the new sibling under.
    ///
    /// `None` when there is nothing to hand up (fewer than two entries), in
    /// which case this node is left untouched.
    pub fn split_off_half(&mut self) -> Option<(SecKey, Self)> {
        // Ceiling, so the half that keeps this node's id is never the empty one.
        let at = self.len().div_ceil(2);
        let upper = match self {
            Self::Leaf(slots) => Self::Leaf(slots.split_off(at)),
            Self::Internal(branches) => Self::Internal(branches.split_off(at)),
        };
        let first = upper.first_key()?.clone();
        Some((first, upper))
    }

    /// One level of a descent **by key**: the last entry whose own least key is
    /// `<= key`, since that is the one covering it.
    ///
    /// `None` when `key` sorts below every entry — the node covers nothing at or
    /// below it.
    #[must_use]
    pub fn step_to_key(&self, key: &SecKey) -> Option<Step> {
        match self {
            Self::Leaf(slots) => {
                let i = Self::last_at_or_below(
                    slots.len(),
                    |i| &slots[i].first,
                    key,
                )?;
                Some(Step::Segment {
                    slot: slots[i].clone(),
                    offset: 0,
                })
            }
            Self::Internal(branches) => {
                let i = Self::last_at_or_below(
                    branches.len(),
                    |i| &branches[i].first,
                    key,
                )?;
                Some(Step::Child {
                    node: branches[i].node,
                    offset: 0,
                })
            }
        }
    }

    /// One level of a descent **by offset**: the entry whose cumulative range
    /// contains `offset`, with the offset rebased on it.
    ///
    /// This is what makes "jump to page k" a descent instead of a walk of k
    /// segments. `None` when `offset` is at or past the node's total.
    #[must_use]
    pub fn step_to_offset(&self, offset: u64) -> Option<Step> {
        match self {
            Self::Leaf(slots) => {
                let (i, within) =
                    Self::containing(offset, slots.len(), |i| slots[i].count)?;
                Some(Step::Segment {
                    slot: slots[i].clone(),
                    offset: within,
                })
            }
            Self::Internal(branches) => {
                let (i, within) =
                    Self::containing(offset, branches.len(), |i| {
                        branches[i].count
                    })?;
                Some(Step::Child {
                    node: branches[i].node,
                    offset: within,
                })
            }
        }
    }

    /// The last index whose key is `<= key`, over an ascending sequence.
    fn last_at_or_below<'k>(
        len: usize,
        key_at: impl Fn(usize) -> &'k SecKey,
        key: &SecKey,
    ) -> Option<usize> {
        // Entries ascend, so the partition point is the count of entries at or
        // below `key`; one less is the last of them. Computed by hand rather than
        // with `slice::partition_point` because the keys are reached through an
        // accessor, not held in one slice.
        let (mut lo, mut hi) = (0usize, len);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if key_at(mid) <= key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo.checked_sub(1)
    }

    /// The index whose cumulative count range contains `offset`, and the
    /// remainder within it.
    fn containing(
        offset: u64,
        len: usize,
        count_at: impl Fn(usize) -> u64,
    ) -> Option<(usize, u64)> {
        let mut seen = 0u64;
        for i in 0..len {
            let count = count_at(i);
            // Empty entries are skipped rather than matched: an empty segment
            // holds no element at any offset.
            if offset < seen + count {
                return Some((i, offset - seen));
            }
            seen += count;
        }
        None
    }

    /// Serialise: the lane tag, the kind byte, then the `WaveWire` payload.
    #[must_use]
    pub fn to_bytes(&self, lane_hash: u64) -> Vec<u8> {
        let mut out = lane_hash.to_le_bytes().to_vec();
        match self {
            Self::Leaf(slots) => {
                out.push(KIND_LEAF);
                out.extend_from_slice(&to_wire(slots));
            }
            Self::Internal(branches) => {
                out.push(KIND_INTERNAL);
                out.extend_from_slice(&to_wire(branches));
            }
        }
        out
    }

    /// Parse an index node, checking the lane tag first.
    ///
    /// # Errors
    /// [`Error::LaneBadTag`] if the first 8 bytes are not `lane_hash`, the value
    /// is shorter than the tag plus kind, or the kind byte is unknown;
    /// [`Error::Wire`] if the payload fails to decode.
    pub fn from_bytes(lane_hash: u64, buf: &[u8]) -> Result<Self> {
        let tag_bytes: [u8; LANE_PREFIX] = buf
            .get(..LANE_PREFIX)
            .and_then(|s| s.try_into().ok())
            .ok_or(Error::LaneBadTag(0))?;
        let tag = u64::from_le_bytes(tag_bytes);
        if tag != lane_hash {
            return Err(Error::LaneBadTag(tag));
        }
        let kind = *buf.get(LANE_PREFIX).ok_or(Error::LaneBadTag(tag))?;
        let payload = &buf[LANE_PREFIX + 1..];
        match kind {
            KIND_LEAF => Ok(Self::Leaf(from_wire::<Vec<Slot>>(payload)?)),
            KIND_INTERNAL => {
                Ok(Self::Internal(from_wire::<Vec<Branch>>(payload)?))
            }
            _ => Err(Error::LaneBadTag(tag)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Branch, Slot, SparseNode, Step};
    use crate::error::Error;
    use crate::index::segment::Lane;
    use crate::local_id::LocalId;

    const TYPE_HASH: u64 = 0x1234_5678_9ABC_DEF0;

    fn lane() -> u64 {
        Lane::Index.hash(TYPE_HASH)
    }

    fn key(instant: u64) -> crate::index::SecKey {
        crate::index::SecKey {
            field: instant.to_be_bytes().to_vec(),
            rec: LocalId::new(instant, true, 3),
        }
    }

    /// A key whose `field` is `value` but whose record anchor is `rec` — what a
    /// low-cardinality ordering produces, where many records share one value.
    fn shared(value: u64, rec: u64) -> crate::index::SecKey {
        crate::index::SecKey {
            field: value.to_be_bytes().to_vec(),
            rec: LocalId::new(rec, false, 3),
        }
    }

    fn slot(first: crate::index::SecKey, seg: u64, count: u64) -> Slot {
        Slot {
            first,
            seg: LocalId::new(seg, true, 9),
            count,
        }
    }

    fn leaf(spec: &[(u64, u64)]) -> SparseNode {
        SparseNode::Leaf(
            spec.iter()
                .map(|&(k, count)| slot(key(k), k, count))
                .collect(),
        )
    }

    fn internal(spec: &[(u64, u64)]) -> SparseNode {
        SparseNode::Internal(
            spec.iter()
                .map(|&(k, count)| Branch {
                    first: key(k),
                    node: LocalId::new(k, true, 4),
                    count,
                })
                .collect(),
        )
    }

    #[test]
    fn a_leaf_roundtrips_through_its_lane() {
        let node = leaf(&[(10, 50), (20, 75)]);
        let bytes = node.to_bytes(lane());
        assert_eq!(SparseNode::from_bytes(lane(), &bytes).unwrap(), node);
    }

    #[test]
    fn an_internal_node_roundtrips_through_its_lane() {
        let node = internal(&[(10, 500), (60, 300)]);
        let bytes = node.to_bytes(lane());
        assert_eq!(SparseNode::from_bytes(lane(), &bytes).unwrap(), node);
    }

    #[test]
    fn a_foreign_lane_tag_is_refused() {
        let bytes = leaf(&[(1, 1)]).to_bytes(lane());
        let records = Lane::Records.hash(TYPE_HASH);
        assert!(matches!(
            SparseNode::from_bytes(records, &bytes),
            Err(Error::LaneBadTag(t)) if t == lane()
        ));
    }

    #[test]
    fn an_unknown_kind_byte_is_refused() {
        let mut bytes = leaf(&[(1, 1)]).to_bytes(lane());
        bytes[8] = 9;
        assert!(matches!(
            SparseNode::from_bytes(lane(), &bytes),
            Err(Error::LaneBadTag(_))
        ));
    }

    #[test]
    fn a_truncated_payload_is_a_wire_fault() {
        let bytes = leaf(&[(1, 1), (2, 2)]).to_bytes(lane());
        assert!(matches!(
            SparseNode::from_bytes(lane(), &bytes[..bytes.len() - 3]),
            Err(Error::Wire(_))
        ));
    }

    #[test]
    fn the_total_is_the_sum_of_the_counts() {
        // The pager's "of M" is this number at the root — one read, exact.
        assert_eq!(leaf(&[(10, 50), (20, 75)]).total(), 125);
        assert_eq!(internal(&[(10, 500), (60, 300)]).total(), 800);
        assert_eq!(leaf(&[]).total(), 0);
    }

    #[test]
    fn a_key_descent_takes_the_entry_covering_the_key() {
        let node = leaf(&[(10, 5), (20, 5), (30, 5)]);
        // A key inside the second segment's range resolves to that segment, not
        // to the one whose least key equals it.
        let Some(Step::Segment { slot, offset }) = node.step_to_key(&key(25))
        else {
            panic!("expected a segment")
        };
        assert_eq!(slot.first, key(20));
        assert_eq!(offset, 0, "a key descent never skips inside the segment");
    }

    #[test]
    fn a_key_descent_takes_an_exact_match_over_its_predecessor() {
        let node = leaf(&[(10, 5), (20, 5)]);
        let Some(Step::Segment { slot, .. }) = node.step_to_key(&key(20))
        else {
            panic!("expected a segment")
        };
        assert_eq!(slot.first, key(20));
    }

    #[test]
    fn a_run_of_segments_sharing_one_value_still_descends_exactly() {
        // A low-cardinality ordering: three segments, all holding value 7, split
        // by record anchor. The separator must be the segment's least *record*
        // key — reusing its trailing pointer as the segment id (phase 2's shape)
        // made this comparison fall through to an unrelated minted id.
        let node = SparseNode::Leaf(vec![
            slot(shared(7, 100), 900, 4),
            slot(shared(7, 200), 901, 4),
            slot(shared(7, 300), 902, 4),
        ]);
        for (probe, want) in [
            (150, 900u64),
            (200, 901),
            (250, 901),
            (300, 902),
            (999, 902),
        ] {
            let Some(Step::Segment { slot, .. }) =
                node.step_to_key(&shared(7, probe))
            else {
                panic!("expected a segment for {probe}")
            };
            assert_eq!(
                slot.seg,
                LocalId::new(want, true, 9),
                "anchor {probe} landed in the wrong segment"
            );
        }
        // Below the whole run, nothing covers it.
        assert_eq!(node.step_to_key(&shared(7, 1)), None);
    }

    #[test]
    fn a_key_below_everything_resolves_to_nothing() {
        // Nothing covers it — the caller inserts before the head rather than
        // guessing at the first segment.
        assert_eq!(leaf(&[(10, 5)]).step_to_key(&key(1)), None);
        assert_eq!(internal(&[(10, 5)]).step_to_key(&key(1)), None);
    }

    #[test]
    fn an_offset_descent_rebases_the_offset_on_the_entry_it_picks() {
        let node = leaf(&[(10, 50), (20, 75), (30, 10)]);

        // Offset 0 is the first element of the first segment.
        assert_eq!(
            node.step_to_offset(0),
            Some(Step::Segment {
                slot: slot(key(10), 10, 50),
                offset: 0
            })
        );
        // Offset 50 is the *boundary*: the first element of the second segment.
        assert_eq!(
            node.step_to_offset(50),
            Some(Step::Segment {
                slot: slot(key(20), 20, 75),
                offset: 0
            })
        );
        // Offset 60 is ten in.
        assert_eq!(
            node.step_to_offset(60),
            Some(Step::Segment {
                slot: slot(key(20), 20, 75),
                offset: 10
            })
        );
        // The last element.
        assert_eq!(
            node.step_to_offset(134),
            Some(Step::Segment {
                slot: slot(key(30), 30, 10),
                offset: 9
            })
        );
    }

    #[test]
    fn an_offset_past_the_total_resolves_to_nothing() {
        let node = leaf(&[(10, 50), (20, 75)]);
        assert_eq!(node.step_to_offset(125), None, "one past the last element");
        assert_eq!(node.step_to_offset(u64::MAX), None);
        assert_eq!(leaf(&[]).step_to_offset(0), None);
    }

    #[test]
    fn an_offset_descent_skips_empty_entries() {
        // An empty segment holds no element at any offset, so it must never be
        // the answer — a pager landing there would render a blank page.
        let node = leaf(&[(10, 0), (20, 3)]);
        let Some(Step::Segment { slot, offset }) = node.step_to_offset(0)
        else {
            panic!("expected a segment")
        };
        assert_eq!(slot.first, key(20));
        assert_eq!(offset, 0);
    }

    #[test]
    fn an_internal_offset_descent_rebases_onto_the_child() {
        let node = internal(&[(10, 500), (60, 300)]);
        assert_eq!(
            node.step_to_offset(500),
            Some(Step::Child {
                node: LocalId::new(60, true, 4),
                offset: 0
            })
        );
        assert_eq!(
            node.step_to_offset(700),
            Some(Step::Child {
                node: LocalId::new(60, true, 4),
                offset: 200
            })
        );
    }

    #[test]
    fn len_counts_entries_not_elements() {
        // The split policy bounds entries; the counts describe what they cover.
        let node = leaf(&[(10, 900), (20, 900)]);
        assert_eq!(node.len(), 2);
        assert_eq!(node.total(), 1800);
        assert!(!node.is_empty());
        assert!(leaf(&[]).is_empty());
    }
}
