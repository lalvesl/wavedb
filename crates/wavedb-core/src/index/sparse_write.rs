//! The sparse index's **write half** ([RFC 0052]) — [`SparseTree`], which keeps
//! one entry per chain segment, and every count above it, in step with the chain.
//!
//! [`sparse`](super::sparse) is the node and [`sparse_tree`](super::sparse_tree)
//! the handle and its reads. This is the edit — descend, edit the leaf, and climb
//! back rewriting each ancestor's separator and count. Both edits a chain performs
//! ([`plan_upsert`] and [`plan_remove`]) return a [`Write`] batch and commit
//! nothing: the chain folds them into the *same* atomic batch as the segment
//! writes they describe, which is what makes an index entry and its segment
//! unable to disagree.
//!
//! ## The root id never changes
//!
//! A `BpTree` root moves whenever the root splits, and the `Pivot` holding it must
//! be rewritten. Here a root that overflows keeps its own id: its contents move
//! into a **freshly minted child** and the root becomes the internal node above the
//! two halves. One extra node write on a level growth — a rare event — in exchange
//! for a `Pivot` that is written at creation and then essentially never, which is
//! the same permanence RFC 0050 gives a chain's head and tail. That is why every
//! method here takes `&self`: there is no root to hand back.
//!
//! ## What is deferred
//!
//! An emptied node is dropped from its parent and removed from the store, so
//! removals leak nothing. But this tree **never merges underfull nodes**, and a
//! root left with a single child is not collapsed: nodes drain and stay drained,
//! and a tree that grew a level never gives it back. The dense `BpTree` has the
//! full cycle (`tree_delete`); this half of RFC 0050 phase 3a was not written.
//!
//! It is **accepted debt**, not a design position (RFC 0050 phase 8a, written up as
//! RFC 0055): this index holds one entry per *segment*, so a million records at
//! N=16 is ~62 500 entries and two or three levels even drained. Collapsing costs a
//! sideways read of a node that may have a pending write in the batch being planned
//! — so through the `Overlay`, not the `Store`; paid, it mirrors `chain_remove`.
//!
//! [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md
//! [`plan_upsert`]: SparseTree::plan_upsert
//! [`plan_remove`]: SparseTree::plan_remove

use crate::error::{Error, Result};
use crate::local_id::LocalId;
use crate::store::{Store, Write};

use super::node_key::SecKey;
use super::segment::mint_lane_id;
use super::sparse::{Branch, Slot, SparseNode};
use super::sparse_tree::SparseTree;

/// One internal level of a write descent: the node, its entries, and which
/// branch the descent took.
struct Frame {
    id: LocalId,
    entries: Vec<Branch>,
    idx: usize,
}

impl SparseTree {
    /// File `slot` under its own least key, replacing any entry already there.
    ///
    /// Replacement is the common case, not the exception: a segment that gained
    /// or lost one record keeps its separator and revises its count. This is the
    /// operation `BpTree` cannot express — its `plan_insert` returns an empty
    /// batch for a key already present, so a count revision through it would be a
    /// silent no-op.
    ///
    /// # Errors
    /// As [`find`](Self::find).
    pub async fn plan_upsert<S: Store>(
        &self,
        store: &S,
        slot: Slot,
    ) -> Result<Vec<Write>> {
        let (path, leaf_id, mut slots) =
            self.descend(store, &slot.first).await?;
        match slots.binary_search_by(|s| s.first.cmp(&slot.first)) {
            Ok(i) => slots[i] = slot,
            Err(i) => slots.insert(i, slot),
        }
        Ok(self.climb(path, leaf_id, SparseNode::Leaf(slots)))
    }

    /// Drop the entry filed under `first`. An absent key writes nothing.
    ///
    /// # Errors
    /// As [`find`](Self::find).
    pub async fn plan_remove<S: Store>(
        &self,
        store: &S,
        first: &SecKey,
    ) -> Result<Vec<Write>> {
        let (path, leaf_id, mut slots) = self.descend(store, first).await?;
        let Ok(i) = slots.binary_search_by(|s| s.first.cmp(first)) else {
            return Ok(Vec::new());
        };
        slots.remove(i);
        Ok(self.climb(path, leaf_id, SparseNode::Leaf(slots)))
    }

    /// Descend to the leaf that owns `key`, recording the internal path.
    async fn descend<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<(Vec<Frame>, LocalId, Vec<Slot>)> {
        let mut path = Vec::new();
        let mut id = self.root;
        loop {
            match self.load(store, id).await? {
                SparseNode::Leaf(slots) => return Ok((path, id, slots)),
                SparseNode::Internal(entries) => {
                    let idx = Self::branch_for(&entries, key);
                    let Some(next) = entries.get(idx).map(|b| b.node) else {
                        return Err(Error::ChainNodeMissing(id));
                    };
                    path.push(Frame { id, entries, idx });
                    id = next;
                }
            }
        }
    }

    /// Which child covers `key` on the way *down a write*.
    ///
    /// The last branch whose least key is `<= key`, clamped to the leftmost when
    /// `key` sorts below everything — a read has nothing to return there, but an
    /// insert belongs at the front of the first leaf. That clamp is why the
    /// read-side `step_to_key`, which answers `None`, is not reused here.
    fn branch_for(entries: &[Branch], key: &SecKey) -> usize {
        entries
            .partition_point(|b| b.first <= *key)
            .saturating_sub(1)
    }

    /// Rewrite the edited leaf and every ancestor above it, propagating splits
    /// and recomputing counts, and grow the root a level if it overflowed.
    fn climb(
        &self,
        path: Vec<Frame>,
        leaf_id: LocalId,
        leaf: SparseNode,
    ) -> Vec<Write> {
        let mut writes = Vec::new();
        let mut id = leaf_id;
        let mut node = leaf;

        for frame in path.into_iter().rev() {
            let mut entries = frame.entries;
            // The least key doubles as the emptiness test, and taking it here
            // (before any split) is what keeps this free of an unwrap: a split
            // moves the *upper* entries, so the lower half's least key is
            // unchanged by it.
            match node.first_key().cloned() {
                None => {
                    // Covers nothing: drop the entry naming it and delete the
                    // value, so a shrinking index leaks no nodes.
                    let node_id = id.to_id(self.tenant);
                    writes.push(Write::Remove(self.lane_hash, node_id));
                    entries.remove(frame.idx);
                }
                Some(first) => {
                    let carry = self.split_sibling(&mut node, &mut writes);
                    entries[frame.idx] = Branch {
                        first,
                        node: id,
                        count: node.total(),
                    };
                    writes.push(self.put(id, &node));
                    if let Some(branch) = carry {
                        entries.insert(frame.idx + 1, branch);
                    }
                }
            }
            id = frame.id;
            node = SparseNode::Internal(entries);
        }

        // `node` now holds the root's contents. Only the root grows a level, and
        // it does so keeping its id: the contents move into a fresh child and the
        // root becomes the node above both halves.
        if let Some(upper) = self.split_sibling(&mut node, &mut writes) {
            let lower_id = mint_lane_id(self.lane_hash);
            let lower = node.first_key().cloned().map(|first| Branch {
                first,
                node: lower_id,
                count: node.total(),
            });
            writes.push(self.put(lower_id, &node));
            node = SparseNode::Internal(
                lower.into_iter().chain(core::iter::once(upper)).collect(),
            );
        }
        // A root that lost its last child would be an `Internal` naming nobody,
        // which no descent can step through — an empty **leaf** is what an empty
        // index is, and it is the state `plan_create` starts from. Restoring it
        // here is what lets a fully drained index be written to again.
        if matches!(&node, SparseNode::Internal(entries) if entries.is_empty())
        {
            node = SparseNode::Leaf(Vec::new());
        }
        writes.push(self.put(self.root, &node));
        writes
    }

    /// Split `node` when it is over capacity, writing the upper half under a
    /// fresh id and returning the [`Branch`] a parent files it under.
    fn split_sibling(
        &self,
        node: &mut SparseNode,
        writes: &mut Vec<Write>,
    ) -> Option<Branch> {
        if node.len() <= self.cap {
            return None;
        }
        let (first, upper) = node.split_off_half()?;
        let id = mint_lane_id(self.lane_hash);
        let branch = Branch {
            first,
            node: id,
            count: upper.total(),
        };
        writes.push(self.put(id, &upper));
        Some(branch)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::{Slot, SparseTree};
    use crate::index::mem_store::MemStore;
    use crate::index::node_key::SecKey;
    use crate::local_id::LocalId;
    use crate::store::Store;
    use crate::u48::U48;

    const TYPE_HASH: u64 = 0x5EED_0001_0002_0003;

    fn tenant() -> U48 {
        U48::new(9).unwrap()
    }

    fn key(value: u64) -> SecKey {
        SecKey {
            field: value.to_be_bytes().to_vec(),
            rec: LocalId::new(value, false, 5),
        }
    }

    fn slot(value: u64, seg: u64, count: u64) -> Slot {
        Slot {
            first: key(value),
            seg: LocalId::new(seg, true, 6),
            count,
        }
    }

    async fn fresh(store: &MemStore, cap: usize) -> SparseTree {
        let (tree, write) = SparseTree::plan_create(tenant(), TYPE_HASH);
        store.apply(&[write]).await.unwrap();
        tree.with_cap(cap)
    }

    async fn upsert(store: &MemStore, tree: &SparseTree, entry: Slot) {
        let writes = tree.plan_upsert(store, entry).await.unwrap();
        store.apply(&writes).await.unwrap();
    }

    async fn remove(store: &MemStore, tree: &SparseTree, first: &SecKey) {
        let writes = tree.plan_remove(store, first).await.unwrap();
        store.apply(&writes).await.unwrap();
    }

    /// Every slot in key order, walked through the **offset** descent: each
    /// step lands on the next segment by adding the previous one's count.
    async fn walk(store: &MemStore, tree: &SparseTree) -> Vec<Slot> {
        let mut out = Vec::new();
        let mut at = 0;
        while let Some((entry, within)) =
            tree.find_offset(store, at).await.unwrap()
        {
            assert_eq!(within, 0, "a walk lands on segment boundaries");
            at += entry.count;
            out.push(entry);
        }
        out
    }

    #[test]
    fn an_empty_index_answers_nothing_without_faulting() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 4).await;
            assert_eq!(tree.total(&store).await.unwrap(), 0);
            assert_eq!(tree.find(&store, &key(1)).await.unwrap(), None);
            assert_eq!(tree.find_offset(&store, 0).await.unwrap(), None);
        });
    }

    #[test]
    fn a_slot_is_found_by_every_key_its_segment_covers() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 4).await;
            upsert(&store, &tree, slot(10, 100, 3)).await;
            upsert(&store, &tree, slot(20, 200, 3)).await;

            // Its own least key, and anything up to the next separator.
            for probe in [10, 11, 19] {
                let found = tree.find(&store, &key(probe)).await.unwrap();
                assert_eq!(
                    found.map(|s| s.seg),
                    Some(LocalId::new(100, true, 6))
                );
            }
            let found = tree.find(&store, &key(20)).await.unwrap();
            assert_eq!(found.map(|s| s.seg), Some(LocalId::new(200, true, 6)));
            // Below everything: the chain's head covers it, not the index.
            assert_eq!(tree.find(&store, &key(1)).await.unwrap(), None);
        });
    }

    #[test]
    fn upserting_a_separator_revises_its_count_instead_of_duplicating_it() {
        block_on(async {
            // The operation `BpTree` cannot express: its `plan_insert` returns an
            // empty batch for a key already present, so this revision would be a
            // silent no-op there.
            let store = MemStore::default();
            let tree = fresh(&store, 4).await;
            upsert(&store, &tree, slot(10, 100, 3)).await;
            upsert(&store, &tree, slot(10, 100, 9)).await;

            assert_eq!(
                walk(&store, &tree).await.len(),
                1,
                "no duplicate entry"
            );
            assert_eq!(tree.total(&store).await.unwrap(), 9);
        });
    }

    #[test]
    fn the_root_id_survives_a_level_growth() {
        block_on(async {
            // The property that keeps the `Pivot` from being rewritten as the
            // index grows (RFC 0050): the root's contents move into a fresh
            // child, the root id does not move.
            let store = MemStore::default();
            let tree = fresh(&store, 3).await;
            let root = tree.root();

            for i in 1..=40u64 {
                upsert(&store, &tree, slot(i * 10, i * 100, 2)).await;
            }

            assert_eq!(tree.root(), root, "the root pointer moved");
            let (_, depth) = tree.reachable(&store).await;
            assert!(depth >= 3, "expected a grown tree, got depth {depth}");
        });
    }

    #[test]
    fn counts_roll_up_through_every_level() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 3).await;
            let mut expected = 0;
            for i in 1..=40u64 {
                upsert(&store, &tree, slot(i * 10, i * 100, i)).await;
                expected += i;
                assert_eq!(
                    tree.total(&store).await.unwrap(),
                    expected,
                    "root total drifted after {i} inserts"
                );
            }
        });
    }

    #[test]
    fn entries_come_out_in_key_order_whatever_the_arrival_order() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 3).await;
            // Deliberately unsorted arrivals, including one below every
            // separator already present (the leftmost-clamp path).
            for v in [50u64, 10, 90, 30, 70, 20, 80, 60, 40, 5] {
                upsert(&store, &tree, slot(v, v, 1)).await;
            }
            let keys: Vec<Vec<u8>> = walk(&store, &tree)
                .await
                .into_iter()
                .map(|s| s.first.field)
                .collect();
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "walk came out unordered");
            assert_eq!(keys.len(), 10);
        });
    }

    #[test]
    fn an_offset_descent_lands_inside_the_right_segment_across_levels() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 3).await;
            // 20 segments of 5 elements: global offset 47 is element 2 of the
            // tenth segment (offsets 45..50).
            for i in 0..20u64 {
                upsert(&store, &tree, slot(i * 10, i, 5)).await;
            }
            let (found, within) =
                tree.find_offset(&store, 47).await.unwrap().unwrap();
            assert_eq!(found.seg, LocalId::new(9, true, 6));
            assert_eq!(within, 2);
            // One past the last element resolves to nothing.
            assert_eq!(tree.find_offset(&store, 100).await.unwrap(), None);
        });
    }

    #[test]
    fn a_shrinking_index_drops_its_emptied_nodes_and_leaks_none() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 3).await;
            for i in 1..=40u64 {
                upsert(&store, &tree, slot(i * 10, i * 100, 2)).await;
            }
            let (reachable, _) = tree.reachable(&store).await;
            assert_eq!(store.len(), reachable, "orphan nodes after growth");

            for i in 1..=40u64 {
                remove(&store, &tree, &key(i * 10)).await;
                let (reachable, _) = tree.reachable(&store).await;
                assert_eq!(
                    store.len(),
                    reachable,
                    "orphan nodes after removing {i}"
                );
            }
            assert_eq!(tree.total(&store).await.unwrap(), 0);
            let observed = walk(&store, &tree).await;
            assert!(observed.is_empty(), "{observed:?}");
        });
    }

    #[test]
    fn a_fully_drained_index_can_be_written_to_again() {
        block_on(async {
            // The state a chain reaches when its last record goes: every level
            // the index grew has collapsed away. If the root were left as an
            // `Internal` naming nobody, the next write's descent would step
            // through no child and fault — reads would stay fine, which is what
            // made this survive a suite that only read after draining.
            let store = MemStore::default();
            let tree = fresh(&store, 3).await;
            for i in 1..=40u64 {
                upsert(&store, &tree, slot(i * 10, i * 100, 2)).await;
            }
            for i in 1..=40u64 {
                remove(&store, &tree, &key(i * 10)).await;
            }
            assert_eq!(tree.total(&store).await.unwrap(), 0);

            for i in 1..=40u64 {
                upsert(&store, &tree, slot(i * 10, i * 100, 3)).await;
            }
            assert_eq!(tree.total(&store).await.unwrap(), 120);
            assert_eq!(walk(&store, &tree).await.len(), 40);
            let (reachable, _) = tree.reachable(&store).await;
            assert_eq!(store.len(), reachable, "orphans after the refill");
        });
    }

    #[test]
    fn removing_an_absent_separator_writes_nothing() {
        block_on(async {
            let store = MemStore::default();
            let tree = fresh(&store, 4).await;
            upsert(&store, &tree, slot(10, 100, 3)).await;
            let writes = tree.plan_remove(&store, &key(99)).await.unwrap();
            assert!(writes.is_empty(), "a no-op still planned {writes:?}");
        });
    }

    #[test]
    fn a_dangling_root_is_a_typed_fault_not_a_panic() {
        block_on(async {
            let store = MemStore::default();
            // Never persisted: the handle points at a root that does not exist.
            let (tree, _) = SparseTree::plan_create(tenant(), TYPE_HASH);
            assert!(matches!(
                tree.find(&store, &key(1)).await,
                Err(crate::error::Error::ChainNodeMissing(_))
            ));
        });
    }
}
