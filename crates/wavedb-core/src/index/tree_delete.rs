//! [`BpTree::remove`] — descend, delete from the leaf, and rebalance upward:
//! an underfull node merges with an adjacent sibling when the pair fits in one
//! node, otherwise redistributes entries with it; a root that loses its last
//! separator collapses into its single child.
//!
//! Thresholds: a non-root node is **underfull** below ¼ of its capacity; a
//! merge happens when the pair's combined size stays within ¾ — past that the
//! pair redistributes to ~half each (both halves land above ¼, so a
//! redistribution never cascades).

use crate::error::Result;
use crate::local_id::LocalId;
use crate::store::{Store, Write};

use super::node::NodeBody;
use super::node_key::NodeKey;
use super::tree::{BpTree, PathFrame, child_and_index};

impl<K: NodeKey> BpTree<K> {
    /// Remove `key`. Returns whether it was present. Underfull nodes
    /// merge or redistribute with a sibling; the root collapses when it loses
    /// its last separator. All touched nodes commit in **one**
    /// [`Store::apply`] batch. Updates [`root`](Self::root) on a collapse.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn remove<S: Store>(
        &mut self,
        store: &S,
        key: impl Into<K>,
    ) -> Result<bool> {
        match self.plan_remove(store, key).await? {
            Some(batch) => {
                store.apply(&batch).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Plan a removal of `key`: every node [`Write`] the removal needs,
    /// **without applying** — so a caller can commit the index change and its
    /// record in one atomic batch. Reads through `store`; updates
    /// [`root`](Self::root) on a collapse (the handle assumes the batch will
    /// be applied). `None` = key not present (nothing to write).
    ///
    /// # Errors
    /// Propagates a [`Store`] read failure.
    pub async fn plan_remove<S: Store>(
        &mut self,
        store: &S,
        key: impl Into<K>,
    ) -> Result<Option<Vec<Write>>> {
        let target: K = key.into();

        // Descend to the leaf, recording the internal path for rebalancing.
        let mut path: Vec<PathFrame<K>> = Vec::new();
        let mut node_id = self.root;
        let mut keys = loop {
            match self.load(store, node_id).await? {
                NodeBody::Leaf(keys) => break keys,
                NodeBody::Internal { leftmost, entries } => {
                    let (next, child_idx) =
                        child_and_index(leftmost, &entries, &target);
                    path.push(PathFrame {
                        node_id,
                        leftmost,
                        entries,
                        child_idx,
                    });
                    node_id = next;
                }
            }
        };

        let Ok(pos) = keys.binary_search(&target) else {
            return Ok(None);
        };
        keys.remove(pos);

        let mut batch: Vec<Write> = Vec::new();
        let mut current = NodeBody::Leaf(keys);
        loop {
            let Some(frame) = path.pop() else {
                self.finish_at_root(&mut batch, node_id, &current);
                break;
            };
            if !self.is_underfull(&current) {
                batch.push(self.put(node_id, &current));
                break; // ancestors untouched
            }
            match self
                .rebalance(store, &mut batch, node_id, current, frame)
                .await?
            {
                // A merge dropped a separator from the parent — the parent
                // itself may now be underfull; keep walking up.
                Rebalanced::ParentPending { id, node } => {
                    node_id = id;
                    current = node;
                }
                Rebalanced::Done => break,
            }
        }

        Ok(Some(batch))
    }

    /// The reached node is the root: persist it, collapsing an internal root
    /// that lost its last separator into its single child.
    fn finish_at_root(
        &mut self,
        batch: &mut Vec<Write>,
        root_id: LocalId,
        root: &NodeBody<K>,
    ) {
        if let NodeBody::Internal { leftmost, entries } = root
            && entries.is_empty()
        {
            batch.push(self.remove_node(root_id));
            self.root = *leftmost;
            return;
        }
        batch.push(self.put(root_id, root));
    }

    /// Merge or redistribute the underfull `current` with an adjacent sibling
    /// reached through its parent `frame`.
    async fn rebalance<S: Store>(
        &self,
        store: &S,
        batch: &mut Vec<Write>,
        node_id: LocalId,
        current: NodeBody<K>,
        frame: PathFrame<K>,
    ) -> Result<Rebalanced<K>> {
        let PathFrame {
            node_id: parent_id,
            leftmost,
            mut entries,
            child_idx,
        } = frame;

        // Degenerate parent (single child, no separators): nothing to borrow
        // from — push the underflow up to the parent.
        if entries.is_empty() {
            batch.push(self.put(node_id, &current));
            return Ok(Rebalanced::ParentPending {
                id: parent_id,
                node: NodeBody::Internal { leftmost, entries },
            });
        }

        // Prefer the left sibling; the leftmost child pairs with its right one.
        // Child `c` is `leftmost` for 0 / `entries[c-1].1` otherwise; the
        // separator between children `c` and `c+1` is `entries[c].0`.
        let left_idx = child_idx.saturating_sub(1);
        let child_id = |c: usize| {
            if c == 0 { leftmost } else { entries[c - 1].1 }
        };
        let (left_id, right_id) = (child_id(left_idx), child_id(left_idx + 1));
        let (left, right) = if child_idx == left_idx {
            (current, self.load(store, right_id).await?)
        } else {
            (self.load(store, left_id).await?, current)
        };
        let sep = entries[left_idx].0.clone();

        if let Some(merged) = self.merge(&left, &right, sep.clone()) {
            batch.push(self.put(left_id, &merged));
            batch.push(self.remove_node(right_id));
            entries.remove(left_idx); // drops the separator + right child
            return Ok(Rebalanced::ParentPending {
                id: parent_id,
                node: NodeBody::Internal { leftmost, entries },
            });
        }

        let (new_left, new_right, new_sep) = redistribute(left, right, sep);
        batch.push(self.put(left_id, &new_left));
        batch.push(self.put(right_id, &new_right));
        entries[left_idx].0 = new_sep;
        batch.push(
            self.put(parent_id, &NodeBody::Internal { leftmost, entries }),
        );
        Ok(Rebalanced::Done) // parent entry count unchanged
    }

    /// `true` when a **non-root** node has fallen below ¼ of its capacity.
    fn is_underfull(&self, node: &NodeBody<K>) -> bool {
        match node {
            NodeBody::Leaf(keys) => keys.len() < self.leaf_cap / 4,
            NodeBody::Internal { entries, .. } => {
                entries.len() < self.internal_cap / 4
            }
        }
    }

    /// Merge `right` into `left` when the pair fits within ¾ of the capacity;
    /// `None` means "too big — redistribute instead".
    fn merge(
        &self,
        left: &NodeBody<K>,
        right: &NodeBody<K>,
        sep: K,
    ) -> Option<NodeBody<K>> {
        match (left, right) {
            (NodeBody::Leaf(l), NodeBody::Leaf(r)) => {
                (l.len() + r.len() <= self.leaf_cap * 3 / 4).then(|| {
                    let mut keys = l.clone();
                    keys.extend(r.iter().cloned());
                    NodeBody::Leaf(keys)
                })
            }
            (
                NodeBody::Internal {
                    leftmost: l_lm,
                    entries: l_e,
                },
                NodeBody::Internal {
                    leftmost: r_lm,
                    entries: r_e,
                },
            ) => {
                // +1: the separator pulled down between the two halves.
                let merged_len = l_e.len() + r_e.len() + 1;
                (merged_len <= self.internal_cap * 3 / 4).then(|| {
                    let mut entries = l_e.clone();
                    entries.push((sep, *r_lm));
                    entries.extend(r_e.iter().cloned());
                    NodeBody::Internal {
                        leftmost: *l_lm,
                        entries,
                    }
                })
            }
            // Mixed kinds — siblings at one level are always the same kind in
            // a well-formed tree; degrade to "leave underfull" rather than
            // corrupt anything.
            _ => {
                debug_assert!(false, "sibling node kinds differ");
                None
            }
        }
    }
}

/// A rebalance step's outcome.
enum Rebalanced<K: NodeKey> {
    /// A merge removed a separator from the parent; the parent (returned as
    /// the new current node) may itself need rebalancing.
    ParentPending { id: LocalId, node: NodeBody<K> },
    /// A redistribution updated the parent in place; nothing propagates.
    Done,
}

/// Split the pair's combined entries evenly across both nodes, returning the
/// rewritten `(left, right, separator)`.
fn redistribute<K: NodeKey>(
    left: NodeBody<K>,
    right: NodeBody<K>,
    sep: K,
) -> (NodeBody<K>, NodeBody<K>, K) {
    match (left, right) {
        (NodeBody::Leaf(l), NodeBody::Leaf(r)) => {
            let mut combined = l;
            combined.extend(r);
            let right_keys = combined.split_off(combined.len() / 2);
            let new_sep = right_keys[0].clone();
            (
                NodeBody::Leaf(combined),
                NodeBody::Leaf(right_keys),
                new_sep,
            )
        }
        (
            NodeBody::Internal {
                leftmost: l_lm,
                entries: l_e,
            },
            NodeBody::Internal {
                leftmost: r_lm,
                entries: r_e,
            },
        ) => {
            // Combined child chain: rotate through the parent separator, then
            // re-split at the median, promoting its separator to the parent.
            let mut combined = l_e;
            combined.push((sep, r_lm));
            combined.extend(r_e);
            let mid = combined.len() / 2;
            let mut right_entries = combined.split_off(mid);
            let (new_sep, new_r_leftmost) = right_entries.remove(0);
            (
                NodeBody::Internal {
                    leftmost: l_lm,
                    entries: combined,
                },
                NodeBody::Internal {
                    leftmost: new_r_leftmost,
                    entries: right_entries,
                },
                new_sep,
            )
        }
        // Unreachable per merge()'s kind check; keep both sides unchanged.
        (l, r) => (l, r, sep),
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use futures::executor::block_on;

    use super::super::Bound;
    use super::super::mem_store::{MemStore, check_invariants};
    use super::super::tree::BpTree;
    use crate::id::Id;
    use crate::u48::U48;

    const TENANT: u32 = 7;

    fn rec(created_at: u64) -> Id {
        Id::new(
            created_at,
            U48::from(TENANT),
            false,
            (created_at & 0x7FFF) as u16,
        )
    }

    async fn all_keys(tree: &BpTree, store: &MemStore) -> Vec<u64> {
        tree.search(store, Bound::All)
            .map(|r| r.unwrap().key())
            .collect()
            .await
    }

    #[test]
    fn remove_takes_key_out() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree =
                BpTree::create(&store, U48::from(TENANT)).await.unwrap();
            for k in [1u64, 2, 3, 4, 5] {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            assert!(tree.remove(&store, rec(3)).await.unwrap());
            assert!(!tree.remove(&store, rec(3)).await.unwrap()); // gone
            assert_eq!(all_keys(&tree, &store).await, vec![1, 2, 4, 5]);
        });
    }

    // Deleting most of a multi-level tree must merge nodes back together,
    // reclaim them from the store, and collapse the root — with the shape
    // invariants holding at every step.
    #[test]
    fn deletion_merges_reclaims_and_collapses() {
        block_on(async {
            let tenant = U48::from(TENANT);
            let store = MemStore::default();
            let mut tree: BpTree = BpTree::create(&store, tenant)
                .await
                .unwrap()
                .with_caps(8, 8);

            let n = 500u64;
            for i in 0..n {
                let k = (i.wrapping_mul(97) % n) + 1; // scrambled 1..=n
                tree.insert(&store, rec(k)).await.unwrap();
            }
            let peak_nodes = store.len();
            let peak_root = tree.root();
            assert!(peak_nodes > 50, "tree too shallow to exercise merges");

            // Remove all but three, scrambled, checking invariants along the way.
            let mut expected: Vec<u64> = (1..=n).collect();
            for i in 0..n - 3 {
                let k = (i.wrapping_mul(61) % n) + 1;
                if expected.contains(&k) {
                    assert!(tree.remove(&store, rec(k)).await.unwrap());
                    expected.retain(|&e| e != k);
                } else {
                    assert!(!tree.remove(&store, rec(k)).await.unwrap());
                }
                if i % 50 == 0 {
                    let keys = check_invariants(&tree, &store, tenant).await;
                    assert_eq!(keys, expected, "walk diverged at step {i}");
                }
            }
            // Drain the stragglers.
            for k in expected.clone() {
                assert!(tree.remove(&store, rec(k)).await.unwrap());
            }

            let keys = check_invariants(&tree, &store, tenant).await;
            assert!(keys.is_empty(), "{keys:?}");
            assert_eq!(store.len(), 1, "empty tree = exactly one root leaf");
            assert_ne!(tree.root(), peak_root, "root never collapsed");
        });
    }

    // The redistribution path: a merge is impossible when the sibling is rich,
    // so entries move across and the parent separator updates in place.
    #[test]
    fn redistribution_borrows_from_rich_sibling() {
        block_on(async {
            let tenant = U48::from(TENANT);
            let store = MemStore::default();
            let mut tree: BpTree = BpTree::create(&store, tenant)
                .await
                .unwrap()
                .with_caps(8, 8); // underfull < 2, merge cap 6

            // Two leaves: left [1..=4], right [5..=12] after splits settle.
            for k in 1..=12u64 {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            check_invariants(&tree, &store, tenant).await;

            // Starve one leaf: each deletion that underflows must rebalance,
            // and with a full sibling the first step is a redistribution.
            for k in [1u64, 2, 3, 4] {
                assert!(tree.remove(&store, rec(k)).await.unwrap());
                let keys = check_invariants(&tree, &store, tenant).await;
                let expected: Vec<u64> = (k + 1..=12).collect();
                assert_eq!(keys, expected);
            }
        });
    }

    // Reopening the tree at its (possibly moved) root keeps working after
    // heavy deletion — the handle's root tracking is the only state.
    #[test]
    fn survives_reopen_at_updated_root() {
        block_on(async {
            let tenant = U48::from(TENANT);
            let store = MemStore::default();
            let mut tree: BpTree = BpTree::create(&store, tenant)
                .await
                .unwrap()
                .with_caps(8, 8);
            for k in 1..=200u64 {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            for k in 1..=150u64 {
                tree.remove(&store, rec(k)).await.unwrap();
            }

            let reopened = BpTree::at(tree.root(), tenant).with_caps(8, 8);
            let keys = all_keys(&reopened, &store).await;
            assert_eq!(keys, (151..=200).collect::<Vec<u64>>());
        });
    }
}
