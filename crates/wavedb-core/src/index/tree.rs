//! [`BpTree`] — the `Store`-generic B+tree indexing a NonUnique collection,
//! one node per [`Store`] value, generic over its key type.
//!
//! ## What the tree keys on
//!
//! The key type `K` is a [`NodeKey`]. The **primary** tree uses `K =`
//! [`LocalId`]: a NonUnique record's `LocalId` is `CREATED_AT` (8 B, most
//! significant) then `FLAG|SALT` (2 B), so ordering the `LocalId`s **is**
//! chronological order, the trailing salt makes every key unique, and the key
//! doubles as the record pointer. A **secondary** index uses `K =`
//! [`SecKey`](super::SecKey): the record's order-preserving field bytes plus
//! its `LocalId` (unique under duplicate field values). Same machinery, fully
//! monomorphized — no `dyn`.
//!
//! ## Where it runs
//!
//! All I/O is delegated to [`Store`] (`get` + `apply`), so the same tree serves
//! the node's page engine and the browser's IndexedDB — backends are
//! interchangeable under it. The write paths are in the sibling modules:
//! insert/split in [`tree_insert`](super::tree_insert), remove/merge in
//! [`tree_delete`](super::tree_delete).

use std::collections::VecDeque;
use std::marker::PhantomData;

use futures::Stream;

use crate::error::{Error, Result};
use crate::id::Id;
use crate::local_id::LocalId;
use crate::store::{Store, Write};
use crate::u48::U48;

use super::Bound;
use super::node::{BPTREE_NODE_STRUCT_HASH, NodeBody, mint_node_id};
use super::node_key::NodeKey;

/// Max keys in a leaf before it splits. Sized so a node fits the storage
/// engine's 32 KiB page with room to spare.
pub const DEFAULT_LEAF_CAP: usize = 1819;
/// Max `(separator, child)` entries in an internal node before it splits.
pub const DEFAULT_INTERNAL_CAP: usize = 1630;

/// A B+tree over a [`Store`], scoped to one tenant, keyed by `K` (the
/// primary [`LocalId`] tree by default).
///
/// Holds only its root pointer and its split/merge capacities; nodes live in
/// the `Store` keyed by `LocalId::to_id(tenant)`. Use one capacity
/// configuration for a tree's whole lifetime — capacities are a handle-side
/// policy, not recorded in the nodes.
#[derive(Debug, Clone)]
pub struct BpTree<K: NodeKey = LocalId> {
    pub(super) root: LocalId,
    pub(super) tenant: U48,
    pub(super) leaf_cap: usize,
    pub(super) internal_cap: usize,
    pub(super) _key: PhantomData<fn() -> K>,
}

// Manual: a `Copy` derive would demand `K: Copy` (a `SecKey` isn't), but the
// handle holds only ids and caps (the `PhantomData` is `fn() -> K`, `Copy`).
impl<K: NodeKey> Copy for BpTree<K> {}

impl<K: NodeKey> BpTree<K> {
    /// Open an existing tree at `root` for `tenant` (default capacities).
    #[must_use]
    pub const fn at(root: LocalId, tenant: U48) -> Self {
        Self {
            root,
            tenant,
            leaf_cap: DEFAULT_LEAF_CAP,
            internal_cap: DEFAULT_INTERNAL_CAP,
            _key: PhantomData,
        }
    }

    /// Override the node capacities (small caps make deep trees cheap to build
    /// in tests; production uses the defaults).
    #[must_use]
    pub const fn with_caps(mut self, leaf: usize, internal: usize) -> Self {
        self.leaf_cap = leaf;
        self.internal_cap = internal;
        self
    }

    /// Plan a fresh, empty tree: the handle plus the [`Write`] that persists its
    /// empty root leaf. The caller commits the write (alone or inside a larger
    /// atomic batch) and persists the root in its `Pivot`.
    #[must_use]
    pub fn plan_create(tenant: U48) -> (Self, Write) {
        let root = mint_node_id();
        let tree = Self::at(root, tenant);
        let write = tree.put(root, &NodeBody::Leaf(Vec::new()));
        (tree, write)
    }

    /// Create a fresh, empty tree: writes an empty leaf root and returns the
    /// handle. The caller persists the returned root in its `Pivot`.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn create<S: Store>(store: &S, tenant: U48) -> Result<Self> {
        let (tree, write) = Self::plan_create(tenant);
        store.apply(&[write]).await?;
        Ok(tree)
    }

    /// The current root pointer (changes when the root splits or collapses).
    #[must_use]
    pub const fn root(&self) -> LocalId {
        self.root
    }

    /// Whether `key` is present in the tree.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn contains<S: Store>(
        &self,
        store: &S,
        key: impl Into<K>,
    ) -> Result<bool> {
        let target = key.into();
        let mut node = self.root;
        loop {
            match self.load(store, node).await? {
                NodeBody::Leaf(keys) => {
                    return Ok(keys.binary_search(&target).is_ok());
                }
                NodeBody::Internal { leftmost, entries } => {
                    node = child_for(leftmost, &entries, &target);
                }
            }
        }
    }

    /// The greatest key in the tree (`None` = empty): the rightmost leaf's
    /// last entry — an O(depth) descent, no scan.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn max_key<S: Store>(&self, store: &S) -> Result<Option<K>> {
        let mut node = self.root;
        loop {
            match self.load(store, node).await? {
                NodeBody::Leaf(keys) => return Ok(keys.last().cloned()),
                NodeBody::Internal { leftmost, entries } => {
                    node = entries.last().map_or(leftmost, |(_, child)| *child);
                }
            }
        }
    }

    /// Stream the record `Id`s in key order whose keys satisfy `bound` —
    /// [`search_keys`](Self::search_keys) resolved to the record pointers.
    ///
    /// Two-phase resolution (index → `Id`s → caller fetch) lives above this: the
    /// stream yields the record `Id`s; resolving them to bytes is the caller's
    /// `Store::get`.
    pub fn search<'a, S: Store>(
        &self,
        store: &'a S,
        bound: Bound,
    ) -> impl Stream<Item = Result<Id>> + use<'a, S, K> {
        use futures::StreamExt;
        let tenant = self.tenant;
        self.search_keys(store, bound)
            .map(move |r| r.map(|k| k.record().to_id(tenant)))
    }

    /// Stream the **keys** in order that satisfy `bound` — what the
    /// instant-keyed system logs scan, where the key itself (not just the
    /// record it points at) carries the answer. The descent prunes subtrees
    /// whose separator window cannot intersect the bound (see
    /// [`NodeKey::may_intersect`]).
    pub fn search_keys<'a, S: Store>(
        &self,
        store: &'a S,
        bound: Bound,
    ) -> impl Stream<Item = Result<K>> + use<'a, S, K> {
        let mut nodes: VecDeque<LocalId> = VecDeque::new();
        nodes.push_back(self.root);
        let init = WalkState::<K> {
            nodes,
            ready: VecDeque::new(),
            bound,
            tenant: self.tenant,
            _key: PhantomData,
        };

        futures::stream::unfold(init, move |mut st| async move {
            loop {
                if let Some(key) = st.ready.pop_front() {
                    return Some((Ok(key), st));
                }
                let node = st.nodes.pop_front()?;
                let bytes = match store
                    .get_of(BPTREE_NODE_STRUCT_HASH, node.to_id(st.tenant))
                    .await
                {
                    Ok(Some(b)) => b,
                    Ok(None) => {
                        return Some((Err(Error::BpTreeNodeMissing(node)), st));
                    }
                    Err(e) => return Some((Err(e), st)),
                };
                match NodeBody::<K>::from_bytes(&bytes) {
                    Ok(NodeBody::Leaf(keys)) => {
                        for k in keys {
                            if k.matches(&st.bound) {
                                st.ready.push_back(k);
                            }
                        }
                    }
                    Ok(NodeBody::Internal { leftmost, entries }) => {
                        expand_children(&mut st, leftmost, &entries);
                    }
                    Err(e) => return Some((Err(e), st)),
                }
            }
        })
    }

    /// Load and decode the node at `id`.
    pub(super) async fn load<S: Store>(
        &self,
        store: &S,
        node: LocalId,
    ) -> Result<NodeBody<K>> {
        let bytes = store
            .get_of(BPTREE_NODE_STRUCT_HASH, node.to_id(self.tenant))
            .await?
            .ok_or(Error::BpTreeNodeMissing(node))?;
        NodeBody::from_bytes(&bytes)
    }

    /// A `Put` write for `node`'s serialised bytes under this tenant.
    pub(super) fn put(&self, node: LocalId, n: &NodeBody<K>) -> Write {
        Write::Put(node.to_id(self.tenant), n.to_bytes())
    }

    /// A `Remove` write freeing `node` under this tenant.
    pub(super) fn remove_node(&self, node: LocalId) -> Write {
        Write::Remove(BPTREE_NODE_STRUCT_HASH, node.to_id(self.tenant))
    }
}

/// One internal node on a descent path, kept so an upward split/merge knows
/// where to insert or drop a separator.
pub(super) struct PathFrame<K: NodeKey> {
    pub node_id: LocalId,
    pub leftmost: LocalId,
    pub entries: Vec<(K, LocalId)>,
    /// Slot the descent took (`0` = leftmost, `i + 1` = `entries[i]`).
    pub child_idx: usize,
}

/// State threaded through the `search_keys` walk.
struct WalkState<K: NodeKey> {
    nodes: VecDeque<LocalId>,
    ready: VecDeque<K>,
    bound: Bound,
    tenant: U48,
    _key: PhantomData<fn() -> K>,
}

/// Queue an internal node's children in key order, skipping subtrees whose
/// key window cannot intersect the bound.
fn expand_children<K: NodeKey>(
    st: &mut WalkState<K>,
    leftmost: LocalId,
    entries: &[(K, LocalId)],
) {
    // Child i covers keys in [sep_{i-1}, sep_i) — probe the inclusive window.
    let keep = |i: usize| -> bool {
        let min = if i == 0 {
            None
        } else {
            Some(&entries[i - 1].0)
        };
        let max = entries.get(i).map(|(sep, _)| sep);
        K::may_intersect(&st.bound, min, max)
    };
    // Push in reverse so the leftmost kept child ends up at the queue's front.
    for (i, (_, child)) in entries.iter().enumerate().rev() {
        if keep(i + 1) {
            st.nodes.push_front(*child);
        }
    }
    if keep(0) {
        st.nodes.push_front(leftmost);
    }
}

/// The child pointer a `target` routes to (without its index).
pub(super) fn child_for<K: NodeKey>(
    leftmost: LocalId,
    entries: &[(K, LocalId)],
    target: &K,
) -> LocalId {
    child_and_index(leftmost, entries, target).0
}

/// The child pointer and its slot index (`0` = leftmost, `i + 1` = `entries[i]`).
pub(super) fn child_and_index<K: NodeKey>(
    leftmost: LocalId,
    entries: &[(K, LocalId)],
    target: &K,
) -> (LocalId, usize) {
    // Last separator `<= target`; if none, the leftmost child.
    match entries.binary_search_by(|(sep, _)| sep.cmp(target)) {
        Ok(i) => (entries[i].1, i + 1),
        Err(0) => (leftmost, 0),
        Err(i) => (entries[i - 1].1, i),
    }
}

/// The index layer's futures are `Send` over a `Send + Sync` store — asserted,
/// because nothing arranges it.
///
/// It holds today only because `BpTree` takes its store **per call**
/// (`S: Store`) instead of owning one, so there is no `Rc` to make it
/// non-`Send`. Moving the store into a field, or holding any non-`Send` value
/// across one of these awaits, would remove the property with no other
/// symptom — and [RFC 0064](../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md)
/// rests on it: a shard's messages cross threads even though the shard's own
/// state never does.
///
/// `MemStore` stands in for any `Send + Sync` backend; `PageStore` is asserted
/// separately in `wavedb-storage`, which is where the concrete engine lives.
#[cfg(test)]
mod send_assertions {
    use super::BpTree;
    use crate::index::mem_store::MemStore;
    use crate::index::node_key::SecKey;
    use crate::local_id::LocalId;
    use crate::u48::U48;

    const _: fn(&MemStore) = |store| {
        fn assert_send<T: Send>(_: T) {}
        let tree = BpTree::<SecKey>::at(LocalId::default(), U48::from(0u32));
        assert_send(async move {
            BpTree::<SecKey>::create(store, U48::from(0u32)).await
        });
        assert_send(async move { tree.max_key(store).await });
        let key = SecKey {
            field: Vec::new(),
            rec: LocalId::default(),
        };
        assert_send(async move { tree.contains(store, key).await });
    };
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use futures::executor::block_on;

    use super::super::Bound;
    use super::super::mem_store::MemStore;
    use super::super::node_key::SecKey;
    use super::BpTree;
    use crate::id::Id;
    use crate::local_id::LocalId;
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
    fn insert_and_search_in_order() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree =
                BpTree::create(&store, U48::from(TENANT)).await.unwrap();
            for k in [50u64, 10, 30, 20, 40] {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            assert_eq!(all_keys(&tree, &store).await, vec![10, 20, 30, 40, 50]);
        });
    }

    #[test]
    fn insert_is_idempotent() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree =
                BpTree::create(&store, U48::from(TENANT)).await.unwrap();
            tree.insert(&store, rec(5)).await.unwrap();
            tree.insert(&store, rec(5)).await.unwrap();
            assert_eq!(all_keys(&tree, &store).await, vec![5]);
            assert!(tree.contains(&store, rec(5)).await.unwrap());
            assert!(!tree.contains(&store, rec(6)).await.unwrap());
        });
    }

    #[test]
    fn grows_multiple_levels_and_stays_sorted() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree = BpTree::create(&store, U48::from(TENANT))
                .await
                .unwrap()
                .with_caps(16, 16);
            let root0 = tree.root();
            // Scrambled inserts force leaf and internal splits at these caps.
            let mut expected: Vec<u64> = Vec::new();
            for i in 0..2000u64 {
                let k = i.wrapping_mul(2_654_435_761) % 1_000_003;
                tree.insert(&store, rec(k)).await.unwrap();
                expected.push(k);
            }
            expected.sort_unstable();
            expected.dedup();

            let got = all_keys(&tree, &store).await;
            assert!(got.windows(2).all(|w| w[0] < w[1]), "not strictly sorted");
            assert_eq!(got, expected, "tree lost or duplicated keys");
            assert_ne!(tree.root(), root0, "root never moved — did not grow");

            super::super::mem_store::check_invariants(
                &tree,
                &store,
                U48::from(TENANT),
            )
            .await;
        });
    }

    #[test]
    fn range_search_filters_by_created_at() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree =
                BpTree::create(&store, U48::from(TENANT)).await.unwrap();
            for k in 0..100u64 {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            let bound = Bound::Range {
                lo: 20u64.to_be_bytes().to_vec(),
                hi: 25u64.to_be_bytes().to_vec(),
            };
            let got: Vec<u64> = tree
                .search(&store, bound)
                .map(|r| r.unwrap().key())
                .collect()
                .await;
            assert_eq!(got, vec![20, 21, 22, 23, 24]); // half-open
        });
    }

    #[test]
    fn range_search_prunes_but_misses_nothing_on_deep_trees() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree = BpTree::create(&store, U48::from(TENANT))
                .await
                .unwrap()
                .with_caps(8, 8);
            for k in 0..500u64 {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            // Several bounds, compared against the brute-force expectation.
            for (lo, hi) in [(0u64, 10u64), (100, 130), (490, 600), (250, 250)]
            {
                let bound = Bound::Range {
                    lo: lo.to_be_bytes().to_vec(),
                    hi: hi.to_be_bytes().to_vec(),
                };
                let got: Vec<u64> = tree
                    .search(&store, bound)
                    .map(|r| r.unwrap().key())
                    .collect()
                    .await;
                let expected: Vec<u64> = (lo..hi.min(500)).collect();
                assert_eq!(got, expected, "range [{lo}, {hi})");
            }
            // Exact hits exactly one key even across pruned descents.
            let got: Vec<u64> = tree
                .search(&store, Bound::Exact(123u64.to_be_bytes().to_vec()))
                .map(|r| r.unwrap().key())
                .collect()
                .await;
            assert_eq!(got, vec![123]);
        });
    }

    #[test]
    fn max_key_is_the_rightmost_leaf_last_entry() {
        block_on(async {
            let store = MemStore::default();
            let mut tree: BpTree = BpTree::create(&store, U48::from(TENANT))
                .await
                .unwrap()
                .with_caps(4, 4);
            assert_eq!(tree.max_key(&store).await.unwrap(), None);
            for k in [50u64, 10, 90, 30, 70, 20, 80, 60, 40] {
                tree.insert(&store, rec(k)).await.unwrap();
            }
            let max = |k: Option<LocalId>| k.map(LocalId::key);
            assert_eq!(max(tree.max_key(&store).await.unwrap()), Some(90));
            assert!(tree.remove(&store, rec(90)).await.unwrap());
            assert_eq!(max(tree.max_key(&store).await.unwrap()), Some(80));
        });
    }

    #[test]
    fn empty_tree_searches_nothing() {
        block_on(async {
            let store = MemStore::default();
            let tree: BpTree =
                BpTree::create(&store, U48::from(TENANT)).await.unwrap();
            let observed = all_keys(&tree, &store).await;
            assert!(observed.is_empty(), "{observed:?}");
        });
    }

    // The same machinery serves a byte-keyed secondary tree: duplicate field
    // values coexist (unique by record), exact/prefix bounds select by field,
    // and splits keep everything reachable.
    #[test]
    fn secondary_tree_indexes_by_field_bytes() {
        async fn by(
            tree: BpTree<SecKey>,
            store: &MemStore,
            bound: Bound,
        ) -> Vec<u64> {
            let mut got: Vec<u64> = tree
                .search(store, bound)
                .map(|r| r.unwrap().key())
                .collect()
                .await;
            got.sort_unstable();
            got
        }

        block_on(async {
            let store = MemStore::default();
            let tenant = U48::from(TENANT);
            let mut tree = BpTree::<SecKey>::create(&store, tenant)
                .await
                .unwrap()
                .with_caps(4, 4);

            let key = |f: &str, k: u64| SecKey {
                field: {
                    let mut out = f.as_bytes().to_vec();
                    out.push(0); // the String IndexKey encoding
                    out
                },
                rec: LocalId::from_id(rec(k)),
            };
            for (f, k) in [
                ("pear", 1u64),
                ("apple", 2),
                ("apple", 3),
                ("plum", 4),
                ("apricot", 5),
                ("banana", 6),
                ("apple", 7),
                ("peach", 8),
            ] {
                tree.insert(&store, key(f, k)).await.unwrap();
            }

            // Exact field value → every record holding it.
            assert_eq!(
                by(tree, &store, Bound::Exact(key("apple", 0).field)).await,
                vec![2, 3, 7]
            );
            // Prefix over the field bytes.
            assert_eq!(
                by(tree, &store, Bound::Prefix(b"ap".to_vec())).await,
                vec![2, 3, 5, 7]
            );
            assert_eq!(
                by(tree, &store, Bound::Prefix(b"pe".to_vec())).await,
                vec![1, 8]
            );
            // Remove one duplicate; the others stay.
            assert!(tree.remove(&store, key("apple", 3)).await.unwrap());
            assert_eq!(
                by(tree, &store, Bound::Exact(key("apple", 0).field)).await,
                vec![2, 7]
            );
        });
    }
}
