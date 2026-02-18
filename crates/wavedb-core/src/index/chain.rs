//! The record chain ([RFC 0050]) — [`Chain`], a doubly-linked run of sorted
//! [`Segment`]s with a [`SparseTree`] above it, over a [`Store`].
//!
//! [`segment`](super::segment) is one segment's bytes and in-place edits;
//! [`sparse_tree`](super::sparse_tree) is the index that finds one. This is the
//! structure: locate the segment a key belongs to, edit it, split it when it
//! fills, and keep the index in step — all as one [`Write`] batch the caller
//! folds into its own, so a segment and the entry naming it can never disagree.
//!
//! This file is the handle. Inserting and splitting live in
//! [`chain_insert`](super::chain_insert), removal and merging in
//! [`chain_remove`](super::chain_remove), reading in order from a seek in
//! [`chain_scan`](super::chain_scan), and reaching a segment in
//! [`chain_io`](super::chain_io).
//!
//! ## Endpoints are permanent, and that is the point
//!
//! A B+tree moves its root as it grows, and every move rewrites the `Pivot` —
//! the reason none is left (RFC 0075). A chain does not: a split always hands the **new** id to the interior side and
//! lets the endpoint keep its own — the growth end keeps the newer half, any other
//! segment keeps the lower half. So after a chain's first split its `head` and
//! `tail` never change again, the index root never moves either
//! ([`SparseTree`]), and the `Pivot` is written at creation and then essentially
//! never.
//!
//! The one exception is that first split, when `head == tail` and one of them must
//! become the fresh id. `plan_insert` takes `&mut self` for exactly that, and the
//! caller compares the endpoints afterwards.
//!
//! ## The size band
//!
//! A segment holds `N…2N` entries: it **splits 50/50 on reaching 2N** and merges
//! at `N/2` (in `chain_remove`). The hysteresis of 4 between the triggers is what
//! stops a chain thrashing at a boundary. RFC 0052 has the reasoning, and why the
//! declared `page = N` is a minimum rather than an exact count.
//!
//! [RFC 0050]: https://github.com/wavedb/wavedb/blob/main/rfcs/0050-clustered-record-chains.md

use core::marker::PhantomData;

#[cfg(doc)]
use crate::error::Error;
use crate::error::Result;
use crate::local_id::LocalId;
use crate::store::{Store, Write};
use crate::u48::U48;
use crate::wire::WaveWire;

use super::sec_key::SecKey;
use super::segment::{Lane, Segment, mint_lane_id};
use super::sparse_tree::SparseTree;
use super::{ChainRoots, LogRoots};

/// Default minimum entries per record segment — a segment holds 16…32 records.
///
/// Small on purpose, and for the *write* side: the built-in chain is ordered by
/// the live version's authoring instant, so every save rewrites the growth-end
/// segment whole. A collection that paginates declares `page = N` and pays the
/// bigger rewrite knowingly (RFC 0052).
pub const DEFAULT_SEGMENT_MIN: usize = 16;

/// Default minimum entries per removal-log segment.
///
/// Far larger than [`DEFAULT_SEGMENT_MIN`] because a `dead` entry is an instant
/// and an anchor — about 18 bytes — and the only read it ever serves is a
/// sequential scan back from the tail.
pub const DEFAULT_DEAD_MIN: usize = 256;

/// A chain of segments holding `P` payloads, keyed by [`SecKey`], with a sparse
/// index above it.
///
/// Holds only its endpoints, its index handle and its size band; the segments
/// live in the [`Store`] under `LocalId::to_id(tenant)`.
#[derive(Debug)]
pub struct Chain<P> {
    pub(super) head: LocalId,
    pub(super) tail: LocalId,
    pub(super) tenant: U48,
    pub(super) lane_hash: u64,
    /// `None` for the removal log, which nothing searches — see
    /// [`log_at`](Chain::log_at).
    pub(super) index: Option<SparseTree>,
    pub(super) min: usize,
    _payload: PhantomData<fn() -> P>,
}

// Manual: a derive would demand `P: Copy`, but the
// handle holds only ids and caps — the `PhantomData` is `fn() -> P`, which is
// unconditionally `Copy`. A scan captures the handle in a long-lived closure,
// so this is load-bearing, not cosmetic.
impl<P> Clone for Chain<P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P> Copy for Chain<P> {}

impl<P: WaveWire> Chain<P> {
    /// Open the chain running `head`..`tail` with its index rooted at
    /// `index_root`, for `tenant`, in `lane`'s lane of `struct_hash`.
    #[must_use]
    pub fn at(
        head: LocalId,
        tail: LocalId,
        index_root: LocalId,
        tenant: U48,
        struct_hash: u64,
        lane: Lane,
    ) -> Self {
        Self {
            head,
            tail,
            tenant,
            lane_hash: lane.hash(struct_hash),
            index: Some(SparseTree::at(index_root, tenant, struct_hash)),
            min: DEFAULT_SEGMENT_MIN,
            _payload: PhantomData,
        }
    }

    /// Open an **index-less** chain — the removal log, and only it.
    ///
    /// Nothing ever *searches* `dead` (RFC 0050): a removal appends at the tail,
    /// a catch-up scans back from it, and "is this record dead?" is answered by
    /// the anchor's own [`Metadata`](crate::Metadata). An index would be pure
    /// write cost on every removal, over a structure that grows forever.
    ///
    /// The price is that locating a key walks back from the tail instead of
    /// descending to it — one read for the monotone appends the engine
    /// actually produces, and O(segments) for a key that belongs further back.
    /// Do not open an ordinary record chain this way.
    #[must_use]
    pub fn log_at(
        head: LocalId,
        tail: LocalId,
        tenant: U48,
        struct_hash: u64,
        lane: Lane,
    ) -> Self {
        Self {
            head,
            tail,
            tenant,
            lane_hash: lane.hash(struct_hash),
            index: None,
            min: DEFAULT_DEAD_MIN,
            _payload: PhantomData,
        }
    }

    /// Override the size band's minimum — the developer's `page = N` (RFC 0052),
    /// or [`DEFAULT_DEAD_MIN`] for a removal log.
    #[must_use]
    pub const fn with_min(mut self, min: usize) -> Self {
        self.min = min;
        self
    }

    /// Override the index's node capacity (tests build deep trees cheaply).
    #[must_use]
    pub const fn with_index_cap(mut self, cap: usize) -> Self {
        if let Some(index) = self.index {
            self.index = Some(index.with_cap(cap));
        }
        self
    }

    /// Plan a fresh, empty chain: one empty segment that is both `head` and
    /// `tail`, plus an empty index. The caller commits the writes in its own
    /// batch and persists the three ids in its `Pivot`.
    #[must_use]
    pub fn plan_create(
        tenant: U48,
        struct_hash: u64,
        lane: Lane,
    ) -> (Self, Vec<Write>) {
        let (index, index_write) = SparseTree::plan_create(tenant, struct_hash);
        let (chain, seed) = Self::seed(
            tenant,
            struct_hash,
            lane,
            Some(index),
            DEFAULT_SEGMENT_MIN,
        );
        (chain, vec![seed, index_write])
    }

    /// Plan a fresh, empty **index-less** chain — the removal log. One empty
    /// segment and nothing above it; see [`log_at`](Self::log_at).
    #[must_use]
    pub fn plan_create_log(
        tenant: U48,
        struct_hash: u64,
        lane: Lane,
    ) -> (Self, Vec<Write>) {
        let (chain, seed) =
            Self::seed(tenant, struct_hash, lane, None, DEFAULT_DEAD_MIN);
        (chain, vec![seed])
    }

    /// The shared body of the two creators: mint the one segment that is both
    /// endpoints and hand back the write that persists it.
    fn seed(
        tenant: U48,
        struct_hash: u64,
        lane: Lane,
        index: Option<SparseTree>,
        min: usize,
    ) -> (Self, Write) {
        let lane_hash = lane.hash(struct_hash);
        let only = mint_lane_id(lane_hash);
        let chain = Self {
            head: only,
            tail: only,
            tenant,
            lane_hash,
            index,
            min,
            _payload: PhantomData,
        };
        let seed = chain.put(only, &Segment::new(None, None));
        (chain, seed)
    }

    /// The segment holding the least keys.
    #[must_use]
    pub const fn head(&self) -> LocalId {
        self.head
    }

    /// The segment holding the greatest keys — the growth end of a chain keyed
    /// by an instant.
    #[must_use]
    pub const fn tail(&self) -> LocalId {
        self.tail
    }

    /// The sparse index above this chain, or `None` for the removal log.
    #[must_use]
    pub const fn index(&self) -> Option<SparseTree> {
        self.index
    }

    /// This chain's ids in the shape a `Pivot` holds them.
    ///
    /// The removal log is held as [`LogRoots`] and uses
    /// [`log_roots`](Self::log_roots) instead, so the index named here is always
    /// a real root.
    #[must_use]
    pub fn roots(&self) -> ChainRoots {
        ChainRoots {
            head: self.head,
            tail: self.tail,
            index: self.index.map_or_else(LocalId::default, |ix| ix.root()),
        }
    }

    /// Every entry in key order, materialised.
    ///
    /// Deliberately test-only: it exists so the chain can be checked against the
    /// `recency` and `dead` trees it is being grown alongside. The read path
    /// proper streams segment by segment and never collects a whole collection.
    #[cfg(test)]
    pub(crate) async fn collect<S: Store>(
        &self,
        store: &S,
    ) -> Result<Vec<(SecKey, P)>>
    where
        P: Clone,
    {
        let mut out = Vec::new();
        let mut cursor = Some(self.head);
        while let Some(id) = cursor {
            let seg = self.load(store, id).await?;
            out.extend(seg.entries().cloned());
            cursor = seg.next();
        }
        Ok(out)
    }

    /// This chain's endpoints in the shape a `Pivot` holds a removal log.
    #[must_use]
    pub const fn log_roots(&self) -> LogRoots {
        LogRoots {
            head: self.head,
            tail: self.tail,
        }
    }

    /// The payload filed under `key`, if the chain holds it.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::ChainNodeMissing`] on a dangling
    /// pointer, or [`Error::LaneBadTag`] when a pointer resolves to a foreign
    /// value.
    pub async fn get<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<Option<P>>
    where
        P: Clone,
    {
        let (_, seg) = self.locate(store, key).await?;
        Ok(seg.get(key).cloned())
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::Chain;
    use crate::index::mem_store::MemStore;
    use crate::index::sec_key::SecKey;
    use crate::index::segment::Lane;
    use crate::local_id::LocalId;
    use crate::store::Store;
    use crate::u48::U48;

    // Insert *and* remove are exercised here rather than split across the two
    // modules: they are one structure, and every assertion below is about the
    // invariant they jointly maintain.

    const TYPE_HASH: u64 = 0x0C4A_1111_2222_3333;
    /// Small band (2N = 8, N/2 = 2) so a handful of inserts forces real splits.
    const MIN: usize = 4;

    fn tenant() -> U48 {
        U48::new(3).unwrap()
    }

    fn key(value: u64) -> SecKey {
        SecKey {
            field: value.to_be_bytes().to_vec(),
            rec: LocalId::new(value, false, 2),
        }
    }

    async fn fresh(store: &MemStore) -> Chain<Vec<u8>> {
        let (chain, writes) =
            Chain::<Vec<u8>>::plan_create(tenant(), TYPE_HASH, Lane::Records);
        store.apply(&writes).await.unwrap();
        chain.with_min(MIN).with_index_cap(3)
    }

    async fn insert(store: &MemStore, chain: &mut Chain<Vec<u8>>, value: u64) {
        let writes = chain
            .plan_insert(store, key(value), value.to_be_bytes().to_vec())
            .await
            .unwrap();
        store.apply(&writes).await.unwrap();
    }

    async fn remove(store: &MemStore, chain: &mut Chain<Vec<u8>>, value: u64) {
        let Some(writes) = chain.plan_remove(store, &key(value)).await.unwrap()
        else {
            panic!("{value} was not in the chain");
        };
        store.apply(&writes).await.unwrap();
    }

    /// Walk the whole chain asserting every invariant it claims, and return the
    /// keys in walk order.
    ///
    /// - links are consistent in both directions and the walk ends at `tail`;
    /// - keys ascend strictly across segment boundaries;
    /// - every segment sits inside the `N/2 … 2N` band (a lone segment is
    ///   exempt: it is the chain's shell);
    /// - the index names every segment, with the right id and the right count;
    /// - the store holds nothing but the walked segments and the live index
    ///   nodes — no orphans.
    async fn check(store: &MemStore, chain: &Chain<Vec<u8>>) -> Vec<u64> {
        let mut keys = Vec::new();
        let mut ids = Vec::new();
        let mut expect_prev = None;
        let mut cursor = Some(chain.head());
        let lone = chain.head() == chain.tail();

        while let Some(id) = cursor {
            let seg = chain.load(store, id).await.unwrap();
            assert_eq!(seg.prev(), expect_prev, "prev link broken at {id:?}");
            assert!(
                lone || seg.len() > chain.min() / 2,
                "segment {id:?} starved: {} entries",
                seg.len()
            );
            assert!(
                seg.len() < chain.min() * 2,
                "segment {id:?} over the band: {} entries",
                seg.len()
            );
            if let Some(index) = chain.index()
                && let Some(first) = seg.first_key()
            {
                let slot = index
                    .find(store, first)
                    .await
                    .unwrap()
                    .expect("segment absent from the index");
                assert_eq!(slot.seg, id, "index names the wrong segment");
                assert_eq!(
                    slot.count,
                    seg.len() as u64,
                    "index count is stale"
                );
            }
            for (k, _) in seg.entries() {
                let bytes: [u8; 8] = k.field.as_slice().try_into().unwrap();
                keys.push(u64::from_be_bytes(bytes));
            }
            ids.push(id);
            expect_prev = Some(id);
            cursor = seg.next();
        }

        assert_eq!(
            ids.last().copied(),
            Some(chain.tail()),
            "the forward walk did not end at the tail"
        );
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "keys not strictly ascending: {keys:?}"
        );
        // An index-less chain (the removal log) has neither a total to compare
        // nor nodes to account for — the segments are the whole structure.
        let index_nodes = if let Some(index) = chain.index() {
            assert_eq!(
                index.total(store).await.unwrap(),
                keys.len() as u64,
                "the index's total disagrees with the chain"
            );
            index.reachable(store).await.0
        } else {
            0
        };
        assert_eq!(
            store.len(),
            ids.len() + index_nodes,
            "orphan values left in the store"
        );
        keys
    }

    /// How many segments the chain currently holds.
    async fn segments(store: &MemStore, chain: &Chain<Vec<u8>>) -> usize {
        let mut n = 0;
        let mut cursor = Some(chain.head());
        while let Some(id) = cursor {
            n += 1;
            cursor = chain.load(store, id).await.unwrap().next();
        }
        n
    }

    /// A removal-log chain: no index, so `locate` walks back from the tail.
    async fn fresh_log(store: &MemStore) -> Chain<Vec<u8>> {
        let (chain, writes) =
            Chain::<Vec<u8>>::plan_create_log(tenant(), TYPE_HASH, Lane::Dead);
        store.apply(&writes).await.unwrap();
        chain.with_min(MIN)
    }

    #[test]
    fn an_index_less_log_orders_and_splits_like_any_chain() {
        block_on(async {
            // The `dead` lane: nothing searches it, so it carries no index and
            // its segments are the whole structure. Everything else — the band,
            // the 50/50 split, the endpoint discipline — is unchanged.
            let store = MemStore::default();
            let mut log = fresh_log(&store).await;
            assert!(log.index().is_none(), "a log must carry no index");
            let tail = log.tail();

            for v in 1..=30u64 {
                insert(&store, &mut log, v).await;
                check(&store, &log).await;
            }
            assert_eq!(check(&store, &log).await, (1..=30).collect::<Vec<_>>());
            assert!(segments(&store, &log).await > 3, "the log never split");
            assert_eq!(log.tail(), tail, "an append moved the log's tail");
        });
    }

    #[test]
    fn an_index_less_locate_still_finds_an_out_of_order_key() {
        block_on(async {
            // Appends are monotone in practice — the collection's instant floor
            // guarantees it — so the backward walk stops at the tail. This pins
            // that it stays *correct* when a key belongs further back, rather
            // than dropping it into the tail and breaking the order.
            let store = MemStore::default();
            let mut log = fresh_log(&store).await;
            for v in (1..=30u64).map(|v| v * 10) {
                insert(&store, &mut log, v).await;
            }
            insert(&store, &mut log, 55).await;
            let keys = check(&store, &log).await;
            let at = keys.iter().position(|&k| k == 55).expect("55 is missing");
            assert_eq!((keys[at - 1], keys[at + 1]), (50, 60));

            assert_eq!(
                log.get(&store, &key(55)).await.unwrap(),
                Some(55u64.to_be_bytes().to_vec())
            );
        });
    }

    #[test]
    fn a_fresh_chain_is_one_empty_shell() {
        block_on(async {
            let store = MemStore::default();
            let chain = fresh(&store).await;
            assert_eq!(chain.head(), chain.tail());
            let observed = check(&store, &chain).await;
            assert!(observed.is_empty(), "{observed:?}");
        });
    }

    #[test]
    fn entries_come_out_in_key_order_whatever_the_arrival_order() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in [50u64, 10, 90, 30, 70, 20, 80, 60, 40, 5, 95, 15] {
                insert(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            let mut want =
                vec![5u64, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 95];
            want.sort_unstable();
            assert_eq!(check(&store, &chain).await, want);
        });
    }

    #[test]
    fn a_tail_split_keeps_the_tail_and_moves_the_head_exactly_once() {
        block_on(async {
            // Ascending arrivals: every insert lands at the growth end, which is
            // the built-in chain's shape. The tail id must never move; the head
            // moves only on the very first split, when head == tail.
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            let tail = chain.tail();
            let first_head = chain.head();

            insert(&store, &mut chain, 1).await;
            let mut moves = 0;
            let mut head = chain.head();
            for v in 2..=40u64 {
                insert(&store, &mut chain, v).await;
                check(&store, &chain).await;
                assert_eq!(chain.tail(), tail, "the tail id moved at {v}");
                if chain.head() != head {
                    moves += 1;
                    head = chain.head();
                }
            }
            assert_eq!(moves, 1, "the head moved {moves} times, not once");
            assert_ne!(
                chain.head(),
                first_head,
                "the first split never happened"
            );
            assert!(segments(&store, &chain).await > 4, "no splits occurred");
        });
    }

    #[test]
    fn an_interior_split_moves_neither_endpoint() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            // Build a chain of several segments first.
            for v in 1..=24u64 {
                insert(&store, &mut chain, v * 10).await;
            }
            let (head, tail) = (chain.head(), chain.tail());
            let before = segments(&store, &chain).await;

            // Now fill one interior segment by inserting between existing keys.
            for v in 1..=9u64 {
                insert(&store, &mut chain, 100 + v).await;
                check(&store, &chain).await;
            }
            assert!(
                segments(&store, &chain).await > before,
                "no interior split happened"
            );
            assert_eq!(chain.head(), head, "an interior split moved the head");
            assert_eq!(chain.tail(), tail, "an interior split moved the tail");
        });
    }

    #[test]
    fn a_stored_payload_comes_back_verbatim() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=30u64 {
                insert(&store, &mut chain, v).await;
            }
            for v in 1..=30u64 {
                assert_eq!(
                    chain.get(&store, &key(v)).await.unwrap(),
                    Some(v.to_be_bytes().to_vec()),
                    "payload for {v} came back wrong"
                );
            }
            assert_eq!(chain.get(&store, &key(999)).await.unwrap(), None);
        });
    }

    #[test]
    fn inserting_an_existing_key_replaces_it_without_growing_the_chain() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=10u64 {
                insert(&store, &mut chain, v).await;
            }
            let before = check(&store, &chain).await;
            let writes =
                chain.plan_insert(&store, key(5), vec![0xFF]).await.unwrap();
            store.apply(&writes).await.unwrap();

            assert_eq!(check(&store, &chain).await, before, "the chain grew");
            assert_eq!(
                chain.get(&store, &key(5)).await.unwrap(),
                Some(vec![0xFF])
            );
        });
    }

    #[test]
    fn a_starved_segment_merges_with_its_neighbour() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=24u64 {
                insert(&store, &mut chain, v).await;
            }
            let before = segments(&store, &chain).await;
            assert!(before >= 3, "need several segments, got {before}");

            // Drain the middle of the chain; segments must fold together rather
            // than linger under the band.
            for v in 9..=16u64 {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            assert!(
                segments(&store, &chain).await < before,
                "nothing merged: still {before} segments"
            );
        });
    }

    #[test]
    fn a_merge_that_would_breach_the_band_redistributes_instead() {
        block_on(async {
            // The starved side and its neighbour must together exceed 2N, or
            // folding them into one is legal and merging is the right answer.
            // Eight ascending keys split into two segments of N; three more land
            // in the second, taking it to 7. Draining the first to N/2 then
            // leaves 2 + 7 = 9 > 2N, so the pair must share out instead of fold.
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=8u64 {
                insert(&store, &mut chain, v * 10).await;
            }
            for v in 1..=3u64 {
                insert(&store, &mut chain, 50 + v).await;
            }
            assert_eq!(segments(&store, &chain).await, 2, "expected one split");

            remove(&store, &mut chain, 10).await;
            check(&store, &chain).await;
            remove(&store, &mut chain, 20).await;
            let keys = check(&store, &chain).await;

            assert_eq!(
                segments(&store, &chain).await,
                2,
                "a redistribute must not change the segment count"
            );
            // `check` already asserts both sides are inside the band; this pins
            // that nothing was lost in the shuffle.
            assert_eq!(keys, vec![30, 40, 50, 51, 52, 53, 60, 70, 80]);
        });
    }

    #[test]
    fn emptying_the_chain_leaves_one_shell_and_no_orphans() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=40u64 {
                insert(&store, &mut chain, v).await;
            }
            for v in 1..=40u64 {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            let observed = check(&store, &chain).await;
            assert!(observed.is_empty(), "{observed:?}");
            assert_eq!(segments(&store, &chain).await, 1, "shell not kept");
            assert_eq!(chain.head(), chain.tail());
            let index = chain.index().expect("an indexed chain");
            assert_eq!(index.total(&store).await.unwrap(), 0);
        });
    }

    #[test]
    fn removing_in_reverse_also_lands_on_one_shell() {
        block_on(async {
            // The mirror of the previous test: draining from the tail exercises
            // the other merge direction (the survivor absorbing leftward).
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=40u64 {
                insert(&store, &mut chain, v).await;
            }
            for v in (1..=40u64).rev() {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            let observed = check(&store, &chain).await;
            assert!(observed.is_empty(), "{observed:?}");
            assert_eq!(segments(&store, &chain).await, 1);
        });
    }

    #[test]
    fn a_merge_never_deletes_an_endpoint() {
        block_on(async {
            // Draining from the tail end starves the tail over and over, and
            // every one of those merges involves it. The tail must be the
            // survivor: if the absorbed side were chosen by position alone, each
            // merge would move the tail id and force a `Pivot` rewrite — the
            // cost the endpoint discipline exists to avoid.
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=40u64 {
                insert(&store, &mut chain, v).await;
            }
            let (head, tail) = (chain.head(), chain.tail());

            // Stop well before the chain would collapse to a single segment,
            // which is the one case where an endpoint legitimately moves.
            for v in (11..=40u64).rev() {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
                assert_eq!(chain.tail(), tail, "a merge moved the tail at {v}");
                assert_eq!(chain.head(), head, "a merge moved the head at {v}");
            }
            assert!(segments(&store, &chain).await > 1, "collapsed too far");
        });
    }

    #[test]
    fn removing_an_absent_key_plans_nothing() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=10u64 {
                insert(&store, &mut chain, v).await;
            }
            assert!(
                chain.plan_remove(&store, &key(99)).await.unwrap().is_none(),
                "an absent key planned work"
            );
        });
    }

    #[test]
    fn a_dangling_head_is_a_typed_fault_not_a_panic() {
        block_on(async {
            let store = MemStore::default();
            // Never committed: the handle names segments that do not exist.
            let (chain, _) = Chain::<Vec<u8>>::plan_create(
                tenant(),
                TYPE_HASH,
                Lane::Records,
            );
            assert!(matches!(
                chain.get(&store, &key(1)).await,
                Err(crate::error::Error::ChainNodeMissing(_))
            ));
        });
    }
}
