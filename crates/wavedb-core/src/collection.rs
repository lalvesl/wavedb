//! [`Collection`] — the typed, `Store`-generic engine behind a NonUnique type's
//! generated collection API, plus the Unique-anchor helpers.
//!
//! `#[wavedb(NonUnique)]` emits `Todo::collection(pivot_id, tenant)` /
//! `Todo::create_pivot(store, tenant)` wrappers over this type, so application
//! code drives inserts, removals, and walks through the collection — never a
//! raw `BpTree`, which stays an index-layer internal:
//!
//! ```text
//! let todos = Todo::create_pivot(&store, tenant).await?;   // once, explicit
//! let col   = Todo::collection(todos, tenant);              // cheap handle
//! let id    = col.insert(&store, &Todo { .. }).await?;      // one atomic batch
//! col.remove(&store, id).await?;                            // leaves the chain
//! col.all(&store)                                           // Stream<Result<Todo>>
//! ```
//!
//! ## Record envelope & history
//!
//! Every stored value starts `[STRUCT_HASH (8 B LE)]` (backends route by it,
//! decode verifies it); user records additionally carry their
//! [`Metadata`](crate::Metadata) (see [`crate::record`]). Saving never destroys old bytes — `save` archives
//! the superseded version and links the modification chain;
//! [`history`](Collection::history) walks it newest-first.
//!
//! ## Atomicity
//!
//! Each mutating op commits **one** [`Store::apply`] batch: the record write
//! (plus its archived predecessor and chain relinks on a save), the record
//! chain segments and sparse-index nodes it touched, every touched secondary
//! B+tree node, and — when a root moved — the rewritten `Pivot` record. A
//! crash replays the whole batch or none of it.

use std::marker::PhantomData;

use crate::error::{Error, Result};
// Only the test-only `load_record` still names an `Id` here; the addressing
// half moved to `collection_roots`.
#[cfg(test)]
use crate::id::Id;
use crate::index::Pivot;
use crate::local_id::LocalId;
use crate::record::{decode_envelope, encode_envelope};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;
use crate::u48::U48;

// The `#[wavedb]` macro reaches the Unique-anchor ops through this module's
// path; the implementation lives with the envelope in [`crate::record`].
pub use crate::record::{
    adopt_unique, get_unique, save_unique, save_unique_as, unique_history,
};

// ---- Collection ---------------------------------------------------------------

/// The typed handle into one NonUnique collection.
///
/// Holds a `PivotId` plus the tenant, nothing else — cheap to construct per
/// call. Every op loads the `Pivot` record fresh, so concurrent root moves are
/// always observed.
#[derive(Debug)]
pub struct Collection<T: NonUniqueStruct> {
    pivot: LocalId,
    // Read by `collection_roots`, which opens the structures this handle names.
    pub(crate) tenant: U48,
    user: U48,
    pub(crate) leaf_cap: usize,
    pub(crate) internal_cap: usize,
    _record: PhantomData<fn() -> T>,
}

// Manual impls: a derive would demand `T: Clone`/`T: Copy`, but the handle
// holds only ids (the `PhantomData` is `fn() -> T`, unconditionally `Copy`).
impl<T: NonUniqueStruct> Clone for Collection<T> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<T: NonUniqueStruct> Copy for Collection<T> {}

impl<T: NonUniqueStruct> Collection<T> {
    /// The handle for the collection referenced by `pivot` under `tenant`.
    /// Authorship (`Metadata.user`) defaults to the tenant — the engine-local
    /// and B2C identity; [`stamped_by`](Self::stamped_by) overrides it.
    #[must_use]
    pub const fn at(pivot: LocalId, tenant: U48) -> Self {
        Self {
            pivot,
            tenant,
            user: tenant,
            leaf_cap: crate::index::DEFAULT_LEAF_CAP,
            internal_cap: crate::index::DEFAULT_INTERNAL_CAP,
            _record: PhantomData,
        }
    }

    /// Stamp mutations' `Metadata.user` with the verified `user` (M8) instead
    /// of the tenant.
    #[must_use]
    pub const fn stamped_by(mut self, user: U48) -> Self {
        self.user = user;
        self
    }

    /// Override the B+tree node capacities (small caps make deep trees cheap
    /// to build in tests; production uses the defaults). Use one configuration
    /// for a collection's whole lifetime.
    #[must_use]
    pub const fn with_caps(mut self, leaf: usize, internal: usize) -> Self {
        self.leaf_cap = leaf;
        self.internal_cap = internal;
        self
    }

    /// The tenant this handle is scoped to.
    pub(crate) const fn tenant(&self) -> U48 {
        self.tenant
    }

    /// The authorship stamped in mutations' `Metadata.user`.
    pub(crate) const fn user(&self) -> U48 {
        self.user
    }

    /// The `LocalId` of this collection's `Pivot` record.
    #[must_use]
    pub const fn pivot(&self) -> LocalId {
        self.pivot
    }

    /// Load and decode the `Pivot` record.
    pub(crate) async fn load_pivot<S: Store>(
        &self,
        store: &S,
    ) -> Result<T::Pivot> {
        let bytes = store
            .get_of(T::Pivot::STRUCT_HASH, self.pivot.to_id(self.tenant))
            .await?
            .ok_or(Error::PivotMissing(self.pivot))?;
        decode_envelope(T::Pivot::STRUCT_HASH, &bytes)
    }

    /// A `Put` rewriting the `Pivot` record with moved roots.
    pub(crate) fn pivot_rewrite(&self, pivot: &T::Pivot) -> Write {
        Write::Put(
            self.pivot.to_id(self.tenant),
            encode_envelope(T::Pivot::STRUCT_HASH, pivot),
        )
    }

    /// Load and decode the record at `id` (metadata + body), failing if it is
    /// gone.
    #[cfg(test)]
    pub(crate) async fn load_record<S: Store>(
        &self,
        store: &S,
        id: Id,
    ) -> Result<(crate::metadata::Metadata, T)> {
        let bytes = store
            .get_of(T::STRUCT_HASH, id)
            .await?
            .ok_or(Error::RecordMissing(id))?;
        crate::record::decode_record(T::STRUCT_HASH, &bytes)
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use futures::executor::block_on;

    use super::{Collection, get_unique, save_unique};
    use crate::error::Error;
    use crate::index::mem_store::MemStore;
    use crate::index::{ChainRoots, LogRoots, Pivot, Roots};
    use crate::local_id::LocalId;
    use crate::metadata::Succession;
    use crate::permission::PermissionRef;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::WaveWire;

    const TENANT: u32 = 9;

    // Hand-rolled fixture — core can't use the `#[wavedb]` proc-macro (it
    // lives downstream), so this is exactly what the macro generates.
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Doc {
        n: u64,
    }

    impl WaveDbStruct for Doc {
        const STRUCT_HASH: u64 = 0xD0C_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct DocPivot {
        recency: ChainRoots,
        removals: LogRoots,
        permission: Option<PermissionRef>,
    }

    impl Pivot for DocPivot {
        const STRUCT_HASH: u64 = 0xD0C_0002;
        fn secondaries(&self) -> &[LocalId] {
            &[]
        }
        fn recency(&self) -> ChainRoots {
            self.recency
        }
        fn removals(&self) -> LogRoots {
            self.removals
        }
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
        fn replace_roots(&self, roots: Roots<'_>) -> Self {
            Self {
                recency: roots.recency,
                removals: roots.removals,
                permission: self.permission.clone(),
            }
        }
    }

    impl NonUniqueStruct for Doc {
        type Pivot = DocPivot;
    }

    // A record with one secondary index (on `label`) — exactly the shape the
    // macro generates for `#[wavedb::pivot(label)]`.
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Tagged {
        label: String,
        n: u64,
    }

    impl WaveDbStruct for Tagged {
        const STRUCT_HASH: u64 = 0x7A6_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct TaggedPivot {
        recency: ChainRoots,
        removals: LogRoots,
        secondaries: [LocalId; 1],
        permission: Option<PermissionRef>,
    }

    impl Pivot for TaggedPivot {
        const STRUCT_HASH: u64 = 0x7A6_0002;
        fn secondaries(&self) -> &[LocalId] {
            &self.secondaries
        }
        fn recency(&self) -> ChainRoots {
            self.recency
        }
        fn removals(&self) -> LogRoots {
            self.removals
        }
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
        fn replace_roots(&self, roots: Roots<'_>) -> Self {
            let mut s = self.secondaries;
            s.copy_from_slice(roots.secondaries);
            Self {
                recency: roots.recency,
                removals: roots.removals,
                secondaries: s,
                permission: self.permission.clone(),
            }
        }
    }

    impl NonUniqueStruct for Tagged {
        type Pivot = TaggedPivot;
        const NUM_SECONDARIES: usize = 1;
        fn secondary_key(&self, index: usize) -> Vec<u8> {
            match index {
                0 => crate::index::IndexKey::key_bytes(&self.label),
                _ => Vec::new(),
            }
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Settings {
        volume: u64,
    }

    impl WaveDbStruct for Settings {
        const STRUCT_HASH: u64 = 0x5E77_0001;
        const SHAPE: Shape = Shape::Unique;
        type PivotId = ();
    }

    fn tenant() -> U48 {
        U48::from(TENANT)
    }

    #[test]
    fn unique_save_is_upsert_and_get_roundtrips() {
        block_on(async {
            let store = MemStore::default();
            assert_eq!(
                get_unique::<Settings, _>(&store, tenant()).await.unwrap(),
                None
            );
            save_unique(&store, tenant(), &Settings { volume: 3 })
                .await
                .unwrap();
            save_unique(&store, tenant(), &Settings { volume: 7 })
                .await
                .unwrap(); // overwrite, same anchor
            assert_eq!(
                get_unique::<Settings, _>(&store, tenant()).await.unwrap(),
                Some(Settings { volume: 7 })
            );
            // A different tenant sees nothing.
            assert_eq!(
                get_unique::<Settings, _>(&store, U48::from(TENANT + 1))
                    .await
                    .unwrap(),
                None
            );
        });
    }

    #[test]
    fn insert_walk_save_remove_lifecycle() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());

            let a = col.insert(&store, &Doc { n: 1 }).await.unwrap();
            let b = col.insert(&store, &Doc { n: 2 }).await.unwrap();
            let c = col.insert(&store, &Doc { n: 3 }).await.unwrap();

            let walked: Vec<(crate::id::Id, Doc)> =
                col.all(&store).try_collect().await.unwrap();
            // The walk comes off the record chain, which is ordered by each
            // record's live version's authoring instant — so it is **most
            // recently written first**, the reverse of insertion order here
            // because nothing has been saved yet (RFC 0050).
            assert_eq!(
                walked.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                vec![c, b, a],
                "walk must be newest-first"
            );
            assert_eq!(walked[0].1, Doc { n: 3 });

            // Update in place: same Id, new bytes, still one walk entry.
            col.save(&store, b, &Doc { n: 22 }).await.unwrap();
            assert_eq!(col.get(&store, b).await.unwrap(), Some(Doc { n: 22 }));

            // Remove drops it from the walk but keeps the bytes (history).
            assert!(col.remove(&store, b).await.unwrap());
            assert!(!col.remove(&store, b).await.unwrap(), "already dead");
            let after: Vec<(crate::id::Id, Doc)> =
                col.all(&store).try_collect().await.unwrap();
            assert_eq!(
                after.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                vec![c, a]
            );
            assert_eq!(
                col.get(&store, b).await.unwrap(),
                Some(Doc { n: 22 }),
                "removed record bytes must survive"
            );
        });
    }

    #[test]
    fn root_moves_rewrite_the_pivot_and_survive_reopen() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant()).with_caps(4, 4);

            // Enough inserts to split the current tree's root repeatedly.
            let mut ids = Vec::new();
            for n in 0..64u64 {
                ids.push(col.insert(&store, &Doc { n }).await.unwrap());
            }

            // A fresh handle (same PivotId) must see everything: the moved
            // root came from the rewritten Pivot record, not handle state.
            let reopened =
                Collection::<Doc>::at(pivot, tenant()).with_caps(4, 4);
            let walked: Vec<(crate::id::Id, Doc)> =
                reopened.all(&store).try_collect().await.unwrap();
            assert_eq!(walked.len(), 64);
            // Newest-first: the chain's order survives its own splits, and the
            // reopened handle reaches it through the rewritten `Pivot`.
            let mut newest_first = ids.clone();
            newest_first.reverse();
            assert_eq!(
                walked.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                newest_first,
                "chain order lost across splits"
            );

            // Remove everything; merges collapse roots — pivot keeps up.
            for id in &ids {
                assert!(reopened.remove(&store, *id).await.unwrap());
            }
            let empty: Vec<(crate::id::Id, Doc)> =
                reopened.all(&store).try_collect().await.unwrap();
            assert!(empty.is_empty(), "{empty:?}");
        });
    }

    // Saving never destroys old bytes: each save archives the superseded
    // version and links the chain; `history` walks it newest-first, and the
    // forward pointers land where the design says.
    #[test]
    fn save_archives_versions_and_history_walks() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());

            let id = col.insert(&store, &Doc { n: 1 }).await.unwrap();
            col.save(&store, id, &Doc { n: 2 }).await.unwrap();
            col.save(&store, id, &Doc { n: 3 }).await.unwrap();

            let versions: Vec<(crate::metadata::Metadata, Doc)> =
                col.history(&store, id).try_collect().await.unwrap();
            assert_eq!(
                versions.iter().map(|(_, d)| d.n).collect::<Vec<_>>(),
                vec![3, 2, 1],
                "history must walk newest-first"
            );

            // Chain shape (instants, not addresses): the live version
            // authored at t3 chains back to t2; each archive's forward
            // instant names its successor; the first version, authored at
            // the anchor's own key, has no predecessor.
            let (live_meta, _) = &versions[0];
            let (v2_meta, _) = &versions[1];
            let (v1_meta, _) = &versions[2];
            let Succession::CreatedAt(t3) = live_meta.succession else {
                panic!("the live record must carry CreatedAt");
            };
            let t2 = live_meta.previous.expect("live chains back");
            assert_eq!(v2_meta.succession, Succession::Next(t3));
            let t1 = v2_meta.previous.expect("middle version chains back");
            assert_eq!(v1_meta.succession, Succession::Next(t2));
            assert!(v1_meta.previous.is_none(), "first version");
            assert!(t1 < t2 && t2 < t3, "authoring instants strictly rise");
            assert_eq!(
                t1,
                id.key(),
                "the first authoring instant is the anchor's own key"
            );
            assert_eq!(
                live_meta.pivot_id,
                Some(pivot),
                "insert stamps the pivot back-link, saves carry it"
            );

            // The live read is unaffected; `get` still yields the value only.
            assert_eq!(col.get(&store, id).await.unwrap(), Some(Doc { n: 3 }));
        });
    }

    // Unique anchors chain the same way through save_unique/unique_history.
    #[test]
    fn unique_save_chains_and_history_walks() {
        use crate::record::unique_history;

        block_on(async {
            let store = MemStore::default();
            // Never saved: empty history, no error.
            let none: Vec<(crate::metadata::Metadata, Settings)> =
                unique_history(&store, tenant())
                    .try_collect()
                    .await
                    .unwrap();
            assert!(none.is_empty(), "{none:?}");

            for volume in [1u64, 2, 3] {
                save_unique(&store, tenant(), &Settings { volume })
                    .await
                    .unwrap();
            }
            assert_eq!(
                get_unique::<Settings, _>(&store, tenant()).await.unwrap(),
                Some(Settings { volume: 3 })
            );
            let versions: Vec<(crate::metadata::Metadata, Settings)> =
                unique_history(&store, tenant())
                    .try_collect()
                    .await
                    .unwrap();
            assert_eq!(
                versions.iter().map(|(_, s)| s.volume).collect::<Vec<_>>(),
                vec![3, 2, 1]
            );
            assert_eq!(versions[0].1.volume, 3);
            assert!(versions.last().unwrap().0.previous.is_none());
        });
    }

    // The whole secondary-index lifecycle over the derived-equivalent shape:
    // insert indexes, save re-keys only changed fields, remove de-indexes,
    // duplicates coexist, root moves land in the rewritten pivot.
    #[test]
    fn secondary_index_lifecycle() {
        use crate::index::IndexKey;

        async fn by_label(
            col: Collection<Tagged>,
            store: &MemStore,
            label: &str,
        ) -> Vec<u64> {
            let mut ns: Vec<u64> = col
                .search_by(
                    store,
                    0,
                    crate::index::Bound::Exact(label.key_bytes()),
                )
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, t)| t.n)
                .collect();
            ns.sort_unstable();
            ns
        }

        block_on(async {
            let store = MemStore::default();
            let pivot = Collection::<Tagged>::create(&store, tenant())
                .await
                .unwrap();
            let col = Collection::<Tagged>::at(pivot, tenant()).with_caps(4, 4);

            let mut ids = Vec::new();
            for (label, n) in
                [("red", 1u64), ("blue", 2), ("red", 3), ("green", 4)]
            {
                let t = Tagged {
                    label: label.into(),
                    n,
                };
                ids.push(col.insert(&store, &t).await.unwrap());
            }
            assert_eq!(by_label(col, &store, "red").await, vec![1, 3]);
            assert_eq!(by_label(col, &store, "blue").await, vec![2]);

            // save with a changed field re-keys that index.
            col.save(
                &store,
                ids[0],
                &Tagged {
                    label: "blue".into(),
                    n: 1,
                },
            )
            .await
            .unwrap();
            assert_eq!(by_label(col, &store, "red").await, vec![3]);
            assert_eq!(by_label(col, &store, "blue").await, vec![1, 2]);

            // save with the field unchanged only rewrites the record.
            col.save(
                &store,
                ids[1],
                &Tagged {
                    label: "blue".into(),
                    n: 22,
                },
            )
            .await
            .unwrap();
            assert_eq!(by_label(col, &store, "blue").await, vec![1, 22]);

            // remove de-indexes the record from its secondary too.
            assert!(col.remove(&store, ids[2]).await.unwrap());
            assert_eq!(by_label(col, &store, "red").await, Vec::<u64>::new());

            // Undeclared index = typed error, not a panic.
            let err = col
                .search_by(&store, 1, crate::index::Bound::All)
                .try_collect::<Vec<_>>()
                .await
                .unwrap_err();
            assert!(matches!(err, Error::SecondaryIndexOutOfRange(1)));

            // Enough inserts to split the secondary tree's root; a fresh
            // handle (same PivotId) sees them through the rewritten pivot.
            for n in 0..40u64 {
                col.insert(
                    &store,
                    &Tagged {
                        label: "bulk".into(),
                        n,
                    },
                )
                .await
                .unwrap();
            }
            let fresh =
                Collection::<Tagged>::at(pivot, tenant()).with_caps(4, 4);
            assert_eq!(
                by_label(fresh, &store, "bulk").await,
                (0..40).collect::<Vec<u64>>()
            );
        });
    }

    // Two plans reading one live version derive the SAME archive slot; the
    // Expect guard must refuse the loser whole — a lost race may cost a
    // retry, never history.
    #[test]
    fn concurrent_saves_of_one_anchor_conflict_not_overwrite() {
        use futures::TryStreamExt;

        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());
            let id = col.insert(&store, &Doc { n: 1 }).await.unwrap();

            // A plan against the current live version…
            let plan = crate::record::SavePlan {
                hash: Doc::STRUCT_HASH,
                shape: Doc::SHAPE,
                live_id: id,
                tenant: tenant(),
                user: tenant(),
                pivot_id: Some(pivot),
                imposed: None,
                floor: 0,
                revives: false,
            };
            let (stale, _, _) = crate::record::plan_chained_save::<Doc, _>(
                &store,
                &plan,
                &Doc { n: 99 },
            )
            .await
            .unwrap();
            // …loses the race to a committed save…
            col.save(&store, id, &Doc { n: 2 }).await.unwrap();
            // …and must refuse whole at commit time.
            let err = crate::store::Store::apply(&store, &stale)
                .await
                .unwrap_err();
            assert!(matches!(err, Error::Conflict(at) if at == id));
            assert_eq!(col.get(&store, id).await.unwrap(), Some(Doc { n: 2 }));
            let versions: Vec<(crate::metadata::Metadata, Doc)> =
                col.history(&store, id).try_collect().await.unwrap();
            assert_eq!(
                versions.iter().map(|(_, d)| d.n).collect::<Vec<_>>(),
                vec![2, 1],
                "history must hold exactly the committed versions"
            );
        });
    }

    // The recency log holds exactly one entry per LIVING record, keyed by
    // its live version's instant; the dead log records removals keyed by
    // removal time. Between them, a tail scan from any cursor is precisely
    // "everything that changed since".
    #[test]
    fn the_record_chain_agrees_with_the_anchors_it_derives_from() {
        block_on(async {
            // The chain holds a *copy* of every living record. At every step
            // of a mixed workload that copy must be byte-identical to the
            // anchor it came from, and the two answers to "does this record
            // live?" — chain membership and `Metadata.removed` — must match.
            //
            // Until phase 5c this also compared the chain against the
            // `recency`/`dead` B+trees written beside it. Those are gone; the
            // anchors are what remains to check against, and they are the
            // authority anyway.
            let store = MemStore::default();
            let pivot_id =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot_id, tenant());

            let mut ids = Vec::new();
            for n in 1..=12u64 {
                ids.push(col.insert(&store, &Doc { n }).await.unwrap());
                agree(&store, &col).await;
            }
            // Saves relocate their records; removals evict them.
            for (i, id) in ids.iter().enumerate() {
                if i % 3 == 0 {
                    col.save(&store, *id, &Doc { n: 900 + i as u64 })
                        .await
                        .unwrap();
                } else if i % 3 == 1 {
                    assert!(col.remove(&store, *id).await.unwrap());
                }
                agree(&store, &col).await;
            }
            // Something must actually have happened, or the assertions above
            // were comparing two empty structures.
            let pivot = col.load_pivot(&store).await.unwrap();
            let recency =
                col.recency_chain(&pivot).collect(&store).await.unwrap();
            assert!(!recency.is_empty(), "{recency:?}");
            let dead = col.dead_log(&pivot).collect(&store).await.unwrap();
            assert!(!dead.is_empty(), "{dead:?}");
        });
    }

    /// Assert every inline chain record equals the bytes at its own anchor,
    /// and that chain membership and `Metadata.removed` tell the same story.
    async fn agree(store: &MemStore, col: &Collection<Doc>) {
        use crate::collection_read::Anchor;
        use crate::local_id::LocalId;
        use crate::store::Store;

        let pivot = col.load_pivot(store).await.unwrap();
        let chain = col.recency_chain(&pivot).collect(store).await.unwrap();
        let mut in_chain: Vec<LocalId> = Vec::with_capacity(chain.len());
        for (key, ()) in &chain {
            let id = key.rec.to_id(col.tenant());
            Store::get(store, id)
                .await
                .unwrap()
                .expect("the chain names a record that is not at its anchor");
            // The two answers to "does this live?" — the structure's and the
            // record's own — are what every read path now depends on agreeing.
            assert!(
                matches!(
                    col.anchor(store, id).await.unwrap(),
                    Anchor::Living(..)
                ),
                "a record the chain holds reads dead at its anchor"
            );
            in_chain.push(key.rec);
        }

        let log = col.dead_log(&pivot).collect(store).await.unwrap();
        for (key, ()) in &log {
            // A revived keyed anchor keeps its removal as a historical event,
            // so the dead side is checked only where the chain has not taken
            // the record back.
            if !in_chain.contains(&key.rec) {
                assert!(
                    matches!(
                        col.anchor(store, key.rec.to_id(col.tenant()))
                            .await
                            .unwrap(),
                        Anchor::Dead
                    ),
                    "a removed record reads living at its anchor"
                );
            }
        }
    }

    #[test]
    fn a_removal_is_stamped_on_the_anchor_and_a_save_does_not_undo_it() {
        block_on(async {
            // Liveness reads off the record itself (RFC 0050), so the anchor
            // must agree with the index at every step — including the step
            // where they could most easily drift: a save aimed at an anchor
            // that is already dead.
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());
            let id = col.insert(&store, &Doc { n: 1 }).await.unwrap();

            let (meta, _) = col.load_record(&store, id).await.unwrap();
            assert!(meta.is_live(), "a fresh insert reads dead");

            assert!(col.remove(&store, id).await.unwrap());
            let (meta, _) = col.load_record(&store, id).await.unwrap();
            assert!(!meta.is_live(), "the anchor did not record the removal");

            // The record is out of the living walk, and writing to it must not
            // put it back: an unkeyed save re-indexes nothing, so a cleared
            // flag here would claim life the walk does not grant.
            col.save(&store, id, &Doc { n: 2 }).await.unwrap();
            let (meta, body) = col.load_record(&store, id).await.unwrap();
            assert_eq!(body, Doc { n: 2 }, "the save did not land");
            assert!(!meta.is_live(), "a plain save silently revived a record");
            let walked: Vec<(crate::id::Id, Doc)> =
                col.all(&store).try_collect().await.unwrap();
            assert!(walked.is_empty(), "the walk disagrees with the anchor");
        });
    }

    // The chain IS the modification log: exactly one entry per living
    // record, at its live version's instant, and a save moves that entry
    // rather than adding one. The removal log is the other half.
    #[test]
    fn the_chain_and_the_removal_log_track_the_living_set() {
        async fn chain_ids(
            col: Collection<Doc>,
            store: &MemStore,
        ) -> Vec<crate::id::Id> {
            let pivot = col.load_pivot(store).await.unwrap();
            col.recency_chain(&pivot)
                .collect(store)
                .await
                .unwrap()
                .into_iter()
                .map(|(key, ())| key.rec.to_id(col.tenant()))
                .collect()
        }

        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());
            let a = col.insert(&store, &Doc { n: 1 }).await.unwrap();
            let b = col.insert(&store, &Doc { n: 2 }).await.unwrap();
            assert_eq!(chain_ids(col, &store).await, vec![a, b]);

            // A save relocates the one entry to the new version's instant —
            // `a` moves to the chain's tail, still a single entry.
            col.save(&store, a, &Doc { n: 10 }).await.unwrap();
            assert_eq!(chain_ids(col, &store).await, vec![b, a]);
            let (meta, _) = col.load_record(&store, a).await.unwrap();
            let live_instant = meta.succession.instant();
            assert!(live_instant > b.key(), "the chain tail is the newest");
            let pivot_rec = col.load_pivot(&store).await.unwrap();
            let entries =
                col.recency_chain(&pivot_rec).collect(&store).await.unwrap();
            let (key, ()) = entries.last().unwrap();
            assert_eq!(
                (
                    u64::from_be_bytes(key.field.clone().try_into().unwrap()),
                    key.rec.to_id(col.tenant()),
                ),
                (live_instant, a),
                "the entry sits exactly at the live version's instant"
            );

            // A removal takes the record out of the chain and appends to the
            // removal log — keyed above every instant minted before it.
            assert!(col.remove(&store, b).await.unwrap());
            assert_eq!(chain_ids(col, &store).await, vec![a]);
            let pivot_rec = col.load_pivot(&store).await.unwrap();
            let log = col.dead_log(&pivot_rec).collect(&store).await.unwrap();
            let (removed_key, ()) = log.last().unwrap();
            assert_eq!(log.len(), 1);
            assert_eq!(removed_key.rec.to_id(col.tenant()), b);
            let removed_at = u64::from_be_bytes(
                removed_key.field.clone().try_into().unwrap(),
            );
            assert!(removed_at > live_instant);
        });
    }

    // The floor is read off both chains' tail segments (RFC 0050, A2), and
    // it needs both: a removed record *leaves* the record chain, so once a
    // collection has been emptied its whole history lives in the removal
    // log alone.
    #[test]
    fn the_floor_is_the_greater_of_the_two_chain_tails() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());

            let pivot_rec = col.load_pivot(&store).await.unwrap();
            assert_eq!(
                col.instant_floor(&store, &pivot_rec).await.unwrap(),
                0,
                "an empty collection floors at zero"
            );

            let a = col.insert(&store, &Doc { n: 1 }).await.unwrap();
            let b = col.insert(&store, &Doc { n: 2 }).await.unwrap();
            let pivot_rec = col.load_pivot(&store).await.unwrap();
            assert_eq!(
                col.instant_floor(&store, &pivot_rec).await.unwrap(),
                b.key(),
                "the floor is the record chain's tail while records live"
            );

            // Empty the collection: the chain now holds nothing, so only the
            // removal log can still answer.
            assert!(col.remove(&store, a).await.unwrap());
            assert!(col.remove(&store, b).await.unwrap());
            let pivot_rec = col.load_pivot(&store).await.unwrap();
            assert!(
                col.recency_chain(&pivot_rec)
                    .collect(&store)
                    .await
                    .unwrap()
                    .is_empty(),
                "the removals did not empty the chain"
            );
            assert!(
                col.instant_floor(&store, &pivot_rec).await.unwrap() > b.key(),
                "the removal log must hold the floor once the chain is empty"
            );
        });
    }

    // A mirror can impose node instants from a faster clock (equivalently:
    // this clock rewound). The persisted floor must keep every locally
    // minted instant above them — a catch-up cursor a client already
    // advanced can never be written under.
    #[test]
    fn imposed_future_instants_floor_local_minting() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());

            let future =
                wavedb_platform::time::key_nanos() + 3_600_000 * 1_000_000; // an hour ahead of this clock
            let id = crate::id::Id::new(
                future,
                tenant(),
                false,
                crate::record::type_salt(Doc::STRUCT_HASH),
            );
            let meta = crate::metadata::Metadata {
                succession: Succession::CreatedAt(future),
                pivot_id: Some(pivot),
                user: tenant(),
                ..Default::default()
            };
            col.adopt_with(&store, id, meta, &Doc { n: 1 })
                .await
                .unwrap();

            let local = col.insert(&store, &Doc { n: 2 }).await.unwrap();
            assert!(local.key() > future, "insert must clear the floor");

            col.save(&store, local, &Doc { n: 3 }).await.unwrap();
            let (meta, _) = col.load_record(&store, local).await.unwrap();
            assert!(
                meta.succession.instant() > local.key(),
                "a save's version instant keeps climbing past the floor"
            );
        });
    }

    // Archive addresses are pure functions of (type, shape, instant): the
    // superseded version is readable at the derived slot — flipped FLAG,
    // type salt — without any stored pointer.
    #[test]
    fn archives_resolve_at_their_derived_slots() {
        use crate::record::archive_id;

        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());
            let id = col.insert(&store, &Doc { n: 1 }).await.unwrap();
            col.save(&store, id, &Doc { n: 2 }).await.unwrap();

            // NonUnique: the minted anchor carries the type salt; the first
            // version's instant IS the anchor key, so its archive sits at
            // the same key with the FLAG flipped.
            assert_eq!(id.salt(), (Doc::STRUCT_HASH & 0x7FFF) as u16);
            assert!(!id.flag());
            let slot =
                archive_id(Doc::STRUCT_HASH, Doc::SHAPE, id.key(), tenant());
            assert!(slot.flag(), "a NonUnique archive flips to FLAG = 1");
            assert_eq!(
                col.get(&store, slot).await.unwrap(),
                Some(Doc { n: 1 }),
                "the superseded version resolves at the derived slot"
            );

            // Unique: anchor FLAG = 1, so archives flip to the time
            // namespace (FLAG = 0) — derived from the instant the live
            // version chains back to.
            save_unique(&store, tenant(), &Settings { volume: 1 })
                .await
                .unwrap();
            save_unique(&store, tenant(), &Settings { volume: 2 })
                .await
                .unwrap();
            let anchor =
                crate::id::Id::new(Settings::STRUCT_HASH, tenant(), true, 0);
            let (live_meta, _) = crate::record::decode_record::<Settings>(
                Settings::STRUCT_HASH,
                &crate::store::Store::get_of(
                    &store,
                    Settings::STRUCT_HASH,
                    anchor,
                )
                .await
                .unwrap()
                .unwrap(),
            )
            .unwrap();
            let first = live_meta.previous.expect("live chains back");
            let slot = archive_id(
                Settings::STRUCT_HASH,
                Settings::SHAPE,
                first,
                tenant(),
            );
            assert!(!slot.flag(), "a Unique archive flips to FLAG = 0");
            let (_, archived) = crate::record::decode_record::<Settings>(
                Settings::STRUCT_HASH,
                &crate::store::Store::get_of(
                    &store,
                    Settings::STRUCT_HASH,
                    slot,
                )
                .await
                .unwrap()
                .unwrap(),
            )
            .unwrap();
            assert_eq!(archived, Settings { volume: 1 });
        });
    }

    // The Unique mirror writes the node's metadata verbatim; a version the
    // cache never held stays a MISS (the cache is state, not full history).
    #[test]
    fn adopt_unique_mirrors_node_metadata_verbatim() {
        use crate::record::adopt_unique;
        use crate::store::Store as _;

        block_on(async {
            let node = MemStore::default();
            save_unique(&node, tenant(), &Settings { volume: 1 })
                .await
                .unwrap();
            save_unique(&node, tenant(), &Settings { volume: 2 })
                .await
                .unwrap();
            let anchor =
                crate::id::Id::new(Settings::STRUCT_HASH, tenant(), true, 0);
            let (node_meta, _) = crate::record::decode_record::<Settings>(
                Settings::STRUCT_HASH,
                &node.get(anchor).await.unwrap().unwrap(),
            )
            .unwrap();

            let cache = MemStore::default();
            adopt_unique::<Settings, _>(
                &cache,
                tenant(),
                node_meta.clone(),
                &Settings { volume: 2 },
            )
            .await
            .unwrap();
            let (cache_meta, cached) =
                crate::record::decode_record::<Settings>(
                    Settings::STRUCT_HASH,
                    &cache.get(anchor).await.unwrap().unwrap(),
                )
                .unwrap();
            assert_eq!(cache_meta, node_meta, "chain data verbatim");
            assert_eq!(cached, Settings { volume: 2 });

            // The cache never held V1 — its archive slot stays a MISS.
            let slot = crate::record::archive_id(
                Settings::STRUCT_HASH,
                Settings::SHAPE,
                node_meta.previous.expect("chains back"),
                tenant(),
            );
            assert_eq!(cache.get(slot).await.unwrap(), None);

            // Re-adopting the identical version writes nothing new.
            adopt_unique::<Settings, _>(
                &cache,
                tenant(),
                node_meta.clone(),
                &Settings { volume: 2 },
            )
            .await
            .unwrap();
            let (again, _) = crate::record::decode_record::<Settings>(
                Settings::STRUCT_HASH,
                &cache.get(anchor).await.unwrap().unwrap(),
            )
            .unwrap();
            assert_eq!(again, node_meta);
        });
    }

    #[test]
    fn stale_pivot_and_wrong_type_are_typed_errors() {
        block_on(async {
            let store = MemStore::default();
            let bogus =
                Collection::<Doc>::at(LocalId::new(42, false, 1), tenant());
            assert!(matches!(
                bogus.insert(&store, &Doc { n: 1 }).await,
                Err(Error::PivotMissing(_))
            ));

            // A Unique record read back as the wrong type is rejected by the
            // envelope's STRUCT_HASH check, not mis-decoded.
            save_unique(&store, tenant(), &Settings { volume: 1 })
                .await
                .unwrap();
            let anchor =
                crate::id::Id::new(Settings::STRUCT_HASH, tenant(), true, 0);
            let pivot =
                Collection::<Doc>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Doc>::at(pivot, tenant());
            assert!(matches!(
                col.get(&store, anchor).await,
                Err(Error::UnknownStructHash(h)) if h == Settings::STRUCT_HASH
            ));
        });
    }
}
