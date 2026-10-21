//! [`Collection`]'s natural-key half — `#[wavedb::key(...)]` types, whose
//! anchor is **derived from the key fields' values** (SeaHash over their
//! wire bytes) instead of minted at insert.
//!
//! The identity being content, `insert` is an **upsert at the computed
//! anchor** — the one id-less write, mirroring the Unique `save`:
//!
//! - a **vacant** anchor takes a guarded first version;
//! - a **living** anchor takes an ordinary chained save (archive + index
//!   re-keys);
//! - a **dead** anchor (the key was removed, then written again) is
//!   **revived**: the new version chains onto the record's whole prior
//!   history and re-enters `current`, the recency log, and every
//!   secondary. The dead log keeps the historical removal entry — it is a
//!   removal *log*, not a membership set, and catch-up navigation merges
//!   both log tails by instant, so a cursor from before the removal
//!   replays `Removed` then `Saved` and converges on the living record.
//!
//! A save may never *address* a foreign anchor: [`Collection::save`]
//! refuses a value whose computed key does not derive the given id as a
//! typed [`Error::KeyMismatch`](crate::Error::KeyMismatch) — "renaming"
//! is an explicit `remove` + `insert` of the new key.

use crate::collection::Collection;
use crate::collection_read::Anchor;
use crate::collection_write::MovedRoots;
use crate::error::Result;
use crate::id::Id;
use crate::index::Pivot;
use crate::local_id::LocalId;
use crate::metadata::{Metadata, Succession};
use crate::notify::{Mutation, MutationKind};
use crate::record::{SavePlan, keyed_id, plan_chained_save};
use crate::store::Store;
use crate::traits::NonUniqueStruct;
use crate::wire::to_wire;

impl<T: NonUniqueStruct> Collection<T> {
    /// The content-derived anchor of `value` under this collection's
    /// tenant — `Some` only for `#[wavedb::key(...)]` types.
    pub(crate) fn keyed_anchor(&self, value: &T) -> Option<Id> {
        value
            .natural_key()
            .map(|key| keyed_id(self.tenant(), T::STRUCT_HASH, key))
    }

    /// The keyed upsert at the computed anchor `id`: a chained save when
    /// the anchor is living, [`chain_into_living`](Self::chain_into_living)
    /// when it is vacant or dead.
    pub(crate) async fn upsert_keyed<S: Store>(
        &self,
        store: &S,
        pivot: &T::Pivot,
        id: Id,
        value: &T,
    ) -> Result<()> {
        // Liveness reads off the anchor, not off `current` (RFC 0050): one
        // record read where a tree descent plus a "is it merely dead?" fetch
        // used to sit.
        if matches!(self.anchor(store, id).await?, Anchor::Living(..)) {
            return self.save_planned(store, id, value, None).await;
        }
        self.chain_into_living(store, pivot, id, None, value).await
    }

    /// Write `value` at a **non-living** anchor (vacant or dead) as a
    /// chained live version and (re-)index it in `current`, the recency
    /// log, and every secondary — one atomic batch. The shared tail of the
    /// keyed upsert and a mirror's revival:
    /// [`adopt_with`](Collection::adopt_with) passes the node's
    /// [`Metadata`] as `imposed`, so a revived mirror archives its dead
    /// version at the node's own derived slot instead of overwriting it.
    pub(crate) async fn chain_into_living<S: Store>(
        &self,
        store: &S,
        pivot: &T::Pivot,
        id: Id,
        imposed: Option<Metadata>,
        value: &T,
    ) -> Result<()> {
        let floor = self.instant_floor(store, pivot).await?;
        let plan = SavePlan {
            hash: T::STRUCT_HASH,
            shape: T::SHAPE,
            live_id: id,
            tenant: self.tenant(),
            user: self.user(),
            pivot_id: Some(self.pivot()),
            imposed,
            floor,
            revives: true,
        };
        let (mut batch, _superseded, live_meta) =
            plan_chained_save::<T, S>(store, &plan, value).await?;
        let Succession::CreatedAt(instant) = live_meta.succession else {
            return Err(crate::Error::ChainCorrupt(id));
        };
        // Distinct structures, so their plans don't overlap (the writes
        // already in `batch` are record slots, never nodes) — no overlay.
        let mut records = self.recency_chain(pivot);
        let mut secs = self.sec_trees(pivot);
        let mut lists = self.list_chains(pivot);
        let key = Self::instant_key(instant, LocalId::from_id(id));
        let envelope =
            crate::record::encode_record(T::STRUCT_HASH, &live_meta, value);
        // A revival re-enters the chain exactly as a first version does: the
        // record's prior copy left it when the key was removed, so there is
        // nothing to relocate — only to insert, at the new live instant. Every
        // declared list re-enters the same way.
        batch.extend(records.plan_insert(store, key, ()).await?);
        self.plan_list_inserts(
            store, &mut batch, &mut lists, id, &envelope, value,
        )
        .await?;
        for (i, tree) in secs.iter_mut().enumerate() {
            let key = Self::sec_key(value, i, id);
            batch.extend(tree.plan_insert(store, key).await?);
        }
        let mut fuzzy = self.fuzzy_trees(pivot);
        let mut view = crate::overlay::Overlay::new(store);
        self.plan_fuzzy_inserts(&mut view, &mut batch, &mut fuzzy, id, value)
            .await?;
        self.push_root_moves(
            &mut batch,
            pivot,
            &MovedRoots {
                secondaries: &secs,
                recency: records.roots(),
                removals: pivot.removals(),
                lists: &lists,
                fuzzy: &fuzzy,
            },
        );
        store.apply(&batch).await?;
        store.note_mutation(|| Mutation {
            struct_hash: T::STRUCT_HASH,
            tenant: self.tenant(),
            pivot: Some(self.pivot()),
            id,
            kind: MutationKind::Saved,
            meta: Some(live_meta),
            body: to_wire(value),
        });
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use futures::executor::block_on;

    use crate::collection::Collection;
    use crate::error::Error;
    use crate::expose::Reply;
    use crate::expose::collection_changes;
    use crate::id::Id;
    use crate::index::mem_store::MemStore;
    use crate::index::{Bound, ChainRoots, IndexKey, LogRoots, Pivot, Roots};
    use crate::local_id::LocalId;
    use crate::metadata::Metadata;
    use crate::permission::PermissionRef;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::{WaveWire, from_wire, to_wire};

    // Hand-rolled fixture of what `#[wavedb(NonUnique)]` with
    // `#[wavedb::key(realm, login)]` + `#[wavedb::pivot(tag)]` generates
    // (core can't use the proc-macro; it lives downstream).
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Cred {
        realm: String,
        login: String,
        tag: String,
    }
    impl WaveDbStruct for Cred {
        const STRUCT_HASH: u64 = 0x5EA_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }
    impl NonUniqueStruct for Cred {
        type Pivot = CredPivot;
        const NUM_SECONDARIES: usize = 1;
        fn secondary_key(&self, index: usize) -> Vec<u8> {
            match index {
                0 => self.tag.key_bytes(),
                _ => Vec::new(),
            }
        }
        fn natural_key(&self) -> Option<u64> {
            let mut bytes = to_wire(&self.realm);
            bytes.extend_from_slice(&to_wire(&self.login));
            Some(crate::natural_key_hash(&bytes))
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct CredPivot {
        recency: ChainRoots,
        removals: LogRoots,
        secondaries: [LocalId; 1],
        permission: Option<PermissionRef>,
    }
    impl Pivot for CredPivot {
        const STRUCT_HASH: u64 = 0x5EA_0002;
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

    fn cred(login: &str, tag: &str) -> Cred {
        Cred {
            realm: "prod".into(),
            login: login.into(),
            tag: tag.into(),
        }
    }

    async fn by_tag(
        col: Collection<Cred>,
        store: &MemStore,
        tag: &str,
    ) -> Vec<(Id, Cred)> {
        col.search_by(store, 0, Bound::Exact(tag.key_bytes()))
            .try_collect()
            .await
            .unwrap()
    }

    #[test]
    fn keyed_insert_is_an_upsert_at_the_content_anchor() {
        block_on(async {
            let tenant = U48::from(6u32);
            let store = MemStore::default();
            let pivot =
                Collection::<Cred>::create(&store, tenant).await.unwrap();
            let col = Collection::<Cred>::at(pivot, tenant);

            let a = col.insert(&store, &cred("ada", "old")).await.unwrap();
            let b = col.insert(&store, &cred("ada", "new")).await.unwrap();
            assert_eq!(a, b, "one key value = one anchor, in any process");

            // One living record at the live state; the superseded version
            // archived and chained, never overwritten.
            let walked: Vec<(Id, Cred)> =
                col.all(&store).try_collect().await.unwrap();
            assert_eq!(walked, vec![(a, cred("ada", "new"))]);
            let versions: Vec<(Metadata, Cred)> =
                col.history(&store, a).try_collect().await.unwrap();
            assert_eq!(versions.len(), 2, "the upsert chains");

            // The secondary re-keyed with the upsert.
            assert_eq!(
                by_tag(col, &store, "new").await,
                vec![(a, cred("ada", "new"))]
            );
            let observed = by_tag(col, &store, "old").await;
            assert!(observed.is_empty(), "{observed:?}");

            // A different key value is a different record.
            let c = col.insert(&store, &cred("bob", "x")).await.unwrap();
            assert_ne!(a, c);
        });
    }

    #[test]
    fn a_save_addressing_a_foreign_anchor_is_key_mismatch() {
        block_on(async {
            let tenant = U48::from(7u32);
            let store = MemStore::default();
            let pivot =
                Collection::<Cred>::create(&store, tenant).await.unwrap();
            let col = Collection::<Cred>::at(pivot, tenant);

            let a = col.insert(&store, &cred("ada", "t")).await.unwrap();
            // Different key fields under `a`'s id: refused, nothing written
            // — the "rename" that would silently duplicate the record.
            let err = col.save(&store, a, &cred("bob", "t")).await.unwrap_err();
            assert!(matches!(err, Error::KeyMismatch(id) if id == a));
            let versions: Vec<(Metadata, Cred)> =
                col.history(&store, a).try_collect().await.unwrap();
            assert_eq!(versions.len(), 1, "the refusal must not write");

            // Addressing the value's own anchor saves normally.
            col.save(&store, a, &cred("ada", "t2")).await.unwrap();
        });
    }

    // A mirror that watched a key die and come back converges: the
    // adopted revival chains onto the mirror's own dead copy, archiving it
    // at the node's derived slot byte-identically, and the record
    // re-enters the mirror's living walk.
    #[test]
    fn adopting_a_revival_chains_the_mirrors_dead_copy() {
        block_on(async {
            let tenant = U48::from(9u32);
            let node = MemStore::default();
            let pivot =
                Collection::<Cred>::create(&node, tenant).await.unwrap();
            let node_col = Collection::<Cred>::at(pivot, tenant);
            let id = node_col.insert(&node, &cred("ada", "v1")).await.unwrap();
            let (v1_meta, _) = node_col.load_record(&node, id).await.unwrap();

            let cache = MemStore::default();
            Collection::<Cred>::adopt_pivot(&cache, tenant, pivot)
                .await
                .unwrap();
            let cache_col = Collection::<Cred>::at(pivot, tenant);
            cache_col
                .adopt_with(&cache, id, v1_meta, &cred("ada", "v1"))
                .await
                .unwrap();

            // The key dies on both sides; the node then writes it again.
            assert!(node_col.remove(&node, id).await.unwrap());
            assert!(cache_col.remove(&cache, id).await.unwrap());
            let back =
                node_col.insert(&node, &cred("ada", "v2")).await.unwrap();
            assert_eq!(back, id);
            let (v2_meta, _) = node_col.load_record(&node, id).await.unwrap();

            cache_col
                .adopt_with(&cache, id, v2_meta.clone(), &cred("ada", "v2"))
                .await
                .unwrap();
            let walked: Vec<(Id, Cred)> =
                cache_col.all(&cache).try_collect().await.unwrap();
            assert_eq!(walked, vec![(id, cred("ada", "v2"))]);

            // The dead copy archived at the NODE's derived slot, and the
            // live records agree byte-for-byte.
            let slot = crate::record::archive_id(
                Cred::STRUCT_HASH,
                Cred::SHAPE,
                v2_meta.previous.expect("the revival chains back"),
                tenant,
            );
            let node_archive = crate::store::Store::get(&node, slot)
                .await
                .unwrap()
                .expect("node archived v1");
            let cache_archive = crate::store::Store::get(&cache, slot)
                .await
                .unwrap()
                .expect("cache archived v1 at the node's slot");
            assert_eq!(cache_archive, node_archive);
            assert_eq!(
                crate::store::Store::get(&cache, id).await.unwrap(),
                crate::store::Store::get(&node, id).await.unwrap(),
                "the revived live records must be byte-identical"
            );
        });
    }

    #[test]
    fn a_removed_key_written_again_revives_the_chain() {
        block_on(async {
            let tenant = U48::from(8u32);
            let store = MemStore::default();
            let pivot =
                Collection::<Cred>::create(&store, tenant).await.unwrap();
            let col = Collection::<Cred>::at(pivot, tenant);

            let id = col.insert(&store, &cred("ada", "v1")).await.unwrap();
            assert!(col.remove(&store, id).await.unwrap());
            let walked: Vec<(Id, Cred)> =
                col.all(&store).try_collect().await.unwrap();
            assert!(walked.is_empty(), "removed = out of the living walk");
            // The anchor says so on its own, without consulting a tree.
            let (dead_meta, _) = col.load_record(&store, id).await.unwrap();
            assert!(!dead_meta.is_live(), "the anchor hid the removal");

            // Same key again: same anchor, revived — the whole history
            // survives the removal.
            let back = col.insert(&store, &cred("ada", "v2")).await.unwrap();
            assert_eq!(back, id, "same key, same anchor, through death");
            let walked: Vec<(Id, Cred)> =
                col.all(&store).try_collect().await.unwrap();
            assert_eq!(walked, vec![(id, cred("ada", "v2"))]);
            // Revival is the one write that clears the flag, and it clears it
            // in the same batch that re-indexes the anchor — the two agree.
            let (live_meta, _) = col.load_record(&store, id).await.unwrap();
            assert!(live_meta.is_live(), "the revived anchor still reads dead");
            let tags: Vec<String> = col
                .history(&store, id)
                .try_collect::<Vec<(Metadata, Cred)>>()
                .await
                .unwrap()
                .into_iter()
                .map(|(_, v)| v.tag)
                .collect();
            assert_eq!(tags, vec!["v2", "v1"], "v2 chains onto v1");

            // Only the living state is indexed.
            assert_eq!(
                by_tag(col, &store, "v2").await,
                vec![(id, cred("ada", "v2"))]
            );
            let observed = by_tag(col, &store, "v1").await;
            assert!(observed.is_empty(), "{observed:?}");

            // Catch-up from before the removal replays the removal *then*
            // the revival (the dead log keeps history; instants order the
            // merge), converging on the living record.
            let Reply::Values(items) =
                collection_changes::<Cred, _>(&store, pivot, tenant, Some(0))
                    .await
                    .unwrap()
            else {
                panic!("changes answers values");
            };
            let changes: Vec<crate::expose::Change> =
                items.iter().map(|b| from_wire(b).unwrap()).collect();
            assert!(matches!(changes[0], crate::expose::Change::Cursor(_)));
            assert!(matches!(
                changes[1],
                crate::expose::Change::Removed(rid, _) if rid == id
            ));
            assert!(matches!(
                &changes[2],
                crate::expose::Change::Saved(sid, _, _) if *sid == id
            ));
        });
    }
}
