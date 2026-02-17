//! [`Collection`]'s adopt half — writing **foreign-minted identities** into a
//! local [`Store`].
//!
//! Everything in [`crate::collection_write`] mints its own ids; a client-side
//! cache (and later, live sync) instead mirrors records whose `Id`s the node
//! already minted. [`adopt_pivot`](Collection::adopt_pivot) bootstraps the
//! local copy of a node collection at the node's `PivotId`;
//! [`adopt`](Collection::adopt) upserts one record under its node `Id` —
//! indexing it like an insert when it is new locally, delegating to `save`
//! (secondary re-keys, version chain) when it is already living, and skipping
//! entirely when the bytes are unchanged, so read-path mirroring never grows
//! the local store.
//!
//! Adopted values are assumed **living** — the mirror sources (a successful
//! node insert/update, a walked `All` item) only ever see living records.

use crate::collection::Collection;
use crate::collection_read::Anchor;
use crate::error::Result;
use crate::id::Id;
use crate::index::Pivot;
use crate::local_id::LocalId;
use crate::store::Store;
use crate::traits::NonUniqueStruct;
use crate::u48::U48;
use crate::wire::to_wire;

impl<T: NonUniqueStruct> Collection<T> {
    /// Bootstrap this collection **at the given `pivot`** (a node-minted
    /// `PivotId`) if no pivot record exists there yet: empty trees + the
    /// `Pivot` record, one atomic batch — [`create`](Self::create) with the
    /// identity supplied instead of minted. Idempotent.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn adopt_pivot<S: Store>(
        store: &S,
        tenant: U48,
        pivot: LocalId,
    ) -> Result<()> {
        let exists = store
            .get_of(<T::Pivot as Pivot>::STRUCT_HASH, pivot.to_id(tenant))
            .await?
            .is_some();
        if exists {
            return Ok(());
        }
        Self::create_rooted(store, tenant, pivot.to_id(tenant)).await
    }

    /// Mirror the **living** record `(id, value)` into this (local) collection
    /// under its node-minted `Id`: index + write it like an insert when the id
    /// is new here, [`save`](Self::save) it (archive + secondary re-keys) when
    /// it is already living with different bytes, and do nothing when the
    /// bytes match — a read-path mirror of an unchanged record must not grow
    /// the store.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::PivotMissing`] when the local
    /// pivot was never adopted, or a decode fault on a corrupt local record.
    ///
    /// [`Error::PivotMissing`]: crate::Error::PivotMissing
    pub async fn adopt<S: Store>(
        &self,
        store: &S,
        id: Id,
        value: &T,
    ) -> Result<()> {
        let pivot = self.load_pivot(store).await?;
        // One read decides all three cases and hands back the bytes the
        // living branch compares — where a `current` descent, a "is it merely
        // dead?" fetch and a re-read of the same record used to sit.
        match self.anchor(store, id).await? {
            // No `PartialEq` bound on record types — unchanged is decided on
            // the wire bytes, the identity the store actually holds.
            Anchor::Living(_, existing)
                if to_wire(&existing) == to_wire(value) =>
            {
                Ok(())
            }
            Anchor::Living(..) => self.save(store, id, value).await,
            // A dead anchor still holding bytes is a locally-dead record
            // coming back — only a keyed type's anchor can recur. Revive it
            // as a chained version so the dead copy archives instead of
            // being overwritten.
            Anchor::Dead => {
                self.chain_into_living(store, &pivot, id, None, value).await
            }
            Anchor::Vacant => self.insert_at(store, &pivot, id, value).await,
        }
    }

    /// [`adopt`](Self::adopt) with the node's [`Metadata`] written
    /// **verbatim** — the mirror then carries authoritative chain data
    /// (who/when/permission) and its archives land at the node's own
    /// derived slots. Used by the paths whose frames ship metadata (watch
    /// events, the `All` walk); plain read mirrors keep [`adopt`](Self::adopt).
    ///
    /// # Errors
    /// As [`adopt`](Self::adopt).
    ///
    /// [`Metadata`]: crate::Metadata
    pub async fn adopt_with<S: Store>(
        &self,
        store: &S,
        id: Id,
        meta: crate::metadata::Metadata,
        value: &T,
    ) -> Result<()> {
        let pivot = self.load_pivot(store).await?;
        match self.anchor(store, id).await? {
            Anchor::Living(existing, body)
                if existing == meta && to_wire(&body) == to_wire(value) =>
            {
                Ok(())
            }
            Anchor::Living(..) => {
                self.save_with_meta(store, id, meta, value).await
            }
            // A revived keyed anchor: chain the node's version onto the
            // locally-dead copy, archiving it at the node's derived slot.
            Anchor::Dead => {
                self.chain_into_living(store, &pivot, id, Some(meta), value)
                    .await
            }
            Anchor::Vacant => {
                self.insert_with_meta(store, &pivot, id, meta, value).await
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use futures::executor::block_on;

    use crate::collection::Collection;
    use crate::error::Error;
    use crate::id::Id;
    use crate::index::mem_store::MemStore;
    use crate::index::{ChainRoots, IndexKey, LogRoots, Pivot, Roots};
    use crate::local_id::LocalId;
    use crate::permission::PermissionRef;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::WaveWire;

    // Hand-rolled fixture with one secondary index — exactly what
    // `#[wavedb] … #[wavedb::pivot(label)]` generates (core can't use the
    // proc-macro; it lives downstream).
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Item {
        label: String,
        n: u64,
    }
    impl WaveDbStruct for Item {
        const STRUCT_HASH: u64 = 0xADD_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }
    impl NonUniqueStruct for Item {
        type Pivot = ItemPivot;
        const NUM_SECONDARIES: usize = 1;
        fn secondary_key(&self, index: usize) -> Vec<u8> {
            match index {
                0 => self.label.key_bytes(),
                _ => Vec::new(),
            }
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct ItemPivot {
        recency: ChainRoots,
        removals: LogRoots,
        secondaries: [ChainRoots; 1],
        permission: Option<PermissionRef>,
    }
    impl Pivot for ItemPivot {
        const STRUCT_HASH: u64 = 0xADD_0002;
        fn secondaries(&self) -> &[ChainRoots] {
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

    fn item(label: &str, n: u64) -> Item {
        Item {
            label: label.into(),
            n,
        }
    }

    // With the node's Metadata riding along, the mirror is byte-faithful:
    // the live copy carries the node's chain data verbatim, and archiving
    // a superseded mirrored version lands at the NODE's own derived slot
    // (addresses are pure functions of the instants both sides share).
    #[test]
    fn adopt_with_carries_node_chain_data_and_slots() {
        block_on(async {
            let tenant = U48::from(5u32);
            let node = MemStore::default();
            let pivot =
                Collection::<Item>::create(&node, tenant).await.unwrap();
            let node_col = Collection::<Item>::at(pivot, tenant);
            let id = node_col.insert(&node, &item("a", 1)).await.unwrap();
            let (v1_meta, _) = node_col.load_record(&node, id).await.unwrap();

            let cache = MemStore::default();
            Collection::<Item>::adopt_pivot(&cache, tenant, pivot)
                .await
                .unwrap();
            let cache_col = Collection::<Item>::at(pivot, tenant);
            cache_col
                .adopt_with(&cache, id, v1_meta.clone(), &item("a", 1))
                .await
                .unwrap();
            assert_eq!(
                cache_col.load_record(&cache, id).await.unwrap(),
                (v1_meta.clone(), item("a", 1)),
                "the mirror must carry the node's metadata verbatim"
            );

            // The node saves V2; the mirror adopts it — and must archive
            // its V1 copy at the node's own derived slot.
            node_col.save(&node, id, &item("a", 2)).await.unwrap();
            let (v2_meta, _) = node_col.load_record(&node, id).await.unwrap();
            cache_col
                .adopt_with(&cache, id, v2_meta.clone(), &item("a", 2))
                .await
                .unwrap();
            assert_eq!(
                cache_col.load_record(&cache, id).await.unwrap(),
                (v2_meta.clone(), item("a", 2))
            );
            let slot = crate::record::archive_id(
                Item::STRUCT_HASH,
                Item::SHAPE,
                v2_meta.previous.expect("v2 chains back"),
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
            assert_eq!(
                cache_archive, node_archive,
                "mirror archives must be byte-identical to the node's"
            );

            // Re-adopting the same version is a no-op (unchanged skip).
            cache_col
                .adopt_with(&cache, id, v2_meta.clone(), &item("a", 2))
                .await
                .unwrap();
            let versions: Vec<(crate::metadata::Metadata, Item)> =
                cache_col.history(&cache, id).try_collect().await.unwrap();
            assert_eq!(versions.len(), 2, "no phantom version on a re-adopt");
        });
    }

    // The "node" mints ids in one store; the "cache" adopts them in another
    // and must agree on identity, order, and secondary lookups.
    #[test]
    fn adopted_records_keep_node_ids_order_and_indexes() {
        block_on(async {
            let tenant = U48::from(3u32);
            let node = MemStore::default();
            let node_pivot =
                Collection::<Item>::create(&node, tenant).await.unwrap();
            let node_col = Collection::<Item>::at(node_pivot, tenant);
            let a = node_col.insert(&node, &item("red", 1)).await.unwrap();
            let b = node_col.insert(&node, &item("blue", 2)).await.unwrap();

            let cache = MemStore::default();
            Collection::<Item>::adopt_pivot(&cache, tenant, node_pivot)
                .await
                .unwrap();
            // Idempotent: adopting an existing pivot is a no-op, not a reset.
            Collection::<Item>::adopt_pivot(&cache, tenant, node_pivot)
                .await
                .unwrap();
            let col = Collection::<Item>::at(node_pivot, tenant);
            col.adopt(&cache, a, &item("red", 1)).await.unwrap();
            col.adopt(&cache, b, &item("blue", 2)).await.unwrap();

            let walked: Vec<(Id, Item)> =
                col.all(&cache).try_collect().await.unwrap();
            // Newest-first, and the instants ordering it are the **node's** —
            // `adopt` writes the node's `Metadata` verbatim, so the mirror's
            // walk reproduces the node's own order rather than its local one.
            assert_eq!(
                walked.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                vec![b, a],
                "the cache walk must yield the node's ids in node order"
            );
            let blues: Vec<(Id, Item)> = col
                .search_by(
                    &cache,
                    0,
                    crate::index::Bound::Exact("blue".key_bytes()),
                )
                .try_collect()
                .await
                .unwrap();
            assert_eq!(blues, vec![(b, item("blue", 2))]);
        });
    }

    #[test]
    fn re_adoption_skips_unchanged_and_rekeys_changed() {
        block_on(async {
            let tenant = U48::from(4u32);
            let store = MemStore::default();
            let pivot =
                Collection::<Item>::create(&store, tenant).await.unwrap();
            let col = Collection::<Item>::at(pivot, tenant);
            let id = Id::new(7, tenant, false, 1);

            col.adopt(&store, id, &item("red", 1)).await.unwrap();
            // Same bytes again (a read-path mirror): no new version.
            col.adopt(&store, id, &item("red", 1)).await.unwrap();
            let versions: Vec<(crate::metadata::Metadata, Item)> =
                col.history(&store, id).try_collect().await.unwrap();
            assert_eq!(versions.len(), 1, "unchanged adopt must not archive");

            // Changed bytes: one archived version, secondary re-keyed.
            col.adopt(&store, id, &item("blue", 1)).await.unwrap();
            let versions: Vec<(crate::metadata::Metadata, Item)> =
                col.history(&store, id).try_collect().await.unwrap();
            assert_eq!(versions.len(), 2);
            let reds: Vec<(Id, Item)> = col
                .search_by(
                    &store,
                    0,
                    crate::index::Bound::Exact("red".key_bytes()),
                )
                .try_collect()
                .await
                .unwrap();
            assert!(reds.is_empty(), "the old key must be de-indexed");
        });
    }

    #[test]
    fn adopt_without_a_local_pivot_is_pivot_missing() {
        block_on(async {
            let tenant = U48::from(5u32);
            let store = MemStore::default();
            let col =
                Collection::<Item>::at(LocalId::new(42, false, 1), tenant);
            let id = Id::new(7, tenant, false, 1);
            assert!(matches!(
                col.adopt(&store, id, &item("red", 1)).await,
                Err(Error::PivotMissing(_))
            ));
        });
    }
}
