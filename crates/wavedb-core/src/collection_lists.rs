//! **Declared lists** ([RFC 0051]) — `#[wavedb::list(...)]`: one more chain of
//! the same records, sorted by a declared property instead of by modification
//! instant, and holding **whole records inline**.
//!
//! That last part is the whole reason to declare one, and the only duplication
//! WaveDB ever performs ([RFC 0054]): the built-in chain carries pointers, so a
//! collection that declares nothing stores each record exactly once. A list buys
//! a dense bulk read in its order, and pays a full copy for it.
//!
//! The mechanism is the built-in chain's: each list is a key range of the
//! type's record lane (RFC 0078 §7), written by the same entry writes and read
//! by the same scans.
//!
//! Two differences from the built-in chain are load-bearing:
//!
//! - **The tie-break is the anchor**, not the live version's authoring instant.
//!   The anchor never changes, so a record relocates inside a sorted chain only
//!   when the declared property itself changed. In the built-in chain the
//!   instant *is* the key and relocating on every save **is** the mechanism —
//!   here the same choice would be pure cost.
//! - **A list reads ascending**, head to tail, because that is what a declared
//!   order means. The built-in chain reads from the tail because its order is
//!   recency.
//!
//! Liveness still lives on the anchor: a list holds exactly the living records,
//! so every planner here is gated on the built-in chain's verdict rather than
//! deciding membership on its own.
//!
//! [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
//! [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md

use futures::{Stream, TryStreamExt};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::id::Id;
use crate::index::{Bound, Chain, SecKey};
use crate::local_id::LocalId;
use crate::record::wire_record;
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;
use crate::wire::Wire;

impl<T: NonUniqueStruct> Collection<T> {
    /// File a freshly inserted record into every declared list.
    pub(crate) fn plan_list_inserts(
        &self,
        batch: &mut Vec<Write>,
        id: Id,
        envelope: &[u8],
        value: &T,
    ) {
        for (i, chain) in self.list_chains().iter().enumerate() {
            let key = Self::list_key(value, i, id);
            batch.push(chain.insert(&key, &envelope.to_vec()));
        }
    }

    /// Re-file a saved record in every declared list.
    ///
    /// The record's bytes are duplicated in each list, so **every** list is
    /// rewritten whether or not its property changed — that is the cost RFC 0051
    /// states plainly (a save carries K× the record's bytes). What the
    /// comparison saves is the *removal*: an unchanged property means the entry
    /// stays where it is and the write replaces its value in place.
    ///
    /// **Skipping the list entirely when the key is unchanged is not available**,
    /// and the reason is worth stating because the shortcut looks free: the
    /// question it would need answered is "did anything *else* about the record
    /// change?", and the only authority on that is the anchor's previous version.
    /// Consulting it costs a read to avoid a write that is already inside the
    /// window this batch pays for — more resource, more machinery, for a
    /// comparison that is wrong the moment any non-ordering field moves. An
    /// unconditional rewrite is the only coherent rule, and
    /// `a_save_refreshes_a_list_whose_order_it_did_not_change` is what keeps it.
    pub(crate) fn plan_list_moves(
        batch: &mut Vec<Write>,
        lists: &[Chain<Vec<u8>>],
        id: Id,
        envelope: &[u8],
        values: (&T, &T),
    ) {
        let (old_value, value) = values;
        for (i, chain) in lists.iter().enumerate() {
            let old_key = Self::list_key(old_value, i, id);
            let new_key = Self::list_key(value, i, id);
            if old_key != new_key {
                batch.push(chain.remove(&old_key));
            }
            batch.push(chain.insert(&new_key, &envelope.to_vec()));
        }
    }

    /// Take a removed record out of every declared list.
    ///
    /// A list holds only living records — the anchor keeps the bytes, and the
    /// removal log keeps the event.
    pub(crate) fn plan_list_removes(
        &self,
        batch: &mut Vec<Write>,
        id: Id,
        value: &T,
    ) {
        for (i, chain) in self.list_chains().iter().enumerate() {
            batch.push(chain.remove(&Self::list_key(value, i, id)));
        }
    }

    /// Stream every living record in declared list `index`'s order, ascending.
    ///
    /// Records **inline** — unlike [`all`](Collection::all), which walks
    /// pointers and resolves each record at its anchor. Buying that
    /// difference is what declaring a list is for. The generated
    /// `listed_by_<fields>` wrapper calls this with the declaration's index.
    pub fn listed<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
    ) -> impl Stream<Item = Result<(Id, Wire<T>)>> + 'a
    where
        T: 'a,
    {
        self.listed_from(store, index, 0)
            .map_ok(|(id, _, value)| (id, value))
    }

    /// [`listed`](Self::listed) entered at global offset `offset` — the pager's
    /// "jump to page k".
    ///
    /// The stream still runs to the end of the list; a caller takes the page it
    /// wants. An offset at or past the end yields nothing.
    pub fn listed_at<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<(Id, Wire<T>)>> + 'a
    where
        T: 'a,
    {
        self.listed_from(store, index, offset)
            .map_ok(|(id, _, value)| (id, value))
    }

    /// [`listed_at`](Self::listed_at) keeping each record's [`Metadata`].
    ///
    /// The wire `Listed` reads through this: a list entry holds the whole
    /// **envelope**, so the metadata is decoded either way and dropping it
    /// would only force a client mirror to author its own — one that disagrees
    /// with the node about the version chain. It costs nothing here and it is
    /// what lets `adopt_with` write the node's chain data verbatim.
    ///
    /// [`Metadata`]: crate::Metadata
    pub(crate) fn listed_meta_at<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<(Id, crate::metadata::Metadata, Wire<T>)>> + 'a
    where
        T: 'a,
    {
        self.listed_from(store, index, offset)
    }

    /// The living records declared list `index` holds under `bound` (over
    /// its [`IndexKey`](crate::index::IndexKey) encoding), in its order and
    /// **inline** ([RFC 0075]): one read from the bound's least key, then the
    /// chain while the bound holds — a secondary's read without a fetch per
    /// record. The generated `by_<fields>` of a list call this.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md
    pub fn listed_where<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<(Id, Wire<T>)>> + 'a
    where
        T: 'a,
    {
        let handle = self;
        let tenant = self.tenant();
        futures::stream::once(async move {
            handle.check_pivot(store).await?;
            let chain = handle.list_chain(index)?;
            let from = SecKey {
                field: bound.least().to_vec(),
                rec: LocalId::default(),
            };
            Ok::<_, Error>(chain.scan_from(store, &from).try_take_while(
                move |(key, _)| {
                    futures::future::ready(Ok(bound.contains(&key.field)))
                },
            ))
        })
        .try_flatten()
        .and_then(move |(key, bytes)| async move {
            let (_, value) = wire_record::<T>(T::STRUCT_HASH, bytes)?;
            Ok((key.rec.to_id(tenant), value))
        })
    }

    /// How many living records declared list `index` holds — the pager's
    /// "of M".
    ///
    /// # Errors
    /// [`Error::ListOutOfRange`] for an undeclared index, or a [`Store`]
    /// failure.
    pub async fn list_len<S: Store>(
        &self,
        store: &S,
        index: usize,
    ) -> Result<u64> {
        self.check_pivot(store).await?;
        self.list_chain(index)?.len(store).await
    }

    /// Every list reader: the chain read forward past its first `offset`
    /// entries, each record with its metadata.
    fn listed_from<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<(Id, crate::metadata::Metadata, Wire<T>)>> + 'a
    where
        T: 'a,
    {
        let handle = self;
        let tenant = self.tenant();
        futures::stream::once(async move {
            handle.check_pivot(store).await?;
            Ok::<_, Error>(handle.list_chain(index)?.scan_skip(store, offset))
        })
        .try_flatten()
        .and_then(move |(key, bytes)| async move {
            let (meta, value) = wire_record::<T>(T::STRUCT_HASH, bytes)?;
            Ok((key.rec.to_id(tenant), meta, value))
        })
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use futures::executor::block_on;

    use crate::collection::Collection;
    use crate::id::Id;
    use crate::index::mem_store::MemStore;
    use crate::index::{Chain, IndexKey, Pivot};
    use crate::permission::PermissionRef;
    use crate::store::Store;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::WaveWire;

    // Hand-rolled fixture of what `#[wavedb(NonUnique)]` with one
    // `#[wavedb::list]` on `name` generates (core can't use the proc-macro).
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Row {
        name: String,
        note: u64,
    }

    impl WaveDbStruct for Row {
        const STRUCT_HASH: u64 = 0x115_0001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct RowPivot {
        permission: Option<PermissionRef>,
    }

    impl Pivot for RowPivot {
        const STRUCT_HASH: u64 = 0x115_0002;
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
    }

    impl NonUniqueStruct for Row {
        type Pivot = RowPivot;
        const NUM_LISTS: usize = 1;
        fn list_key(&self, index: usize) -> Vec<u8> {
            match index {
                0 => self.name.key_bytes(),
                _ => Vec::new(),
            }
        }
    }

    fn tenant() -> U48 {
        U48::new(5).unwrap()
    }

    fn row(name: &str) -> Row {
        Row {
            name: name.into(),
            note: 0,
        }
    }

    /// Assert a list's count, its pages and the living set agree.
    ///
    /// This is the invariant a pager rests on: `_len` counts the list and
    /// `_at_page` skips into it, so a count or a skip that drifted from the
    /// entries does not fail, it silently returns the wrong rows.
    async fn counts_agree(store: &MemStore, col: &Collection<Row>) {
        let list = col.list_chain(0).unwrap();
        let held = list.collect(store).await.unwrap();
        let living = col.recency_chain().len(store).await.unwrap();
        assert_eq!(col.list_len(store, 0).await.unwrap(), held.len() as u64);
        assert_eq!(living, held.len() as u64, "the list and the living set");
        for at in 0..=held.len() {
            let page: Vec<Id> = col
                .listed_at(store, 0, at as u64)
                .map_ok(|(id, _)| id)
                .try_collect()
                .await
                .unwrap();
            let want: Vec<Id> = held[at..]
                .iter()
                .map(|(k, _)| k.rec.to_id(col.tenant()))
                .collect();
            assert_eq!(page, want, "a page from row {at}");
        }
    }

    /// Assert every entry a list holds is byte-identical to the record at its
    /// anchor.
    ///
    /// A list stores **whole records**, so it is a derived copy that can drift
    /// from its source — the one invariant RFC 0050 names as new, and the one a
    /// count check cannot see: a stale body has exactly the right length.
    async fn payloads_agree(
        store: &MemStore,
        chain: &Chain<Vec<u8>>,
        tenant: U48,
        what: &str,
    ) {
        for (key, payload) in chain.collect(store).await.unwrap() {
            let anchor = Store::get(store, key.rec.to_id(tenant))
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{what}: names an absent record"));
            assert_eq!(
                payload, anchor,
                "{what}: the list's copy diverged from its anchor"
            );
        }
    }

    /// A collection's list, checked whole — counts, pages and bytes.
    async fn all_counts_agree(store: &MemStore, col: &Collection<Row>) {
        counts_agree(store, col).await;
        // Only a declared list duplicates bytes, so only a list can drift from
        // its anchor; the built-in chain holds pointers and cannot.
        let list = col.list_chain(0).unwrap();
        payloads_agree(store, &list, col.tenant(), "list 0").await;
    }

    // Every list write rides the collection's one batch: the record write,
    // the recency entry, every declared list. A plan that forgot one in that
    // traffic would leave a list that reads and a pager that lies.
    #[test]
    fn a_list_counts_and_pages_through_a_collection_mutation() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Row>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Row>::at(pivot, tenant());

            let mut ids = Vec::new();
            for n in 0..12u32 {
                ids.push(
                    col.insert(&store, &row(&format!("r{n:02}")))
                        .await
                        .unwrap(),
                );
                all_counts_agree(&store, &col).await;
            }

            // A save relocates in the built-in chain (its key is the authoring
            // instant, which just changed) and, when the ordering property
            // changed, in the list too.
            for (i, id) in ids.iter().enumerate().take(6) {
                col.save(&store, *id, &row(&format!("s{i:02}")))
                    .await
                    .unwrap();
                all_counts_agree(&store, &col).await;
            }

            // And down to empty.
            for id in &ids {
                assert!(col.remove(&store, *id).await.unwrap());
                all_counts_agree(&store, &col).await;
            }
        });
    }

    // A save that leaves the ordering property alone still has to rewrite the
    // record in every list — the bytes are duplicated there, and only the
    // *position* is unchanged.
    //
    // This is the failure a count check cannot see: the entry stays where it
    // was, the count is right, and the body is last week's. Found by mutation — skipping the list when
    // `old_key == new_key` reads like an obvious optimisation and passed the
    // entire suite.
    #[test]
    fn a_save_refreshes_a_list_whose_order_it_did_not_change() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Row>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Row>::at(pivot, tenant());

            let mut ids = Vec::new();
            for n in 0..6u32 {
                ids.push(
                    col.insert(&store, &row(&format!("r{n:02}")))
                        .await
                        .unwrap(),
                );
            }

            // Same name, new note: the list must not move the record, and must
            // still carry the new bytes.
            for (n, id) in ids.iter().enumerate() {
                col.save(
                    &store,
                    *id,
                    &Row {
                        name: format!("r{n:02}"),
                        note: 7,
                    },
                )
                .await
                .unwrap();
            }
            all_counts_agree(&store, &col).await;

            let list = col.list_chain(0).unwrap();
            let notes: Vec<u64> = list
                .collect(&store)
                .await
                .unwrap()
                .iter()
                .map(|(_, bytes)| {
                    crate::record::decode_record::<Row>(Row::STRUCT_HASH, bytes)
                        .unwrap()
                        .1
                        .note
                })
                .collect();
            assert_eq!(
                notes,
                vec![7; 6],
                "the list served bodies from before the save"
            );
        });
    }

    // The same invariant under the arrival order that stresses it hardest: a
    // list is keyed by a domain value, so records land in the *middle* of the
    // list rather than at its growth end.
    #[test]
    fn a_list_keeps_its_counts_under_middle_insertions() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Row>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Row>::at(pivot, tenant());

            // Descending names: every arrival sorts below everything already
            // in the list, while the built-in chain only ever appends at its
            // end.
            let mut ids = Vec::new();
            for n in (0..14u32).rev() {
                ids.push(
                    col.insert(&store, &row(&format!("r{n:02}")))
                        .await
                        .unwrap(),
                );
                all_counts_agree(&store, &col).await;
            }

            // Removing from the middle outward.
            for id in ids.iter().skip(3).take(8) {
                assert!(col.remove(&store, *id).await.unwrap());
                all_counts_agree(&store, &col).await;
            }
        });
    }
}
