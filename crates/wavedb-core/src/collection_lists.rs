//! **Declared lists** ([RFC 0051]) — `#[wavedb::list(...)]`: one more chain of
//! the same records, sorted by a declared property instead of by modification
//! instant, and holding **whole records inline**.
//!
//! That last part is the whole reason to declare one, and the only duplication
//! WaveDB ever performs ([RFC 0054]): the built-in chain carries pointers, so a
//! collection that declares nothing stores each record exactly once. A list buys
//! a dense bulk read in its order, and pays a full copy for it.
//!
//! The mechanism is RFC 0050's, unchanged: the write half threads each list
//! through the same `plan_insert`/`plan_remove` the built-in chain uses, and the
//! read half is the same segment walk.
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
use crate::record::{Overlay, decode_record};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;

impl<T: NonUniqueStruct> Collection<T> {
    /// File a freshly inserted record into every declared list.
    ///
    /// Each list is an independent chain with its own segment ids, so they need
    /// no shared overlay between them — only within one chain, which
    /// `plan_insert` already arranges.
    pub(crate) async fn plan_list_inserts<S: Store>(
        &self,
        store: &S,
        batch: &mut Vec<Write>,
        lists: &mut [Chain<Vec<u8>>],
        id: Id,
        envelope: &[u8],
        value: &T,
    ) -> Result<()> {
        for (i, chain) in lists.iter_mut().enumerate() {
            let key = Self::list_key(value, i, id);
            batch.extend(
                chain.plan_insert(store, key, envelope.to_vec()).await?,
            );
        }
        Ok(())
    }

    /// Re-file a saved record in every declared list.
    ///
    /// The record's bytes are duplicated in each list, so **every** list is
    /// rewritten whether or not its property changed — that is the cost RFC 0051
    /// states plainly (a save carries K× the record's bytes). What the
    /// comparison saves is the *removal*: an unchanged property means the entry
    /// stays where it is and `plan_insert` replaces its payload in place, one
    /// segment write instead of two.
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
    pub(crate) async fn plan_list_moves<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        batch: &mut Vec<Write>,
        lists: &mut [Chain<Vec<u8>>],
        id: Id,
        envelope: &[u8],
        values: (&T, &T),
    ) -> Result<()> {
        let (old_value, value) = values;
        for (i, chain) in lists.iter_mut().enumerate() {
            let old_key = Self::list_key(old_value, i, id);
            let new_key = Self::list_key(value, i, id);
            if old_key != new_key
                && let Some(writes) =
                    chain.plan_remove(&*view, &old_key).await?
            {
                view.stage(&writes);
                batch.extend(writes);
            }
            let writes = chain
                .plan_insert(&*view, new_key, envelope.to_vec())
                .await?;
            // Staging after the *last* plan on a chain is defensive rather than
            // load-bearing — chains never share ids, so nothing later in this
            // batch reads back what was just written, and a mutation deleting
            // this line survives the suite. It stays because the discipline is
            // "stage after every plan" (the secondaries loop in
            // `collection_write` does the same), and a chain that skipped it
            // would be quietly wrong the day an operation is added after it.
            view.stage(&writes);
            batch.extend(writes);
        }
        Ok(())
    }

    /// Take a removed record out of every declared list.
    ///
    /// A list holds only living records — the anchor keeps the bytes, and the
    /// removal log keeps the event.
    pub(crate) async fn plan_list_removes<S: Store>(
        &self,
        store: &S,
        batch: &mut Vec<Write>,
        lists: &mut [Chain<Vec<u8>>],
        id: Id,
        value: &T,
    ) -> Result<()> {
        for (i, chain) in lists.iter_mut().enumerate() {
            let key = Self::list_key(value, i, id);
            if let Some(writes) = chain.plan_remove(store, &key).await? {
                batch.extend(writes);
            }
        }
        Ok(())
    }

    /// Stream every living record in declared list `index`'s order, ascending.
    ///
    /// One read per segment, records **inline** — unlike
    /// [`all`](Collection::all), which walks pointers and resolves each record
    /// at its anchor. Buying that difference is what declaring a list is for.
    /// The generated `listed_by_<fields>` wrapper calls this with the
    /// declaration's index.
    pub fn listed<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
    ) -> impl Stream<Item = Result<(Id, T)>> + 'a
    where
        T: 'a,
    {
        self.listed_from(store, index, None)
            .map_ok(|(id, _, value)| (id, value))
    }

    /// [`listed`](Self::listed) entered at global offset `offset` — the pager's
    /// "jump to page k", one sparse-index descent rather than a walk.
    ///
    /// The stream still runs to the end of the list; a caller takes the page it
    /// wants. An offset at or past the end yields nothing.
    pub fn listed_at<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<(Id, T)>> + 'a
    where
        T: 'a,
    {
        self.listed_from(store, index, Some(offset))
            .map_ok(|(id, _, value)| (id, value))
    }

    /// [`listed_at`](Self::listed_at) keeping each record's [`Metadata`].
    ///
    /// The wire `Listed` reads through this: a list segment holds the whole
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
    ) -> impl Stream<Item = Result<(Id, crate::metadata::Metadata, T)>> + 'a
    where
        T: 'a,
    {
        self.listed_from(store, index, Some(offset))
    }

    /// The living records declared list `index` holds under `bound` (over
    /// its [`IndexKey`](crate::index::IndexKey) encoding), in its order and
    /// **inline** ([RFC 0075]): one sparse-index descent to the bound's least
    /// key, then the chain while the bound holds — a secondary's read without
    /// a fetch per record. The generated `by_<fields>` of a list call this.
    ///
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md
    pub fn listed_where<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<(Id, T)>> + 'a
    where
        T: 'a,
    {
        let handle = self;
        let tenant = self.tenant();
        futures::stream::once(async move {
            let pivot = handle.load_pivot(store).await?;
            let chain = handle.list_chain(&pivot, index)?;
            let from = SecKey {
                field: bound.least().to_vec(),
                rec: LocalId::default(),
            };
            Ok::<_, Error>(chain.scan_from(store, from).try_take_while(
                move |(key, _)| {
                    futures::future::ready(Ok(bound.contains(&key.field)))
                },
            ))
        })
        .try_flatten()
        .and_then(move |(key, bytes)| async move {
            let (_, value) = decode_record::<T>(T::STRUCT_HASH, &bytes)?;
            Ok((key.rec.to_id(tenant), value))
        })
    }

    /// How many living records declared list `index` holds — the pager's
    /// "of M", one read cold (the index root carries the sum).
    ///
    /// # Errors
    /// [`Error::ListOutOfRange`] for an undeclared index, or a [`Store`]
    /// failure.
    pub async fn list_len<S: Store>(
        &self,
        store: &S,
        index: usize,
    ) -> Result<u64> {
        let pivot = self.load_pivot(store).await?;
        let chain = self.list_chain(&pivot, index)?;
        match chain.index() {
            Some(tree) => tree.total(store).await,
            None => Ok(0),
        }
    }

    /// Every list reader: walk the chain forward from `offset` (or from the
    /// head), yielding whole segments' worth of records with their metadata.
    fn listed_from<'a, S: Store>(
        self,
        store: &'a S,
        index: usize,
        offset: Option<u64>,
    ) -> impl Stream<Item = Result<(Id, crate::metadata::Metadata, T)>> + 'a
    where
        T: 'a,
    {
        let handle = self;
        futures::stream::once(async move {
            let pivot = handle.load_pivot(store).await?;
            let chain = handle.list_chain(&pivot, index)?;
            let tenant = handle.tenant();
            // Where the walk starts, and how far into that first segment: an
            // offset descent when one was asked for, the head otherwise.
            let start = match (offset.filter(|at| *at > 0), chain.index()) {
                (Some(at), Some(tree)) => tree
                    .find_offset(store, at)
                    .await?
                    .map(|(slot, within)| (slot.seg, within)),
                // No index to descend, so no way to skip: an index-less chain
                // is the removal log's shape, which is never a declared list.
                (Some(_), None) => None,
                // Offset zero is the head by definition — no descent, and no
                // dependence on the index for the common whole-list read.
                (None, _) => Some((chain.head(), 0)),
            };
            Ok::<_, Error>(
                futures::stream::try_unfold(start, move |cursor| async move {
                    let Some((id, skip)) = cursor else {
                        return Ok::<_, Error>(None);
                    };
                    let seg = chain.segment(store, id).await?;
                    let mut page = Vec::with_capacity(seg.len());
                    // Ascending, and no reverse: a declared list reads in its
                    // own order, where recency reads backwards.
                    for (key, bytes) in
                        seg.entries().skip(usize::try_from(skip).unwrap_or(0))
                    {
                        let (meta, value) =
                            decode_record::<T>(T::STRUCT_HASH, bytes)?;
                        page.push(Ok((key.rec.to_id(tenant), meta, value)));
                    }
                    Ok(Some((
                        futures::stream::iter(page),
                        seg.next().map(|next| (next, 0)),
                    )))
                })
                .try_flatten(),
            )
        })
        .try_flatten()
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use crate::collection::Collection;
    use crate::index::mem_store::MemStore;
    use crate::index::{Chain, ChainRoots, IndexKey, LogRoots, Pivot, Roots};
    use crate::permission::PermissionRef;
    use crate::store::Store;
    use crate::traits::{NonUniqueStruct, Shape, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::WaveWire;

    // Hand-rolled fixture of what `#[wavedb(NonUnique, page = 4)]` with one
    // `#[wavedb::list]` on `name` generates (core can't use the proc-macro).
    // `page = 4` puts the split at 8 and the merge at 2, so a dozen records
    // exercise both without a long setup.
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
        recency: ChainRoots,
        removals: LogRoots,
        lists: [ChainRoots; 1],
        permission: Option<PermissionRef>,
    }

    impl Pivot for RowPivot {
        const STRUCT_HASH: u64 = 0x115_0002;
        fn secondaries(&self) -> &[ChainRoots] {
            &[]
        }
        fn recency(&self) -> ChainRoots {
            self.recency
        }
        fn removals(&self) -> LogRoots {
            self.removals
        }
        fn lists(&self) -> &[ChainRoots] {
            &self.lists
        }
        fn permission(&self) -> Option<&PermissionRef> {
            self.permission.as_ref()
        }
        fn replace_roots(&self, roots: Roots<'_>) -> Self {
            let mut lists = self.lists;
            lists.copy_from_slice(roots.lists);
            Self {
                recency: roots.recency,
                removals: roots.removals,
                lists,
                permission: self.permission.clone(),
            }
        }
    }

    impl NonUniqueStruct for Row {
        type Pivot = RowPivot;
        const PAGE: usize = 4;
        const NUM_LISTS: usize = 1;
        fn list_page(index: usize) -> usize {
            match index {
                0 => 16,
                _ => Self::PAGE,
            }
        }
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

    /// Assert the sparse index's counts agree with the segments they name —
    /// per segment and in aggregate.
    ///
    /// This is the invariant a pager rests on: `_at_page` descends by count, so
    /// a count that has drifted from its segment does not fail, it silently
    /// returns the wrong rows.
    async fn counts_agree<P: crate::wire::WaveWire>(
        store: &MemStore,
        chain: &Chain<P>,
        what: &str,
    ) {
        let index = chain.index().expect("a record chain always has an index");
        let mut total = 0;
        let mut cursor = Some(chain.head());
        while let Some(id) = cursor {
            let seg = chain.segment(store, id).await.unwrap();
            if let Some(first) = seg.first_key() {
                let slot =
                    index.find(store, first).await.unwrap().unwrap_or_else(
                        || panic!("{what}: segment not indexed"),
                    );
                assert_eq!(slot.seg, id, "{what}: index names another segment");
                assert_eq!(
                    slot.count,
                    seg.len() as u64,
                    "{what}: count drifted from segment {id:?}"
                );
            }
            total += seg.len() as u64;
            cursor = seg.next();
        }
        assert_eq!(
            index.total(store).await.unwrap(),
            total,
            "{what}: the root's sum disagrees with the chain"
        );
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

    /// Both of a collection's record chains, checked together — counts and
    /// bytes.
    async fn all_counts_agree(store: &MemStore, col: &Collection<Row>) {
        let pivot = col.load_pivot(store).await.unwrap();
        let records = col.recency_chain(&pivot);
        let list = col.list_chain(&pivot, 0).unwrap();
        counts_agree(store, &records, "built-in chain").await;
        counts_agree(store, &list, "list 0").await;
        // Only a declared list duplicates bytes, so only a list can drift from
        // its anchor; the built-in chain holds pointers and cannot.
        payloads_agree(store, &list, col.tenant(), "list 0").await;
    }

    // RFC 0052 asked whether an index entry and its segment can ever disagree,
    // and answered it with an argument: the single-batch rule makes it
    // impossible. The chain's own tests check that over a bare `Chain`; this
    // checks it where the batch is **composed** — the collection interleaves
    // the record write, the built-in chain, every declared list and the pivot
    // into one `apply`, and a plan that forgot an index write in that traffic
    // would still leave a chain that reads correctly and a pager that lies.
    #[test]
    fn index_counts_track_the_segments_through_a_collection_mutation() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Row>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Row>::at(pivot, tenant());

            // Past 2N = 8 in both chains, so both have really split.
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
            // changed, in the list too — two counts revised per chain.
            for (i, id) in ids.iter().enumerate().take(6) {
                col.save(&store, *id, &row(&format!("s{i:02}")))
                    .await
                    .unwrap();
                all_counts_agree(&store, &col).await;
            }

            // And down through the merges: N/2 = 2, so emptying a 12-record
            // chain folds segments repeatedly.
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
    // was, the segment length is right, the index is right, and the body is
    // last week's. Found by mutation — skipping the list when
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

            let pivot = col.load_pivot(&store).await.unwrap();
            let list = col.list_chain(&pivot, 0).unwrap();
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

    // A list is laid out at **its own** capacity, not the struct's. The two
    // chains hold the very same records, so a difference in segment count can
    // only come from the capacity — which is the whole point of separating
    // them: the built-in chain holds only pointers (RFC 0054 — no duplication
    // by default) and wants the removal log's large capacity, while a list
    // holds whole records and wants the page a view renders.
    #[test]
    fn a_list_lays_out_at_its_own_page() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Row>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Row>::at(pivot, tenant());
            for n in 0..40u32 {
                col.insert(&store, &row(&format!("r{n:02}"))).await.unwrap();
            }

            let pivot = col.load_pivot(&store).await.unwrap();
            let built_in = segments(&store, &col.recency_chain(&pivot)).await;
            let list =
                segments(&store, &col.list_chain(&pivot, 0).unwrap()).await;

            // 40 records, three capacities, three answers — which is what makes
            // this an assertion rather than a coincidence:
            //   the struct's `page = 4`   → ~10 segments
            //   the list's   `page = 16`  → 2…4 segments   ← what it declared
            //   the pointer chain's 256   → 1 segment
            assert!(
                (2..=4).contains(&list),
                "the list is not laid out at its own page = 16: {list} \
                 segment(s) — a 4 would give ~10, a 256 would give 1"
            );
            assert_eq!(
                built_in, 1,
                "the built-in chain holds pointers, so it takes the removal \
                 log's capacity rather than the struct's page"
            );
        });
    }

    /// How many segments a chain currently holds.
    async fn segments<P: crate::wire::WaveWire>(
        store: &MemStore,
        chain: &Chain<P>,
    ) -> usize {
        let mut n = 0;
        let mut cursor = Some(chain.head());
        while let Some(id) = cursor {
            n += 1;
            cursor = chain.segment(store, id).await.unwrap().next();
        }
        n
    }

    // The same invariant under the arrival order that stresses it hardest: a
    // list is keyed by a domain value, so records land in the *middle* of the
    // chain rather than at its growth end, and every insert can split a segment
    // that is not an endpoint.
    #[test]
    fn a_list_keeps_its_counts_under_middle_insertions() {
        block_on(async {
            let store = MemStore::default();
            let pivot =
                Collection::<Row>::create(&store, tenant()).await.unwrap();
            let col = Collection::<Row>::at(pivot, tenant());

            // Descending names: every arrival sorts below everything already
            // in the list, so the list's head splits over and over while the
            // built-in chain only ever appends at its tail.
            let mut ids = Vec::new();
            for n in (0..14u32).rev() {
                ids.push(
                    col.insert(&store, &row(&format!("r{n:02}")))
                        .await
                        .unwrap(),
                );
                all_counts_agree(&store, &col).await;
            }

            // Removing from the middle outward drains interior segments, which
            // is the merge case the endpoints never exercise.
            for id in ids.iter().skip(3).take(8) {
                assert!(col.remove(&store, *id).await.unwrap());
                all_counts_agree(&store, &col).await;
            }
        });
    }
}
