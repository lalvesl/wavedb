//! Which **structures** a collection has, and how they are created.
//!
//! Split from [`collection`](crate::collection) for the file budget.
//!
//! Every collection has three chains of the **same shape** — instant-keyed
//! segments holding ids and nothing else ([RFC 0054]): the **recency** chain
//! (one entry per living record, at its live version's instant) and the
//! **removal log** (one per removed record, at its removal instant), which
//! together answer "what changed" and "what died"; plus one chain per declared
//! `#[wavedb::list]`, which is the only kind that carries records.
//!
//! Everything here answers "open the structure this `Pivot` names"; the creator
//! at the end is where they are made.
//!
//! [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::id::Id;
use crate::index::{
    BpTree, Chain, ChainRoots, DEFAULT_DEAD_MIN, Lane, Pivot, Roots, SecKey,
};
use crate::local_id::LocalId;
use crate::record::{encode_envelope, mint_floored_id};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;
use crate::u48::U48;

impl<T: NonUniqueStruct> Collection<T> {
    /// A posting-tree handle at `root` with the same capacities.
    pub(crate) fn sec_tree(&self, root: LocalId) -> BpTree<SecKey> {
        BpTree::at(root, self.tenant)
            .with_caps(self.leaf_cap, self.internal_cap)
    }

    /// A **secondary chain** at `roots` ([RFC 0075]): the recency chain's
    /// shape — `[field][anchor]` keys, no payload ([RFC 0054]), a sparse
    /// index above — keyed by a declared field instead of an instant.
    ///
    /// Pointer entries, so the removal log's kind of capacity: there are no
    /// records in here to paginate.
    ///
    /// [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains-WIP.md
    pub(crate) fn sec_chain(&self, roots: ChainRoots) -> Chain<()> {
        self.pointer_chain(roots, Lane::Secondary)
    }

    /// A payload-free chain at `roots` in `lane`, at the index capacity.
    fn pointer_chain(&self, roots: ChainRoots, lane: Lane) -> Chain<()> {
        Chain::at(
            roots.head,
            roots.tail,
            roots.index,
            self.tenant,
            T::STRUCT_HASH,
            lane,
        )
        .with_min(self.index_min)
    }

    /// The secondary chains this pivot declares, in declaration order.
    pub(crate) fn sec_chains(&self, pivot: &T::Pivot) -> Vec<Chain<()>> {
        pivot
            .secondaries()
            .iter()
            .map(|roots| self.sec_chain(*roots))
            .collect()
    }

    /// The **fuzzy posting trees** this pivot declares, in declaration order
    /// ([RFC 0056]).
    ///
    /// Structurally a secondary index and nothing more: the same `BpTree`, the
    /// same `SecKey`, the same capacities. What differs is only what the key
    /// means — one gram of the indexed value rather than the whole value — and
    /// that a record contributes many keys instead of one.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    pub(crate) fn fuzzy_trees(&self, pivot: &T::Pivot) -> Vec<BpTree<SecKey>> {
        pivot
            .fuzzy()
            .iter()
            .map(|root| self.sec_tree(*root))
            .collect()
    }

    /// The **record chain** this pivot names: one entry per living record, in
    /// modification order, with a sparse index above it (RFC 0050).
    ///
    /// The entry carries **no payload** ([RFC 0054]) — a `SecKey` already holds
    /// `rec`, the anchor, and the record lives there and nowhere else. So one
    /// segment read gives membership and order, and there is no derived copy
    /// that could disagree with its source. Duplication is what a
    /// `#[wavedb::list]` asks for, and only that.
    ///
    /// [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md
    pub(crate) fn recency_chain(&self, pivot: &T::Pivot) -> Chain<()> {
        let roots = pivot.recency();
        Chain::at(
            roots.head,
            roots.tail,
            roots.index,
            self.tenant,
            T::STRUCT_HASH,
            Lane::Recency,
        )
        // Pointer entries are ~18 bytes, so this chain wants the removal
        // log's kind of capacity, not a page size — there are no records in
        // here to paginate.
        .with_min(DEFAULT_DEAD_MIN)
    }

    /// The **removal log** this pivot names — the same chain shape with no
    /// index, keyed by removal instant, payload-free.
    pub(crate) fn dead_log(&self, pivot: &T::Pivot) -> Chain<()> {
        let roots = pivot.removals();
        Chain::log_at(
            roots.head,
            roots.tail,
            self.tenant,
            T::STRUCT_HASH,
            Lane::Dead,
        )
    }

    /// The **declared lists** this pivot names, in declaration order — the same
    /// chain shape as the built-in one, sorted by a declared property instead of
    /// by modification instant (RFC 0051).
    ///
    /// These own `Lane::Records` outright: they are the only chains that carry
    /// record envelopes, so one directory and one zstd dictionary model one kind
    /// of content — which is exactly what a lane is for. Recency and the removal
    /// log, being id-only, are filed elsewhere.
    pub(crate) fn list_chains(&self, pivot: &T::Pivot) -> Vec<Chain<Vec<u8>>> {
        pivot
            .lists()
            .iter()
            .enumerate()
            .map(|(i, r)| self.record_chain(*r, T::list_page(i)))
            .collect()
    }

    /// One declared list's chain, by declaration index.
    ///
    /// # Errors
    /// [`Error::ListOutOfRange`] when the pivot declares no such list.
    pub(crate) fn list_chain(
        &self,
        pivot: &T::Pivot,
        index: usize,
    ) -> Result<Chain<Vec<u8>>> {
        pivot
            .lists()
            .get(index)
            .map(|r| self.record_chain(*r, T::list_page(index)))
            .ok_or(Error::ListOutOfRange(index))
    }

    /// A record-lane chain handle at `roots` holding `min`…`2*min` records —
    /// the built-in chain and every declared list are the same structure,
    /// differing only in the key they are laid out by and the capacity they
    /// were declared at.
    fn record_chain(&self, roots: ChainRoots, min: usize) -> Chain<Vec<u8>> {
        Chain::at(
            roots.head,
            roots.tail,
            roots.index,
            self.tenant,
            T::STRUCT_HASH,
            Lane::Records,
        )
        // The declared `page = N` (RFC 0052), or the engine default. It folds
        // into the `STRUCT_HASH`, so every segment this chain ever held was
        // laid out at this same capacity.
        .with_min(min)
    }

    /// Declared list `i`'s sort key for `value` stored at `id`.
    ///
    /// Tie-broken by the **anchor**, not by the live version's instant: the
    /// anchor never changes, so a save relocates the record only when the
    /// declared property did (RFC 0051).
    pub(crate) fn list_key(value: &T, i: usize, id: Id) -> SecKey {
        SecKey {
            field: value.list_key(i),
            rec: LocalId::from_id(id),
        }
    }

    /// Secondary index `i`'s key for `value` stored at `id`.
    pub(crate) fn sec_key(value: &T, i: usize, id: Id) -> SecKey {
        SecKey {
            field: value.secondary_key(i),
            rec: LocalId::from_id(id),
        }
    }

    /// Create a new, empty collection under `tenant`: the recency chain and
    /// its sparse index, the removal log, one chain per
    /// `#[wavedb::pivot(...)]`, and the `Pivot` record pointing at them all,
    /// committed in one atomic batch. Returns the pivot's `LocalId` — the caller stores it (via the
    /// generated `{Name}PivotId`) in an owning record.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn create<S: Store>(store: &S, tenant: U48) -> Result<LocalId> {
        let pivot = Self::mint_pivot(tenant);
        Self::create_at(store, tenant, pivot).await?;
        Ok(pivot)
    }

    /// The id a new collection of this type gets — [`create`](Self::create)'s
    /// half that needs no store. A node mints it where a request arrived and
    /// creates the collection where that id places it (RFC 0067).
    #[must_use]
    pub fn mint_pivot(tenant: U48) -> LocalId {
        LocalId::from_id(mint_floored_id(
            tenant,
            <T::Pivot as Pivot>::STRUCT_HASH,
            0, // a pivot's id is pure addressing — no cursor scans it
        ))
    }

    /// [`create`](Self::create) at a pivot from [`mint_pivot`](Self::mint_pivot).
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub async fn create_at<S: Store>(
        store: &S,
        tenant: U48,
        pivot: LocalId,
    ) -> Result<()> {
        Self::create_rooted(store, tenant, pivot.to_id(tenant)).await
    }

    /// [`create`](Self::create)'s body with the pivot's identity supplied
    /// instead of minted — the seam [`adopt_pivot`](Self::adopt_pivot)
    /// bootstraps a cache-local copy of a **node-minted** collection through.
    pub(crate) async fn create_rooted<S: Store>(
        store: &S,
        tenant: U48,
        pivot_id: Id,
    ) -> Result<()> {
        // One chain per declared `#[wavedb::pivot(...)]` index (RFC 0075),
        // in its own lane. The recency chain IS the membership set and the
        // modification order at once (RFC 0050 phase 5c folded them together).
        let mut batch = Vec::new();
        let mut sec_roots = Vec::with_capacity(T::NUM_SECONDARIES);
        for _ in 0..T::NUM_SECONDARIES {
            let (chain, writes) = Chain::<()>::plan_create(
                tenant,
                T::STRUCT_HASH,
                Lane::Secondary,
            );
            sec_roots.push(chain.roots());
            batch.extend(writes);
        }
        // The **recency chain**: one pointer per living record, keyed by the
        // instant its live version was authored (RFC 0054). It is the same
        // shape as the removal log — a chain of ids and nothing else — and for
        // the same reason: the key already names the anchor, and the record
        // lives there. The two together are "what changed, and what died".
        //
        // No `with_min` here on purpose: creation only seeds the one empty
        // segment that is both endpoints, and a capacity has nothing to say
        // about an empty segment. `recency_chain` applies the capacity on every
        // subsequent open, which is where splits and merges are decided.
        let (recency_chain, record_writes) =
            Chain::<()>::plan_create(tenant, T::STRUCT_HASH, Lane::Recency);
        let recency = recency_chain.roots();
        batch.extend(record_writes);
        // The removal log: the same shape again, minus the index, since nothing
        // ever *searches* it.
        let (removals, removal_writes) =
            Chain::<()>::plan_create_log(tenant, T::STRUCT_HASH, Lane::Dead);
        batch.extend(removal_writes);
        // One more chain per `#[wavedb::list(...)]` — same shape, same lane,
        // sorted by a declared property, and the **only** one of these chains
        // that carries records (RFC 0051). Declaring one is how a collection
        // asks for duplication; declaring none is how it pays for nothing.
        let mut list_roots = Vec::with_capacity(T::NUM_LISTS);
        for _ in 0..T::NUM_LISTS {
            let (chain, writes) = Chain::<Vec<u8>>::plan_create(
                tenant,
                T::STRUCT_HASH,
                Lane::Records,
            );
            list_roots.push(chain.roots());
            batch.extend(writes);
        }
        // One posting tree per `#[wavedb::fuzzy]` (RFC 0056) — a `BpTree` like
        // a secondary, because a posting is a key and an anchor and nothing
        // else. Nothing here duplicates a record.
        let mut fuzzy_roots = Vec::with_capacity(T::NUM_FUZZY);
        for _ in 0..T::NUM_FUZZY {
            let (tree, write) = BpTree::<SecKey>::plan_create(tenant);
            fuzzy_roots.push(tree.root());
            batch.push(write);
        }
        let pivot_record = T::Pivot::default().replace_roots(Roots {
            secondaries: &sec_roots,
            recency,
            removals: removals.log_roots(),
            lists: &list_roots,
            fuzzy: &fuzzy_roots,
        });
        batch.push(Write::Put(
            pivot_id,
            encode_envelope(T::Pivot::STRUCT_HASH, &pivot_record),
        ));
        store.apply(&batch).await
    }
}
