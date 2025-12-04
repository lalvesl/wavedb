//! Which **chains** a collection has, and how it is created.
//!
//! Split from [`collection`](crate::collection) for the file budget.
//!
//! A collection is named by its Pivot, and every chain of it is the key
//! range of a lane under that name ([RFC 0078] §7) — so a chain is opened
//! from the Pivot's id alone, and nothing is created for it:
//!
//! - the **recency** chain, one entry per living record at its live
//!   version's instant, and the **removal log**, one per removed record at
//!   its removal instant — together "what changed" and "what died" — both
//!   ids and nothing else ([RFC 0054]);
//! - one chain per `#[wavedb::pivot(...)]` secondary, per `#[wavedb::fuzzy]`
//!   posting index, and per `#[wavedb::list]` — the only kind that carries
//!   records — each under its declaration's index.
//!
//! [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md
//! [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::id::Id;
use crate::index::{Chain, Lane, Pivot, SecKey};
use crate::local_id::LocalId;
use crate::record::{encode_envelope, mint_floored_id};
use crate::store::{Store, Write};
use crate::traits::NonUniqueStruct;
use crate::u48::U48;
use crate::wire::WaveWire;

impl<T: NonUniqueStruct> Collection<T> {
    /// This collection's chain `declaration` in `lane`.
    fn chain<P: WaveWire>(&self, lane: Lane, declaration: usize) -> Chain<P> {
        Chain::at(
            self.pivot().to_id(self.tenant),
            T::STRUCT_HASH,
            lane,
            u8::try_from(declaration).unwrap_or(u8::MAX),
        )
    }

    /// Secondary index `i`'s chain ([RFC 0075]): `[field][anchor]` keys, no
    /// payload ([RFC 0054]).
    ///
    /// [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md
    /// [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md
    pub(crate) fn sec_chain(&self, i: usize) -> Chain<()> {
        self.chain(Lane::Secondary, i)
    }

    /// Every secondary chain, in declaration order.
    pub(crate) fn sec_chains(&self) -> Vec<Chain<()>> {
        (0..T::NUM_SECONDARIES).map(|i| self.sec_chain(i)).collect()
    }

    /// The **fuzzy posting chains**, in declaration order ([RFC 0056]):
    /// structurally a secondary index, one gram of the indexed value per key
    /// rather than the whole value.
    ///
    /// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
    pub(crate) fn fuzzy_chains(&self) -> Vec<Chain<()>> {
        (0..T::NUM_FUZZY)
            .map(|i| self.chain(Lane::Fuzzy, i))
            .collect()
    }

    /// The **recency chain**: one entry per living record, in modification
    /// order.
    ///
    /// The entry carries **no payload** ([RFC 0054]) — a `SecKey` already
    /// holds `rec`, the anchor, and the record lives there and nowhere else.
    ///
    /// [RFC 0054]: https://github.com/wavedb/wavedb/blob/main/rfcs/0054-no-duplication-by-default.md
    pub(crate) fn recency_chain(&self) -> Chain<()> {
        self.chain(Lane::Recency, 0)
    }

    /// The **removal log** — keyed by removal instant, payload-free.
    pub(crate) fn dead_log(&self) -> Chain<()> {
        self.chain(Lane::Dead, 0)
    }

    /// The **declared lists**, in declaration order — sorted by a declared
    /// property, each entry the record's envelope (RFC 0051).
    pub(crate) fn list_chains(&self) -> Vec<Chain<Vec<u8>>> {
        (0..T::NUM_LISTS)
            .map(|i| self.chain(Lane::Records, i))
            .collect()
    }

    /// One declared list's chain, by declaration index.
    ///
    /// # Errors
    /// [`Error::ListOutOfRange`] when the type declares no such list.
    pub(crate) fn list_chain(&self, index: usize) -> Result<Chain<Vec<u8>>> {
        if index < T::NUM_LISTS {
            Ok(self.chain(Lane::Records, index))
        } else {
            Err(Error::ListOutOfRange(index))
        }
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

    /// Create a new, empty collection under `tenant`: its `Pivot` record,
    /// one write. Returns the pivot's `LocalId` — the caller stores it (via
    /// the generated `{Name}PivotId`) in an owning record.
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
        Self::create_rooted(store, pivot.to_id(tenant)).await
    }

    /// [`create`](Self::create)'s body with the pivot's identity supplied
    /// instead of minted — the seam [`adopt_pivot`](Self::adopt_pivot)
    /// bootstraps a cache-local copy of a **node-minted** collection through.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure.
    pub(crate) async fn create_rooted<S: Store>(
        store: &S,
        pivot_id: Id,
    ) -> Result<()> {
        let record =
            encode_envelope(T::Pivot::STRUCT_HASH, &T::Pivot::default());
        store.apply(&[Write::Put(pivot_id, record)]).await
    }
}
