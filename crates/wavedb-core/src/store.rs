//! `Store` — the backend seam.
//!
//! Key→value over [`Id`] + wire bytes, with an atomic batch. This is the only
//! thing that differs native vs web; the index layer above (`Pivot`/`BpTree`) is
//! written once against this contract.
//!
//! Async, **no concrete I/O** — the page engine ([`wavedb-storage`]), the native
//! client file store, and the browser IndexedDB store each supply their own impl.
//!
//! [`wavedb-storage`]: https://docs.rs/wavedb-storage

use crate::error::Result;
use crate::id::Id;
use crate::wire::WaveWire;

/// One write inside an atomic [`Store::apply`] batch.
///
/// Derives [`WaveWire`], so a batch (`Vec<Write>`) is itself a wire value — the
/// journal in `wavedb-storage` persists batches with the checked encoding
/// instead of a hand-rolled format.
///
/// ## Why two variants name their type and one does not
///
/// An [`Id`] names no type (RFC 0063). A backend with per-type storage must
/// therefore be *told* which type a write concerns, or search every one it
/// has — which is what `Remove` and `Expect` used to force, at a page read
/// per registered type, inside the lock that serializes all writers.
///
/// `Put` needs no such field: its bytes are STRUCT_HASH-headed, so the head
/// already answers the question and a second copy could only disagree with
/// it.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum Write {
    /// Insert or overwrite `Id`'s wire bytes. The type is the STRUCT_HASH at
    /// the head of `bytes`.
    Put(Id, Vec<u8>),
    /// Delete `Id` from the storage of `struct_hash` — the type whose bytes
    /// were `Put` there.
    Remove(u64, Id),
    /// Commit-time guard: the batch applies only if `Id`, in `struct_hash`'s
    /// storage, currently holds exactly these bytes (`None` = is absent).
    /// Every `Expect` in a batch is validated against the **pre-batch** state
    /// inside the backend's atomic section; any mismatch refuses the whole
    /// batch as [`Error::Conflict`](crate::Error::Conflict) and nothing is
    /// written. A guard is not state — a durable backend validates it but
    /// never persists it.
    Expect(u64, Id, Option<Vec<u8>>),
}

impl Write {
    /// The `Id` this write targets.
    #[must_use]
    pub const fn id(&self) -> Id {
        match self {
            Self::Put(id, _) | Self::Remove(_, id) | Self::Expect(_, id, _) => {
                *id
            }
        }
    }

    /// The STRUCT_HASH this write routes to, when the variant carries it.
    /// `Put` returns `None` — its type lives in the bytes' head, and reading
    /// it is the decoding layer's job, not this accessor's.
    #[must_use]
    pub const fn struct_hash(&self) -> Option<u64> {
        match self {
            Self::Put(..) => None,
            Self::Remove(hash, _) | Self::Expect(hash, _, _) => Some(*hash),
        }
    }
}

/// Key→value backend over [`Id`] plus an **atomic batch**.
///
/// `apply` commits all-or-nothing: a multi-record op (a record **and** the
/// `BpTree` node it touches) is one batch, so a reader never sees a half-applied
/// mutation and a crash either replays the whole batch or none of it. There is no
/// separate transaction manager — the batch *is* the atomic unit.
pub trait Store {
    /// Fetch a record's wire bytes, or `None` if absent.
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>>;

    /// Type-directed fetch: `struct_hash` names the value's type (the same
    /// hash stamped at the head of its stored bytes). The typed layers above
    /// (`Collection`, `BpTree`) always know it at compile time, so a backend
    /// with per-type storage can route straight to one type's slot instead of
    /// searching a shared keyspace. The default just falls back to [`get`]
    /// — simple backends (one flat map, IndexedDB) need nothing extra.
    ///
    /// [`get`]: Store::get
    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> Result<Option<Vec<u8>>> {
        let _ = struct_hash;
        self.get(id).await
    }

    /// Apply a batch of writes atomically (all-or-nothing).
    async fn apply(&self, batch: &[Write]) -> Result<()>;

    /// Observe one committed mutation — the write paths call this right
    /// after the op's `apply` resolves, carrying the op-level meaning a
    /// raw batch cannot (see [`crate::notify`]). The default does nothing
    /// and drops `mutation` unbuilt, so ordinary stores pay nothing; the
    /// node's event-routing wrapper overrides it (M7 push).
    fn note_mutation(
        &self,
        mutation: impl FnOnce() -> crate::notify::Mutation,
    ) {
        let _ = mutation;
    }
}

/// A shared store is a store: every method forwards through the [`Rc`]. This
/// lets a wrapper own its backend by value (`W<Rc<S>>`) while another handle
/// — a maintenance loop, a seeding path — keeps its own clone of the same
/// engine. Single-thread by design (the engine futures are non-`Send`), so
/// [`Rc`] not [`Arc`].
///
/// [`Rc`]: std::rc::Rc
/// [`Arc`]: std::sync::Arc
impl<S: Store + ?Sized> Store for std::rc::Rc<S> {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        (**self).get(id).await
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> Result<Option<Vec<u8>>> {
        (**self).get_of(struct_hash, id).await
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        (**self).apply(batch).await
    }

    fn note_mutation(
        &self,
        mutation: impl FnOnce() -> crate::notify::Mutation,
    ) {
        (**self).note_mutation(mutation);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    use super::{Store, Write};
    use crate::error::Result;
    use crate::id::Id;
    use crate::u48::U48;

    /// In-memory `Store` for exercising the contract and the index layer.
    #[derive(Default)]
    struct MemStore(Mutex<BTreeMap<u128, Vec<u8>>>);

    // A test double, written in the trait's own (async) shape.
    #[allow(clippy::unused_async_trait_impl)]
    impl Store for MemStore {
        async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
            Ok(self.0.lock().unwrap().get(&id.raw()).cloned())
        }

        async fn apply(&self, batch: &[Write]) -> Result<()> {
            // No await between lock and unlock — the guard never spans a yield.
            {
                let mut map = self.0.lock().unwrap();
                // One flat keyspace, so the writes' `struct_hash` is ignored:
                // it exists for backends with per-type storage.
                for w in batch {
                    if let Write::Expect(_, id, expected) = w
                        && map.get(&id.raw()) != expected.as_ref()
                    {
                        return Err(crate::Error::Conflict(*id));
                    }
                }
                for w in batch {
                    match w {
                        Write::Put(id, bytes) => {
                            map.insert(id.raw(), bytes.clone());
                        }
                        Write::Remove(_, id) => {
                            map.remove(&id.raw());
                        }
                        Write::Expect(..) => {}
                    }
                }
            }
            Ok(())
        }
    }

    fn id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, 0)
    }

    /// Any hash: this store has one flat keyspace and ignores routing.
    const SH: u64 = 0xABCD;

    #[test]
    fn apply_is_all_or_nothing_visible() {
        futures::executor::block_on(async {
            let store = MemStore::default();
            assert_eq!(store.get(id(1)).await.unwrap(), None);

            store
                .apply(&[
                    Write::Put(id(1), vec![10, 20]),
                    Write::Put(id(2), vec![30]),
                ])
                .await
                .unwrap();
            assert_eq!(store.get(id(1)).await.unwrap(), Some(vec![10, 20]));
            assert_eq!(store.get(id(2)).await.unwrap(), Some(vec![30]));

            store
                .apply(&[Write::Remove(SH, id(1)), Write::Put(id(2), vec![99])])
                .await
                .unwrap();
            assert_eq!(store.get(id(1)).await.unwrap(), None);
            assert_eq!(store.get(id(2)).await.unwrap(), Some(vec![99]));
        });
    }

    #[test]
    fn write_id_accessor() {
        assert_eq!(Write::Put(id(7), vec![]).id(), id(7));
        assert_eq!(Write::Remove(SH, id(7)).id(), id(7));
        assert_eq!(Write::Expect(SH, id(7), None).id(), id(7));
    }

    #[test]
    fn only_the_untyped_variants_carry_a_struct_hash() {
        // `Put`'s type is the head of its bytes, so a second copy on the
        // variant could only disagree with it.
        assert_eq!(Write::Put(id(7), vec![]).struct_hash(), None);
        assert_eq!(Write::Remove(SH, id(7)).struct_hash(), Some(SH));
        assert_eq!(Write::Expect(SH, id(7), None).struct_hash(), Some(SH));
    }
}
