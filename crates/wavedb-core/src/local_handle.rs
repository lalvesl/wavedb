//! [`LocalHandle`] — the engine-local [`DbHandle`], split from
//! [`handle`](crate::handle) for the file budget.
//!
//! Every op here is a direct [`Collection`] / record call against a borrowed
//! [`Store`]: no transport, no cache, no buffering. It is what core and
//! storage tests drive typed generated code through, and what an in-process
//! embedding uses — and it is the reference the remote contexts (the client
//! `Db`, the node's `ServerDb`) have to agree with, since the same generated
//! call sites resolve against all three.

use futures::{Stream, StreamExt, TryStreamExt};

use crate::collection::Collection;
use crate::durability::Durability;
use crate::error::Error;
use crate::fuzzy::{Fuzzy, Scored};
use crate::handle::DbHandle;
use crate::id::Id;
use crate::index::Bound;
use crate::local_id::LocalId;
use crate::metadata::Metadata;
use crate::record;
use crate::store::Store;
use crate::traits::{NonUniqueStruct, UniqueStruct};
use crate::u48::U48;
use crate::wire::Wire;

/// A [`DbHandle`] over a borrowed [`Store`] — the engine-local context.
///
/// Core/storage tests and in-process embeddings drive typed code through it;
/// every op is a direct [`Collection`] / record call.
#[derive(Debug)]
pub struct LocalHandle<'a, S> {
    store: &'a S,
    tenant: U48,
}

// Manual impls: a derive would demand `S: Clone`, but only the reference is
// copied.
impl<S> Clone for LocalHandle<'_, S> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<S> Copy for LocalHandle<'_, S> {}

impl<'a, S: Store> LocalHandle<'a, S> {
    /// A local context over `store`, bound to `tenant`.
    #[must_use]
    pub const fn new(store: &'a S, tenant: U48) -> Self {
        Self { store, tenant }
    }

    /// The backing store.
    #[must_use]
    pub const fn store(&self) -> &'a S {
        self.store
    }

    /// This type's collection engine handle at `pivot`.
    fn col<T: NonUniqueStruct>(&self, pivot: LocalId) -> Collection<T> {
        Collection::at(pivot, self.tenant)
    }

    /// The backing store, its batches applied durably — what the
    /// `_durable` operations write through.
    const fn durable(&self) -> Durability<&'a S> {
        Durability::new(self.store, true)
    }
}

impl<S: Store> DbHandle for LocalHandle<'_, S> {
    type Error = Error;

    fn tenant(&self) -> U48 {
        self.tenant
    }

    fn as_tenant(&self, tenant: U48) -> Self {
        Self {
            store: self.store,
            tenant,
        }
    }

    async fn get_unique<T: UniqueStruct>(
        &self,
    ) -> Result<Option<Wire<T>>, Error> {
        record::get_unique(self.store, self.tenant).await
    }

    async fn save_unique<T: UniqueStruct>(
        &self,
        value: &T,
    ) -> Result<(), Error> {
        record::save_unique(self.store, self.tenant, value).await
    }

    async fn save_unique_durable<T: UniqueStruct>(
        &self,
        value: &T,
    ) -> Result<(), Error> {
        record::save_unique(&self.durable(), self.tenant, value).await
    }

    fn unique_history<T: UniqueStruct + 'static>(
        &self,
    ) -> impl Stream<Item = Result<(Metadata, Wire<T>), Error>> {
        record::unique_history(self.store, self.tenant)
    }

    async fn create_pivot<T: NonUniqueStruct>(&self) -> Result<LocalId, Error> {
        Collection::<T>::create(self.store, self.tenant).await
    }

    async fn create_pivot_durable<T: NonUniqueStruct>(
        &self,
    ) -> Result<LocalId, Error> {
        Collection::<T>::create(&self.durable(), self.tenant).await
    }

    async fn insert<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id, Error> {
        self.col::<T>(pivot).insert(self.store, value).await
    }

    async fn insert_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        value: &T,
    ) -> Result<Id, Error> {
        self.col::<T>(pivot).insert(&self.durable(), value).await
    }

    async fn get_record<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<Option<Wire<T>>, Error> {
        self.col::<T>(pivot).get(self.store, id).await
    }

    async fn update<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<(), Error> {
        self.col::<T>(pivot).save(self.store, id, value).await
    }

    async fn update_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
        value: &T,
    ) -> Result<(), Error> {
        self.col::<T>(pivot).save(&self.durable(), id, value).await
    }

    async fn remove<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool, Error> {
        self.col::<T>(pivot).remove(self.store, id).await
    }

    async fn remove_durable<T: NonUniqueStruct>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> Result<bool, Error> {
        self.col::<T>(pivot).remove(&self.durable(), id).await
    }

    fn all<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
    ) -> impl Stream<Item = Result<Wire<T>, Error>> {
        self.col::<T>(pivot)
            .all(self.store)
            .map_ok(|(_, value)| value)
    }

    fn search_by<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<Wire<T>, Error>> {
        self.col::<T>(pivot)
            .search_by(self.store, index, bound)
            .map_ok(|(_, value)| value)
    }

    fn listed_where<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        bound: Bound,
    ) -> impl Stream<Item = Result<Wire<T>, Error>> {
        self.col::<T>(pivot)
            .listed_where(self.store, index, bound)
            .map_ok(|(_, value)| value)
    }

    fn listed<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
    ) -> impl Stream<Item = Result<Wire<T>, Error>> {
        self.col::<T>(pivot)
            .listed_at(self.store, index, offset)
            .map_ok(|(_, value)| value)
    }

    fn listed_page<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        offset: u64,
        limit: u32,
    ) -> impl Stream<Item = Result<Wire<T>, Error>> {
        // Engine-local: the bound is just a `take`, since there is no exchange
        // to size. It exists so the same call is one round trip remotely.
        self.listed::<T>(pivot, index, offset).take(limit as usize)
    }

    async fn list_len<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
    ) -> Result<u64, Error> {
        self.col::<T>(pivot).list_len(self.store, index).await
    }

    async fn fuzzy_search<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        index: usize,
        query: &str,
        mode: Fuzzy,
        limit: usize,
    ) -> Result<Vec<Scored<(Id, Wire<T>)>>, Error> {
        self.col::<T>(pivot)
            .fuzzy_search(self.store, index, query, mode, limit)
            .await
    }

    fn record_history<T: NonUniqueStruct + 'static>(
        &self,
        pivot: LocalId,
        id: Id,
    ) -> impl Stream<Item = Result<(Metadata, Wire<T>), Error>> {
        self.col::<T>(pivot).history(self.store, id)
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use futures::executor::block_on;

    use super::{DbHandle, LocalHandle};
    use crate::index::mem_store::MemStore;
    use crate::index::{ChainRoots, IndexKey, LogRoots, Pivot, Roots};
    use crate::permission::PermissionRef;
    use crate::traits::{NonUniqueStruct, Shape, UniqueStruct, WaveDbStruct};
    use crate::u48::U48;
    use crate::wire::WaveWire;

    // Hand-rolled fixtures — exactly what `#[wavedb]` generates (core can't
    // use the proc-macro; it lives downstream).
    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Settings {
        volume: u64,
    }
    impl WaveDbStruct for Settings {
        const STRUCT_HASH: u64 = 0x5E77_1001;
        const SHAPE: Shape = Shape::Unique;
        type PivotId = ();
    }
    impl UniqueStruct for Settings {}

    #[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
    struct Note {
        label: String,
        n: u64,
    }
    impl WaveDbStruct for Note {
        const STRUCT_HASH: u64 = 0x2077_1001;
        const SHAPE: Shape = Shape::NonUnique;
        type PivotId = ();
    }
    impl NonUniqueStruct for Note {
        type Pivot = NotePivot;
        const NUM_SECONDARIES: usize = 1;
        fn secondary_key(&self, index: usize) -> Vec<u8> {
            match index {
                0 => self.label.key_bytes(),
                _ => Vec::new(),
            }
        }
    }

    #[derive(Debug, Clone, Default, PartialEq, Eq, WaveWire)]
    struct NotePivot {
        recency: ChainRoots,
        removals: LogRoots,
        secondaries: [ChainRoots; 1],
        permission: Option<PermissionRef>,
    }
    impl Pivot for NotePivot {
        const STRUCT_HASH: u64 = 0x2077_1002;
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

    fn tenant() -> U48 {
        U48::from(7u32)
    }

    #[test]
    fn unique_roundtrip_and_history_through_the_handle() {
        block_on(async {
            let store = MemStore::default();
            let db = LocalHandle::new(&store, tenant());

            assert_eq!(
                db.get_unique::<Settings>()
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                None
            );
            db.save_unique(&Settings { volume: 3 }).await.unwrap();
            db.save_unique(&Settings { volume: 7 }).await.unwrap();
            assert_eq!(
                db.get_unique::<Settings>()
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                Some(Settings { volume: 7 })
            );

            let versions: Vec<(crate::metadata::Metadata, Settings)> = db
                .unique_history()
                .map_ok(|(a, w)| (a, w.decode().unwrap()))
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                versions.iter().map(|(_, s)| s.volume).collect::<Vec<_>>(),
                vec![7, 3],
                "history walks newest-first"
            );

            // Another tenant's context sees nothing — same store, own space.
            let other = db.as_tenant(U48::from(8u32));
            assert_eq!(
                other
                    .get_unique::<Settings>()
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                None
            );
            assert_eq!(other.tenant(), U48::from(8u32));
        });
    }

    #[test]
    fn collection_lifecycle_through_the_handle() {
        block_on(async {
            let store = MemStore::default();
            let db = LocalHandle::new(&store, tenant());
            let pivot = db.create_pivot::<Note>().await.unwrap();

            let note = |label: &str, n| Note {
                label: label.into(),
                n,
            };
            let a = db.insert(pivot, &note("red", 1)).await.unwrap();
            let b = db.insert(pivot, &note("blue", 2)).await.unwrap();

            let walked: Vec<Note> = db
                .all::<Note>(pivot)
                .map_ok(|w| w.decode().unwrap())
                .try_collect()
                .await
                .unwrap();
            assert_eq!(
                walked.iter().map(|v| v.n).collect::<Vec<_>>(),
                vec![2, 1],
                "walk is newest-first and yields values"
            );

            db.update(pivot, b, &note("blue", 22)).await.unwrap();
            assert_eq!(
                db.get_record::<Note>(pivot, b)
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                Some(note("blue", 22))
            );

            // Secondary lookup through the handle: index 0 = `label`.
            let reds: Vec<Note> = db
                .search_by::<Note>(
                    pivot,
                    0,
                    crate::index::Bound::Exact("red".key_bytes()),
                )
                .map_ok(|w| w.decode().unwrap())
                .try_collect()
                .await
                .unwrap();
            assert_eq!(reds, vec![note("red", 1)]);

            assert!(db.remove::<Note>(pivot, a).await.unwrap());
            assert!(!db.remove::<Note>(pivot, a).await.unwrap());
            let after: Vec<Note> = db
                .all::<Note>(pivot)
                .map_ok(|w| w.decode().unwrap())
                .try_collect()
                .await
                .unwrap();
            assert_eq!(after, vec![note("blue", 22)]);
            assert_eq!(
                db.get_record::<Note>(pivot, a)
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                Some(note("red", 1)),
                "removed record bytes survive (history)"
            );
        });
    }

    /// Each `_durable` twin is its plain write as one durable batch, and
    /// the plain writes ask nothing of the store's durability.
    #[test]
    fn durable_twins_apply_one_durable_batch_each() {
        block_on(async {
            let store = MemStore::default();
            let db = LocalHandle::new(&store, tenant());
            let note = Note {
                label: "red".into(),
                n: 1,
            };
            let pivot = db.create_pivot::<Note>().await.unwrap();
            db.save_unique(&Settings { volume: 1 }).await.unwrap();
            let a = db.insert(pivot, &note).await.unwrap();
            db.update(pivot, a, &note).await.unwrap();
            assert!(db.remove::<Note>(pivot, a).await.unwrap());
            assert_eq!(store.durable_batches(), 0);

            let pivot = db.create_pivot_durable::<Note>().await.unwrap();
            db.save_unique_durable(&Settings { volume: 2 })
                .await
                .unwrap();
            let b = db.insert_durable(pivot, &note).await.unwrap();
            db.update_durable(pivot, b, &note).await.unwrap();
            assert!(db.remove_durable::<Note>(pivot, b).await.unwrap());
            assert_eq!(store.durable_batches(), 5);
            assert_eq!(
                db.get_unique::<Settings>()
                    .await
                    .unwrap()
                    .map(|w| w.decode().unwrap()),
                Some(Settings { volume: 2 })
            );
        });
    }

    /// A durable node operation runs over a durable store: the route a
    /// `#[server]` body uses keeps the promise to the engine.
    #[test]
    fn a_durable_node_op_applies_durably() {
        use crate::node_op::{NodeOp, NodeReply, run_nonunique};
        block_on(async {
            let store = MemStore::default();
            let db = LocalHandle::new(&store, tenant());
            let pivot = db.create_pivot::<Note>().await.unwrap();
            let body = crate::wire::to_wire(&Note {
                label: "red".into(),
                n: 1,
            });
            for durable in [false, true] {
                let op = NodeOp::Insert {
                    pivot,
                    body: body.clone(),
                    durable,
                };
                let reply =
                    run_nonunique::<Note, _>(&store, tenant(), op).await;
                assert!(matches!(reply, Ok(NodeReply::Inserted(_))));
            }
            assert_eq!(store.durable_batches(), 1);
        });
    }
}
