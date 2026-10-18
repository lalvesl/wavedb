//! `ShardStore` — a shard's view of storage ([RFC 0064]).
//!
//! This is the whole seam. The index layer above (`Collection`, `BpTree`, the
//! chains, the declared lists, everything `#[wavedb]` expands to) reaches its
//! backend through four `Store` methods and takes the store **per call**, so
//! replacing `impl Store for PageStore` with this changes nothing above it.
//!
//! What a shard owns is its **cache**; what it does not own, it asks the disk
//! actor for. That division is the design: a working set that fits stays on
//! one thread and never synchronises with anything, and only a miss costs a
//! message.
//!
//! Deliberately non-`Send` (`RefCell`, `Rc`): a shard's state must not be able
//! to move between threads, and the type system saying so is stronger than a
//! scheduler promising it. Only [`super::msg::DiskRequest`] crosses.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

use std::cell::RefCell;
use std::collections::HashMap;

use futures::channel::oneshot;
use wavedb_core::store::{Store, Write};
use wavedb_core::{Error, Id, Result};

use super::disk::DiskHandle;
use super::msg::{DiskRequest, EngineStats};

/// Bytes of cached record images a shard holds before it drops them.
///
/// Per shard, so a process running N shards budgets N times this.
pub const CACHE_BYTES: usize = 64 << 20;

/// One worker's store: its own cache in front of the shared disk actor,
/// reached per data shard through [`at`](Self::at).
///
/// ## The invariant this rests on
///
/// The cache remembers **absence** as well as presence, which is only sound
/// because a record is reached by exactly one worker: routing sends an
/// owner's every operation to the worker holding its data shard, and a
/// record's tree nodes are minted by the tree that owns it. Nothing else
/// writes what this cache holds, so nothing can invalidate it behind its
/// back. Ids never repeat across data shards, so one cache serves all of a
/// worker's shards.
///
/// Break that — hand two workers the same Pivot — and a cached `None`
/// outlives the other worker's insert. The routing function is what keeps it
/// true, and it is the reason the routing is a *function* rather than a
/// policy.
pub struct ShardStore {
    disk: DiskHandle,
    /// `Id::raw()` → the record's bytes, or `None` for "known absent".
    cache: RefCell<HashMap<u128, Option<Vec<u8>>>>,
    /// Live byte count of the `Some` entries, maintained on write so the
    /// budget check is not a walk of the map.
    bytes: RefCell<usize>,
    budget: usize,
}

impl ShardStore {
    /// A shard talking to `disk`, with the default cache budget.
    #[must_use]
    pub fn new(disk: DiskHandle) -> Self {
        Self::with_budget(disk, CACHE_BYTES)
    }

    /// A shard with an explicit cache budget (`0` disables caching, which is
    /// what the cross-shard tests want).
    #[must_use]
    pub fn with_budget(disk: DiskHandle, budget: usize) -> Self {
        Self {
            disk,
            cache: RefCell::new(HashMap::new()),
            bytes: RefCell::new(0),
            budget,
        }
    }

    /// Cached entries, for tests and for a node's introspection.
    #[must_use]
    pub fn cached_len(&self) -> usize {
        self.cache.borrow().len()
    }

    /// Cached record bytes.
    #[must_use]
    pub fn cached_bytes(&self) -> usize {
        *self.bytes.borrow()
    }

    /// What the engine behind the actor currently holds.
    ///
    /// Not derivable from this side: once the actor owns the `PageStore`,
    /// "is anything unsettled?" has to be asked.
    ///
    /// # Errors
    /// The actor is gone.
    pub async fn engine_stats(&self) -> Result<EngineStats> {
        self.ask(|answer| DiskRequest::Stats { answer }).await
    }

    /// Send `request` and await its answer.
    ///
    /// Both failure modes become [`Error::Backend`] at this seam rather than
    /// being invented further up: a full-and-closed queue means the actor is
    /// gone, and a dropped answer means it died mid-request.
    async fn ask<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T>>) -> DiskRequest,
    ) -> Result<T> {
        let (answer, wait) = oneshot::channel();
        self.disk
            .send(build(answer))
            .await
            .map_err(|_| Error::Backend("disk actor stopped".into()))?;
        wait.await.map_err(|_| {
            Error::Backend("disk actor dropped the answer".into())
        })?
    }

    /// Remember `id`'s current bytes (`None` = absent).
    fn memoise(&self, id: Id, value: Option<Vec<u8>>) {
        if self.budget == 0 {
            return;
        }
        let mut cache = self.cache.borrow_mut();
        let mut bytes = self.bytes.borrow_mut();
        if let Some(Some(old)) = cache.get(&id.raw()) {
            *bytes -= old.len();
        }
        *bytes += value.as_ref().map_or(0, Vec::len);
        cache.insert(id.raw(), value);
        if *bytes > self.budget {
            // The crudest bound that is still a bound. An unbounded cache is
            // how the benchmark harness OOMed twice, so "no policy" is not an
            // option; a proper eviction order belongs with the page cache
            // (RFC 0044) rather than being guessed at here.
            cache.clear();
            *bytes = 0;
        }
    }

    /// Fold a committed batch into the cache.
    ///
    /// Only after the disk actor acknowledges, so the cache never shows a
    /// write the engine refused — a lost `Expect` race leaves the cache
    /// exactly as it was.
    fn adopt(&self, batch: &[Write]) {
        for write in batch {
            match write {
                Write::Put(id, bytes) => self.memoise(*id, Some(bytes.clone())),
                Write::Remove(_, id) => self.memoise(*id, None),
                // A guard is not state: the engine validates it and never
                // stores it, so there is nothing here to reflect.
                Write::Expect(..) => {}
            }
        }
    }
}

impl ShardStore {
    /// The store of data shard `shard`: this cache, in front of that shard's
    /// engine.
    #[must_use]
    pub fn at(&self, shard: usize) -> ShardView<'_> {
        // Past `u16` is no shard at all; the disk actor answers it as such.
        let shard = u16::try_from(shard).unwrap_or(u16::MAX);
        ShardView { store: self, shard }
    }
}

/// One data shard, as a [`Store`]: the worker's cache in front of that
/// shard's engine.
#[derive(Clone, Copy)]
pub struct ShardView<'a> {
    store: &'a ShardStore,
    shard: u16,
}

impl Store for ShardView<'_> {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        let (store, shard) = (self.store, self.shard);
        if let Some(hit) = store.cache.borrow().get(&id.raw()) {
            return Ok(hit.clone());
        }
        let value = store
            .ask(|answer| DiskRequest::Get { shard, id, answer })
            .await?;
        store.memoise(id, value.clone());
        Ok(value)
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> Result<Option<Vec<u8>>> {
        let (store, shard) = (self.store, self.shard);
        if let Some(hit) = store.cache.borrow().get(&id.raw()) {
            return Ok(hit.clone());
        }
        let value = store
            .ask(|answer| DiskRequest::GetOf {
                shard,
                struct_hash,
                id,
                answer,
            })
            .await?;
        store.memoise(id, value.clone());
        Ok(value)
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        let (store, shard) = (self.store, self.shard);
        let owned = batch.to_vec();
        store
            .ask(|answer| DiskRequest::Apply {
                shard,
                batch: owned,
                answer,
            })
            .await?;
        store.adopt(batch);
        Ok(())
    }
}

// A note on what is *not* asserted here. `ShardStore` must be non-`Send`, and
// stable Rust cannot state a negative bound — `fn assert<T>()` accepts
// everything, so an assertion in that shape would check nothing while looking
// like it did. The property is enforced structurally instead: `RefCell` makes
// the type non-`Sync` and the shard is only ever reached through `Rc`, so
// `spawn_detached`'s factory (which is `Send`) cannot capture one.
