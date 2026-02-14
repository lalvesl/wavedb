//! [`PageStore`]'s commit path — the [`Store`] impl and the batch pipeline
//! behind it: route, validate guards, append to the types' logs (RFC 0068).
//! A batch is committed there and nothing else; it is durable once the
//! settle cycle carrying it is (`crate::cycle`, RFC 0072), which
//! [`apply_durable`](PageStore::apply_durable) waits for.
//!
//! Split from [`page_store`](crate::page_store) (which keeps open/recovery
//! and the accessors) the same way the read fallback lives in
//! `read_through`.

use wavedb_core::{Id, Result as CoreResult, Store, Write};

use crate::barrier::Barrier;
use crate::block::{BLOCK_SIZE, MAX_PAGE_BYTES};
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::record_log::MAX_RECORD;
use crate::struct_storage::TypeSlot;

/// The largest record a batch may commit: one the record cache can hold that
/// still fits a page of its own, a block left for the page's envelope
/// (RFC 0074). A page takes its bytes uncompressed when zstd would not
/// shrink them, so the envelope is all a lone record adds.
const MAX_COMMIT: usize = {
    let page = (MAX_PAGE_BYTES - BLOCK_SIZE as u64) as usize;
    if page < MAX_RECORD { page } else { MAX_RECORD }
};

impl PageStore {
    /// Take batch barriers on `barrier`'s helper thread from now on, instead
    /// of the thread stepping the cycle ([`crate::barrier`]).
    ///
    /// A durable write then **suspends** until its batch's barrier is
    /// answered, and its caller's executor runs other work meanwhile. A
    /// caller that interleaves operations on one owner needs the owner lock
    /// that makes that safe — the node holds it around every operation
    /// (`CONCURRENCY_BRAKE.md`). A store without a helper takes the barrier
    /// inline, and a durable write on it settles its batch itself.
    pub fn use_barrier(&mut self, barrier: Barrier) {
        self.barrier = Some(barrier);
    }

    /// Commit `batch` to the types' logs, where a settle cycle finds it —
    /// durable once that cycle's batch is.
    ///
    /// Synchronous, and memory work only: routing, the guards, the commit.
    /// One commit at a time comes free: the store is used from one thread
    /// and this does not suspend, so the logs' order is the commits' order,
    /// which a cycle's mark relies on.
    fn apply_inner(&self, batch: &[Write]) -> StorageResult<()> {
        // Refuse unroutable writes before anything sees the batch.
        self.route_batch(batch)?;
        if self.is_fenced() {
            return Err(StorageError::Fenced);
        }
        if self.has_disk() {
            self.check_failed()?;
        }
        let effective = self.validate_guards(batch)?;
        self.commit_to_caches(&effective);
        self.note_unuploaded();
        Ok(())
    }

    /// [`apply`](Store::apply), and then wait until the batch is durable:
    /// settled and its barrier taken — at the next settle cycle on a store
    /// stepped by a node, at once (the cycle run here) on a store with no
    /// barrier helper. In object mode, until an upload carries it.
    ///
    /// Everything committed before it is durable with it: a batch holds
    /// every write before its mark.
    ///
    /// # Errors
    /// What [`apply`](Store::apply) refuses; a settle or barrier fault.
    pub async fn apply_durable(&self, batch: &[Write]) -> StorageResult<()> {
        self.commit(batch).await?;
        if !self.has_disk() {
            return self.upload_all().await;
        }
        let wait = self.wait_for_batch();
        if self.barrier.is_none() {
            self.drain()?;
        }
        wait.await
    }

    /// Commit `batch`, restoring first every evicted bucket it writes to.
    async fn commit(&self, batch: &[Write]) -> StorageResult<()> {
        if self.evicted_in(batch).is_empty() {
            return self.apply_inner(batch);
        }
        self.apply_restoring(batch).await
    }

    /// Check `batch`'s guards against the pre-batch state, returning its
    /// effective writes — the guards stripped.
    ///
    /// A mismatch refuses the whole batch before anything is committed.
    /// Guards are not state: the logs never hold them.
    fn validate_guards(&self, batch: &[Write]) -> StorageResult<Vec<Write>> {
        for w in batch {
            // One routed lookup, not a scan. Before RFC 0063's yield map made
            // it visible, this read the id's *type* out of the store by
            // probing every registered slot in turn — so an
            // `Expect(id, None)`, the guard on every first save, matched
            // nothing and paid a page read plus a zstd decode per type.
            if let Write::Expect(hash, id, expected) = w
                && self.read_of(*hash, *id)? != *expected
            {
                return Err(StorageError::Core(wavedb_core::Error::Conflict(
                    *id,
                )));
            }
        }
        Ok(batch
            .iter()
            .filter(|w| !matches!(w, Write::Expect(..)))
            .cloned()
            .collect())
    }

    /// Verify every write in `batch` routes to a registered slot — `Put` by
    /// the STRUCT_HASH at its bytes' head, `Remove` and `Expect` by the one
    /// they carry.
    ///
    /// Checked before anything is committed; it is what lets the commit path
    /// treat a routing miss as unreachable rather than handling it twice.
    pub(crate) fn route_batch(&self, batch: &[Write]) -> StorageResult<()> {
        for w in batch {
            let sh = match w {
                Write::Put(_, bytes) if bytes.len() > MAX_COMMIT => {
                    return Err(StorageError::RecordTooLarge(bytes.len()));
                }
                Write::Put(_, bytes) => struct_hash_of(bytes)?,
                Write::Remove(hash, _) | Write::Expect(hash, _, _) => *hash,
            };
            if self.slot_of(sh).is_none() {
                return Err(StorageError::UnregisteredStructHash(sh));
            }
        }
        Ok(())
    }

    /// Append `batch` to the types' logs (routing validated beforehand).
    ///
    /// Every write here routes by a hash it already carries, so this touches
    /// no page and cannot fault: it is memory work under the commit lock, as
    /// RFC 0063's map requires of anything inside that section.
    pub(crate) fn commit_to_caches(&self, batch: &[Write]) {
        let mut cache = self.cache.borrow_mut();
        for w in batch {
            // `route_batch` accepted every hash here, so neither lookup can
            // miss; a write that somehow did would have no log to go to.
            match w {
                Write::Put(id, bytes) => {
                    let idx = struct_hash_of(bytes)
                        .ok()
                        .and_then(|sh| self.slot_index(sh));
                    if let Some(idx) = idx {
                        cache.put(idx, *id, Some(bytes));
                    }
                }
                // The page still holds the bytes until the settle lands —
                // the tombstone keeps reads from resurrecting them.
                Write::Remove(hash, id) => {
                    if let Some(idx) = self.slot_index(*hash) {
                        cache.put(idx, *id, None);
                    }
                }
                // Validated and stripped in `apply_inner`.
                Write::Expect(..) => {}
            }
        }
    }

    /// The slot index for a registered hash.
    pub(crate) fn slot_index(&self, struct_hash: u64) -> Option<usize> {
        self.types
            .binary_search_by_key(&struct_hash, TypeSlot::struct_hash)
            .ok()
    }
}

// A read does its synchronous half — record cache, page on disk — when it is
// called, and so does `apply`: a disk-only store's futures are then ready at
// once, and an engine that never suspends is what lets a collection op run
// read-to-apply without interleaving on a current-thread executor
// (`concurrent_node_clobber.rs`). An object-backed store suspends on the
// object store, for a read of an evicted bucket or a write that restores one
// (RFC 0070 §14), and a durable write waits for its batch: a store that
// suspends is used under the owner lock, as on the node.
impl Store for PageStore {
    fn get(&self, id: Id) -> impl Future<Output = CoreResult<Option<Vec<u8>>>> {
        // Untyped fallback: the id alone names no type, so probe every slot —
        // caches first (cheap), then settled pages. Typed callers use
        // `get_of` and skip this scan entirely.
        let now = self.read_any_now(id);
        async move { self.read_any_rest(now, id).await.map_err(Into::into) }
    }

    fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> impl Future<Output = CoreResult<Option<Vec<u8>>>> {
        // One binary search on the route table, then this type's own log in
        // the record cache. A miss reads through to the settled page, and an
        // evicted page to the object store.
        let now = self.read_now(struct_hash, id);
        async move { self.read_rest(now, id).await.map_err(Into::into) }
    }

    fn apply(&self, batch: &[Write]) -> impl Future<Output = CoreResult<()>> {
        // The batch is committed now, on this call — unless it writes to an
        // evicted bucket, which comes back to disk first, so that no settle
        // round ever meets one.
        let staged = self
            .evicted_in(batch)
            .is_empty()
            .then(|| self.apply_inner(batch));
        async move {
            let staged = match staged {
                Some(staged) => staged,
                None => self.apply_restoring(batch).await,
            };
            staged.map_err(Into::into) // StorageError → core::Error
        }
    }

    async fn apply_durable(&self, batch: &[Write]) -> CoreResult<()> {
        // The inherent one: commit, then wait for the batch's barrier.
        Ok(Self::apply_durable(self, batch).await?)
    }
}

impl PageStore {
    /// Restore every evicted bucket `batch` writes to, then commit it —
    /// checking again after the awaits, since an eviction may have run
    /// meanwhile; the commit itself is synchronous, so nothing can evict
    /// between that check and it.
    async fn apply_restoring(&self, batch: &[Write]) -> StorageResult<()> {
        loop {
            let evicted = self.evicted_in(batch);
            if evicted.is_empty() {
                return self.apply_inner(batch);
            }
            for (idx, bucket) in evicted {
                self.restore(idx, bucket).await?;
            }
        }
    }
}

/// The `STRUCT_HASH` at the head of a wire-encoded record, if it has one.
pub fn head_of(bytes: &[u8]) -> Option<u64> {
    let head: [u8; 8] = bytes.get(..8)?.try_into().ok()?;
    Some(u64::from_le_bytes(head))
}

/// The `STRUCT_HASH` at the head of a wire-encoded record (`[STRUCT_HASH][…]`).
fn struct_hash_of(bytes: &[u8]) -> StorageResult<u64> {
    head_of(bytes)
        .ok_or(StorageError::Corrupt("record shorter than STRUCT_HASH"))
}
