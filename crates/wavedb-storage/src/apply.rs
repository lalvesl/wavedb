//! [`PageStore`]'s commit path — the [`Store`] impl and the batch pipeline
//! behind it: route, validate guards, journal (the durability point),
//! append to the types' logs (RFC 0068).
//!
//! Split from [`page_store`](crate::page_store) (which keeps open/recovery
//! and the accessors) the same way the read fallback lives in
//! `read_through`.

use wavedb_core::{Id, Result as CoreResult, Store, Write};

use crate::barrier::{Barrier, Durable};
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::record_log::MAX_RECORD;
use crate::struct_storage::TypeSlot;

impl PageStore {
    /// Take batch barriers on `barrier`'s helper thread from now on, instead
    /// of the thread calling [`apply`](Store::apply) ([`crate::barrier`]).
    ///
    /// The store's futures then **suspend**: an `apply` returns once its
    /// barrier is done, and its caller's executor runs other work meanwhile.
    /// A caller that interleaves operations on one owner needs the owner
    /// lock that makes that safe — the node holds it around every operation
    /// (`CONCURRENCY_BRAKE.md`); a store without a helper never suspends.
    pub fn use_barrier(&mut self, barrier: Barrier) {
        self.barrier = Some(barrier);
    }

    /// Barriers completed on the live journal, on any thread.
    #[must_use]
    pub fn journal_barriers(&self) -> u64 {
        self.disk
            .as_ref()
            .map_or(0, |disk| disk.journal.borrow_mut().barriers())
    }

    /// Journal first, then append to the types' logs, where the settle drain
    /// finds them. The page write happens later
    /// ([`drain`](Self::drain)) — a crash before it replays from the journal.
    ///
    /// The batch is durable when this returns — or, with a barrier helper,
    /// when the returned [`Durable`] resolves; a durability window (RFC 0061)
    /// relaxes either to "within the window".
    // The journal guard deliberately spans the cache commit — releasing it
    // earlier (the lint's suggestion) would let two applies commit their
    // caches in the opposite order to their journal frames, so replay could
    // disagree with what live readers saw, and the logs' order is what the
    // checkpoint's rotation mark (taken under this lock too) relies on.
    #[allow(clippy::significant_drop_tightening)]
    fn apply_inner(&self, batch: &[Write]) -> StorageResult<Option<Durable>> {
        // Refuse unroutable writes *before* the journal sees the batch, so the
        // log never holds a batch replay would choke on.
        self.route_batch(batch)?;
        if self.is_fenced() {
            return Err(StorageError::Fenced);
        }
        let Some(disk) = &self.disk else {
            // Object mode (RFC 0070 §9): no journal — the upload that
            // carries the batch is its durability point.
            let effective = self.validate_guards(batch)?;
            self.commit_to_caches(&effective);
            self.note_unuploaded();
            return Ok(None);
        };
        let mut journal = disk.journal.borrow_mut();
        let effective = self.validate_guards(batch)?;
        let frame = crate::journal::JournalFrame::Batch(effective.clone());
        // The durability point, and the one place the window changes anything
        // (RFC 0061). The check runs under the journal lock this function
        // already holds, so a burst inside one window amortises to a single
        // barrier with no timer and no flusher task. With a helper, the
        // barrier is asked for here — after the write, so it covers it — and
        // waited for by the caller, off this lock.
        let mut wait = None;
        if self.relax_window.is_zero() {
            match &self.barrier {
                None => journal.append(&frame)?, // write + fsync
                Some(barrier) => {
                    journal.append_deferred(&frame)?;
                    wait = Some(barrier.sync(journal.synced()));
                }
            }
        } else {
            journal.append_deferred(&frame)?; // write, no barrier
            if journal.since_last_sync() >= self.relax_window {
                match &self.barrier {
                    None => journal.sync()?, // one barrier for the window
                    // Not asked (a descriptor could not be duplicated):
                    // the window stays open, so the next batch asks again.
                    Some(barrier) => {
                        if barrier.flush(journal.synced()) {
                            journal.flush_requested();
                        }
                    }
                }
            }
        }
        // Commit under the journal lock: cache order == journal order.
        self.commit_to_caches(&effective);
        self.note_unuploaded();
        // The append's fsync flushed the whole file, so a checkpoint's deferred
        // `Commit` frame is now durable too — for free, and with nothing to do
        // about it here. Acting on it (deleting the retired journal, rolling
        // protection) belongs to the next checkpoint, which can see it in the
        // journal's barrier count (RFC 0047); the write path stays a journal
        // append and a cache commit.
        Ok(wait)
    }

    /// Check `batch`'s guards against the pre-batch state, returning its
    /// effective writes — the guards stripped.
    ///
    /// Called under the same lock that orders commits; a mismatch refuses
    /// the batch before the durability point. Guards are not state — the
    /// journal never holds them, so replay re-applies only the effective
    /// writes.
    fn validate_guards(&self, batch: &[Write]) -> StorageResult<Vec<Write>> {
        for w in batch {
            // One routed lookup, not a scan. Before RFC 0063's yield map made
            // it visible, this read the id's *type* out of the store by
            // probing every registered slot in turn — so an
            // `Expect(id, None)`, the guard on every first save, matched
            // nothing and paid a page read plus a zstd decode per type, here,
            // under the lock that serializes all writers.
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
    /// Checked before the journal sees the batch, so the log never holds a
    /// batch replay would choke on; and it is what lets the commit path treat
    /// a routing miss as unreachable rather than handling it twice.
    pub(crate) fn route_batch(&self, batch: &[Write]) -> StorageResult<()> {
        for w in batch {
            let sh = match w {
                Write::Put(_, bytes) if bytes.len() > MAX_RECORD => {
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

    /// Append `batch` to the types' logs (routing validated beforehand). Runs
    /// under the journal lock, so the logs' order is the journal's.
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
                // Validated and stripped in `apply_inner`; never reaches a
                // journaled batch (nor, therefore, replay).
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
// (RFC 0070 §14), and a barrier helper makes `apply` wait (`use_barrier`): a
// store that suspends is used under the owner lock, as on the node.
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
        // The batch is written and committed now, on this call — unless it
        // writes to an evicted bucket, which comes back to disk first, so
        // that no settle round ever meets one. Then only the barrier, when a
        // helper takes it, is left to wait for.
        let staged = self
            .evicted_in(batch)
            .is_empty()
            .then(|| self.apply_inner(batch));
        async move {
            let staged = match staged {
                Some(staged) => staged,
                None => self.apply_restoring(batch).await,
            };
            // StorageError → core::Error (typed seam)
            match staged {
                Ok(Some(durable)) => durable.await.map_err(Into::into),
                Ok(None) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }
}

impl PageStore {
    /// Restore every evicted bucket `batch` writes to, then commit it —
    /// checking again after the awaits, since an eviction may have run
    /// meanwhile; the commit itself is synchronous, so nothing can evict
    /// between that check and it.
    async fn apply_restoring(
        &self,
        batch: &[Write],
    ) -> StorageResult<Option<Durable>> {
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
