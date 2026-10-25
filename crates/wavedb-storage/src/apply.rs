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
use crate::record_cache::Lookup;
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
        self.journal.borrow_mut().barriers()
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
        let mut journal = self.journal.borrow_mut();
        // Guards validate against the pre-batch state under the same lock
        // that orders commits; a mismatch refuses the batch before the
        // durability point. Guards are not state — the journal never holds
        // them, so replay re-applies only the effective writes.
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
        let effective: Vec<Write> = batch
            .iter()
            .filter(|w| !matches!(w, Write::Expect(..)))
            .cloned()
            .collect();
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
        // The append's fsync flushed the whole file, so a checkpoint's deferred
        // `Commit` frame is now durable too — for free, and with nothing to do
        // about it here. Acting on it (deleting the retired journal, rolling
        // protection) belongs to the next checkpoint, which can see it in the
        // journal's barrier count (RFC 0047); the write path stays a journal
        // append and a cache commit.
        Ok(wait)
    }

    /// Read `id`'s current bytes from `struct_hash`'s slot: the record
    /// cache, then its settled page. The sync body [`Store::get_of`] wraps, reused by the
    /// `Expect` guard — one binary search on the route table and at most one
    /// page read, whatever the schema's width.
    ///
    /// An unregistered `struct_hash` reads as absent rather than faulting:
    /// [`route_batch`](Self::route_batch) has already refused any batch that
    /// would reach here with one.
    pub(crate) fn read_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> StorageResult<Option<Vec<u8>>> {
        let Some(idx) = self.slot_index(struct_hash) else {
            return Ok(None);
        };
        let cached = self.cache.borrow().get(idx, id);
        match cached {
            Lookup::Hit(bytes) => Ok(Some(bytes)),
            Lookup::Removed => Ok(None),
            Lookup::Miss => self.read_from_pages(&self.types[idx], id),
        }
    }

    /// Read `id`'s current bytes without knowing its type: the record cache,
    /// then every settled page — the sync body [`Store::get`] wraps.
    ///
    /// The cost is a page read per registered type, so nothing on a write
    /// path may call this (RFC 0063). `Store::get` keeps it because an
    /// untyped read is what that method *means*; typed callers spell
    /// [`read_of`](Self::read_of).
    fn read_any(&self, id: Id) -> StorageResult<Option<Vec<u8>>> {
        let cached = self.cache.borrow().get_any(id);
        match cached {
            Lookup::Hit(bytes) => return Ok(Some(bytes)),
            Lookup::Removed => return Ok(None),
            Lookup::Miss => {}
        }
        for slot in &self.types {
            if let Some(bytes) = self.read_from_pages(slot, id)? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
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
    fn slot_index(&self, struct_hash: u64) -> Option<usize> {
        self.types
            .binary_search_by_key(&struct_hash, TypeSlot::struct_hash)
            .ok()
    }
}

// Every read is synchronous and its future is `Ready`, and so is `apply`
// unless the store hands its barriers to a helper (`use_barrier`): an engine
// that never suspends is what lets a collection op run read-to-apply without
// interleaving on a current-thread executor (`concurrent_node_clobber.rs`).
// A store that does suspend is used under the owner lock, as on the node.
impl Store for PageStore {
    fn get(&self, id: Id) -> impl Future<Output = CoreResult<Option<Vec<u8>>>> {
        // Untyped fallback: the id alone names no type, so probe every slot —
        // caches first (cheap), then settled pages. Typed callers use
        // `get_of` and skip this scan entirely.
        std::future::ready(self.read_any(id).map_err(Into::into))
    }

    fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> impl Future<Output = CoreResult<Option<Vec<u8>>>> {
        // One binary search on the route table, then this type's own cache
        // read lock — a cached read of one type never contends with another
        // type's. A miss reads through to the settled page.
        std::future::ready(self.read_of(struct_hash, id).map_err(Into::into))
    }

    fn apply(&self, batch: &[Write]) -> impl Future<Output = CoreResult<()>> {
        // The batch is written and committed now, on this call; only its
        // barrier, when a helper takes it, is left to wait for.
        let staged = self.apply_inner(batch);
        async move {
            // StorageError → core::Error (typed seam)
            match staged {
                Ok(Some(durable)) => durable.await.map_err(Into::into),
                Ok(None) => Ok(()),
                Err(e) => Err(e.into()),
            }
        }
    }
}

/// The `STRUCT_HASH` at the head of a wire-encoded record (`[STRUCT_HASH][…]`).
fn struct_hash_of(bytes: &[u8]) -> StorageResult<u64> {
    if bytes.len() < 8 {
        return Err(StorageError::Corrupt("record shorter than STRUCT_HASH"));
    }
    let mut head = [0u8; 8];
    head.copy_from_slice(&bytes[..8]);
    Ok(u64::from_le_bytes(head))
}
