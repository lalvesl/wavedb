//! [`PageStore`]'s commit path — the [`Store`] impl and the batch pipeline
//! behind it: route, validate guards, journal (the durability point),
//! commit to the per-type caches, queue the settle.
//!
//! Split from [`page_store`](crate::page_store) (which keeps open/recovery
//! and the accessors) the same way the read fallback lives in
//! `read_through`.

use wavedb_core::{Id, Result as CoreResult, Store, Write};

use crate::error::{StorageError, StorageResult};
use crate::page_store::{PageStore, Touched};
use crate::struct_storage::TypeSlot;

impl PageStore {
    /// Journal first (durable fsync), then commit to the per-type caches and
    /// queue the touched ids for the settle drain. The batch is durable when
    /// this returns — unless the store was opened with a durability window,
    /// in which case it is durable within that window (RFC 0061). The page
    /// write happens later ([`drain`](Self::drain)) — a crash before it
    /// replays from the journal.
    // The journal guard deliberately spans the cache commit — releasing it
    // earlier (the lint's suggestion) would let two applies commit their
    // caches in the opposite order to their journal frames, so replay could
    // disagree with what live readers saw. The pending-queue push also stays
    // under it: eviction relies on "empty queue under its lock ⇒ every
    // committed id is settled" (see `evict_settled`).
    #[allow(clippy::significant_drop_tightening)]
    fn apply_inner(&self, batch: &[Write]) -> StorageResult<()> {
        // Refuse unroutable writes *before* the journal sees the batch, so the
        // log never holds a batch replay would choke on.
        self.route_batch(batch)?;
        let mut journal = self.journal.lock();
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
        // barrier with no timer, no flusher task, and no interaction with the
        // engine's non-`Send` current-thread model.
        if self.relax_window.is_zero() {
            journal.append(&frame)?; // write + fsync
        } else {
            journal.append_deferred(&frame)?; // write, no barrier
            if journal.since_last_sync() >= self.relax_window {
                journal.sync()?; // one barrier for the whole window
            }
        }
        // Commit under the journal lock: cache order == journal order.
        let touched = self.commit_to_caches(&effective);
        merge_touched(&mut self.pending.lock(), touched);
        // The append's fsync flushed the whole file, so a checkpoint's deferred
        // `Commit` frame is now durable too — for free, and with nothing to do
        // about it here. Acting on it (deleting the retired journal, rolling
        // protection) belongs to the next checkpoint, which can see it in the
        // journal's barrier count (RFC 0047); the write path stays a journal
        // append and a cache commit.
        Ok(())
    }

    /// Read `id`'s current bytes from `struct_hash`'s slot: its cache, then
    /// its settled page. The sync body [`Store::get_of`] wraps, reused by the
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
        let Some(slot) = self.slot_of(struct_hash) else {
            return Ok(None);
        };
        if let Some(bytes) = slot.get(id) {
            return Ok(Some(bytes));
        }
        self.read_from_pages(slot, id)
    }

    /// Read `id`'s current bytes without knowing its type: every cache, then
    /// every settled page — the sync body [`Store::get`] wraps.
    ///
    /// The cost is a page read per registered type, so nothing on a write
    /// path may call this (RFC 0063). `Store::get` keeps it because an
    /// untyped read is what that method *means*; typed callers spell
    /// [`read_of`](Self::read_of).
    fn read_any(&self, id: Id) -> StorageResult<Option<Vec<u8>>> {
        if let Some(bytes) = self.types.iter().find_map(|s| s.get(id)) {
            return Ok(Some(bytes));
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
                Write::Put(_, bytes) => struct_hash_of(bytes)?,
                Write::Remove(hash, _) | Write::Expect(hash, _, _) => *hash,
            };
            if self.slot_of(sh).is_none() {
                return Err(StorageError::UnregisteredStructHash(sh));
            }
        }
        Ok(())
    }

    /// Apply `batch` to the per-type caches (routing validated beforehand),
    /// returning the touched ids per slot. Runs under the journal lock, so
    /// commits are ordered and each type's write guard is held only briefly.
    ///
    /// Every write here routes by a hash it already carries, so this touches
    /// no page and cannot fault: it is memory work under the commit lock, as
    /// RFC 0063's map requires of anything inside that section.
    pub(crate) fn commit_to_caches(&self, batch: &[Write]) -> Touched {
        let mut touched: Touched = Vec::new();
        for w in batch {
            match w {
                Write::Put(id, bytes) => {
                    let sh = struct_hash_of(bytes)
                        .expect("route_batch validated the head");
                    let idx = self.slot_index(sh);
                    self.types[idx]
                        .mem_cache()
                        .write()
                        .insert(id.raw(), bytes.clone());
                    // A Put supersedes any unsettled remove of the same id.
                    self.types[idx].clear_removed(*id);
                    note_touched(&mut touched, idx, *id);
                }
                Write::Remove(hash, id) => {
                    let idx = self.slot_index(*hash);
                    self.types[idx].mem_cache().write().remove(&id.raw());
                    // The page still holds the bytes until the settle
                    // lands — tombstone so reads don't resurrect them.
                    self.types[idx].mark_removed(*id);
                    note_touched(&mut touched, idx, *id);
                }
                // Validated and stripped in `apply_inner`; never reaches a
                // journaled batch (nor, therefore, replay).
                Write::Expect(..) => {}
            }
        }
        touched
    }

    /// The slot index for a hash [`route_batch`](Self::route_batch) has
    /// already accepted.
    fn slot_index(&self, struct_hash: u64) -> usize {
        self.types
            .binary_search_by_key(&struct_hash, TypeSlot::struct_hash)
            .expect("route_batch validated registration")
    }
}

impl Store for PageStore {
    async fn get(&self, id: Id) -> CoreResult<Option<Vec<u8>>> {
        // Untyped fallback: the id alone names no type, so probe every slot —
        // caches first (cheap), then settled pages. Typed callers use
        // `get_of` and skip this scan entirely.
        Ok(self.read_any(id)?)
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> CoreResult<Option<Vec<u8>>> {
        // One binary search on the route table, then this type's own cache
        // read lock — a cached read of one type never contends with another
        // type's. A miss reads through to the settled page.
        Ok(self.read_of(struct_hash, id)?)
    }

    async fn apply(&self, batch: &[Write]) -> CoreResult<()> {
        self.apply_inner(batch)?; // StorageError → core::Error (typed seam)
        Ok(())
    }
}

/// Record `id` as touched under slot `idx`.
fn note_touched(touched: &mut Touched, idx: usize, id: Id) {
    match touched.iter_mut().find(|(i, _)| *i == idx) {
        Some((_, ids)) => ids.push(id),
        None => touched.push((idx, vec![id])),
    }
}

/// Merge one batch's touched ids into the pending queue (slot-grouped).
fn merge_touched(pending: &mut Touched, touched: Touched) {
    for (idx, ids) in touched {
        for id in ids {
            note_touched(pending, idx, id);
        }
    }
}

/// The `STRUCT_HASH` at the head of a wire-encoded record (`[STRUCT_HASH][…]`).
fn struct_hash_of(bytes: &[u8]) -> StorageResult<u64> {
    if bytes.len() < 8 {
        return Err(StorageError::Corrupt("record shorter than STRUCT_HASH"));
    }
    Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
}
