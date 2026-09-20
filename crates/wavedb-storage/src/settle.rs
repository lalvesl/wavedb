//! The settle drain — feeding rounds of touched ids to the checkpoint writer,
//! plus the cache-eviction policy.
//!
//! The work itself lives in [`crate::plan`] (page images, grouped per bucket)
//! and [`crate::checkpoint`] (one window, one write, then the descriptor
//! swap). This module owns only the queue side: what a round is, when it
//! retries, and when a settled entry may leave the cache.

use crate::error::StorageResult;
use crate::page_store::{PageStore, Touched};

/// What one [`settle_step`](PageStore::settle_step) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Progress {
    /// A round was settled. There may be more — including writes that landed
    /// while this one ran.
    Settled,
    /// The queue was observed empty. Nothing to do until another write lands.
    Done,
}

impl PageStore {
    /// Settle everything queued: drain the pending touched ids into their
    /// pages. Loops until the queue is observed empty (writes landing while
    /// a round settles are picked up by the next round).
    ///
    /// # Errors
    /// A page write fault. Nothing acked is at risk — the journal still
    /// holds every unsettled batch; the failed round's ids stay pending so
    /// a later drain (or the reopen replay) retries them.
    pub fn drain(&self) -> StorageResult<()> {
        while self.settle_step(usize::MAX)? == Progress::Settled {}
        Ok(())
    }

    /// **One** round, then return — the interruptible form of
    /// [`drain`](Self::drain) ([RFC 0063] part 3).
    ///
    /// The caller drives it, and between two calls it may do anything: serve a
    /// read, take a write, stop. That boundary is the engine's only legal
    /// yield point, and it is legal for a reason worth stating rather than
    /// assuming.
    ///
    /// `evict_settled` trusts "the pending queue is empty ⇒ everything
    /// committed is settled" (invariant I2). The `mem::take` below falsifies
    /// that — for the length of the round, the queue is empty while its ids
    /// are still only in cache. But that window is entirely **inside** one
    /// step: on entry, `pending` holds exactly the unsettled ids, and on
    /// return the round's pages have been written and published. So I2 holds
    /// at every point a caller can observe, and nowhere in between.
    ///
    /// ## Why a `step` and not an `async fn`
    ///
    /// 0063 left that open, to be settled by wasm artifact size. It does not
    /// need to be: the engine contains no await point, so an `async fn drain`
    /// would be a future that never yields — the machinery of suspension with
    /// nothing to suspend on. The driver that makes a step useful (the node's
    /// maintenance loop, the disk actor's queue loop) already exists.
    ///
    /// ## `budget`
    ///
    /// The most ids a round may take, so one burst does not become one
    /// uninterruptible round.
    ///
    /// The bound holds **within** a slot as well as across slots. Whole slots
    /// only would have been tidier — splitting one slot's ids lets two rounds
    /// rewrite a bucket they share, paying a second page read and write — but
    /// it would also make the budget useless in the common case, since a burst
    /// of one type is one slot and would be taken whole however small the
    /// budget. And that extra rewrite is not a new cost: writes landing
    /// *during* a round are already picked up by the next one and already
    /// rewrite the buckets they share. The design accepts it.
    ///
    /// `usize::MAX` means "no bound", which is what [`drain`](Self::drain)
    /// asks for.
    ///
    /// # Errors
    /// A page write fault. The round's ids go back on the queue, so a later
    /// step — or the reopen replay — retries them.
    ///
    /// [RFC 0063]: ../../../rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md
    pub fn settle_step(&self, budget: usize) -> StorageResult<Progress> {
        let round = self.take_round(budget);
        if round.is_empty() {
            return Ok(Progress::Done);
        }
        if let Err(e) = self.settle(&round) {
            // Put the round back: ids may be partially settled, but settling
            // writes cache state, so re-settling is idempotent.
            requeue(&mut self.pending.lock(), round);
            return Err(e);
        }
        Ok(Progress::Settled)
    }

    /// Take up to `budget` ids off the pending queue.
    ///
    /// Whatever is not taken stays queued, which is what keeps I2 true at the
    /// boundary: the queue always holds exactly the ids that are committed and
    /// not yet in a page.
    // The guard spans the whole selection: what is taken and what is left
    // behind have to be decided in one observation, or a write landing in the
    // middle could be split across the boundary the round is drawing.
    #[allow(clippy::significant_drop_tightening)]
    fn take_round(&self, budget: usize) -> Touched {
        let mut pending = self.pending.lock();
        if budget == usize::MAX {
            return std::mem::take(&mut *pending);
        }
        let mut round = Touched::new();
        let mut room = budget;
        while room > 0 {
            let Some((idx, ids)) = pending.last_mut() else {
                break;
            };
            if ids.len() <= room {
                room -= ids.len();
                let whole = (*idx, std::mem::take(ids));
                round.push(whole);
                pending.pop();
            } else {
                // Take the tail, leave the head queued.
                let keep = ids.len() - room;
                let part = (*idx, ids.split_off(keep));
                round.push(part);
                room = 0;
            }
        }
        round
    }

    /// Whether any committed batch still awaits its page write.
    #[must_use]
    pub fn has_pending(&self) -> bool {
        !self.pending.lock().is_empty()
    }

    /// Bytes of committed journal frames — the checkpoint policy's input.
    #[must_use]
    pub fn journal_len(&self) -> u64 {
        self.journal.lock().len_bytes()
    }

    /// Evict settled cache entries until the caches hold at most
    /// `budget_bytes`. A no-op while anything is pending — only a settled
    /// entry may leave the cache (the page then serves reads). Quiesces
    /// writers for the duration (journal lock), so "queue empty" can't race
    /// a commit whose ids aren't queued yet.
    #[allow(clippy::significant_drop_tightening)]
    pub fn evict_settled(&self, budget_bytes: usize) {
        let _journal = self.journal.lock();
        let pending = self.pending.lock();
        if !pending.is_empty() {
            return;
        }
        let mut total: usize =
            self.types.iter().map(|s| s.cached_bytes()).sum();
        for slot in &self.types {
            if total <= budget_bytes {
                return;
            }
            total -= slot.evict_up_to(total - budget_bytes);
        }
    }

    /// The record-cache budget this store was opened with
    /// ([`StoreOptions::record_cache_bytes`](crate::StoreOptions::record_cache_bytes)).
    #[must_use]
    pub const fn record_cache_bytes(&self) -> usize {
        self.record_cache_bytes
    }

    /// [`evict_settled`](Self::evict_settled) down to the budget the store was
    /// opened with.
    ///
    /// The engine never calls this itself — eviction stays the caller's
    /// decision, because dropping cache entries takes the journal lock and a
    /// store that did it on a schedule of its own would be stalling writers
    /// inside a window someone else is timing. What the store supplies is the
    /// number, so a caller stops having to remember one.
    pub fn evict_to_budget(&self) {
        self.evict_settled(self.record_cache_bytes);
    }
}

/// Merge a failed round back into the pending queue (slot-grouped; ids may
/// duplicate what landed meanwhile — settling twice is idempotent).
pub fn requeue(pending: &mut Touched, round: Touched) {
    for (idx, ids) in round {
        match pending.iter_mut().find(|(i, _)| *i == idx) {
            Some((_, existing)) => existing.extend(ids),
            None => pending.push((idx, ids)),
        }
    }
}
