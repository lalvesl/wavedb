//! Deferred retirement — the second half of a checkpoint, disposed of by the
//! checkpoint that follows it ([RFC 0047]).
//!
//! A checkpoint's `Commit` frame is written unsynced ([RFC 0046]): it is a
//! *pointer* into an addressing log the data files already made durable, so it
//! needs no barrier of its own and the next ordinary `Batch` append carries it
//! (`fsync` flushes the file, not one write). A checkpoint therefore costs
//! **one** barrier — the data files' sync.
//!
//! What has to wait for that durability is everything the frame *authorises*:
//!
//! - **deleting the retired journal.** If the frame is torn or never lands,
//!   recovery falls back to the previous `Commit`, and that journal's batches
//!   are the only record of what happened since.
//! - **dropping the older generation's block protection.** The allocator
//!   protects the last *two* checkpoints' runs precisely because either can be
//!   the root a crash reopens from; releasing the older one asserts that it
//!   cannot (`BlockAllocator::release_previous`).
//!
//! Both are held in a [`Retiring`] until the **next checkpoint**. Waiting is
//! free: a retained journal is disk, and disk is not the scarce resource.
//! Chasing the durability is not free — completing from the write path puts a
//! lock and an `unlink` inside a batch, and completing from an idle timer pays
//! a whole barrier for housekeeping.
//!
//! The next checkpoint is also where the proof is cheapest. The frame lives in
//! the journal that checkpoint rotates *out*, and [`Journal::barriers`] counts
//! that file's `fsync`s — so comparing it against the count recorded when the
//! frame was appended says whether an ordinary write already carried it. On any
//! node that reached its checkpoint threshold by writing, it did.
//!
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md
//! [RFC 0047]: ../../../rfcs/0047-generational-journal-retirement-PLANNED.md

use crate::error::StorageResult;
use crate::journal::Journal;
use crate::page_store::PageStore;

/// A checkpoint whose `Commit` frame is written but not yet durable.
#[derive(Debug)]
pub struct Retiring {
    /// The retired journal, deleted once the frame that retires it is durable.
    pub journal: Journal,
    /// Barriers *begun* on the journal carrying the frame, read immediately
    /// after appending it. The frame is durable once more barriers than this
    /// have *completed* on that journal — one of them began after it
    /// (`crate::barrier`, where barriers run on a helper thread too).
    ///
    /// Read **after** the append and under the same guard, deliberately:
    /// `commit_journal` rotates before it appends, so a concurrent writer can
    /// `fsync` a batch into the new journal in between — a bare "any barrier at
    /// all" test would read that sync as proof of a frame not yet written.
    pub frame_barrier: u64,
}

impl PageStore {
    /// Dispose of the generation the previous checkpoint left pending, given
    /// `carrier` — the journal this checkpoint just rotated out, which is the
    /// one holding that pending frame.
    ///
    /// Free in the ordinary case: a checkpoint fires because its journal grew,
    /// growth means `Batch` appends, and a `Batch` append `fsync`s — every
    /// time on a zero-window store, once per elapsed window on a relaxed one
    /// (RFC 0061), which a journal grown enough to trigger a checkpoint has
    /// crossed many times over. The fallback barrier is reached only by a
    /// checkpoint with no writes to settle at all.
    ///
    /// The `barriers()` comparison is what makes that a matter of cost rather
    /// than correctness: it asks whether *this file* was flushed since the
    /// frame was appended, which is the right question in both modes.
    ///
    /// # Errors
    /// A sync or unlink fault. The protection roll happens **first** precisely
    /// so an unlink failure cannot strand the deferred frees; the leftover file
    /// is then cleaned up by recovery, which sees it covered by the durable
    /// frame. A fault here drops the pending record — safe in the same way, and
    /// the reason the caller claims it before it can be observed half-done.
    pub(crate) fn retire_previous(
        &self,
        pending: Option<Retiring>,
        carrier: &mut Journal,
    ) -> StorageResult<()> {
        let Some(retiring) = pending else {
            return Ok(());
        };
        if carrier.barriers() <= retiring.frame_barrier {
            // Nothing has flushed this file since the frame was appended, so it
            // is still only in the page cache. One barrier, in the case where
            // the checkpoint had no work anyway.
            carrier.sync()?;
        }
        self.dispose(retiring)
    }

    /// Make a pending checkpoint's frame durable **now** and complete it.
    ///
    /// A no-op when nothing is pending. Not needed on a running engine — the
    /// next checkpoint disposes of the generation for free. Call it when there
    /// is no next checkpoint: a graceful shutdown, where the engine is about to
    /// be dropped and a clean data directory is worth one barrier.
    ///
    /// # Errors
    /// A sync or unlink fault. Nothing acked is at risk: until this succeeds
    /// the retired journal is still on disk and still rules.
    pub fn force_retirement(&self) -> StorageResult<()> {
        let Some(disk) = &self.disk else {
            return Ok(()); // object mode: no checkpoint, nothing retiring
        };
        if !self.is_retiring() {
            return Ok(());
        }
        // The pending frame lives in the current journal — `commit_journal`
        // claims the record before it rotates, so there is no state in which a
        // retirement is observable while its carrier is not the live journal.
        disk.journal.borrow_mut().sync()?;
        let taken = self.retiring.borrow_mut().take();
        let Some(retiring) = taken else {
            return Ok(()); // someone else got there first
        };
        self.dispose(retiring)
    }

    /// Whether a checkpoint is still waiting for its frame to become durable —
    /// the steady state between checkpoints, not an anomaly.
    #[must_use]
    pub fn is_retiring(&self) -> bool {
        self.retiring.borrow_mut().is_some()
    }

    /// Drop the older generation's protection and the retired file. The frame's
    /// durability is what authorises both, so their order is free — and the
    /// infallible one goes first.
    ///
    /// Takes the record by value: `retiring` is never held across another
    /// lock, which is what keeps the lock graph acyclic (see `page_store`).
    fn dispose(&self, retiring: Retiring) -> StorageResult<()> {
        self.alloc.borrow_mut().release_previous();
        self.release_remote_root();
        retiring.journal.delete()?;
        // What the release freed at a file's tail leaves it now, during a
        // compaction as much as after one (RFC 0071 §5).
        self.reclaim().map(|_| ())
    }
}
