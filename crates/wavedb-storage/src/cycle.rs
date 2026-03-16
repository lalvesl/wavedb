//! The settle cycle ([RFC 0072] §2) — how what the record cache holds
//! becomes a durable batch on disk.
//!
//! 1. **Open** ([`begin_cycle`](PageStore::begin_cycle)): every type's log is
//!    marked at its end. The store runs on one thread, so the mark falls
//!    between two writes: the batch is every write before it, whole.
//! 2. **Rounds** ([`cycle_step`](PageStore::cycle_step)): bounded rounds of
//!    what the mark owes, each one window — `[pages][header]` — at the end
//!    of the active file. The caller may do anything between two of them;
//!    writes landing meanwhile go past the mark, to the next batch.
//! 3. **End**: the round that settles the last of it ends the batch — its
//!    header says so, once nothing is left for a header to name — and the
//!    batch's barrier is asked of the helper ([`Barrier`](crate::Barrier)),
//!    or taken inline by a store without one. When it is taken, every
//!    durable write of the batch is answered, and the runs the batch stopped
//!    naming become holes.
//!
//! A cycle opens only once the previous batch's barrier has answered, so at
//! most one batch is not durable at any time — the one a reconstruction may
//! find cut by a crash (`crate::reopen`). A tick with nothing to settle opens
//! nothing: no window, no header, no `fsync`.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::time::Duration;

use crate::barrier::{Durable, Syncs, Waiter};
use crate::error::{StorageError, StorageResult};
use crate::header::NO_FILE;
use crate::page_store::PageStore;
use crate::settle::Progress;
use crate::wait::park_on;

/// Where a store's batches stand.
#[derive(Debug, Default)]
pub struct Cycle {
    /// A batch is open: marked, its end not written yet.
    open: bool,
    /// The newest batch ended — batches are numbered from 1.
    ended: u64,
    /// The durable writes the open batch carries.
    current: Vec<Waiter>,
    /// Those committed since — the next batch's.
    next: Vec<Waiter>,
    /// Per batch ended and not known durable: its number, and where the
    /// deferred frees stood when it ended.
    covers: Vec<(u64, u64)>,
    pub(crate) syncs: Syncs,
}

impl PageStore {
    /// Whether a batch is open: marked, its end not written yet.
    #[must_use]
    pub fn is_cycling(&self) -> bool {
        self.cycle.borrow().open
    }

    /// Whether a durable write waits on the open batch.
    #[must_use]
    pub fn is_awaited(&self) -> bool {
        let cycle = self.cycle.borrow();
        cycle.open && !cycle.current.is_empty()
    }

    /// Whether a durable write waits for a batch not opened yet — one a
    /// cycle opened now would carry.
    #[must_use]
    pub fn is_wanted(&self) -> bool {
        !self.cycle.borrow().next.is_empty()
    }

    /// Open a batch, if there is anything to settle and the previous one is
    /// durable. Answers whether it opened one; object mode never does — its
    /// writes go up with an upload.
    ///
    /// # Errors
    /// [`StorageError::BarrierFailed`] once a barrier or a window write
    /// failed.
    pub fn begin_cycle(&self) -> StorageResult<bool> {
        if !self.has_disk() {
            return Ok(false);
        }
        self.settle_durable();
        let mut cycle = self.cycle.borrow_mut();
        cycle.syncs.check()?;
        if cycle.open || !cycle.syncs.settled() {
            return Ok(false);
        }
        // Windows written since the last batch ended — a compaction's moves,
        // a restore — need a batch to end over them as much as writes do.
        // And so do headers a full restatement left behind.
        let since = {
            let chain = self.chain.borrow();
            chain.tail != chain.last_end || chain.outlived()
        };
        if !self.has_pending() && !self.has_unrecorded() && !since {
            return Ok(false);
        }
        self.cache.borrow_mut().rotate();
        cycle.open = true;
        cycle.current = std::mem::take(&mut cycle.next);
        Ok(true)
    }

    /// One step of the open batch: a round of at most `budget` owed ids, or
    /// a header naming what is left unrecorded. [`Progress::Done`] once the
    /// batch is ended — or when none was open.
    ///
    /// The round waits for its window from the storage shard (RFC 0073 §3)
    /// — at once from a store's own files — and a node's task is parked
    /// meanwhile; [`cycle_step`](Self::cycle_step) is the same step for a
    /// caller with nothing else to run.
    ///
    /// # Errors
    /// A page write fault — the store is then failed (`place_in`).
    pub async fn settle_step(&self, budget: usize) -> StorageResult<Progress> {
        if !self.is_cycling() {
            return Ok(Progress::Done);
        }
        let through = self.gate.enter().await;
        let owes = self.cache.borrow().owes();
        let ended = if owes {
            let mut round = self.cache.borrow().round(budget);
            let work = std::mem::take(&mut round.work);
            let ended = self.settle(work, round.complete).await?;
            self.cache.borrow_mut().advance(&round);
            ended
        } else {
            self.record_unrecorded(true).await?
        };
        drop(through);
        if !ended {
            return Ok(Progress::Settled);
        }
        self.end_batch()?;
        Ok(Progress::Done)
    }

    /// [`settle_step`](Self::settle_step) to its end on the calling thread.
    ///
    /// # Errors
    /// As [`settle_step`](Self::settle_step).
    pub fn cycle_step(&self, budget: usize) -> StorageResult<Progress> {
        park_on(self.settle_step(budget))
    }

    /// The batch's last header is written: drop the headers it leaves out of
    /// reach, and ask for its barrier.
    fn end_batch(&self) -> StorageResult<()> {
        let tail = self.chain.borrow().tail;
        if let Some(at) = tail {
            self.drop_dead_headers(at);
        }
        self.root_remote();
        let mark = self.own().map_or(0, |alloc| alloc.borrow().mark());
        let (batch, waiters) = {
            let mut cycle = self.cycle.borrow_mut();
            cycle.open = false;
            cycle.ended += 1;
            let batch = cycle.ended;
            cycle.covers.push((batch, mark));
            (batch, std::mem::take(&mut cycle.current))
        };
        if self.own().is_none() {
            // The storage thread takes the barrier (RFC 0073 §4).
            let sealed = self.seal(batch, tail.unwrap_or(NO_FILE), waiters);
            if sealed.is_err() {
                self.fail();
            }
            return sealed;
        }
        let Some(barrier) = &self.barrier else {
            let synced = self.file.sync();
            self.cycle
                .borrow_mut()
                .syncs
                .done_inline(batch, synced.is_ok());
            answer(waiters, synced.is_ok());
            self.settle_durable();
            return synced;
        };
        match self.file.take_dirty() {
            Ok(files) => {
                let mut cycle = self.cycle.borrow_mut();
                barrier.sync(&mut cycle.syncs, batch, files, waiters);
                Ok(())
            }
            Err(e) => {
                self.fail();
                answer(waiters, false);
                Err(e)
            }
        }
    }

    /// Take in what the barriers answered: the frees a durable batch
    /// covered become holes, and the object store's files only the batch
    /// before it named may go (`crate::remote_compact`).
    pub(crate) fn settle_durable(&self) {
        let mark = {
            let mut cycle = self.cycle.borrow_mut();
            let answered = cycle.syncs.answered();
            self.file.landed(answered);
            let mark = cycle
                .covers
                .iter()
                .filter(|(batch, _)| *batch <= answered)
                .map(|(_, mark)| *mark)
                .max();
            cycle.covers.retain(|(batch, _)| *batch > answered);
            mark
        };
        if let Some(mark) = mark {
            if let Some(alloc) = self.own() {
                alloc.borrow_mut().release_to(mark);
            }
            self.release_remote_root();
        }
    }

    /// The batch a window written now belongs to: the next to end.
    pub(crate) fn next_batch(&self) -> u64 {
        self.cycle.borrow().ended + 1
    }

    /// A wait for the next batch to end and be durable — a durable write's,
    /// registered right after it commits.
    pub(crate) fn wait_for_batch(&self) -> Durable {
        let (waiter, wait) = Durable::new();
        self.cycle.borrow_mut().next.push(waiter);
        wait
    }

    /// Refuse every later write: a write or a barrier failed, and what is
    /// on disk is not known any more.
    pub(crate) fn fail(&self) {
        self.cycle.borrow_mut().syncs.fail();
    }

    /// Refuse a write once the store failed.
    ///
    /// # Errors
    /// [`StorageError::BarrierFailed`].
    pub(crate) fn check_failed(&self) -> StorageResult<()> {
        self.cycle.borrow_mut().syncs.check()
    }

    /// Settle everything committed and make it durable before returning:
    /// the open batch ended, then one more for whatever came after it, each
    /// barrier waited for — on the calling thread, which blocks for it. What
    /// a close, a conversion or a test needs; a node steps
    /// [`cycle_step`](Self::cycle_step) between its jobs instead.
    ///
    /// In object mode it settles into the pages in RAM, which an upload
    /// then carries.
    ///
    /// # Errors
    /// A page write or barrier fault.
    pub fn drain(&self) -> StorageResult<()> {
        if !self.has_disk() {
            loop {
                let mut round = self.cache.borrow().round(usize::MAX);
                if round.is_empty() {
                    return Ok(());
                }
                let work = std::mem::take(&mut round.work);
                park_on(async {
                    let _through = self.gate.enter().await;
                    self.settle(work, false).await
                })?;
                self.cache.borrow_mut().advance(&round);
            }
        }
        loop {
            self.hurry();
            while self.cycle_step(usize::MAX)? == Progress::Settled {}
            self.cycle.borrow_mut().syncs.wait();
            self.settle_durable();
            if !self.begin_cycle()? {
                return self.check_failed();
            }
        }
    }

    /// [`drain`](Self::drain) without blocking the thread: every step
    /// awaited, and each barrier's answer awaited by looking again every
    /// millisecond — what a worker runs as a job beside its others (a
    /// benchmark's phase boundary, an operator's "settle now"). A batch the
    /// worker's own maintenance opened meanwhile is stepped to its end here
    /// too, so it returns once nothing committed before it is owed.
    ///
    /// Object mode has no barrier to wait for: it settles as `drain` does.
    ///
    /// # Errors
    /// A page write or barrier fault.
    pub async fn settle_all(&self) -> StorageResult<()> {
        if !self.has_disk() {
            return self.drain();
        }
        loop {
            self.hurry();
            while self.settle_step(usize::MAX).await? == Progress::Settled {}
            self.settle_durable();
            let durable = {
                let mut cycle = self.cycle.borrow_mut();
                cycle.syncs.check()?;
                cycle.syncs.settled()
            };
            if !durable {
                wavedb_platform::time::sleep(Duration::from_millis(1)).await;
                continue;
            }
            if !self.begin_cycle()? {
                return self.check_failed();
            }
        }
    }
}

/// Answer every waiter of a batch.
fn answer(waiters: Vec<Waiter>, ok: bool) {
    for waiter in waiters {
        let _ = waiter.send(if ok {
            Ok(())
        } else {
            Err(StorageError::BarrierFailed)
        });
    }
}
