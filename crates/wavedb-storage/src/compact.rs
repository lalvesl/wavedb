//! Compaction ([RFC 0071] §7) — carrying the live runs of the emptiest data
//! files forward into the active one, so the files they leave can be cut
//! from the tail and deleted.
//!
//! Writing only ever appends, so a superseded page leaves a hole in its file
//! that nothing fills. Compaction picks the files that are mostly holes — at
//! least `victim_free_percent` free or held back, the fewest live blocks
//! first — and rewrites their runs, pages and dictionaries, **verbatim**,
//! at the end of the active file, highest first, so each file's tail empties
//! first. The active file itself can be one: it is closed to new runs
//! first, or a shard smaller than one file would never give anything back.
//!
//! ## In steps
//!
//! A compaction is stepped ([`PageStore::compact_step`]), the caller
//! choosing when; the store keeps where it is ([`Phase`]):
//!
//! 1. **Restating.** A header cannot move — the others name where it is —
//!    so a file holding one a reconstruction may still read is never a
//!    victim. The compaction first has every address restated
//!    (`record_all`): once that batch, and the one after it, have ended,
//!    no header older than the restatement is live.
//! 2. **Moving.** Bounded passes, each a few rounds of one window write —
//!    the same write a settle round makes, ending with its header.
//! 3. **Sealing.** The vacated runs are still named by the newest durable
//!    batch — a crash would reopen from it — so they are only deferred
//!    frees. The next batch to end names the moved state; once it is
//!    durable they are holes ([RFC 0072] §8).
//! 4. **Reclaiming.** Every file's free tail is cut off, and a file left
//!    empty is deleted ([`reclaim`](PageStore::reclaim)).
//!
//! Safety is the batch's: a moved run is written before any header names
//! it, and nothing is cut while a durable batch names it.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use crate::error::StorageResult;
use crate::page_store::PageStore;

/// A shard's space, in 4 KiB blocks, across its data files.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    /// The files' length.
    pub file_blocks: u64,
    /// Free: holes no run will use again.
    pub free_blocks: u64,
    /// Freed, but named by the newest durable batch until the next one is.
    pub deferred_blocks: u64,
}

/// What one [`compact_step`](PageStore::compact_step) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compacting {
    /// A pass moved this much; more passes or the sealing follow.
    Moved { images: usize, blocks: u64 },
    /// Waiting for a batch to end and be durable — the restatement before
    /// the moves, or the one over them. The caller steps the cycle
    /// ([`cycle_step`](PageStore::cycle_step)).
    Sealing,
    /// Finished: the emptied files are cut and deleted.
    Done { reclaimed_blocks: u64 },
}

/// A compaction run to the end ([`compact`](PageStore::compact)).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Compacted {
    pub moved_images: usize,
    pub moved_blocks: u64,
    pub reclaimed_blocks: u64,
}

/// Where a store's compaction is between steps (see the module docs);
/// `since` is what [`reclaimed`](PageStore::reclaimed) read when it began,
/// `mark` where the deferred frees stood when the moves ended.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Idle,
    Restating {
        since: u64,
    },
    Moving {
        since: u64,
    },
    Sealing {
        since: u64,
        mark: u64,
    },
}

impl PageStore {
    /// How the data files' blocks are used — through a storage thread,
    /// this data shard's share of its storage shard's (RFC 0073 §7).
    #[must_use]
    pub fn space(&self) -> Space {
        if let Some(share) = self.share_of_space() {
            return share;
        }
        let Some(alloc) = self.own() else {
            return Space::default();
        };
        let alloc = alloc.borrow();
        Space {
            file_blocks: alloc.total_blocks(),
            free_blocks: alloc.free_blocks(),
            deferred_blocks: alloc.deferred_blocks(),
        }
    }

    /// Whether a compaction is under way.
    #[must_use]
    pub fn is_compacting(&self) -> bool {
        self.compaction.get() != Phase::Idle
    }

    /// Blocks cut off the data files since the store opened.
    #[must_use]
    pub const fn reclaimed(&self) -> u64 {
        self.reclaimed.get()
    }

    /// One step of a compaction, starting one if none is under way: a pass
    /// copying about `budget_blocks` (at least one image), or a look at
    /// whether the moves are sealed. While [`Compacting::Sealing`], the
    /// caller steps the cycle. Its rounds wait for their windows as a
    /// settle's do ([`settle_step`](Self::settle_step)).
    ///
    /// # Errors
    /// Read, write or sync faults. A move that faults has repointed nothing;
    /// the phase stays, and the next step retries.
    pub async fn compact_pass(
        &self,
        budget_blocks: u64,
    ) -> StorageResult<Compacting> {
        loop {
            match self.compaction.get() {
                Phase::Idle => {
                    let since = self.reclaimed();
                    if !self.has_disk() {
                        self.compaction.set(Phase::Moving { since });
                        continue;
                    }
                    if self.own().is_some() {
                        self.roll_if_worth_emptying();
                    } else {
                        // The storage shard says which files to empty.
                        let percent = self.victim_free_percent;
                        let victims = self.shared_victims(percent).await?;
                        if victims.is_empty() {
                            return Ok(Compacting::Done {
                                reclaimed_blocks: 0,
                            });
                        }
                        *self.victims.borrow_mut() = victims;
                    }
                    self.record_all();
                    self.compaction.set(Phase::Restating { since });
                }
                Phase::Restating { since } => {
                    self.settle_durable();
                    let restated = {
                        let chain = self.chain.borrow();
                        !chain.restating && !chain.outlived()
                    };
                    if !restated {
                        return Ok(Compacting::Sealing);
                    }
                    self.compaction.set(Phase::Moving { since });
                }
                Phase::Moving { since } => {
                    let (images, blocks, finished) =
                        self.shift(budget_blocks).await?;
                    if finished {
                        let mark = self.seal_mark();
                        self.compaction.set(Phase::Sealing { since, mark });
                    }
                    return Ok(Compacting::Moved { images, blocks });
                }
                Phase::Sealing { since, mark } => {
                    self.settle_durable();
                    if !self.sealed(mark) {
                        return Ok(Compacting::Sealing);
                    }
                    self.reclaim()?;
                    self.evacuated()?;
                    self.victims.borrow_mut().clear();
                    self.compaction.set(Phase::Idle);
                    return Ok(Compacting::Done {
                        reclaimed_blocks: self.reclaimed() - since,
                    });
                }
            }
        }
    }

    /// Where the frees stand when the moves end: the deferred frees of a
    /// store's own files, or the batch that will cover them.
    fn seal_mark(&self) -> u64 {
        self.own()
            .map_or_else(|| self.next_batch(), |alloc| alloc.borrow().mark())
    }

    /// Whether the moves `mark` stands for are sealed: their frees holes,
    /// or the batch covering them durable.
    fn sealed(&self, mark: u64) -> bool {
        self.own().map_or_else(
            || self.cycle.borrow_mut().syncs.answered() >= mark,
            |alloc| alloc.borrow().released(mark),
        )
    }

    /// [`compact_pass`](Self::compact_pass) to its end on the calling
    /// thread.
    ///
    /// # Errors
    /// As [`compact_pass`](Self::compact_pass).
    pub fn compact_step(
        &self,
        budget_blocks: u64,
    ) -> StorageResult<Compacting> {
        crate::wait::park_on(self.compact_pass(budget_blocks))
    }

    /// Compact to the end, settling the batches itself — for a caller with
    /// nothing else to do meanwhile (a bench phase, a test, a maintenance
    /// tool). A node steps [`compact_step`](Self::compact_step) between its
    /// jobs instead.
    ///
    /// # Errors
    /// The first fault of any step.
    pub fn compact(&self) -> StorageResult<Compacted> {
        let mut out = Compacted::default();
        loop {
            match self.compact_step(u64::MAX)? {
                Compacting::Moved { images, blocks } => {
                    out.moved_images += images;
                    out.moved_blocks += blocks;
                }
                Compacting::Sealing => self.drain()?,
                Compacting::Done { reclaimed_blocks } => {
                    out.reclaimed_blocks += reclaimed_blocks;
                    return Ok(out);
                }
            }
        }
    }

    /// Cut the free tail off every data file but the active one, and delete
    /// the files left empty, returning the blocks reclaimed. Only holes are
    /// in a tail — a run the newest durable batch names is a deferred free,
    /// not a hole — so no reopen can need them.
    ///
    /// # Errors
    /// [`StorageError::Io`](crate::StorageError::Io) if a resize or an unlink
    /// fails; the file is then merely longer than the allocator says, which
    /// a reopen reads as free tail.
    pub fn reclaim(&self) -> StorageResult<u64> {
        // A storage thread reclaims after every durable write.
        let Some(alloc) = self.own() else {
            return Ok(0);
        };
        let cuts = alloc.borrow_mut().reclaim(false);
        let mut blocks = 0;
        for cut in cuts {
            self.file.cut(cut.file, cut.end)?;
            blocks += cut.was - cut.end;
            self.reclaimed.set(self.reclaimed.get() + cut.was - cut.end);
        }
        Ok(blocks)
    }
}
