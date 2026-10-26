//! Compaction ([RFC 0069]) — moving live runs **down** into the holes, so the
//! free space gathers at the tail of `data.bin` and the file can shrink.
//!
//! Copy-on-write leaves a hole wherever a page used to be. Best-fit reuses
//! the holes a settle round fits, but nothing ever gives the rest back: the
//! file only grows. Compaction takes the runs nearest the end of the file —
//! pages and dictionaries — and rewrites them, verbatim, into the
//! lowest-addressed hole that holds them ([`alloc_below`](crate::block::BlockAllocator::alloc_below)).
//! A run only ever moves down, so the live data packs toward the front and
//! what it vacated is one free extent at the back, which
//! [`truncate`](crate::block::BlockAllocator::truncate) cuts off.
//!
//! ## In steps
//!
//! A compaction is stepped like a checkpoint ([`PageStore::compact_step`]),
//! the caller choosing when; the store keeps where it is ([`Phase`]):
//!
//! 1. **Moving.** Bounded passes, each a few rounds of one window write per
//!    hole filled — the same write a settle round makes, carrying its own
//!    addressing delta ([RFC 0046]).
//! 2. **The old addressing goes.** The moves leave a chain of delta chunks,
//!    and the chunks before them may sit anywhere in the file. One snapshot
//!    in the lowest hole that holds it replaces the whole chain
//!    ([RFC 0048]), releasing every chunk it supersedes.
//! 3. **Sealing.** The vacated runs are still named by the last `Commit`
//!    frame — a crash would reopen from it — so the allocator holds them back
//!    ([RFC 0047]). One checkpoint names the moved state; the next one's
//!    opening proves that frame durable and releases the old generation.
//! 4. **Truncating.** The free tail leaves the file. Should the snapshot of
//!    step 2 have had to land high, it is rewritten once more into the room
//!    the release opened, and sealed again.
//!
//! Safety is the checkpoint's: a moved run is written before any descriptor
//! names it, and nothing is freed into reuse while a recovery root names it.
//!
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md
//! [RFC 0047]: ../../../rfcs/0047-generational-journal-retirement.md
//! [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md
//! [RFC 0069]: ../../../rfcs/0069-compaction-into-the-holes.md

use crate::block::Run;
use crate::error::StorageResult;
use crate::page_store::PageStore;

/// `data.bin`'s space, in 4 KiB blocks.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    /// The file's length.
    pub file_blocks: u64,
    /// Free and reusable now.
    pub free_blocks: u64,
    /// Freed, but named by a recovery root until the next checkpoints.
    pub deferred_blocks: u64,
}

/// What one [`compact_step`](PageStore::compact_step) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Compacting {
    /// A pass moved this much; more passes or the sealing follow.
    Moved { images: usize, blocks: u64 },
    /// Waiting for a checkpoint to close — the caller steps it
    /// ([`checkpoint_step`](PageStore::checkpoint_step)).
    Sealing,
    /// Finished: the free tail is out of the file.
    Done { reclaimed_blocks: u64 },
}

/// A compaction run to the end ([`compact`](PageStore::compact)).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Compacted {
    pub moved_images: usize,
    pub moved_blocks: u64,
    pub reclaimed_blocks: u64,
}

/// Where a store's compaction is between steps (see the module docs).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Idle,
    Moving,
    /// `last`: the snapshot was already rewritten after a release once;
    /// `reclaimed`: blocks truncated so far.
    Sealing {
        last: bool,
        reclaimed: u64,
    },
    Releasing {
        last: bool,
        reclaimed: u64,
    },
}

impl PageStore {
    /// How `data.bin`'s blocks are used.
    #[must_use]
    pub fn space(&self) -> Space {
        let alloc = self.alloc.borrow();
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

    /// One step of a compaction, starting one if none is under way: a pass
    /// copying about `budget_blocks` (at least one image), or the next move
    /// of the sealing. A [`Compacting::Sealing`] step leaves a checkpoint
    /// open for the caller to step closed.
    ///
    /// # Errors
    /// Read, write or sync faults. A move that faults has repointed nothing;
    /// the phase stays, and the next step retries.
    pub fn compact_step(
        &self,
        budget_blocks: u64,
    ) -> StorageResult<Compacting> {
        match self.compaction.get() {
            Phase::Idle | Phase::Moving => {
                self.compaction.set(Phase::Moving);
                let (images, blocks, finished) = self.shift(budget_blocks)?;
                if finished {
                    let lower = self.snapshot_would_drop();
                    if self.meta.borrow().has_deltas() || lower {
                        self.rewrite_addressing()?;
                    }
                    self.compaction.set(Phase::Sealing {
                        last: false,
                        reclaimed: 0,
                    });
                }
                Ok(Compacting::Moved { images, blocks })
            }
            Phase::Sealing { last, reclaimed } => {
                // An open checkpoint closes over the moved state as well as a
                // fresh one would.
                if !self.is_checkpointing() {
                    self.begin_checkpoint()?;
                }
                self.compaction.set(Phase::Releasing { last, reclaimed });
                Ok(Compacting::Sealing)
            }
            Phase::Releasing { last, reclaimed } => {
                if self.is_checkpointing() {
                    return Ok(Compacting::Sealing);
                }
                // Opening the next checkpoint proves the sealing frame durable
                // and drops the generation that still named the vacated runs.
                self.begin_checkpoint()?;
                let reclaimed_blocks = reclaimed + self.reclaim_tail()?;
                if !last && self.snapshot_would_drop() {
                    self.rewrite_addressing()?;
                    self.compaction.set(Phase::Sealing {
                        last: true,
                        reclaimed: reclaimed_blocks,
                    });
                    return Ok(Compacting::Sealing);
                }
                self.compaction.set(Phase::Idle);
                Ok(Compacting::Done { reclaimed_blocks })
            }
        }
    }

    /// Compact to the end, stepping the checkpoints itself — for a caller with
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
                Compacting::Sealing => self.close_open_checkpoint()?,
                Compacting::Done { reclaimed_blocks } => {
                    out.reclaimed_blocks += reclaimed_blocks;
                    self.close_open_checkpoint()?;
                    return Ok(out);
                }
            }
        }
    }

    /// Cut the free extent at the end of the file off `data.bin`, returning
    /// the blocks reclaimed. Only free blocks are ever in that extent — a run
    /// a recovery root names is held back, not freed — so no reopen can need
    /// them.
    ///
    /// # Errors
    /// [`StorageError::Io`](crate::StorageError::Io) if the resize fails; the
    /// file is then merely longer than the allocator says, which a reopen
    /// reads as free tail.
    pub fn reclaim_tail(&self) -> StorageResult<u64> {
        let mut alloc = self.alloc.borrow_mut();
        let reclaimed = alloc.truncate();
        if reclaimed > 0 {
            let end = alloc.total_blocks();
            self.file.page_cache().invalidate(Run::new(end, reclaimed));
            self.file.truncate_to_blocks(end)?;
        }
        Ok(reclaimed)
    }

    fn close_open_checkpoint(&self) -> StorageResult<()> {
        while self.checkpoint_step(usize::MAX)?
            == crate::commit_steps::Checkpointing::Settling
        {}
        Ok(())
    }
}
