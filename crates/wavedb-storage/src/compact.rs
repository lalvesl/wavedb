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
//! A compaction is stepped like a checkpoint ([`PageStore::compact_step`]),
//! the caller choosing when; the store keeps where it is ([`Phase`]):
//!
//! 1. **Moving.** Bounded passes, each a few rounds of one window write —
//!    the same write a settle round makes, carrying its own addressing delta
//!    ([RFC 0046]). A file holding nothing but the addressing log's chunks
//!    gets one snapshot in the active file, which releases them all
//!    ([RFC 0048]).
//! 2. **The old addressing goes.** The moves leave a chain of delta chunks;
//!    one snapshot replaces it.
//! 3. **Sealing.** The vacated runs are still named by the last `Commit`
//!    frame — a crash would reopen from it — so the allocator holds them back
//!    ([RFC 0047]). One checkpoint names the moved state; the next one's
//!    opening proves that frame durable and releases the old generation.
//! 4. **Reclaiming.** Every file's free tail is cut off, and a file left
//!    empty is deleted ([`reclaim`](PageStore::reclaim)). Every checkpoint's
//!    opening does the same (`crate::retire`), so a long compaction gives
//!    space back as it goes, not only at its end.
//!
//! Safety is the checkpoint's: a moved run is written before any descriptor
//! names it, and nothing is cut while a recovery root names it.
//!
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md
//! [RFC 0047]: ../../../rfcs/0047-generational-journal-retirement.md
//! [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use crate::error::StorageResult;
use crate::page_store::PageStore;

/// A shard's space, in 4 KiB blocks, across its data files.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Space {
    /// The files' length.
    pub file_blocks: u64,
    /// Free: holes no run will use again.
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
/// `since` is what [`reclaimed`](PageStore::reclaimed) read when it began.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    #[default]
    Idle,
    Moving {
        since: u64,
    },
    Sealing {
        since: u64,
    },
    Releasing {
        since: u64,
    },
}

impl PageStore {
    /// How the data files' blocks are used.
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

    /// Blocks cut off the data files since the store opened.
    #[must_use]
    pub const fn reclaimed(&self) -> u64 {
        self.reclaimed.get()
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
            Phase::Idle => {
                let since = self.reclaimed();
                self.compaction.set(Phase::Moving { since });
                self.compact_step(budget_blocks)
            }
            Phase::Moving { since } => {
                let (images, blocks, finished) = self.shift(budget_blocks)?;
                if finished {
                    if self.meta.borrow().has_deltas() {
                        self.rewrite_addressing()?;
                    }
                    self.compaction.set(Phase::Sealing { since });
                }
                Ok(Compacting::Moved { images, blocks })
            }
            Phase::Sealing { since } => {
                // An open checkpoint closes over the moved state as well as a
                // fresh one would.
                if !self.is_checkpointing() {
                    self.begin_checkpoint()?;
                }
                self.compaction.set(Phase::Releasing { since });
                Ok(Compacting::Sealing)
            }
            Phase::Releasing { since } => {
                if self.is_checkpointing() {
                    return Ok(Compacting::Sealing);
                }
                // Opening the next checkpoint proves that frame durable and
                // drops the generation that still named the vacated runs —
                // and reclaims what that released.
                self.begin_checkpoint()?;
                self.reclaim()?;
                self.compaction.set(Phase::Idle);
                Ok(Compacting::Done {
                    reclaimed_blocks: self.reclaimed() - since,
                })
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

    /// Cut the free tail off every data file but the active one, and delete
    /// the files left empty, returning the blocks reclaimed. Only free blocks
    /// are in a tail — a run a recovery root names is held back, not freed —
    /// so no reopen can need them.
    ///
    /// # Errors
    /// [`StorageError::Io`](crate::StorageError::Io) if a resize or an unlink
    /// fails; the file is then merely longer than the allocator says, which
    /// a reopen reads as free tail.
    pub fn reclaim(&self) -> StorageResult<u64> {
        let cuts = self.alloc.borrow_mut().reclaim(false);
        let mut blocks = 0;
        for cut in cuts {
            self.file.cut(cut.file, cut.end)?;
            blocks += cut.was - cut.end;
            self.reclaimed.set(self.reclaimed.get() + cut.was - cut.end);
        }
        Ok(blocks)
    }

    fn close_open_checkpoint(&self) -> StorageResult<()> {
        while self.checkpoint_step(usize::MAX)?
            == crate::commit_steps::Checkpointing::Settling
        {}
        Ok(())
    }
}
