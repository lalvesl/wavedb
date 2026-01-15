//! Phases 2–4 and 6 of a checkpoint ([RFC 0041]) — one allocation, one write,
//! then the in-memory swap that publishes the new pages.
//!
//! Every image [`crate::plan`] produced — the rewritten pages and a grown
//! dictionary — is placed inside **one contiguous window**: the allocator's
//! existing best-fit finds the smallest free extent that holds the whole
//! round (a recycled window when one exists, the file's tail otherwise), the
//! window is filled block-aligned, and it reaches the disk as a single
//! positioned write. Only afterwards are the descriptor vectors swapped — so
//! a reader either sees the previous pages (still allocated, still valid:
//! page writes are copy-on-write) or the new ones, never a half-written run.
//!
//! The window's last target is the round's own [`EditChunk`](crate::edit):
//! the descriptor changes these pages caused, riding the same write
//! ([RFC 0046]). Its length is fixed by the round's *shape*, so a reservation
//! pass over the same plans predicts it exactly; the second pass, after
//! `install`, fills in the addresses.
//!
//! Nothing on disk records that these blocks were written together: the window
//! is an allocation-time concept only, and each page keeps its own descriptor.
//! That is what lets pages be freed one by one later and the extent coalesce
//! back into a large hole.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md

use std::collections::BTreeMap;

use crate::block::{BlockAllocator, BlockDescriptor};
use crate::edit;
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::record_cache::Records;
use crate::window::{
    Target, assemble, carve, dict_descriptors, install, size_of, superseded,
    targets_of,
};

/// Where the window itself is allocated.
#[derive(Clone, Copy)]
enum Placement {
    /// The allocator's best fit: a recycled hole when one holds the whole
    /// round, the tail otherwise. Every ordinary settle.
    BestFit,
    /// The lowest-addressed hole that holds the round and ends at or before
    /// this block — compaction's move, which must only ever go down. No such
    /// hole means no round.
    Below(u64),
    /// A full addressing snapshot even if no page moves, in the lowest hole
    /// that holds it — compaction dropping the chain it has outgrown.
    Snapshot,
}

impl PageStore {
    /// Settle a round's records into pages: plan every slot, then write the
    /// whole round as one window and publish it.
    ///
    /// Durability is not taken here: the journal already holds the batch, and
    /// `data.bin` only becomes authoritative at
    /// [`commit_journal`](Self::commit_journal), which syncs the pages before
    /// the `Commit` frame addresses them.
    ///
    /// # Errors
    /// Propagates page read/write, corruption, or compression faults. The
    /// caller leaves the settle cursors where they were; re-settling is
    /// idempotent because a round copies each record's newest entry.
    pub(crate) fn settle(
        &self,
        work: Vec<(usize, Records)>,
    ) -> StorageResult<()> {
        let mut plans = Vec::with_capacity(work.len());
        for (idx, records) in work {
            plans.push(self.plan_slot(idx, records)?);
        }
        self.place_in(plans, Placement::BestFit).map(|_| ())
    }

    /// Write already-serialised images into the lowest hole that ends at or
    /// before `limit`, and repoint them — compaction's move
    /// ([RFC 0069](../../../rfcs/0069-compaction-into-the-holes.md)).
    /// `false`, with nothing changed, when no such hole holds the round.
    ///
    /// # Errors
    /// Propagates the window write fault; nothing is repointed if it fails.
    pub(crate) fn relocate(
        &self,
        plans: Vec<SlotPlan>,
        limit: u64,
    ) -> StorageResult<bool> {
        self.place_in(plans, Placement::Below(limit))
    }

    /// Rewrite the addressing log as one snapshot in the lowest hole that
    /// holds it, releasing every chunk before it.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) fn rewrite_addressing(&self) -> StorageResult<()> {
        self.place_in(Vec::new(), Placement::Snapshot).map(|_| ())
    }

    /// Phases 2–4 + 6: carve one window for every planned image *and* the
    /// round's edit chunk, write it, swap the descriptors in, and release the
    /// superseded runs. `false` only for a [`Placement::Below`] no hole fits,
    /// and then nothing has happened.
    ///
    /// # Errors
    /// The window write's fault, or [`StorageError::Corrupt`] if the filled
    /// edit chunk outgrew the space its skeleton reserved (unreachable: both
    /// passes build the same shape). Either way nothing has been written,
    /// freed, or published.
    // The allocator guard deliberately spans carve → write → free: the window
    // must not be handed out twice, and a superseded run must not return to
    // the pool before the page replacing it is on disk.
    #[allow(clippy::significant_drop_tightening)]
    fn place_in(
        &self,
        mut plans: Vec<SlotPlan>,
        placement: Placement,
    ) -> StorageResult<bool> {
        let snapshot = matches!(placement, Placement::Snapshot);
        let mut targets = targets_of(&plans);
        if targets.is_empty() && !snapshot {
            return Ok(true);
        }
        let mut alloc = self.alloc.borrow_mut();
        let mut meta = self.meta.borrow_mut();

        // Pass one: the chunk's shape fixes its length, so a skeleton built
        // from the same plans reserves exactly what the filled one needs.
        let full = snapshot || meta.wants_snapshot();
        // A snapshot stands alone, so it ends the chain rather than extending
        // it (RFC 0048). `prev` is known before the carve and is a fixed 8
        // bytes, so it does not disturb the shape both passes agree on.
        let prev = if full { 0 } else { meta.head() };
        let reserved = edit::encode(&edit::chunk_of(
            self,
            &plans,
            full,
            &BTreeMap::new(),
            prev,
        ));
        targets.push(Target::Edit);
        let sizes: Vec<u64> = targets
            .iter()
            .map(|t| size_of(&plans, t, &reserved))
            .collect();
        let total: u64 = sizes.iter().sum();

        let window = match placement {
            _ if total == 0 => None,
            Placement::BestFit => Some(alloc.alloc(total)),
            Placement::Snapshot => Some(alloc.alloc_lowest(total)),
            Placement::Below(limit) => match alloc.alloc_below(total, limit) {
                Some(run) => Some(run),
                None => return Ok(false),
            },
        };
        let runs = window
            .map_or_else(|| vec![None; targets.len()], |w| carve(w, &sizes));
        // A settle writes what the object store has not seen; a move does
        // not change what it moves.
        let pending = matches!(placement, Placement::BestFit)
            .then_some(self.object.is_some());
        let changed = install(&mut plans, &targets, &runs, pending);

        // Pass two: the same shape, now naming the runs just handed out.
        let dicts = dict_descriptors(&plans, &targets, &runs);
        let chunk =
            edit::encode(&edit::chunk_of(self, &plans, full, &dicts, prev));
        if chunk.len() > reserved.len() {
            return Err(StorageError::Corrupt("edit chunk outgrew its window"));
        }

        if let Some(window) = window {
            let buf = assemble(window, &plans, &targets, &runs, &chunk);
            self.file.write_run(window, &buf)?; // the round's one write
        }
        if let Some(object) = &self.object {
            // The chunk just written carries every upload result it had.
            object.unrecorded.borrow_mut().slots.clear();
        }
        for run in superseded(&changed, self.object.as_ref()) {
            // No directory names it any more, so no read will ask for it:
            // its image goes now rather than when the budget pushes it out.
            self.file.page_cache().invalidate(run);
            alloc.free(run);
        }
        if let Some(run) = runs.last().copied().flatten() {
            let desc = BlockDescriptor::from_run(run);
            for stale in meta.record(desc, full) {
                alloc.free(stale);
            }
        }
        self.publish(plans, &dicts, &mut alloc);
        Ok(true)
    }

    /// Phase 4: adopt the working directories and the new dictionary runs.
    fn publish(
        &self,
        plans: Vec<SlotPlan>,
        dicts: &BTreeMap<usize, BlockDescriptor>,
        alloc: &mut BlockAllocator,
    ) {
        for (plan, work) in plans.iter().enumerate() {
            let Some(&desc) = dicts.get(&plan) else {
                continue;
            };
            let old =
                self.types[work.idx].dictionary().borrow_mut().repoint(desc);
            if old.is_allocated() {
                alloc.free(old.run());
            }
        }
        for work in plans {
            *self.types[work.idx].directory().borrow_mut() = Some(work.dir);
        }
    }
}
