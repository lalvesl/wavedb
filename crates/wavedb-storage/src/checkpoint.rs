//! A settle window ([RFC 0041], [RFC 0072] §2–§3) — one allocation, one
//! write, then the in-memory swap that publishes the new pages.
//!
//! Every image [`crate::plan`] produced — the rewritten pages and a grown
//! dictionary — is placed inside **one contiguous window** at the end of the
//! active data file (writing only ever appends, [RFC 0071]), and on disk the
//! window ends with its **header**: 32 KiB naming the addresses it changed
//! (`crate::disk_header`). The window reaches the disk as a single
//! positioned write. Only afterwards are the descriptor vectors swapped — so
//! a reader either sees the previous pages (still allocated, still valid:
//! page writes are copy-on-write) or the new ones, never a half-written run.
//!
//! Nothing on disk records that these blocks were written together: each
//! page keeps its own descriptor, which is what lets pages be freed one by
//! one later, and compaction carry the live ones forward on their own.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::collections::BTreeMap;

use crate::block::{BlockAllocator, BlockDescriptor, Run};
use crate::error::{StorageError, StorageResult};
use crate::page_cache::Lane;
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::record_cache::Records;
use crate::window::{
    Target, assemble, carve, dict_descriptors, install, size_of, superseded,
    targets_of,
};

/// What a window writes — which decides the "not uploaded yet" flag of the
/// pages it places. Every window appends to the active data file.
#[derive(Clone, Copy)]
pub enum Placement {
    /// Every ordinary settle: what the object store has not seen.
    Settle,
    /// Compaction's copies (RFC 0071 §7): a move does not change what it
    /// moves, flag included.
    Move,
    /// A page brought back from the object store (RFC 0070 §7): what it
    /// holds is what the object store holds, so no flag.
    Restore,
}

impl PageStore {
    /// Settle a round's records into pages: plan every slot, then write the
    /// whole round as one window and publish it. Whether its header ended
    /// the batch — `end` asks it to.
    ///
    /// # Errors
    /// Propagates page read/write, corruption, or compression faults. The
    /// caller leaves the settle cursors where they were; re-settling is
    /// idempotent because a round copies each record's newest entry.
    pub(crate) fn settle(
        &self,
        work: Vec<(usize, Records)>,
        end: bool,
    ) -> StorageResult<bool> {
        let mut plans = Vec::with_capacity(work.len());
        for (idx, records) in work {
            plans.push(self.plan_slot(idx, records)?);
        }
        self.place_in(plans, Placement::Settle, end)
    }

    /// Write already-serialised images at the end of the active file, and
    /// repoint them — compaction's move (RFC 0071 §7).
    ///
    /// # Errors
    /// Propagates the window write fault; nothing is repointed if it fails.
    pub(crate) fn relocate(&self, plans: Vec<SlotPlan>) -> StorageResult<()> {
        self.place_in(plans, Placement::Move, false).map(|_| ())
    }

    /// Write already-planned images (a recovery's dictionaries) as one
    /// ordinary window.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) fn place_plans(
        &self,
        plans: Vec<SlotPlan>,
    ) -> StorageResult<()> {
        self.place_in(plans, Placement::Settle, false).map(|_| ())
    }

    /// Write a page restored from the object store.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) fn place_restored(&self, plan: SlotPlan) -> StorageResult<()> {
        self.place_in(vec![plan], Placement::Restore, false)
            .map(|_| ())
    }

    /// A window of only a header: what is unrecorded, named — ending the
    /// batch if `end` asks and it all fits. Whether it ended it.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) fn record_unrecorded(&self, end: bool) -> StorageResult<bool> {
        self.place_in(Vec::new(), Placement::Settle, end)
    }

    /// Carve one window for every planned image and, on disk, the header;
    /// write it, swap the descriptors in, and release the superseded runs.
    /// Whether the header ended the batch.
    ///
    /// # Errors
    /// The window write's fault. Nothing is published then — but the
    /// header's bookkeeping has moved on and the file's state is unknown, so
    /// the store is failed: every later write is refused until a reopen
    /// reconstructs it from what is on disk.
    // The allocator guard deliberately spans carve → write → free: the window
    // must not be handed out twice, and a superseded run must not return to
    // the pool before the page replacing it is on disk.
    #[allow(clippy::significant_drop_tightening)]
    fn place_in(
        &self,
        mut plans: Vec<SlotPlan>,
        placement: Placement,
        end: bool,
    ) -> StorageResult<bool> {
        // Object mode keeps no header: nothing reopens from RAM (RFC 0070
        // §9), so a window holds its images and nothing else.
        let logged = self.has_disk();
        let mut targets = targets_of(&plans);
        if targets.is_empty() && !(logged && (end || self.has_unrecorded())) {
            return Ok(false);
        }
        if logged {
            targets.push(Target::Header);
        }
        let mut alloc = self.alloc.borrow_mut();
        let sizes: Vec<u64> =
            targets.iter().map(|t| size_of(&plans, t)).collect();
        let total: u64 = sizes.iter().sum();
        let window = (total > 0).then(|| alloc.alloc(total));
        let runs = window
            .map_or_else(|| vec![None; targets.len()], |w| carve(w, &sizes));
        // A settle writes what the object store has not seen; a move does
        // not change what it moves.
        let pending = match placement {
            Placement::Settle => Some(self.object.is_some()),
            Placement::Restore => Some(false),
            Placement::Move => None,
        };
        let changed = install(&mut plans, &targets, &runs, pending);
        let dicts = dict_descriptors(&plans, &targets, &runs);
        let mut ended = false;
        let mut header = Vec::new();
        if logged {
            let pages = targets.iter().filter_map(|t| match t {
                Target::Page { plan, bucket } => {
                    Some((plans[*plan].idx, *bucket))
                }
                _ => None,
            });
            let grown = dicts.keys().map(|&p| plans[p].idx);
            self.note_placed(pages, grown);
            if let Some(at) = runs.last().copied().flatten() {
                (header, ended) =
                    self.header_at(at.start, &plans, &dicts, end)?;
            }
        }
        if let Some(window) = window {
            let buf = assemble(window, &plans, &targets, &runs, &header);
            let written = alloc
                .file_of(window.start)
                .ok_or(StorageError::Corrupt("a window outside every file"))
                .and_then(|file| self.file.write_run(file, window, &buf));
            if let Err(e) = written {
                self.fail();
                return Err(e); // the round's one write
            }
        }
        // The pages just written are the ones read next: held decoded under
        // their new runs (RFC 0044) — after the write, which forgot the
        // window.
        for (target, run) in targets.iter().zip(&runs) {
            if let (Target::Page { plan, bucket }, Some(run)) = (target, run)
                && let Some(page) = plans[*plan].decoded.remove(bucket)
            {
                self.file.page_cache().insert(Lane::Disk, *run, page);
            }
        }
        for run in superseded(&changed, self.object.as_ref()) {
            // No directory names it any more, so no read will ask for it:
            // its image goes now rather than when the budget pushes it out.
            self.file.page_cache().invalidate(run);
            self.retire_run(&mut alloc, run);
        }
        if pending.is_some() {
            // A page written for use — settled or brought back — is hot.
            for target in &targets {
                if let Target::Page { plan, bucket } = target {
                    self.touch(plans[*plan].idx, *bucket);
                }
            }
        }
        self.publish(plans, &dicts, &mut alloc);
        Ok(ended)
    }

    /// A run no directory names any more: on disk, freed once the batch
    /// that stopped naming it is durable (`crate::alloc`); in RAM, at once.
    pub(crate) fn retire_run(&self, alloc: &mut BlockAllocator, run: Run) {
        if self.has_disk() {
            alloc.defer(run);
        } else {
            alloc.free(run);
        }
    }

    /// Adopt the working directories and the new dictionary runs.
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
                self.retire_run(alloc, old.run());
            }
        }
        for work in plans {
            *self.types[work.idx].directory().borrow_mut() = Some(work.dir);
        }
    }
}
