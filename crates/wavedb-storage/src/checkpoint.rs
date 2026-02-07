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

use crate::block::{BlockDescriptor, Run};
use crate::error::StorageResult;
use crate::page_cache::Lane;
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::record_cache::Records;
use crate::window::{
    Target, assemble, carve, dict_descriptors, install, mark_run, size_of,
    superseded, targets_of,
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

/// A round's plans and what its window holds, between its reservation and
/// its build.
struct Round {
    plans: Vec<SlotPlan>,
    targets: Vec<Target>,
    placement: Placement,
    end: bool,
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
    ///
    /// The caller holds the store's [`Gate`](crate::wait::Gate): the plans
    /// are made from the directories the round then publishes into.
    pub(crate) async fn settle(
        &self,
        work: Vec<(usize, Records)>,
        end: bool,
    ) -> StorageResult<bool> {
        let mut plans = Vec::with_capacity(work.len());
        for (idx, records) in work {
            plans.push(self.plan_slot(idx, records)?);
        }
        self.place_in(plans, Placement::Settle, end).await
    }

    /// Write already-serialised images at the end of the active file, and
    /// repoint them — compaction's move (RFC 0071 §7).
    ///
    /// # Errors
    /// Propagates the window write fault; nothing is repointed if it fails.
    pub(crate) async fn relocate(
        &self,
        plans: Vec<SlotPlan>,
    ) -> StorageResult<()> {
        self.place_in(plans, Placement::Move, false)
            .await
            .map(|_| ())
    }

    /// Write already-planned images (a recovery's dictionaries) as one
    /// ordinary window.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) async fn place_plans(
        &self,
        plans: Vec<SlotPlan>,
    ) -> StorageResult<()> {
        self.place_in(plans, Placement::Settle, false)
            .await
            .map(|_| ())
    }

    /// Write a page restored from the object store.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) async fn place_restored(
        &self,
        plan: SlotPlan,
    ) -> StorageResult<()> {
        self.place_in(vec![plan], Placement::Restore, false)
            .await
            .map(|_| ())
    }

    /// A window of only a header: what is unrecorded, named — ending the
    /// batch if `end` asks and it all fits. Whether it ended it.
    ///
    /// # Errors
    /// Propagates the window write fault.
    pub(crate) async fn record_unrecorded(
        &self,
        end: bool,
    ) -> StorageResult<bool> {
        self.place_in(Vec::new(), Placement::Settle, end).await
    }

    /// One round through the storage shard ([RFC 0073] §3): size the window
    /// — every planned image and, on disk, the header — **reserve** it, then
    /// build it at its real addresses, write it, swap the descriptors in and
    /// release the superseded runs. Whether the header ended the batch.
    ///
    /// The reservation is the one wait; the caller holds the store's
    /// [`Gate`](crate::wait::Gate) across it.
    ///
    /// # Errors
    /// The window write's fault. Nothing is published then — but the
    /// header's bookkeeping has moved on and the file's state is unknown, so
    /// the store is failed: every later write is refused until a reopen
    /// reconstructs it from what is on disk.
    ///
    /// [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md
    async fn place_in(
        &self,
        plans: Vec<SlotPlan>,
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
            // A storage thread ends its own writes with one (RFC 0073 §4).
            if end && self.own().is_some() {
                targets.push(Target::Roots);
            }
        }
        let sizes: Vec<u64> =
            targets.iter().map(|t| size_of(&plans, t)).collect();
        let total: u64 = sizes.iter().sum();
        let window = match total {
            0 => None,
            blocks => Some(self.reserve(blocks).await?),
        };
        let round = Round {
            plans,
            targets,
            placement,
            end,
        };
        let built = self.build(round, &sizes, window);
        if built.is_err()
            && let Some(window) = window
        {
            self.abandon(window);
        }
        built
    }

    /// Build the round at its reserved `window`, write it and publish it
    /// (see [`place_in`](Self::place_in)).
    fn build(
        &self,
        round: Round,
        sizes: &[u64],
        window: Option<Run>,
    ) -> StorageResult<bool> {
        let Round {
            mut plans,
            targets,
            placement,
            end,
        } = round;
        let logged = self.has_disk();
        let runs = window
            .map_or_else(|| vec![None; targets.len()], |w| carve(w, sizes));
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
        let (mut header, mut roots) = (Vec::new(), Vec::new());
        if logged {
            let pages = targets.iter().filter_map(|t| match t {
                Target::Page { plan, bucket } => {
                    Some((plans[*plan].idx, *bucket))
                }
                _ => None,
            });
            let grown = dicts.keys().map(|&p| plans[p].idx);
            self.note_placed(pages, grown);
            if let Some(at) = mark_run(&targets, &runs, false) {
                (header, ended) =
                    self.header_at(at.start, &plans, &dicts, end)?;
                if let Some(block) = mark_run(&targets, &runs, true) {
                    roots = self.roots_at(block.start, at.start, ended)?;
                }
            }
        }
        if let Some(window) = window {
            let marks = (header.as_slice(), roots.as_slice());
            let buf = assemble(window, &plans, &targets, &runs, marks);
            let written = self.write_window(window, buf);
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
            self.retire_run(run);
        }
        if pending.is_some() {
            // A page written for use — settled or brought back — is hot.
            for target in &targets {
                if let Target::Page { plan, bucket } = target {
                    self.touch(plans[*plan].idx, *bucket);
                }
            }
        }
        self.publish(plans, &dicts);
        Ok(ended)
    }

    /// Adopt the working directories and the new dictionary runs.
    fn publish(
        &self,
        plans: Vec<SlotPlan>,
        dicts: &BTreeMap<usize, BlockDescriptor>,
    ) {
        for (plan, work) in plans.iter().enumerate() {
            let Some(&desc) = dicts.get(&plan) else {
                continue;
            };
            let old =
                self.types[work.idx].dictionary().borrow_mut().repoint(desc);
            if old.is_allocated() {
                self.retire_run(old.run());
            }
        }
        for work in plans {
            *self.types[work.idx].directory().borrow_mut() = Some(work.dir);
        }
    }
}
