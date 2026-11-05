//! A settle window ([RFC 0041], [RFC 0072] §2–§3) — one allocation, one
//! write, then the in-memory swap that publishes the new pages.
//!
//! Every image [`crate::plan`] produced — the rewritten pages and a grown
//! dictionary — is placed inside **one contiguous window** at the end of the
//! active data file (writing only ever appends, [RFC 0071]), and on disk the
//! window ends with its **header**, whole blocks naming the addresses it
//! changed (`crate::disk_header`), reserved once its pages are placed —
//! when its size is known (RFC 0079 §4) — right after them. The window
//! reaches the disk as a single positioned write. Only afterwards are the descriptor vectors swapped — so
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
use crate::header::HeaderLink;
use crate::page_store::PageStore;
use crate::pages::PENDING;
use crate::plan::SlotPlan;
use crate::record_cache::Records;
use crate::roots::ROOTS_BLOCKS;
use crate::window::{
    Round, assemble, carve, dict_descriptors, install, size_of, targets_of,
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

    /// One round through the storage shard ([RFC 0073] §3): size the
    /// window's pages and dictionaries, **reserve** them, place them; on
    /// disk, size the header naming them and reserve it, with the roots
    /// block after it when the round ends a batch of a store writing its
    /// own files ([RFC 0079] §4); then build the header at its real
    /// address, write it all, swap the descriptors in and release the
    /// superseded runs. Whether the header ended the batch.
    ///
    /// The reservations are the waits; the caller holds the store's
    /// [`Gate`](crate::wait::Gate) across them.
    ///
    /// # Errors
    /// The window write's fault. Nothing is published then — but the
    /// header's bookkeeping has moved on and the file's state is unknown, so
    /// the store is failed: every later write is refused until a reopen
    /// reconstructs it from what is on disk.
    ///
    /// [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md
    /// [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md
    async fn place_in(
        &self,
        plans: Vec<SlotPlan>,
        placement: Placement,
        end: bool,
    ) -> StorageResult<bool> {
        // Object mode keeps no header: nothing reopens from RAM (RFC 0070
        // §9), so a window holds its images and nothing else.
        let logged = self.has_disk();
        let targets = match placement {
            // A settle's round packs its small pages (`crate::window_pack`).
            Placement::Settle => {
                crate::window_pack::packed(&plans, targets_of(&plans), |plan| {
                    self.types[plans[plan].idx].struct_hash()
                })
            }
            Placement::Move | Placement::Restore => targets_of(&plans),
        };
        if targets.is_empty() && !(logged && (end || self.has_unrecorded())) {
            return Ok(false);
        }
        let sizes: Vec<u64> =
            targets.iter().map(|t| size_of(&plans, t)).collect();
        let total: u64 = sizes.iter().sum();
        let body = match total {
            0 => None,
            blocks => Some(self.reserve(blocks).await?),
        };
        // A settle writes what the object store has not seen; a move does
        // not change what it moves.
        let flags = match placement {
            Placement::Settle if self.object.is_some() => Some(PENDING),
            Placement::Settle | Placement::Restore => Some(0),
            Placement::Move => None,
        };
        let mut round = Round {
            plans,
            runs: body.map_or_else(
                || vec![None; targets.len()],
                |w| carve(w, &sizes),
            ),
            targets,
            flags,
            end,
            body,
            marks: None,
            added: 0,
            dicts: BTreeMap::new(),
        };
        let marked = self.mark(&mut round, total).await;
        #[cfg(feature = "probe")]
        crate::probe::round(
            self.position,
            &self.types,
            &round.plans,
            (&round.targets, &sizes),
            round.marks.map_or((0, 0), |(run, size)| {
                (u64::from(size.blocks), run.count - u64::from(size.blocks))
            }),
        );
        let windows = (round.body, round.marks.map(|(run, _)| run));
        let built = marked.and_then(|()| self.build(round));
        if built.is_err() {
            for window in
                <[Option<Run>; 2]>::from(windows).into_iter().flatten()
            {
                self.abandon(window);
            }
        }
        built
    }

    /// Point the round's pages at the runs they were given and, on disk,
    /// size its header — what they leave unrecorded is known now — and
    /// reserve it.
    ///
    /// # Errors
    /// A page whose entry the plan lost; a storage thread gone.
    async fn mark(&self, round: &mut Round, body: u64) -> StorageResult<()> {
        round.added = install(
            &mut round.plans,
            &round.targets,
            &round.runs,
            round.flags,
        )?;
        round.dicts =
            dict_descriptors(&round.plans, &round.targets, &round.runs);
        if !self.has_disk() {
            return Ok(());
        }
        self.note_dicts(round.dicts.keys().map(|&p| round.plans[p].idx));
        let size = self.header_size(&round.plans, &round.dicts, body);
        // A storage thread ends its own writes with one (RFC 0073 §4).
        let roots = if round.end && self.own().is_some() {
            ROOTS_BLOCKS
        } else {
            0
        };
        let run = self.reserve(u64::from(size.blocks) + roots).await?;
        round.marks = Some((run, size));
        Ok(())
    }

    /// Build the round at its reserved windows, write it and publish it
    /// (see [`place_in`](Self::place_in)).
    fn build(&self, round: Round) -> StorageResult<bool> {
        let Round {
            mut plans,
            targets,
            runs,
            flags,
            end,
            body,
            marks,
            added,
            dicts,
        } = round;
        if let Some(object) = &self.object {
            let punched: u64 = plans.iter().map(|p| p.punched).sum();
            let unpended: u64 = plans.iter().map(|p| p.unpended).sum();
            object.adjust_pending(added + punched, unpended);
        }
        let mut ended = false;
        let marked = if let Some((run, size)) = marks {
            let (mut bytes, e) =
                self.header_at((run.start, size), &mut plans, &dicts, end)?;
            ended = e;
            let header = u64::from(size.blocks);
            if run.count > header {
                let link = HeaderLink::new(run.start, size.blocks);
                let roots = self.roots_at(run.start + header, link, ended)?;
                bytes.extend_from_slice(&roots);
            }
            Some((run, bytes))
        } else {
            None
        };
        let body = body.map(|w| (w, assemble(w, &plans, &targets, &runs)));
        if let Err(e) = self.write_windows(body, marked) {
            self.fail();
            return Err(e); // the round's one write
        }
        self.hold_written(&mut plans, &targets, &runs);
        for run in plans
            .iter_mut()
            .flat_map(|p| std::mem::take(&mut p.retired))
        {
            // An entry stopped naming it, so no read through that entry
            // will ask for it: its image goes now rather than when the
            // budget pushes it out — the run itself once no entry names it.
            self.release(run);
        }
        if flags.is_some() {
            // A page written for use — settled or brought back — is hot.
            for (target, run) in targets.iter().zip(&runs) {
                let Some(run) = run else { continue };
                for (plan, _) in target.pages() {
                    self.touch(plans[plan].idx, run.start);
                }
            }
        }
        self.publish(plans, &dicts);
        Ok(ended)
    }

    /// Adopt the working indexes and the new dictionary runs.
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
            *self.types[work.idx].directory().borrow_mut() = Some(work.index);
        }
    }
}
