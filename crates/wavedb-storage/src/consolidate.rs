//! The index budget and inline consolidation ([RFC 0081] §6).
//!
//! Fragments and spans are entries a round adds to the index rather than
//! rewriting a page whole (`crate::fragment`, `crate::partial`): RAM per
//! write. A compaction gives it back (`crate::regroup_merge`), but a store
//! written without pause may not compact for hours. The budget
//! ([`StoreOptions::index_budget_bytes`](crate::StoreOptions)) bounds
//! what they add: past it, the rounds consolidate inline.
//!
//! - **What counts.** The index's RAM past a clean index of the same data:
//!   the entries a merge would give back — runs of small neighbouring
//!   pages, and the pages a span left named either side, with the spans
//!   over them (`small_runs`) — at [`entry_bytes`] each; and the ranges a
//!   regroup would fold (`PageIndex::scattered_runs`) — a span over a
//!   page its range's window no longer reaches is a range of its own, and
//!   cuts the range it came from in two (RFC 0081 §3).
//! - **When it is looked at.** Finding them walks every entry, so it is
//!   not done every batch: a walk notes the clean part of the index (what
//!   it held less what it found), and the next is due only once the index
//!   passes that by the budget. Between two walks the index grows by the
//!   budget at most; past one, it is consolidated back.
//! - **What a batch pays.** Its first round consolidates the runs that give
//!   back the most entries — a merge round, as a compaction's (`ROUND_RUNS`
//!   of them at most) — before it settles its records, which land on the
//!   merged pages. One round a batch: the write amplification of today's
//!   whole-page rewrites, paid while over budget, and no more.
//! - **What a batch does not pay.** The ranges cost a move of every page
//!   of the run, side by side — a write pays no more than its span for
//!   them. The store asks for the cleaner instead
//!   ([`PageStore::wants_regroup`]): its regroup moves them, on the node's
//!   idle time.
//!
//! A merge round is safe across a crash as any round of its batch is: its
//! pages are written before a header names them, and the runs it retires
//! are freed once the batch is durable.
//!
//! [`entry_bytes`]: crate::ordered::PageIndex::entry_bytes
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

use crate::error::StorageResult;
use crate::merge_runs::small_runs;
use crate::page_store::PageStore;
use crate::pages::Pages;
use crate::plan::SlotPlan;

/// Runs one consolidation round takes at most — as a compaction's merge.
const ROUND_RUNS: usize = 64;

/// A store's index budget, and the consolidation it has found to do.
#[derive(Debug, Default)]
pub struct Budget {
    /// Bytes the index may hold past its clean part.
    bytes: u64,
    /// The clean part, as the last walk found it.
    clean: Cell<u64>,
    /// Runs to merge, the most entries given back first: a type's slot and
    /// a key finding each of its entries.
    queue: RefCell<VecDeque<(usize, Vec<Vec<u8>>)>>,
    /// The open batch's first round consolidates.
    due: Cell<bool>,
    /// A walk over budget found ranges for a regroup to fold.
    regroup: Cell<bool>,
}

impl Budget {
    pub fn new(bytes: usize) -> Self {
        Self {
            bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
            ..Self::default()
        }
    }

    /// A compaction planned its regroup: what was asked for is under way.
    pub fn regrouped(&self) {
        self.regroup.set(false);
    }
}

impl PageStore {
    /// The RAM every registered type's page index holds, estimated
    /// (`PageIndex::ram_bytes`): what [`StoreOptions::index_budget_bytes`]
    /// bounds past a clean index.
    ///
    /// [`StoreOptions::index_budget_bytes`]: crate::StoreOptions::index_budget_bytes
    #[must_use]
    pub fn index_bytes(&self) -> u64 {
        self.types
            .iter()
            .filter_map(|slot| {
                slot.directory().borrow().as_ref().map(Pages::ram_bytes)
            })
            .sum()
    }

    /// Whether a walk past the budget found ranges a regroup would fold:
    /// the cleaner's work, not a write's — a node compacts on it.
    #[must_use]
    pub const fn wants_regroup(&self) -> bool {
        self.budget.regroup.get()
    }

    /// At a batch's opening: whether its first round consolidates — the
    /// index past its clean part by more than the budget — walking the
    /// indexes for what to merge when nothing is queued.
    pub(crate) fn plan_consolidation(&self) {
        let budget = &self.budget;
        let held = self.index_bytes();
        // What a regroup or a removal gave back is no longer counted clean.
        budget.clean.set(budget.clean.get().min(held));
        let over = held > budget.clean.get().saturating_add(budget.bytes);
        if !over {
            budget.queue.borrow_mut().clear();
            budget.due.set(false);
            return;
        }
        if budget.queue.borrow().is_empty() {
            let (merges, folds) = self.walk_for_merges();
            // The folds wait for the cleaner: counted clean until it runs,
            // or every batch would walk again.
            budget.clean.set(held - merges.min(held));
            budget.regroup.set(budget.regroup.get() || folds > 0);
        }
        budget.due.set(!budget.queue.borrow().is_empty());
    }

    /// Queue every type's runs to merge, the most entries given back first;
    /// the bytes they would give back, and those a regroup would.
    fn walk_for_merges(&self) -> (u64, u64) {
        let (target, cold) = (self.target_blocks_per_bucket, self.cold_line());
        let (mut found, mut folds) = (Vec::new(), 0);
        for (slot, ts) in self.types.iter().enumerate() {
            let index = ts.directory().borrow();
            let Some(index) = index.as_ref() else {
                continue;
            };
            let entry = index.entry_bytes();
            folds += index
                .scattered_runs(usize::MAX)
                .iter()
                .map(|run| run.ranges.saturating_sub(1) as u64)
                .sum::<u64>()
                * index.range_bytes();
            for run in small_runs(index, target, cold) {
                let after = run.blocks.div_ceil(target.max(1)).max(1);
                let back = (run.keys.len() as u64).saturating_sub(after);
                if back > 0 {
                    found.push((back * entry, slot, run.keys));
                }
            }
        }
        found.sort_by_key(|f| std::cmp::Reverse(f.0));
        let excess = found.iter().map(|f| f.0).sum();
        let mut queue = self.budget.queue.borrow_mut();
        queue.clear();
        queue.extend(found.into_iter().map(|(_, slot, keys)| (slot, keys)));
        (excess, folds)
    }

    /// The open batch's consolidation round, once: the slot of the run
    /// that gives back most, and up to [`ROUND_RUNS`] of its queued runs,
    /// merged. `None` when none is due, or every run queued has changed
    /// since (`plan_merge` checks each again).
    ///
    /// # Errors
    /// A page that fails to read.
    pub(crate) fn consolidation_round(
        &self,
    ) -> StorageResult<Option<SlotPlan>> {
        if !self.budget.due.replace(false) {
            return Ok(None);
        }
        loop {
            let batch = {
                let mut queue = self.budget.queue.borrow_mut();
                let Some(&(slot, _)) = queue.front() else {
                    return Ok(None);
                };
                let mut runs = Vec::new();
                let mut rest = VecDeque::with_capacity(queue.len());
                for (s, keys) in queue.drain(..) {
                    if s == slot && runs.len() < ROUND_RUNS {
                        runs.push(keys);
                    } else {
                        rest.push_back((s, keys));
                    }
                }
                *queue = rest;
                (slot, runs)
            };
            // Inline, in a batch a write may wait on: the write path's
            // compression.
            if let Some(plan) = self.plan_merge(batch.0, &batch.1, false)? {
                return Ok(Some(plan));
            }
        }
    }
}
