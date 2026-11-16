//! What the disk's headers keep track of between windows ([RFC 0072] §3–§8):
//! the changes no header names yet, and where the chain of headers stands.
//! Split from [`crate::disk_header`], which writes the headers.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::collections::BTreeMap;
use std::time::Instant;

use crate::block::Run;
use crate::header::{HeaderLink, Rotation};
use crate::page_store::PageStore;
use crate::pages::{Pages, disk_runs};
use crate::plan::SlotPlan;
use crate::shares::Shares;

/// Where the disk's chain of headers stands.
#[derive(Debug, Default)]
pub struct DiskChain {
    /// The newest header written.
    pub(crate) tail: Option<HeaderLink>,
    /// The header ending the batch before the newest one ended.
    pub(crate) last_end: Option<HeaderLink>,
    /// Per type, the disk indexes' rotation.
    pub(crate) types: BTreeMap<u64, Rotation>,
    /// Per type, the object store indexes' rotation.
    pub(crate) remote: BTreeMap<u64, Rotation>,
    /// The lowest header the open read: the horizon stays there until the
    /// rotations pass it.
    pub(crate) floor: u64,
    /// The headers a reconstruction may still read, each with its blocks.
    pub(crate) live: BTreeMap<u64, u16>,
    /// A full restatement is under way (`record_all`).
    pub(crate) restating: bool,
    /// The header it began in, once one is written.
    pub(crate) restate_from: Option<u64>,
    /// The header the last full restatement began in: every bucket is named
    /// at or after it.
    pub(crate) restated: Option<u64>,
    /// The roots blocks this store wrote (RFC 0073 §5).
    pub(crate) roots: RootsChain,
    /// The newest header's horizon: the walk a reconstruction rooted at
    /// the next one makes is the live headers at or above it
    /// (`crate::header_room`).
    pub(crate) horizon: u64,
    /// Headers written since the last restating header.
    pub(crate) since_restate: usize,
    /// When the last restating header was written — or the first header
    /// since the store opened.
    pub(crate) restated_at: Option<Instant>,
}

/// Where a storage thread's roots blocks stand. A store writing its own
/// files writes none (RFC 0081 §1).
#[derive(Debug, Default)]
pub struct RootsChain {
    /// The newest roots block its storage thread wrote, as its last
    /// reservation told it: every header names it (RFC 0073 §5).
    pub(crate) newest: Option<u64>,
}

impl DiskChain {
    /// Whether headers below the last full restatement are still live —
    /// one more batch has to end for them to go.
    pub(crate) fn outlived(&self) -> bool {
        self.restated
            .is_some_and(|from| self.live.range(..from).next().is_some())
    }
}

impl PageStore {
    /// Take up the chain an open rebuilt, and count the runs named more
    /// than once again: a live header sharing its block with a page is one
    /// more name of that page's run (RFC 0081 §1).
    pub(crate) fn reopened(&self, chain: DiskChain) {
        let headers: Vec<Run> = chain
            .live
            .iter()
            .map(|(&at, &blocks)| HeaderLink::new(at, blocks).run())
            .collect();
        *self.chain.borrow_mut() = chain;
        let entries: Vec<Run> = self
            .types
            .iter()
            .flat_map(|slot| {
                slot.directory()
                    .borrow()
                    .iter()
                    .flat_map(disk_runs)
                    .collect::<Vec<_>>()
            })
            .collect();
        *self.shares.borrow_mut() =
            Shares::of(entries.into_iter().chain(headers));
    }

    /// Whether anything is waiting for a header to name it.
    pub(crate) fn has_unrecorded(&self) -> bool {
        self.types.iter().any(|s| {
            s.directory()
                .borrow()
                .as_ref()
                .is_some_and(Pages::has_unrecorded)
                || !s.remote_journal().borrow().is_empty()
        })
    }

    /// Mark everything unrecorded: every range of every index whole, so the
    /// batch that ends next names the whole addressing — what a conversion
    /// and an adoption need, their flags and evictions changing every page
    /// at once.
    pub(crate) fn record_all(&self) {
        if self.has_disk() {
            let mut chain = self.chain.borrow_mut();
            chain.restating = true;
            chain.restate_from = None;
        }
        for slot in &self.types {
            if let Some(index) = slot.directory().borrow_mut().as_mut() {
                index.record_all();
            }
            if let Some(index) = slot.remote_directory().borrow().as_ref() {
                slot.remote_journal().borrow_mut().whole_all(index);
            }
        }
    }

    /// Whether anything is still unrecorded once the window's plans are
    /// counted in.
    pub(crate) fn unrecorded_after(&self, plans: &[SlotPlan]) -> bool {
        let planned = |idx: usize| plans.iter().find(|p| p.idx == idx);
        self.types.iter().enumerate().any(|(idx, slot)| {
            let disk = planned(idx).map_or_else(
                || {
                    slot.directory()
                        .borrow()
                        .as_ref()
                        .is_some_and(Pages::has_unrecorded)
                },
                |p| p.index.has_unrecorded(),
            );
            disk || !slot.remote_journal().borrow().is_empty()
        })
    }
}
