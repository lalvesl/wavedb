//! What the disk's headers keep track of between windows ([RFC 0072] §3–§8):
//! the changes no header names yet, and where the chain of headers stands.
//! Split from [`crate::disk_header`], which writes the headers.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::collections::{BTreeMap, BTreeSet};

use crate::block::BlockDescriptor;

use crate::header::Rotation;
use crate::page_store::PageStore;
use crate::pages::Pages;
use crate::plan::SlotPlan;

/// Changes no header names yet, per slot index.
#[derive(Debug, Default)]
pub struct Unrecorded {
    pub(crate) slots: BTreeMap<usize, Touched>,
}

/// One slot's unrecorded changes besides its indexes', which each index
/// journals itself (RFC 0078 §6).
#[derive(Debug, Default)]
pub struct Touched {
    /// The dictionary moved.
    pub(crate) dict: bool,
    /// The object store's copy of the dictionary moved.
    pub(crate) remote_dict: bool,
}

impl Touched {
    pub(crate) const fn is_empty(&self) -> bool {
        !self.dict && !self.remote_dict
    }
}

/// Where the disk's chain of headers stands.
#[derive(Debug, Default)]
pub struct DiskChain {
    /// The newest header written.
    pub(crate) tail: Option<u64>,
    /// The header ending the batch before the newest one ended.
    pub(crate) last_end: Option<u64>,
    /// Per type, the disk indexes' rotation.
    pub(crate) types: BTreeMap<u64, Rotation>,
    /// Per type, the object store indexes' rotation.
    pub(crate) remote: BTreeMap<u64, Rotation>,
    /// The lowest header the open read: the horizon stays there until the
    /// rotations pass it.
    pub(crate) floor: u64,
    /// The headers a reconstruction may still read.
    pub(crate) live: BTreeSet<u64>,
    /// A full restatement is under way (`record_all`).
    pub(crate) restating: bool,
    /// The header it began in, once one is written.
    pub(crate) restate_from: Option<u64>,
    /// The header the last full restatement began in: every bucket is named
    /// at or after it.
    pub(crate) restated: Option<u64>,
    /// The roots blocks this store wrote (RFC 0073 §5).
    pub(crate) roots: RootsChain,
}

/// Where the roots blocks stand: the newest, and those a reconstruction
/// may still read.
#[derive(Debug, Default)]
pub struct RootsChain {
    /// The newest roots block written.
    pub(crate) newest: Option<u64>,
    /// The roots blocks not freed yet: the newest durable one and any
    /// written since.
    pub(crate) live: BTreeSet<u64>,
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
    /// Every type's dictionary as the window leaves it, as a header names
    /// it.
    pub(crate) fn dicts_after(
        &self,
        plans: &[SlotPlan],
        dicts: &BTreeMap<usize, BlockDescriptor>,
    ) -> Vec<Option<u64>> {
        self.types
            .iter()
            .enumerate()
            .map(|(idx, slot)| {
                let plan = plans.iter().position(|p| p.idx == idx);
                let desc = plan
                    .and_then(|p| dicts.get(&p).copied())
                    .unwrap_or_else(|| slot.dictionary().borrow().descriptor());
                desc.is_allocated().then_some(desc.raw())
            })
            .collect()
    }

    /// Whether anything is waiting for a header to name it.
    pub(crate) fn has_unrecorded(&self) -> bool {
        !self.unrecorded.borrow().slots.is_empty()
            || self.types.iter().any(|s| {
                s.directory()
                    .borrow()
                    .as_ref()
                    .is_some_and(Pages::has_unrecorded)
                    || !s.remote_journal().borrow().is_empty()
            })
    }

    /// Mark everything unrecorded: every range of every index whole, and
    /// every dictionary, so the batch that ends next names the whole
    /// addressing — what a conversion and an adoption need, their flags and
    /// evictions changing every page at once.
    pub(crate) fn record_all(&self) {
        if self.has_disk() {
            let mut chain = self.chain.borrow_mut();
            chain.restating = true;
            chain.restate_from = None;
        }
        let mut unrecorded = self.unrecorded.borrow_mut();
        for (idx, slot) in self.types.iter().enumerate() {
            if let Some(index) = slot.directory().borrow_mut().as_mut() {
                index.record_all();
            }
            if let Some(index) = slot.remote_directory().borrow().as_ref() {
                slot.remote_journal().borrow_mut().whole_all(index);
            }
            let touched = unrecorded.slots.entry(idx).or_default();
            touched.dict = true;
            touched.remote_dict = self.object.is_some();
        }
    }

    /// Note the dictionaries a window grows.
    pub(crate) fn note_dicts(&self, dicts: impl Iterator<Item = usize>) {
        let mut unrecorded = self.unrecorded.borrow_mut();
        for idx in dicts {
            unrecorded.slots.entry(idx).or_default().dict = true;
        }
    }

    /// Whether anything is still unrecorded once the window's plans are
    /// counted in.
    pub(crate) fn unrecorded_after(&self, plans: &[SlotPlan]) -> bool {
        let planned = |idx: usize| plans.iter().find(|p| p.idx == idx);
        !self.unrecorded.borrow().slots.is_empty()
            || self.types.iter().enumerate().any(|(idx, slot)| {
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
