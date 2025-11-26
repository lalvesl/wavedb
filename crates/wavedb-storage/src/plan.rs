//! Phase 1 of a settle round ([RFC 0041], [RFC 0078] §5) — a round's
//! records into page **images**, writing nothing.
//!
//! The records are routed to the pages their keys fall in, each page read
//! once, receiving every record that routes to it, and serialised once
//! (`crate::shape`); the images go to [`crate::checkpoint`], which places
//! all of them in one contiguous window and then points their entries at
//! the runs they got. Nothing is written until the whole shape is decided,
//! so no page is ever written twice in one round.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use crate::block::Run;
use crate::dictionary::DictState;
use crate::error::StorageResult;
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::pages::{Pages, disk_pages, run_of, take_page};
use crate::record_cache::Records;
pub use crate::shape::{PlannedPage, blocks_for};
use crate::shape::{Rules, shape};

/// One slot's planned contribution to a round.
pub struct SlotPlan {
    /// Index into [`PageStore::types`].
    pub(crate) idx: usize,
    /// The index after the plan: its new pages' entries at stand-ins until
    /// the window is placed.
    pub(crate) index: Pages,
    /// The pages to write.
    pub(crate) pages: Vec<PlannedPage>,
    /// The grown dictionary buffer, when sampling extended it this round.
    pub(crate) dict: Option<Vec<u8>>,
    /// Runs the plan stops naming, freed once the window is written.
    pub(crate) retired: Vec<Run>,
    /// Pending blocks the retired pages took with them.
    pub(crate) unpended: u64,
    /// Holes left pending — removals the object store must learn.
    pub(crate) punched: u64,
}

impl SlotPlan {
    /// A plan carrying pages that are already serialised — a move, which
    /// carries stored bytes verbatim, or a page brought back.
    pub(crate) const fn verbatim(idx: usize, index: Pages) -> Self {
        Self {
            idx,
            index,
            pages: Vec::new(),
            dict: None,
            retired: Vec::new(),
            unpended: 0,
            punched: 0,
        }
    }
}

impl PageStore {
    /// Plan slot `idx`'s pages for a round's `records` (`None` bytes
    /// remove): sample the dictionary, then route, rebuild, cut and
    /// serialise (`crate::shape`).
    ///
    /// The index is cloned rather than held: the settle path is the only
    /// writer of an index and runs one round at a time, so the copy cannot
    /// go stale under the plan, and readers keep resolving the old — still
    /// valid, still allocated — pages while the window is written.
    ///
    /// # Errors
    /// Page read, corruption or compression faults.
    // The dictionary guard spans the whole plan by design: every page this
    // slot serialises must bind the same (possibly grown) buffer state.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn plan_slot(
        &self,
        idx: usize,
        mut records: Records,
    ) -> StorageResult<SlotPlan> {
        let slot = &self.types[idx];
        let mut index =
            slot.directory().borrow().clone().unwrap_or_else(disk_pages);
        records.sort_by_key(|(id, _)| id.raw());
        let mut dict = slot.dictionary().borrow_mut();
        let mut grew = false;
        for bytes in records.iter().filter_map(|(_, b)| b.as_ref()) {
            grew |= dict.sample(bytes);
        }
        let dict = &*dict;
        let rules = Rules {
            target_blocks: self.target_blocks_per_bucket,
            keep_holes: self.object.is_some(),
        };
        let shaped = shape(
            &mut index,
            records,
            &rules,
            slot.struct_hash(),
            |page| take_page(&self.file, run_of(page), dict),
            |page| image_of(page, dict),
        )?;
        Ok(SlotPlan {
            idx,
            index,
            pages: shaped.pages,
            dict: grew.then(|| dict.image()),
            retired: shaped.retired,
            unpended: shaped.unpended,
            punched: shaped.punched,
        })
    }
}

/// `page` serialised against `dict`.
///
/// # Errors
/// A compression fault, or a page whose records pass what its body can
/// index.
pub fn image_of(
    page: &mut SlotPage,
    dict: &DictState,
) -> StorageResult<Vec<u8>> {
    page.to_bytes(dict.dictionary(), dict.enabled())
}
