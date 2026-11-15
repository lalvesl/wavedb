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
use crate::error::StorageResult;
use crate::page::{Compression, SlotPage};
use crate::page_store::PageStore;
use crate::pages::{Pages, read_page, run_of};
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
    /// Runs the plan stops naming, freed once the window is written.
    pub(crate) retired: Vec<Run>,
    /// Runs it names once more: a page rewritten in part (`crate::partial`).
    pub(crate) named: Vec<Run>,
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
            retired: Vec::new(),
            named: Vec::new(),
            unpended: 0,
            punched: 0,
        }
    }
}

impl PageStore {
    /// Plan slot `idx`'s pages for a round's `records` (`None` bytes
    /// remove): route, rebuild, cut and serialise (`crate::shape`).
    ///
    /// The index is cloned rather than held: the settle path is the only
    /// writer of an index and runs one round at a time, so the copy cannot
    /// go stale under the plan, and readers keep resolving the old — still
    /// valid, still allocated — pages while the window is written.
    ///
    /// # Errors
    /// Page read, corruption or compression faults.
    pub(crate) fn plan_slot(
        &self,
        idx: usize,
        mut records: Records,
    ) -> StorageResult<SlotPlan> {
        let slot = &self.types[idx];
        let mut index = slot
            .directory()
            .borrow()
            .clone()
            .unwrap_or_else(|| slot.new_pages());
        records.sort_by(|a, b| a.0.cmp(&b.0));
        let rules = Rules {
            target_blocks: self.target_blocks_per_bucket,
            keep_holes: self.object.is_some(),
            partial: true,
            next: self.next_address(),
        };
        let compression = slot.compression();
        let shaped = shape(
            &mut index,
            records,
            &rules,
            slot.struct_hash(),
            // Read, not taken out of the page cache: a round that rewrites a
            // page in part (`crate::partial`) leaves it, and it is read next.
            |page| read_page(&self.file, run_of(page), slot.struct_hash()),
            |page| image_of(page, compression),
        )?;
        Ok(SlotPlan {
            idx,
            index,
            pages: shaped.pages,
            retired: shaped.retired,
            named: shaped.named,
            unpended: shaped.unpended,
            punched: shaped.punched,
        })
    }
}

/// `page` serialised as `compression` says (zstd, the page alone: RFC 0081
/// §9).
///
/// # Errors
/// A compression fault, or a page whose records pass what its body can
/// index.
pub fn image_of(
    page: &mut SlotPage,
    compression: Compression,
) -> StorageResult<Vec<u8>> {
    page.encode(compression)
}
