//! Phase 1 of a settle round ([RFC 0041]) — turning a round of touched ids into
//! page **images**, grouped per bucket, writing nothing.
//!
//! The pre-0041 settle walked a round **per `Id`**, and every id did a whole
//! read-modify-write of its bucket page: a hundred ids landing in one bucket
//! cost a hundred read/alloc/write/free cycles of the same page. Here the ids
//! are grouped by bucket first, so a page is read once, receives every id that
//! routes to it, and is serialised once. The images go to
//! [`crate::checkpoint`], which places all of them in one contiguous window.
//!
//! Splits are planned here too, **after** the ids are applied: a split only
//! ever moves keys out of the page being split, so a record placed against
//! the pre-split directory keeps its address (or is repartitioned by the
//! split itself, [RFC 0074]). Nothing is written until the whole shape is decided, so
//! no page is ever written twice in one round.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use crate::block::{BLOCK_SIZE, MAX_COUNT, MAX_PAGE_BYTES, round_count};
use crate::dictionary::DictState;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::record_cache::Records;
use crate::split::split_staged;
use crate::struct_storage::TypeSlot;

/// One slot's planned contribution to a settle round.
pub struct SlotPlan {
    /// Index into [`PageStore::types`].
    pub(crate) idx: usize,
    /// The directory after the plan: final bucket count, with every rewritten
    /// bucket still holding its **old** descriptor (freed at install time).
    pub(crate) dir: Directory,
    /// Bucket → serialised page. An empty image means the bucket becomes
    /// [`BlockDescriptor::EMPTY`] and consumes no window space.
    pub(crate) pages: BTreeMap<usize, Vec<u8>>,
    /// The grown dictionary buffer, when sampling extended it this round.
    pub(crate) dict: Option<Vec<u8>>,
    /// Bucket → the page its image encodes, where the plan has it decoded:
    /// held in the page cache under its new run once the window is written.
    pub(crate) decoded: BTreeMap<usize, SlotPage>,
}

impl SlotPlan {
    /// A plan carrying pages that are already serialised — the
    /// defragmenter, which moves stored bytes verbatim.
    pub(crate) const fn verbatim(idx: usize, dir: Directory) -> Self {
        Self {
            idx,
            dir,
            pages: BTreeMap::new(),
            dict: None,
            decoded: BTreeMap::new(),
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
