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

/// Blocks a serialised image occupies (an empty image occupies none),
/// rounded up to a count its descriptor can carry.
pub const fn blocks_of(bytes: &[u8]) -> u64 {
    let blocks = bytes.len().div_ceil(BLOCK_SIZE) as u64;
    match round_count(blocks) {
        Some(rounded) => rounded,
        // Past the largest page: refused by `image_of` before it is placed.
        None => MAX_COUNT,
    }
}

impl PageStore {
    /// Plan slot `idx`'s pages for a round's `records` — read each bucket
    /// once, apply every record that routes to it (`None` bytes remove it),
    /// decide splits, serialise.
    ///
    /// The directory is cloned rather than held: the settle path is the only
    /// writer of a directory and runs one round at a time (maintenance loop /
    /// open replay), so the working copy cannot go stale under the plan, and
    /// readers keep resolving the old — still valid, still allocated — pages
    /// while the window is written.
    ///
    /// # Errors
    /// Propagates page read / corruption / compression faults.
    // The dictionary guard spans the whole plan by design: every page this
    // slot serialises must bind the same (possibly grown) buffer state, so
    // dropping it early — the lint's suggestion — would let two pages of one
    // round stamp different `dict_len`s.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn plan_slot(
        &self,
        idx: usize,
        records: Records,
    ) -> StorageResult<SlotPlan> {
        let slot = &self.types[idx];
        let mut dir = slot
            .directory()
            .borrow_mut()
            .clone()
            .unwrap_or_else(|| Directory::new(self.seed));
        let mut dict = slot.dictionary().borrow_mut();

        let mut staged: BTreeMap<usize, SlotPage> = BTreeMap::new();
        let mut dict_grew = false;

        for (id, bytes) in records {
            let bucket = dir.bucket_of(id.raw());
            let page =
                staged_page(self, slot, &dir, &dict, &mut staged, bucket)?;
            if let Some(bytes) = bytes {
                dict_grew |= dict.sample(&bytes);
                page.upsert(id, bytes);
            } else {
                page.remove(id);
            }
        }

        let mut pages = BTreeMap::new();
        for (bucket, page) in &mut staged {
            pages.insert(*bucket, image_of(page, &dict)?);
        }
        split_staged(
            &mut dir,
            &mut staged,
            &mut pages,
            self.target_blocks_per_bucket,
            |page| image_of(page, &dict),
        )?;

        Ok(SlotPlan {
            idx,
            dir,
            pages,
            dict: dict_grew.then(|| dict.image()),
            decoded: staged,
        })
    }
}

/// `page` serialised — refused, typed, past the largest page a descriptor
/// can size ([`MAX_PAGE_BYTES`]), never truncated.
pub fn image_of(
    page: &mut SlotPage,
    dict: &DictState,
) -> StorageResult<Vec<u8>> {
    let bytes = page.to_bytes(dict.dictionary(), dict.enabled())?;
    if bytes.len() as u64 > MAX_PAGE_BYTES {
        return Err(StorageError::PageTooLarge(bytes.len() as u64));
    }
    Ok(bytes)
}

/// The staged page for `bucket`, read from its settled run on first touch.
fn staged_page<'a>(
    store: &PageStore,
    slot: &TypeSlot,
    dir: &Directory,
    dict: &DictState,
    staged: &'a mut BTreeMap<usize, SlotPage>,
    bucket: usize,
) -> StorageResult<&'a mut SlotPage> {
    match staged.entry(bucket) {
        Entry::Occupied(e) => Ok(e.into_mut()),
        Entry::Vacant(v) => {
            // Every write restores its bucket before it commits (RFC 0070
            // §7), so an evicted one here would lose its records.
            if dir.descriptor(bucket).is_evicted() {
                return Err(StorageError::Corrupt(
                    "a settle reached an evicted bucket",
                ));
            }
            // Out of the page cache: this round places the page in a new
            // run, and holds it there once written (`crate::checkpoint`).
            let page =
                dir.take_page(slot.struct_hash(), &store.file, bucket, dict)?;
            Ok(v.insert(page))
        }
    }
}
