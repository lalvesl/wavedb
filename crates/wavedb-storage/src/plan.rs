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
