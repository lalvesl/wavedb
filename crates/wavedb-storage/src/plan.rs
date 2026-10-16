//! Phase 1 of a checkpoint ([RFC 0041]) — turning a round of touched ids into
//! page **images**, grouped per bucket, writing nothing.
//!
//! The pre-0041 settle walked a round **per `Id`**, and every id did a whole
//! read-modify-write of its bucket page: a hundred ids landing in one bucket
//! cost a hundred read/alloc/write/free cycles of the same page. Here the ids
//! are grouped by bucket first, so a page is read once, receives every id that
//! routes to it, and is serialised once. The images go to
//! [`crate::checkpoint`], which places all of them in one contiguous window.
//!
//! Splits are planned here too, **after** the ids are applied: linear hashing
//! only ever moves keys out of the bucket being split, so a record placed
//! against the pre-split directory keeps its address (or is repartitioned by
//! the split itself). Nothing is written until the whole shape is decided, so
//! no page is ever written twice in one checkpoint.
//!
//! [RFC 0041]: ../../../rfcs/0041-single-barrier-checkpoint.md

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

use wavedb_core::Id;

use crate::block::{BLOCK_SIZE, BlockDescriptor};
use crate::dictionary::DictState;
use crate::directory::Directory;
use crate::error::StorageResult;
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::struct_storage::TypeSlot;

/// Splits one round may perform — a pacing budget on how fast the directory
/// grows in one checkpoint. Page size is a target, not an invariant: a bucket
/// still over target is split when its turn next comes round, or never, if
/// splitting cannot relieve it ([RFC 0049]).
///
/// [RFC 0049]: ../../../rfcs/0049-elastic-pages-and-load-driven-splits.md
const MAX_SPLITS_PER_ROUND: usize = 64;

/// One slot's planned contribution to a checkpoint.
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
    /// Ids whose page write clears an unsettled-remove tombstone.
    pub(crate) cleared: Vec<Id>,
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
            cleared: Vec::new(),
        }
    }
}

/// Blocks a serialised image occupies (an empty image occupies none).
pub const fn blocks_of(bytes: &[u8]) -> u64 {
    bytes.len().div_ceil(BLOCK_SIZE) as u64
}

/// Blocks `bucket` will occupy once this round lands: its planned image when
/// the round rewrites it, its settled span otherwise. The settled case reads
/// the descriptor, so deciding against a split costs no IO.
fn planned_blocks(
    dir: &Directory,
    pages: &BTreeMap<usize, Vec<u8>>,
    bucket: usize,
) -> u64 {
    pages
        .get(&bucket)
        .map_or_else(|| dir.descriptor(bucket).count(), |b| blocks_of(b))
}

impl PageStore {
    /// Plan slot `idx`'s pages for the touched `ids` — read each bucket once,
    /// apply every id that routes to it, decide splits, serialise.
    ///
    /// The directory is cloned rather than held: the settle path is the only
    /// writer of a directory and runs serially (maintenance loop / open
    /// replay), so the working copy cannot go stale under the plan, and
    /// readers keep resolving the old — still valid, still allocated — pages
    /// while the window is written.
    ///
    /// # Errors
    /// Propagates page read / corruption / compression faults.
    // The dictionary guard spans the whole plan by design: every page this
    // slot serialises must bind the same (possibly grown) buffer state, so
    // dropping it early — the lint's suggestion — would let two pages of one
    // checkpoint stamp different `dict_len`s.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn plan_slot(
        &self,
        idx: usize,
        ids: &[Id],
    ) -> StorageResult<SlotPlan> {
        let slot = &self.types[idx];
        let mut dir = slot
            .directory()
            .lock()
            .clone()
            .unwrap_or_else(|| Directory::new(self.seed));
        let mut dict = slot.dictionary().lock();

        let mut staged: BTreeMap<usize, SlotPage> = BTreeMap::new();
        let mut cleared = Vec::new();
        let mut dict_grew = false;

        for &id in ids {
            let bucket = dir.bucket_of(id.raw());
            let page =
                staged_page(self, slot, &dir, &dict, &mut staged, bucket)?;
            if let Some(bytes) = slot.get(id) {
                dict_grew |= dict.sample(&bytes);
                page.upsert(id, bytes);
            } else {
                page.remove(id);
                cleared.push(id);
            }
        }

        let mut pages = BTreeMap::new();
        for (bucket, page) in &staged {
            pages.insert(
                *bucket,
                page.to_bytes(dict.dictionary(), dict.enabled())?,
            );
        }
        self.plan_splits(slot, &mut dir, &dict, &mut staged, &mut pages)?;

        Ok(SlotPlan {
            idx,
            dir,
            pages,
            dict: dict_grew.then(|| dict.image()),
            cleared,
        })
    }

    /// Split while the bucket **whose turn it is** is over target, growing the
    /// working directory, up to the round's pacing budget ([RFC 0049]).
    ///
    /// Linear hashing's split order is derived from the directory's length
    /// (`Directory::next_split_bucket`), because addressing depends on that
    /// length and nothing else — so a split can never be aimed at whichever
    /// bucket happens to have overflowed. The trigger therefore asks about the
    /// bucket that is going to be split *anyway*: a split then never happens
    /// except where it relieves something, and the loop terminates because
    /// splitting the source is what makes it stop qualifying.
    ///
    /// A bucket over target whose turn has not come simply spans more blocks
    /// until it does — including one holding a record larger than the target,
    /// which splitting could never relieve at all, since splits distribute
    /// whole records.
    ///
    /// [RFC 0049]: ../../../rfcs/0049-elastic-pages-and-load-driven-splits.md
    fn plan_splits(
        &self,
        slot: &TypeSlot,
        dir: &mut Directory,
        dict: &DictState,
        staged: &mut BTreeMap<usize, SlotPage>,
        pages: &mut BTreeMap<usize, Vec<u8>>,
    ) -> StorageResult<()> {
        for _ in 0..MAX_SPLITS_PER_ROUND {
            let level = dir.split_bit();
            let source = dir.next_split_bucket() as usize;
            if planned_blocks(dir, pages, source)
                <= self.target_blocks_per_bucket
            {
                return Ok(());
            }
            let page = match staged_page(self, slot, dir, dict, staged, source)
            {
                Ok(page) => {
                    std::mem::replace(page, SlotPage::new(slot.struct_hash()))
                }
                Err(e) => return Err(e),
            };

            let mut keep = SlotPage::new(slot.struct_hash());
            let mut moved = SlotPage::new(slot.struct_hash());
            for (id, bytes) in page.into_entries() {
                // Bit `level` decides: 0 stays put, 1 moves to the new bucket.
                if (dir.hash(id.raw()) >> level) & 1 == 0 {
                    keep.upsert(id, bytes);
                } else {
                    moved.upsert(id, bytes);
                }
            }
            let target = dir.push_bucket(BlockDescriptor::EMPTY);
            pages.insert(
                source,
                keep.to_bytes(dict.dictionary(), dict.enabled())?,
            );
            pages.insert(
                target,
                moved.to_bytes(dict.dictionary(), dict.enabled())?,
            );
            staged.insert(source, keep);
            staged.insert(target, moved);
        }
        Ok(())
    }
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
            let page =
                dir.read_page(slot.struct_hash(), &store.file, bucket, dict)?;
            Ok(v.insert(page))
        }
    }
}
