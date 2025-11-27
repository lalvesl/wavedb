//! An upload's middle ([RFC 0070] §5, [RFC 0078]): which remote pages meet
//! the flagged disk pages, and those pages rebuilt — every record within a
//! flagged page's coverage replaced by the flagged page's own — then cut
//! where they run over target.
//!
//! The disk and the object store each have an ordered index of their own,
//! over the same keys. A flagged disk entry is the whole truth about the
//! keys it covers, `[lo, hi)`: the remote pages meeting that span are
//! exactly the ones that can hold a record of it, so a record the disk no
//! longer has is dropped from them, and every one it has goes in.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::{BTreeMap, BTreeSet};

use wavedb_core::Id;

use crate::block::Run;
use crate::error::{StorageError, StorageResult};
use crate::ordered::{Journal, Placed};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use futures::stream::{self, StreamExt, TryStreamExt};

use crate::pages::{
    PENDING, Pages, coverage, entries_between, key_of_id, read_page,
    remote_pages, run_of, within,
};
use crate::plan::{PlannedPage, image_of};
use crate::shape::weight;
use crate::shape::{Rules, Shaped, shape};
use crate::upload::{IN_FLIGHT, UPLOAD_INPUT_BLOCKS};

/// Blocks a remote page should hold (128 KiB): a target, as on disk — a
/// page over it is cut in the upload that rewrites it.
pub const REMOTE_TARGET_BLOCKS: u64 = 32;

/// One flagged disk entry an upload carries.
pub struct Flagged {
    /// The keys it covers, `[lo, hi)`; `lo` finds it again.
    pub(crate) lo: Vec<u8>,
    pub(crate) hi: Option<Vec<u8>>,
    /// The entry as taken: its page (`None` for a hole) and its flags — its
    /// flag is cleared only if it is still so once the files are up.
    pub(crate) taken: Option<Placed<u8>>,
    pub(crate) flags: u8,
    /// Its records; none for a hole.
    pub(crate) page: SlotPage,
}

/// One type's share of an upload, from the snapshot to the encoded pages.
pub struct SlotUpload {
    /// Index into [`PageStore::types`].
    pub(crate) idx: usize,
    pub(crate) flagged: Vec<Flagged>,
    /// The remote index being rebuilt — the upload's own copy.
    pub(crate) remote: Pages,
    /// Remote pages fetched, by their run.
    pub(crate) fetched: BTreeMap<Run, SlotPage>,
    /// Remote pages carried forward as they are, out of files a compaction
    /// empties (`crate::remote_compact`): a key finding each, and its run.
    pub(crate) moved: Vec<(Vec<u8>, Run)>,
    /// Whether the dictionary is carried forward too.
    pub(crate) move_dict: bool,
    /// The pages it writes, once rebuilt.
    pub(crate) shaped: Shaped,
    /// What of the remote index it changed, for the disk's headers.
    pub(crate) changes: Journal,
}

impl SlotUpload {
    /// The remote runs this upload reads: those meeting a flagged coverage,
    /// and those carried forward.
    pub(crate) fn affected(&self) -> BTreeSet<Run> {
        let meeting = self.flagged.iter().flat_map(|f| {
            entries_between(&self.remote, &f.lo, f.hi.as_deref())
                .into_iter()
                .filter_map(|(_, page)| page.map(run_of))
        });
        meeting
            .chain(self.moved.iter().map(|&(_, run)| run))
            .collect()
    }

    /// The records the remote pages take: every one a flagged page holds,
    /// and the removal of every one the fetched pages hold within a flagged
    /// coverage that the page does not — in key order.
    fn records(&self) -> Vec<(Id, Option<Vec<u8>>)> {
        let mut out: BTreeMap<u128, Option<Vec<u8>>> = BTreeMap::new();
        for page in self.fetched.values() {
            for (id, _) in page.iter() {
                let key = key_of_id(id);
                let covered = self
                    .flagged
                    .iter()
                    .any(|f| within(&key, &f.lo, f.hi.as_deref()));
                if covered {
                    out.insert(id.raw(), None);
                }
            }
        }
        for f in &self.flagged {
            for (id, bytes) in f.page.iter() {
                out.insert(id.raw(), Some(bytes.to_vec()));
            }
        }
        out.into_iter()
            .map(|(raw, bytes)| (Id::from_raw(raw), bytes))
            .collect()
    }
}

impl PageStore {
    /// Step 1: every type's flagged disk entries, up to the input bound, and
    /// a copy of each type's remote index — and whether that took every
    /// flagged entry, the bound leaving none behind (a cut). The remote
    /// pages a compaction carries forward come along, up to their own bound.
    pub(crate) fn take_flagged(
        &self,
        idx: usize,
        page: &mut SlotPage,
    ) -> StorageResult<Vec<u8>> {
        if page.is_empty() {
            return Ok(Vec::new());
        }
        image_of(page, &self.types[idx].dictionary().borrow())
    }

    /// Decode an uploaded image of slot `idx`.
    pub(crate) fn decode_remote(
        &self,
        idx: usize,
        image: &[u8],
    ) -> StorageResult<SlotPage> {
        let dict = self.types[idx].dictionary().borrow();
        SlotPage::from_bytes(image, dict.dictionary())
    }

    /// Split every staged remote page over [`REMOTE_TARGET_BLOCKS`], as the
    /// disk's settle does (`crate::split`). Returns every staged bucket's
    /// encoded image.
    pub(crate) fn split_remote(
        &self,
        work: &mut SlotUpload,
    ) -> StorageResult<BTreeMap<usize, Vec<u8>>> {
        let mut images = BTreeMap::new();
        for (&bucket, page) in &mut work.staged {
            images.insert(bucket, self.encode_remote(work.idx, page)?);
        }
        let idx = work.idx;
        split_staged(
            &mut work.remote,
            &mut work.staged,
            &mut images,
            REMOTE_TARGET_BLOCKS,
            |page| self.encode_remote(idx, page),
        )?;
        Ok(images)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::sharing;
    use crate::directory::Directory;

    const SEED: [u64; 4] = [5, 6, 7, 8];

    /// A directory grown by `splits` splits of pseudo-random buckets, so
    /// its pages sit at uneven depths.
    fn grown(splits: u64, salt: u64) -> Directory {
        let mut dir = Directory::new(SEED);
        let mut x = salt | 1;
        for _ in 0..splits {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let bucket = dir.bucket_at((x as usize) % dir.len());
            if dir.can_split(bucket) {
                dir.split(bucket);
            }
        }
        dir
    }

    /// The congruence names exactly the remote buckets that hold some id
    /// of the disk bucket — checked against routing ids for real.
    #[test]
    fn sharing_is_exactly_where_the_ids_go() {
        for (disk_splits, remote_splits) in [(0, 0), (9, 2), (2, 9), (40, 13)] {
            let disk = grown(disk_splits, 11 + disk_splits);
            let remote = grown(remote_splits, 29 + remote_splits);
            let mut truth: BTreeMap<usize, BTreeSet<usize>> = BTreeMap::new();
            for id in 0u128..20_000 {
                truth
                    .entry(disk.bucket_of(id))
                    .or_default()
                    .insert(remote.bucket_of(id));
            }
            for (bucket, _) in disk.buckets() {
                let got = sharing(&disk, bucket, &remote);
                let want = truth.remove(&bucket).unwrap_or_default();
                assert_eq!(got, want, "disk bucket {bucket}");
            }
        }
    }
}
