//! An upload's middle ([RFC 0070] §5): which remote pages share records with
//! the flagged disk pages, and those pages rebuilt — the old one, minus every
//! record of a flagged disk bucket, plus the flagged pages' records — then
//! split while over target ([RFC 0074]).
//!
//! The disk and the object store each have an extendible hash of their
//! own. A page's **depth** is how many low hash bits its records share, so
//! disk bucket `d` and a remote page can hold the same record exactly when
//! the remote page has an entry `j ≡ d (mod 2^min(depth(d), D))`, `D` the
//! remote directory's depth — arithmetic, no lookup.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing-WIP.md

use std::collections::{BTreeMap, BTreeSet};

use wavedb_core::Id;

use crate::block::BlockDescriptor;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::plan::image_of;
use crate::split::split_staged;

/// Blocks a remote page should hold (128 KiB): a target, as on disk — a
/// page over it splits in the upload that rewrites it.
pub const REMOTE_TARGET_BLOCKS: u64 = 32;

/// The buckets of `remote` that can share records with bucket `bucket` of
/// `disk`: those with an entry agreeing with it on every bit both depths
/// route by.
pub fn sharing(
    disk: &Directory,
    bucket: usize,
    remote: &Directory,
) -> BTreeSet<usize> {
    let bits = disk.descriptor(bucket).depth().min(remote.depth());
    let step = 1usize << bits;
    (bucket & (step - 1)..remote.len())
        .step_by(step)
        .map(|entry| remote.bucket_at(entry))
        .collect()
}

/// One type's share of an upload, from the snapshot to the encoded pages.
pub struct SlotUpload {
    /// Index into [`PageStore::types`].
    pub(crate) idx: usize,
    /// The disk directory as it stood when the upload began.
    pub(crate) disk: Directory,
    /// Every flagged disk bucket taken, with its descriptor then.
    pub(crate) flagged: Vec<(usize, BlockDescriptor)>,
    /// Those buckets' pages, in the same order.
    pub(crate) pages: Vec<SlotPage>,
    /// The remote directory being rebuilt — the upload's own copy.
    pub(crate) remote: Directory,
    /// Remote bucket → its rebuilt page.
    pub(crate) staged: BTreeMap<usize, SlotPage>,
    /// Remote buckets carried forward as they are, out of files a
    /// compaction empties (`crate::remote_compact`).
    pub(crate) moved: BTreeSet<usize>,
    /// Whether the dictionary is carried forward too.
    pub(crate) move_dict: bool,
}

impl SlotUpload {
    /// The remote buckets this upload rewrites: those sharing records with a
    /// flagged disk page, and those carried forward.
    pub(crate) fn affected(&self) -> BTreeSet<usize> {
        self.flagged
            .iter()
            .flat_map(|&(d, _)| sharing(&self.disk, d, &self.remote))
            .chain(self.moved.iter().copied())
            .collect()
    }

    /// Rebuild the staged pages: each holds its old records (fetched into
    /// `staged` beforehand) minus those of a flagged disk bucket, plus the
    /// flagged pages' records that route to it.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] if a record routes to a remote page the
    /// congruence did not name — the arithmetic above is wrong, and writing
    /// on would drop that page's other records.
    pub(crate) fn rebuild(&mut self) -> StorageResult<()> {
        let flagged: BTreeSet<usize> =
            self.flagged.iter().map(|&(d, _)| d).collect();
        for page in self.staged.values_mut() {
            let stale: Vec<Id> = page
                .iter()
                .map(|(id, _)| id)
                .filter(|id| flagged.contains(&self.disk.bucket_of(id.raw())))
                .collect();
            for id in stale {
                page.remove(id);
            }
        }
        for page in std::mem::take(&mut self.pages) {
            for (id, bytes) in page.into_entries() {
                let bucket = self.remote.bucket_of(id.raw());
                self.staged
                    .get_mut(&bucket)
                    .ok_or(StorageError::Corrupt("upload congruence"))?
                    .upsert(id, bytes);
            }
        }
        Ok(())
    }
}

impl PageStore {
    /// Encode one remote page with its type's dictionary; an empty page is
    /// no bytes at all — its bucket becomes vacant.
    pub(crate) fn encode_remote(
        &self,
        idx: usize,
        page: &SlotPage,
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
        for (&bucket, page) in &work.staged {
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
