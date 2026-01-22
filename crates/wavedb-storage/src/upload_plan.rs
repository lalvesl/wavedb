//! An upload's middle ([RFC 0070] §5): which remote pages share records with
//! the flagged disk pages, and those pages rebuilt — the old one, minus every
//! record of a flagged disk bucket, plus the flagged pages' records — then
//! split while the bucket whose turn it is runs over target.
//!
//! The disk and the object store each have a linear hash of their own. A
//! bucket's **level** is how many low hash bits route to it, so disk bucket
//! `d` and remote bucket `s` can hold the same record exactly when
//! `d ≡ s (mod 2^min(level(d), level(s)))` — arithmetic, no lookup.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::collections::{BTreeMap, BTreeSet};

use wavedb_core::Id;

use crate::block::BlockDescriptor;
use crate::directory::Directory;
use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;
use crate::page_store::PageStore;
use crate::plan::blocks_of;

/// Blocks a remote page should hold (128 KiB): a target, as on disk — a
/// bucket over it is split when its turn comes round (RFC 0049).
pub const REMOTE_TARGET_BLOCKS: u64 = 32;

/// Splits one upload may perform per type.
const MAX_REMOTE_SPLITS: usize = 64;

/// The low hash bits bucket `bucket` of a `len`-bucket linear hash routes by.
pub const fn level_of(len: u64, bucket: u64) -> u32 {
    let level = len.ilog2();
    let base = 1u64 << level;
    if bucket < len - base || bucket >= base {
        level + 1
    } else {
        level
    }
}

/// The buckets of a `remote_len`-bucket directory that can share records
/// with bucket `disk` of a `disk_len`-bucket one.
pub fn sharing(disk_len: u64, disk: u64, remote_len: u64) -> Vec<usize> {
    let disk_level = level_of(disk_len, disk);
    // Every candidate agrees with `disk` on the bits both levels have at
    // least; walk those, then keep the ones that agree on all they share.
    let step = 1u64 << disk_level.min(remote_len.ilog2());
    (disk % step..remote_len)
        .step_by(step as usize)
        .filter(|&s| {
            let bits = disk_level.min(level_of(remote_len, s));
            (s ^ disk) & ((1u64 << bits) - 1) == 0
        })
        .map(|s| s as usize)
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
        let (disk_len, remote_len) =
            (self.disk.len() as u64, self.remote.len() as u64);
        self.flagged
            .iter()
            .flat_map(|&(d, _)| sharing(disk_len, d as u64, remote_len))
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
        let dict = self.types[idx].dictionary().borrow();
        page.to_bytes(dict.dictionary(), dict.enabled())
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

    /// Split the remote directory while the bucket whose turn it is runs
    /// over [`REMOTE_TARGET_BLOCKS`], as the disk's `plan_splits` does,
    /// fetching a source page this upload did not stage. Returns every
    /// staged bucket's encoded image.
    pub(crate) async fn split_remote(
        &self,
        work: &mut SlotUpload,
    ) -> StorageResult<BTreeMap<usize, Vec<u8>>> {
        let mut images = BTreeMap::new();
        for (&bucket, page) in &work.staged {
            images.insert(bucket, self.encode_remote(work.idx, page)?);
        }
        let hash = self.types[work.idx].struct_hash();
        for _ in 0..MAX_REMOTE_SPLITS {
            let level = work.remote.split_bit();
            let source = work.remote.next_split_bucket() as usize;
            let blocks = images.get(&source).map_or_else(
                || work.remote.descriptor(source).count(),
                |image| blocks_of(image),
            );
            if blocks <= REMOTE_TARGET_BLOCKS {
                break;
            }
            let page = if let Some(page) = work.staged.remove(&source) {
                page
            } else {
                let desc = work.remote.descriptor(source);
                let image = self.fetch_image(desc).await?;
                self.decode_remote(work.idx, &image)?
            };
            let (mut keep, mut moved) =
                (SlotPage::new(hash), SlotPage::new(hash));
            for (id, bytes) in page.into_entries() {
                if (work.remote.hash(id.raw()) >> level) & 1 == 0 {
                    keep.upsert(id, bytes);
                } else {
                    moved.upsert(id, bytes);
                }
            }
            let target = work.remote.push_bucket(BlockDescriptor::EMPTY);
            images.insert(source, self.encode_remote(work.idx, &keep)?);
            images.insert(target, self.encode_remote(work.idx, &moved)?);
            work.staged.insert(source, keep);
            work.staged.insert(target, moved);
        }
        Ok(images)
    }
}

#[cfg(test)]
mod tests {
    use super::{level_of, sharing};
    use crate::directory::bucket_index;

    #[test]
    fn a_bucket_level_is_the_bits_that_route_to_it() {
        assert_eq!(level_of(1, 0), 0);
        // len 6: base 4, buckets 0 and 1 split (3 bits), 2 and 3 not yet
        // (2 bits), 4 and 5 new (3 bits).
        let levels: Vec<u32> = (0..6).map(|b| level_of(6, b)).collect();
        assert_eq!(levels, [3, 3, 2, 2, 3, 3]);
        assert_eq!(level_of(8, 5), 3, "a power of two is one level");
    }

    /// The congruence names exactly the remote buckets that hold some hash
    /// of the disk bucket — checked against routing every hash for real.
    #[test]
    fn sharing_is_exactly_where_the_hashes_go() {
        for disk_len in 1u64..40 {
            for remote_len in 1u64..20 {
                let mut truth =
                    vec![std::collections::BTreeSet::new(); disk_len as usize];
                for hash in 0u64..512 {
                    let d = bucket_index(disk_len, hash);
                    truth[d].insert(bucket_index(remote_len, hash));
                }
                for d in 0..disk_len {
                    let got: std::collections::BTreeSet<usize> =
                        sharing(disk_len, d, remote_len).into_iter().collect();
                    assert_eq!(
                        got, truth[d as usize],
                        "disk {d}/{disk_len}, remote len {remote_len}"
                    );
                }
            }
        }
    }
}
