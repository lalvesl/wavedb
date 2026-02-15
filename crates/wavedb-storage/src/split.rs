//! A round's over-target pages split ([RFC 0074]) — the disk's settle
//! (`crate::plan`) and the object store's upload (`crate::upload_plan`)
//! alike.
//!
//! Only a page the round staged splits: it is being rewritten anyway, and
//! its two halves together are the same bytes, so a split costs no IO. A
//! half still over target — every record landed on one side — splits again
//! in the same round. A page stops splitting once it is under target, holds
//! a single record (a large record is a large page, RFC 0049), or cannot
//! split ([`Directory::can_split`]).
//!
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing-WIP.md

use std::collections::BTreeMap;

use crate::directory::Directory;
use crate::error::StorageResult;
use crate::page::SlotPage;
use crate::plan::blocks_of;

/// Split every staged page over `target` blocks, cascading, keeping
/// `images` — every staged page's encoding, by `encode` — in step.
///
/// # Errors
/// What `encode` returns.
pub fn split_staged(
    dir: &mut Directory,
    staged: &mut BTreeMap<usize, SlotPage>,
    images: &mut BTreeMap<usize, Vec<u8>>,
    target: u64,
    mut encode: impl FnMut(&SlotPage) -> StorageResult<Vec<u8>>,
) -> StorageResult<()> {
    let mut queue: Vec<usize> = images.keys().copied().collect();
    // Pages that could not split yet: the round's other splits may make
    // room for them (`DIR_RATIO`), and they are tried again then.
    let mut waiting = Vec::new();
    let mut split_any = false;
    loop {
        let Some(bucket) = queue.pop() else {
            if !split_any || waiting.is_empty() {
                return Ok(());
            }
            queue = std::mem::take(&mut waiting);
            split_any = false;
            continue;
        };
        let over = images.get(&bucket).is_some_and(|i| blocks_of(i) > target);
        let Some(page) = staged.get_mut(&bucket) else {
            continue;
        };
        if !over || page.len() < 2 {
            continue;
        }
        if !dir.can_split(bucket) {
            waiting.push(bucket);
            continue;
        }
        split_any = true;
        let hash = page.struct_hash();
        let page = std::mem::replace(page, SlotPage::new(hash));
        let depth = dir.descriptor(bucket).depth();
        let (mut keep, mut moved) = (SlotPage::new(hash), SlotPage::new(hash));
        for (id, bytes) in page.into_entries() {
            // Bit ℓ decides: 0 stays put, 1 moves to the new bucket.
            if (dir.hash(id.raw()) >> depth) & 1 == 0 {
                keep.upsert(id, bytes);
            } else {
                moved.upsert(id, bytes);
            }
        }
        let new = dir.split(bucket);
        images.insert(bucket, encode(&keep)?);
        images.insert(new, encode(&moved)?);
        staged.insert(bucket, keep);
        staged.insert(new, moved);
        queue.extend([bucket, new]);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use wavedb_core::{Id, U48};

    use super::split_staged;
    use crate::directory::Directory;
    use crate::error::StorageResult;
    use crate::page::SlotPage;

    const SH: u64 = 0x0074_0000_0000_0001;
    const SEED: [u64; 4] = [3, 1, 4, 1];

    fn id(k: u64) -> Id {
        Id::new(k, U48::from(1u32), false, 1)
    }

    /// A stand-in encoding: one block per record.
    fn encode(page: &SlotPage) -> StorageResult<Vec<u8>> {
        let blocks = vec![0; page.len() * 4096];
        if page.len() > 1_000 {
            return Err(crate::error::StorageError::PageTooLarge(0));
        }
        Ok(blocks)
    }

    fn stage(records: u64) -> (Directory, BTreeMap<usize, SlotPage>) {
        let mut page = SlotPage::new(SH);
        for k in 0..records {
            page.upsert(id(k), vec![1]);
        }
        (Directory::new(SEED), BTreeMap::from([(0, page)]))
    }

    /// One round cascades until every page is under target — or may not
    /// split, the directory at its ratio — and every record ends on the
    /// page its id routes to.
    #[test]
    fn one_round_cascades_until_under_target() {
        let (mut dir, mut staged) = stage(200);
        let mut images = BTreeMap::from([(0, vec![0; 200 * 4096])]);
        split_staged(&mut dir, &mut staged, &mut images, 4, encode).unwrap();
        assert!(dir.pages() >= 50, "{} pages", dir.pages());
        assert_eq!(images.len(), staged.len());
        let mut total = 0;
        for (&bucket, page) in &staged {
            assert!(
                page.len() <= 4 || !dir.can_split(bucket),
                "bucket {bucket}: {}",
                page.len()
            );
            for (id, _) in page.iter() {
                assert_eq!(dir.bucket_of(id.raw()), bucket);
            }
            total += page.len();
        }
        assert_eq!(total, 200);
    }

    /// A page of one record never splits, however large.
    #[test]
    fn a_single_record_never_splits() {
        let (mut dir, mut staged) = stage(1);
        let mut images = BTreeMap::from([(0, vec![0; 64 * 4096])]);
        split_staged(&mut dir, &mut staged, &mut images, 4, encode).unwrap();
        assert_eq!((dir.len(), dir.pages()), (1, 1));
    }

    /// A page the round did not stage is left as it is, over target or not.
    #[test]
    fn an_unstaged_page_is_not_split() {
        let (mut dir, _) = stage(0);
        let mut images = BTreeMap::from([(0, vec![0; 64 * 4096])]);
        let mut staged = BTreeMap::new();
        split_staged(&mut dir, &mut staged, &mut images, 4, encode).unwrap();
        assert_eq!(dir.pages(), 1);
    }
}
