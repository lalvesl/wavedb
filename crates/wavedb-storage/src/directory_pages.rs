//! [`Directory`] page **reads** — resolving a bucket to its [`SlotPage`] over
//! a [`BlockFile`].
//!
//! The addressing math and the container live in [`crate::directory`]. The
//! write half is not here: pages are rewritten by the checkpoint
//! ([`crate::plan`] builds the images, [`crate::checkpoint`] places them all
//! in one window), so no single-record page rewrite path exists.

use wavedb_core::Id;

use crate::block_file::BlockFile;
use crate::dictionary::DictState;
use crate::directory::Directory;
use crate::error::StorageResult;
use crate::page::SlotPage;

impl Directory {
    /// Read the [`SlotPage`] backing `bucket`, or a fresh empty page if the slot
    /// is unallocated.
    ///
    /// # Errors
    /// [`StorageError::Io`](crate::StorageError::Io) on a read fault or
    /// [`StorageError::Corrupt`](crate::StorageError::Corrupt) if the page fails
    /// its crc / bounds checks.
    pub fn read_page(
        &self,
        struct_hash: u64,
        file: &BlockFile,
        bucket: usize,
        dict: &DictState,
    ) -> StorageResult<SlotPage> {
        let desc = self.slots[bucket];
        if !desc.is_allocated() {
            return Ok(SlotPage::new(struct_hash));
        }
        // Through the page cache (RFC 0044): a bucket's image is immutable for
        // as long as its run stays allocated — page writes are copy-on-write
        // and land in a *new* run — so a hit needs no validation, only the
        // decompression below.
        let page = file.read_run_with(desc.run(), |image| {
            SlotPage::from_bytes(image, dict.dictionary())
        })??;
        debug_assert_eq!(page.struct_hash(), struct_hash);
        Ok(page)
    }

    /// The record bytes stored at `id`, if present.
    ///
    /// # Errors
    /// Propagates read / corruption faults from [`read_page`](Self::read_page).
    pub fn get_record(
        &self,
        struct_hash: u64,
        file: &BlockFile,
        id: Id,
        dict: &DictState,
    ) -> StorageResult<Option<Vec<u8>>> {
        let bucket = self.bucket_of(id.raw());
        Ok(self
            .read_page(struct_hash, file, bucket, dict)?
            .get(id)
            .map(<[u8]>::to_vec))
    }
}

#[cfg(test)]
mod tests {
    use crate::block::{BlockDescriptor, Run};
    use crate::block_file::{BlockFile, RESERVED_BLOCKS};
    use crate::dictionary::DictState;
    use crate::directory::Directory;
    use crate::page::SlotPage;
    use wavedb_core::{Id, U48};

    const SH: u64 = 0xABCD;

    fn rec_id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, (key & 0x7FFF) as u16)
    }

    /// A page written by hand (what the checkpoint's window does in bulk)
    /// resolves back through the directory's read path.
    #[test]
    fn a_placed_page_resolves_through_the_directory() {
        let d = tempfile::tempdir().unwrap();
        let file = BlockFile::open(d.path().join("data.bin")).unwrap();
        let dict = DictState::new(true);
        let mut dir = Directory::new(file.seed());

        let mut page = SlotPage::new(SH);
        page.upsert(rec_id(1), vec![1, 2, 3]);
        page.upsert(rec_id(2), vec![4, 5]);
        let bytes = page.to_bytes(dict.dictionary(), dict.enabled()).unwrap();

        let run = Run::new(RESERVED_BLOCKS, 1);
        file.write_run(run, &bytes).unwrap();
        dir.set_descriptor(
            0,
            BlockDescriptor::from_run_used(run, bytes.len() as u64),
        );

        assert_eq!(
            dir.get_record(SH, &file, rec_id(1), &dict).unwrap(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            dir.get_record(SH, &file, rec_id(2), &dict).unwrap(),
            Some(vec![4, 5])
        );
        assert_eq!(dir.get_record(SH, &file, rec_id(9), &dict).unwrap(), None);
    }

    /// The page cache is actually on the read path, measured by the engine's
    /// own IO counter rather than by asking the cache whether it was used.
    ///
    /// This is the assertion that would fail if `read_page` went back to
    /// `read_run`: everything else about the cache would still pass its unit
    /// tests while serving nobody.
    #[test]
    fn a_second_read_of_a_page_costs_no_disk_read() {
        let d = tempfile::tempdir().unwrap();
        let file = BlockFile::open(d.path().join("data.bin")).unwrap();
        let dict = DictState::new(true);
        let mut dir = Directory::new(file.seed());

        let mut page = SlotPage::new(SH);
        page.upsert(rec_id(1), vec![1, 2, 3]);
        let bytes = page.to_bytes(dict.dictionary(), dict.enabled()).unwrap();
        let run = Run::new(RESERVED_BLOCKS, 1);
        file.write_run(run, &bytes).unwrap();
        dir.set_descriptor(
            0,
            BlockDescriptor::from_run_used(run, bytes.len() as u64),
        );

        let before = file.io().snapshot().0;
        assert_eq!(
            dir.get_record(SH, &file, rec_id(1), &dict).unwrap(),
            Some(vec![1, 2, 3])
        );
        let after_cold = file.io().snapshot().0;
        assert_eq!(after_cold, before + 1, "the cold read must reach the disk");

        for _ in 0..10 {
            assert_eq!(
                dir.get_record(SH, &file, rec_id(1), &dict).unwrap(),
                Some(vec![1, 2, 3])
            );
        }
        assert_eq!(
            file.io().snapshot().0,
            after_cold,
            "ten warm reads must cost no further disk read"
        );

        // Writing into the extent is what makes the held bytes wrong, and it
        // is the one event a run-keyed cache has to notice.
        file.write_run(run, &bytes).unwrap();
        assert_eq!(
            dir.get_record(SH, &file, rec_id(1), &dict).unwrap(),
            Some(vec![1, 2, 3])
        );
        assert_eq!(
            file.io().snapshot().0,
            after_cold + 1,
            "a write into the run must have invalidated the cached image"
        );
    }

    /// An unallocated bucket reads as an empty page, never as a fault.
    #[test]
    fn vacant_bucket_reads_empty() {
        let d = tempfile::tempdir().unwrap();
        let file = BlockFile::open(d.path().join("data.bin")).unwrap();
        let dict = DictState::new(true);
        let dir = Directory::new(file.seed());
        assert!(dir.read_page(SH, &file, 0, &dict).unwrap().is_empty());
        assert_eq!(dir.get_record(SH, &file, rec_id(1), &dict).unwrap(), None);
    }
}
