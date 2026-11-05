//! The page cache on the data files' read path ([RFC 0044]) — split from
//! [`crate::page_cache`], the cache itself: a page read through it is held
//! decoded, under its run and the slot whose page it is (a pack holds
//! several, `crate::pack`).
//!
//! [RFC 0044]: ../../../rfcs/0044-page-cache.md

use crate::block::Run;
use crate::block_file::BlockFile;
use crate::error::StorageResult;
use crate::page::SlotPage;
use crate::page_cache::{Lane, PageCache};

impl BlockFile {
    /// The page of slot `slot` at `run`, lent to `read` — held by the page
    /// cache, or read, decoded by `decode` and then held, decoded (RFC
    /// 0044): a hit costs no read and no decompression. `read` must not
    /// reach the cache itself.
    /// Pages only: the dictionary image, the headers and compaction's
    /// buffers are read once and want the stored bytes
    /// ([`read_run`](BlockFile::read_run)).
    ///
    /// # Errors
    /// The same faults as [`read_run`](BlockFile::read_run), and `decode`'s. A
    /// cache hit cannot fault at all, which is the point.
    pub fn with_page<R>(
        &self,
        run: Run,
        slot: u64,
        decode: impl FnOnce(&[u8]) -> StorageResult<SlotPage>,
        read: impl FnOnce(&SlotPage) -> R,
    ) -> StorageResult<R> {
        let read = match self.cache.with(Lane::Disk, run, slot, read) {
            Ok(done) => {
                #[cfg(feature = "probe")]
                crate::probe::page_hit();
                return Ok(done);
            }
            Err(read) => read,
        };
        #[cfg(feature = "probe")]
        let page = crate::probe::page_miss(|| self.read_run(run), decode)?;
        #[cfg(not(feature = "probe"))]
        let page = decode(&self.read_run(run)?)?;
        let done = read(&page);
        self.cache.insert(Lane::Disk, run, slot, page);
        Ok(done)
    }

    /// The page at `run`, owned — taken out of the page cache, or read and
    /// decoded — for a settle about to rewrite it into a new run, which
    /// leaves this one dead anyway.
    ///
    /// # Errors
    /// As [`with_page`](Self::with_page).
    pub fn take_page(
        &self,
        run: Run,
        slot: u64,
        decode: impl FnOnce(&[u8]) -> StorageResult<SlotPage>,
    ) -> StorageResult<SlotPage> {
        match self.cache.take(Lane::Disk, run, slot) {
            Some(page) => Ok(page),
            None => decode(&self.read_run(run)?),
        }
    }

    /// The page cache backing [`with_page`](Self::with_page).
    #[must_use]
    pub const fn page_cache(&self) -> &PageCache {
        &self.cache
    }
}
