//! The read-through half of the cache-or-page read path (split from
//! [`crate::page_store`] for the file budget): serving an id the cache no
//! longer holds from its settled page.
//!
//! There used to be an `owner_of` here too — "which slot holds this id?",
//! answered by probing every one of them. It went with RFC 0063: `Remove`
//! and `Expect` now carry the STRUCT_HASH they route by, so nothing has to
//! search for an id's type any more.

use wavedb_core::Id;

use crate::error::StorageResult;
use crate::page_store::PageStore;
use crate::struct_storage::TypeSlot;

impl PageStore {
    /// Read `id` from `slot`'s settled pages — the fallback when the cache
    /// does not hold it. `None` when the type has never settled anything or
    /// the page does not hold the id. A remove not yet settled never reaches
    /// here: its tombstone is in the cache, which answers first.
    // The directory guard must span the page read — `dir` borrows from it.
    #[allow(clippy::significant_drop_tightening)]
    pub(crate) fn read_from_pages(
        &self,
        slot: &TypeSlot,
        id: Id,
    ) -> StorageResult<Option<Vec<u8>>> {
        let dir_guard = slot.directory().borrow_mut();
        let Some(dir) = dir_guard.as_ref() else {
            return Ok(None);
        };
        let dict = slot.dictionary().borrow_mut();
        dir.get_record(slot.struct_hash(), &self.file, id, &dict)
    }
}
