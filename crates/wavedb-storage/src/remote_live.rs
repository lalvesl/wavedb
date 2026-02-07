//! What of the object store a store still names ([RFC 0070] §8) — split
//! from [`crate::remote_compact`], which compacts by it: the live blocks
//! per file, the space compaction weighs, and the files the disk's newest
//! roots name.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::collections::BTreeMap;

use crate::compact::Space;
use crate::page_store::PageStore;
use crate::remote::{FILE_BLOCKS, file_of};
use crate::remote_link::RemoteLink;

impl PageStore {
    /// The object store's space as compaction weighs it: every file not
    /// deleted counted at its nominal 8 MiB, and free whatever of that is
    /// not a live page — so the small files an upload interval leaves read
    /// as mostly free, which is what they cost in objects.
    #[must_use]
    pub fn remote_space(&self) -> Space {
        let Some(object) = &self.object else {
            return Space::default();
        };
        let chain = object.chain.borrow();
        let files = (chain.start..chain.end.div_ceil(FILE_BLOCKS))
            .filter(|f| !object.deleted.borrow().contains(f))
            .count() as u64;
        // Through a storage shard, this data shard's share of its files.
        let share = object.link.as_ref().map_or(1, RemoteLink::per);
        let file_blocks = files * FILE_BLOCKS / share;
        let live: u64 = self.remote_live().values().sum();
        Space {
            file_blocks,
            free_blocks: file_blocks.saturating_sub(live),
            deferred_blocks: 0,
        }
    }

    /// Live blocks per file: every remote page and dictionary named.
    pub(crate) fn remote_live(&self) -> BTreeMap<u64, u64> {
        let mut live = BTreeMap::new();
        for slot in &self.types {
            let remote = slot.remote_directory().borrow();
            let pages = remote.iter().flat_map(|dir| dir.slots().iter());
            let dict = slot.remote_dict().get();
            for desc in pages.copied().chain(std::iter::once(dict)) {
                if desc.is_allocated() {
                    *live.entry(file_of(desc.start())).or_insert(0) +=
                        desc.count();
                }
            }
        }
        live
    }

    /// A batch ended naming the store's addressing: the object store's files
    /// it names become the newest root's, the newest's the previous one's.
    pub(crate) fn root_remote(&self) {
        let Some(object) = &self.object else { return };
        if !self.has_disk() {
            return; // nothing reopens from RAM
        }
        let named = self.remote_live().into_keys().collect();
        let mut rooted = object.rooted.borrow_mut();
        rooted[1] = std::mem::replace(&mut rooted[0], named);
    }

    /// The newest batch is durable: the root before it is not one any more.
    pub(crate) fn release_remote_root(&self) {
        if let Some(object) = &self.object {
            object.rooted.borrow_mut()[1].clear();
        }
    }
}
