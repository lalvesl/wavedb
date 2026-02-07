//! A storage shard's compaction ([RFC 0073] §7) — split from
//! [`crate::storage_shard`], whose thread runs it.
//!
//! The files belong to the storage shard; the directories naming what is in
//! them belong to its data shards. So emptying a file is an **evacuation**:
//! the first data shard to compact asks which files to empty and opens one
//! — every file at least its `victim_free_percent` free or held back, the
//! active one closed first if it is one — and every data shard compacting
//! carries its own runs out of those files, after restating its addresses
//! so none of its headers there is live. Each says so once a batch over its
//! moves is durable; the evacuation ends when all have, and the next one
//! opens with the next ask.
//!
//! Nothing waits for that to give space back: after every durable storage
//! write the free tails are cut and a file left all holes is deleted — a
//! hole is a run no durable batch names, so no reconstruction reads it.
//! The data shards hear of the deletions with their next reservation and
//! close their handles.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::BTreeSet;

use crate::compact::Space;
use crate::storage_shard::StorageShard;

impl StorageShard {
    /// The files the data shard at `position` is to empty: the running
    /// evacuation's — none once it is done with them — or a new one's.
    pub(crate) fn victims(
        &mut self,
        percent: u8,
        position: usize,
    ) -> Vec<(u64, u64)> {
        if let Some((files, done)) = &self.evacuation {
            return if done.contains(&position) {
                Vec::new()
            } else {
                files.clone()
            };
        }
        let percent = u64::from(percent);
        let worth = |start: u64, end: u64, unused: u64| {
            let size = end - start;
            unused < size && unused * 100 >= size * percent
        };
        let files = self.alloc.files();
        let active = self.alloc.active();
        if files.iter().any(|f| {
            Some(f.start) == active
                && worth(f.start, f.end, f.free + f.deferred)
        }) {
            // Emptying the file new runs go to: they go elsewhere first.
            self.alloc.roll();
        }
        let victims: Vec<(u64, u64)> = files
            .iter()
            .filter(|f| worth(f.start, f.end, f.free + f.deferred))
            .map(|f| (f.start, f.end))
            .collect();
        if !victims.is_empty() {
            self.evacuation = Some((victims.clone(), BTreeSet::new()));
        }
        victims
    }

    /// The data shard at `position` is done with the evacuation.
    pub(crate) fn evacuated(&mut self, position: usize) {
        let ended = self.evacuation.as_mut().is_some_and(|(_, done)| {
            done.insert(position);
            done.len() >= self.by_shard.len()
        });
        if ended {
            self.evacuation = None;
        }
    }

    /// Cut every free tail and delete every file left empty — the active
    /// one excepted.
    pub(crate) fn reclaim(&mut self) {
        for cut in self.alloc.reclaim(false) {
            if self.file.cut(cut.file, cut.end).is_ok() && cut.end == cut.file {
                self.deleted.push(cut.file);
            }
        }
    }

    /// How the storage shard's blocks are used.
    pub(crate) fn space(&self) -> Space {
        Space {
            file_blocks: self.alloc.total_blocks(),
            free_blocks: self.alloc.free_blocks(),
            deferred_blocks: self.alloc.deferred_blocks(),
        }
    }

    /// The files deleted since the data shard at `position` was last told.
    pub(crate) fn gone_for(&mut self, position: usize) -> Vec<u64> {
        let Some(told) = self.told.get_mut(position) else {
            return Vec::new();
        };
        let gone = self.deleted[*told..].to_vec();
        *told = self.deleted.len();
        gone
    }
}
