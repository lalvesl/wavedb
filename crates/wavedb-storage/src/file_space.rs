//! One data file as the allocator sees it ([RFC 0071]): its end and its
//! holes — split from [`crate::alloc`] for the file budget.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::collections::BTreeMap;

use crate::block::Run;

/// The allocator's view of one data file.
#[derive(Debug, Default, Clone)]
pub struct FileSpace {
    /// One past its last block.
    pub end: u64,
    /// Its holes: start → count, coalesced within the file.
    pub free: BTreeMap<u64, u64>,
    /// The blocks `free` holds.
    pub free_blocks: u64,
}

/// One data file's use, for choosing what to compact
/// ([`files`](crate::BlockAllocator::files)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileUse {
    /// Its first block — its name.
    pub start: u64,
    /// One past its last block.
    pub end: u64,
    /// Free blocks in it.
    pub free: u64,
    /// Blocks in it freed but held back for a recovery root.
    pub deferred: u64,
}

/// What a [`reclaim`](crate::BlockAllocator::reclaim) cut off one file: its
/// blocks from `end` to `was`. A file cut back to its own start is deleted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cut {
    /// The file's first block.
    pub file: u64,
    /// Its new end.
    pub end: u64,
    /// Its end before.
    pub was: u64,
}

impl FileSpace {
    /// Add `run` to the holes, merged with the ones it touches.
    pub fn coalesce(&mut self, run: Run) {
        let (mut start, mut count) = (run.start, run.count);
        if let Some((&p, &n)) = self.free.range(..start).next_back() {
            debug_assert!(p + n <= start, "double free overlaps predecessor");
            if p + n == start {
                self.remove(p, n);
                start = p;
                count += n;
            }
        }
        if let Some(n) = self.free.get(&(start + count)).copied() {
            self.remove(start + count, n);
            count += n;
        }
        debug_assert!(
            self.free.range(start..start + count).next().is_none(),
            "double free overlaps successor"
        );
        self.insert(start, count);
    }

    pub fn insert(&mut self, start: u64, count: u64) {
        self.free.insert(start, count);
        self.free_blocks += count;
    }

    pub fn remove(&mut self, start: u64, count: u64) {
        self.free.remove(&start);
        self.free_blocks -= count;
    }
}
