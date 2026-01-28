//! [`BlockAllocator`] — where the blocks of a shard's data files go
//! ([RFC 0071]), split from [`crate::block`] for the file budget.
//!
//! A shard's blocks are one address space, cut into **data files** each
//! named by the block it starts at. Allocation only ever **appends**, to the
//! end of the newest file (the *active* one); once that file has reached the
//! ceiling, the next run opens a new file where it ended. A run may reach
//! past the ceiling — the ceiling bounds where a run *starts* — and is never
//! split across two files. No freed block is handed out again: what a freed
//! run leaves is a hole its file keeps until compaction empties the file,
//! and the free extents (coalesced per file, never across two) exist to say
//! how much of a file is holes and to give its free tail back
//! ([`reclaim`](BlockAllocator::reclaim)).
//!
//! This is a **pure in-memory structure**: the files themselves are the
//! [`BlockFile`](crate::BlockFile)'s, which creates one the first time a run
//! in it is written, and cuts or deletes one when told to.
//!
//! ## Frees wait for a durable batch
//!
//! A run freed by a batch is still named by the batch before it, the one a
//! crash would reopen from until this one is durable ([RFC 0072] §8). No
//! freed block is ever written again, so the run itself is safe; what must
//! wait is giving it back to the filesystem — a file's free tail cut, an
//! emptied file deleted. So a store **defers** a free
//! ([`defer`](BlockAllocator::defer)) and [`release_to`](BlockAllocator::release_to)s
//! the frees a batch covered once that batch is durable; only then does the
//! run become a hole a [`reclaim`](BlockAllocator::reclaim) may cut.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, Run};
use crate::file_space::FileSpace;
pub use crate::file_space::{Cut, FileUse};

/// Bytes a data file grows to before the next one opens, by default
/// (10 GiB).
pub const DEFAULT_FILE_BYTES: u64 = 10 << 30;

/// Where a shard's blocks go (see the module docs).
#[derive(Debug)]
pub struct BlockAllocator {
    /// The data files, by first block.
    files: BTreeMap<u64, FileSpace>,
    /// Blocks the active file reaches before the next run opens a new one.
    ceiling: u64,
    /// Where the next file starts when none is active: past every file.
    next: u64,
    /// Whether the active file is closed to new runs ([`roll`](Self::roll)).
    rolled: bool,
    /// Frees waiting for the batch that made them to be durable, oldest
    /// first (see module docs).
    pending: Vec<Run>,
    /// Frees deferred and released since the allocator was made — what a
    /// [`mark`](Self::mark) counts in.
    deferred_total: u64,
    released_total: u64,
}

impl Default for BlockAllocator {
    fn default() -> Self {
        Self::new(DEFAULT_FILE_BYTES)
    }
}

impl BlockAllocator {
    /// An allocator over no file yet, opening files of `file_bytes` (at
    /// least one block).
    #[must_use]
    pub fn new(file_bytes: u64) -> Self {
        Self {
            files: BTreeMap::new(),
            ceiling: (file_bytes / BLOCK_SIZE as u64).max(1),
            next: 0,
            rolled: false,
            pending: Vec::new(),
            deferred_total: 0,
            released_total: 0,
        }
    }

    /// Rebuild an allocator over existing data files — `(first block, end)`
    /// each — and the runs in use in them. Every gap between used runs is a
    /// hole.
    #[must_use]
    pub fn from_layout(
        files: &[(u64, u64)],
        used: &[Run],
        file_bytes: u64,
    ) -> Self {
        let mut a = Self::new(file_bytes);
        for &(start, end) in files {
            a.files.insert(
                start,
                FileSpace {
                    end,
                    ..FileSpace::default()
                },
            );
            a.next = a.next.max(end);
        }
        let mut runs: Vec<Run> =
            used.iter().copied().filter(|r| r.count > 0).collect();
        runs.sort_by_key(|r| r.start);
        let mut runs = runs.into_iter().peekable();
        for (&start, space) in &mut a.files {
            let mut cursor = start;
            while let Some(run) = runs.next_if(|r| r.start < space.end) {
                if run.start > cursor {
                    space.insert(cursor, run.start - cursor);
                }
                cursor = cursor.max(run.end());
            }
            if cursor < space.end {
                space.insert(cursor, space.end - cursor);
            }
        }
        a
    }

    /// Blocks across every data file.
    #[must_use]
    pub fn total_blocks(&self) -> u64 {
        self.files.iter().map(|(start, f)| f.end - start).sum()
    }

    /// Free blocks across every data file (excludes deferred frees).
    #[must_use]
    pub fn free_blocks(&self) -> u64 {
        self.files.values().map(|f| f.free_blocks).sum()
    }

    /// Blocks held back by deferred frees — runs the newest durable batch
    /// still names, holes once the next one is durable.
    #[must_use]
    pub fn deferred_blocks(&self) -> u64 {
        self.pending.iter().map(|r| r.count).sum()
    }

    /// Every data file's use, lowest first.
    #[must_use]
    pub fn files(&self) -> Vec<FileUse> {
        let mut out: Vec<FileUse> = self
            .files
            .iter()
            .map(|(&start, f)| FileUse {
                start,
                end: f.end,
                free: f.free_blocks,
                deferred: 0,
            })
            .collect();
        for run in &self.pending {
            let at = out.partition_point(|f| f.start <= run.start);
            if let Some(file) = at.checked_sub(1).and_then(|i| out.get_mut(i)) {
                file.deferred += run.count;
            }
        }
        out
    }

    /// The file new runs go to, while one is open to them.
    #[must_use]
    pub fn active(&self) -> Option<u64> {
        let (&start, space) = self.files.iter().next_back()?;
        let full = space.end - start >= self.ceiling;
        (!self.rolled && !full).then_some(start)
    }

    /// The data file holding the run that starts at `start`.
    #[must_use]
    pub fn file_of(&self, start: u64) -> Option<u64> {
        self.files.range(..=start).next_back().map(|(&s, _)| s)
    }

    /// Close the active file: the next run opens a new one. Compaction's
    /// first step when the active file is the one to empty.
    pub fn roll(&mut self) {
        if self.files.values().next_back().is_some() {
            self.rolled = true;
        }
    }

    /// Allocate `count` blocks at the end of the active file, opening a new
    /// one first when there is none. `count` must be `>= 1`.
    ///
    /// # Panics
    /// Panics if `count == 0`.
    pub fn alloc(&mut self, count: u64) -> Run {
        assert!(count > 0, "cannot allocate a zero-length run");
        let file = self.active().unwrap_or_else(|| {
            let start = self.next_start();
            self.files.insert(
                start,
                FileSpace {
                    end: start,
                    ..FileSpace::default()
                },
            );
            self.rolled = false;
            start
        });
        let space = self.files.entry(file).or_default();
        let run = Run::new(space.end, count);
        space.end += count;
        self.next = self.next.max(space.end);
        run
    }

    /// Where a new file starts: past every file there is or was.
    pub fn next_start(&self) -> u64 {
        let last = self.files.values().next_back().map_or(0, |f| f.end);
        self.next.max(last)
    }

    /// Return a run to its file's holes, coalescing with its neighbours.
    pub fn free(&mut self, run: Run) {
        if run.count == 0 {
            return;
        }
        let file = self.file_of(run.start);
        if let Some(space) = file.and_then(|f| self.files.get_mut(&f)) {
            debug_assert!(run.end() <= space.end, "freeing past its file");
            space.coalesce(run);
        }
    }

    /// Free `run` once the batch freeing it is durable (see module docs).
    pub fn defer(&mut self, run: Run) {
        if run.count > 0 {
            self.pending.push(run);
            self.deferred_total += 1;
        }
    }

    /// Where the deferred frees stand — what a batch ending now covers.
    #[must_use]
    pub const fn mark(&self) -> u64 {
        self.deferred_total
    }

    /// Whether every free deferred before `mark` is a hole by now.
    #[must_use]
    pub const fn released(&self, mark: u64) -> bool {
        self.released_total >= mark
    }

    /// The batch that covered every free deferred before `mark` is
    /// durable: those frees become holes.
    pub fn release_to(&mut self, mark: u64) {
        let count = mark.saturating_sub(self.released_total) as usize;
        let count = count.min(self.pending.len());
        let released: Vec<Run> = self.pending.drain(..count).collect();
        self.released_total += count as u64;
        for run in released {
            self.free(run);
        }
    }

    /// Cut every file's free tail off — the active file's too when
    /// `active` — and forget a file cut back to its own start, returning
    /// what to cut or delete. Only free blocks are in a tail: a run a
    /// recovery root names is held back, not freed.
    pub fn reclaim(&mut self, active: bool) -> Vec<Cut> {
        let open = if active { None } else { self.active() };
        let mut cuts = Vec::new();
        for (&file, space) in &mut self.files {
            if Some(file) == open {
                continue;
            }
            let was = space.end;
            let tail = space.free.iter().next_back().map(|(&s, &c)| (s, c));
            if let Some((start, count)) = tail
                && start + count == space.end
            {
                space.remove(start, count);
                space.end = start;
            }
            if space.end < was {
                cuts.push(Cut {
                    file,
                    end: space.end,
                    was,
                });
            }
        }
        for cut in cuts.iter().filter(|c| c.end == c.file) {
            self.files.remove(&cut.file);
        }
        cuts
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockAllocator, Cut};
    use crate::block::{BLOCK_SIZE, Run};

    /// Files of `blocks` blocks.
    fn alloc(blocks: u64) -> BlockAllocator {
        BlockAllocator::new(blocks * BLOCK_SIZE as u64)
    }

    #[test]
    fn runs_append_to_the_active_file() {
        let mut a = alloc(100);
        assert_eq!(a.alloc(3), Run::new(0, 3));
        assert_eq!(a.alloc(2), Run::new(3, 2));
        assert_eq!(a.total_blocks(), 5);
        assert_eq!(a.free_blocks(), 0);
        assert_eq!(a.file_of(4), Some(0));
    }

    /// A freed run is a hole, never handed out again.
    #[test]
    fn a_hole_is_never_reused() {
        let mut a = alloc(100);
        let r = a.alloc(4);
        a.alloc(1);
        a.free(r);
        assert_eq!(a.free_blocks(), 4);
        assert_eq!(a.alloc(2), Run::new(5, 2));
    }

    /// The ceiling bounds where a run starts: the run that reaches it lands
    /// whole, and the next opens a file named after where it ended.
    #[test]
    fn a_full_file_rolls_over_where_it_ended() {
        let mut a = alloc(10);
        a.alloc(8);
        assert_eq!(a.alloc(5), Run::new(8, 5), "never split");
        assert_eq!(a.alloc(1), Run::new(13, 1));
        assert_eq!(a.file_of(12), Some(0));
        assert_eq!(a.file_of(13), Some(13));
        assert_eq!(a.active(), Some(13));
    }

    #[test]
    fn a_rolled_file_takes_no_more_runs() {
        let mut a = alloc(100);
        a.alloc(3);
        a.roll();
        assert_eq!(a.active(), None);
        assert_eq!(a.alloc(1), Run::new(3, 1));
        assert_eq!(a.file_of(3), Some(3));
    }

    #[test]
    fn frees_coalesce_within_a_file_only() {
        let mut a = alloc(4);
        let (x, y) = (a.alloc(4), a.alloc(4)); // two files, [0,4) and [4,8)
        a.free(x);
        a.free(y);
        let files = a.files();
        assert_eq!(files.len(), 2);
        assert!(files.iter().all(|f| f.free == 4), "{files:?}");
    }

    #[test]
    fn reclaim_cuts_free_tails_and_deletes_empty_files() {
        let mut a = alloc(4);
        let (x0, x1) = (a.alloc(2), a.alloc(2)); // file 0: [0,4)
        let y = a.alloc(4); // file 4: [4,8)
        a.alloc(1); // file 8, active
        a.free(x1);
        a.free(y);
        let cuts = a.reclaim(false);
        assert_eq!(
            cuts,
            vec![
                Cut {
                    file: 0,
                    end: 2,
                    was: 4
                },
                Cut {
                    file: 4,
                    end: 4,
                    was: 8
                }
            ]
        );
        assert_eq!(a.files().len(), 2, "file 4 is gone");
        a.free(x0);
        assert_eq!(
            a.reclaim(false),
            vec![Cut {
                file: 0,
                end: 0,
                was: 2
            }]
        );
        assert_eq!(a.alloc(1), Run::new(9, 1), "the active file goes on");
    }

    /// The active file keeps its tail while it takes runs.
    #[test]
    fn reclaim_spares_the_active_file_unless_asked() {
        let mut a = alloc(100);
        a.alloc(1);
        let r = a.alloc(3);
        a.free(r);
        assert_eq!(a.reclaim(false), Vec::new());
        assert_eq!(
            a.reclaim(true),
            vec![Cut {
                file: 0,
                end: 1,
                was: 4
            }]
        );
        assert_eq!(a.alloc(1), Run::new(1, 1), "nothing durable named them");
    }

    /// A deferred free counts as deferred, not free, until its batch is
    /// durable — and a reclaim does not cut it meanwhile.
    #[test]
    fn deferred_frees_wait_for_their_release() {
        let mut a = alloc(100);
        let (x, y) = (a.alloc(2), a.alloc(3));
        a.alloc(1);
        a.defer(x);
        let first = a.mark();
        a.defer(y);
        assert_eq!((a.free_blocks(), a.deferred_blocks()), (0, 5));
        a.release_to(first);
        assert_eq!((a.free_blocks(), a.deferred_blocks()), (2, 3));
        a.release_to(first);
        assert_eq!(a.free_blocks(), 2, "a mark releases once");
        a.release_to(a.mark());
        assert_eq!((a.free_blocks(), a.deferred_blocks()), (5, 0));
    }

    #[test]
    fn from_layout_frees_the_gaps_per_file() {
        let a = BlockAllocator::from_layout(
            &[(0, 10), (10, 14)],
            &[Run::new(3, 2), Run::new(8, 1), Run::new(11, 1)],
            100 * BLOCK_SIZE as u64,
        );
        assert_eq!(a.total_blocks(), 14);
        // File 0: [0,3) [5,8) [9,10); file 10: [10,11) [12,14).
        let files = a.files();
        assert_eq!((files[0].free, files[1].free), (7, 3));
        let mut a = a;
        assert_eq!(a.alloc(1), Run::new(14, 1), "appends to the newest");
    }
}
