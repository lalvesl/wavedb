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
//! ## Checkpoint protection
//!
//! Runs a **recovery root** names must survive — a crash reopens *from* one,
//! so cutting or deleting a run it points at would corrupt the reopened
//! state. [`set_protected`](BlockAllocator::set_protected) registers those
//! runs; freeing one is **deferred** (held in a pending list) and released
//! once protection lifts. With no checkpoint (a fresh or rebuild-on-open
//! store) nothing is protected and frees are immediate.
//!
//! **Two generations, not one.** A checkpoint's `Commit` frame is written
//! without a barrier ([RFC 0046]), so until an ordinary write flushes it there
//! are *two* possible roots: the new frame and the one before it. Both sets are
//! therefore protected — `set_protected` installs the new one and keeps the old
//! as `previous`, which [`release_previous`](BlockAllocator::release_previous)
//! drops once the next checkpoint proves the frame durable ([RFC 0047]).
//!
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md
//! [RFC 0047]: ../../../rfcs/0047-generational-journal-retirement.md
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
    /// start → count of the newest checkpoint's runs (see module docs).
    protected: BTreeMap<u64, u64>,
    /// The checkpoint before it, retained while its successor's frame is
    /// unproven — the other root a crash could reopen from.
    previous: BTreeMap<u64, u64>,
    /// Frees deferred because the run was protected at the time.
    pending: Vec<Run>,
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
            protected: BTreeMap::new(),
            previous: BTreeMap::new(),
            pending: Vec::new(),
        }
    }

    /// Rebuild an allocator over existing data files — `(first block, end)`
    /// each — and the runs in use in them. Every gap between used runs is a
    /// hole. The used runs are **not** protected: call
    /// [`set_protected`](Self::set_protected) with the checkpoint's after.
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

    /// Blocks held back by deferred frees — runs a live generation still
    /// protects. Zero when no checkpoint is reachable (a fresh store), and the
    /// gauge that says protection is actually engaged.
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
    fn next_start(&self) -> u64 {
        let last = self.files.values().next_back().map_or(0, |f| f.end);
        self.next.max(last)
    }

    /// Return a run to its file's holes, coalescing with its neighbours. A
    /// run the last durable checkpoint still points at is **deferred**
    /// instead (released by the next
    /// [`set_protected`](Self::set_protected)).
    pub fn free(&mut self, run: Run) {
        if run.count == 0 {
            return;
        }
        if self.is_protected(run) {
            self.pending.push(run);
            return;
        }
        let file = self.file_of(run.start);
        if let Some(space) = file.and_then(|f| self.files.get_mut(&f)) {
            debug_assert!(run.end() <= space.end, "freeing past its file");
            space.coalesce(run);
        }
    }

    /// Install a checkpoint's runs as the newest protected set, demoting the
    /// one it supersedes to `previous` — which stays protected until
    /// [`release_previous`](Self::release_previous), because that older
    /// checkpoint is still a reachable recovery root (see module docs).
    pub fn set_protected(&mut self, runs: &[Run]) {
        let fresh = runs
            .iter()
            .filter(|r| r.count > 0)
            .map(|r| (r.start, r.count))
            .collect();
        self.previous = std::mem::replace(&mut self.protected, fresh);
        self.retry_pending();
    }

    /// Drop the older generation's protection and return whatever it alone was
    /// holding to the pool. Called once the newest checkpoint's `Commit` frame
    /// is known durable, which makes it the only reachable root.
    pub fn release_previous(&mut self) {
        self.previous.clear();
        self.retry_pending();
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

    /// Re-offer every deferred free — anything no longer protected returns to
    /// its holes.
    fn retry_pending(&mut self) {
        let pending = std::mem::take(&mut self.pending);
        for run in pending {
            self.free(run); // re-checks protection
        }
    }

    /// Whether `run` intersects a run either live generation protects.
    fn is_protected(&self, run: Run) -> bool {
        Self::covers(&self.protected, run) || Self::covers(&self.previous, run)
    }

    /// Whether one generation's set covers any block of `run`. Within a single
    /// set the runs are a snapshot of live allocations and so never overlap,
    /// which is what lets a single probe decide it.
    fn covers(set: &BTreeMap<u64, u64>, run: Run) -> bool {
        set.range(..run.end())
            .next_back()
            .is_some_and(|(&start, &count)| start + count > run.start)
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

    #[test]
    fn protected_frees_defer_until_released() {
        let mut a = alloc(100);
        let ckpt = a.alloc(2);
        let page = a.alloc(3);
        a.set_protected(&[ckpt]);
        a.free(ckpt);
        assert_eq!(a.free_blocks(), 0, "protected free must defer");
        assert_eq!(a.files()[0].deferred, 2);
        a.free(page);
        assert_eq!(a.free_blocks(), 3);
        // The next checkpoint only *demotes* the old set: still a root.
        a.set_protected(&[Run::new(5, 2)]);
        assert_eq!(a.free_blocks(), 3, "a demoted set still protects");
        a.release_previous();
        assert_eq!(a.free_blocks(), 5, "deferred free must release");
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
