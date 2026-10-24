//! A checkpoint, in steps ([RFC 0063] part 3, [RFC 0067] step 3).
//!
//! The sequence is the one `crate::commit` documents — rotate, dispose of
//! the previous generation, settle, sync `data.bin`, append the `Commit`
//! frame — but the settle in the middle is no longer one `drain`. A
//! checkpoint is **opened** ([`PageStore::begin_checkpoint`]: claim, rotate,
//! retire — bounded) and then **stepped** ([`PageStore::checkpoint_step`]):
//! each step settles one bounded round of what the open checkpoint owes, and
//! the step that finds nothing owed **closes** it (sync, frame — one
//! barrier). Between two steps the caller may do anything: serve a read,
//! take a write, step another shard.
//!
//! ## Why that is legal
//!
//! The frame says "every batch in the rotated-out journal is in the pages
//! this frame addresses". Those batches are exactly the ids queued at the
//! rotation, which [`Pending::rotate`](crate::pending::Pending::rotate) set
//! aside as **owed**; writes after it commit into the new journal and queue
//! as fresh, which the frame does not claim. So a step boundary is a round
//! boundary — the one yield point RFC 0063 Part 2 declares legal — and the
//! checkpoint closes only once no owed id is queued *or still being written*.
//!
//! A crash while a checkpoint is open is the crash today's checkpoint could
//! already suffer mid-drain: no frame names the old journal, so recovery
//! replays it.
//!
//! [RFC 0063]: ../../../rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md

use crate::block::Run;
use crate::block_file::RESERVED_BLOCKS;
use crate::error::StorageResult;
use crate::journal::{self, CommitFrame, Journal, JournalFrame};
use crate::page_store::PageStore;
use crate::retire::Retiring;

/// What one [`checkpoint_step`](PageStore::checkpoint_step) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Checkpointing {
    /// A round of the open checkpoint was settled; it still owes more, or its
    /// close is next.
    Settling,
    /// The checkpoint closed — its `Commit` frame is appended — or none was
    /// open.
    Done,
}

impl PageStore {
    /// Retire the current journal as **one** uninterrupted checkpoint: rotate,
    /// settle everything it covers, then append the atomic `Commit` frame —
    /// naming the addressing log and the retired journal's DONE marker — to
    /// the new journal.
    ///
    /// A checkpoint already open (a stepped one) is closed first, and a new
    /// one is then run whole, so on return the journal that was live at the
    /// call is covered — which is what a shutdown asks for.
    ///
    /// Costs **one** barrier per checkpoint (the `data.bin` sync). The frame
    /// is written unsynced, so on return the retirement is *pending*: the old
    /// journal stays on disk, and recovery still roots in it, until an
    /// ordinary append makes the frame durable — which the **next** checkpoint
    /// verifies and acts on (`crate::retire`). Two journals on disk is the
    /// steady state; [`force_retirement`](Self::force_retirement) closes the
    /// last one out at shutdown, where there is no next checkpoint.
    ///
    /// # Errors
    /// A write/sync fault. Until the `Commit` frame is durable, the old
    /// journal (and the previous commit) still rule — nothing acked is at
    /// risk.
    pub fn commit_journal(&self) -> StorageResult<()> {
        while self.checkpoint_step(usize::MAX)? == Checkpointing::Settling {}
        self.begin_checkpoint()?;
        while self.checkpoint_step(usize::MAX)? == Checkpointing::Settling {}
        Ok(())
    }

    /// Whether a checkpoint is open: rotated, not yet framed.
    #[must_use]
    pub fn is_checkpointing(&self) -> bool {
        self.checkpoint.lock().is_some()
    }

    /// Open a checkpoint — steps 0–2 of `crate::commit`: claim the previous
    /// checkpoint's pending retirement, rotate the journal (setting aside
    /// what it owes), and dispose of the previous generation. Bounded: one
    /// file created, at most one sync and one unlink.
    ///
    /// Answers `false`, doing nothing, when one is already open.
    ///
    /// # Errors
    /// A create, sync or unlink fault. A fault after the rotation leaves the
    /// checkpoint open, to be stepped and closed as usual.
    // The checkpoint guard spans the whole open on purpose: two callers must
    // not both see "none open" and rotate twice.
    #[allow(clippy::significant_drop_tightening)]
    pub fn begin_checkpoint(&self) -> StorageResult<bool> {
        let mut open = self.checkpoint.lock();
        if open.is_some() {
            return Ok(false);
        }
        // 0. Claim the previous checkpoint's pending retirement *before*
        // rotating. Between the rotation and step 2 the invariant
        // `force_retirement` relies on — the pending frame lives in the
        // *current* journal — is false, so the record must not be observable
        // there: it would sync the wrong file and delete a journal whose frame
        // is not durable. A fault before step 2 drops the record instead, which
        // only leaves a covered journal for recovery to clean.
        let pending = self.retiring.lock().take();

        // 1. Rotate under the append lock — writers redirect immediately —
        // and, under the same lock, set aside what the old journal holds:
        // the commit path queues under this lock too, so the owed set is
        // exactly the old journal's ids.
        let mut old = {
            let mut journal = self.journal.lock();
            let fresh = Journal::create(
                &self.data_dir,
                journal::next_ts(journal.ts()),
            )?;
            self.pending.lock().rotate();
            std::mem::replace(&mut *journal, fresh)
        };

        // 2. The journal just rotated out carries the previous checkpoint's
        // `Commit` frame — so this is where that generation is disposed of
        // (RFC 0047). Before the settle, so the frees it releases are
        // available to this checkpoint's windows.
        let retired = self.retire_previous(pending, &mut old);
        *open = Some(old);
        retired.map(|()| true)
    }

    /// One step of the open checkpoint: settle a round of at most `budget`
    /// owed ids, or — when nothing is owed — close it (sync `data.bin`,
    /// append the frame). [`Checkpointing::Done`] when it closed, or when
    /// none was open.
    ///
    /// # Errors
    /// A page write or sync fault. The round goes back to the owed queue and
    /// the checkpoint stays open; the next step retries.
    // Held for the whole step on purpose: the open checkpoint is outermost in
    // the lock order, and a step that let go of it mid-round could see it
    // closed under its feet.
    #[allow(clippy::significant_drop_tightening)]
    pub fn checkpoint_step(
        &self,
        budget: usize,
    ) -> StorageResult<Checkpointing> {
        let mut open = self.checkpoint.lock();
        let Some(old) = open.as_ref() else {
            return Ok(Checkpointing::Done);
        };
        if self.pending.lock().owes() {
            // A round drawn while anything is owed comes from the owed half.
            self.settle_step(budget)?;
            return Ok(Checkpointing::Settling);
        }
        let journal_ts = old.ts();
        let frame_barrier = self.close_checkpoint(journal_ts)?;
        if let Some(journal) = open.take() {
            *self.retiring.lock() = Some(Retiring {
                journal,
                frame_barrier,
            });
        }
        Ok(Checkpointing::Done)
    }

    /// Steps 3–4: sync what the settle wrote, then frame it. Returns the
    /// carrying journal's barrier count, read under the append.
    // The alloc/dir guards span the snapshot loop by design: the frame must
    // name exactly the state that was just synced, with no write landing in
    // between.
    #[allow(clippy::significant_drop_tightening)]
    fn close_checkpoint(&self, journal_ts: u64) -> StorageResult<u64> {
        // Every run the frame will keep reachable — the next protected set.
        // The allocator guard spans the snapshot so no window can land between
        // reading this state and syncing it.
        let mut alloc = self.alloc.lock();
        let meta = self.meta.lock();
        let mut used = vec![Run::new(0, RESERVED_BLOCKS)];
        for slot in &self.types {
            let dir_guard = slot.directory().lock();
            if let Some(dir) = dir_guard.as_ref() {
                used.extend(
                    dir.slots()
                        .iter()
                        .filter(|d| d.is_allocated())
                        .map(|d| d.run()),
                );
            }
            let dict = slot.dictionary().lock();
            if dict.descriptor().is_allocated() {
                used.push(dict.descriptor().run());
            }
        }
        used.extend(meta.runs()); // the addressing log itself
        // Protect it *now*, under this guard: from here on the frame may become
        // the recovery root at any moment, so no settle round may free into
        // this state. The previous checkpoint's set stays protected alongside
        // until step 2 of the next checkpoint proves this frame durable — until
        // then either can be the root a crash reopens from (RFC 0047).
        alloc.set_protected(&used);
        // The pages — and the delta chunks describing them — must be durable
        // before the frame addressing them.
        self.file.sync()?;

        // 4. The atomic commit: where the addressing log lives, and the DONE
        // marker, in ONE crc-framed append — written **without** a barrier.
        // The frame only points at state the sync above already made durable,
        // so the next `Batch` append's fsync carries it for free.
        let mut journal = self.journal.lock();
        journal.append_deferred(&JournalFrame::Commit(CommitFrame {
            journal_ts,
            head: meta.head(),
        }))?;
        // Under the same guard as the append — see `Retiring`.
        Ok(journal.barrier_floor())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};

    use super::Checkpointing;
    use crate::journal;
    use crate::page_store::PageStore;
    use crate::struct_storage::StructStorage;

    const SH: u64 = 0x5EED_C0DE_0067_0003;
    static SLOT: StructStorage = StructStorage::new(SH);

    fn open(dir: &Path) -> PageStore {
        PageStore::open(dir, &[&SLOT]).unwrap()
    }

    fn id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, (key & 0x7FFF) as u16)
    }

    fn put(s: &PageStore, keys: std::ops::Range<u64>) {
        block_on(async {
            for k in keys {
                let mut bytes = SH.to_le_bytes().to_vec();
                bytes.extend_from_slice(&k.to_le_bytes());
                s.apply(&[Write::Put(id(k), bytes)]).await.unwrap();
            }
        });
    }

    fn assert_all(s: &PageStore, keys: std::ops::Range<u64>) {
        block_on(async {
            for k in keys {
                let got = s.get_of(SH, id(k)).await.unwrap();
                let got = got.unwrap_or_else(|| panic!("record {k} is gone"));
                assert_eq!(got[8..], k.to_le_bytes(), "record {k}");
            }
        });
    }

    /// Step to the close, counting the rounds it took.
    fn step_out(s: &PageStore, budget: usize) -> usize {
        let mut rounds = 0;
        while s.checkpoint_step(budget).unwrap() == Checkpointing::Settling {
            rounds += 1;
        }
        rounds
    }

    /// The point of the module: a checkpoint is many bounded rounds, not one
    /// drain — and the frame it closes with still covers its journal, so a
    /// reopen roots in it and cleans the journal up.
    #[test]
    fn a_checkpoint_settles_in_bounded_rounds_and_covers_its_journal() {
        let d = tempfile::tempdir().unwrap();
        {
            let s = open(d.path());
            put(&s, 0..100);
            assert!(s.begin_checkpoint().unwrap());
            assert!(!s.begin_checkpoint().unwrap(), "one open at a time");
            assert!(s.is_checkpointing());
            let rounds = step_out(&s, 10);
            assert!(rounds >= 10, "100 ids at 10 a round, got {rounds}");
            assert!(!s.is_checkpointing());
            assert!(!s.has_pending());
        }
        let s = open(d.path());
        assert_all(&s, 0..100);
        let journals = journal::scan(d.path()).unwrap();
        assert_eq!(journals.len(), 1, "the covered journal was cleaned up");
    }

    /// Writes landing between steps commit into the new journal, which the
    /// frame does not claim — so a steady stream of them cannot keep the
    /// checkpoint from closing, and they survive a reopen by replay.
    #[test]
    fn writes_between_steps_are_not_owed() {
        let d = tempfile::tempdir().unwrap();
        {
            let s = open(d.path());
            put(&s, 0..50);
            s.begin_checkpoint().unwrap();
            let mut next = 1000;
            let mut rounds = 0;
            while s.checkpoint_step(5).unwrap() == Checkpointing::Settling {
                put(&s, next..next + 1);
                next += 1;
                rounds += 1;
                assert!(rounds <= 12, "the owed set is fixed at 50 ids");
            }
            assert!(s.has_pending(), "the fresh writes are still queued");
            assert_all(&s, 1000..next);
        }
        let s = open(d.path());
        assert_all(&s, 0..50);
    }

    /// A store dropped with a checkpoint open has no frame naming the old
    /// journal, so recovery replays it — the crash an unstepped checkpoint
    /// could already suffer mid-drain.
    #[test]
    fn a_store_closed_mid_checkpoint_replays_the_old_journal() {
        let d = tempfile::tempdir().unwrap();
        {
            let s = open(d.path());
            put(&s, 0..40);
            s.begin_checkpoint().unwrap();
            assert_eq!(s.checkpoint_step(10).unwrap(), Checkpointing::Settling);
        }
        let s = open(d.path());
        assert_all(&s, 0..40);
        s.commit_journal().unwrap();
        assert_all(&s, 0..40);
    }

    /// `commit_journal` is a whole checkpoint that starts at the call: an
    /// open one is closed first, then the journal live at the call is
    /// rotated and covered too — what a shutdown needs.
    #[test]
    fn commit_journal_closes_an_open_checkpoint_then_covers_the_live_one() {
        let d = tempfile::tempdir().unwrap();
        {
            let s = open(d.path());
            put(&s, 0..20);
            s.begin_checkpoint().unwrap();
            put(&s, 20..30);
            s.commit_journal().unwrap();
            assert!(!s.is_checkpointing());
            assert!(!s.has_pending(), "the live journal's writes settled too");
        }
        let s = open(d.path());
        assert_all(&s, 0..30);
        assert_eq!(journal::scan(d.path()).unwrap().len(), 1);
    }

    #[test]
    fn a_step_with_nothing_open_is_done() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        assert_eq!(s.checkpoint_step(10).unwrap(), Checkpointing::Done);
    }
}
