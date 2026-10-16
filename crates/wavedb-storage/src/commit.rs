//! Journal commit + recovery — the journal's crc framing is the engine's
//! **only** atomicity mechanism (user-directed design, 2026-07-07).
//!
//! ## Commit ( [`PageStore::commit_journal`] )
//!
//! ```text
//! 1. rotate: create journal_<ts+1>.log, swap under the append lock (µs —
//!    writers redirect, no settle work under the lock)
//! 2. dispose of the PREVIOUS checkpoint's generation: the journal just
//!    rotated out carries its `Commit` frame, so this is where that frame is
//!    known durable — delete the journal it retired, roll the protected set
//!    (RFC 0047). No barrier: an ordinary `Batch` append already flushed it
//! 3. drain: settle everything queued into pages — ONE window write
//!    (RFC 0041), carrying that round's descriptor changes with it
//!    (RFC 0046) — then fsync `data.bin`
//! 4. append ONE `Commit { done: old_ts, head }` frame to the NEW journal —
//!    **no fsync**. The frame is a *pointer* into the addressing log the
//!    windows already wrote, naming only its newest chunk since each chunk
//!    names the one before it (RFC 0048), so it is a fixed 16 bytes however
//!    long the log is — and the next `Batch` append's barrier carries it
//!    (RFC 0046)
//! 5. hold this round's own retirement for the next checkpoint's step 2
//! ```
//!
//! A checkpoint therefore costs ONE barrier — the `data.bin` sync in step 3 —
//! and no barrier is paid anywhere else on its behalf.
//!
//! ## Recovery ( [`restore`] )
//!
//! Scan `journal_*.log` sorted by timestamp; the **newest decodable
//! `Commit`** is the base: the chain is walked back from the head it names
//! ([`crate::edit::walk`]) and folded on in write order
//! ([`crate::edit::Replay`]) to rebuild every
//! directory and dictionary run, the allocator derives from those runs plus
//! the log's own, journals it covers (`ts <= journal_ts`) are skipped (and
//! cleaned up), and every `Batch` in the remaining journals replays through
//! the normal commit + settle path (re-settling is idempotent). A torn
//! `Commit` frame is invisible — the retired journal is still on disk and
//! rules. No `Commit` anywhere ⇒ first-generation full rebuild.

use std::path::Path;

use wavedb_core::Write;

use crate::alloc::BlockAllocator;
use crate::block::{BlockDescriptor, Run};
use crate::block_file::{BlockFile, RESERVED_BLOCKS};
use crate::dictionary::Dictionary;
use crate::directory::Directory;
use crate::edit::{Replay, read_chunk, walk};
use crate::error::{StorageError, StorageResult};
use crate::journal::{self, CommitFrame, Journal, JournalFrame};
use crate::meta_log::MetaLog;
use crate::page_store::PageStore;
use crate::retire::Retiring;
use crate::struct_storage::TypeSlot;

impl PageStore {
    /// Retire the current journal: rotate, settle everything it covers, then
    /// append the atomic `Commit` frame — naming the addressing log and the
    /// retired journal's DONE marker — to the new journal.
    ///
    /// Costs **one** barrier (the `data.bin` sync). The frame itself is
    /// written unsynced, so on return the retirement is *pending*: the old
    /// journal stays on disk, and recovery still roots in it, until an ordinary
    /// append makes the frame durable — which the **next** checkpoint verifies
    /// and acts on (`crate::retire`). Two journals on disk is the steady state;
    /// [`force_retirement`](Self::force_retirement) closes the last one out at
    /// shutdown, where there is no next checkpoint.
    ///
    /// # Errors
    /// A write/sync fault. Until the `Commit` frame is durable, the old
    /// journal (and the previous commit) still rule — nothing acked is at
    /// risk.
    // The alloc/dir guards span the snapshot loop by design: the frame must
    // name exactly the state that was just synced, with no write landing in
    // between.
    #[allow(clippy::significant_drop_tightening)]
    pub fn commit_journal(&self) -> StorageResult<()> {
        // 0. Claim the previous checkpoint's pending retirement *before*
        // rotating. Between the rotation and step 2 the invariant
        // `force_retirement` relies on — the pending frame lives in the
        // *current* journal — is false, so the record must not be observable
        // there: it would sync the wrong file and delete a journal whose frame
        // is not durable. A fault before step 2 drops the record instead, which
        // only leaves a covered journal for recovery to clean.
        let pending = self.retiring.lock().take();

        // 1. Rotate under the append lock — writers redirect immediately.
        let mut old = {
            let mut journal = self.journal.lock();
            let fresh = Journal::create(
                &self.data_dir,
                journal::next_ts(journal.ts()),
            )?;
            std::mem::replace(&mut *journal, fresh)
        };

        // 2. The journal just rotated out is the one carrying the previous
        // checkpoint's `Commit` frame — so this is where that generation is
        // disposed of (RFC 0047): its journal deleted, its protected set rolled
        // in. Before the drain, so the frees it releases are available to this
        // round's window.
        self.retire_previous(pending, &mut old)?;

        // 3. Everything the old journal holds settles into pages — one
        // planned window, one positioned write (RFC 0041). Writes landing in
        // the new journal may settle too: harmless, their journal survives
        // and re-settling converges.
        self.drain()?;

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
        let head = meta.head();
        let frame_barrier = {
            let mut journal = self.journal.lock();
            journal.append_deferred(&JournalFrame::Commit(CommitFrame {
                journal_ts: old.ts(),
                head,
            }))?;
            // Under the same guard as the append — see `Retiring`.
            journal.barriers()
        };

        // 5. Deleting the retired journal and dropping the older generation's
        // protection are what the frame *authorises*, so both wait for it to be
        // durable (`crate::retire`) — which the next checkpoint checks at its
        // step 2. Until then the old journal still rules.
        *self.retiring.lock() = Some(Retiring {
            journal: old,
            frame_barrier,
        });
        Ok(())
    }
}

/// What [`restore`] recovered: the allocator and the batches still to
/// replay (in order), plus the journal to keep appending to and the
/// addressing log to keep extending.
pub struct Recovered {
    pub alloc: BlockAllocator,
    pub journal: Journal,
    pub replay: Vec<Vec<Write>>,
    pub meta: MetaLog,
}

/// Recover engine state from the journals under `dir` (see module docs).
/// The slots must already be reset; on return the recovered directories and
/// dictionaries are loaded into them, caches empty.
///
/// # Errors
/// [`StorageError::Corrupt`] on a lost recovery root (`data.bin` with no
/// journal) or an unreadable addressing log; [`StorageError::UnregisteredStructHash`]
/// when a commit names a type this open's registry does not list.
pub fn restore(
    dir: &Path,
    data_bin_existed: bool,
    file: &BlockFile,
    types: &[TypeSlot],
) -> StorageResult<Recovered> {
    let found = journal::scan(dir)?;
    journal::require_journal_for(data_bin_existed, &found)?;

    // Fresh database: first journal, empty everything.
    if found.is_empty() {
        let journal = Journal::create(dir, journal::next_ts(0))?;
        return Ok(Recovered {
            alloc: fresh_alloc(file)?,
            journal,
            replay: Vec::new(),
            meta: MetaLog::default(),
        });
    }

    // Replay every journal present; remember the newest decodable Commit.
    let mut journals = Vec::with_capacity(found.len());
    let mut newest_commit: Option<CommitFrame> = None;
    for (ts, path) in &found {
        let mut journal = Journal::open(path, *ts)?;
        let frames = journal.replay()?;
        for frame in &frames {
            if let JournalFrame::Commit(c) = frame {
                newest_commit = Some(c.clone());
            }
        }
        journals.push((*ts, journal, frames));
    }

    let ((alloc, meta), covered_ts) = if let Some(commit) = &newest_commit {
        (load_commit(file, types, commit)?, commit.journal_ts)
    } else {
        // First generation (never rotated): rebuild pages from scratch.
        ((fresh_alloc(file)?, MetaLog::default()), 0)
    };

    // Batches from journals the commit does not cover, oldest first. The
    // covered ones only linger after a crash between commit and delete —
    // clean them up now.
    let mut replay = Vec::new();
    let mut current = None;
    for (ts, journal, frames) in journals {
        if newest_commit.is_some() && ts <= covered_ts {
            journal.delete()?;
            continue;
        }
        for frame in frames {
            if let JournalFrame::Batch(batch) = frame {
                replay.push(batch);
            }
        }
        current = Some(journal); // the newest survives the loop
    }
    let journal = match current {
        Some(j) => j,
        // Every journal was covered and deleted — start a fresh one.
        None => Journal::create(dir, journal::next_ts(covered_ts))?,
    };
    Ok(Recovered {
        alloc,
        journal,
        replay,
        meta,
    })
}

/// Replay the addressing log a `Commit` names into the slots, and derive the
/// allocator from everything it references.
///
/// The frame names only the newest chunk, so the log is discovered by walking
/// `prev` back to the snapshot ([RFC 0048]); the chunks are then folded on in
/// write order — the reverse of the walk.
///
/// [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md
fn load_commit(
    file: &BlockFile,
    types: &[TypeSlot],
    commit: &CommitFrame,
) -> StorageResult<(BlockAllocator, MetaLog)> {
    let mut used = vec![Run::new(0, RESERVED_BLOCKS)];
    let mut replay = Replay::default();
    let chain = walk(file, BlockDescriptor::from_raw(commit.head))?;
    for desc in &chain {
        replay.apply(&read_chunk(file, desc.run())?)?;
        used.push(desc.run());
    }

    let (slots, dicts) = replay.into_parts();
    for (hash, addresses) in slots {
        let slot = slot_of(types, hash)?;
        if addresses.is_empty() {
            continue; // the type never settled anything
        }
        let descriptors: Vec<BlockDescriptor> = addresses
            .iter()
            .map(|&a| BlockDescriptor::from_raw(a))
            .collect();
        used.extend(
            descriptors
                .iter()
                .filter(|d| d.is_allocated())
                .map(|d| d.run()),
        );
        *slot.directory().lock() =
            Some(Directory::from_slots(descriptors, file.seed()));
    }
    for (hash, desc_raw) in dicts {
        let slot = slot_of(types, hash)?;
        let desc = BlockDescriptor::from_raw(desc_raw);
        if !desc.is_allocated() {
            continue;
        }
        used.push(desc.run());
        let dict = Dictionary::from_bytes(&file.read_run(desc.run())?)?;
        slot.dictionary().lock().load(dict, desc);
    }
    let mut alloc = BlockAllocator::from_layout(file.len_blocks()?, &used);
    alloc.set_protected(&used);
    Ok((alloc, MetaLog::restored(chain)))
}

/// The registered slot for `hash` — a commit naming an unlisted type
/// refuses, like every other allowlist edge.
fn slot_of(types: &[TypeSlot], hash: u64) -> StorageResult<&TypeSlot> {
    types
        .binary_search_by_key(&hash, TypeSlot::struct_hash)
        .ok()
        .and_then(|i| types.get(i))
        .ok_or(StorageError::UnregisteredStructHash(hash))
}

/// An allocator over a page-less `data.bin` (superblock only).
fn fresh_alloc(file: &BlockFile) -> StorageResult<BlockAllocator> {
    file.truncate_to_blocks(RESERVED_BLOCKS)?;
    let mut alloc = BlockAllocator::new();
    alloc.alloc(RESERVED_BLOCKS);
    Ok(alloc)
}
