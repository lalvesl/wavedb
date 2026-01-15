//! Journal commit + recovery — the journal's crc framing is the engine's
//! **only** atomicity mechanism (user-directed design, 2026-07-07).
//!
//! ## Commit ( [`PageStore::commit_journal`](crate::page_store::PageStore::commit_journal), stepped by `crate::commit_steps` )
//!
//! ```text
//! 1. rotate: create journal_<ts+1>.log, swap under the append lock (µs —
//!    writers redirect, no settle work under the lock)
//! 2. dispose of the PREVIOUS checkpoint's generation: the journal just
//!    rotated out carries its `Commit` frame, so this is where that frame is
//!    known durable — delete the journal it retired, roll the protected set
//!    (RFC 0047). No barrier: an ordinary `Batch` append already flushed it
//! 3. settle what the old journal holds into pages — one window write per
//!    round (RFC 0041), each carrying its descriptor changes (RFC 0046),
//!    in as many bounded rounds as the caller steps it — then fsync
//!    `data.bin`
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
use crate::struct_storage::TypeSlot;

/// What [`restore`] recovered: the allocator and the batches still to
/// replay (in order), plus the journal to keep appending to and the
/// addressing log to keep extending.
pub struct Recovered {
    pub alloc: BlockAllocator,
    pub journal: Journal,
    pub replay: Vec<Vec<Write>>,
    pub meta: MetaLog,
    /// Where the next upload writes in the object store (RFC 0070).
    pub remote_end: u64,
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
            remote_end: 0,
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

    let ((alloc, meta, remote_end), covered_ts) =
        if let Some(commit) = &newest_commit {
            (load_commit(file, types, commit)?, commit.journal_ts)
        } else {
            // First generation (never rotated): rebuild pages from scratch.
            ((fresh_alloc(file)?, MetaLog::default(), 0), 0)
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
        remote_end,
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
) -> StorageResult<(BlockAllocator, MetaLog, u64)> {
    let mut used = vec![Run::new(0, RESERVED_BLOCKS)];
    let mut replay = Replay::default();
    let chain = walk(file, BlockDescriptor::from_raw(commit.head))?;
    for desc in &chain {
        replay.apply(&read_chunk(file, desc.run())?)?;
        used.push(desc.run());
    }

    let parts = replay.into_parts();
    for (hash, addresses) in parts.remote {
        // Remote descriptors name the object store's blocks, so none of
        // them enters the disk allocator.
        let slot = slot_of(types, hash)?;
        let descriptors: Vec<BlockDescriptor> = addresses
            .into_iter()
            .map(BlockDescriptor::from_raw)
            .collect();
        if !descriptors.is_empty() {
            *slot.remote_directory().borrow_mut() =
                Some(Directory::from_slots(descriptors, file.seed()));
        }
    }
    for (hash, addresses) in parts.slots {
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
        *slot.directory().borrow_mut() =
            Some(Directory::from_slots(descriptors, file.seed()));
    }
    for (hash, desc_raw) in parts.dicts {
        let slot = slot_of(types, hash)?;
        let desc = BlockDescriptor::from_raw(desc_raw);
        if !desc.is_allocated() {
            continue;
        }
        used.push(desc.run());
        let dict = Dictionary::from_bytes(&file.read_run(desc.run())?)?;
        slot.dictionary().borrow_mut().load(dict, desc);
    }
    let mut alloc = BlockAllocator::from_layout(file.len_blocks()?, &used);
    alloc.set_protected(&used);
    Ok((alloc, MetaLog::restored(chain), parts.remote_end))
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
