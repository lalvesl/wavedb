//! Reconstruction from the data files ([RFC 0072] §6–§7): the newest whole
//! batch's header roots a walk back through the headers, and every
//! directory, dictionary and the allocator come out of it. Nothing is
//! replayed; the caches start empty.
//!
//! 1. **The newest header** is the last 8 blocks of the newest data file —
//!    a header whose crc holds and whose `at` is where it was read. After a
//!    crash the end may be torn: the blocks before it are read back, a
//!    chunk at a time, until one is.
//! 2. **The batch it belongs to** ends at the first header down its `prev`
//!    links that says `end_of_batch`. That batch is the root if it is whole —
//!    every header back to the one ending the batch before present — and
//!    every page and dictionary it placed is intact: one write reaches the
//!    disk in any order, so a crash before its barrier can keep a header and
//!    lose a page. Only the newest batch can be in that state (a batch opens
//!    only once the one before is durable); a batch that is not whole is
//!    passed over for the one before it, which is.
//! 3. **The walk** (`crate::walk`) rebuilds the directories — and in
//!    disk + object mode the object store's, from the same headers.
//! 4. **The allocator** is the files' layout, every run the directories and
//!    dictionaries name and every header the walk read; the rest is holes,
//!    and the tail past the root — what a crash left — is cut.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point-WIP.md

use std::collections::BTreeMap;

use futures::FutureExt;

use crate::block::{BLOCK_SIZE, BlockAllocator, BlockDescriptor, Run};
use crate::block_file::BlockFile;
use crate::dictionary::Dictionary;
use crate::directory::Directory;
use crate::disk_chain::DiskChain;
use crate::error::{StorageError, StorageResult};
use crate::header::{ChainRecord, Header, NO_FILE, Rotation};
use crate::page::SlotPage;
use crate::remote::HEADER_BLOCKS;
use crate::struct_storage::TypeSlot;
use crate::walk::{HeaderSource, Lane, walk};

/// Blocks read at a time while looking for a header behind a torn end.
const SCAN_BLOCKS: u64 = 256;

/// What a reconstruction rebuilt besides the directories, which it loads
/// into the slots.
pub struct Reopened {
    pub alloc: BlockAllocator,
    pub chain: DiskChain,
    /// Where the object store's chain stood, in disk + object mode.
    pub remote_chain: Option<ChainRecord>,
}

/// The headers read from the data files, by where they start.
struct DiskHeaders<'a> {
    file: &'a BlockFile,
    seen: BTreeMap<u64, Option<Header>>,
    lowest: u64,
}

impl DiskHeaders<'_> {
    /// The header at block `at`, if the 8 blocks there hold one that says
    /// it is there.
    fn read(&mut self, at: u64) -> Option<Header> {
        self.lowest = self.lowest.min(at);
        let file = self.file;
        self.seen
            .entry(at)
            .or_insert_with(|| {
                let bytes = file.read_run(Run::new(at, HEADER_BLOCKS)).ok()?;
                Header::decode(&bytes).ok().filter(|h| h.at == at)
            })
            .clone()
    }
}

impl HeaderSource for DiskHeaders<'_> {
    // The disk is read on the spot: the future is ready at once.
    fn get(
        &mut self,
        at: u64,
    ) -> impl Future<Output = StorageResult<Option<Header>>> {
        std::future::ready(Ok(self.read(at)))
    }
}

/// Rebuild the store over `file` into `types` (see the module docs).
///
/// # Errors
/// Read faults; [`StorageError::Corrupt`] when a header a type's chain
/// names is missing; [`StorageError::UnregisteredStructHash`] when the
/// headers name a type this open does not list.
pub fn reopen(
    file: &BlockFile,
    types: &[TypeSlot],
    file_bytes: u64,
    object_backed: bool,
) -> StorageResult<Reopened> {
    let mut headers = DiskHeaders {
        file,
        seen: BTreeMap::new(),
        lowest: u64::MAX,
    };
    let Some(root) = root(&mut headers)? else {
        // Nothing durable: every block is free, and cut.
        return Ok(Reopened {
            alloc: cut_tails(file, &[], file_bytes)?,
            chain: DiskChain::default(),
            remote_chain: None,
        });
    };
    // The root itself, read where it was found, is one of the headers kept.
    headers.seen.insert(root.at, Some(root.clone()));
    headers.lowest = headers.lowest.min(root.at);
    let registered: Vec<u64> =
        types.iter().map(TypeSlot::struct_hash).collect();
    let disk = on_disk(walk(&mut headers, &root, &registered, Lane::Types))?;
    let remote = if object_backed {
        on_disk(walk(&mut headers, &root, &registered, Lane::Remote))?
    } else {
        Vec::new()
    };
    let mut chain = DiskChain {
        tail: Some(root.at),
        last_end: Some(root.at),
        floor: headers.lowest,
        ..DiskChain::default()
    };
    let mut used = Vec::new();
    for w in disk {
        let slot = slot_of(types, w.struct_hash)?;
        chain.types.insert(w.struct_hash, rotation(w.newest));
        used.extend(
            w.slots.iter().filter(|d| d.is_allocated()).map(|d| d.run()),
        );
        if !w.slots.is_empty() {
            *slot.directory().borrow_mut() =
                Some(Directory::from_slots(w.slots, file.seed()));
        }
        if let Some(desc) = w.dict.filter(|d| d.is_allocated()) {
            used.push(desc.run());
            let dict = Dictionary::from_bytes(&file.read_run(desc.run())?)?;
            slot.dictionary().borrow_mut().load(dict, desc);
        }
    }
    for w in remote {
        let slot = slot_of(types, w.struct_hash)?;
        chain.remote.insert(w.struct_hash, rotation(w.newest));
        if !w.slots.is_empty() {
            *slot.remote_directory().borrow_mut() =
                Some(Directory::from_slots(w.slots, file.seed()));
        }
        slot.remote_dict()
            .set(w.dict.unwrap_or(BlockDescriptor::EMPTY));
    }
    for (&at, header) in &headers.seen {
        if header.is_some() && at <= root.at {
            used.push(Run::new(at, HEADER_BLOCKS));
            chain.live.insert(at);
        }
    }
    Ok(Reopened {
        alloc: cut_tails(file, &used, file_bytes)?,
        chain,
        remote_chain: root.chain,
    })
}

/// A type's rotation as an open starts it: named last in `newest`.
fn rotation(newest: u64) -> Rotation {
    Rotation {
        last: newest,
        ..Rotation::default()
    }
}

/// The header ending the newest whole batch, if any.
fn root(headers: &mut DiskHeaders<'_>) -> StorageResult<Option<Header>> {
    let mut limit = u64::MAX;
    loop {
        let Some(top) = newest_below(headers.file, limit)? else {
            return Ok(None);
        };
        let mut end = top;
        while !end.end_of_batch && end.prev != NO_FILE {
            match headers.read(end.prev) {
                Some(before) => end = before,
                None => break,
            }
        }
        if !end.end_of_batch {
            // Down to a header that is not there, or to the first one with
            // no batch ended: look below it.
            limit = end.at;
            continue;
        }
        match whole(headers, &end)? {
            Ok(()) => return Ok(Some(end)),
            Err(below) => limit = below,
        }
    }
}

/// Whether the batch `end` ends is whole and every page and dictionary it
/// placed intact — or where to look below for an older one.
fn whole(
    headers: &mut DiskHeaders<'_>,
    end: &Header,
) -> StorageResult<Result<(), u64>> {
    let mut batch = vec![end.clone()];
    let base = loop {
        let Some(last) = batch.last() else {
            return Ok(Err(end.at));
        };
        if last.prev == NO_FILE {
            break 0;
        }
        match headers.read(last.prev) {
            None => return Ok(Err(last.prev)),
            Some(before) if before.end_of_batch => {
                break before.at + HEADER_BLOCKS;
            }
            Some(before) => batch.push(before),
        }
    };
    // What the batch wrote lies between the header before it and its end.
    let placed = |raw: u64| {
        let desc = BlockDescriptor::from_raw(raw);
        (desc.is_allocated() && (base..end.at).contains(&desc.start()))
            .then(|| desc.run())
    };
    for header in &batch {
        for entry in &header.types {
            for &(_, raw) in &entry.changed {
                if let Some(run) = placed(raw)
                    && !SlotPage::intact(&headers.file.read_run(run)?)
                {
                    return Ok(Err(base));
                }
            }
            if let Some(run) = entry.dict.and_then(placed)
                && Dictionary::from_bytes(&headers.file.read_run(run)?).is_err()
            {
                return Ok(Err(base));
            }
        }
    }
    Ok(Ok(()))
}

/// The newest header whose 8 blocks end at or before block `limit`,
/// newest file first, read back a chunk at a time.
fn newest_below(file: &BlockFile, limit: u64) -> StorageResult<Option<Header>> {
    for (start, end) in file.layout().into_iter().rev() {
        let mut hi = end.min(limit);
        while hi >= start + HEADER_BLOCKS {
            let lo = hi.saturating_sub(SCAN_BLOCKS).max(start);
            let bytes = file.read_run(Run::new(lo, hi - lo))?;
            for at in (lo..=hi - HEADER_BLOCKS).rev() {
                let offset = ((at - lo) as usize) * BLOCK_SIZE;
                if let Ok(header) = Header::decode(&bytes[offset..])
                    && header.at == at
                {
                    return Ok(Some(header));
                }
            }
            if lo == start {
                break;
            }
            // The next positions are below `lo`; their headers reach past it.
            hi = lo + HEADER_BLOCKS - 1;
        }
    }
    Ok(None)
}

/// The allocator over the files' layout and the `used` runs, every free tail
/// cut off — the active file's too: past the root is what a crash left —
/// and the files left empty deleted.
fn cut_tails(
    file: &BlockFile,
    used: &[Run],
    file_bytes: u64,
) -> StorageResult<BlockAllocator> {
    let mut alloc =
        BlockAllocator::from_layout(&file.layout(), used, file_bytes);
    for cut in alloc.reclaim(true) {
        file.cut(cut.file, cut.end)?;
    }
    Ok(alloc)
}

/// The registered slot for `hash` — a header naming an unlisted type
/// refuses, like every other allowlist edge.
fn slot_of(types: &[TypeSlot], hash: u64) -> StorageResult<&TypeSlot> {
    types
        .binary_search_by_key(&hash, TypeSlot::struct_hash)
        .ok()
        .and_then(|i| types.get(i))
        .ok_or(StorageError::UnregisteredStructHash(hash))
}

/// Run a walk over the disk's headers, which never waits on anything: it is
/// done the first time it is polled — inside an executor or out of one.
fn on_disk<T>(
    walk: impl Future<Output = StorageResult<T>>,
) -> StorageResult<T> {
    walk.now_or_never()
        .unwrap_or(Err(StorageError::Corrupt("a walk over the disk waited")))
}
