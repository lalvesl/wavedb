//! Reconstruction from the data files ([RFC 0072] §6–§7, [RFC 0073] §6):
//! the newest whole storage write's roots block names each data shard's
//! root, whose header roots a walk back through that data shard's headers;
//! every directory, dictionary and the allocator come out of it. Nothing is
//! replayed; the caches start empty.
//!
//! 1. **The roots block** a reconstruction roots in: the newest whose
//!    storage write is whole (`crate::roots_find`).
//! 2. **The walk** (`crate::walk`), per data shard ([`rebuild`]), rebuilds
//!    its directories — and in disk + object mode the object store's, from
//!    the same headers — reading through its own handles.
//! 3. **The allocator** is the files' layout, every run the data shards'
//!    directories and dictionaries name, every header their walks read and
//!    the roots block; the rest is holes, and the tail past the root — what
//!    a crash left — is cut.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards-WIP.md

use std::collections::{BTreeMap, BTreeSet};

use futures::FutureExt;

use crate::block::{BlockAllocator, BlockDescriptor, Run};
use crate::block_file::BlockFile;
use crate::dictionary::Dictionary;
use crate::directory::Directory;
use crate::disk_chain::DiskChain;
use crate::error::{StorageError, StorageResult};
use crate::header::{ChainRecord, Header, Rotation};
use crate::remote::HEADER_BLOCKS;
use crate::roots::ROOTS_BLOCKS;
use crate::roots_find::find_roots;
use crate::struct_storage::TypeSlot;
use crate::walk::{HeaderSource, Lane, walk};

/// What a reconstruction rebuilt besides the directories, which it loads
/// into the slots.
pub struct Reopened {
    pub alloc: BlockAllocator,
    pub chain: DiskChain,
    /// Where the object store's chain stood, in disk + object mode.
    pub remote_chain: Option<ChainRecord>,
}

/// What one data shard's walk rebuilt besides its directories.
#[derive(Default)]
pub struct Rebuilt {
    pub chain: DiskChain,
    /// The runs it names: pages, dictionaries, headers.
    pub used: Vec<Run>,
    pub remote_chain: Option<ChainRecord>,
}

/// The headers read from the data files, by where they start.
pub struct DiskHeaders<'a> {
    pub file: &'a BlockFile,
    seen: BTreeMap<u64, Option<Header>>,
    lowest: u64,
}

impl<'a> DiskHeaders<'a> {
    /// None read yet from `file`.
    pub const fn new(file: &'a BlockFile) -> Self {
        Self {
            file,
            seen: BTreeMap::new(),
            lowest: u64::MAX,
        }
    }

    /// The header at block `at`, if the 8 blocks there hold one that says
    /// it is there.
    pub fn read(&mut self, at: u64) -> Option<Header> {
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

/// Rebuild the store writing its own files in `file` into `types` (see the
/// module docs): the one data shard of its storage shard.
///
/// # Errors
/// As [`rebuild`].
pub fn reopen(
    file: &BlockFile,
    types: &[TypeSlot],
    file_bytes: u64,
    object_backed: bool,
) -> StorageResult<Reopened> {
    let mut headers = DiskHeaders::new(file);
    let found = find_roots(&mut headers, 1)?;
    let root = found
        .as_ref()
        .and_then(|r| r.root(0))
        .and_then(|at| headers.read(at));
    let mut rebuilt = rebuild(&mut headers, types, object_backed, root)?;
    if let Some(roots) = found {
        rebuilt.used.push(Run::new(roots.at, ROOTS_BLOCKS));
        rebuilt.chain.roots.newest = Some(roots.at);
        rebuilt.chain.roots.live = BTreeSet::from([roots.at]);
    }
    Ok(Reopened {
        alloc: cut_tails(file, &rebuilt.used, file_bytes)?,
        chain: rebuilt.chain,
        remote_chain: rebuilt.remote_chain,
    })
}

/// Rebuild one data shard into `types` by the walk from its `root` — the
/// header ending its newest whole batch, if it ever ended one.
///
/// # Errors
/// Read faults; [`StorageError::Corrupt`] when a header a type's chain
/// names is missing; [`StorageError::UnregisteredStructHash`] when the
/// headers name a type this open does not list.
pub fn rebuild(
    headers: &mut DiskHeaders<'_>,
    types: &[TypeSlot],
    object_backed: bool,
    root: Option<Header>,
) -> StorageResult<Rebuilt> {
    let Some(root) = root else {
        return Ok(Rebuilt::default()); // nothing durable
    };
    let file = headers.file;
    // The root itself, read where it was found, is one of the headers kept.
    headers.seen.insert(root.at, Some(root.clone()));
    headers.lowest = headers.lowest.min(root.at);
    let registered: Vec<u64> =
        types.iter().map(TypeSlot::struct_hash).collect();
    let disk = on_disk(walk(headers, &root, &registered, Lane::Types))?;
    let remote = if object_backed {
        on_disk(walk(headers, &root, &registered, Lane::Remote))?
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
    Ok(Rebuilt {
        chain,
        used,
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

/// The allocator over the files' layout and the `used` runs, every free tail
/// cut off — the active file's too: past the root is what a crash left —
/// and the files left empty deleted.
pub fn cut_tails(
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
