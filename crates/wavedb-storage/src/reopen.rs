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
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::{BTreeMap, BTreeSet};

use futures::FutureExt;

use crate::block::{BLOCK_SIZE, BlockAllocator, BlockDescriptor, Run};
use crate::block_file::BlockFile;
use crate::dictionary::Dictionary;
use crate::disk_chain::DiskChain;
use crate::error::{StorageError, StorageResult};
use crate::header::{ChainRecord, Header, HeaderLink, Rotation};
use crate::ordered::Shape;
use crate::pages::disk_runs;
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

/// Blocks a walk reads back at a time once its headers lie close together
/// (1 MiB, [RFC 0079] §5a).
///
/// [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md
const CHUNK_BLOCKS: u64 = 256;

/// The headers read from the data files, by where they start, each with
/// the blocks read for it.
///
/// A walk goes down: a durable stream's windows lie end to end, its
/// headers a few blocks apart. Once a header is found within a chunk of
/// the one read before it, the read takes the chunk ending at it, and the
/// headers below it in that chunk need no read of their own. A sparse
/// walk reads each header alone, exactly its blocks.
pub struct DiskHeaders<'a> {
    pub file: &'a BlockFile,
    seen: BTreeMap<u64, (u16, Option<Header>)>,
    lowest: u64,
    /// The last chunk read, and where.
    chunk: Option<(Run, Vec<u8>)>,
    /// The last header read from the file.
    last: Option<u64>,
}

impl<'a> DiskHeaders<'a> {
    /// None read yet from `file`.
    pub const fn new(file: &'a BlockFile) -> Self {
        Self {
            file,
            seen: BTreeMap::new(),
            lowest: u64::MAX,
            chunk: None,
            last: None,
        }
    }

    /// The header `link` leads to, if the blocks it names hold one that
    /// says it is there — read with them, or from a chunk read before.
    pub fn read(&mut self, link: HeaderLink) -> Option<Header> {
        self.lowest = self.lowest.min(link.at);
        if let Some((_, header)) = self.seen.get(&link.at) {
            return header.clone();
        }
        let header = self
            .bytes_of(link.run())
            .and_then(|b| Header::decode(&b).ok())
            .filter(|h| h.at == link.at);
        self.seen.insert(link.at, (link.blocks, header.clone()));
        header
    }

    /// The bytes of `run`: from the chunk in hand, or read — with the chunk
    /// ending at it when the last header read lies close above it.
    fn bytes_of(&mut self, run: Run) -> Option<Vec<u8>> {
        let close = self
            .last
            .replace(run.start)
            .is_some_and(|at| at >= run.end() && at - run.end() < CHUNK_BLOCKS);
        if let Some((chunk, bytes)) = &self.chunk
            && chunk.start <= run.start
            && run.end() <= chunk.end()
        {
            let from = ((run.start - chunk.start) as usize) * BLOCK_SIZE;
            return bytes
                .get(from..from + run.byte_len() as usize)
                .map(<[u8]>::to_vec);
        }
        let floor = self
            .file
            .layout()
            .into_iter()
            .find(|&(start, end)| start <= run.start && run.start < end)
            .map_or(run.start, |(start, _)| start);
        let lo = run.end().saturating_sub(CHUNK_BLOCKS).max(floor);
        if !close || lo >= run.start {
            return self.file.read_run(run).ok();
        }
        let chunk = Run::new(lo, run.end() - lo);
        let bytes = self.file.read_run(chunk).ok()?;
        let from = ((run.start - lo) as usize) * BLOCK_SIZE;
        let out = bytes.get(from..).map(<[u8]>::to_vec);
        self.chunk = Some((chunk, bytes));
        out
    }

    /// A header found without a link — a reconstruction's root — kept as
    /// if read through one.
    pub fn keep(&mut self, link: HeaderLink, header: Header) {
        self.lowest = self.lowest.min(link.at);
        self.last = Some(link.at);
        self.seen.insert(link.at, (link.blocks, Some(header)));
    }
}

impl HeaderSource for DiskHeaders<'_> {
    // The disk is read on the spot: the future is ready at once.
    fn get(
        &mut self,
        link: HeaderLink,
    ) -> impl Future<Output = StorageResult<Option<Header>>> {
        std::future::ready(Ok(self.read(link)))
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
        .and_then(|link| Some((headers.read(link)?, link)));
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
    root: Option<(Header, HeaderLink)>,
) -> StorageResult<Rebuilt> {
    let Some((root, link)) = root else {
        return Ok(Rebuilt::default()); // nothing durable
    };
    let file = headers.file;
    // The root itself, read where it was found, is one of the headers kept.
    headers.keep(link, root.clone());
    let registered: Vec<u64> =
        types.iter().map(TypeSlot::struct_hash).collect();
    let disk = on_disk(walk(
        headers,
        (&root, link),
        &registered,
        Lane::Types,
        Shape::DISK,
    ))?;
    let remote = if object_backed {
        on_disk(walk(
            headers,
            (&root, link),
            &registered,
            Lane::Remote,
            Shape::REMOTE,
        ))?
    } else {
        Vec::new()
    };
    let mut chain = DiskChain {
        tail: Some(link),
        last_end: Some(link),
        floor: headers.lowest,
        horizon: root.horizon,
        ..DiskChain::default()
    };
    let mut used = Vec::new();
    for w in disk {
        let slot = slot_of(types, w.struct_hash)?;
        chain.types.insert(w.struct_hash, rotation(w.newest));
        if let Some(index) = &w.index {
            used.extend(disk_runs(index));
        }
        *slot.directory().borrow_mut() = w.index;
        if let Some(desc) = w.dict.filter(|d| d.is_allocated()) {
            used.push(desc.run());
            let dict = Dictionary::from_bytes(&file.read_run(desc.run())?)?;
            slot.dictionary().borrow_mut().load(dict, desc);
        }
    }
    for w in remote {
        let slot = slot_of(types, w.struct_hash)?;
        chain.remote.insert(w.struct_hash, rotation(w.newest));
        *slot.remote_directory().borrow_mut() = w.index;
        slot.remote_dict()
            .set(w.dict.unwrap_or(BlockDescriptor::EMPTY));
    }
    for (&at, &(blocks, ref header)) in &headers.seen {
        if header.is_some() && at <= root.at {
            let kept = HeaderLink::new(at, blocks);
            used.push(kept.run());
            chain.live.insert(at, blocks);
        }
    }
    Ok(Rebuilt {
        chain,
        used,
        remote_chain: root.chain,
    })
}

/// A type's rotation as an open starts it: named last in `newest`.
fn rotation(newest: HeaderLink) -> Rotation {
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

#[cfg(test)]
mod tests {
    use super::DiskHeaders;
    use crate::block::{BLOCK_SIZE, Run};
    use crate::block_file::BlockFile;
    use crate::header::{Header, HeaderLink, NO_FILE};

    /// A one-block header at `at`, after the one at `at - 1`.
    fn header(at: u64) -> Header {
        Header {
            at,
            prev: at
                .checked_sub(1)
                .map_or(HeaderLink::NONE, |p| HeaderLink::new(p, 1)),
            horizon: 0,
            shard: 0,
            roots: NO_FILE,
            end_of_batch: true,
            types: Vec::new(),
            remote: Vec::new(),
            chain: None,
        }
    }

    fn reads(file: &BlockFile) -> u64 {
        file.io().snapshot().0
    }

    /// Headers lying close together are read back a chunk at a time: the
    /// first alone, the one after it with the chunk ending there, the rest
    /// from that chunk. Each comes back as it was written.
    #[test]
    fn a_dense_walk_reads_its_headers_a_chunk_at_a_time() {
        let d = tempfile::tempdir().unwrap();
        let file = BlockFile::open(d.path(), [1, 2, 3, 4], false, 0).unwrap();
        for at in 0..6 {
            let mut bytes = header(at).encode();
            bytes.resize(BLOCK_SIZE, 0);
            file.write_run(0, Run::new(at, 1), &bytes).unwrap();
        }
        let mut headers = DiskHeaders::new(&file);
        let before = reads(&file);
        for at in (0..6).rev() {
            let got = headers.read(HeaderLink::new(at, 1));
            assert_eq!(got, Some(header(at)), "header {at}");
        }
        assert_eq!(reads(&file) - before, 2, "one alone, then one chunk");
    }

    /// A sparse walk reads each header alone.
    #[test]
    fn a_sparse_walk_reads_each_header_alone() {
        let d = tempfile::tempdir().unwrap();
        let file = BlockFile::open(d.path(), [1, 2, 3, 4], false, 0).unwrap();
        let far = [0, 600, 1200];
        for &at in &far {
            let mut bytes = header(at).encode();
            bytes.resize(BLOCK_SIZE, 0);
            file.write_run(0, Run::new(at, 1), &bytes).unwrap();
        }
        let mut headers = DiskHeaders::new(&file);
        let before = reads(&file);
        for &at in far.iter().rev() {
            let got = headers.read(HeaderLink::new(at, 1)).map(|h| h.at);
            assert_eq!(got, Some(at));
        }
        assert_eq!(reads(&file) - before, 3, "one block each");
    }
}
