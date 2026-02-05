//! Opening a storage shard its data shards share ([RFC 0073] §6): the
//! storage side finds the roots block to root in, each data shard walks
//! from its root through read handles of its own, and the allocator is
//! rebuilt from everything they name before the storage thread starts.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards-WIP.md

use std::path::Path;

use crate::block::Run;
use crate::block_file::BlockFile;
use crate::error::StorageResult;
use crate::header::NO_FILE;
use crate::link::{Link, Storage};
use crate::manifest::Manifest;
use crate::page_store::{PageStore, StoreOptions};
use crate::reopen::{DiskHeaders, cut_tails, rebuild};
use crate::roots::ROOTS_BLOCKS;
use crate::roots_find::find_roots;
use crate::storage_shard::StorageShard;
use crate::struct_storage::{StructStorage, slots_of};

/// Open storage shard `n` of the database `facts` describe, its data files
/// in `dir`: one store per data shard writing through it, in position
/// order, and its thread started (see the module docs).
///
/// # Errors
/// [`StorageError::EngineBusy`](crate::StorageError::EngineBusy) when the
/// files are held by another open; read faults and the walks' faults; the
/// OS refusing a thread.
pub fn open_storage_shard(
    dir: &Path,
    n: usize,
    types: &[&'static StructStorage],
    options: &StoreOptions,
    facts: &Manifest,
) -> StorageResult<Vec<PageStore>> {
    let per = facts.per_storage();
    let file = BlockFile::open(dir, facts.seed(), false, 0)?;
    let found = find_roots(&mut DiskHeaders::new(&file), per)?;
    let by_shard: Vec<u64> = (0..per)
        .map(|i| found.as_ref().and_then(|r| r.root(i)).unwrap_or(NO_FILE))
        .collect();
    let mut used: Vec<Run> =
        found.iter().map(|r| Run::new(r.at, ROOTS_BLOCKS)).collect();
    let mut opened = Vec::with_capacity(per);
    for (position, &root) in by_shard.iter().enumerate() {
        let reader = BlockFile::reader(
            dir,
            facts.seed(),
            false,
            options.page_cache_bytes,
        )?;
        let slots = slots_of(types);
        let rebuilt = {
            let mut headers = DiskHeaders::new(&reader);
            let root = (root != NO_FILE).then(|| headers.read(root)).flatten();
            rebuild(&mut headers, &slots, false, root)?
        };
        used.extend_from_slice(&rebuilt.used);
        opened.push((reader, slots, rebuilt.chain, position));
    }
    let alloc = cut_tails(&file, &used, options.file_bytes)?;
    if !file.has_files() {
        // A data file held — locked — whatever was written, as a store
        // writing its own files keeps one.
        file.create(alloc.next_start())?;
    }
    let newest = found.map(|r| r.at);
    let (shard, jobs) = StorageShard::new(file, alloc, by_shard, newest);
    let inbox = shard.spawn(jobs, &format!("wavedb-storage-{n:02x}"), per)?;
    Ok(opened
        .into_iter()
        .map(|(reader, slots, chain, position)| {
            let link = Storage::Shared(Link::new(inbox.clone(), per));
            let mut store =
                PageStore::assemble(reader, slots, link, options.clone());
            store.position = position as u32;
            *store.chain.borrow_mut() = chain;
            store.forget_remote();
            store
        })
        .collect())
}
