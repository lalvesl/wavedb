//! Opening a storage shard its data shards share ([RFC 0073] §6, §8): the
//! storage side finds the roots block to root in, each data shard walks
//! from its root through read handles of its own, and the allocator is
//! rebuilt from everything they name before the storage thread starts.
//! With an object store, the storage shard's chain of files gets a thread
//! of its own, started from where the data shards last saw it — or from the
//! object store, when the disk holds nothing yet; in object mode it is the
//! only thread.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::path::Path;
use std::sync::mpsc;

use crate::block::{BlockAllocator, Run};
use crate::block_file::BlockFile;
use crate::error::{StorageError, StorageResult};
use crate::header::{Chain, NO_FILE};
use crate::link::{Link, Storage};
use crate::manifest::Manifest;
use crate::page_store::{PageStore, StoreOptions};
use crate::recover::{Found, newest_file, recover};
use crate::remote::{FILE_BLOCKS, Remote};
use crate::remote_link::RemoteLink;
use crate::remote_shard::{RemoteMsg, RemoteShard};
use crate::reopen::{DiskHeaders, cut_tails, rebuild};
use crate::roots::ROOTS_BLOCKS;
use crate::roots_find::find_roots;
use crate::storage_shard::StorageShard;
use crate::struct_storage::{StructStorage, TypeSlot, slots_of};

/// One data shard opened, not yet linked.
struct Opened {
    file: BlockFile,
    slots: Vec<TypeSlot>,
    chain: crate::disk_chain::DiskChain,
    remote: Option<Chain>,
    found: Option<Found>,
}

/// Open storage shard `n` of the database `facts` describe, its data files
/// in `dir`: one store per data shard writing through it, in position
/// order, and its threads started (see the module docs).
///
/// # Errors
/// [`StorageError::EngineBusy`] when the files are held by another open;
/// read faults, the walks' and the recoveries' faults; the OS refusing a
/// thread.
pub fn open_storage_shard(
    dir: &Path,
    n: usize,
    types: &[&'static StructStorage],
    options: &StoreOptions,
    facts: &Manifest,
) -> StorageResult<Vec<PageStore>> {
    let per = facts.per_storage();
    let remote = options.remote.as_ref().map(|r| r.storage(n));
    let file = BlockFile::open(dir, facts.seed(), facts.object_backed(), 0)?;
    let had_files = file.has_files();
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
            facts.object_backed(),
            options.page_cache_bytes,
        )?;
        let slots = slots_of(types);
        let rebuilt = {
            let mut headers = DiskHeaders::new(&reader);
            let root = (root != NO_FILE).then(|| headers.read(root)).flatten();
            rebuild(&mut headers, &slots, remote.is_some(), root)?
        };
        used.extend_from_slice(&rebuilt.used);
        let found = match &remote {
            Some(remote) if !had_files => {
                recovered(remote, &slots, position as u32)?
            }
            _ => None,
        };
        opened.push(Opened {
            file: reader,
            slots,
            chain: rebuilt.chain,
            remote: rebuilt.remote_chain.as_ref().map(Chain::from_record),
            found,
        });
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
    let remote_inbox = match &remote {
        Some(remote) => Some(start_remote(remote, n, &opened)?),
        None => None,
    };
    let mut stores = Vec::with_capacity(per);
    for (position, o) in opened.into_iter().enumerate() {
        let link = Storage::Shared(Link::new(inbox.clone(), per));
        let options = StoreOptions {
            remote: remote.clone(),
            ..options.clone()
        };
        let mut store = PageStore::assemble(o.file, o.slots, link, options);
        store.position = position as u32;
        *store.chain.borrow_mut() = o.chain;
        link_remote(&mut store, (remote_inbox.as_ref(), per), o.remote);
        if store.object.is_some() {
            store.prepare_object(had_files)?;
        } else {
            store.forget_remote();
        }
        if let Some(found) = o.found {
            store.adopt(found)?;
        }
        stores.push(store);
    }
    Ok(stores)
}

/// Open storage shard `n` of the database `facts` describe in object mode:
/// one store per data shard, in RAM, each recovered from the storage
/// shard's chain of files and uploading through its remote thread.
///
/// # Errors
/// [`StorageError::NotObjectBacked`] without `options.remote`; the
/// recoveries' faults; the OS refusing a thread.
pub fn open_object_storage_shard(
    n: usize,
    types: &[&'static StructStorage],
    options: &StoreOptions,
    facts: &Manifest,
) -> StorageResult<Vec<PageStore>> {
    let remote = options
        .remote
        .as_ref()
        .ok_or(StorageError::NotObjectBacked)?
        .storage(n);
    let mut opened = Vec::with_capacity(facts.per_storage());
    for position in 0..facts.per_storage() {
        let slots = slots_of(types);
        let found = recovered(&remote, &slots, position as u32)?;
        opened.push(Opened {
            file: BlockFile::in_memory(facts.seed()),
            slots,
            chain: crate::disk_chain::DiskChain::default(),
            remote: None,
            found,
        });
    }
    let inbox = start_remote(&remote, n, &opened)?;
    let mut stores = Vec::with_capacity(opened.len());
    for (position, o) in opened.into_iter().enumerate() {
        let alloc = BlockAllocator::new(options.file_bytes);
        let own = Storage::Own(std::cell::RefCell::new(alloc));
        let options = StoreOptions {
            remote: Some(remote.clone()),
            ..options.clone()
        };
        let mut store = PageStore::assemble(o.file, o.slots, own, options);
        store.position = position as u32;
        link_remote(&mut store, (Some(&inbox), facts.per_storage()), None);
        if let Some(found) = o.found {
            store.adopt(found)?;
        }
        stores.push(store);
    }
    Ok(stores)
}

/// What the object store holds for the data shard at `position`.
fn recovered(
    remote: &Remote,
    slots: &[TypeSlot],
    position: u32,
) -> StorageResult<Option<Found>> {
    let registered: Vec<u64> =
        slots.iter().map(TypeSlot::struct_hash).collect();
    futures::executor::block_on(recover(remote, &registered, position))
}

/// Start storage shard `n`'s remote thread where its data shards last saw
/// the chain — on disk, or as a recovery found it — and past every file
/// the object store holds: a number once written is never written again.
fn start_remote(
    remote: &Remote,
    n: usize,
    opened: &[Opened],
) -> StorageResult<mpsc::Sender<RemoteMsg>> {
    let chains: Vec<Option<&Chain>> = opened
        .iter()
        .map(|o| o.found.as_ref().map(|f| &f.chain).or(o.remote.as_ref()))
        .collect();
    let newest = futures::executor::block_on(newest_file(remote))?;
    let past = newest.map_or(0, |f| (f + 1) * FILE_BLOCKS);
    let end = chains.iter().flatten().map(|c| c.end).fold(past, u64::max);
    let start = chains.iter().flatten().map(|c| c.start).max().unwrap_or(0);
    let by_shard = chains
        .iter()
        .map(|c| c.and_then(|c| c.cut).unwrap_or(NO_FILE))
        .collect();
    let roots = chains.iter().flatten().filter_map(|c| c.roots).max();
    let shard = RemoteShard::new(remote.clone(), (start, end), by_shard, roots);
    Ok(shard.spawn(&format!("wavedb-remote-{n:02x}"), opened.len())?)
}

/// Link `store` to its storage shard's remote thread, its chain where the
/// disk last saw it.
fn link_remote(
    store: &mut PageStore,
    (inbox, per): (Option<&mpsc::Sender<RemoteMsg>>, usize),
    chain: Option<Chain>,
) {
    let (Some(object), Some(inbox)) = (store.object.as_mut(), inbox) else {
        return;
    };
    object.link = Some(RemoteLink::new(inbox.clone(), per));
    if let Some(chain) = chain {
        *object.chain.borrow_mut() = chain;
    }
}
