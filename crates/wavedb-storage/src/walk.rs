//! The walk that rebuilds the directories from the headers ([RFC 0072] §6,
//! [RFC 0070] §6) — split from [`crate::recover`], which finds where it
//! starts.
//!
//! Each type is followed along its `last` until every entry of its
//! directory is known — each named bucket filling its aliases (RFC 0074); the
//! way down by `prev`, for a registered type no header met yet names, stops
//! at the root's [`Header::horizon`]: every type that existed when the root
//! was written is named at or above it, so one still unnamed there was
//! added since — it is new, and empty. Without the bound, every open after
//! a type is added would read down through every surviving header, the dead
//! ones included.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::collections::{BTreeMap, BTreeSet};

use crate::block::BlockDescriptor;
use crate::directory::D_MAX;
use crate::error::{StorageError, StorageResult};
use crate::header::{Header, NO_FILE, TypeEntry};

/// Where a walk reads its headers from: the object store's files
/// (`crate::recover_headers`) or the disk's windows (`crate::reopen`).
pub trait HeaderSource {
    /// The header at `at`, or `None` when nothing there is one.
    async fn get(&mut self, at: u64) -> StorageResult<Option<Header>>;
}

/// Which directories of a header a walk rebuilds.
#[derive(Clone, Copy)]
pub enum Lane {
    /// The directories the header's own pages are in.
    Types,
    /// On disk, in disk + object mode: the object store's.
    Remote,
}

impl Lane {
    fn of(self, header: &Header) -> &[TypeEntry] {
        match self {
            Self::Types => &header.types,
            Self::Remote => &header.remote,
        }
    }
}

/// One type's directory being gathered.
struct Gathering {
    /// `2^depth` entries, as the newest header naming the type sizes them.
    slots: Vec<Option<u64>>,
    depth: u8,
    missing: usize,
    dict: Option<u64>,
    /// The next header with entries for it ([`NO_FILE`] once the chain
    /// ends).
    next: u64,
    /// The newest header naming it — the first read that did.
    newest: u64,
}

impl Gathering {
    /// Take `changed` — the pages not known yet, each filling every entry
    /// of its bucket still unknown ([RFC 0074]) — and follow on to `last`.
    ///
    /// Newest first, the first naming an entry meets is its page now: the
    /// split that made that page was the last change to the entry, and the
    /// round of it named it.
    ///
    /// [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md
    fn take(&mut self, changed: &[(u32, u64)], last: u64) -> StorageResult<()> {
        for &(bucket, raw) in changed {
            let depth = BlockDescriptor::from_raw(raw).depth();
            let bucket = bucket as usize;
            if depth > self.depth || bucket >> depth != 0 {
                return Err(StorageError::Corrupt(
                    "a header names no bucket of its directory",
                ));
            }
            for entry in (bucket..self.slots.len()).step_by(1 << depth) {
                if let Some(slot) = self.slots.get_mut(entry)
                    && slot.is_none()
                {
                    *slot = Some(raw);
                    self.missing -= 1;
                }
            }
        }
        self.next = last;
        Ok(())
    }
}

/// One type as a walk rebuilt it.
pub struct Walked {
    pub struct_hash: u64,
    pub slots: Vec<BlockDescriptor>,
    pub dict: Option<BlockDescriptor>,
    /// The newest header naming it.
    pub newest: u64,
}

/// Every type's directory and dictionary descriptor, gathered from `top`
/// back along the types' chains, and down by `prev` while a `registered`
/// type is still unnamed — never below `top`'s horizon, where a type still
/// unnamed is new and empty (see the module docs).
///
/// # Errors
/// Object-store faults; [`StorageError::Corrupt`] when a header a type's
/// chain names is missing or does not name it. A header missing on the way
/// down by `prev` ends the way down: a compaction deleted it, which it does
/// only below every header naming a type.
pub async fn walk<S: HeaderSource>(
    headers: &mut S,
    top: &Header,
    registered: &[u64],
    lane: Lane,
) -> StorageResult<Vec<Walked>> {
    let mut types: BTreeMap<u64, Gathering> = BTreeMap::new();
    let mut unnamed: BTreeSet<u64> = registered.iter().copied().collect();
    gather(&mut types, &mut unnamed, top, lane)?;
    // The next header the way down reads.
    let mut below = top.prev;
    loop {
        let chained = types
            .values()
            .filter(|t| t.missing > 0 && t.next != NO_FILE)
            .map(|t| t.next)
            .max();
        let down =
            (!unnamed.is_empty() && below != NO_FILE && below >= top.horizon)
                .then_some(below);
        let Some(file) = chained.max(down) else { break };
        let Some(header) = headers.get(file).await? else {
            if chained == Some(file) {
                return Err(StorageError::Corrupt("a chained file is missing"));
            }
            below = NO_FILE;
            continue;
        };
        if header.shard != top.shard {
            // Data shards share their storage shard's files, never a
            // chain (RFC 0073 §5).
            return Err(StorageError::Corrupt(
                "a link lands on another data shard's header",
            ));
        }
        if file == below {
            // Only ever down: a block a dead header was freed from may hold
            // a newer one since, whose `prev` leads back up.
            below = if header.prev < file {
                header.prev
            } else {
                NO_FILE
            };
        }
        gather(&mut types, &mut unnamed, &header, lane)?;
        if types.values().any(|t| t.missing > 0 && t.next == file) {
            return Err(StorageError::Corrupt(
                "a chain names a file without it",
            ));
        }
    }
    Ok(types
        .into_iter()
        .map(|(hash, t)| Walked {
            struct_hash: hash,
            slots: t
                .slots
                .into_iter()
                .map(|raw| {
                    raw.map_or(
                        BlockDescriptor::EMPTY,
                        BlockDescriptor::from_raw,
                    )
                })
                .collect(),
            dict: t.dict.map(BlockDescriptor::from_raw),
            newest: t.newest,
        })
        .collect())
}

/// Fold `header` in: the types already followed take what it holds for
/// them if their chain is here, and a type not met yet starts here — reads
/// go highest first, so this is its newest header.
fn gather(
    types: &mut BTreeMap<u64, Gathering>,
    unnamed: &mut BTreeSet<u64>,
    header: &Header,
    lane: Lane,
) -> StorageResult<()> {
    for entry in lane.of(header) {
        if let Some(t) = types.get_mut(&entry.struct_hash) {
            if t.next == header.at {
                t.take(&entry.changed, entry.last)?;
            }
            continue;
        }
        let depth = entry.depth.unwrap_or(0);
        if depth > D_MAX {
            return Err(StorageError::Corrupt("a directory past its depth"));
        }
        let n = entry.depth.map_or(0, |d| 1usize << d);
        let mut t = Gathering {
            slots: vec![None; n],
            depth,
            missing: n,
            dict: entry.dict,
            next: NO_FILE,
            newest: header.at,
        };
        t.take(&entry.changed, entry.last)?;
        types.insert(entry.struct_hash, t);
        unnamed.remove(&entry.struct_hash);
    }
    Ok(())
}
