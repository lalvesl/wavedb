//! The header that ends every settle window on disk ([RFC 0072] §3–§5,
//! §8): the addresses the window changed, the spare room restating old
//! ones, and which headers a reconstruction can no longer reach.
//!
//! What a window changes is **unrecorded** until a header names it: the
//! pages it placed, a dictionary it grew — and, in disk + object mode, what
//! uploads and evictions changed between windows. A header names as much of
//! it as its 32 KiB hold; what does not fit waits for the next header, and
//! only a header that leaves nothing unrecorded may end a batch.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md

use std::cell::Ref;
use std::collections::BTreeMap;

use crate::block::{BlockDescriptor, Run};
use crate::directory::Directory;
use crate::disk_chain::{DiskChain, Touched};
use crate::error::{StorageError, StorageResult};
use crate::header::{
    ENTRY_BYTES, Header, ITEM_BYTES, NO_FILE, Rotation, TypeEntry,
};
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::remote::{HEADER_BLOCKS, HEADER_BYTES};
use crate::restate::{TypeView, horizon, restate};

/// Which directories a view is of.
#[derive(Clone, Copy)]
enum Side {
    Disk,
    Remote,
}

impl PageStore {
    /// Whether anything is waiting for a header to name it.
    pub(crate) fn has_unrecorded(&self) -> bool {
        !self.unrecorded.borrow().slots.is_empty()
    }

    /// Mark everything unrecorded: every bucket of every directory and every
    /// dictionary, so the batch that ends next names the whole addressing —
    /// what a conversion and an adoption need, their flags and evictions
    /// changing every bucket at once.
    pub(crate) fn record_all(&self) {
        if self.has_disk() {
            let mut chain = self.chain.borrow_mut();
            chain.restating = true;
            chain.restate_from = None;
        }
        let mut unrecorded = self.unrecorded.borrow_mut();
        for (idx, slot) in self.types.iter().enumerate() {
            let touched = unrecorded.slots.entry(idx).or_default();
            if let Some(dir) = slot.directory().borrow().as_ref() {
                touched.disk.extend(0..dir.len());
            }
            if let Some(dir) = slot.remote_directory().borrow().as_ref() {
                touched.remote.extend(0..dir.len());
            }
            touched.dict = true;
            touched.remote_dict = self.object.is_some();
        }
    }

    /// Note what a window changes: the buckets it places, the dictionaries
    /// it grows.
    pub(crate) fn note_placed(
        &self,
        pages: impl Iterator<Item = (usize, usize)>,
        dicts: impl Iterator<Item = usize>,
    ) {
        let mut unrecorded = self.unrecorded.borrow_mut();
        for (idx, bucket) in pages {
            unrecorded.slots.entry(idx).or_default().disk.insert(bucket);
        }
        for idx in dicts {
            unrecorded.slots.entry(idx).or_default().dict = true;
        }
    }

    /// The header at block `at`, padded to its 32 KiB, and whether it ends
    /// the batch: it does when `end` asks and it left nothing unrecorded.
    /// `plans` and `dicts` are the window's, not yet published: their
    /// directories and dictionaries are what it names.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] when the header outgrew its room
    /// (unreachable: every entry and item is budgeted).
    pub(crate) fn header_at(
        &self,
        at: u64,
        plans: &[SlotPlan],
        dicts: &BTreeMap<usize, BlockDescriptor>,
        end: bool,
    ) -> StorageResult<(Vec<u8>, bool)> {
        let disk: Vec<_> =
            self.types.iter().map(|s| s.directory().borrow()).collect();
        let remote: Vec<_> = self
            .types
            .iter()
            .map(|s| s.remote_directory().borrow())
            .collect();
        let views = self.views_of(&disk, plans, dicts);
        let remote_views = self.remote_views_of(&remote);
        let mut chain = self.chain.borrow_mut();
        if chain.restating && chain.restate_from.is_none() {
            chain.restate_from = Some(at);
        }
        let mut header = Header {
            at,
            prev: chain.tail.unwrap_or(NO_FILE),
            horizon: 0,
            end_of_batch: false,
            start: 0,
            end: 0,
            types: Vec::new(),
            remote: Vec::new(),
            chain: self.object.as_ref().map(|o| o.chain.borrow().record()),
        };
        let mut room = HEADER_BYTES.saturating_sub(header.encoded_len());
        {
            let mut unrecorded = self.unrecorded.borrow_mut();
            for (&idx, touched) in &mut unrecorded.slots {
                let lanes = [
                    (Side::Disk, &views, &mut header.types),
                    (Side::Remote, &remote_views, &mut header.remote),
                ];
                for (side, views, entries) in lanes {
                    let Some(view) = views.get(idx) else { continue };
                    let rotations = match side {
                        Side::Disk => &chain.types,
                        Side::Remote => &chain.remote,
                    };
                    record(touched, side, view, rotations, entries, &mut room);
                }
            }
            unrecorded.slots.retain(|_, t| !t.is_empty());
            header.end_of_batch = end && unrecorded.slots.is_empty();
            if unrecorded.slots.is_empty()
                && chain.restating
                && let Some(from) = chain.restate_from.take()
            {
                chain.restated = Some(from);
                chain.restating = false;
            }
        }
        if self.object.is_some() && self.has_disk() {
            let half = room / 2;
            let left = restate(
                &mut header.remote,
                half,
                &remote_views,
                &mut chain.remote,
                at,
            );
            room = room - half + left;
        }
        restate(&mut header.types, room, &views, &mut chain.types, at);
        for e in &header.types {
            chain.types.entry(e.struct_hash).or_default().last = at;
        }
        for e in &header.remote {
            chain.remote.entry(e.struct_hash).or_default().last = at;
        }
        header.horizon =
            coverage(&views, &remote_views, &chain, self.object.is_some());
        chain.tail = Some(at);
        chain.live.insert(at);
        let mut bytes = header.encode();
        if bytes.len() > HEADER_BYTES {
            return Err(StorageError::Corrupt("a header outgrew its room"));
        }
        bytes.resize(HEADER_BYTES, 0);
        Ok((bytes, header.end_of_batch))
    }

    /// The batch just ended at header `at`: every header below the horizon
    /// — what a reconstruction from it still reads — is dead, and its run
    /// freed once this batch is durable.
    pub(crate) fn drop_dead_headers(&self, at: u64) {
        let disk: Vec<_> =
            self.types.iter().map(|s| s.directory().borrow()).collect();
        let remote: Vec<_> = self
            .types
            .iter()
            .map(|s| s.remote_directory().borrow())
            .collect();
        let views = self.views_of(&disk, &[], &BTreeMap::new());
        let remote_views = self.remote_views_of(&remote);
        let mut chain = self.chain.borrow_mut();
        let mut line =
            coverage(&views, &remote_views, &chain, self.object.is_some());
        // A reconstruction checks the batch it roots in back to the header
        // ending the batch before.
        if let Some(before) = chain.last_end {
            line = line.min(before);
        }
        chain.last_end = Some(at);
        let dead: Vec<u64> = chain.live.range(..line).copied().collect();
        let mut alloc = self.alloc.borrow_mut();
        for header in dead {
            chain.live.remove(&header);
            alloc.defer(Run::new(header, HEADER_BLOCKS));
        }
    }

    /// Every type's disk directory as the window leaves it.
    fn views_of<'a>(
        &self,
        disk: &'a [Ref<'_, Option<Directory>>],
        plans: &'a [SlotPlan],
        dicts: &BTreeMap<usize, BlockDescriptor>,
    ) -> Vec<TypeView<'a>> {
        self.types
            .iter()
            .enumerate()
            .map(|(idx, slot)| {
                let plan = plans.iter().position(|p| p.idx == idx);
                let slots = plan.map_or_else(
                    || disk[idx].as_ref().map_or(&[][..], Directory::slots),
                    |p| plans[p].dir.slots(),
                );
                let dict = plan
                    .and_then(|p| dicts.get(&p).copied())
                    .unwrap_or_else(|| slot.dictionary().borrow().descriptor());
                TypeView {
                    struct_hash: slot.struct_hash(),
                    slots,
                    dict: dict.is_allocated().then_some(dict.raw()),
                }
            })
            .collect()
    }

    /// Every type's remote directory, empty in a disk-only store.
    fn remote_views_of<'a>(
        &self,
        remote: &'a [Ref<'_, Option<Directory>>],
    ) -> Vec<TypeView<'a>> {
        if self.object.is_none() {
            return Vec::new();
        }
        self.types
            .iter()
            .enumerate()
            .map(|(idx, slot)| {
                let dict = slot.remote_dict().get();
                TypeView {
                    struct_hash: slot.struct_hash(),
                    slots: remote[idx]
                        .as_ref()
                        .map_or(&[][..], Directory::slots),
                    dict: dict.is_allocated().then_some(dict.raw()),
                }
            })
            .collect()
    }
}

/// The lowest header a reconstruction from the newest one reads
/// ([`Header::horizon`]): where the rotations of the disk's directories —
/// and in disk + object mode the object store's — last came round, or the
/// last full restatement, whichever is higher.
fn coverage(
    views: &[TypeView<'_>],
    remote_views: &[TypeView<'_>],
    chain: &DiskChain,
    object_backed: bool,
) -> u64 {
    let mut line = horizon(views, &chain.types, chain.floor);
    if object_backed {
        line = line.min(horizon(remote_views, &chain.remote, chain.floor));
    }
    line.max(chain.restated.unwrap_or(0))
}

/// Name as much of `touched`'s `side` as `room` holds in an entry of
/// `entries`, spending the room and forgetting what was named.
fn record(
    touched: &mut Touched,
    side: Side,
    view: &TypeView<'_>,
    rotations: &BTreeMap<u64, Rotation>,
    entries: &mut Vec<TypeEntry>,
    room: &mut usize,
) {
    let (buckets, dict) = match side {
        Side::Disk => (&mut touched.disk, &mut touched.dict),
        Side::Remote => (&mut touched.remote, &mut touched.remote_dict),
    };
    if (buckets.is_empty() && !*dict) || *room < ENTRY_BYTES {
        return;
    }
    *room -= ENTRY_BYTES;
    let mut changed = Vec::new();
    while *room >= ITEM_BYTES {
        let Some(bucket) = buckets.pop_first() else {
            break;
        };
        if let Some(desc) = view.slots.get(bucket) {
            changed.push((bucket as u32, desc.raw()));
            *room -= ITEM_BYTES;
        }
    }
    *dict = false;
    entries.push(TypeEntry {
        struct_hash: view.struct_hash,
        buckets: view.slots.len() as u32,
        last: rotations.get(&view.struct_hash).map_or(NO_FILE, |r| r.last),
        dict: view.dict,
        changed,
    });
}
