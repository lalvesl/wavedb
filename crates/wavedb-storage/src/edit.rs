//! The addressing delta that rides every settle window ([RFC 0046]).
//!
//! A round of [`crate::checkpoint`] already writes one contiguous window and
//! already knows, by the time it has placed its pages, exactly which buckets
//! changed address. This module turns that into an [`EditChunk`] appended to
//! the same window: **the metadata costs no IOp of its own**.
//!
//! Each chunk names the one written before it ([RFC 0048]), so the log is a
//! chain in the data files and the `Commit` frame is one address — the head. The
//! frame therefore costs the same whether the log holds one chunk or a
//! thousand, which is what lets compaction be spaced by the ratio rule below
//! instead of by how large a frame is affordable.
//!
//! ## Two passes, one shape
//!
//! The chunk's byte length has to be known before the window is carved, but
//! its contents are only known after the runs are handed out. Both passes call
//! [`chunk_of`] with the same `full` flag and the same plans, so they build the
//! *same shape* — same slot count, same `changed` length, same `Option` tags —
//! and therefore encode to the same length. The first pass reserves; the second
//! fills in the addresses.
//!
//! ## Compaction
//!
//! [`crate::meta_log::MetaLog`] decides when a round should emit a **full**
//! chunk instead of a delta: one naming every bucket of every type, built from
//! the directories already resident in RAM. A snapshot writes `prev = 0` — it
//! stands alone, so it ends the chain rather than extending it.
//!
//! [RFC 0046]: ../../../rfcs/0046-directory-deltas-in-the-window.md
//! [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md

use std::collections::{BTreeMap, BTreeSet};

use wavedb_core::wire::{WaveWire, from_wire_checked, to_wire_checked};

use crate::block::{BlockDescriptor, Run};
use crate::block_file::BlockFile;
use crate::error::{StorageError, StorageResult};
use crate::page_store::PageStore;
use crate::plan::SlotPlan;
use crate::struct_storage::{StructDirectory, TypeSlot};

/// Hard cap on chunks between snapshots — bounds a recovery's scattered reads
/// even when the snapshot is large enough that the ratio alone would not.
pub const MAX_EDIT_CHUNKS: usize = 1024;

/// One type's descriptor changes in a round.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct SlotEdit {
    pub struct_hash: u64,
    /// Bucket count *after* the round: replay grows the vector to it, so a
    /// linear-hashing split needs no record of its own.
    pub buckets: u32,
    /// `(bucket, raw BlockDescriptor)` for every bucket that moved.
    pub changed: Vec<(u32, u64)>,
    /// The dictionary's run descriptor — `Some` only when it changed.
    pub dict: Option<u64>,
}

/// What one settle window changed, and where the round before it recorded its
/// own changes.
///
/// A **full** chunk names every bucket of every type, so applying it over any
/// prior state yields exactly that state; nothing on the wire distinguishes it
/// from a delta, and nothing needs to — a snapshot is simply a chunk that ends
/// the chain.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct EditChunk {
    /// Raw [`BlockDescriptor`] of the chunk written before this one; `0` ends
    /// the walk ([RFC 0048]).
    ///
    /// A **descriptor**, not an address: a chunk spans however many blocks its
    /// round needed, so the pointer carries the count as well as the start and
    /// the walk reads each chunk with one positioned read of exactly N blocks —
    /// no probe read to discover a length. It rides inside the envelope's crc
    /// like every other field, so a corrupted pointer fails its chunk's check
    /// before there is anything to follow.
    ///
    /// [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md
    pub prev: u64,
    pub slots: Vec<SlotEdit>,
    /// The object store's directories (RFC 0070 §2): the same edits, over
    /// remote descriptors. A disk-only store writes none.
    pub remote: Vec<SlotEdit>,
    /// Where the object store's chain of files stands (RFC 0070 §6) — in a
    /// snapshot, and in a delta that carries upload results.
    pub chain: Option<crate::header::ChainRecord>,
}

/// Walk the chain back from `head`, newest first, returning every live chunk
/// **oldest first** — the order [`Replay`] folds them in.
///
/// Reads each chunk to learn its `prev` and reads it again to apply it (in
/// `load_commit`): 2N reads for O(1) RAM, the trade [RFC 0048] chose, since
/// startup reads are the resource this design already spends and holding every
/// chunk at once is not.
///
/// # Errors
/// [`StorageError::Corrupt`] on an unreadable chunk, or on a chain longer than
/// [`MAX_EDIT_CHUNKS`] — the reader-side bound that replaces the structural one
/// a frame-carried list gave for free, and which also stops a walk that somehow
/// re-enters itself.
///
/// [RFC 0048]: ../../../rfcs/0048-chained-addressing-log.md
pub fn walk(
    file: &BlockFile,
    head: BlockDescriptor,
) -> StorageResult<Vec<BlockDescriptor>> {
    let mut chain = Vec::new();
    let mut cursor = head;
    while cursor.is_allocated() {
        if chain.len() > MAX_EDIT_CHUNKS {
            return Err(StorageError::Corrupt("edit chain too long"));
        }
        chain.push(cursor);
        cursor =
            BlockDescriptor::from_raw(read_chunk(file, cursor.run())?.prev);
    }
    chain.reverse();
    Ok(chain)
}

/// Serialise a chunk for the window: `[len u32 LE][crc32][wire]`, the same
/// self-delimiting envelope a page uses (a run is block-padded, so the length
/// prefix is what bounds the decode).
pub fn encode(chunk: &EditChunk) -> Vec<u8> {
    let payload = to_wire_checked(chunk);
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Read one chunk back from the run a `Commit` frame named.
///
/// # Errors
/// [`StorageError::Io`] on a read fault; [`StorageError::Corrupt`] if the
/// envelope, its crc, or its payload does not decode — a frame naming a run
/// asserts that run is durable, so a bad chunk is corruption, not a torn tail.
pub fn read_chunk(file: &BlockFile, run: Run) -> StorageResult<EditChunk> {
    let bytes = file.read_run(run)?;
    let len = bytes
        .get(..4)
        .and_then(|p| p.try_into().ok())
        .map(u32::from_le_bytes)
        .ok_or(StorageError::Corrupt("edit chunk header"))?
        as usize;
    let body = bytes
        .get(4..4 + len)
        .ok_or(StorageError::Corrupt("edit chunk length"))?;
    from_wire_checked(body).map_err(|_| StorageError::Corrupt("edit chunk"))
}

/// Build the chunk describing `plans`.
///
/// `dicts` maps a plan's index to the dictionary descriptor it was just given;
/// on the reservation pass it is empty and the placeholder keeps the shape.
pub fn chunk_of(
    store: &PageStore,
    plans: &[SlotPlan],
    full: bool,
    dicts: &BTreeMap<usize, BlockDescriptor>,
    prev: u64,
) -> EditChunk {
    let unrecorded = store.object.as_ref().map(|o| o.unrecorded.borrow());
    let touched =
        |idx: usize| unrecorded.as_ref().and_then(|u| u.slots.get(&idx));
    let mut slots: Vec<SlotEdit> = plans
        .iter()
        .enumerate()
        .map(|(plan, work)| {
            let cleared = touched(work.idx).map(|t| &t.disk);
            planned_edit(store, plan, work, full, dicts, cleared)
        })
        .collect();
    // A snapshot must stand alone, so the types this round did not touch
    // contribute their resident state. A delta carries what uploads changed
    // since the last chunk: the flags they cleared, the remote pages they
    // placed (RFC 0070 §5).
    let mut remote = Vec::new();
    for (idx, slot) in store.types.iter().enumerate() {
        if store.object.is_some() {
            let listed = touched(idx).map(|t| &t.remote);
            let dir = slot.remote_directory();
            if let Some(mut edit) = listed_edit(dir, slot, full, listed) {
                // The dictionary's copy in the object store rides along.
                let dict = slot.remote_dict().get();
                edit.dict = dict.is_allocated().then_some(dict.raw());
                remote.push(edit);
            }
        }
        if plans.iter().any(|work| work.idx == idx) {
            continue;
        }
        let listed = touched(idx).map(|t| &t.disk);
        if let Some(mut edit) =
            listed_edit(slot.directory(), slot, full, listed)
        {
            if full {
                edit.dict = Some(slot.dictionary().borrow().descriptor().raw());
            }
            slots.push(edit);
        }
    }
    let chain = store
        .object
        .as_ref()
        .filter(|_| full || store.has_unrecorded())
        .map(|o| o.chain.borrow().record());
    EditChunk {
        prev,
        slots,
        remote,
        chain,
    }
}

/// The edit for a slot this round planned — its working directory holds the
/// old descriptors on the reservation pass and the new ones after `install`.
fn planned_edit(
    store: &PageStore,
    plan: usize,
    work: &SlotPlan,
    full: bool,
    dicts: &BTreeMap<usize, BlockDescriptor>,
    cleared: Option<&BTreeSet<usize>>,
) -> SlotEdit {
    let slot = &store.types[work.idx];
    let changed = if full {
        every_bucket(work.dir.slots())
    } else {
        // The rewritten buckets, and those whose flag an upload cleared.
        let buckets: BTreeSet<usize> = work
            .pages
            .keys()
            .chain(cleared.into_iter().flatten())
            .copied()
            .collect();
        buckets
            .into_iter()
            .map(|bucket| (bucket as u32, work.dir.descriptor(bucket).raw()))
            .collect()
    };
    let dict = if work.dict.is_some() {
        Some(dicts.get(&plan).copied().map_or(0, BlockDescriptor::raw))
    } else if full {
        Some(slot.dictionary().borrow_mut().descriptor().raw())
    } else {
        None
    };
    SlotEdit {
        struct_hash: slot.struct_hash(),
        buckets: work.dir.len() as u32,
        changed,
        dict,
    }
}

/// The edit for a directory this round planned no page of: every bucket of
/// it for a snapshot, the `listed` ones for a delta — `None` when that is
/// nothing, or the directory does not exist yet.
fn listed_edit(
    cell: &StructDirectory,
    slot: &TypeSlot,
    full: bool,
    listed: Option<&BTreeSet<usize>>,
) -> Option<SlotEdit> {
    let guard = cell.borrow();
    let dir = guard.as_ref()?;
    let changed: Vec<(u32, u64)> = if full {
        every_bucket(dir.slots())
    } else {
        listed?
            .iter()
            .map(|&bucket| (bucket as u32, dir.descriptor(bucket).raw()))
            .collect()
    };
    (full || !changed.is_empty()).then(|| SlotEdit {
        struct_hash: slot.struct_hash(),
        buckets: dir.len() as u32,
        changed,
        dict: None,
    })
}

fn every_bucket(slots: &[BlockDescriptor]) -> Vec<(u32, u64)> {
    slots
        .iter()
        .enumerate()
        .map(|(bucket, desc)| (bucket as u32, desc.raw()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{EditChunk, SlotEdit, encode};
    use crate::replay::Replay;
    use wavedb_core::wire::from_wire_checked;

    fn edit(hash: u64, buckets: u32, changed: &[(u32, u64)]) -> SlotEdit {
        SlotEdit {
            struct_hash: hash,
            buckets,
            changed: changed.to_vec(),
            dict: None,
        }
    }

    #[test]
    fn chunk_roundtrips_through_its_envelope() {
        let chunk = EditChunk {
            prev: 0,
            remote: Vec::new(),
            chain: None,
            slots: vec![SlotEdit {
                struct_hash: 7,
                buckets: 3,
                changed: vec![(0, 111), (2, 333)],
                dict: Some(99),
            }],
        };
        let bytes = encode(&chunk);
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        assert_eq!(
            from_wire_checked::<EditChunk>(&bytes[4..4 + len]).unwrap(),
            chunk
        );
    }

    /// The reservation pass must predict the final pass exactly: same shape,
    /// same encoded length, whatever addresses end up in it.
    #[test]
    fn encoded_length_depends_on_shape_not_addresses() {
        let skeleton = EditChunk {
            prev: 0,
            remote: Vec::new(),
            chain: None,
            slots: vec![edit(7, 3, &[(0, 0), (2, 0)])],
        };
        let filled = EditChunk {
            prev: 0,
            remote: Vec::new(),
            chain: None,
            slots: vec![edit(7, 3, &[(0, u64::MAX), (2, 0x1234_5678)])],
        };
        assert_eq!(encode(&skeleton).len(), encode(&filled).len());
    }

    #[test]
    fn replay_folds_deltas_over_a_snapshot() {
        let mut replay = Replay::default();
        replay
            .apply(&EditChunk {
                prev: 0,
                remote: Vec::new(),
                chain: None,
                slots: vec![SlotEdit {
                    struct_hash: 7,
                    buckets: 2,
                    changed: vec![(0, 10), (1, 20)],
                    dict: Some(5),
                }],
            })
            .unwrap();
        // A delta: bucket 1 moves, bucket 2 is new, the dictionary is untouched.
        replay
            .apply(&EditChunk {
                prev: 0,
                remote: Vec::new(),
                chain: None,
                slots: vec![edit(7, 3, &[(1, 21), (2, 30)])],
            })
            .unwrap();
        let parts = replay.into_parts();
        let (slots, dicts) = (parts.slots, parts.dicts);
        assert_eq!(slots[&7], vec![10, 21, 30]);
        assert_eq!(dicts[&7], 5, "an untouched dictionary must carry forward");
    }

    #[test]
    fn a_bucket_outside_the_directory_is_corruption() {
        let mut replay = Replay::default();
        assert!(
            replay
                .apply(&EditChunk {
                    prev: 0,
                    remote: Vec::new(),
                    chain: None,
                    slots: vec![edit(7, 2, &[(5, 10)])],
                })
                .is_err()
        );
    }
}
