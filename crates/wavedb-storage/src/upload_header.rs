//! The headers of an upload's files ([RFC 0070] §5–§6, [RFC 0079] §3) —
//! split from [`crate::upload_file`], which lays the files out.
//!
//! - **Sized to what they name.** A file's header takes whole blocks: what
//!   the object store's indexes journaled and its dictionaries moved, as the
//!   headers will drain it ([`need`]), spread over the upload's files
//!   ([`rooms`]), each capped at [`MAX_HEADER_BLOCKS`]. The rest of a
//!   header's last block is spare room, restated into for free.
//! - **A restating header** heads the upload's first file when the walk a
//!   recovery from the newest header makes grows past its bound, on the
//!   same rule as the disk's (`crate::header_room`).
//! - [`finish`] writes them, in order: each links to the one before, names
//!   what changed as far as its room goes, spends its spare room restating
//!   old ranges, and records the horizon those restatements leave.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use std::time::Instant;

use crate::block::BLOCK_SIZE;
use crate::error::{StorageError, StorageResult};
use crate::header::{
    BARE_BYTES, Chain, ENTRY_BYTES, Header, HeaderLink, NO_FILE, TypeEntry,
};
use crate::header_room::{
    MAX_HEADER_BLOCKS, RESTATE_AFTER, pass_len, restates,
};
use crate::remote::FILE_BLOCKS;
use crate::restate::{TypeView, horizon, restate};
use crate::roots::Roots;
use crate::upload_file::{Named, Packed, SECTION};

/// Bytes the headers naming everything `named` holds unrecorded take, past
/// their bare headers.
pub fn need(named: &[Named<'_>]) -> usize {
    named
        .iter()
        .map(|t| {
            let journaled = t
                .index
                .as_deref()
                .filter(|i| i.has_unrecorded())
                .map(|i| i.pending_len_from(i.journal()));
            match journaled {
                None if !t.dict_moved => 0,
                journaled => ENTRY_BYTES + journaled.unwrap_or(0),
            }
        })
        .sum()
}

/// Blocks each of `files` headers takes to hold `bytes` past their bare
/// headers between them, the first ones first: one at least, the cap at
/// most.
pub fn rooms(mut bytes: usize, files: usize) -> Vec<u64> {
    (0..files.max(1))
        .map(|_| {
            let blocks = (BARE_BYTES + bytes)
                .div_ceil(BLOCK_SIZE)
                .clamp(1, usize::from(MAX_HEADER_BLOCKS));
            bytes = bytes.saturating_sub(blocks * BLOCK_SIZE - BARE_BYTES);
            blocks as u64
        })
        .collect()
}

/// Bytes the upload's first header spends restating, if it is a restating
/// one: a pass over `named`'s indexes, up to its ceiling. `body` is the
/// blocks of pages the upload carries.
pub fn restating(
    chain: &Chain,
    named: &[Named<'_>],
    body: u64,
) -> Option<usize> {
    let walk = chain.walk.range(chain.floor..).count() + 1;
    let stale = chain
        .restated_at
        .is_some_and(|at| at.elapsed() >= RESTATE_AFTER);
    restates(walk, chain.since_restate, body, stale)
        .then(|| pass_len(named.iter().filter_map(|t| t.index.as_deref())))
}

/// Write every file's roots block and header, in order, advancing `chain`
/// as it goes (see the module docs); the last ends the batch if the upload
/// is a cut and nothing is left to name, and becomes the root its roots
/// block names. `chain` ends at the last file. `restating`: the first
/// header is a restating one. Returns whether the headers named everything.
///
/// # Errors
/// [`StorageError::Corrupt`] when a header outgrew its blocks
/// (unreachable: every entry and record is budgeted).
pub fn finish(
    packed: &mut Packed,
    named: &mut [Named<'_>],
    chain: &mut Chain,
    (position, restating): (u32, bool),
) -> StorageResult<bool> {
    let files = packed.files.len();
    let mut all = true;
    for (i, file) in packed.files.iter_mut().enumerate() {
        let base = file.number * FILE_BLOCKS;
        let blocks = u16::try_from(file.header).unwrap_or(MAX_HEADER_BLOCKS);
        let link = HeaderLink::new(file.base, blocks);
        let room = file.header as usize * BLOCK_SIZE;
        let mut header = Header {
            at: file.base,
            blocks,
            prev: chain.tail.unwrap_or(HeaderLink::NONE),
            horizon: 0,
            shard: position,
            roots: chain.roots.unwrap_or(NO_FILE),
            end_of_batch: false,
            types: Vec::new(),
            remote: Vec::new(),
            chain: None,
        };
        let mut left = room.saturating_sub(header.encoded_len());
        for t in named.iter_mut() {
            name(t, chain, &mut header.types, &mut left);
        }
        all = named.iter().all(|t| {
            !t.dict_moved
                && t.index.as_ref().is_none_or(|i| !i.has_unrecorded())
        });
        header.end_of_batch = packed.cut && all && i + 1 == files;
        let views: Vec<TypeView<'_>> = named
            .iter()
            .map(|t| TypeView {
                struct_hash: t.struct_hash,
                index: t.index.as_deref(),
                dict: t.dict,
            })
            .collect();
        let spare = room.saturating_sub(header.encoded_len());
        restate(
            &mut header.types,
            spare,
            &views,
            &mut chain.types,
            file.base,
        );
        for entry in &header.types {
            chain.types.entry(entry.struct_hash).or_default().last = link;
        }
        header.horizon = horizon(&views, &chain.types, chain.floor);
        wrote(chain, &header, restating && i == 0);
        let encoded = header.encode();
        if encoded.len() > room {
            return Err(StorageError::Corrupt("a header outgrew its room"));
        }
        let from = if file.roots {
            SECTION as usize * BLOCK_SIZE
        } else {
            0
        };
        file.bytes[from..from + encoded.len()].copy_from_slice(&encoded);
        chain.tail = Some(link);
        if header.end_of_batch {
            chain.cut = Some(link);
        }
        if !file.roots {
            continue; // the storage shard heads the file it lands in
        }
        let roots = Roots::new(
            base,
            chain.roots.unwrap_or(NO_FILE),
            vec![chain.cut.unwrap_or(HeaderLink::NONE)],
        )
        .with_chain(chain.start, packed.end)
        .encode()?;
        file.bytes[..BLOCK_SIZE].copy_from_slice(&roots);
        chain.roots = Some(base);
        chain.end = packed.end;
    }
    Ok(all)
}

/// `header` was written: the floor rises to its horizon, the walk keeps
/// the headers at or above it, and the restating count moves on.
fn wrote(chain: &mut Chain, header: &Header, restating: bool) {
    chain.floor = header.horizon;
    chain.walk.insert(header.at);
    chain.walk = chain.walk.split_off(&chain.floor);
    if restating {
        chain.since_restate = 0;
        chain.restated_at = Some(Instant::now());
    } else {
        chain.since_restate += 1;
        chain.restated_at.get_or_insert_with(Instant::now);
    }
}

/// Name as much of `t` as `room` holds in an entry of `entries`.
fn name(
    t: &mut Named<'_>,
    chain: &Chain,
    entries: &mut Vec<TypeEntry>,
    room: &mut usize,
) {
    let journaled = t.index.as_ref().is_some_and(|i| i.has_unrecorded());
    if (!journaled && !t.dict_moved) || *room < ENTRY_BYTES {
        return;
    }
    *room -= ENTRY_BYTES;
    let (changed, ranges) = t.index.as_mut().map_or((Vec::new(), 0), |index| {
        let (records, used) = index.drain(*room);
        *room -= used;
        (records, index.range_count() as u32)
    });
    t.dict_moved = false;
    entries.push(TypeEntry {
        struct_hash: t.struct_hash,
        ranges,
        last: chain.last(t.struct_hash),
        dict: t.dict,
        changed,
    });
}

#[cfg(test)]
mod tests {
    use super::rooms;
    use crate::block::BLOCK_SIZE;
    use crate::header::BARE_BYTES;
    use crate::header_room::MAX_HEADER_BLOCKS;

    /// What the headers name goes to the first ones, a block at least each,
    /// none past the cap.
    #[test]
    fn the_first_headers_take_what_the_upload_names() {
        assert_eq!(rooms(0, 2), [1, 1]);
        assert_eq!(rooms(BLOCK_SIZE, 2), [2, 1]);
        let cap = usize::from(MAX_HEADER_BLOCKS) * BLOCK_SIZE - BARE_BYTES;
        assert_eq!(rooms(cap + 1, 3), [32, 1, 1]);
        assert_eq!(rooms(cap + BLOCK_SIZE, 2), [32, 2]);
        assert_eq!(rooms(0, 0), [1], "a layout opens one file at least");
    }
}
