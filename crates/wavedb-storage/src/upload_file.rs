//! An upload's files ([RFC 0070] §3–§5, [RFC 0073] §8): the encoded remote
//! pages laid out in files of `[roots block][32 KiB header][page][page]…`,
//! each page at block boundaries — the header and its pages a file's
//! **section**.
//!
//! - A page is placed while the cursor is below the file's 8 MiB mark; the
//!   first page ending past it closes the file. Nothing checks whether a
//!   page *fits*: the last one runs as far as it is long, so every page
//!   starts inside its file's window and is read from that one object.
//! - The next file starts at the first file boundary at or after the cursor
//!   — the numbers a large page ran over are never used.
//! - The header is always [`HEADER_BYTES`]. [`finish`] writes it
//!   ([`Header`]) once the pages are placed and the object store's indexes
//!   point at them: each names what of the indexes changed as far as its
//!   room goes (RFC 0078 §6), linked to the header before, its spare room
//!   restating the ranges of the types restated longest ago ([RFC 0072] §5).
//! - **The roots block** heads every file ([`Roots`]): the newest header
//!   ending a batch, and where the chain of files stands — what a recovery
//!   roots in.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, BlockDescriptor, Run};
use crate::error::{StorageError, StorageResult};
use crate::header::{Chain, ENTRY_BYTES, Header, NO_FILE, TypeEntry};
use crate::pages::Pages;
use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};
use crate::restate::{TypeView, horizon, restate};
use crate::roots::{ROOTS_BLOCKS, Roots};
use crate::shape::blocks_for;

/// Where a file's section — its header — starts, past the roots block.
pub const SECTION: u64 = ROOTS_BLOCKS;

/// Where a file's first page starts: past the roots block and the header.
pub const FIRST_PAGE: u64 = ROOTS_BLOCKS + HEADER_BLOCKS;

/// One type's images for an upload.
pub struct SlotImages {
    /// Encoded pages, each with the blocks its entry names.
    pub(crate) pages: Vec<(Vec<u8>, u64)>,
    /// The dictionary, when the object store's copy is behind.
    pub(crate) dict: Option<Vec<u8>>,
}

/// Where an image landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placed {
    Page { slot: usize, page: usize, run: Run },
    Dict { slot: usize, desc: BlockDescriptor },
}

/// One file: its pages laid out, its roots block and header written by
/// [`finish`].
pub struct File {
    pub(crate) number: u64,
    /// The block its section — its header — starts at.
    pub(crate) base: u64,
    /// Whether the bytes open with room for the roots block: a file of its
    /// own, not a section a storage shard places among others.
    pub(crate) roots: bool,
    /// The object: room for the roots block and the header, then the
    /// pages.
    pub(crate) bytes: Vec<u8>,
}

/// The files an upload writes, where every image landed, and the remote end
/// after them.
pub struct Packed {
    pub(crate) files: Vec<File>,
    pub(crate) placed: Vec<Placed>,
    pub(crate) end: u64,
    /// Whether the upload ends a batch ([`Header::end_of_batch`]).
    pub(crate) cut: bool,
}

/// One type as an upload's headers name it: its index in the object store,
/// as the upload leaves it, and its dictionary there.
pub struct Named<'a> {
    pub(crate) struct_hash: u64,
    pub(crate) index: Option<&'a mut Pages>,
    pub(crate) dict: Option<u64>,
    /// The dictionary moved in this upload: a header must name it.
    pub(crate) dict_moved: bool,
}

/// Lay `slots` out in files from remote block `end` on.
pub fn layout(end: u64, slots: &[SlotImages]) -> Packed {
    let mut packed = Packed {
        files: Vec::new(),
        placed: Vec::new(),
        end,
        cut: true,
    };
    for (slot, images) in slots.iter().enumerate() {
        if let Some(dict) = &images.dict {
            let run = place(&mut packed, dict, blocks_for(dict.len()));
            packed.placed.push(Placed::Dict {
                slot,
                desc: BlockDescriptor::from_run(run),
            });
        }
        for (page, (bytes, blocks)) in images.pages.iter().enumerate() {
            let run = place(&mut packed, bytes, *blocks);
            packed.placed.push(Placed::Page { slot, page, run });
        }
    }
    packed
}

/// Place `bytes` in `blocks` at the cursor, opening a file when the cursor
/// has passed the last one's 8 MiB mark.
fn place(packed: &mut Packed, bytes: &[u8], blocks: u64) -> Run {
    let fits = packed
        .files
        .last()
        .is_some_and(|file| packed.end < (file.number + 1) * FILE_BLOCKS);
    if !fits {
        let number = packed.end.div_ceil(FILE_BLOCKS);
        packed.end = number * FILE_BLOCKS + FIRST_PAGE;
        packed.files.push(File {
            number,
            base: number * FILE_BLOCKS + SECTION,
            roots: true,
            bytes: vec![0; FIRST_PAGE as usize * BLOCK_SIZE],
        });
    }
    let run = Run::new(packed.end, blocks);
    packed.end += blocks;
    if let Some(file) = packed.files.last_mut() {
        let at = file.bytes.len();
        file.bytes.extend_from_slice(bytes);
        file.bytes.resize(at + blocks as usize * BLOCK_SIZE, 0);
    }
    run
}

/// Write every file's roots block and header, in order, advancing `chain`
/// as it goes: each header links to the one before, names what of
/// `named`'s indexes changed as far as its room goes, spends its spare room
/// restating old ranges, and records the horizon those restatements leave;
/// the last ends the batch if the upload is a cut and nothing is left to
/// name, and becomes the root its roots block names. `chain` ends at the
/// last file. Returns whether the headers named everything.
///
/// # Errors
/// [`StorageError::Corrupt`] when a header outgrew [`HEADER_BYTES`]
/// (unreachable: every entry and record is budgeted).
pub fn finish(
    packed: &mut Packed,
    named: &mut [Named<'_>],
    chain: &mut Chain,
    position: u32,
) -> StorageResult<bool> {
    let files = packed.files.len();
    let mut all = true;
    for (i, file) in packed.files.iter_mut().enumerate() {
        let base = file.number * FILE_BLOCKS;
        let mut header = Header {
            at: file.base,
            prev: chain.tail.unwrap_or(NO_FILE),
            horizon: 0,
            shard: position,
            roots: chain.roots.unwrap_or(NO_FILE),
            end_of_batch: false,
            types: Vec::new(),
            remote: Vec::new(),
            chain: None,
        };
        let mut room = HEADER_BYTES.saturating_sub(header.encoded_len());
        for t in named.iter_mut() {
            name(t, chain, &mut header.types, &mut room);
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
        let spare = HEADER_BYTES.saturating_sub(header.encoded_len());
        restate(
            &mut header.types,
            spare,
            &views,
            &mut chain.types,
            header.at,
        );
        for entry in &header.types {
            chain.types.entry(entry.struct_hash).or_default().last = header.at;
        }
        header.horizon = horizon(&views, &chain.types, chain.floor);
        chain.floor = header.horizon;
        let encoded = header.encode();
        if encoded.len() > HEADER_BYTES {
            return Err(StorageError::Corrupt("a header outgrew its room"));
        }
        let from = if file.roots {
            SECTION as usize * BLOCK_SIZE
        } else {
            0
        };
        file.bytes[from..from + encoded.len()].copy_from_slice(&encoded);
        chain.tail = Some(header.at);
        if header.end_of_batch {
            chain.cut = Some(header.at);
        }
        if !file.roots {
            continue; // the storage shard heads the file it lands in
        }
        let roots = Roots::new(
            base,
            chain.roots.unwrap_or(NO_FILE),
            vec![chain.cut.unwrap_or(NO_FILE)],
        )
        .with_chain(chain.start, packed.end)
        .encode()?;
        file.bytes[..BLOCK_SIZE].copy_from_slice(&roots);
        chain.roots = Some(base);
        chain.end = packed.end;
    }
    Ok(all)
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

/// Move the files laid out from block 0 to the sections a storage shard
/// reserved for them, at `bases` (RFC 0073 §8): every run shifted with its
/// section, and no room for a roots block — the storage shard heads each
/// file it assembles.
pub fn rebase(packed: &mut Packed, bases: &[u64]) {
    let mut shifts = BTreeMap::new();
    for (file, &base) in packed.files.iter_mut().zip(bases) {
        shifts.insert(file.number, (file.base, base));
        file.number = crate::remote::file_of(base);
        file.base = base;
        file.roots = false;
        file.bytes.drain(..BLOCK_SIZE);
    }
    let moved = |start: u64| {
        shifts
            .get(&crate::remote::file_of(start))
            .map_or(start, |&(from, to)| start - from + to)
    };
    for placed in &mut packed.placed {
        match placed {
            Placed::Page { run, .. } => run.start = moved(run.start),
            Placed::Dict { desc, .. } => {
                *desc = desc.moved_to(moved(desc.start()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FIRST_PAGE, Placed, SECTION, SlotImages, layout};
    use crate::block::{BLOCK_SIZE, Run};
    use crate::remote::FILE_BLOCKS;

    fn images(pages: &[u64]) -> SlotImages {
        SlotImages {
            pages: pages
                .iter()
                .map(|&blocks| (vec![7u8; 10], blocks))
                .collect(),
            dict: None,
        }
    }

    /// Pages follow the header, each at the blocks its entry names.
    #[test]
    fn pages_follow_the_header_at_the_blocks_they_are_given() {
        let packed = layout(0, &[images(&[2, 1])]);
        assert_eq!(packed.files.len(), 1);
        assert_eq!(packed.files[0].base, SECTION);
        let runs: Vec<Run> = packed
            .placed
            .iter()
            .filter_map(|p| match p {
                Placed::Page { run, .. } => Some(*run),
                Placed::Dict { .. } => None,
            })
            .collect();
        assert_eq!(
            runs,
            [Run::new(FIRST_PAGE, 2), Run::new(FIRST_PAGE + 2, 1)]
        );
        let len = packed.files[0].bytes.len();
        assert_eq!(len, (FIRST_PAGE as usize + 3) * BLOCK_SIZE);
    }

    /// A page starting before the 8 MiB mark runs past it; the next starts
    /// a file on the next boundary.
    #[test]
    fn a_page_starting_before_the_mark_runs_past_it() {
        let first = FILE_BLOCKS - FIRST_PAGE - 1;
        let packed = layout(0, &[images(&[first, 4, 1])]);
        assert_eq!(packed.files.len(), 2);
        assert_eq!(packed.files[1].number, 2, "file 1's numbers were run over");
    }

    /// An upload starts on the first file boundary at or after the end.
    #[test]
    fn an_upload_starts_on_the_next_file_boundary() {
        let packed = layout(FILE_BLOCKS + 5, &[images(&[1])]);
        assert_eq!(packed.files[0].number, 2);
    }
}
