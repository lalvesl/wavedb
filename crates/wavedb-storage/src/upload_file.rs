//! An upload's files ([RFC 0070] §3–§5, [RFC 0073] §8): the encoded remote
//! pages laid out in files of `[roots block][header][page][page]…`,
//! each page at block boundaries — the header and its pages a file's
//! **section**.
//!
//! - A page is placed while the cursor is below the file's 8 MiB mark; the
//!   first page ending past it closes the file. Nothing checks whether a
//!   page *fits*: the last one runs as far as it is long, so every page
//!   starts inside its file's window and is read from that one object.
//! - The next file starts at the first file boundary at or after the cursor
//!   — the numbers a large page ran over are never used.
//! - The header takes whole blocks, as many as the upload says for that
//!   file — what it names, sized once the pages are placed
//!   ([RFC 0079] §3, `crate::upload_header`). `finish` writes it
//!   ([`Header`](crate::header::Header)) once the pages are placed and the object store's indexes
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
//! [RFC 0079]: ../../../rfcs/0079-headers-sized-to-their-change-WIP.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, Run};
use crate::pages::Pages;
use crate::remote::FILE_BLOCKS;
use crate::roots::ROOTS_BLOCKS;

/// Where a file's section — its header — starts, past the roots block.
pub const SECTION: u64 = ROOTS_BLOCKS;

/// Where a file's first page starts with a one-block header: past the roots
/// block and the header.
#[cfg(test)]
pub const FIRST_PAGE: u64 = ROOTS_BLOCKS + 1;

/// One type's images for an upload.
pub struct SlotImages {
    /// Encoded pages, each with the blocks its entry names.
    pub(crate) pages: Vec<(Vec<u8>, u64)>,
}

/// Where a page landed: the work item's, its page, its run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placed {
    pub(crate) slot: usize,
    pub(crate) page: usize,
    pub(crate) run: Run,
}

/// One file: its pages laid out, its roots block and header written by
/// [`finish`].
pub struct File {
    pub(crate) number: u64,
    /// The block its section — its header — starts at.
    pub(crate) base: u64,
    /// Blocks its header takes.
    pub(crate) header: u64,
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
/// as the upload leaves it.
pub struct Named<'a> {
    pub(crate) struct_hash: u64,
    pub(crate) index: Option<&'a mut Pages>,
}

/// Lay `slots` out in files from remote block `end` on, the `i`-th file's
/// header `rooms[i]` blocks long (one past the last given). One file at
/// least, pages or none: an upload of removals alone still has a header
/// to write, naming the entries it took out.
pub fn layout(end: u64, slots: &[SlotImages], rooms: &[u64]) -> Packed {
    let mut packed = Packed {
        files: Vec::new(),
        placed: Vec::new(),
        end,
        cut: true,
    };
    for (slot, images) in slots.iter().enumerate() {
        for (page, (bytes, blocks)) in images.pages.iter().enumerate() {
            let run = place(&mut packed, bytes, *blocks, rooms);
            packed.placed.push(Placed { slot, page, run });
        }
    }
    if packed.files.is_empty() {
        open_file(&mut packed, rooms);
    }
    packed
}

/// Place `bytes` in `blocks` at the cursor, opening a file when the cursor
/// has passed the last one's 8 MiB mark.
fn place(packed: &mut Packed, bytes: &[u8], blocks: u64, rooms: &[u64]) -> Run {
    let fits = packed
        .files
        .last()
        .is_some_and(|file| packed.end < (file.number + 1) * FILE_BLOCKS);
    if !fits {
        open_file(packed, rooms);
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

/// Open the next file at the cursor: its roots block and header, the
/// header as long as `rooms` asks for it.
fn open_file(packed: &mut Packed, rooms: &[u64]) {
    let number = packed.end.div_ceil(FILE_BLOCKS);
    let header = rooms.get(packed.files.len()).copied().unwrap_or(1);
    let first = SECTION + header;
    packed.end = number * FILE_BLOCKS + first;
    packed.files.push(File {
        number,
        base: number * FILE_BLOCKS + SECTION,
        header,
        roots: true,
        bytes: vec![0; first as usize * BLOCK_SIZE],
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
        placed.run.start = moved(placed.run.start);
    }
}

#[cfg(test)]
mod tests {
    use super::{FIRST_PAGE, SECTION, SlotImages, layout};
    use crate::block::{BLOCK_SIZE, Run};
    use crate::remote::FILE_BLOCKS;

    fn images(pages: &[u64]) -> SlotImages {
        SlotImages {
            pages: pages
                .iter()
                .map(|&blocks| (vec![7u8; 10], blocks))
                .collect(),
        }
    }

    /// Pages follow the header, each at the blocks its entry names.
    #[test]
    fn pages_follow_the_header_at_the_blocks_they_are_given() {
        let packed = layout(0, &[images(&[2, 1])], &[]);
        assert_eq!(packed.files.len(), 1);
        assert_eq!(packed.files[0].base, SECTION);
        let runs: Vec<Run> = packed.placed.iter().map(|p| p.run).collect();
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
        let packed = layout(0, &[images(&[first, 4, 1])], &[]);
        assert_eq!(packed.files.len(), 2);
        assert_eq!(packed.files[1].number, 2, "file 1's numbers were run over");
    }

    /// An upload starts on the first file boundary at or after the end.
    #[test]
    fn an_upload_starts_on_the_next_file_boundary() {
        let packed = layout(FILE_BLOCKS + 5, &[images(&[1])], &[]);
        assert_eq!(packed.files[0].number, 2);
    }

    /// An upload of removals alone places no page, and still opens a file
    /// for the header that names them.
    #[test]
    fn an_upload_with_no_page_still_opens_a_file() {
        let packed = layout(FILE_BLOCKS + 5, &[images(&[])], &[]);
        assert_eq!(packed.files.len(), 1);
        assert_eq!(packed.files[0].number, 2);
        assert_eq!(packed.placed.len(), 0);
        assert_eq!(packed.end, 2 * FILE_BLOCKS + FIRST_PAGE);
    }
}
