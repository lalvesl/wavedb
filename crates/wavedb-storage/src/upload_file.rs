//! An upload's files ([RFC 0070] §3–§5): the encoded remote pages laid out
//! in files of `[32 KiB header][page][page]…`, each page block-aligned.
//!
//! - A page is placed while the cursor is below the file's 8 MiB mark; the
//!   first page ending past it closes the file. Nothing checks whether a
//!   page *fits*: the last one runs as far as it is long, so every page
//!   starts inside its file's window and is read from that one object.
//! - The next file starts at the first file boundary at or after the cursor
//!   — the numbers a large page ran over are never used.
//! - The header is the file's addressing delta and is always
//!   [`HEADER_BYTES`]; a file also closes when the next page's entry would
//!   not fit it, so no schema can overflow one.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

use std::collections::BTreeMap;

use wavedb_core::wire::{CRC_PREFIX_LEN, WaveWire, to_wire_checked};

use crate::block::{BLOCK_SIZE, BlockDescriptor};
use crate::edit::SlotEdit;
use crate::plan::blocks_of;
use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};

/// The head of every uploaded file: what it changed in the remote
/// directories, and the oldest file still live when it was written.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct FileHeader {
    /// The oldest file not deleted when this one was written. It can lag
    /// behind the truth, never run ahead of it (§4). Nothing deletes files
    /// before compaction by file (phase 7), so it is the first file there is.
    pub first_file: u64,
    /// Per type: the remote directory's bucket count after the upload, the
    /// `(bucket, remote descriptor)` of each page in this file, and the
    /// dictionary's descriptor when this file carries it.
    pub slots: Vec<SlotEdit>,
}

/// One type's images for an upload.
pub struct SlotImages {
    pub(crate) struct_hash: u64,
    /// The remote directory's bucket count after the upload.
    pub(crate) buckets: u32,
    /// Bucket → encoded page; empty bytes leave the bucket vacant.
    pub(crate) pages: BTreeMap<usize, Vec<u8>>,
    /// The dictionary, when the object store's copy is behind.
    pub(crate) dict: Option<Vec<u8>>,
}

/// Where an image landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placed {
    Page {
        slot: usize,
        bucket: usize,
        desc: BlockDescriptor,
    },
    Dict {
        slot: usize,
        desc: BlockDescriptor,
    },
}

/// One file, ready for its PUT.
pub struct File {
    pub(crate) number: u64,
    pub(crate) bytes: Vec<u8>,
}

/// The files an upload writes, where every image landed, and the remote end
/// after them.
pub struct Packed {
    pub(crate) files: Vec<File>,
    pub(crate) placed: Vec<Placed>,
    pub(crate) end: u64,
}

/// A file being filled.
struct Open {
    number: u64,
    header: FileHeader,
    /// The header's encoded length, kept as entries are added.
    len: usize,
    /// The object being built: room for the header, then the pages — the
    /// header goes in when the file closes, so nothing is copied twice.
    body: Vec<u8>,
}

/// Lay `slots` out in files from remote block `end` on.
pub fn pack(end: u64, slots: &[SlotImages]) -> Packed {
    let mut packed = Packed {
        files: Vec::new(),
        placed: Vec::new(),
        end,
    };
    let mut open: Option<Open> = None;
    for (slot, images) in slots.iter().enumerate() {
        let dict = images.dict.iter().map(|bytes| (None, bytes));
        let pages = images.pages.iter().map(|(&b, bytes)| (Some(b), bytes));
        for (bucket, bytes) in dict.chain(pages) {
            let file = room(&mut open, &mut packed, images, bucket.is_none());
            // An emptied bucket takes an entry and no bytes.
            let desc = if bytes.is_empty() {
                BlockDescriptor::EMPTY
            } else {
                BlockDescriptor::new(packed.end, blocks_of(bytes))
            };
            file.len += growth(&file.header, images, bucket.is_none());
            let edit = entry(&mut file.header, images);
            if let Some(bucket) = bucket {
                edit.changed.push((bucket as u32, desc.raw()));
                packed.placed.push(Placed::Page { slot, bucket, desc });
            } else {
                edit.dict = Some(desc.raw());
                packed.placed.push(Placed::Dict { slot, desc });
            }
            file.body.extend_from_slice(bytes);
            file.body
                .resize(file.body.len().next_multiple_of(BLOCK_SIZE), 0);
            packed.end += desc.count();
        }
    }
    if let Some(file) = open {
        close(file, &mut packed);
    }
    packed
}

/// The open file, closing it first when the cursor has passed its 8 MiB
/// mark or the next entry would not fit its header.
fn room<'a>(
    open: &'a mut Option<Open>,
    packed: &mut Packed,
    images: &SlotImages,
    dict: bool,
) -> &'a mut Open {
    if let Some(file) = open.take() {
        let past = packed.end >= (file.number + 1) * FILE_BLOCKS;
        let full = file.len + growth(&file.header, images, dict) > HEADER_BYTES;
        if past || full {
            close(file, packed);
        } else {
            *open = Some(file);
        }
    }
    open.get_or_insert_with(|| start(packed))
}

/// Open the next file at the first boundary at or after the cursor.
fn start(packed: &mut Packed) -> Open {
    let number = packed.end.div_ceil(FILE_BLOCKS);
    packed.end = number * FILE_BLOCKS + HEADER_BLOCKS;
    let header = FileHeader {
        first_file: 0,
        slots: Vec::new(),
    };
    Open {
        number,
        len: 4 + CRC_PREFIX_LEN + FileHeader::STACK_SIZE + header.heap_size(),
        header,
        body: vec![0; HEADER_BYTES],
    }
}

/// Header and body, as the object's bytes.
fn close(file: Open, packed: &mut Packed) {
    let Open {
        number,
        header,
        len,
        body: mut bytes,
    } = file;
    let header = encode_header(&header);
    debug_assert_eq!(header.len(), len, "the header's running length");
    bytes[..header.len()].copy_from_slice(&header);
    packed.files.push(File { number, bytes });
}

/// The header's entry for `images`' type, added if it has none yet.
fn entry<'a>(
    header: &'a mut FileHeader,
    images: &SlotImages,
) -> &'a mut SlotEdit {
    let at = header
        .slots
        .iter()
        .position(|s| s.struct_hash == images.struct_hash)
        .unwrap_or_else(|| {
            header.slots.push(SlotEdit {
                struct_hash: images.struct_hash,
                buckets: images.buckets,
                changed: Vec::new(),
                dict: None,
            });
            header.slots.len() - 1
        });
    &mut header.slots[at]
}

/// Bytes one more entry for `images`' type — a page, or its dictionary —
/// adds to `header`'s encoding: a `(bucket, descriptor)` pair, or the
/// dictionary's descriptor, plus the type's own entry the first time.
fn growth(header: &FileHeader, images: &SlotImages, dict: bool) -> usize {
    let present = header
        .slots
        .iter()
        .find(|s| s.struct_hash == images.struct_hash);
    let own = if present.is_some() {
        0
    } else {
        SlotEdit::STACK_SIZE
    };
    let item = if !dict {
        <(u32, u64)>::STACK_SIZE
    } else if present.and_then(|s| s.dict).is_some() {
        0
    } else {
        <u64>::STACK_SIZE
    };
    own + item
}

/// `[len u32 LE][crc32][wire]` — the envelope an edit chunk uses.
pub fn encode_header(header: &FileHeader) -> Vec<u8> {
    let payload = to_wire_checked(header);
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

#[cfg(test)]
mod tests {
    use wavedb_core::wire::from_wire_checked;

    use super::{FileHeader, Placed, SlotImages, encode_header, pack};
    use crate::block::{BLOCK_SIZE, BlockDescriptor};
    use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};

    fn images(hash: u64, pages: &[(usize, usize)]) -> SlotImages {
        SlotImages {
            struct_hash: hash,
            buckets: 64,
            pages: pages
                .iter()
                .map(|&(bucket, blocks)| {
                    (bucket, vec![7u8; blocks * BLOCK_SIZE])
                })
                .collect(),
            dict: None,
        }
    }

    fn header_of(bytes: &[u8]) -> FileHeader {
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        from_wire_checked(&bytes[4..4 + len]).unwrap()
    }

    fn page_desc(placed: &[Placed], want: usize) -> BlockDescriptor {
        placed
            .iter()
            .find_map(|p| match *p {
                Placed::Page { bucket, desc, .. } if bucket == want => {
                    Some(desc)
                }
                _ => None,
            })
            .unwrap()
    }

    /// Pages follow the header, block-aligned, and the header names each.
    #[test]
    fn pages_follow_the_header_and_the_header_names_them() {
        let packed = pack(0, &[images(9, &[(0, 2), (3, 1)])]);
        assert_eq!(packed.files.len(), 1);
        let file = &packed.files[0];
        assert_eq!(file.number, 0);
        assert_eq!(file.bytes.len(), HEADER_BYTES + 3 * BLOCK_SIZE);
        assert_eq!(
            page_desc(&packed.placed, 0),
            BlockDescriptor::new(HEADER_BLOCKS, 2)
        );
        assert_eq!(
            page_desc(&packed.placed, 3),
            BlockDescriptor::new(HEADER_BLOCKS + 2, 1)
        );
        assert_eq!(packed.end, HEADER_BLOCKS + 3);

        let header = header_of(&file.bytes);
        assert_eq!(header.slots.len(), 1);
        assert_eq!(header.slots[0].buckets, 64);
        assert_eq!(
            header.slots[0].changed,
            vec![
                (0, BlockDescriptor::new(HEADER_BLOCKS, 2).raw()),
                (3, BlockDescriptor::new(HEADER_BLOCKS + 2, 1).raw()),
            ]
        );
    }

    /// The bound is on where a page starts: the one starting before 8 MiB
    /// runs past it in the same file, and the next file starts at the first
    /// boundary after it — the numbers it ran over are skipped.
    #[test]
    fn a_page_starting_before_the_mark_runs_past_it() {
        let big = (FILE_BLOCKS * 3) as usize; // 24 MiB
        let packed = pack(0, &[images(9, &[(0, 1), (1, big), (2, 1)])]);
        assert_eq!(packed.files.len(), 2);
        assert_eq!(packed.files[0].number, 0);
        let first = page_desc(&packed.placed, 1);
        assert_eq!(first.start(), HEADER_BLOCKS + 1);
        assert_eq!(
            packed.files[0].bytes.len(),
            HEADER_BYTES + (1 + big) * BLOCK_SIZE,
            "one object holds the whole large page"
        );
        let next = packed.files[1].number;
        assert_eq!(
            next,
            first
                .start()
                .saturating_add(first.count())
                .div_ceil(FILE_BLOCKS)
        );
        assert_eq!(next, 4, "files 1 to 3 are never used");
        assert_eq!(
            page_desc(&packed.placed, 2).start(),
            4 * FILE_BLOCKS + HEADER_BLOCKS
        );
    }

    /// An upload resumes at the end the last one left, on a fresh file.
    #[test]
    fn an_upload_starts_on_the_next_file_boundary() {
        let packed = pack(HEADER_BLOCKS + 5, &[images(9, &[(0, 1)])]);
        assert_eq!(packed.files[0].number, 1);
        assert_eq!(
            page_desc(&packed.placed, 0).start(),
            FILE_BLOCKS + HEADER_BLOCKS
        );
    }

    /// Thousands of types with one tiny page each would outgrow 32 KiB of
    /// header long before 8 MiB of pages: the file closes on its header.
    #[test]
    fn a_full_header_closes_the_file() {
        let slots: Vec<SlotImages> =
            (0..1500).map(|h| images(h, &[(0, 1)])).collect();
        let packed = pack(0, &slots);
        assert!(packed.files.len() > 1, "{} files", packed.files.len());
        for file in &packed.files {
            let len = encode_header(&header_of(&file.bytes)).len();
            assert!(len <= HEADER_BYTES, "header of {len} bytes");
        }
        let named: usize = packed
            .files
            .iter()
            .map(|f| header_of(&f.bytes).slots.len())
            .sum();
        assert_eq!(named, 1500, "every type is named exactly once");
    }

    /// An emptied bucket costs an entry and no bytes; a dictionary is an
    /// image like a page, named by its own field.
    #[test]
    fn emptied_buckets_and_dictionaries_take_their_entries() {
        let mut slot = images(9, &[(0, 1)]);
        slot.pages.insert(5, Vec::new());
        slot.dict = Some(vec![1u8; 100]);
        let packed = pack(0, &[slot]);
        let header = header_of(&packed.files[0].bytes);
        let edit = &header.slots[0];
        assert_eq!(
            edit.dict,
            Some(BlockDescriptor::new(HEADER_BLOCKS, 1).raw())
        );
        assert!(edit.changed.contains(&(5, BlockDescriptor::EMPTY.raw())));
        assert_eq!(page_desc(&packed.placed, 0).start(), HEADER_BLOCKS + 1);
        assert_eq!(packed.files[0].bytes.len(), HEADER_BYTES + 2 * BLOCK_SIZE);
    }
}
