//! An upload's files ([RFC 0070] §3–§5): the encoded remote pages laid out
//! in files of `[32 KiB header][page][page]…`, each page block-aligned.
//!
//! - A page is placed while the cursor is below the file's 8 MiB mark; the
//!   first page ending past it closes the file. Nothing checks whether a
//!   page *fits*: the last one runs as far as it is long, so every page
//!   starts inside its file's window and is read from that one object.
//! - The next file starts at the first file boundary at or after the cursor
//!   — the numbers a large page ran over are never used.
//! - The header is always [`HEADER_BYTES`]. [`layout`] budgets it for every
//!   type's entry plus the file's own items and closes a file that would
//!   outgrow it; [`finish`] then writes it ([`FileHeader`]): chained to the
//!   file before, and its spare room restating the entries of the type
//!   mentioned longest ago.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

use std::collections::BTreeMap;

use wavedb_core::wire::{CRC_PREFIX_LEN, WaveWire};

use crate::block::{BLOCK_SIZE, BlockDescriptor};
use crate::error::{StorageError, StorageResult};
use crate::file_header::{
    Chain, ENTRY_BYTES, FileHeader, ITEM_BYTES, NO_FILE, TypeEntry, header_hash,
};
use crate::plan::blocks_of;
use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};

/// One type's images for an upload.
pub struct SlotImages {
    pub(crate) struct_hash: u64,
    /// Bucket → encoded page; empty bytes leave the bucket vacant.
    pub(crate) pages: BTreeMap<usize, Vec<u8>>,
    /// The dictionary, when the object store's copy is behind.
    pub(crate) dict: Option<Vec<u8>>,
}

/// A type's remote directory as it stands once the upload is in — what a
/// header names, and restates from.
pub struct TypeView<'a> {
    pub(crate) struct_hash: u64,
    pub(crate) slots: &'a [BlockDescriptor],
    /// Its dictionary's remote descriptor, if it has one.
    pub(crate) dict: Option<u64>,
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

/// One file: its pages laid out, its header written by [`finish`].
pub struct File {
    pub(crate) number: u64,
    /// Struct hash → the `(bucket, descriptor)` items of the pages it holds.
    items: BTreeMap<u64, Vec<(u32, u64)>>,
    /// The header's encoded length, budgeted as items are added.
    len: usize,
    /// The object: room for the header, then the pages.
    pub(crate) bytes: Vec<u8>,
}

/// The files an upload writes, where every image landed, and the remote end
/// after them.
pub struct Packed {
    pub(crate) files: Vec<File>,
    pub(crate) placed: Vec<Placed>,
    pub(crate) end: u64,
    /// Whether the upload ends a cut ([`FileHeader::cut`]).
    pub(crate) cut: bool,
}

/// Lay `slots` out in files from remote block `end` on, budgeting each
/// header for `types` type entries.
pub fn layout(end: u64, slots: &[SlotImages], types: usize) -> Packed {
    let mut packed = Packed {
        files: Vec::new(),
        placed: Vec::new(),
        end,
        cut: true,
    };
    for (slot, images) in slots.iter().enumerate() {
        let dict = images.dict.iter().map(|bytes| (None, bytes));
        let pages = images.pages.iter().map(|(&b, bytes)| (Some(b), bytes));
        for (bucket, bytes) in dict.chain(pages) {
            open_room(&mut packed, types);
            // An emptied bucket takes an item and no bytes.
            let desc = if bytes.is_empty() {
                BlockDescriptor::EMPTY
            } else {
                BlockDescriptor::new(packed.end, blocks_of(bytes))
            };
            packed.end += desc.count();
            let Some(file) = packed.files.last_mut() else {
                continue;
            };
            if let Some(bucket) = bucket {
                let items = file.items.entry(images.struct_hash).or_default();
                items.push((bucket as u32, desc.raw()));
                file.len += ITEM_BYTES;
                packed.placed.push(Placed::Page { slot, bucket, desc });
            } else {
                packed.placed.push(Placed::Dict { slot, desc });
            }
            file.bytes.extend_from_slice(bytes);
            let aligned = file.bytes.len().next_multiple_of(BLOCK_SIZE);
            file.bytes.resize(aligned, 0);
        }
    }
    packed
}

/// Make sure the last file can take the next image, opening a new one when
/// the cursor has passed its 8 MiB mark or one more item would outgrow its
/// header.
fn open_room(packed: &mut Packed, types: usize) {
    let fits = packed.files.last().is_some_and(|file| {
        packed.end < (file.number + 1) * FILE_BLOCKS
            && file.len + ITEM_BYTES <= HEADER_BYTES
    });
    if !fits {
        let number = packed.end.div_ceil(FILE_BLOCKS);
        packed.end = number * FILE_BLOCKS + HEADER_BLOCKS;
        packed.files.push(File {
            number,
            items: BTreeMap::new(),
            len: bare_len() + types * ENTRY_BYTES,
            bytes: vec![0; HEADER_BYTES],
        });
    }
}

/// A header's length with no type in it.
const fn bare_len() -> usize {
    4 + CRC_PREFIX_LEN + FileHeader::STACK_SIZE
}

/// Write every file's header, in order, advancing `chain` as it goes: each
/// header links to the one before, names every type in `views`, and spends
/// its spare room restating old entries. `chain` ends at the last file.
///
/// # Errors
/// [`StorageError::Corrupt`] when a header outgrew [`HEADER_BYTES`] — more
/// types than one header holds (about 990).
pub fn finish(
    packed: &mut Packed,
    views: &[TypeView<'_>],
    chain: &mut Chain,
) -> StorageResult<()> {
    let group = packed.files.first().map_or(NO_FILE, |f| f.number);
    let group_files = packed.files.len() as u32;
    for file in &mut packed.files {
        let (prev, prev_hash) = chain.tail.unwrap_or((NO_FILE, 0));
        let mut header = FileHeader {
            file: file.number,
            group,
            group_files,
            cut: packed.cut,
            prev,
            prev_hash,
            start: chain.start,
            end: packed.end,
            types: views
                .iter()
                .map(|v| TypeEntry {
                    struct_hash: v.struct_hash,
                    buckets: v.slots.len() as u32,
                    last: chain
                        .last
                        .get(&v.struct_hash)
                        .copied()
                        .unwrap_or(NO_FILE),
                    dict: v.dict,
                    changed: file
                        .items
                        .remove(&v.struct_hash)
                        .unwrap_or_default(),
                })
                .collect(),
        };
        refresh(&mut header, views, chain);
        for entry in header.types.iter().filter(|e| !e.changed.is_empty()) {
            chain.last.insert(entry.struct_hash, file.number);
        }
        let encoded = header.encode();
        if encoded.len() > HEADER_BYTES {
            return Err(StorageError::Corrupt(
                "a file header outgrew its room",
            ));
        }
        file.bytes[..encoded.len()].copy_from_slice(&encoded);
        chain.tail = Some((file.number, header_hash(&encoded)));
    }
    chain.end = packed.end;
    Ok(())
}

/// Spend `header`'s spare room restating entries: the type whose last
/// whole rotation is oldest first, from its rotating cursor, then the next,
/// until the room runs out or every type had its turn in this header.
///
/// The order is by rotation, not by mention: a type written in every upload
/// is always mentioned, yet its untouched buckets are restated only on its
/// turn — ordered by mention, it could lose every tie for ever, and the
/// recovery horizon would never pass the files its old entries are in
/// (RFC 0070 §8). A type that never finished a rotation since the store
/// opened goes first.
fn refresh(header: &mut FileHeader, views: &[TypeView<'_>], chain: &mut Chain) {
    let mut spare = HEADER_BYTES.saturating_sub(header.encoded_len());
    let mut order: Vec<usize> = (0..views.len()).collect();
    order.sort_by_key(|&i| {
        chain
            .rotated
            .get(&views[i].struct_hash)
            .map_or(0, |&f| f.saturating_add(1))
    });
    for i in order {
        let view = &views[i];
        let buckets = view.slots.len();
        if spare < ITEM_BYTES {
            break;
        }
        let n = (spare / ITEM_BYTES).min(buckets);
        if n == 0 {
            continue;
        }
        let cursor = chain.cursor.entry(view.struct_hash).or_insert(0);
        let entry = &mut header.types[i];
        for k in 0..n {
            let bucket = (*cursor + k) % buckets;
            entry
                .changed
                .push((bucket as u32, view.slots[bucket].raw()));
        }
        if *cursor + n >= buckets {
            chain.rotated.insert(view.struct_hash, header.file);
        }
        *cursor = (*cursor + n) % buckets;
        spare -= n * ITEM_BYTES;
    }
}

#[cfg(test)]
mod tests {
    use super::{Packed, Placed, SlotImages, TypeView, finish, layout};
    use crate::block::{BLOCK_SIZE, BlockDescriptor};
    use crate::file_header::{Chain, FileHeader, NO_FILE};
    use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};

    fn images(hash: u64, pages: &[(usize, usize)]) -> SlotImages {
        SlotImages {
            struct_hash: hash,
            pages: pages
                .iter()
                .map(|&(bucket, blocks)| {
                    (bucket, vec![7u8; blocks * BLOCK_SIZE])
                })
                .collect(),
            dict: None,
        }
    }

    /// A remote directory of `n` buckets, every one placed somewhere.
    fn dir(n: usize) -> Vec<BlockDescriptor> {
        (0..n)
            .map(|b| BlockDescriptor::new(b as u64 + 1, 1))
            .collect()
    }

    fn view(hash: u64, slots: &[BlockDescriptor]) -> TypeView<'_> {
        TypeView {
            struct_hash: hash,
            slots,
            dict: None,
        }
    }

    fn header(packed: &Packed, i: usize) -> FileHeader {
        FileHeader::decode(&packed.files[i].bytes).unwrap().0
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

    /// Pages follow the header, block-aligned, and the header names each
    /// with its final descriptor — plus every type the store holds.
    #[test]
    fn pages_follow_the_header_and_the_header_names_them() {
        let mut packed = layout(0, &[images(9, &[(0, 2), (3, 1)])], 2);
        assert_eq!(packed.files.len(), 1);
        assert_eq!(packed.files[0].bytes.len(), HEADER_BYTES + 3 * BLOCK_SIZE);
        let first = page_desc(&packed.placed, 0);
        assert_eq!(first, BlockDescriptor::new(HEADER_BLOCKS, 2));
        let second = page_desc(&packed.placed, 3);
        assert_eq!(second, BlockDescriptor::new(HEADER_BLOCKS + 2, 1));
        assert_eq!(packed.end, HEADER_BLOCKS + 3);

        let (nine, other) = (dir(4), dir(1));
        let views = [view(5, &other), view(9, &nine)];
        let mut chain = Chain::default();
        finish(&mut packed, &views, &mut chain).unwrap();
        let h = header(&packed, 0);
        assert_eq!((h.file, h.group, h.group_files), (0, 0, 1));
        assert_eq!((h.prev, h.end), (NO_FILE, packed.end));
        assert_eq!(h.types.len(), 2, "every type, placed or not");
        let nine_entry = &h.types[1];
        assert_eq!(nine_entry.buckets, 4);
        assert!(
            nine_entry
                .changed
                .starts_with(&[(0, first.raw()), (3, second.raw())])
        );
        assert_eq!(chain.tail.map(|t| t.0), Some(0));
        assert_eq!(chain.last.get(&9), Some(&0));
    }

    /// The bound is on where a page starts: the one starting before 8 MiB
    /// runs past it in the same file, and the next file starts at the first
    /// boundary after it — the numbers it ran over are skipped.
    #[test]
    fn a_page_starting_before_the_mark_runs_past_it() {
        let big = (FILE_BLOCKS * 3) as usize; // 24 MiB
        let packed = layout(0, &[images(9, &[(0, 1), (1, big), (2, 1)])], 1);
        assert_eq!(packed.files.len(), 2);
        let first = page_desc(&packed.placed, 1);
        assert_eq!(first.start(), HEADER_BLOCKS + 1);
        assert_eq!(
            packed.files[0].bytes.len(),
            HEADER_BYTES + (1 + big) * BLOCK_SIZE,
            "one object holds the whole large page"
        );
        assert_eq!(packed.files[1].number, 4, "files 1 to 3 are never used");
        let next = page_desc(&packed.placed, 2).start();
        assert_eq!(next, 4 * FILE_BLOCKS + HEADER_BLOCKS);
    }

    #[test]
    fn an_upload_starts_on_the_next_file_boundary() {
        let packed = layout(HEADER_BLOCKS + 5, &[images(9, &[(0, 1)])], 1);
        assert_eq!(packed.files[0].number, 1);
        let start = page_desc(&packed.placed, 0).start();
        assert_eq!(start, FILE_BLOCKS + HEADER_BLOCKS);
    }

    /// Nine hundred types with a page each outgrow one header long before
    /// 8 MiB of pages: the file closes on its header, and no header passes
    /// its room.
    #[test]
    fn a_full_header_closes_the_file() {
        let slots: Vec<SlotImages> =
            (0..900).map(|h| images(h, &[(0, 1)])).collect();
        let mut packed = layout(0, &slots, 900);
        assert!(packed.files.len() > 1, "{} files", packed.files.len());
        let one = dir(1);
        let views: Vec<TypeView<'_>> =
            (0..900).map(|h| view(h, &one)).collect();
        finish(&mut packed, &views, &mut Chain::default()).unwrap();
        for i in 0..packed.files.len() {
            assert!(header(&packed, i).encode().len() <= HEADER_BYTES);
        }
    }

    /// An upload's files link to each other and to the upload before, each
    /// by the hash of the header it follows; the group says how many.
    #[test]
    fn headers_chain_by_hash_across_uploads() {
        let one = dir(1);
        let views = [view(9, &one)];
        let mut chain = Chain::default();
        let big = FILE_BLOCKS as usize;
        let mut first = layout(0, &[images(9, &[(0, big), (1, 1)])], 1);
        finish(&mut first, &views, &mut chain).unwrap();
        assert_eq!(first.files.len(), 2);
        let (a, b) = (header(&first, 0), header(&first, 1));
        assert_eq!((b.group, b.group_files, b.prev), (a.file, 2, a.file));
        let a_hash = FileHeader::decode(&first.files[0].bytes).unwrap().1;
        assert_eq!(b.prev_hash, a_hash);

        let mut second = layout(chain.end, &[images(9, &[(0, 1)])], 1);
        finish(&mut second, &views, &mut chain).unwrap();
        let c = header(&second, 0);
        let b_hash = FileHeader::decode(&first.files[1].bytes).unwrap().1;
        assert_eq!((c.prev, c.prev_hash), (b.file, b_hash));
        assert_eq!((c.group, c.group_files), (c.file, 1));
        assert_eq!(c.types[0].last, b.file, "the newest earlier entries");
    }

    /// Spare room restates entries from a rotating cursor, the type
    /// mentioned longest ago first — so a type nobody writes still shows
    /// up near the head of the chain.
    #[test]
    fn spare_room_restates_on_from_the_cursor() {
        let (busy, idle) = (dir(4), dir(5_000));
        let views = [view(1, &busy), view(2, &idle)];
        let mut chain = Chain::default();
        chain.last.insert(1, 50);
        chain.last.insert(2, 10);
        chain.end = 60 * FILE_BLOCKS;
        let mut packed = layout(chain.end, &[images(1, &[(0, 1)])], 2);
        finish(&mut packed, &views, &mut chain).unwrap();
        let h = header(&packed, 0);
        let idle_entry = &h.types[1];
        assert_eq!(idle_entry.last, 10);
        let restated = idle_entry.changed.len();
        assert!(
            restated > 2_000,
            "the idle type filled the room: {restated}"
        );
        assert_eq!(idle_entry.changed[0], (0, idle[0].raw()));
        assert_eq!(chain.last.get(&2), Some(&h.file));
        assert_eq!(chain.cursor.get(&2), Some(&restated));

        // The next file continues where the cursor stopped.
        let mut next = layout(chain.end, &[images(1, &[(0, 1)])], 2);
        finish(&mut next, &views, &mut chain).unwrap();
        let h = header(&next, 0);
        let first = h.types[1].changed.first().copied();
        assert_eq!(first, Some((restated as u32, idle[restated].raw())));
    }

    /// A type written in every upload is mentioned in every header, and
    /// ties with the types restated there; ordered by mention it lost every
    /// tie to the lower-indexed ones and its untouched buckets were never
    /// restated — the recovery horizon stuck below them. Ordered by the
    /// oldest whole rotation, every type comes round.
    #[test]
    fn a_type_written_every_upload_still_comes_round() {
        let (a, b, busy) = (dir(3_000), dir(3_000), dir(3_000));
        let views = [view(1, &a), view(2, &b), view(3, &busy)];
        let mut chain = Chain::default();
        for _ in 0..12 {
            let mut packed = layout(chain.end, &[images(3, &[(0, 1)])], 3);
            finish(&mut packed, &views, &mut chain).unwrap();
        }
        for hash in [1, 2, 3] {
            assert!(
                chain.rotated.contains_key(&hash),
                "type {hash} never came round: {:?}",
                chain.rotated
            );
        }
    }
}
