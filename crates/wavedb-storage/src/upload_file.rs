//! An upload's files ([RFC 0070] §3–§5, [RFC 0073] §8): the encoded remote
//! pages laid out in files of `[roots block][32 KiB header][page][page]…`,
//! each page block-aligned — the header and its pages a file's **section**.
//!
//! - A page is placed while the cursor is below the file's 8 MiB mark; the
//!   first page ending past it closes the file. Nothing checks whether a
//!   page *fits*: the last one runs as far as it is long, so every page
//!   starts inside its file's window and is read from that one object.
//! - The next file starts at the first file boundary at or after the cursor
//!   — the numbers a large page ran over are never used.
//! - The header is always [`HEADER_BYTES`]. [`layout`] budgets it for the
//!   entries of the types the file holds pages of and their items, and
//!   closes a file that would outgrow it; [`finish`] then writes it
//!   ([`Header`]): at its block, linked to the header before, and its spare
//!   room restating the addresses of the types restated longest ago
//!   ([RFC 0072] §5).
//! - **The roots block** heads every file ([`Roots`]): the newest header
//!   ending a batch, and where the chain of files stands — what a recovery
//!   roots in.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point.md
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, BlockDescriptor};
use crate::error::{StorageError, StorageResult};
use crate::header::{
    BARE_BYTES, Chain, ENTRY_BYTES, Header, ITEM_BYTES, NO_FILE, TypeEntry,
    depth_of,
};
use crate::plan::blocks_of;
use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};
pub use crate::restate::TypeView;
use crate::restate::{horizon, restate};
use crate::roots::{ROOTS_BLOCKS, Roots};

/// Where a file's section — its header — starts, past the roots block.
pub const SECTION: u64 = ROOTS_BLOCKS;

/// Where a file's first page starts: past the roots block and the header.
pub const FIRST_PAGE: u64 = ROOTS_BLOCKS + HEADER_BLOCKS;

/// One type's images for an upload.
pub struct SlotImages {
    pub(crate) struct_hash: u64,
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

/// One file: its pages laid out, its roots block and header written by
/// [`finish`].
pub struct File {
    pub(crate) number: u64,
    /// The block its section — its header — starts at.
    pub(crate) base: u64,
    /// Whether the bytes open with room for the roots block: a file of its
    /// own, not a section a storage shard places among others.
    pub(crate) roots: bool,
    /// Struct hash → the `(bucket, descriptor)` items of the pages it holds;
    /// a type whose dictionary it holds has an entry, items or not.
    items: BTreeMap<u64, Vec<(u32, u64)>>,
    /// The header's encoded length, budgeted as items are added.
    len: usize,
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

/// Lay `slots` out in files from remote block `end` on.
pub fn layout(end: u64, slots: &[SlotImages]) -> Packed {
    let mut packed = Packed {
        files: Vec::new(),
        placed: Vec::new(),
        end,
        cut: true,
    };
    for (slot, images) in slots.iter().enumerate() {
        let hash = images.struct_hash;
        let dict = images.dict.iter().map(|bytes| (None, bytes));
        let pages = images.pages.iter().map(|(&b, bytes)| (Some(b), bytes));
        for (bucket, bytes) in dict.chain(pages) {
            let item = if bucket.is_some() { ITEM_BYTES } else { 0 };
            open_room(&mut packed, hash, item);
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
            file.len += cost(file, hash, item);
            let items = file.items.entry(hash).or_default();
            if let Some(bucket) = bucket {
                items.push((bucket as u32, desc.raw()));
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

/// What adding `item` bytes of `hash` costs `file`'s header: the item, and
/// the type's entry if the file has none yet.
fn cost(file: &File, hash: u64, item: usize) -> usize {
    let entry = if file.items.contains_key(&hash) {
        0
    } else {
        ENTRY_BYTES
    };
    entry + item
}

/// Make sure the last file can take the next image of `hash`, opening a new
/// one when the cursor has passed its 8 MiB mark or the image would outgrow
/// its header.
fn open_room(packed: &mut Packed, hash: u64, item: usize) {
    let fits = packed.files.last().is_some_and(|file| {
        packed.end < (file.number + 1) * FILE_BLOCKS
            && file.len + cost(file, hash, item) <= HEADER_BYTES
    });
    if !fits {
        let number = packed.end.div_ceil(FILE_BLOCKS);
        packed.end = number * FILE_BLOCKS + FIRST_PAGE;
        packed.files.push(File {
            number,
            base: number * FILE_BLOCKS + SECTION,
            roots: true,
            items: BTreeMap::new(),
            len: BARE_BYTES,
            bytes: vec![0; FIRST_PAGE as usize * BLOCK_SIZE],
        });
    }
}

/// Write every file's roots block and header, in order, advancing `chain`
/// as it goes: each header links to the one before, names the types it
/// holds pages of, spends its spare room restating old addresses, and
/// records the horizon those restatements leave; the last ends the batch if
/// the upload is a cut, and becomes the root its roots block names. `chain`
/// ends at the last file.
///
/// # Errors
/// [`StorageError::Corrupt`] when a header outgrew [`HEADER_BYTES`]
/// (unreachable: the layout budgeted every entry and item).
pub fn finish(
    packed: &mut Packed,
    views: &[TypeView<'_>],
    chain: &mut Chain,
    position: u32,
) -> StorageResult<()> {
    let files = packed.files.len();
    for (i, file) in packed.files.iter_mut().enumerate() {
        let base = file.number * FILE_BLOCKS;
        let mut header = Header {
            at: file.base,
            prev: chain.tail.unwrap_or(NO_FILE),
            horizon: 0,
            shard: position,
            roots: chain.roots.unwrap_or(NO_FILE),
            end_of_batch: packed.cut && i + 1 == files,
            types: std::mem::take(&mut file.items)
                .into_iter()
                .map(|(hash, changed)| {
                    let view = views.iter().find(|v| v.struct_hash == hash);
                    // The layout placed each page at depth 0: its bucket's
                    // depth is the directory's to say.
                    let changed = changed
                        .into_iter()
                        .map(|(bucket, raw)| {
                            (bucket, deepened(view, bucket, raw))
                        })
                        .collect();
                    TypeEntry {
                        struct_hash: hash,
                        depth: view.and_then(|v| depth_of(v.slots)),
                        last: chain.last(hash),
                        dict: view.and_then(|v| v.dict),
                        changed,
                    }
                })
                .collect(),
            remote: Vec::new(),
            chain: None,
        };
        refresh(&mut header, views, chain);
        for entry in &header.types {
            chain.types.entry(entry.struct_hash).or_default().last = header.at;
        }
        header.horizon = horizon(views, &chain.types, chain.floor);
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
    Ok(())
}

/// Move the files laid out from block 0 to the sections a storage shard
/// reserved for them, at `bases` (RFC 0073 §8): every descriptor shifted
/// with its section, and no room for a roots block — the storage shard
/// heads each file it assembles.
pub fn rebase(packed: &mut Packed, bases: &[u64]) {
    let mut shifts = BTreeMap::new();
    for (file, &base) in packed.files.iter_mut().zip(bases) {
        let from = file.base;
        shifts.insert(file.number, (from, base));
        let moved = |raw: u64| shift(raw, from, base);
        for items in file.items.values_mut() {
            for item in items.iter_mut() {
                item.1 = moved(item.1);
            }
        }
        file.number = crate::remote::file_of(base);
        file.base = base;
        file.roots = false;
        file.bytes.drain(..BLOCK_SIZE);
    }
    for placed in &mut packed.placed {
        let (Placed::Page { desc, .. } | Placed::Dict { desc, .. }) = placed;
        let file = crate::remote::file_of(desc.start());
        if let (true, Some(&(from, base))) =
            (desc.is_allocated(), shifts.get(&file))
        {
            *desc = BlockDescriptor::from_raw(shift(desc.raw(), from, base));
        }
    }
}

/// `raw`'s descriptor moved with its section, from `from` to `to`.
fn shift(raw: u64, from: u64, to: u64) -> u64 {
    let desc = BlockDescriptor::from_raw(raw);
    if !desc.is_allocated() {
        return raw;
    }
    desc.moved_to(desc.start() - from + to).raw()
}

/// `raw` at the depth `view`'s directory gives `bucket`.
fn deepened(view: Option<&TypeView<'_>>, bucket: u32, raw: u64) -> u64 {
    let depth = view
        .and_then(|v| v.slots.get(bucket as usize))
        .map_or(0, |d| d.depth());
    BlockDescriptor::from_raw(raw).with_depth(depth).raw()
}

/// Spend `header`'s spare room restating old addresses (`crate::restate`).
fn refresh(header: &mut Header, views: &[TypeView<'_>], chain: &mut Chain) {
    let spare = HEADER_BYTES.saturating_sub(header.encoded_len());
    restate(&mut header.types, spare, views, &mut chain.types, header.at);
}

#[cfg(test)]
mod tests {
    use super::{
        FIRST_PAGE, Packed, Placed, SECTION, SlotImages, TypeView, finish,
        layout,
    };
    use crate::block::{BLOCK_SIZE, BlockDescriptor};
    use crate::header::{Chain, Header, NO_FILE, Rotation};
    use crate::remote::{FILE_BLOCKS, HEADER_BLOCKS, HEADER_BYTES};
    use crate::roots::Roots;

    /// Bytes ahead of a file's first page.
    const HEAD: usize = FIRST_PAGE as usize * BLOCK_SIZE;

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

    /// A remote directory of `n` pages (`n` a power of two), every one
    /// placed somewhere.
    fn dir(n: usize) -> Vec<BlockDescriptor> {
        let depth = n.trailing_zeros() as u8;
        (0..n)
            .map(|b| BlockDescriptor::new(b as u64 + 1, 1).with_depth(depth))
            .collect()
    }

    fn view(hash: u64, slots: &[BlockDescriptor]) -> TypeView<'_> {
        TypeView {
            struct_hash: hash,
            slots,
            dict: None,
        }
    }

    fn header(packed: &Packed, i: usize) -> Header {
        Header::decode(&packed.files[i].bytes[SECTION as usize * BLOCK_SIZE..])
            .unwrap()
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
    /// with its final descriptor; a type the upload did not touch is named
    /// only to restate it.
    #[test]
    fn pages_follow_the_header_and_the_header_names_them() {
        let mut packed = layout(0, &[images(9, &[(0, 2), (3, 1)])]);
        assert_eq!(packed.files.len(), 1);
        assert_eq!(packed.files[0].bytes.len(), HEAD + 3 * BLOCK_SIZE);
        let first = page_desc(&packed.placed, 0);
        assert_eq!(first, BlockDescriptor::new(FIRST_PAGE, 2));
        let second = page_desc(&packed.placed, 3);
        assert_eq!(second, BlockDescriptor::new(FIRST_PAGE + 2, 1));
        assert_eq!(packed.end, FIRST_PAGE + 3);

        let (nine, other) = (dir(4), dir(1));
        let views = [view(5, &other), view(9, &nine)];
        let mut chain = Chain::default();
        finish(&mut packed, &views, &mut chain, 0).unwrap();
        let h = header(&packed, 0);
        assert_eq!((h.at, h.prev, h.end_of_batch), (SECTION, NO_FILE, true));
        let roots = Roots::decode(&packed.files[0].bytes).unwrap();
        assert_eq!((roots.at, roots.end), (0, packed.end));
        assert_eq!(roots.root(0), Some(SECTION), "the batch it ends");
        assert_eq!(h.types[0].struct_hash, 9, "the changed type first");
        assert_eq!(h.types[0].depth, Some(2));
        assert!(
            h.types[0].changed.starts_with(&[
                (0, first.with_depth(2).raw()),
                (3, second.with_depth(2).raw())
            ]),
            "named at the depth the directory gives them"
        );
        assert_eq!(h.types[1].struct_hash, 5, "then the restated one");
        assert_eq!(chain.tail, Some(SECTION));
        assert_eq!(chain.last(9), SECTION);
    }

    /// The bound is on where a page starts: the one starting before 8 MiB
    /// runs past it in the same file, and the next file starts at the first
    /// boundary after it — the numbers it ran over are skipped.
    #[test]
    fn a_page_starting_before_the_mark_runs_past_it() {
        let big = (FILE_BLOCKS * 3) as usize; // 24 MiB
        let packed = layout(0, &[images(9, &[(0, 1), (1, big), (2, 1)])]);
        assert_eq!(packed.files.len(), 2);
        let first = page_desc(&packed.placed, 1);
        assert_eq!(first.start(), FIRST_PAGE + 1);
        assert_eq!(
            packed.files[0].bytes.len(),
            HEAD + (1 + big) * BLOCK_SIZE,
            "one object holds the whole large page"
        );
        assert_eq!(packed.files[1].number, 4, "files 1 to 3 are never used");
        let next = page_desc(&packed.placed, 2).start();
        assert_eq!(next, 4 * FILE_BLOCKS + FIRST_PAGE);
    }

    #[test]
    fn an_upload_starts_on_the_next_file_boundary() {
        let packed = layout(HEADER_BLOCKS + 5, &[images(9, &[(0, 1)])]);
        assert_eq!(packed.files[0].number, 1);
        let start = page_desc(&packed.placed, 0).start();
        assert_eq!(start, FILE_BLOCKS + FIRST_PAGE);
    }

    /// Nine hundred types with a page each outgrow one header long before
    /// 8 MiB of pages: the file closes on its header, and no header passes
    /// its room.
    #[test]
    fn a_full_header_closes_the_file() {
        let slots: Vec<SlotImages> =
            (0..900).map(|h| images(h, &[(0, 1)])).collect();
        let mut packed = layout(0, &slots);
        assert!(packed.files.len() > 1, "{} files", packed.files.len());
        let one = dir(1);
        let views: Vec<TypeView<'_>> =
            (0..900).map(|h| view(h, &one)).collect();
        finish(&mut packed, &views, &mut Chain::default(), 0).unwrap();
        for i in 0..packed.files.len() {
            assert!(header(&packed, i).encode().len() <= HEADER_BYTES);
        }
    }

    /// An upload's files link each to the one before, across uploads too,
    /// and only the last file of a cut ends the batch.
    #[test]
    fn headers_link_back_and_only_the_last_ends_the_batch() {
        let one = dir(1);
        let views = [view(9, &one)];
        let mut chain = Chain::default();
        let big = FILE_BLOCKS as usize;
        let mut first = layout(0, &[images(9, &[(0, big), (1, 1)])]);
        finish(&mut first, &views, &mut chain, 0).unwrap();
        assert_eq!(first.files.len(), 2);
        let (a, b) = (header(&first, 0), header(&first, 1));
        assert_eq!((a.end_of_batch, b.end_of_batch), (false, true));
        assert_eq!(b.prev, a.at);

        let mut second = layout(chain.end, &[images(9, &[(0, 1)])]);
        second.cut = false;
        finish(&mut second, &views, &mut chain, 0).unwrap();
        let c = header(&second, 0);
        assert_eq!(c.prev, b.at);
        assert!(!c.end_of_batch, "a part of a backlog ends nothing");
        assert_eq!(c.types[0].last, b.at, "the newest earlier entries");
    }

    /// Spare room restates addresses from a rotating cursor, the type
    /// restated longest ago first — so a type nobody writes still shows up
    /// near the head of the chain.
    #[test]
    fn spare_room_restates_on_from_the_cursor() {
        let (busy, idle) = (dir(4), dir(4_096));
        let views = [view(1, &busy), view(2, &idle)];
        let mut chain = Chain::default();
        for (hash, shifted) in [(1, 50), (2, 10)] {
            let rotation = Rotation {
                last: shifted,
                shifted: Some(shifted),
                ..Rotation::default()
            };
            chain.types.insert(hash, rotation);
        }
        chain.end = 60 * FILE_BLOCKS;
        let mut packed = layout(chain.end, &[images(1, &[(0, 1)])]);
        finish(&mut packed, &views, &mut chain, 0).unwrap();
        let h = header(&packed, 0);
        let idle_entry = &h.types[1];
        assert_eq!((idle_entry.struct_hash, idle_entry.last), (2, 10));
        let restated = idle_entry.changed.len();
        assert!(
            restated > 2_000,
            "the idle type filled the room: {restated}"
        );
        assert_eq!(idle_entry.changed[0], (0, idle[0].raw()));
        let rotation = chain.types[&2];
        assert_eq!(rotation.last, h.at);
        assert_eq!((rotation.shift, rotation.shifted), (restated, Some(h.at)));

        // The next file continues where the cursor stopped — the busy type,
        // restated in file 50, ahead of the one restated just now.
        let mut next = layout(chain.end, &[images(1, &[(0, 1)])]);
        finish(&mut next, &views, &mut chain, 0).unwrap();
        let h = header(&next, 0);
        let idle_entry = h.types.iter().find(|e| e.struct_hash == 2);
        let first = idle_entry.and_then(|e| e.changed.first().copied());
        assert_eq!(first, Some((restated as u32, idle[restated].raw())));
    }

    /// A type changed in every upload is named in every header, yet its
    /// quiet buckets are restated only in its turn — ordered by mention it
    /// would never get one. Ordered by restatement, every type comes round,
    /// and a type never written is named in the very first header.
    #[test]
    fn every_type_comes_round_a_busy_one_and_an_empty_one_too() {
        let (a, b, busy) = (dir(4_096), dir(4_096), dir(4_096));
        let empty: [BlockDescriptor; 0] = [];
        let views = [view(1, &a), view(2, &b), view(3, &busy), view(4, &empty)];
        let mut chain = Chain::default();
        let mut packed = layout(chain.end, &[images(3, &[(0, 1)])]);
        finish(&mut packed, &views, &mut chain, 0).unwrap();
        let first = header(&packed, 0);
        let named = first.types.iter().find(|e| e.struct_hash == 4);
        assert_eq!(named.map(|e| e.depth), Some(None), "the empty type");
        for _ in 0..12 {
            let mut packed = layout(chain.end, &[images(3, &[(0, 1)])]);
            finish(&mut packed, &views, &mut chain, 0).unwrap();
        }
        for hash in [1, 2, 3, 4] {
            let shifted = chain.types.get(&hash).and_then(|r| r.shifted);
            assert!(shifted.is_some(), "type {hash} never came round");
        }
        assert!(chain.types[&3].shift > 0, "the busy type was restated");
    }
}
