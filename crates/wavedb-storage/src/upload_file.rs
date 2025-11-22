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
