//! One round's pages, shaped ([RFC 0078] §5): a round's records routed to
//! the pages their keys fall in, those pages rebuilt, and each over target
//! cut on a quantum boundary — the disk's settle and the object store's
//! upload alike, each loading pages and encoding them its own way.
//!
//! 1. **Route**, in key order. A record whose key has a page goes into it,
//!    the page loaded on first touch. One with no page — a hole, or keys no
//!    range expresses — gets a new page, its entry pointing at a stand-in
//!    the range can always address (`placeholder_at`). Nothing moves an
//!    entry while routing, so every page is staged under its entry's key.
//! 2. **Shape**, from the last page back — a cut refining a range moves only
//!    the entries after the page it cuts, all shaped already. A page left
//!    empty goes, or becomes a hole that carries [`PENDING`] where another
//!    store must still learn its keys are gone; one over target is cut in
//!    two — at the edge between two collections nearest its middle when it
//!    holds more than one, so a collection that spans pages owns them;
//!    else at its middle, or at the quantum boundary nearest it.
//!
//! Every page comes out with a key that finds its entry: its first
//! record's. Once the window holds it, the entry is pointed at its run
//! (`crate::window::install`).
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::BTreeMap;

use crate::block::{BLOCK_SIZE, Run};
use crate::cut::{cut_at, halves};
use crate::error::{StorageError, StorageResult};
use crate::ordered::{Found, Placed, Slot};
use crate::page::SlotPage;
use crate::pages::{EVICTED, PENDING, Pages, clip, run_of};
use crate::partial::partial;
use crate::record_cache::Records;

/// A page a round writes.
pub struct PlannedPage {
    /// Finds the page's entry once the round is installed.
    pub(crate) key: Vec<u8>,
    pub(crate) image: Vec<u8>,
    /// The page the image encodes, held under its new run once written.
    pub(crate) decoded: Option<SlotPage>,
    /// The flags it is placed with when the placement keeps them (a move).
    pub(crate) flags: u8,
    /// More keys whose entries name it: a page a span left named either
    /// side (`crate::partial`), moved once (`crate::regroup`).
    pub(crate) also: Vec<Vec<u8>>,
}

/// What a round's pages came to.
#[derive(Default)]
pub struct Shaped {
    pub(crate) pages: Vec<PlannedPage>,
    /// Runs the index stops naming: every page the round rewrote or dropped.
    pub(crate) retired: Vec<Run>,
    /// Runs the index names once more: a page the round rewrote in part,
    /// named before and after the part (`crate::partial`).
    pub(crate) named: Vec<Run>,
    /// Pending blocks those took with them.
    pub(crate) unpended: u64,
    /// Holes left pending: a removal another store must learn.
    pub(crate) punched: u64,
}

/// How a round shapes its pages.
pub struct Rules {
    /// Blocks a page should hold: one over it is cut.
    pub(crate) target_blocks: u64,
    /// An emptied page becomes a [`PENDING`] hole rather than going.
    pub(crate) keep_holes: bool,
    /// A page the round changes in part is rewritten in that part
    /// (`crate::partial`): a settle's rounds, not a merge's or an upload's.
    pub(crate) partial: bool,
}

/// One staged page: where it came from, and what it now holds.
pub struct Staged {
    pub(crate) old: Option<Placed<u8>>,
    pub(crate) page: SlotPage,
    /// The keys the round put or removed in it, in order.
    pub(crate) changed: Vec<Vec<u8>>,
    /// The bytes of the records it held as read.
    pub(crate) held: u64,
}

/// Blocks an encoded image takes before rounding.
#[must_use]
pub const fn blocks_for(len: usize) -> u64 {
    let blocks = len.div_ceil(BLOCK_SIZE) as u64;
    if blocks == 0 { 1 } else { blocks }
}

/// What a page weighs on the pending upload: its blocks, or one for a hole.
#[must_use]
pub const fn weight(flags: u8, blocks: u64) -> u64 {
    if flags & PENDING == 0 {
        0
    } else if blocks == 0 {
        1
    } else {
        blocks
    }
}

/// Shape `records` (sorted by key) into `index`'s pages, loading a page by
/// `load` and encoding one by `encode`.
///
/// # Errors
/// [`StorageError::Corrupt`] for a page it must load that was evicted; a
/// page past what a size unit can name is [`StorageError::PageTooLarge`];
/// and the loader's and encoder's faults.
pub fn shape<L, E>(
    index: &mut Pages,
    records: Records,
    rules: &Rules,
    struct_hash: u64,
    mut load: L,
    mut encode: E,
) -> StorageResult<Shaped>
where
    L: FnMut(Placed<u8>) -> StorageResult<SlotPage>,
    E: FnMut(&mut SlotPage) -> StorageResult<Vec<u8>>,
{
    let mut staged: BTreeMap<Vec<u8>, Staged> = BTreeMap::new();
    for (key, bytes) in records {
        let at = match index.find(&key) {
            Found::Page { at, page } => {
                let sk = staged_key(index, at)?;
                if let std::collections::btree_map::Entry::Vacant(vacant) =
                    staged.entry(sk)
                {
                    if page.meta & EVICTED != 0 {
                        return Err(StorageError::Corrupt(
                            "a round reached an evicted page",
                        ));
                    }
                    let loaded = clip(load(page)?, index, at)?;
                    vacant.insert(Staged {
                        old: Some(page),
                        held: crate::partial::bytes(&loaded, 0, usize::MAX),
                        page: loaded,
                        changed: Vec::new(),
                    });
                }
                at
            }
            // Removing what is not there changes nothing.
            Found::Hole(_) | Found::Uncovered if bytes.is_none() => continue,
            Found::Hole(at) => {
                let stand_in = index
                    .placeholder_at(at, 0)
                    .ok_or(StorageError::Corrupt("a hole without a range"))?;
                index
                    .retarget(at, stand_in)
                    .ok_or(StorageError::Corrupt("a stand-in refused"))?
            }
            Found::Uncovered => {
                let stand_in = Placed {
                    address: 0,
                    blocks: 1,
                    meta: 0,
                };
                index
                    .place(&key, stand_in)
                    .ok_or(StorageError::Corrupt("a range refused to open"))?
            }
        };
        let sk = staged_key(index, at)?;
        let entry = staged.entry(sk).or_insert_with(|| Staged {
            old: None,
            page: SlotPage::new(struct_hash),
            changed: Vec::new(),
            held: 0,
        });
        let changed = match bytes {
            Some(bytes) => {
                entry.page.upsert(&key, bytes);
                true
            }
            None => entry.page.remove(&key),
        };
        if changed {
            entry.changed.push(key);
        }
    }
    let mut shaped = Shaped::default();
    for (sk, st) in staged.into_iter().rev() {
        let Some(st) = partial(index, st, rules, &mut encode, &mut shaped)?
        else {
            continue; // the page stays, rewritten in part or not at all
        };
        if let Some(old) = st.old {
            shaped.retired.push(run_of(old));
            shaped.unpended += weight(old.meta, old.blocks);
        }
        let Found::Page { at, .. } = index.find(&sk) else {
            return Err(StorageError::Corrupt("a staged page lost its entry"));
        };
        if st.page.is_empty() {
            // Its entry stays, a hole, while its range holds others: its
            // quanta would go to a neighbour whose page may hold keys there
            // past its entry — a page either side of a span, one a fragment
            // took a quantum from (`crate::partial`) — and they would come
            // back.
            let pending = rules.keep_holes && st.old.is_some();
            if pending || !index.is_alone(at) {
                index.punch(at, if pending { PENDING } else { 0 });
                shaped.punched += u64::from(pending);
            } else {
                index.remove(at);
            }
            continue;
        }
        cut_down(index, st.page, rules, &mut encode, &mut shaped)?;
    }
    Ok(shaped)
}

/// The key a staged page is kept under: its entry's own, trimmed.
fn staged_key(index: &Pages, at: Slot) -> StorageResult<Vec<u8>> {
    let mut key = index
        .key_of(at)
        .ok_or(StorageError::Corrupt("an entry without a key"))?;
    let len = key.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    key.truncate(len);
    Ok(key)
}

/// Emit `page`, cut into pages under target first — each half again until
/// it fits, or holds one record, or its keys cannot part.
pub fn cut_down<E>(
    index: &mut Pages,
    page: SlotPage,
    rules: &Rules,
    encode: &mut E,
    shaped: &mut Shaped,
) -> StorageResult<()>
where
    E: FnMut(&mut SlotPage) -> StorageResult<Vec<u8>>,
{
    let mut stack = vec![page];
    while let Some(mut page) = stack.pop() {
        let keys: Vec<Vec<u8>> = page.iter().map(|(k, _)| k.to_vec()).collect();
        let first = keys[0].clone();
        let image = encode(&mut page)?;
        let blocks = blocks_for(image.len());
        let fits = index.canonical(blocks) <= rules.target_blocks;
        let parts = if fits || keys.len() < 2 {
            None
        } else {
            cut_at(index, &first, &keys)?
        };
        let Some(at) = parts else {
            // Only a page that cannot be cut is refused for its size.
            if index.seed().unit_for(blocks) > u64::from(u8::MAX) {
                return Err(StorageError::PageTooLarge(image.len() as u64));
            }
            shaped.pages.push(PlannedPage {
                key: first,
                image,
                decoded: Some(page),
                flags: 0,
                also: Vec::new(),
            });
            continue;
        };
        let (left, right) = halves(&page, &keys[at]);
        stack.push(left);
        stack.push(right);
    }
    Ok(())
}
