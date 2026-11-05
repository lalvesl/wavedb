//! A type's pages as a store addresses them ([RFC 0078]): an ordered page
//! index over the order-preserving bytes of each record's `Id`, and the
//! reads through it.
//!
//! An `Id`'s key is its raw `u128`, big-endian — `KEY`, then `TENANT`, then
//! `FLAG | SALT` — so a NonUnique type's anchors run in creation order and
//! append at the tail, a keyed one's spread evenly, and a Unique type's sort
//! by tenant. Beside each page the index keeps a flag byte: the object
//! store's "not uploaded yet" ([`PENDING`]) and "gone from disk"
//! ([`EVICTED`]) (RFC 0070).
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;

use wavedb_core::Id;

use crate::block::Run;
use crate::block_file::BlockFile;
use crate::dictionary::DictState;
use crate::error::StorageResult;
use crate::ordered::key::cmp_padded;
use crate::ordered::{Found, PageIndex, Placed, Shape, Slot};
use crate::pack::page_image;
use crate::page::SlotPage;

/// A type's page index, its flags beside each page.
pub type Pages = PageIndex<u8>;

/// A page — or an emptied page's hole — the object store has not received.
pub const PENDING: u8 = 1;
/// A page gone from disk: the object store holds it.
pub const EVICTED: u8 = 2;

/// An empty index for a disk lane, its pages counted or not.
#[must_use]
pub const fn disk_pages(counted: bool) -> Pages {
    counting(PageIndex::new(Shape::DISK), counted)
}

/// An empty index for the object store's pages, counted or not.
#[must_use]
pub const fn remote_pages(counted: bool) -> Pages {
    counting(PageIndex::new(Shape::REMOTE), counted)
}

const fn counting(pages: Pages, counted: bool) -> Pages {
    if counted { pages.counting() } else { pages }
}

/// The key `id` is stored under.
#[must_use]
pub const fn key_of_id(id: Id) -> [u8; 16] {
    id.raw().to_be_bytes()
}

/// The run a placed page occupies.
#[must_use]
pub const fn run_of(page: Placed<u8>) -> Run {
    Run::new(page.address, page.blocks)
}

/// The runs `pages` names on disk, once per entry naming each: an evicted
/// page's run went back to the files.
pub fn disk_runs(pages: &Pages) -> impl Iterator<Item = Run> + '_ {
    pages
        .pages()
        .filter(|(_, p)| p.meta & EVICTED == 0)
        .map(|(_, p)| run_of(p))
}

/// Where a key's page is.
pub enum Located {
    /// The page covering the key — on disk, or [`EVICTED`] from it.
    Page { at: Slot, run: Run, flags: u8 },
    /// No page holds the key.
    Absent,
}

/// Where `key`'s page is in `pages`.
#[must_use]
pub fn locate(pages: &Pages, key: &[u8]) -> Located {
    match pages.find(key) {
        Found::Page { at, page } => Located::Page {
            at,
            run: run_of(page),
            flags: page.meta,
        },
        Found::Hole(_) | Found::Uncovered => Located::Absent,
    }
}

/// The keys `at` covers, `[lo, hi)`; `hi` is `None` past the last entry.
#[must_use]
pub fn coverage(pages: &Pages, at: Slot) -> Option<(Vec<u8>, Option<Vec<u8>>)> {
    let lo = pages.key_of(at)?;
    let hi = pages.next(at).and_then(|next| pages.key_of(next));
    Some((lo, hi))
}

/// `page` as the entry at `at` reads it: the records its coverage holds.
///
/// A page may hold keys its entry no longer covers — another entry names
/// them now, from a newer run (RFC 0081 §2–§3) — so whatever takes a page's
/// records to write them again takes them through its entry, or it would
/// bring back what the newer run replaced. A point lookup needs no clip: the
/// index routes the key to the entry covering it.
///
/// # Errors
/// [`StorageError::Corrupt`](crate::StorageError::Corrupt) for an entry
/// with no key.
pub fn clip(
    page: SlotPage,
    pages: &Pages,
    at: Slot,
) -> StorageResult<SlotPage> {
    let (lo, hi) = coverage(pages, at)
        .ok_or(crate::StorageError::Corrupt("an entry without a key"))?;
    let hi = hi.as_deref();
    let inside = |key: Option<&[u8]>| key.is_none_or(|k| within(k, &lo, hi));
    if inside(page.iter().next().map(|r| r.0))
        && inside(page.iter().last().map(|r| r.0))
    {
        return Ok(page);
    }
    let mut own = SlotPage::new(page.struct_hash());
    for (key, bytes) in page.iter().filter(|(k, _)| within(k, &lo, hi)) {
        own.upsert(key, bytes.to_vec());
    }
    Ok(own)
}

/// Whether `key` lies in `[lo, hi)`.
#[must_use]
pub fn within(key: &[u8], lo: &[u8], hi: Option<&[u8]>) -> bool {
    cmp_padded(key, lo) != Ordering::Less
        && hi.is_none_or(|hi| cmp_padded(key, hi) == Ordering::Less)
}

/// The entries whose coverage meets `[lo, hi)`, in key order.
#[must_use]
pub fn entries_between(
    pages: &Pages,
    lo: &[u8],
    hi: Option<&[u8]>,
) -> Vec<(Slot, Option<Placed<u8>>)> {
    let mut out = Vec::new();
    let mut at = pages.slot_from(lo);
    while let Some(slot) = at {
        let Some(start) = pages.key_of(slot) else {
            break;
        };
        if hi.is_some_and(|hi| cmp_padded(&start, hi) != Ordering::Less) {
            break;
        }
        out.push((slot, pages.page_at(slot)));
        at = pages.next(slot);
    }
    out
}

/// Lend slot `slot`'s page at `run` to `read`, through the page cache (RFC 0044): a
/// run is immutable for as long as it is allocated — page writes are
/// copy-on-write — so a hit needs no validation.
///
/// # Errors
/// A read fault, or a page that fails its checks.
pub fn with_page<R>(
    file: &BlockFile,
    run: Run,
    slot: u64,
    dict: &DictState,
    read: impl FnOnce(&SlotPage) -> R,
) -> StorageResult<R> {
    file.with_page(run, slot, |image| decode(image, slot, dict), read)
}

/// The page of slot `slot` at `run`, owned: taken out of the page cache, or
/// read — for a round about to rewrite it into a new run.
///
/// # Errors
/// As [`with_page`].
pub fn take_page(
    file: &BlockFile,
    run: Run,
    slot: u64,
    dict: &DictState,
) -> StorageResult<SlotPage> {
    file.take_page(run, slot, |image| decode(image, slot, dict))
}

/// The page of slot `slot` at `run`, owned — a copy of the held one.
///
/// # Errors
/// As [`with_page`].
pub fn read_page(
    file: &BlockFile,
    run: Run,
    slot: u64,
    dict: &DictState,
) -> StorageResult<SlotPage> {
    with_page(file, run, slot, dict, SlotPage::clone)
}

/// Slot `slot`'s page in the run's bytes `image`: the page, or its section
/// of a pack (`crate::pack`).
fn decode(
    image: &[u8],
    slot: u64,
    dict: &DictState,
) -> StorageResult<SlotPage> {
    SlotPage::from_bytes(&page_image(image, slot)?, dict.dictionary())
}

#[cfg(test)]
mod tests {
    use super::{Pages, clip, disk_pages};
    use crate::StorageResult;
    use crate::ordered::{Cut, CutOutcome, Found, Placed};
    use crate::page::SlotPage;
    use crate::shape::{Rules, shape};

    const SH: u64 = 0x5EED_0081_0003_0001;

    fn page(address: u64) -> Placed<u8> {
        Placed {
            address,
            blocks: 1,
            meta: 0,
        }
    }

    fn key(n: u8) -> Vec<u8> {
        vec![n, 0]
    }

    fn of(keys: &[u8]) -> SlotPage {
        let mut p = SlotPage::new(SH);
        for &k in keys {
            p.upsert(&key(k), vec![k]);
        }
        p
    }

    /// Two entries: keys from 1 at page 10, keys from 5 at page 20.
    fn two() -> Pages {
        let mut ix = disk_pages(false);
        let at = ix.place(&key(1), page(10)).unwrap();
        let cut = Cut {
            first: &key(1),
            left_last: &key(4),
            right_first: &key(5),
            last: &key(9),
        };
        assert!(matches!(ix.cut(at, &cut, page(20)), CutOutcome::Cut(_)));
        ix
    }

    fn keys(p: &SlotPage) -> Vec<u8> {
        p.iter().map(|(k, _)| k[0]).collect()
    }

    #[test]
    fn a_page_is_read_through_its_entry() {
        let ix = two();
        let Found::Page { at, .. } = ix.find(&key(1)) else {
            panic!("a page");
        };
        // Page 10 still holds 5 and 6, which the second entry covers now.
        assert_eq!(keys(&clip(of(&[1, 3, 5, 6]), &ix, at).unwrap()), [1, 3]);
        assert_eq!(keys(&clip(of(&[1, 3]), &ix, at).unwrap()), [1, 3]);
    }

    #[test]
    fn a_round_rewrites_a_page_without_what_its_entry_gave_up() {
        let mut ix = two();
        let rules = Rules {
            target_blocks: 16,
            keep_holes: false,
            partial: false,
        };
        let load = |p: Placed<u8>| -> StorageResult<SlotPage> {
            Ok(if p.address == 10 {
                of(&[1, 5])
            } else {
                of(&[5, 6])
            })
        };
        let records = vec![(key(2), Some(vec![2]))];
        let dict = crate::dictionary::Dictionary::new();
        let shaped = shape(&mut ix, records, &rules, SH, load, |p| {
            p.to_bytes(&dict, false)
        })
        .unwrap();
        let written: Vec<Vec<u8>> = shaped
            .pages
            .iter()
            .map(|p| keys(p.decoded.as_ref().unwrap()))
            .collect();
        assert_eq!(written, [vec![1, 2]], "5 is the second page's");
    }
}
