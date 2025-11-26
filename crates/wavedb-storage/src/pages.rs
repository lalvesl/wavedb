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
use crate::page::SlotPage;

/// A type's page index, its flags beside each page.
pub type Pages = PageIndex<u8>;

/// A page — or an emptied page's hole — the object store has not received.
pub const PENDING: u8 = 1;
/// A page gone from disk: the object store holds it.
pub const EVICTED: u8 = 2;

/// An empty index for a disk lane.
#[must_use]
pub const fn disk_pages() -> Pages {
    PageIndex::new(Shape::DISK)
}

/// An empty index for the object store's pages.
#[must_use]
pub const fn remote_pages() -> Pages {
    PageIndex::new(Shape::REMOTE)
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

/// Where `id`'s page is.
pub enum Located {
    /// The page covering `id` — on disk, or [`EVICTED`] from it.
    Page { at: Slot, run: Run, flags: u8 },
    /// No page holds `id`.
    Absent,
}

/// Where `id`'s page is in `pages`.
#[must_use]
pub fn locate(pages: &Pages, id: Id) -> Located {
    match pages.find(&key_of_id(id)) {
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

/// Lend the page at `run` to `read`, through the page cache (RFC 0044): a
/// run is immutable for as long as it is allocated — page writes are
/// copy-on-write — so a hit needs no validation.
///
/// # Errors
/// A read fault, or a page that fails its checks.
pub fn with_page<R>(
    file: &BlockFile,
    run: Run,
    dict: &DictState,
    read: impl FnOnce(&SlotPage) -> R,
) -> StorageResult<R> {
    file.with_page(
        run,
        |image| SlotPage::from_bytes(image, dict.dictionary()),
        read,
    )
}

/// The page at `run`, owned: taken out of the page cache, or read — for a
/// round about to rewrite it into a new run.
///
/// # Errors
/// As [`with_page`].
pub fn take_page(
    file: &BlockFile,
    run: Run,
    dict: &DictState,
) -> StorageResult<SlotPage> {
    file.take_page(run, |image| SlotPage::from_bytes(image, dict.dictionary()))
}

/// The page at `run`, owned — a copy of the held one.
///
/// # Errors
/// As [`with_page`].
pub fn read_page(
    file: &BlockFile,
    run: Run,
    dict: &DictState,
) -> StorageResult<SlotPage> {
    with_page(file, run, dict, SlotPage::clone)
}
