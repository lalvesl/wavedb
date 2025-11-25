//! A lane's page index: its sorted sub-headers, and under each the entries
//! of its pages ([RFC 0078] §3).
//!
//! Every quantum of a range belongs to exactly one entry — the last one
//! starting at or before it — so a page boundary is always a quantum
//! boundary and a lookup names one page, never two. Coverage may reach over
//! quanta holding no key: the first entry of a range always starts at
//! quantum 0, and a removed page's quanta go to the page before it.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;

use super::head::{QUANTA, Shape, SubHeader, q_of, with_q};
use super::journal::Journal;
use super::key::{prefix, shared_prefix, slice};

/// The entries under one sub-header, ascending by quantum.
///
/// Two parallel columns: the `u32` that finds the page, and what a lane keeps
/// beside it (`()` where nothing is counted, a `u8` count in a list's lane).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Entries<M> {
    pub(super) pages: Vec<u32>,
    pub(super) meta: Vec<M>,
}

impl<M: Copy> Entries<M> {
    pub(super) fn one(entry: u32, meta: M) -> Self {
        Self {
            pages: vec![entry],
            meta: vec![meta],
        }
    }

    pub(super) const fn len(&self) -> usize {
        self.pages.len()
    }

    /// Where quantum `q`'s entry ends: the next entry's start, or the end of
    /// the range.
    pub(super) fn end_of(&self, i: usize) -> u32 {
        self.pages
            .get(i + 1)
            .map_or(QUANTA, |&e| u32::from(q_of(e)))
    }

    pub(super) fn insert(&mut self, i: usize, entry: u32, meta: M) {
        self.pages.insert(i, entry);
        self.meta.insert(i, meta);
    }

    pub(super) fn remove(&mut self, i: usize) {
        self.pages.remove(i);
        self.meta.remove(i);
    }

    /// The entries from `i` on, moved `by` quanta down.
    pub(super) fn tail(&self, i: usize, by: u32) -> Self {
        let pages = self.pages[i..]
            .iter()
            .map(|&e| with_q(e, (u32::from(q_of(e)) - by) as u16))
            .collect();
        Self {
            pages,
            meta: self.meta[i..].to_vec(),
        }
    }

    /// The entries before `i`.
    pub(super) fn head(&self, i: usize) -> Self {
        Self {
            pages: self.pages[..i].to_vec(),
            meta: self.meta[..i].to_vec(),
        }
    }
}

/// One range: its sub-header and its entries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Range<M> {
    pub(super) head: SubHeader,
    pub(super) entries: Entries<M>,
}

/// An entry's position: the range, then the entry within it. Positions move
/// when ranges or entries are inserted before them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Slot {
    pub range: usize,
    pub entry: usize,
}

/// A page as the caller hands it to the index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placed<M> {
    pub address: u64,
    pub blocks: u64,
    pub meta: M,
}

/// Where a key's page is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Found<M> {
    /// The page whose coverage holds the key. Its blocks may be more than
    /// the page uses; the page's envelope carries its own length.
    Page { at: Slot, page: Placed<M> },
    /// Quanta no page covers: the key is absent.
    Hole(Slot),
    /// No range expresses the key: the key is absent.
    Uncovered,
}

/// A lane's page index.
#[derive(Debug, Clone)]
pub struct PageIndex<M> {
    /// The shape a range starts with when nothing before it suggests one.
    pub(super) seed: Shape,
    pub(super) ranges: Vec<Range<M>>,
    /// What changed since a header last named it.
    pub(super) journal: Journal,
}

impl<M: Copy + Default> PageIndex<M> {
    /// An empty index whose first range will take `seed`.
    #[must_use]
    pub const fn new(seed: Shape) -> Self {
        Self {
            seed,
            ranges: Vec::new(),
            journal: Journal::new(),
        }
    }

    /// Pages and holes under every range.
    #[must_use]
    pub fn entries(&self) -> usize {
        self.ranges.iter().map(|r| r.entries.len()).sum()
    }

    /// The sub-headers, in key order.
    pub fn sub_headers(&self) -> impl Iterator<Item = &SubHeader> {
        self.ranges.iter().map(|r| &r.head)
    }

    /// The last range starting at or before `key`.
    fn range_of(&self, key: &[u8]) -> Option<usize> {
        let after = self
            .ranges
            .partition_point(|r| r.head.start_cmp(key) != Ordering::Greater);
        after.checked_sub(1)
    }

    /// Where `key`'s page is.
    #[must_use]
    pub fn find(&self, key: &[u8]) -> Found<M> {
        let Some(range) = self.range_of(key) else {
            return Found::Uncovered;
        };
        let r = &self.ranges[range];
        let Some(q) = r.head.quantum(key) else {
            return Found::Uncovered;
        };
        let entry = r.entries.pages.partition_point(|&e| q_of(e) <= q) - 1;
        self.at(Slot { range, entry }).unwrap_or(Found::Uncovered)
    }

    /// What `at` names.
    #[must_use]
    pub fn at(&self, at: Slot) -> Option<Found<M>> {
        let r = self.ranges.get(at.range)?;
        let entry = *r.entries.pages.get(at.entry)?;
        Some(match r.head.decode(entry) {
            Some((address, blocks)) => Found::Page {
                at,
                page: Placed {
                    address,
                    blocks,
                    meta: r.entries.meta[at.entry],
                },
            },
            None => Found::Hole(at),
        })
    }

    /// The entry after `at`, in key order.
    #[must_use]
    pub fn next(&self, at: Slot) -> Option<Slot> {
        let r = self.ranges.get(at.range)?;
        if at.entry + 1 < r.entries.len() {
            return Some(Slot {
                range: at.range,
                entry: at.entry + 1,
            });
        }
        (at.range + 1 < self.ranges.len()).then_some(Slot {
            range: at.range + 1,
            entry: 0,
        })
    }

    /// The quantum `key` falls in within `at`'s range.
    #[must_use]
    pub fn quantum_of(&self, at: Slot, key: &[u8]) -> Option<u16> {
        self.ranges.get(at.range)?.head.quantum(key)
    }

    /// Put a first page where `key` has none: a hole is filled, and where no
    /// range expresses the key a new one opens at it. `None` when `key`
    /// already has a page, or `page` is larger than any size unit can say.
    pub fn place(&mut self, key: &[u8], page: Placed<M>) -> Option<Slot> {
        match self.find(key) {
            Found::Page { .. } => None,
            Found::Hole(at) => self.retarget(at, page),
            Found::Uncovered => self.open(key, page),
        }
    }

    /// Open a range at `key` holding `page` alone, quantized as the seed
    /// says.
    ///
    /// It continues at the depth of the range before it when the key shares
    /// that range's prefix — a stream running past its range's end — and
    /// otherwise starts where the key leaves that prefix. Either way its
    /// start lies after the range before and no later than `key`, so the
    /// ranges stay sorted. The shape is the seed's, not the range before's:
    /// that one may have been refined for a dense spot the stream has left.
    fn open(&mut self, key: &[u8], page: Placed<M>) -> Option<Slot> {
        let before = self.range_of(key);
        let depth = before.map_or(0, |i| {
            let prev = &self.ranges[i].head;
            shared_prefix(key, &prev.prefix).min(prev.depth())
        });
        let head = SubHeader {
            base_block: page.address,
            first_key: slice(key, depth),
            prefix: prefix(key, depth),
            shape: self.seed.sized_for(page.blocks)?,
        };
        let entry = head.encode(0, page.address, page.blocks)?;
        let range = before.map_or(0, |i| i + 1);
        self.ranges.insert(
            range,
            Range {
                head,
                entries: Entries::one(entry, page.meta),
            },
        );
        self.note_whole(range);
        Some(Slot { range, entry: 0 })
    }

    /// Point `at` — a page or a hole — at `page`: rewritten elsewhere, or
    /// filled. A page its range cannot address gets a range of its own
    /// (`isolate`). Returns where the entry is now; `None` when `at` names
    /// nothing or `page` is larger than any size unit can say.
    pub fn retarget(&mut self, at: Slot, page: Placed<M>) -> Option<Slot> {
        let r = self.ranges.get_mut(at.range)?;
        let entry = *r.entries.pages.get(at.entry)?;
        match r.head.encode(q_of(entry), page.address, page.blocks) {
            Some(encoded) => {
                r.entries.pages[at.entry] = encoded;
                r.entries.meta[at.entry] = page.meta;
                self.note_entry(at);
                Some(at)
            }
            None => self.isolate(at, page),
        }
    }

    /// Change what is kept beside `at`'s page.
    pub fn set_meta(&mut self, at: Slot, meta: M) -> Option<()> {
        let slot = self
            .ranges
            .get_mut(at.range)?
            .entries
            .meta
            .get_mut(at.entry)?;
        *slot = meta;
        self.note_entry(at);
        Some(())
    }

    /// Drop `at`'s entry: its quanta go to the entry before it, or, for the
    /// first of a range, to the one after. A range left empty goes.
    pub fn remove(&mut self, at: Slot) -> Option<()> {
        let r = self.ranges.get_mut(at.range)?;
        if at.entry >= r.entries.len() {
            return None;
        }
        // Named before it may go: a range gone is named by where it started.
        self.note_whole(at.range);
        let r = &mut self.ranges[at.range];
        r.entries.remove(at.entry);
        match r.entries.pages.first_mut() {
            Some(first) => *first = with_q(*first, 0),
            None => {
                self.ranges.remove(at.range);
            }
        }
        Some(())
    }
}
