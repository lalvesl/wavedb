//! What the store asks of an index besides lookups: rebuilding it, walking
//! its pages, naming a page by a key, sizing a page, and keeping a range
//! within what one header can restate.

use super::head::{Shape, SubHeader, q_of};
use super::index::{Found, PageIndex, Placed, Range, Slot};
use super::journal::Journal;

/// Entries a range holds at most: one range, restated whole, takes ~5 KiB
/// of a 32 KiB header.
pub const MAX_RANGE_ENTRIES: usize = 1024;

impl<M: Copy + Default> PageIndex<M> {
    /// An index over `ranges`, sorted and disjoint, nothing unrecorded.
    #[must_use]
    pub(super) const fn from_ranges(
        seed: Shape,
        ranges: Vec<Range<M>>,
    ) -> Self {
        Self {
            seed,
            ranges,
            journal: Journal::new(),
        }
    }

    /// The shape new ranges take.
    #[must_use]
    pub const fn seed(&self) -> Shape {
        self.seed
    }

    /// Ranges held.
    #[must_use]
    pub const fn range_count(&self) -> usize {
        self.ranges.len()
    }

    /// The blocks a page of `blocks` is given, so its entry names its run
    /// exactly ([`Shape::canonical`]).
    #[must_use]
    pub const fn canonical(&self, blocks: u64) -> u64 {
        self.seed.canonical(blocks)
    }

    /// Every page, in key order, with its slot.
    pub fn pages(&self) -> impl Iterator<Item = (Slot, Placed<M>)> + '_ {
        self.ranges.iter().enumerate().flat_map(|(range, r)| {
            r.entries
                .pages
                .iter()
                .zip(&r.entries.meta)
                .enumerate()
                .filter_map(move |(entry, (&e, &meta))| {
                    let (address, blocks) = r.head.decode(e)?;
                    let at = Slot { range, entry };
                    Some((
                        at,
                        Placed {
                            address,
                            blocks,
                            meta,
                        },
                    ))
                })
        })
    }

    /// A key that finds `at`: its quantum's first, under its range's
    /// prefix.
    #[must_use]
    pub fn key_of(&self, at: Slot) -> Option<Vec<u8>> {
        let r = self.ranges.get(at.range)?;
        let q = q_of(*r.entries.pages.get(at.entry)?);
        let mut key = r.head.prefix.to_vec();
        key.extend_from_slice(
            &r.head.quantum_start(u32::from(q))?.to_be_bytes(),
        );
        Some(key)
    }

    /// Every entry — page or hole — in key order: its slot, its page if it
    /// is one, and what is kept beside it.
    pub fn entries_all(
        &self,
    ) -> impl Iterator<Item = (Slot, Option<Placed<M>>)> + '_ {
        self.ranges.iter().enumerate().flat_map(|(range, r)| {
            r.entries.pages.iter().zip(&r.entries.meta).enumerate().map(
                move |(entry, (&e, &meta))| {
                    let page =
                        r.head.decode(e).map(|(address, blocks)| Placed {
                            address,
                            blocks,
                            meta,
                        });
                    (Slot { range, entry }, page)
                },
            )
        })
    }

    /// What is kept beside `at`'s entry, page or hole.
    #[must_use]
    pub fn meta_at(&self, at: Slot) -> Option<M> {
        self.ranges
            .get(at.range)?
            .entries
            .meta
            .get(at.entry)
            .copied()
    }

    /// A page `at`'s range can always address, standing in for one whose
    /// address is not known yet: the range's base, one size unit. A round
    /// fills holes and cuts pages with it, and points the entries at the
    /// real runs once its window is placed.
    #[must_use]
    pub fn placeholder_at(&self, at: Slot, meta: M) -> Option<Placed<M>> {
        let head = &self.ranges.get(at.range)?.head;
        Some(Placed {
            address: head.base_block,
            blocks: u64::from(head.shape.size_factor()),
            meta,
        })
    }

    /// Make `at`'s entry a hole carrying `meta`: its quanta stay its own,
    /// with no page — a page emptied whose keys another store must still
    /// learn are gone.
    pub fn punch(&mut self, at: Slot, meta: M) -> Option<()> {
        let r = self.ranges.get_mut(at.range)?;
        let entry = *r.entries.pages.get(at.entry)?;
        r.entries.pages[at.entry] = r.head.hole(q_of(entry));
        r.entries.meta[at.entry] = meta;
        self.note_entry(at);
        Some(())
    }

    /// The first entry whose coverage reaches `key` or lies past it: the
    /// one covering `key`, or — where no range expresses it — the first
    /// entry after it.
    #[must_use]
    pub fn slot_from(&self, key: &[u8]) -> Option<Slot> {
        match self.find(key) {
            Found::Page { at, .. } | Found::Hole(at) => Some(at),
            Found::Uncovered => {
                let after = self.ranges.partition_point(|r| {
                    r.head.start_cmp(key) != std::cmp::Ordering::Greater
                });
                (after < self.ranges.len()).then_some(Slot {
                    range: after,
                    entry: 0,
                })
            }
        }
    }

    /// The page at `at`, if a page is there.
    #[must_use]
    pub fn page_at(&self, at: Slot) -> Option<Placed<M>> {
        match self.at(at)? {
            Found::Page { page, .. } => Some(page),
            _ => None,
        }
    }

    /// Cut every range over [`MAX_RANGE_ENTRIES`] in two at the middle
    /// entry, until none is: the right half starts at its first entry's
    /// quantum, with the same shape and base.
    pub fn split_long(&mut self) {
        let mut range = 0;
        while range < self.ranges.len() {
            if self.ranges[range].entries.len() <= MAX_RANGE_ENTRIES {
                range += 1;
                continue;
            }
            let r = &self.ranges[range];
            let cut = r.entries.len() / 2;
            let from = u32::from(q_of(r.entries.pages[cut]));
            let Some(first_key) = r.head.quantum_start(from) else {
                range += 1;
                continue;
            };
            let right = Range {
                head: SubHeader {
                    first_key,
                    ..r.head.clone()
                },
                entries: r.entries.tail(cut, from),
            };
            let left = r.entries.head(cut);
            self.ranges[range].entries = left;
            self.ranges.insert(range + 1, right);
            self.note_whole(range);
            self.note_whole(range + 1);
        }
    }
}
