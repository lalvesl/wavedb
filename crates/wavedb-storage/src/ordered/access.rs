//! What the store asks of an index besides lookups: rebuilding it, walking
//! its pages, naming a page by a key, sizing a page, and keeping a range
//! within what one header can restate.

use super::head::{Shape, q_of};
use super::index::{Found, PageIndex, Placed, Range, Slot};
use super::journal::Journal;

/// Entries a range holds at most: one range, restated whole, takes ~5 KiB
/// of a header (at most 128 KiB, RFC 0079).
pub const MAX_RANGE_ENTRIES: usize = 1024;

/// A page's count from here up says only "this many or more": its entries
/// are counted by reading it ([RFC 0078] §7).
///
/// [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
pub const COUNT_SATURATED: u8 = u8::MAX;

impl<M: Copy + Default> PageIndex<M> {
    /// An index over `ranges`, sorted and disjoint, nothing unrecorded —
    /// counted when they count.
    #[must_use]
    pub(super) fn from_ranges(seed: Shape, ranges: Vec<Range<M>>) -> Self {
        Self {
            seed,
            counted: ranges.first().is_some_and(|r| r.entries.counts.is_some()),
            ranges,
            journal: Journal::new(),
        }
    }

    /// How many entries `at`'s page holds — `None` where the index counts
    /// nothing, or the page holds [`COUNT_SATURATED`] or more.
    #[must_use]
    pub fn count_at(&self, at: Slot) -> Option<u64> {
        let n = self.ranges.get(at.range)?.entries.count(at.entry)?;
        (n < COUNT_SATURATED).then_some(u64::from(n))
    }

    /// `at`'s page holds `n` entries — kept in a counted index, saturating
    /// at [`COUNT_SATURATED`].
    pub fn set_count(&mut self, at: Slot, n: usize) -> Option<()> {
        let counts = self.ranges.get_mut(at.range)?.entries.counts.as_mut()?;
        let held = counts.get_mut(at.entry)?;
        let n = u8::try_from(n).unwrap_or(COUNT_SATURATED);
        if *held != n {
            *held = n;
            self.note_entry(at);
        }
        Some(())
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

    /// Bytes an entry takes: its `u32`, what is kept beside it, and in a
    /// counted index its count byte.
    #[must_use]
    pub fn entry_bytes(&self) -> u64 {
        (size_of::<u32>() + size_of::<M>() + usize::from(self.counted)) as u64
    }

    /// Bytes a range takes beside its entries, its prefix aside: its
    /// sub-header and its vectors.
    #[must_use]
    pub const fn range_bytes(&self) -> u64 {
        size_of::<Range<M>>() as u64
    }

    /// The RAM the index holds, estimated from its parts: each range
    /// ([`range_bytes`](Self::range_bytes)) and its prefix, and its entries
    /// ([`entry_bytes`](Self::entry_bytes) each). O(ranges).
    #[must_use]
    pub fn ram_bytes(&self) -> u64 {
        self.ranges
            .iter()
            .map(|r| {
                self.range_bytes()
                    + r.head.prefix.len() as u64
                    + self.entry_bytes() * r.entries.len() as u64
            })
            .sum()
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
        Some(r.head.key_at(q))
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
        if let Some(counts) = &mut r.entries.counts {
            counts[at.entry] = 0;
        }
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

    /// The entry before `at`, in key order.
    #[must_use]
    pub fn prev(&self, at: Slot) -> Option<Slot> {
        if at.entry > 0 {
            return Some(Slot {
                range: at.range,
                entry: at.entry - 1,
            });
        }
        let range = at.range.checked_sub(1)?;
        let entries = self.ranges.get(range)?.entries.len();
        Some(Slot {
            range,
            entry: entries.checked_sub(1)?,
        })
    }

    /// The last entry, in key order.
    #[must_use]
    pub fn last(&self) -> Option<Slot> {
        let range = self.ranges.len().checked_sub(1)?;
        let entry = self.ranges[range].entries.len().checked_sub(1)?;
        Some(Slot { range, entry })
    }

    /// Whether `at` is its range's only entry: removing it removes the
    /// range, and gives its quanta to no other entry.
    #[must_use]
    pub fn is_alone(&self, at: Slot) -> bool {
        self.ranges
            .get(at.range)
            .is_some_and(|r| r.entries.len() == 1)
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
            let right = Range {
                head: r.head.from_quantum(q_of(r.entries.pages[cut])),
                entries: r.entries.tail(cut),
            };
            let left = r.entries.head(cut);
            self.ranges[range].entries = left;
            self.ranges.insert(range + 1, right);
            self.note_whole(range);
            self.note_whole(range + 1);
        }
    }
}
