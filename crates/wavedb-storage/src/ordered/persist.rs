//! An index as the headers record it ([RFC 0078] §6), and its rebuilding
//! from them, newest header first.
//!
//! A range is named by its **start**: its prefix and first quantum, trailing
//! zero bytes trimmed — the same start however deep its sub-header reads
//! it. Three records name a range:
//! - [`RangeRecord::Whole`]: the sub-header and every entry — what any
//!   change to its shape writes (an entry added or dropped, a range opened
//!   or cut), and what the spare room restates;
//! - [`RangeRecord::Entries`]: some entries, by quantum, with the range's
//!   shape unchanged since its last `Whole` — a page rewritten in place;
//! - [`RangeRecord::Gone`]: no range starts there any more.
//!
//! Read newest first, the first record naming an entry is its value now,
//! and a range is known whole at its first `Whole` or `Gone`: anything
//! older about it is history.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::BTreeMap;

use wavedb_core::wire::WaveWire;

use super::head::{Shape, SubHeader, q_of};
use super::index::{Entries, PageIndex, Range};
use super::key::join;
use crate::error::{StorageError, StorageResult};

/// One range's record in a header.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum RangeRecord {
    Whole(WholeRange),
    /// Some entries of a range: its base and shape restated, so the
    /// entries decode on their own; their counts beside them in a counted
    /// index, and none otherwise.
    Entries {
        start: Vec<u8>,
        base_block: u64,
        shape: [u8; 3],
        entries: Vec<(u32, u8)>,
        counts: Vec<u8>,
    },
    Gone {
        start: Vec<u8>,
    },
}

/// A range in full: its sub-header and its entries.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct WholeRange {
    /// The prefix's bytes, `prefix_bits.div_ceil(8)` of them.
    pub prefix: Vec<u8>,
    pub prefix_bits: u16,
    /// The quantum the range starts at: its first entry's.
    pub start: u16,
    pub base_block: u64,
    /// [`Shape::to_bytes`].
    pub shape: [u8; 3],
    pub pages: Vec<u32>,
    pub meta: Vec<u8>,
    /// Each page's count in a counted index; empty otherwise.
    pub counts: Vec<u8>,
}

impl RangeRecord {
    /// Bytes it takes in a header.
    #[must_use]
    pub fn wire_len(&self) -> usize {
        Self::STACK_SIZE + self.heap_size()
    }

    /// The start of the range it names.
    #[must_use]
    pub fn start(&self) -> Vec<u8> {
        match self {
            Self::Whole(w) => trimmed_start(&w.prefix, w.prefix_bits, w.start),
            Self::Entries { start, .. } | Self::Gone { start } => start.clone(),
        }
    }

    /// The pages it names, `(address, blocks)` — holes left out; none for
    /// a record whose shape is not one.
    #[must_use]
    pub fn pages(&self) -> Vec<(u64, u64)> {
        let (base_block, shape, entries): (u64, [u8; 3], Vec<u32>) = match self
        {
            Self::Whole(w) => (w.base_block, w.shape, w.pages.clone()),
            Self::Entries {
                base_block,
                shape,
                entries,
                ..
            } => (*base_block, *shape, entries.iter().map(|e| e.0).collect()),
            Self::Gone { .. } => return Vec::new(),
        };
        let Some(shape) = Shape::from_bytes(shape) else {
            return Vec::new();
        };
        let head = SubHeader {
            base_block,
            prefix: Box::default(),
            prefix_bits: 0,
            start: 0,
            shape,
        };
        entries.iter().filter_map(|&e| head.decode(e)).collect()
    }
}

/// A range's start: its prefix's `prefix_bits` bits, then `start`'s 16,
/// trailing zero bytes trimmed — one start however deep its sub-header
/// reads it.
#[must_use]
pub fn trimmed_start(prefix: &[u8], prefix_bits: u16, start: u16) -> Vec<u8> {
    let mut start = join(prefix, usize::from(prefix_bits), start);
    let len = start.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    start.truncate(len);
    start
}

/// The record naming `range` whole.
pub(super) fn whole_of(range: &Range<u8>) -> RangeRecord {
    RangeRecord::Whole(WholeRange {
        prefix: range.head.prefix.to_vec(),
        prefix_bits: range.head.prefix_bits,
        start: range.head.start,
        base_block: range.head.base_block,
        shape: range.head.shape.to_bytes(),
        pages: range.entries.pages.clone(),
        meta: range.entries.meta.clone(),
        counts: range.entries.counts.clone().unwrap_or_default(),
    })
}

/// An entry as a record restates it: the entry, its flags, its count.
type Restated = (u32, u8, Option<u8>);

/// One range being gathered.
enum Building {
    /// Entries newer than any `Whole` met, by quantum.
    Open(BTreeMap<u16, Restated>),
    /// Known whole.
    Closed(Range<u8>),
    /// Known gone.
    Gone,
}

/// One index being rebuilt from its records, newest header first.
#[derive(Default)]
pub struct Rebuild {
    ranges: BTreeMap<Vec<u8>, Building>,
    /// Ranges the newest header says the index holds.
    live: usize,
    closed: usize,
    open: usize,
}

impl Rebuild {
    /// An index the newest header says holds `live` ranges.
    #[must_use]
    pub fn new(live: u32) -> Self {
        Self {
            live: live as usize,
            ..Self::default()
        }
    }

    /// Whether every range is known.
    #[must_use]
    pub const fn is_done(&self) -> bool {
        self.open == 0 && self.closed >= self.live
    }

    /// Take one header's records, which are older than every one taken
    /// before.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] for a record that is not a range, or a
    /// range newer records still change that an older one says is gone.
    pub fn take(&mut self, records: &[RangeRecord]) -> StorageResult<()> {
        for record in records {
            let start = record.start();
            let state = self.ranges.remove(&start);
            let next = match (state, record) {
                (Some(done @ (Building::Closed(_) | Building::Gone)), _) => {
                    done
                }
                (open, RangeRecord::Whole(whole)) => {
                    let newer = match open {
                        Some(Building::Open(newer)) => {
                            self.open -= 1;
                            newer
                        }
                        _ => BTreeMap::new(),
                    };
                    self.closed += 1;
                    Building::Closed(range_of(whole, &newer)?)
                }
                (
                    open,
                    RangeRecord::Entries {
                        entries, counts, ..
                    },
                ) => {
                    if !counts.is_empty() && counts.len() != entries.len() {
                        return Err(StorageError::Corrupt("a range's counts"));
                    }
                    let mut known = if let Some(Building::Open(known)) = open {
                        known
                    } else {
                        self.open += 1;
                        BTreeMap::new()
                    };
                    for (i, &(entry, meta)) in entries.iter().enumerate() {
                        let count = counts.get(i).copied();
                        known
                            .entry(q_of(entry))
                            .or_insert((entry, meta, count));
                    }
                    Building::Open(known)
                }
                (Some(Building::Open(_)), RangeRecord::Gone { .. }) => {
                    return Err(StorageError::Corrupt(
                        "a range changed after it was gone",
                    ));
                }
                (None, RangeRecord::Gone { .. }) => Building::Gone,
            };
            self.ranges.insert(start, next);
        }
        Ok(())
    }

    /// The index rebuilt, quantizing new ranges by `seed`.
    ///
    /// # Errors
    /// [`StorageError::Corrupt`] when a range is still partly unknown, or
    /// the ranges known are not the count the newest header gave.
    pub fn finish(self, seed: Shape) -> StorageResult<PageIndex<u8>> {
        if !self.is_done() || self.closed != self.live {
            return Err(StorageError::Corrupt("an index rebuilt in part"));
        }
        let ranges = self
            .ranges
            .into_values()
            .filter_map(|b| match b {
                Building::Closed(range) => Some(range),
                _ => None,
            })
            .collect();
        Ok(PageIndex::from_ranges(seed, ranges))
    }
}

/// The range `whole` names, with the `newer` entries over its own.
fn range_of(
    whole: &WholeRange,
    newer: &BTreeMap<u16, Restated>,
) -> StorageResult<Range<u8>> {
    let shape = Shape::from_bytes(whole.shape)
        .ok_or(StorageError::Corrupt("a range's shape"))?;
    if whole.prefix.len() != usize::from(whole.prefix_bits).div_ceil(8) {
        return Err(StorageError::Corrupt("a range's prefix"));
    }
    let n = whole.pages.len();
    let counted = !whole.counts.is_empty();
    if n == 0 || whole.meta.len() != n || (counted && whole.counts.len() != n) {
        return Err(StorageError::Corrupt("a range's entries"));
    }
    // The newer entries over the range's own, by quantum: a quantum the
    // range did not have is an entry added since — a cut, a span — which
    // changed no sub-header.
    let mut by_q: BTreeMap<u16, Restated> = (0..n)
        .map(|i| {
            let count = counted.then(|| whole.counts[i]);
            (q_of(whole.pages[i]), (whole.pages[i], whole.meta[i], count))
        })
        .collect();
    for (&q, &(entry, meta, count)) in newer {
        let held = by_q
            .entry(q)
            .or_insert_with(|| (entry, meta, count.or(Some(0))));
        *held = (entry, meta, count.or(held.2));
    }
    let mut entries = Entries {
        pages: Vec::with_capacity(by_q.len()),
        meta: Vec::with_capacity(by_q.len()),
        counts: counted.then(|| Vec::with_capacity(by_q.len())),
    };
    for (entry, meta, count) in by_q.into_values() {
        entries.pages.push(entry);
        entries.meta.push(meta);
        if let Some(counts) = &mut entries.counts {
            counts.push(count.unwrap_or(0));
        }
    }
    if q_of(entries.pages[0]) != whole.start {
        return Err(StorageError::Corrupt("a range's quanta"));
    }
    Ok(Range {
        head: SubHeader {
            base_block: whole.base_block,
            prefix: whole.prefix.clone().into_boxed_slice(),
            prefix_bits: whole.prefix_bits,
            start: whole.start,
            shape,
        },
        entries,
    })
}
