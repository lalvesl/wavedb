//! An index as the headers record it ([RFC 0078] §6), and its rebuilding
//! from them, newest header first.
//!
//! A range is named by its **start**: its prefix and first slice, trailing
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
use crate::error::{StorageError, StorageResult};

/// One range's record in a header.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub enum RangeRecord {
    Whole(WholeRange),
    /// Some entries of a range: its base and shape restated, so the
    /// entries decode on their own.
    Entries {
        start: Vec<u8>,
        base_block: u64,
        shape: u32,
        entries: Vec<(u32, u8)>,
    },
    Gone {
        start: Vec<u8>,
    },
}

/// A range in full: its sub-header and its entries.
#[derive(Debug, Clone, PartialEq, Eq, WaveWire)]
pub struct WholeRange {
    pub prefix: Vec<u8>,
    pub first_key: u64,
    pub base_block: u64,
    /// [`Shape::to_bytes`], as a `u32` (little-endian bytes).
    pub shape: u32,
    pub pages: Vec<u32>,
    pub meta: Vec<u8>,
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
            Self::Whole(w) => trimmed_start(&w.prefix, w.first_key),
            Self::Entries { start, .. } | Self::Gone { start } => start.clone(),
        }
    }

    /// The pages it names, `(address, blocks)` — holes left out; none for
    /// a record whose shape is not one.
    #[must_use]
    pub fn pages(&self) -> Vec<(u64, u64)> {
        let (base_block, shape, entries): (u64, u32, Vec<u32>) = match self {
            Self::Whole(w) => (w.base_block, w.shape, w.pages.clone()),
            Self::Entries {
                base_block,
                shape,
                entries,
                ..
            } => (*base_block, *shape, entries.iter().map(|e| e.0).collect()),
            Self::Gone { .. } => return Vec::new(),
        };
        let Some(shape) = Shape::from_bytes(shape.to_le_bytes()) else {
            return Vec::new();
        };
        let head = SubHeader {
            base_block,
            first_key: 0,
            prefix: Box::default(),
            shape,
        };
        entries.iter().filter_map(|&e| head.decode(e)).collect()
    }
}

/// A range's start: `prefix ++ first_key` big-endian, trailing zeros
/// trimmed.
#[must_use]
pub fn trimmed_start(prefix: &[u8], first_key: u64) -> Vec<u8> {
    let mut start = prefix.to_vec();
    start.extend_from_slice(&first_key.to_be_bytes());
    let len = start.iter().rposition(|&b| b != 0).map_or(0, |i| i + 1);
    start.truncate(len);
    start
}

/// The record naming `range` whole.
pub(super) fn whole_of(range: &Range<u8>) -> RangeRecord {
    RangeRecord::Whole(WholeRange {
        prefix: range.head.prefix.to_vec(),
        first_key: range.head.first_key,
        base_block: range.head.base_block,
        shape: u32::from_le_bytes(range.head.shape.to_bytes()),
        pages: range.entries.pages.clone(),
        meta: range.entries.meta.clone(),
    })
}

/// One range being gathered.
enum Building {
    /// Entries newer than any `Whole` met, by quantum.
    Open(BTreeMap<u16, (u32, u8)>),
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
                (open, RangeRecord::Entries { entries, .. }) => {
                    let mut known = if let Some(Building::Open(known)) = open {
                        known
                    } else {
                        self.open += 1;
                        BTreeMap::new()
                    };
                    for &(entry, meta) in entries {
                        known.entry(q_of(entry)).or_insert((entry, meta));
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
    newer: &BTreeMap<u16, (u32, u8)>,
) -> StorageResult<Range<u8>> {
    let shape = Shape::from_bytes(whole.shape.to_le_bytes())
        .ok_or(StorageError::Corrupt("a range's shape"))?;
    if whole.pages.is_empty() || whole.pages.len() != whole.meta.len() {
        return Err(StorageError::Corrupt("a range's entries"));
    }
    let mut entries = Entries {
        pages: whole.pages.clone(),
        meta: whole.meta.clone(),
    };
    for (i, entry) in entries.pages.iter_mut().enumerate() {
        if let Some(&(newer, meta)) = newer.get(&q_of(*entry)) {
            *entry = newer;
            entries.meta[i] = meta;
        }
    }
    let ascending = entries.pages.windows(2).all(|w| q_of(w[0]) < q_of(w[1]));
    if q_of(entries.pages[0]) != 0 || !ascending {
        return Err(StorageError::Corrupt("a range's quanta"));
    }
    Ok(Range {
        head: SubHeader {
            base_block: whole.base_block,
            first_key: whole.first_key,
            prefix: whole.prefix.clone().into_boxed_slice(),
            shape,
        },
        entries,
    })
}
