//! What changed in an index since a header last named it ([RFC 0078] §6),
//! and the records that name it — drained as far as a header's room goes,
//! the rest left for the next header.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use super::head::q_of;
use super::index::{PageIndex, Slot};
use super::persist::{RangeRecord, trimmed_start, whole_of};

/// Ranges an index changed, by start: whole (`None`) or some entries, by
/// quantum.
#[derive(Debug, Clone, Default)]
pub struct Journal {
    ranges: BTreeMap<Vec<u8>, Option<BTreeSet<u16>>>,
}

impl Journal {
    /// Nothing changed.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            ranges: BTreeMap::new(),
        }
    }

    /// Whether anything is waiting for a header.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Everything `other` names, named here too.
    pub fn merge(&mut self, other: &Self) {
        for (start, qs) in &other.ranges {
            match (self.ranges.get_mut(start), qs) {
                (Some(Some(mine)), Some(theirs)) => mine.extend(theirs),
                (Some(mine), None) => *mine = None,
                (Some(None), Some(_)) => {}
                (None, _) => {
                    self.ranges.insert(start.clone(), qs.clone());
                }
            }
        }
    }

    /// Every range of `pages` named whole.
    pub fn whole_all<M: Copy + Default>(&mut self, pages: &PageIndex<M>) {
        for range in 0..pages.ranges.len() {
            self.whole(pages.start_of(range));
        }
    }

    fn whole(&mut self, start: Vec<u8>) {
        self.ranges.insert(start, None);
    }

    fn entry(&mut self, start: Vec<u8>, q: u16) {
        if let Some(qs) = self
            .ranges
            .entry(start)
            .or_insert_with(|| Some(BTreeSet::new()))
        {
            qs.insert(q);
        }
    }
}

impl<M: Copy + Default> PageIndex<M> {
    /// The start of range `range`, as records name it.
    pub(super) fn start_of(&self, range: usize) -> Vec<u8> {
        let head = &self.ranges[range].head;
        trimmed_start(&head.prefix, head.first_key)
    }

    /// Range `range` changed shape: a header names it whole — or gone, if
    /// no range starts there by then.
    pub(super) fn note_whole(&mut self, range: usize) {
        let start = self.start_of(range);
        self.journal.whole(start);
    }

    /// The entry at `at` changed in place.
    pub(super) fn note_entry(&mut self, at: Slot) {
        let start = self.start_of(at.range);
        let q = q_of(self.ranges[at.range].entries.pages[at.entry]);
        self.journal.entry(start, q);
    }

    /// Whether anything is waiting for a header.
    #[must_use]
    pub fn has_unrecorded(&self) -> bool {
        !self.journal.is_empty()
    }

    /// The range starting at `start`, if one does.
    fn range_at(&self, start: &[u8]) -> Option<usize> {
        self.ranges
            .binary_search_by(|r| {
                trimmed_start(&r.head.prefix, r.head.first_key)
                    .as_slice()
                    .cmp(start)
            })
            .ok()
    }

    /// The first range starting after `cursor` (or the first of all) — the
    /// spare room's rotation, which goes by start so a range added or gone
    /// meanwhile never makes it skip one.
    #[must_use]
    pub fn range_after(&self, cursor: Option<&[u8]>) -> Option<usize> {
        let Some(cursor) = cursor else {
            return (!self.ranges.is_empty()).then_some(0);
        };
        let at = self.ranges.partition_point(|r| {
            trimmed_start(&r.head.prefix, r.head.first_key)
                .as_slice()
                .cmp(cursor)
                != Ordering::Greater
        });
        (at < self.ranges.len()).then_some(at)
    }
}

impl PageIndex<u8> {
    /// Name everything: every range whole, for the next header.
    pub fn record_all(&mut self) {
        for range in 0..self.ranges.len() {
            self.note_whole(range);
        }
    }

    /// The record naming range `range` whole, and its start.
    #[must_use]
    pub fn whole_record(&self, range: usize) -> (Vec<u8>, RangeRecord) {
        (self.start_of(range), whole_of(&self.ranges[range]))
    }

    /// Drain what changed into records, as many as `room` bytes hold; what
    /// does not fit stays for the next header. Returns the records and the
    /// bytes they take.
    pub fn drain(&mut self, room: usize) -> (Vec<RangeRecord>, usize) {
        let mut journal = std::mem::take(&mut self.journal);
        let drained = self.drain_from(&mut journal, room);
        self.journal = journal;
        drained
    }

    /// [`drain`](Self::drain), from a journal kept outside the index — a
    /// second reader of its changes.
    pub fn drain_from(
        &self,
        journal: &mut Journal,
        room: usize,
    ) -> (Vec<RangeRecord>, usize) {
        let mut records = Vec::new();
        let mut used = 0;
        let starts: Vec<Vec<u8>> = journal.ranges.keys().cloned().collect();
        for start in starts {
            let record = self.record_for(journal, &start);
            let len = record.wire_len();
            if used + len > room {
                break;
            }
            used += len;
            records.push(record);
            journal.ranges.remove(&start);
        }
        (records, used)
    }

    /// What changed since a header last named it — for a second reader to
    /// take a copy of.
    #[must_use]
    pub const fn journal(&self) -> &Journal {
        &self.journal
    }

    /// The record naming what changed of the range at `start` now.
    fn record_for(&self, journal: &Journal, start: &[u8]) -> RangeRecord {
        let Some(range) = self.range_at(start) else {
            return RangeRecord::Gone {
                start: start.to_vec(),
            };
        };
        let r = &self.ranges[range];
        match journal.ranges.get(start) {
            Some(Some(qs)) => {
                let entries = r
                    .entries
                    .pages
                    .iter()
                    .zip(&r.entries.meta)
                    .filter(|(e, _)| qs.contains(&q_of(**e)))
                    .map(|(&e, &m)| (e, m))
                    .collect();
                RangeRecord::Entries {
                    start: start.to_vec(),
                    base_block: r.head.base_block,
                    shape: u32::from_le_bytes(r.head.shape.to_bytes()),
                    entries,
                }
            }
            _ => whole_of(r),
        }
    }
}
