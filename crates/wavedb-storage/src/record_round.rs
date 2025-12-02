//! A settle round over the record cache ([RFC 0068]) — what it copies out
//! of the logs, and what the cache forgets once its pages are written.
//! Split from [`crate::record_cache`] for the file budget.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use crate::record_cache::{RecordCache, Records};
use crate::record_index::Loc;

/// One settle round, copied out of the logs: the records to write per slot,
/// and how far each log's settle cursor moves once they are written.
#[derive(Debug, Default)]
pub struct Round {
    pub work: Vec<(usize, Records)>,
    pub(crate) marks: Vec<Mark>,
    /// Whether it reached every log's limit — the budget cut it nowhere.
    pub complete: bool,
}

impl Round {
    /// Nothing scanned: there was nothing to settle.
    pub const fn is_empty(&self) -> bool {
        self.marks.is_empty()
    }
}

#[derive(Debug)]
pub struct Mark {
    slot: usize,
    end: u64,
    /// Tombstones the round settled — `(hash, location)`.
    tombstones: Vec<(u64, Loc)>,
}

impl RecordCache {
    /// Copy out a round of at most `budget` entries: what the open batch
    /// owes while it owes anything, else whatever is pending. Entries the
    /// index no longer names are dead — a newer copy follows — and are
    /// skipped, though they count against the budget and the cursor passes
    /// them.
    pub fn round(&self, budget: usize) -> Round {
        let owed = self.owes();
        let mut left = budget;
        let mut round = Round {
            complete: true,
            ..Round::default()
        };
        for (slot, log) in self.logs.iter().enumerate() {
            let until = if owed { log.rotated() } else { log.written() };
            let mut records = Records::new();
            let mut mark = None;
            for (at, entry, next) in log.scan(until) {
                if left == 0 {
                    round.complete = false;
                    break;
                }
                left -= 1;
                let mark = mark.get_or_insert_with(|| Mark {
                    slot,
                    end: next,
                    tombstones: Vec::new(),
                });
                mark.end = next;
                let hash = self.index.hash(entry.key);
                if self.index.get(hash, |l| l == at) != Some(at) {
                    continue;
                }
                if entry.bytes.is_none() {
                    mark.tombstones.push((hash, at));
                }
                records.push((
                    entry.key.to_vec(),
                    entry.bytes.map(<[u8]>::to_vec),
                ));
            }
            if let Some(mark) = mark {
                round.marks.push(mark);
                if !records.is_empty() {
                    round.work.push((slot, records));
                }
            }
        }
        round
    }

    /// `round`'s pages are written: move the settle cursors past it, and
    /// forget the tombstones it settled — their pages agree now — unless a
    /// newer write has taken their place.
    pub fn advance(&mut self, round: &Round) {
        for mark in &round.marks {
            if let Some(log) = self.logs.get_mut(mark.slot) {
                log.advance(mark.end);
            }
            for &(hash, at) in &mark.tombstones {
                if self.index.remove(hash, |l| l == at).is_some()
                    && let Some(log) = self.logs.get_mut(at.slot as usize)
                {
                    if let Some(entry) = log.entry(at) {
                        Self::forget(&mut self.ordered, mark.slot, entry.key);
                    }
                    log.mark_dead(at);
                }
            }
        }
    }
}
