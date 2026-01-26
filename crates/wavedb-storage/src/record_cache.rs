//! A store's record cache ([RFC 0068]): one [`RecordIndex`] over one
//! [`TypeLog`] per registered type.
//!
//! The logs keep the types apart — a page holds one type, so a settle round
//! is per type anyway, and a type that is rarely used costs a few hundred
//! bytes of log, not a share of everyone else's chunks. The index is shared:
//! its slot already names the type, and one index per type would be one
//! mostly empty segment per type per shard.
//!
//! Everything here is memory work, never borrowed across a page read or
//! write.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use wavedb_core::Id;

use crate::directory::hash_of;
use crate::record_index::{Loc, RecordIndex};
use crate::record_log::TypeLog;

/// What the cache knows about one record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lookup {
    /// Its newest bytes.
    Hit(Vec<u8>),
    /// Removed, and the page may still hold its old bytes.
    Removed,
    /// Not cached: its page is the truth.
    Miss,
}

/// One type's records a round settles: `None` bytes remove the id.
pub type Records = Vec<(Id, Option<Vec<u8>>)>;

/// One settle round, copied out of the logs: the records to write per slot,
/// and how far each log's settle cursor moves once they are written.
#[derive(Debug, Default)]
pub struct Round {
    pub work: Vec<(usize, Records)>,
    marks: Vec<Mark>,
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
struct Mark {
    slot: usize,
    end: u64,
    /// Tombstones the round settled — `(hash, location)`.
    tombstones: Vec<(u64, Loc)>,
}

/// The cache (see the module docs).
#[derive(Debug)]
pub struct RecordCache {
    index: RecordIndex,
    logs: Vec<TypeLog>,
    /// Stamps chunks with their age, across every log.
    clock: u64,
}

/// Whether `loc` holds `id`'s entry — the slot check is free, the id is read
/// from the log.
fn holds(logs: &[TypeLog], loc: Loc, id: u128) -> bool {
    logs.get(loc.slot as usize)
        .and_then(|log| log.entry(loc))
        .is_some_and(|entry| entry.id == id)
}

impl RecordCache {
    /// An empty cache for `types` registered slots, its index seeded with
    /// `seed`.
    pub fn new(types: usize, seed: [u64; 4]) -> Self {
        Self {
            index: RecordIndex::new(seed),
            logs: (0..types).map(|slot| TypeLog::new(slot as u16)).collect(),
            clock: 0,
        }
    }

    /// Commit `id`'s newest bytes in `slot` — `None` removes it.
    pub fn put(&mut self, slot: usize, id: Id, bytes: Option<&[u8]>) {
        let Some(log) = self.logs.get_mut(slot) else {
            return;
        };
        let loc = log.append(id.raw(), bytes, &mut self.clock);
        let seed = self.index.seed();
        let logs = &self.logs;
        self.index.upsert(
            hash_of(id.raw(), seed),
            loc,
            |at| at.slot == loc.slot && holds(logs, at, id.raw()),
            |at| {
                let held = logs.get(at.slot as usize).and_then(|l| l.entry(at));
                held.map_or(0, |entry| hash_of(entry.id, seed))
            },
        );
    }

    /// What the cache knows about `id` in `slot`.
    pub fn get(&self, slot: usize, id: Id) -> Lookup {
        self.lookup(id, |at| at.slot as usize == slot)
    }

    /// What the cache knows about `id`, in whichever type holds it.
    pub fn get_any(&self, id: Id) -> Lookup {
        self.lookup(id, |_| true)
    }

    fn lookup(&self, id: Id, mut slot_ok: impl FnMut(Loc) -> bool) -> Lookup {
        let hash = self.index.hash(id.raw());
        let found = self
            .index
            .get(hash, |at| slot_ok(at) && holds(&self.logs, at, id.raw()));
        let entry = found.and_then(|at| {
            self.logs.get(at.slot as usize).and_then(|l| l.entry(at))
        });
        entry.map_or(Lookup::Miss, |entry| {
            entry
                .bytes
                .map_or(Lookup::Removed, |bytes| Lookup::Hit(bytes.to_vec()))
        })
    }

    /// Records indexed — live ones, and removes not yet settled.
    pub const fn len(&self) -> usize {
        self.index.len()
    }

    /// Bytes the cache holds: the index and every log.
    pub fn footprint(&self) -> usize {
        self.index.footprint()
            + self.logs.iter().map(TypeLog::footprint).sum::<usize>()
    }

    /// Bytes of log not settled yet, across every type.
    pub fn unsettled_bytes(&self) -> u64 {
        self.logs.iter().map(TypeLog::unsettled).sum()
    }

    /// Whether anything committed may still be owed to a page.
    pub fn has_pending(&self) -> bool {
        self.logs.iter().any(TypeLog::pending)
    }

    /// Whether the open batch still owes a settle.
    pub fn owes(&self) -> bool {
        self.logs.iter().any(TypeLog::owes)
    }

    /// A batch opened: everything written is owed to it.
    pub fn rotate(&mut self) {
        self.logs.iter_mut().for_each(TypeLog::rotate);
    }

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
                let hash = self.index.hash(entry.id);
                if self.index.get(hash, |l| l == at) != Some(at) {
                    continue;
                }
                let id = Id::from_raw(entry.id);
                if entry.bytes.is_none() {
                    mark.tombstones.push((hash, at));
                }
                records.push((id, entry.bytes.map(<[u8]>::to_vec)));
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
                self.index.remove(hash, |l| l == at);
            }
        }
    }

    /// Drop settled chunks, oldest first across every type, until the cache
    /// holds at most `budget` bytes or nothing settled is left. An entry
    /// whose chunk goes leaves the index if the index still names it; a
    /// read of it then falls through to its page.
    pub fn evict(&mut self, budget: usize) {
        while self.footprint() > budget {
            let oldest = self
                .logs
                .iter()
                .enumerate()
                .filter_map(|(slot, log)| {
                    log.oldest_settled().map(|age| (age, slot))
                })
                .min();
            let Some((_, slot)) = oldest else {
                return;
            };
            let seed = self.index.seed();
            let index = &mut self.index;
            if let Some(log) = self.logs.get_mut(slot) {
                log.drop_oldest(|at, id| {
                    index.remove(hash_of(id, seed), |l| l == at);
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use wavedb_core::{Id, U48};

    use super::{Lookup, RecordCache};

    fn id(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, 0)
    }

    fn cache() -> RecordCache {
        RecordCache::new(2, [9, 8, 7, 6])
    }

    fn hit(bytes: &[u8]) -> Lookup {
        Lookup::Hit(bytes.to_vec())
    }

    #[test]
    fn the_newest_write_answers_and_a_remove_is_remembered() {
        let mut c = cache();
        c.put(0, id(1), Some(b"v1"));
        c.put(0, id(1), Some(b"v2"));
        assert_eq!(c.get(0, id(1)), hit(b"v2"));
        assert_eq!(c.len(), 1, "a rewrite repoints, it does not add");
        c.put(0, id(1), None);
        assert_eq!(c.get(0, id(1)), Lookup::Removed);
        assert_eq!(c.get(0, id(2)), Lookup::Miss);
    }

    /// The key is `(Id, type)`: the same id in two types is two records.
    #[test]
    fn one_id_in_two_types_is_two_records() {
        let mut c = cache();
        c.put(0, id(1), Some(b"zero"));
        c.put(1, id(1), Some(b"one"));
        assert_eq!(c.get(0, id(1)), hit(b"zero"));
        assert_eq!(c.get(1, id(1)), hit(b"one"));
        assert_eq!(c.len(), 2);
    }

    /// A round settles a record once, at its newest version, and a write
    /// landing during the round is the next round's.
    #[test]
    fn a_round_takes_each_record_once_at_its_newest() {
        let mut c = cache();
        for v in 0..5u8 {
            c.put(0, id(1), Some(&[v]));
        }
        c.put(1, id(2), Some(b"other type"));
        let round = c.round(usize::MAX);
        assert_eq!(round.work.len(), 2, "one batch of records per type");
        assert_eq!(round.work[0].1, vec![(id(1), Some(vec![4]))]);
        c.put(0, id(1), Some(b"during"));
        c.advance(&round);
        assert!(c.has_pending(), "the write during the round is pending");
        let next = c.round(usize::MAX);
        assert_eq!(
            next.work,
            vec![(0, vec![(id(1), Some(b"during".to_vec()))])]
        );
        c.advance(&next);
        assert!(!c.has_pending());
    }

    #[test]
    fn a_settled_tombstone_leaves_the_index() {
        let mut c = cache();
        c.put(0, id(1), Some(b"v"));
        c.put(0, id(1), None);
        let round = c.round(usize::MAX);
        assert_eq!(round.work, vec![(0, vec![(id(1), None)])]);
        c.advance(&round);
        assert_eq!(c.get(0, id(1)), Lookup::Miss, "the page agrees now");
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn a_rotation_bounds_the_rounds_until_it_is_settled() {
        let mut c = cache();
        c.put(0, id(1), Some(b"owed"));
        c.rotate();
        c.put(0, id(2), Some(b"fresh"));
        assert!(c.owes());
        let round = c.round(usize::MAX);
        assert_eq!(
            round.work,
            vec![(0, vec![(id(1), Some(b"owed".to_vec()))])]
        );
        c.advance(&round);
        assert!(!c.owes() && c.has_pending());
    }

    /// Eviction never drops what is unsettled, drops what is, and the
    /// dropped records leave the index.
    #[test]
    fn eviction_drops_only_settled_chunks() {
        let mut c = cache();
        let body = vec![7u8; 1_000];
        for k in 0..300 {
            c.put(0, id(k), Some(&body));
        }
        c.evict(0);
        assert_eq!(c.len(), 300, "nothing is settled yet");
        let mut round = c.round(150);
        c.advance(&round);
        c.evict(0);
        assert_eq!(c.get(0, id(299)), hit(&body), "unsettled stays");
        assert_eq!(c.get(0, id(0)), Lookup::Miss, "settled leaves");
        round = c.round(usize::MAX);
        c.advance(&round);
        c.evict(0);
        assert_eq!(c.len(), 0);
        assert!(c.footprint() < 64 << 10, "only the index is left");
    }
}
