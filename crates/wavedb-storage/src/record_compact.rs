//! Compaction of a type's record log ([RFC 0068], amended 2026-10-18): a
//! settled chunk that is mostly superseded copies is rewritten with only the
//! entries the index still names, and folded into the chunk before it when
//! both fit one.
//!
//! Eviction alone dropped whole chunks, oldest first. That is right for a
//! chunk of records, and wrong for one that is mostly dead: a collection's
//! chain segments and index nodes are rewritten whole by every write, so a
//! chunk holding three new records also holds three dead copies of a
//! segment and of an index node — ~98 % of its bytes — and dropping it to
//! reclaim them drops the records too. Measured on the micro bench, the
//! cache held 1 437 of 200 000 records in its 96 MiB, the rest of it dead
//! copies, and a hot read hit it 0.8 % of the time.
//!
//! Only chunks wholly before `s` are compacted, never the one being filled:
//! a settle round reads from `s` on by logical position, and a compaction
//! keeps the span it rewrites, so nothing after it moves. Every entry kept is
//! repointed in the index by the caller (`relocate`), which is also what
//! tells a live entry from a dead one.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

use crate::record_index::Loc;
use crate::record_log::{CHUNK, TypeLog, parse};

/// A chunk is worth compacting once this share of it is dead: 1 in 4.
const DEAD_SHARE: usize = 4;

/// Append to `into` (chunk `n` of slot `slot`) every entry of `from` that
/// `relocate(id, from, to)` confirms — it repoints the index and answers
/// whether it named that entry. An entry it does not name is dead and is
/// left behind.
fn carry(
    from: &[u8],
    from_chunk: u32,
    into: &mut Vec<u8>,
    slot: u16,
    n: u32,
    relocate: &mut impl FnMut(u128, Loc, Loc) -> bool,
) {
    let mut at = 0;
    while let Some((entry, len)) = parse(from, at) {
        let old = Loc {
            slot,
            offset: at as u16,
            chunk: from_chunk,
        };
        let new = Loc {
            slot,
            offset: into.len() as u16,
            chunk: n,
        };
        let mark = into.len();
        into.extend_from_slice(&from[at..at + len]);
        if !relocate(entry.id, old, new) {
            into.truncate(mark);
        }
        at += len;
    }
}

impl TypeLog {
    /// The chunk most worth compacting — wholly settled, not the one being
    /// filled, at least a [`DEAD_SHARE`]th of it dead — as `(dead bytes,
    /// number)`, the most dead first.
    pub fn compactable(&self) -> Option<(usize, u32)> {
        let back = self.back();
        self.order
            .iter()
            .filter(|&&n| Some(n) != back)
            .filter_map(|&n| {
                let c = self.chunk(n)?;
                let worth = c.end <= self.s
                    && c.dead > 0
                    && c.dead * DEAD_SHARE >= c.bytes.len();
                worth.then_some((c.dead, n))
            })
            .max()
    }

    /// Rewrite chunk `n` with the entries `relocate` confirms live — it
    /// repoints each in the index — then fold it into the chunk before it
    /// when both fit in one, and let it go if nothing is left.
    ///
    /// The caller has checked [`compactable`](Self::compactable): `n` is
    /// wholly settled and not the chunk being filled.
    pub fn compact(
        &mut self,
        n: u32,
        mut relocate: impl FnMut(u128, Loc, Loc) -> bool,
    ) {
        let slot = self.slot;
        let Some(chunk) = self.chunk_mut(n) else {
            return;
        };
        let old = std::mem::take(&mut chunk.bytes);
        let mut kept = Vec::new();
        carry(&old, n, &mut kept, slot, n, &mut relocate);
        drop(old);
        kept.shrink_to_fit();
        let empty = kept.is_empty();
        chunk.bytes = kept;
        chunk.dead = 0;
        if empty {
            self.release(n);
        } else {
            self.fold(n, &mut relocate);
        }
    }

    /// Fold chunk `n` into the one before it in the log, when their entries
    /// fit one chunk: its entries move there (repointed), the earlier chunk
    /// takes its span, and `n` goes. Both are settled — the one before a
    /// settled chunk is — and neither is the chunk being filled.
    fn fold(
        &mut self,
        n: u32,
        relocate: &mut impl FnMut(u128, Loc, Loc) -> bool,
    ) {
        let Some(at) = self.order.iter().position(|&m| m == n) else {
            return;
        };
        let Some(prev) =
            at.checked_sub(1).and_then(|p| self.order.get(p).copied())
        else {
            return;
        };
        let (Some(p), Some(c)) = (self.chunk(prev), self.chunk(n)) else {
            return;
        };
        if p.bytes.len() + c.bytes.len() > CHUNK {
            return;
        }
        let Some(moving) = self.release(n) else {
            return;
        };
        let slot = self.slot;
        let Some(into) = self.chunk_mut(prev) else {
            return;
        };
        into.bytes.reserve_exact(moving.bytes.len());
        carry(&moving.bytes, n, &mut into.bytes, slot, prev, relocate);
        into.end = moving.end;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::record_index::Loc;
    use crate::record_log::{CHUNK, TypeLog};

    /// A stand-in index: id → the location of its newest entry.
    #[derive(Default)]
    struct Names(BTreeMap<u128, Loc>);

    impl Names {
        fn put(
            &mut self,
            log: &mut TypeLog,
            id: u128,
            bytes: &[u8],
            clock: &mut u64,
        ) {
            let loc = log.append(id, Some(bytes), clock);
            if let Some(old) = self.0.insert(id, loc) {
                log.mark_dead(old);
            }
        }

        fn relocate(&mut self) -> impl FnMut(u128, Loc, Loc) -> bool + '_ {
            |id, from, to| match self.0.get_mut(&id) {
                Some(at) if *at == from => {
                    *at = to;
                    true
                }
                _ => false,
            }
        }
    }

    /// Three records interleaved with ten rewrites of one hot entry: once
    /// settled, compaction keeps the records and the hot entry's newest
    /// copy, each readable at its new place, and frees the rest.
    #[test]
    fn compaction_keeps_what_the_index_names_and_frees_the_rest() {
        let mut log = TypeLog::new(2);
        let mut names = Names::default();
        let mut clock = 0;
        let hot = vec![0xEE; 6_000];
        for round in 0..30u128 {
            names.put(&mut log, 1_000 + round, &[round as u8; 300], &mut clock);
            names.put(&mut log, 1, &hot, &mut clock);
        }
        // One more chunk, so the ones before it are not the chunk being
        // filled.
        names.put(&mut log, 9_999, &[0; 10], &mut clock);
        log.advance(log.written());
        while let Some((_, n)) = log.compactable() {
            log.compact(n, names.relocate());
        }
        // Everything but the chunk being filled holds live entries only.
        let back = log.back();
        let held: usize = log
            .order
            .iter()
            .filter(|&&n| Some(n) != back)
            .map(|&n| log.chunk(n).unwrap().bytes.len())
            .sum();
        assert!(held <= 30 * (300 + 20) + 6_000 + 20, "{held}");
        for (&id, &loc) in &names.0 {
            assert_eq!(
                log.entry(loc).unwrap().id,
                id,
                "{id} moved with its name"
            );
        }
        assert_eq!(log.entry(names.0[&1]).unwrap().bytes, Some(&hot[..]));
        assert_eq!(
            log.scan(log.written()).count(),
            0,
            "nothing came unsettled"
        );
    }

    /// Small compacted chunks fold into the one before them, so the log
    /// does not end as one tiny chunk per original one.
    #[test]
    fn compacted_neighbours_fold_into_one_chunk() {
        let mut log = TypeLog::new(0);
        let mut names = Names::default();
        let mut clock = 0;
        let filler = vec![1u8; CHUNK / 2];
        for k in 0..8u128 {
            names.put(&mut log, 100 + k, &[k as u8; 50], &mut clock);
            names.put(&mut log, 7, &filler, &mut clock);
        }
        names.put(&mut log, 9_999, &[0; 10], &mut clock);
        log.advance(log.written());
        let chunks = log.order.len();
        while let Some((_, n)) = log.compactable() {
            log.compact(n, names.relocate());
        }
        assert!(
            log.order.len() < chunks / 2,
            "{} of {chunks}",
            log.order.len()
        );
        for (&id, &loc) in &names.0 {
            assert_eq!(log.entry(loc).unwrap().id, id);
        }
    }

    /// Neither an unsettled chunk nor the chunk being filled is compacted,
    /// however dead.
    #[test]
    fn nothing_unsettled_is_compactable() {
        let mut log = TypeLog::new(0);
        let mut names = Names::default();
        let mut clock = 0;
        for _ in 0..40 {
            names.put(&mut log, 1, &[3u8; 4_000], &mut clock);
        }
        assert_eq!(log.compactable(), None, "nothing is settled");
        log.advance(log.written());
        let back = log.back();
        while let Some((_, n)) = log.compactable() {
            assert_ne!(Some(n), back);
            log.compact(n, names.relocate());
        }
        assert_eq!(log.back(), back, "the chunk being filled is still there");
    }
}
