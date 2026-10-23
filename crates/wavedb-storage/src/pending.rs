//! The pending queue — ids committed to the caches but not yet settled into
//! pages — split by **who owes them** ([RFC 0063] part 3, [RFC 0067] step 3).
//!
//! A checkpoint used to rotate the journal and then `drain` everything queued
//! in one go: uninterruptible, and as long as the journal it was retiring.
//! Stepping it means a checkpoint stays open across many calls while writes
//! keep landing, so "everything queued" is no longer the set it has to cover.
//! The set it covers is exactly the ids committed into the journal it rotated
//! out — and those are the ids queued **at the rotation**, because the commit
//! path queues under the journal lock the rotation also holds.
//!
//! So the queue has two halves:
//!
//! - **owed** — what the open checkpoint must settle before its `Commit`
//!   frame may name the old journal as covered;
//! - **fresh** — committed since, into the live journal; the next
//!   checkpoint's problem, though any settle round may take them.
//!
//! A round is drawn from one half, owed first, and remembers which, so a
//! failed round goes back where it came from. Rounds taken from `owed` and
//! not yet finished are counted: the checkpoint is not done while one is
//! still writing its pages.
//!
//! [RFC 0063]: ../../../rfcs/0063-engine-yield-map-and-interruptible-engine-PLANNED.md
//! [RFC 0067]: ../../../rfcs/0067-thread-per-core-shards-WIP.md

use wavedb_core::Id;

use crate::page_store::Touched;

/// The two halves of the queue (see the module docs).
#[derive(Debug, Default)]
pub struct Pending {
    owed: Touched,
    fresh: Touched,
    /// Rounds taken from `owed` whose pages are not written yet.
    owed_taken: usize,
}

/// One settle round: the ids, and which half they came from.
#[derive(Debug)]
pub struct Round {
    pub ids: Touched,
    owed: bool,
}

impl Round {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

impl Pending {
    /// Nothing queued in either half — invariant I2's "queue empty", which
    /// `evict_settled` trusts.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.owed.is_empty() && self.fresh.is_empty()
    }

    /// Whether the open checkpoint still owes a settle: ids queued for it, or
    /// a round of them still being written.
    #[must_use]
    pub const fn owes(&self) -> bool {
        !self.owed.is_empty() || self.owed_taken > 0
    }

    /// Queue one batch's touched ids — always `fresh`: a batch commits into
    /// the live journal.
    pub fn note(&mut self, touched: Touched) {
        merge(&mut self.fresh, touched);
    }

    /// A checkpoint rotated the journal: everything queued until now is what
    /// it owes. Called under the journal lock, with the rotation.
    pub fn rotate(&mut self) {
        let fresh = std::mem::take(&mut self.fresh);
        merge(&mut self.owed, fresh);
    }

    /// Take a round of at most `budget` ids — from `owed` while it has any,
    /// so a settle always works toward the open checkpoint first.
    pub fn take(&mut self, budget: usize) -> Round {
        let owed = !self.owed.is_empty();
        let queue = if owed {
            &mut self.owed
        } else {
            &mut self.fresh
        };
        let ids = take_from(queue, budget);
        if owed && !ids.is_empty() {
            self.owed_taken += 1;
        }
        Round { ids, owed }
    }

    /// The round's pages are written: it no longer counts against the
    /// checkpoint.
    pub const fn settled(&mut self, round: &Round) {
        if round.owed && !round.is_empty() {
            self.owed_taken -= 1;
        }
    }

    /// Put a failed round back where it came from. Ids may duplicate what
    /// landed meanwhile; settling twice is idempotent.
    pub fn requeue(&mut self, round: Round) {
        self.settled(&round);
        let queue = if round.owed {
            &mut self.owed
        } else {
            &mut self.fresh
        };
        for (idx, ids) in round.ids {
            match queue.iter_mut().find(|(i, _)| *i == idx) {
                Some((_, existing)) => existing.extend(ids),
                None => queue.push((idx, ids)),
            }
        }
    }
}

/// Record `id` as touched under slot `idx`.
pub fn note_touched(touched: &mut Touched, idx: usize, id: Id) {
    match touched.iter_mut().find(|(i, _)| *i == idx) {
        Some((_, ids)) => ids.push(id),
        None => touched.push((idx, vec![id])),
    }
}

/// Merge `touched` into `queue` (slot-grouped).
fn merge(queue: &mut Touched, touched: Touched) {
    for (idx, ids) in touched {
        for id in ids {
            note_touched(queue, idx, id);
        }
    }
}

/// Up to `budget` ids off `queue`, `usize::MAX` meaning all of it.
///
/// The bound holds **within** a slot as well as across slots: whole slots
/// only would make the budget useless in the common case, since a burst of
/// one type is one slot. Two rounds may then rewrite a bucket they share;
/// writes landing during a round already do the same, and re-settling
/// converges.
fn take_from(queue: &mut Touched, budget: usize) -> Touched {
    if budget == usize::MAX {
        return std::mem::take(queue);
    }
    let mut round = Touched::new();
    let mut room = budget;
    while room > 0 {
        let Some((idx, ids)) = queue.last_mut() else {
            break;
        };
        if ids.len() <= room {
            room -= ids.len();
            let whole = (*idx, std::mem::take(ids));
            round.push(whole);
            queue.pop();
        } else {
            // Take the tail, leave the head queued.
            let keep = ids.len() - room;
            let part = (*idx, ids.split_off(keep));
            round.push(part);
            room = 0;
        }
    }
    round
}

#[cfg(test)]
mod tests {
    use wavedb_core::{Id, U48};

    use super::Pending;
    use crate::page_store::Touched;

    fn ids(slot: usize, keys: std::ops::Range<u64>) -> Touched {
        vec![(
            slot,
            keys.map(|k| Id::new(k, U48::from(1u32), true, 0)).collect(),
        )]
    }

    fn count(t: &Touched) -> usize {
        t.iter().map(|(_, ids)| ids.len()).sum()
    }

    /// The rotation is the line: what was queued before it is owed, what
    /// lands after it is fresh, and rounds work the owed half first.
    #[test]
    fn a_rotation_makes_the_queue_owed_and_rounds_take_it_first() {
        let mut p = Pending::default();
        p.note(ids(0, 0..10));
        assert!(!p.owes(), "nothing is owed before a checkpoint rotates");
        p.rotate();
        p.note(ids(0, 100..105));
        assert!(p.owes());

        let first = p.take(usize::MAX);
        assert_eq!(count(&first.ids), 10, "the owed half, and only it");
        assert!(p.owes(), "a round being written still counts");
        p.settled(&first);
        assert!(!p.owes(), "settled: the checkpoint may close");
        assert!(!p.is_empty(), "the fresh ids are still queued");
        assert_eq!(count(&p.take(usize::MAX).ids), 5);
    }

    #[test]
    fn a_budget_bounds_a_round_inside_one_slot() {
        let mut p = Pending::default();
        p.note(ids(0, 0..10));
        p.rotate();
        let round = p.take(4);
        assert_eq!(count(&round.ids), 4);
        p.settled(&round);
        assert!(p.owes(), "six still owed");
    }

    /// A failed round must go back to the half it came from, or a checkpoint
    /// could close over ids it never wrote.
    #[test]
    fn a_failed_owed_round_is_owed_again() {
        let mut p = Pending::default();
        p.note(ids(0, 0..3));
        p.rotate();
        let round = p.take(usize::MAX);
        p.requeue(round);
        assert!(p.owes());
        assert_eq!(count(&p.take(usize::MAX).ids), 3);
    }

    #[test]
    fn a_fresh_round_never_counts_against_the_checkpoint() {
        let mut p = Pending::default();
        p.note(ids(0, 0..3));
        let round = p.take(usize::MAX);
        assert!(!p.owes());
        p.requeue(round);
        assert!(!p.owes(), "requeued into fresh, not owed");
        assert!(!p.is_empty());
    }
}
