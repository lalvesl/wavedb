//! Runs named more than once ([RFC 0081] §5): a pack's, by every entry of
//! its sections (`crate::pack`).
//!
//! **The count lives in RAM and is never written.** Every open rebuilds the
//! indexes, and a run's count is the number of entries naming it, so the
//! open counts them ([`Shares::of`]) and the rounds keep them: a round that
//! places a pack names it once per section ([`Shares::share`]), and a run
//! an entry stops naming is freed only once none does ([`Shares::release`]).
//! A run named once — nearly every one — carries no counter.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

use std::collections::BTreeMap;

use crate::block::Run;

/// The runs named more than once, by first block: the names past the first.
#[derive(Debug, Default)]
pub struct Shares {
    extra: BTreeMap<u64, u32>,
}

impl Shares {
    /// The counts of `runs`, every entry's run once per entry naming it.
    #[must_use]
    pub fn of(runs: impl IntoIterator<Item = Run>) -> Self {
        let mut extra = BTreeMap::new();
        let mut seen = std::collections::BTreeSet::new();
        for run in runs {
            if !seen.insert(run.start) {
                *extra.entry(run.start).or_insert(0) += 1;
            }
        }
        Self { extra }
    }

    /// The run at `start` was just named `names` times.
    pub fn share(&mut self, start: u64, names: u32) {
        if let Some(more) = names.checked_sub(1).filter(|&n| n > 0) {
            *self.extra.entry(start).or_insert(0) += more;
        }
    }

    /// An entry stopped naming the run at `start`: whether none does now.
    pub fn release(&mut self, start: u64) -> bool {
        match self.extra.get_mut(&start) {
            Some(1) => {
                self.extra.remove(&start);
                false
            }
            Some(more) => {
                *more -= 1;
                false
            }
            None => true,
        }
    }

    /// How many entries name the run at `start`, as counted.
    #[cfg(test)]
    #[must_use]
    pub fn names(&self, start: u64) -> u32 {
        1 + self.extra.get(&start).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::Shares;
    use crate::block::Run;

    #[test]
    fn a_run_goes_with_its_last_name() {
        let mut s = Shares::default();
        s.share(10, 3);
        assert_eq!(s.names(10), 3);
        assert!(!s.release(10));
        assert!(!s.release(10));
        assert!(s.release(10), "the last name");
        assert!(s.extra.is_empty());
        assert!(s.release(20), "a run named once");
    }

    #[test]
    fn an_open_counts_what_the_indexes_name() {
        let runs = [
            Run::new(10, 1),
            Run::new(5, 2),
            Run::new(10, 1),
            Run::new(10, 1),
        ];
        let s = Shares::of(runs);
        assert_eq!((s.names(10), s.names(5), s.extra.len()), (3, 1, 1));
    }
}
