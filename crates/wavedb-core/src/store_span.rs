//! [`Span`] — the part of an ordered lane a [`Store::scan`] reads — and the
//! reading of one over a sorted map, which every in-memory store shares.
//!
//! [`Store::scan`]: crate::Store::scan

use std::collections::BTreeMap;
use std::ops::Bound;

/// One entry of an ordered lane: its key, and its value.
pub type Entry = (Vec<u8>, Vec<u8>);

/// What part of an ordered lane a [`Store::scan`](crate::Store::scan)
/// reads: the keys in `[lo, hi)`, ascending or — `rev` — descending, the
/// first `skip` of them passed over, at most `limit` returned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Span {
    /// The least key read.
    pub lo: Vec<u8>,
    /// The key the read stops before; `None` reads to the lane's end.
    pub hi: Option<Vec<u8>>,
    /// Greatest key first.
    pub rev: bool,
    /// Entries passed over, in the read's order, before any is returned.
    pub skip: u64,
    /// Entries returned at most.
    pub limit: usize,
}

impl Span {
    /// Every key starting with `prefix`, ascending, at most `limit`.
    #[must_use]
    pub fn prefix(prefix: &[u8], limit: usize) -> Self {
        Self {
            lo: prefix.to_vec(),
            hi: successor(prefix),
            rev: false,
            skip: 0,
            limit,
        }
    }

    /// The same read, greatest key first.
    #[must_use]
    pub const fn reversed(mut self) -> Self {
        self.rev = !self.rev;
        self
    }

    /// Whether `key` lies in `[lo, hi)`.
    #[must_use]
    pub fn contains(&self, key: &[u8]) -> bool {
        key >= self.lo.as_slice()
            && self.hi.as_deref().is_none_or(|hi| key < hi)
    }

    /// The range bounds of `[lo, hi)`.
    #[must_use]
    pub fn bounds(&self) -> (Bound<&[u8]>, Bound<&[u8]>) {
        (
            Bound::Included(self.lo.as_slice()),
            self.hi.as_deref().map_or(Bound::Unbounded, Bound::Excluded),
        )
    }
}

/// The least key above every key starting with `prefix`: the prefix with its
/// last byte below `0xFF` raised and the rest dropped. `None` when every
/// byte is `0xFF` (or there is none) — no key is above them all.
#[must_use]
pub fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let last = prefix.iter().rposition(|&b| b != 0xFF)?;
    let mut out = prefix[..=last].to_vec();
    out[last] += 1;
    Some(out)
}

/// `span`'s entries of `lane`, a sorted map.
#[must_use]
pub fn read_span(lane: &BTreeMap<Vec<u8>, Vec<u8>>, span: &Span) -> Vec<Entry> {
    if span
        .hi
        .as_deref()
        .is_some_and(|hi| hi <= span.lo.as_slice())
    {
        return Vec::new();
    }
    let range = lane.range::<[u8], _>(span.bounds());
    let skip = usize::try_from(span.skip).unwrap_or(usize::MAX);
    let own = |(k, v): (&Vec<u8>, &Vec<u8>)| (k.clone(), v.clone());
    if span.rev {
        range.rev().skip(skip).take(span.limit).map(own).collect()
    } else {
        range.skip(skip).take(span.limit).map(own).collect()
    }
}

/// How many entries `lane`, a sorted map, holds in `[lo, hi)`.
#[must_use]
pub fn read_count(
    lane: &BTreeMap<Vec<u8>, Vec<u8>>,
    lo: &[u8],
    hi: Option<&[u8]>,
) -> u64 {
    let span = Span {
        lo: lo.to_vec(),
        hi: hi.map(<[u8]>::to_vec),
        rev: false,
        skip: 0,
        limit: 0,
    };
    if span.hi.as_deref().is_some_and(|hi| hi <= lo) {
        return 0;
    }
    lane.range::<[u8], _>(span.bounds()).count() as u64
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Span, read_count, read_span, successor};

    fn lane() -> BTreeMap<Vec<u8>, Vec<u8>> {
        (1..=9u8).map(|k| (vec![7, k], vec![k])).collect()
    }

    fn keys(span: &Span) -> Vec<u8> {
        read_span(&lane(), span)
            .into_iter()
            .map(|(k, _)| k[1])
            .collect()
    }

    #[test]
    fn a_span_reads_its_range_in_either_order_skipping_and_limited() {
        let mut span = Span::prefix(&[7], 4);
        assert_eq!(keys(&span), [1, 2, 3, 4]);
        span.skip = 2;
        assert_eq!(keys(&span), [3, 4, 5, 6]);
        let back = span.reversed();
        assert_eq!(keys(&back), [7, 6, 5, 4]);
        let tight = Span {
            lo: vec![7, 3],
            hi: Some(vec![7, 5]),
            rev: false,
            skip: 0,
            limit: 10,
        };
        assert_eq!(keys(&tight), [3, 4]);
        assert_eq!(read_count(&lane(), &[7, 3], Some(&[7, 5])), 2);
        assert_eq!(read_count(&lane(), &[7], None), 9);
        assert_eq!(read_count(&lane(), &[8], Some(&[7])), 0, "an empty range");
    }

    #[test]
    fn a_successor_raises_the_last_byte_below_ff() {
        assert_eq!(successor(&[1, 2]), Some(vec![1, 3]));
        assert_eq!(successor(&[1, 0xFF]), Some(vec![2]));
        assert_eq!(successor(&[0xFF, 0xFF]), None);
        assert_eq!(successor(&[]), None);
    }
}
