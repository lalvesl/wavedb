//! Fuzzy string search ([RFC 0056]) — the text layer.
//!
//! An n-gram posting index over a `Chain<()>` (RFC 0075): the indexed
//! string is normalized, padded, cut into n-grams, and each distinct gram
//! becomes a key `[gram][len][anchor]`. A query decomposes the same way, so
//! looking up one gram is a `Bound::Prefix` scan.
//!
//! This module is everything that happens **before** the tree and **after**
//! it: normalization ([`fold`]), decomposition ([`grams`]), and the
//! filter/verify arithmetic ([`distance`]). It touches no `Store` and is
//! therefore the half that can be property-tested against brute force — which
//! RFC 0056 names as load-bearing, because a filter that is wrong by one
//! silently loses matches and nothing downstream would notice.
//!
//! [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md

pub mod distance;
pub mod fold;
pub mod grams;

pub use distance::{Scored, containment, jaccard, levenshtein_within};

/// A fuzzy search's answer: each hit's id and record, best first.
pub type Hits<T> = Vec<Scored<(crate::id::Id, crate::wire::Wire<T>)>>;
pub use fold::{Fold, normalize};
pub use grams::{DEFAULT_N, GramSet, field_key, gram_prefixes, key_len};

/// What a fuzzy lookup is asking for.
///
/// Both modes reduce to the same prefilter — "share at least `threshold`
/// distinct grams with the query" — and differ only in how that threshold is
/// derived and how a survivor is scored. That is why one posting index serves
/// both.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Fuzzy {
    /// Exact edit distance: every record within `k` edits of the query.
    Distance(usize),
    /// Trigram Jaccard similarity: every record scoring at least `t` in
    /// `|G(q) ∩ G(s)| / |G(q) ∪ G(s)|`.
    ///
    /// **Symmetric** — it asks "are these two strings alike?", so a short
    /// query against a long title scores low however well it matches: the
    /// union carries the title's whole length. Right for de-duplication and
    /// "did someone already add this?", wrong for type-ahead. That is what
    /// [`Contains`](Self::Contains) is for.
    Similarity(f64),
    /// Trigram **containment**: every record scoring at least `t` in
    /// `|G(q) ∩ G(s)| / |G(q)|` — how much of what the caller typed appears
    /// in the record.
    ///
    /// **Asymmetric**, and that is the point: a perfect substring scores
    /// `1.0` whatever the record's length, so typing `milk` finds
    /// `Buy milk before the shop closes`. This is the "search as you type"
    /// mode. RFC 0056 assigned that role to Jaccard, which does not survive
    /// the arithmetic — see the note on this enum's `threshold`.
    Contains(f64),
}

impl Fuzzy {
    /// Every record within `k` edits.
    #[must_use]
    pub const fn distance(k: usize) -> Self {
        Self::Distance(k)
    }

    /// Every record scoring at least `t` (0.0…1.0) in trigram Jaccard.
    #[must_use]
    pub const fn similarity(t: f64) -> Self {
        Self::Similarity(t)
    }

    /// Every record containing at least a fraction `t` of the query's grams
    /// — the type-ahead mode.
    #[must_use]
    pub const fn contains(t: f64) -> Self {
        Self::Contains(t)
    }

    /// How many of the query's `total` distinct grams a candidate must share
    /// to survive the prefilter.
    ///
    /// **Both forms admit false positives and never false negatives**, which
    /// is what makes the result exact once the survivors are verified:
    ///
    /// - `Distance(k)`: at most `n` gram *occurrences* are disrupted per edit,
    ///   and a gram value leaves the query's set only if every one of its
    ///   occurrences was disrupted — so at most `n*k` distinct grams can go
    ///   missing, and a match shares at least `total - n*k`.
    /// - `Similarity(t)`: the union is at least `|G(q)| = total`, so the true
    ///   Jaccard is at most `shared / total`. Anything below `t * total`
    ///   cannot reach `t`.
    /// - `Contains(t)`: the score **is** `shared / total`, so this threshold
    ///   is not a bound at all — it is the answer. The prefilter and the
    ///   verdict coincide, and the record is read only to return it.
    ///
    /// A `k` large enough to drive the threshold to zero degenerates to a
    /// scan, correctly: at that distance every record really is a candidate.
    ///
    /// ## Why `Contains` exists
    ///
    /// RFC 0056 called Jaccard the "search as you type" mode. The arithmetic
    /// says otherwise: Jaccard divides by the **union**, so a 4-character
    /// query against a 20-character title cannot exceed roughly `6/22 ≈ 0.27`
    /// even when it matches perfectly as a substring. Any threshold loose
    /// enough to accept that is loose enough to accept noise. Containment
    /// divides by the query alone, so a perfect substring is `1.0` whatever
    /// the title's length — which is what a type-ahead actually means.
    #[must_use]
    pub fn threshold(self, total: usize, n: usize) -> usize {
        match self {
            Self::Distance(k) => total.saturating_sub(n * k),
            // `ceil`, so a candidate exactly on the boundary survives to be
            // verified rather than being rejected by a rounding step.
            Self::Similarity(t) | Self::Contains(t) => {
                #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
                #[allow(clippy::cast_possible_truncation)]
                let want = (t.clamp(0.0, 1.0) * total as f64).ceil() as usize;
                want.max(1).min(total)
            }
        }
    }

    /// The most a length difference of `gap` codepoints can be tolerated.
    ///
    /// `ed(a,b) ≥ | |a| - |b| |` is free rejection: the length is already in
    /// the key bytes the prefix scan landed on. For similarity there is no
    /// sound length bound (two very different lengths can still share grams
    /// proportionally), so it does not reject.
    #[must_use]
    pub const fn allows_length_gap(self, gap: usize) -> bool {
        match self {
            Self::Distance(k) => gap <= k,
            // Neither ratio mode has a sound length bound — containment least
            // of all, since finding a short query inside a long record is
            // exactly what it is for.
            Self::Similarity(_) | Self::Contains(_) => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Fuzzy;

    #[test]
    fn the_distance_threshold_never_exceeds_the_query_size() {
        // A generous k drives the filter to "everything is a candidate",
        // which is correct rather than degenerate: at that distance it is.
        assert_eq!(Fuzzy::distance(0).threshold(10, 3), 10);
        assert_eq!(Fuzzy::distance(1).threshold(10, 3), 7);
        assert_eq!(Fuzzy::distance(3).threshold(10, 3), 1);
        assert_eq!(Fuzzy::distance(9).threshold(10, 3), 0);
    }

    #[test]
    fn the_similarity_threshold_rounds_up_and_stays_in_range() {
        assert_eq!(Fuzzy::similarity(0.0).threshold(10, 3), 1);
        assert_eq!(Fuzzy::similarity(0.35).threshold(10, 3), 4);
        assert_eq!(Fuzzy::similarity(1.0).threshold(10, 3), 10);
        // Out-of-range inputs clamp rather than producing a nonsense count.
        assert_eq!(Fuzzy::similarity(2.0).threshold(10, 3), 10);
        assert_eq!(Fuzzy::similarity(-1.0).threshold(10, 3), 1);
    }

    #[test]
    fn only_distance_rejects_on_length() {
        assert!(Fuzzy::distance(2).allows_length_gap(2));
        assert!(!Fuzzy::distance(2).allows_length_gap(3));
        assert!(Fuzzy::similarity(0.9).allows_length_gap(100));
    }
}
