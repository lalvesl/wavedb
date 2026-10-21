//! Decomposition: normalized codepoints in, posting keys out.
//!
//! A string of `L` codepoints is padded with `n-1` sentinels at each end and
//! cut into every window of `n`, yielding `L + n - 1` grams — the padding is
//! what makes the first and last characters carry as much weight as the
//! middle ones, so a typo in `"ada"`'s first letter still costs the same as
//! one in its second.
//!
//! ## Why the key is fixed-width codepoints, not UTF-8
//!
//! RFC 0056 justified this by prefix safety — that the UTF-8 encoding of `ab`
//! byte-prefixes that of `abc`, so a scan for one would sweep up the other.
//! **That reason does not survive contact with the invariant.** `n` is fixed
//! per index (it folds into the `STRUCT_HASH`), so every gram has the same
//! *codepoint* count, and UTF-8 is self-synchronizing: if one such gram's
//! bytes prefixed another's, decoding would give the same `n` codepoints and
//! they would be the same gram. There is no collision to prevent, and
//! `utf8_is_prefix_safe_too_which_is_not_why` pins that down.
//!
//! Three reasons that do hold, and they are enough:
//!
//! - **The sentinel cannot be encoded in UTF-8 at all.** Padding needs a
//!   symbol no user can type; `u32::MAX` is above the Unicode range, so it is
//!   free here and would need an in-band escape — and escaping rules — in any
//!   character encoding. This one is decisive on its own.
//! - **The length byte has a known home.** [`key_len`] reads the last byte,
//!   which is only sound because everything before it is exactly `4n`. With
//!   variable-width grams the boundary between gram and length moves with the
//!   gram's content.
//! - **The scan prefix is a constant.** `Bound::Prefix` takes `4n` bytes, not
//!   a length re-derived per query.
//!
//! Four bytes per codepoint costs 12 where 3–6 would do, and space is the
//! abundant resource.

use std::collections::BTreeSet;

/// The default gram width. Three is the usual trigram choice: wide enough
/// that a shared gram means something, narrow enough that a short name still
/// produces several.
pub const DEFAULT_N: usize = 3;

/// The padding codepoint — above the Unicode range, so it can never be a
/// character the user typed.
const SENTINEL: u32 = u32::MAX;

/// A record's (or query's) distinct grams, each as its `4n`-byte prefix.
///
/// A **set**, not a multiset, and that is forced rather than chosen: a
/// repeated gram in one record produces the very same tree key, so the index
/// can only ever answer set-wise. Every filter in [`super::Fuzzy::threshold`]
/// is stated against set semantics for that reason.
pub type GramSet = BTreeSet<Vec<u8>>;

/// Every distinct gram of `normalized`, as the key prefix a scan matches on.
///
/// An empty input still yields one gram — all sentinels — so a record with an
/// empty indexed field is findable by an empty query rather than invisible.
#[must_use]
pub fn gram_prefixes(normalized: &[char], n: usize) -> GramSet {
    let n = n.max(1);
    let mut padded = Vec::with_capacity(normalized.len() + 2 * (n - 1));
    padded.extend(std::iter::repeat_n(SENTINEL, n - 1));
    padded.extend(normalized.iter().map(|c| *c as u32));
    padded.extend(std::iter::repeat_n(SENTINEL, n - 1));

    let mut out = GramSet::new();
    for window in padded.windows(n) {
        let mut key = Vec::with_capacity(4 * n);
        for word in window {
            key.extend_from_slice(&word.to_be_bytes());
        }
        out.insert(key);
    }
    out
}

/// The stored key field for one gram of a record whose normalized value is
/// `len` codepoints long: the gram prefix, then the length.
///
/// The length rides **after** the gram and **before** the anchor so a prefix
/// scan reads it for free — it is already in the leaf bytes the descent landed
/// on — which is what makes the length filter cost no IO at all.
#[must_use]
pub fn field_key(prefix: &[u8], len: usize) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 1);
    key.extend_from_slice(prefix);
    key.push(saturating_len(len));
    key
}

/// The length a scanned posting key encodes, or `None` if the key is too
/// short to carry one (a corrupt node, which decode surfaces long before
/// this).
#[must_use]
pub fn key_len(field: &[u8]) -> Option<usize> {
    field.last().map(|len| *len as usize)
}

/// The stored length, saturated at 255.
///
/// Saturation is sound in **one** direction, and it is the one the length
/// filter needs. `sat` is monotone and 1-Lipschitz, so
/// `|sat(a) - sat(b)| ≤ |a - b|`: the computed gap never *exceeds* the true
/// gap, so a rejection is never a false negative. Two strings of 300 and 400
/// characters look identical in length here — they are both simply "long" —
/// and the verify step sorts them out.
fn saturating_len(len: usize) -> u8 {
    u8::try_from(len).unwrap_or(u8::MAX)
}

/// The length gap between a query of `query_len` codepoints and a posting
/// claiming `stored`, both saturated so the comparison is the sound one.
#[must_use]
pub fn length_gap(query_len: usize, stored: usize) -> usize {
    let q = saturating_len(query_len) as usize;
    let s = stored.min(u8::MAX as usize);
    q.abs_diff(s)
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_N, field_key, gram_prefixes, key_len, length_gap};
    use crate::fuzzy::fold::{Fold, normalize};

    fn grams(s: &str) -> Vec<Vec<u8>> {
        gram_prefixes(&normalize(s, Fold::Latin), DEFAULT_N)
            .into_iter()
            .collect()
    }

    #[test]
    fn a_string_of_l_codepoints_yields_l_plus_n_minus_one_grams() {
        // "ada" → 3 + 3 - 1 = 5 windows, all distinct here.
        assert_eq!(grams("ada").len(), 5);
        assert_eq!(grams("lovelace").len(), 8 + DEFAULT_N - 1);
    }

    #[test]
    fn repeats_collapse_because_the_index_can_only_answer_set_wise() {
        // "aaaa" has 6 gram *occurrences* but only 4 distinct keys: the
        // window `aaa` appears twice and is one tree key either way. Every
        // filter is stated against this count, not the occurrence count.
        assert_eq!(grams("aaaa").len(), 5);
        let occurrences = "aaaa".chars().count() + DEFAULT_N - 1;
        assert!(
            grams("aaaa").len() < occurrences,
            "the set must be smaller than the multiset for a repeat"
        );
    }

    #[test]
    fn an_empty_value_is_still_indexed() {
        // One all-sentinel gram: an empty field is findable, not invisible.
        assert_eq!(grams("").len(), 1);
    }

    #[test]
    fn padding_gives_the_edges_their_weight() {
        // The first character participates in `n` grams, exactly like an
        // interior one — which is the whole reason to pad.
        let g = grams("ab");
        assert_eq!(g.len(), 4);
        // Without padding "ab" would produce no trigram at all.
        assert!(!g.is_empty(), "{g:?}");
    }

    #[test]
    fn every_gram_of_one_index_is_the_same_width() {
        // The invariant a `Bound::Prefix` scan rests on. `n` is fixed per
        // index (it folds into the STRUCT_HASH), so every key in one tree is
        // `4n` bytes and **no key can be a prefix of another** — matching a
        // gram is therefore matching exactly that gram.
        for (n, text) in [(2, "ab"), (3, "abc"), (4, "lovelace")] {
            let keys = gram_prefixes(&normalize(text, Fold::Latin), n);
            assert!(keys.iter().all(|k| k.len() == 4 * n));
            for a in &keys {
                for b in &keys {
                    assert!(
                        a == b || !b.starts_with(a),
                        "n={n}: one gram key prefixed another"
                    );
                }
            }
        }
    }

    #[test]
    fn utf8_is_prefix_safe_too_which_is_not_why() {
        // RFC 0056 justified fixed-width by prefix safety. It is the wrong
        // reason, and this is the check that says so: across a mixed-width
        // alphabet, no UTF-8 3-gram byte-prefixes a different 3-gram, because
        // UTF-8 is self-synchronizing and the codepoint count is fixed.
        //
        // The real reasons are the sentinel (u32::MAX is unencodable in
        // UTF-8), the length byte's fixed home, and a constant scan prefix.
        let alphabet = ['a', 'é', '東', '\u{10000}'];
        for a in alphabet {
            for b in alphabet {
                for c in alphabet {
                    let g1: String = [a, b, c].iter().collect();
                    for d in alphabet {
                        let g2: String = [b, c, d].iter().collect();
                        assert!(
                            g1 == g2
                                || !g2.as_bytes().starts_with(g1.as_bytes()),
                            "{g1:?} byte-prefixed {g2:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn the_sentinel_is_not_a_character_anyone_can_type() {
        // The decisive reason for u32 words: padding needs a symbol outside
        // the alphabet, and `u32::MAX` is above the Unicode range — so no
        // input can ever produce it and no escape rule is needed.
        assert!(char::from_u32(super::SENTINEL).is_none());
        assert!(super::SENTINEL > char::MAX as u32);
    }

    #[test]
    fn the_length_rides_in_the_key_and_saturates_downward() {
        let prefix = grams("ada").into_iter().next().unwrap();
        assert_eq!(key_len(&field_key(&prefix, 3)), Some(3));
        assert_eq!(key_len(&field_key(&prefix, 999)), Some(255));
        // Saturation compresses the high end, so the computed gap is never
        // larger than the true one — a rejection is never a false negative.
        assert_eq!(length_gap(10, 3), 7);
        assert!(length_gap(300, 400) <= 100);
        assert_eq!(length_gap(300, 400), 0);
    }
}
