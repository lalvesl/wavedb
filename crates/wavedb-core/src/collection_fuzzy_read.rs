//! The read half of `#[wavedb::fuzzy]` ([RFC 0056]): prefix scans, two
//! filters, then verify.
//!
//! ```text
//! 1. decompose the query into its distinct grams
//! 2. one prefix scan per gram            → per-anchor share counts
//! 3. the COUNT filter                   → reject without reading the record
//! 4. the LENGTH filter                  → free; the length is in the key
//! 5. verify                             → one random read each, the real cost
//! ```
//!
//! Steps 3 and 4 admit false positives and **never** false negatives — that
//! asymmetry is what makes step 5 turn an approximate candidate set into an
//! **exact** answer. Their arithmetic, and the property tests pinning it, live
//! in [`crate::fuzzy`].
//!
//! Step 5 is the dominant cost: one anchor read per survivor. Everything above
//! it exists to make that set small, which is also why the result is
//! **buffered and ranked** rather than streamed — a best-first order is not
//! known until the last candidate is scored, and pretending otherwise would be
//! the kind of lie `All` refuses to tell about its own buffering.
//!
//! [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md

use std::collections::HashMap;

use futures::TryStreamExt;

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::fuzzy::{
    Fuzzy, Scored, containment, gram_prefixes, jaccard, key_len,
    levenshtein_within, normalize,
};
use crate::id::Id;
use crate::index::SecKey;
use crate::local_id::LocalId;
use crate::record::decode_record;
use crate::store::Store;
use crate::traits::NonUniqueStruct;

/// What the scans learned about one candidate before it was read.
struct Candidate {
    /// How many of the query's distinct grams it shares.
    shared: usize,
    /// Its indexed value's normalized length, straight off the key bytes.
    len: usize,
}

impl<T: NonUniqueStruct> Collection<T> {
    /// Records whose fuzzy index `index` matches `query` under `mode`, best
    /// first, at most `limit` of them.
    ///
    /// # Errors
    /// [`Error::FuzzyOutOfRange`] for an undeclared index, or a [`Store`]
    /// failure / decode fault while scanning.
    pub async fn fuzzy_search<S: Store>(
        &self,
        store: &S,
        index: usize,
        query: &str,
        mode: Fuzzy,
        limit: usize,
    ) -> Result<Vec<Scored<(Id, T)>>> {
        let pivot = self.load_pivot(store).await?;
        let chain = self
            .fuzzy_chains(&pivot)
            .into_iter()
            .nth(index)
            .ok_or(Error::FuzzyOutOfRange(index))?;

        let (n, fold) = T::fuzzy_profile(index);
        let text = normalize(query, fold);
        let grams = gram_prefixes(&text, n);
        let threshold = mode.threshold(grams.len(), n);

        // Steps 1–2: one descent per gram, then the chain while the gram
        // holds (RFC 0075), merged into a share count.
        let mut seen: HashMap<LocalId, Candidate> = HashMap::new();
        for prefix in &grams {
            let from = SecKey {
                field: prefix.clone(),
                rec: LocalId::default(),
            };
            let keys = chain
                .scan_from(store, from)
                .try_take_while(|(key, ())| {
                    futures::future::ready(Ok(key.field.starts_with(prefix)))
                })
                .try_collect::<Vec<_>>()
                .await?;
            for (key, ()) in keys {
                // The length rides in the same bytes the descent already
                // landed on, so reading it costs nothing.
                let len = key_len(&key.field).unwrap_or(0);
                seen.entry(key.rec)
                    .and_modify(|c| c.shared += 1)
                    .or_insert(Candidate { shared: 1, len });
            }
        }

        // Steps 3–4: reject on arithmetic alone, before touching a record.
        let survivors = seen.into_iter().filter(|(_, c)| {
            c.shared >= threshold
                && mode.allows_length_gap(crate::fuzzy::grams::length_gap(
                    text.len(),
                    c.len,
                ))
        });

        // Step 5: the exact answer, one anchor read each.
        let mut hits = Vec::new();
        for (rec, _) in survivors {
            let id = rec.to_id(self.tenant());
            let Some(bytes) = store.get_of(T::STRUCT_HASH, id).await? else {
                // A posting whose anchor is gone: the index is maintained in
                // the record's own batch, so this means a torn write, not an
                // ordinary race. Skip rather than fail the whole query — a
                // search is a best-effort view, and the batch rule is what
                // makes it not happen.
                continue;
            };
            let (_, value) = decode_record::<T>(T::STRUCT_HASH, &bytes)?;
            if let Some(scored) = Self::score(&value, index, &text, mode) {
                hits.push(Scored {
                    item: (id, value),
                    score: scored.score,
                    rank: scored.rank,
                });
            }
        }

        // Ranked, therefore buffered. `rank` is ascending for both modes, so
        // the sort has no mode to branch on; ties break on the anchor, which
        // never changes, so repeating a query repeats its order.
        hits.sort_by(|a, b| {
            a.rank
                .partial_cmp(&b.rank)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.item.0.cmp(&b.item.0))
        });
        hits.truncate(limit);
        Ok(hits)
    }

    /// Verify one candidate exactly, or reject it — step 5's per-record half.
    fn score(
        value: &T,
        index: usize,
        query: &[char],
        mode: Fuzzy,
    ) -> Option<Scored<()>> {
        let (n, fold) = T::fuzzy_profile(index);
        let text = normalize(value.fuzzy_source(index), fold);
        match mode {
            Fuzzy::Distance(k) => levenshtein_within(query, &text, k)
                .map(|d| Scored::at_distance((), d)),
            Fuzzy::Similarity(t) => {
                let score =
                    jaccard(&gram_prefixes(query, n), &gram_prefixes(&text, n));
                (score >= t).then(|| Scored::at_similarity((), score))
            }
            Fuzzy::Contains(t) => {
                let score = containment(
                    &gram_prefixes(query, n),
                    &gram_prefixes(&text, n),
                );
                (score >= t).then(|| Scored::at_similarity((), score))
            }
        }
    }
}
