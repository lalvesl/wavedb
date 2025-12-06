//! A chain read as a stream ([RFC 0078] §7): from a key, from either end,
//! or past a number of entries — one [`Store::scan`] of the chain's range
//! per [`BATCH`] entries, the next one starting where the last one ended.
//!
//! A caller after a bounded range stops taking at its first key past it,
//! and the stream asks for nothing more.
//!
//! [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use std::collections::VecDeque;

use futures::Stream;

use crate::error::Result;
use crate::store::{Span, Store};
use crate::wire::{WaveWire, from_wire};

use super::chain::Chain;
use super::sec_key::SecKey;

/// Entries one read of the store asks for.
pub const BATCH: usize = 256;

/// Where a stream stands: the span still to read (`None` once the range is
/// done), and the entries of the last read not yet yielded.
struct Cursor<P> {
    span: Option<Span>,
    ready: VecDeque<(SecKey, P)>,
}

impl<P: WaveWire + Clone> Chain<P> {
    /// Every entry with a key `≥ from`, ascending.
    ///
    /// # Errors
    /// Each item may be a [`Store`] failure, or an entry that does not read
    /// back; the stream ends after one.
    pub fn scan_from<'a, S: Store>(
        self,
        store: &'a S,
        from: &SecKey,
    ) -> impl Stream<Item = Result<(SecKey, P)>> + use<'a, S, P>
    where
        P: 'a,
    {
        let span = Span {
            lo: self.entry_key(from),
            ..self.span(BATCH)
        };
        self.stream(store, span)
    }

    /// Every entry past the first `skip`, ascending — a page of a list.
    ///
    /// # Errors
    /// As [`scan_from`](Self::scan_from).
    pub fn scan_skip<'a, S: Store>(
        self,
        store: &'a S,
        skip: u64,
    ) -> impl Stream<Item = Result<(SecKey, P)>> + 'a
    where
        P: 'a,
    {
        let span = Span {
            skip,
            ..self.span(BATCH)
        };
        self.stream(store, span)
    }

    /// Every entry, descending — newest first in a chain keyed by instant.
    ///
    /// # Errors
    /// As [`scan_from`](Self::scan_from).
    pub fn scan_back<'a, S: Store>(
        self,
        store: &'a S,
    ) -> impl Stream<Item = Result<(SecKey, P)>> + 'a
    where
        P: 'a,
    {
        self.stream(store, self.span(BATCH).reversed())
    }

    /// The entries `span` reads, then the rest of the range past them, read
    /// [`BATCH`] at a time.
    fn stream<'a, S: Store>(
        self,
        store: &'a S,
        span: Span,
    ) -> impl Stream<Item = Result<(SecKey, P)>> + 'a
    where
        P: 'a,
    {
        let start = Cursor {
            span: Some(span),
            ready: VecDeque::new(),
        };
        futures::stream::unfold(start, move |mut at| async move {
            loop {
                if let Some(entry) = at.ready.pop_front() {
                    return Some((Ok(entry), at));
                }
                let span = at.span.take()?;
                match self.read(store, span).await {
                    Ok((entries, next)) => {
                        at.ready.extend(entries);
                        at.span = next;
                    }
                    Err(e) => return Some((Err(e), at)),
                }
            }
        })
    }

    /// One read of `span`: its entries, and the span reading on past them —
    /// `None` when this read reached the range's end.
    async fn read<S: Store>(
        &self,
        store: &S,
        span: Span,
    ) -> Result<(Vec<(SecKey, P)>, Option<Span>)> {
        let found = store.scan(self.lane(), &span).await?;
        let next = found.last().filter(|_| found.len() == span.limit).map(
            |(last, _)| {
                let mut next = Span {
                    skip: 0,
                    ..span.clone()
                };
                if span.rev {
                    next.hi = Some(last.clone());
                } else {
                    // The least key above `last`.
                    let mut above = last.clone();
                    above.push(0);
                    next.lo = above;
                }
                next
            },
        );
        let mut entries = Vec::with_capacity(found.len());
        for (key, bytes) in found {
            entries.push((self.sec_key(&key)?, from_wire(&bytes)?));
        }
        Ok((entries, next))
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::{StreamExt, TryStreamExt};

    use super::BATCH;
    use crate::id::Id;
    use crate::index::Chain;
    use crate::index::lane::Lane;
    use crate::index::mem_store::MemStore;
    use crate::index::sec_key::SecKey;
    use crate::local_id::LocalId;
    use crate::store::Store;
    use crate::u48::U48;

    const TYPE_HASH: u64 = 0x0075_5CA1_0000_0001;

    fn key(value: u64) -> SecKey {
        SecKey {
            field: value.to_be_bytes().to_vec(),
            rec: LocalId::new(value, false, 3),
        }
    }

    /// The least key of field `value`: what a bound's seek starts from.
    fn floor(value: u64) -> SecKey {
        SecKey {
            field: value.to_be_bytes().to_vec(),
            rec: LocalId::default(),
        }
    }

    /// A chain of the even numbers below `2n` — more than one read's worth —
    /// beside another collection's chain in the same lane.
    fn evens(store: &MemStore, n: u64) -> Chain<()> {
        let owner = |k| Id::new(k, U48::new(5).unwrap(), false, 1);
        let chain = Chain::<()>::at(owner(1), TYPE_HASH, Lane::Recency, 0);
        let other = Chain::<()>::at(owner(2), TYPE_HASH, Lane::Recency, 0);
        let mut writes = Vec::new();
        for v in 0..n {
            writes.push(chain.insert(&key(v * 2), &()));
            writes.push(other.insert(&key(v * 2 + 1), &()));
        }
        block_on(store.apply(&writes)).unwrap();
        chain
    }

    fn values(keys: Vec<(SecKey, ())>) -> Vec<u64> {
        keys.into_iter()
            .map(|(k, ())| u64::from_be_bytes(k.field.try_into().unwrap()))
            .collect()
    }

    fn scan(store: &MemStore, chain: Chain<()>, from: u64) -> Vec<u64> {
        values(
            block_on(chain.scan_from(store, &floor(from)).try_collect())
                .unwrap(),
        )
    }

    /// From anywhere, the rest of the chain in order, across reads.
    #[test]
    fn a_scan_from_a_key_yields_every_key_above_it() {
        let store = MemStore::default();
        let n = BATCH as u64 * 2 + 10;
        let chain = evens(&store, n);
        assert_eq!(
            scan(&store, chain, 0),
            (0..n).map(|v| v * 2).collect::<Vec<_>>()
        );
        assert_eq!(
            scan(&store, chain, 101),
            (51..n).map(|v| v * 2).collect::<Vec<_>>()
        );
        assert_eq!(scan(&store, chain, (n - 1) * 2), [(n - 1) * 2]);
        assert_eq!(scan(&store, chain, n * 2), [0; 0]);
    }

    /// A bounded take stops at its end.
    #[test]
    fn a_bounded_range_stops_at_its_end() {
        let store = MemStore::default();
        let chain = evens(&store, 100);
        let got: Vec<(SecKey, ())> = block_on(
            chain
                .scan_from(&store, &floor(40))
                .try_take_while(|(k, ())| {
                    futures::future::ready(Ok(
                        k.field < 47u64.to_be_bytes().to_vec()
                    ))
                })
                .try_take_while(|&v| futures::future::ready(Ok(v < 47)))
                .try_collect(),
        )
        .unwrap();
        assert_eq!(got, [40, 42, 44, 46]);
    }

    /// Below the head is the whole chain; past the tail is nothing.
    #[test]
    fn seeks_off_either_end() {
        let store = MemStore::default();
        let chain = evens(&store, 30);
        assert_eq!(scan(&store, chain, 0).len(), 30);
        assert_eq!(scan(&store, chain, 61), [0; 0]);
        assert_eq!(scan(&store, chain, u64::MAX), [0; 0]);
    }

    /// An empty chain — its one shell segment — yields nothing.
    #[test]
    fn an_empty_chain_yields_nothing() {
        let store = MemStore::default();
        let chain = evens(&store, 0);
        assert_eq!(scan(&store, chain, 0), [0; 0]);
    }

    /// A seek landing exactly on a segment's first key — the key the index
    /// files that segment under — starts at it, not after it.
    #[test]
    fn a_seek_on_a_separator_starts_at_it() {
        let store = MemStore::default();
        let chain = evens(&store, 100);
        let mut separators = Vec::new();
        let mut cursor = Some(chain.head());
        while let Some(id) = cursor {
            let seg = block_on(chain.segment(&store, id)).unwrap();
            if let Some(first) = seg.first_key() {
                separators.push(first.clone());
            }
            cursor = seg.next();
        }
        assert!(separators.len() > 10, "{} segments", separators.len());
        for sep in &separators[1..] {
            let scan = chain.scan_from(&store, sep.clone());
            let first = block_on(Box::pin(scan).next()).unwrap().unwrap();
            assert_eq!(&first.0, sep);
        }
    }
}
