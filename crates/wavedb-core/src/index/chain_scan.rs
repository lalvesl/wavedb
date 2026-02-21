//! A chain read in key order from a seek ([RFC 0075]): one descent to the
//! segment covering a key, then the `next` links.
//!
//! This is what makes a chain an index and not only a log: a secondary or a
//! fuzzy posting chain answers "every key from here" with one sparse-index
//! descent and one read per segment it passes, and a range read stops at
//! the first key past its bound — it reads what it returns, and one segment
//! more at most.
//!
//! [RFC 0075]: https://github.com/wavedb/wavedb/blob/main/rfcs/0075-secondary-indexes-as-chains.md

use std::collections::VecDeque;

use futures::Stream;

use crate::error::Result;
use crate::local_id::LocalId;
use crate::store::Store;
use crate::wire::WaveWire;

use super::chain::Chain;
use super::sec_key::SecKey;
use super::segment::Segment;

/// Where a scan stands: the seek still to make, the next segment to read,
/// and the entries of the last one read not yet yielded.
struct Scan<P> {
    seek: Option<SecKey>,
    next: Option<LocalId>,
    ready: VecDeque<(SecKey, P)>,
}

impl<P: WaveWire + Clone> Chain<P> {
    /// Every entry with a key `≥ from`, ascending — the chain's own order,
    /// so a caller after a bounded range stops taking at its first key past
    /// it, and the scan reads no further.
    ///
    /// A key below every entry starts at the head; one past every entry
    /// yields nothing.
    ///
    /// # Errors
    /// Each item may be a [`Store`] failure, or a dangling or foreign
    /// segment pointer, as [`get`](Self::get); the stream ends after one.
    pub fn scan_from<'a, S: Store>(
        self,
        store: &'a S,
        from: SecKey,
    ) -> impl Stream<Item = Result<(SecKey, P)>> + 'a
    where
        P: 'a,
    {
        let start = Scan {
            seek: Some(from),
            next: None,
            ready: VecDeque::new(),
        };
        futures::stream::unfold(start, move |mut st| async move {
            loop {
                if let Some(entry) = st.ready.pop_front() {
                    return Some((Ok(entry), st));
                }
                let read = if let Some(from) = st.seek.take() {
                    self.locate(store, &from)
                        .await
                        .map(|(_, seg)| (seg, Some(from)))
                } else {
                    // Nothing left to follow: the stream ends here.
                    let id = st.next.take()?;
                    self.load(store, id).await.map(|seg| (seg, None))
                };
                match read {
                    Ok((seg, floor)) => st.take(&seg, floor.as_ref()),
                    // Neither a seek nor a link is left: it ends after this.
                    Err(e) => return Some((Err(e), st)),
                }
            }
        })
    }
}

impl<P: Clone> Scan<P> {
    /// Queue `seg`'s entries at or above `floor` and follow its link.
    fn take(&mut self, seg: &Segment<P>, floor: Option<&SecKey>) {
        self.next = seg.next();
        self.ready.extend(
            seg.entries()
                .filter(|(key, _)| floor.is_none_or(|f| key >= f))
                .cloned(),
        );
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::{StreamExt, TryStreamExt};

    use crate::index::Chain;
    use crate::index::mem_store::MemStore;
    use crate::index::sec_key::SecKey;
    use crate::index::segment::Lane;
    use crate::local_id::LocalId;
    use crate::store::Store;
    use crate::u48::U48;

    const TYPE_HASH: u64 = 0x0075_5CA1_0000_0001;

    fn tenant() -> U48 {
        U48::new(5).unwrap()
    }

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

    /// A chain of the even numbers below `2n`, small segments (4…8) and a
    /// narrow index, so a scan crosses many of both.
    fn evens(store: &MemStore, n: u64) -> Chain<()> {
        block_on(async {
            let (chain, writes) =
                Chain::<()>::plan_create(tenant(), TYPE_HASH, Lane::Recency);
            store.apply(&writes).await.unwrap();
            let mut chain = chain.with_min(4).with_index_cap(3);
            for v in 0..n {
                let writes =
                    chain.plan_insert(store, key(v * 2), ()).await.unwrap();
                store.apply(&writes).await.unwrap();
            }
            chain
        })
    }

    fn scan(store: &MemStore, chain: Chain<()>, from: u64) -> Vec<u64> {
        block_on(
            chain
                .scan_from(store, floor(from))
                .map_ok(|(k, ())| {
                    u64::from_be_bytes(k.field.as_slice().try_into().unwrap())
                })
                .try_collect(),
        )
        .unwrap()
    }

    /// From anywhere, the rest of the chain in order, across every segment
    /// boundary after the seek.
    #[test]
    fn a_scan_from_a_key_yields_every_key_above_it() {
        let store = MemStore::default();
        let chain = evens(&store, 100);
        assert_eq!(
            scan(&store, chain, 0),
            (0..100).map(|v| v * 2).collect::<Vec<_>>()
        );
        assert_eq!(
            scan(&store, chain, 101),
            (51..100).map(|v| v * 2).collect::<Vec<_>>()
        );
        assert_eq!(scan(&store, chain, 198), [198]);
    }

    /// A bound inside one segment reads it and stops: taking ends at the
    /// first key past the bound.
    #[test]
    fn a_bounded_range_stops_at_its_end() {
        let store = MemStore::default();
        let chain = evens(&store, 100);
        let got: Vec<u64> = block_on(
            chain
                .scan_from(&store, floor(40))
                .map_ok(|(k, ())| {
                    u64::from_be_bytes(k.field.as_slice().try_into().unwrap())
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
