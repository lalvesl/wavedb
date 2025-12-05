//! The chain ([RFC 0078] §7) — a collection's sorted entries, as a key
//! range of one ordered lane: `[owner][declaration]` then each entry's key.
//!
//! A chain is not a structure of its own. Its lane is ordered by the store
//! (the page index of RFC 0078 natively, a key range in IndexedDB), every
//! collection of the type shares the lane, and the chain is the range under
//! its owner — the collection's Pivot — and its declaration. So there is
//! nothing to create, nothing to link and no root to move:
//!
//! - an **insert** is one [`Write::Set`], blind — no read, so two inserts
//!   planned from the same state cannot clobber each other;
//! - a **removal** is one [`Write::Unset`]; [`plan_remove`](Chain::plan_remove)
//!   reads first, for the callers whose verdict is whether the key was there;
//! - a **read** is a [`Store::scan`] of the range, from a key or from either
//!   end, skipping as far as asked ([`chain_scan`](super::chain_scan)).
//!
//! An entry's key is `prefix ‖ field ‖ anchor`: the [`SecKey`]'s
//! order-preserving field bytes, then its record's `LocalId` as 10 bytes
//! (`KEY` big-endian, then `FLAG | SALT`). The anchor is a fixed width at the
//! end, which is what lets a key read back into its `SecKey`.
//!
//! [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

use core::marker::PhantomData;

use crate::error::{Error, Result};
use crate::id::Id;
use crate::local_id::LocalId;
use crate::store::{Span, Store, Write, successor};
use crate::wire::{WaveWire, from_wire, to_wire};

use super::IndexKey;
use super::lane::Lane;
use super::sec_key::SecKey;

/// Bytes before an entry's own key: the owner's `Id`, then the declaration.
pub const PREFIX: usize = 17;

/// Bytes an entry's anchor takes at the end of its key.
const ANCHOR: usize = 10;

/// A collection's chain of `P` values, keyed by [`SecKey`] — a key range of
/// its lane in the [`Store`].
#[derive(Debug)]
pub struct Chain<P> {
    lane: u64,
    prefix: [u8; PREFIX],
    _payload: PhantomData<fn() -> P>,
}

// Manual: a derive would demand `P: Copy`, but the handle holds only its lane
// and prefix. A scan captures the handle in a long-lived closure, so this is
// load-bearing, not cosmetic.
impl<P> Clone for Chain<P> {
    fn clone(&self) -> Self {
        *self
    }
}
impl<P> Copy for Chain<P> {}

impl<P: WaveWire> Chain<P> {
    /// The chain `declaration` of `owner` (a collection's Pivot) in `lane`'s
    /// lane of `struct_hash`.
    #[must_use]
    pub fn at(
        owner: Id,
        struct_hash: u64,
        lane: Lane,
        declaration: u8,
    ) -> Self {
        let mut prefix = [0; PREFIX];
        prefix[..16].copy_from_slice(&owner.raw().to_be_bytes());
        prefix[16] = declaration;
        Self {
            lane: lane.hash(struct_hash),
            prefix,
            _payload: PhantomData,
        }
    }

    /// The lane's hash: where the store keeps this chain.
    #[must_use]
    pub const fn lane(&self) -> u64 {
        self.lane
    }

    /// The bytes every key of this chain starts with.
    #[must_use]
    pub const fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    /// The lane key of `key`.
    #[must_use]
    pub fn entry_key(&self, key: &SecKey) -> Vec<u8> {
        let mut out = Vec::with_capacity(PREFIX + key.field.len() + ANCHOR);
        out.extend_from_slice(&self.prefix);
        out.extend_from_slice(&key.field);
        key.rec.encode_key(&mut out);
        out
    }

    /// The [`SecKey`] a lane key of this chain holds.
    ///
    /// # Errors
    /// [`Error::LaneBadTag`] for a key outside the chain or too short to
    /// hold an anchor.
    pub fn sec_key(&self, entry: &[u8]) -> Result<SecKey> {
        let own = entry
            .strip_prefix(self.prefix.as_slice())
            .filter(|rest| rest.len() >= ANCHOR)
            .ok_or(Error::LaneBadTag(self.lane))?;
        let (field, anchor) = own.split_at(own.len() - ANCHOR);
        let key = u64::from_be_bytes(
            anchor[..8]
                .try_into()
                .map_err(|_| Error::LaneBadTag(self.lane))?,
        );
        let lower = u16::from_be_bytes([anchor[8], anchor[9]]);
        Ok(SecKey {
            field: field.to_vec(),
            rec: LocalId::new(key, lower >> 15 == 1, lower & 0x7FFF),
        })
    }

    /// The span of every key of this chain, ascending, at most `limit`.
    #[must_use]
    pub fn span(&self, limit: usize) -> Span {
        Span::prefix(&self.prefix, limit)
    }

    /// The write putting `payload` under `key` — whether or not the key was
    /// there.
    #[must_use]
    pub fn insert(&self, key: &SecKey, payload: &P) -> Write {
        Write::Set(self.lane, self.entry_key(key), to_wire(payload))
    }

    /// The write taking `key` out — whether or not it was there.
    #[must_use]
    pub fn remove(&self, key: &SecKey) -> Write {
        Write::Unset(self.lane, self.entry_key(key))
    }

    /// The write taking `key` out, when the chain holds it — the verdict a
    /// caller gates on: `None` is a key that was not there.
    ///
    /// # Errors
    /// A [`Store`] failure.
    pub async fn plan_remove<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<Option<Write>> {
        Ok(self.holds(store, key).await?.then(|| self.remove(key)))
    }

    /// Whether the chain holds `key`.
    ///
    /// # Errors
    /// A [`Store`] failure.
    pub async fn holds<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<bool> {
        Ok(self.entry(store, key).await?.is_some())
    }

    /// The payload filed under `key`, if the chain holds it.
    ///
    /// # Errors
    /// A [`Store`] failure, or a payload that does not decode.
    pub async fn get<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<Option<P>> {
        self.entry(store, key)
            .await?
            .map(|bytes| from_wire(&bytes).map_err(Into::into))
            .transpose()
    }

    /// The stored value under `key`, if the chain holds it.
    async fn entry<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<Option<Vec<u8>>> {
        let at = self.entry_key(key);
        let span = Span {
            hi: successor(&at),
            lo: at.clone(),
            rev: false,
            skip: 0,
            limit: 1,
        };
        let found = store.scan(self.lane, &span).await?;
        Ok(found.into_iter().find(|(k, _)| *k == at).map(|(_, v)| v))
    }

    /// The greatest key the chain holds — the growth end of one keyed by an
    /// instant.
    ///
    /// # Errors
    /// A [`Store`] failure, or a key that does not read back.
    pub async fn last_key<S: Store>(
        &self,
        store: &S,
    ) -> Result<Option<SecKey>> {
        let span = self.span(1).reversed();
        store
            .scan(self.lane, &span)
            .await?
            .first()
            .map(|(key, _)| self.sec_key(key))
            .transpose()
    }

    /// How many entries the chain holds.
    ///
    /// # Errors
    /// A [`Store`] failure.
    pub async fn len<S: Store>(&self, store: &S) -> Result<u64> {
        let span = self.span(0);
        store.count(self.lane, &span.lo, span.hi.as_deref()).await
    }

    /// Every entry in key order, materialised — tests only: a read proper
    /// streams.
    #[cfg(test)]
    pub(crate) async fn collect<S: Store>(
        &self,
        store: &S,
    ) -> Result<Vec<(SecKey, P)>> {
        let mut out = Vec::new();
        for (key, bytes) in
            store.scan(self.lane, &self.span(usize::MAX)).await?
        {
            out.push((self.sec_key(&key)?, from_wire(&bytes)?));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;

    use super::Chain;
    use crate::id::Id;
    use crate::index::lane::Lane;
    use crate::index::mem_store::MemStore;
    use crate::index::sec_key::SecKey;
    use crate::local_id::LocalId;
    use crate::store::Store;
    use crate::u48::U48;

    const TYPE_HASH: u64 = 0x0C4A_1111_2222_3333;

    fn owner(n: u64) -> Id {
        Id::new(n, U48::new(3).unwrap(), false, 7)
    }

    fn key(value: u64) -> SecKey {
        SecKey {
            field: value.to_be_bytes().to_vec(),
            rec: LocalId::new(value, value.is_multiple_of(2), 2),
        }
    }

    fn chain(n: u64) -> Chain<Vec<u8>> {
        Chain::at(owner(n), TYPE_HASH, Lane::Records, 0)
    }

    fn insert(store: &MemStore, chain: &Chain<Vec<u8>>, value: u64) {
        let write = chain.insert(&key(value), &value.to_be_bytes().to_vec());
        block_on(store.apply(&[write])).unwrap();
    }

    /// How many segments the chain currently holds.
    async fn segments(store: &MemStore, chain: &Chain<Vec<u8>>) -> usize {
        let mut n = 0;
        let mut cursor = Some(chain.head());
        while let Some(id) = cursor {
            n += 1;
            cursor = chain.load(store, id).await.unwrap().next();
        }
        n
    }

    /// A removal-log chain: no index, so `locate` walks back from the tail.
    async fn fresh_log(store: &MemStore) -> Chain<Vec<u8>> {
        let (chain, writes) =
            Chain::<Vec<u8>>::plan_create_log(tenant(), TYPE_HASH, Lane::Dead);
        store.apply(&writes).await.unwrap();
        chain.with_min(MIN)
    }

    #[test]
    fn an_index_less_log_orders_and_splits_like_any_chain() {
        block_on(async {
            // The `dead` lane: nothing searches it, so it carries no index and
            // its segments are the whole structure. Everything else — the band,
            // the 50/50 split, the endpoint discipline — is unchanged.
            let store = MemStore::default();
            let mut log = fresh_log(&store).await;
            assert!(log.index().is_none(), "a log must carry no index");
            let tail = log.tail();

            for v in 1..=30u64 {
                insert(&store, &mut log, v).await;
                check(&store, &log).await;
            }
            assert_eq!(check(&store, &log).await, (1..=30).collect::<Vec<_>>());
            assert!(segments(&store, &log).await > 3, "the log never split");
            assert_eq!(log.tail(), tail, "an append moved the log's tail");
        });
    }

    #[test]
    fn an_index_less_locate_still_finds_an_out_of_order_key() {
        block_on(async {
            // Appends are monotone in practice — the collection's instant floor
            // guarantees it — so the backward walk stops at the tail. This pins
            // that it stays *correct* when a key belongs further back, rather
            // than dropping it into the tail and breaking the order.
            let store = MemStore::default();
            let mut log = fresh_log(&store).await;
            for v in (1..=30u64).map(|v| v * 10) {
                insert(&store, &mut log, v).await;
            }
            insert(&store, &mut log, 55).await;
            let keys = check(&store, &log).await;
            let at = keys.iter().position(|&k| k == 55).expect("55 is missing");
            assert_eq!((keys[at - 1], keys[at + 1]), (50, 60));

            assert_eq!(
                log.get(&store, &key(55)).await.unwrap(),
                Some(55u64.to_be_bytes().to_vec())
            );
        });
    }

    #[test]
    fn a_fresh_chain_is_one_empty_shell() {
        block_on(async {
            let store = MemStore::default();
            let chain = fresh(&store).await;
            assert_eq!(chain.head(), chain.tail());
            let observed = check(&store, &chain).await;
            assert!(observed.is_empty(), "{observed:?}");
        });
    }

    #[test]
    fn entries_come_out_in_key_order_whatever_the_arrival_order() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in [50u64, 10, 90, 30, 70, 20, 80, 60, 40, 5, 95, 15] {
                insert(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            let mut want =
                vec![5u64, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 95];
            want.sort_unstable();
            assert_eq!(check(&store, &chain).await, want);
        });
    }

    #[test]
    fn a_tail_split_keeps_the_tail_and_moves_the_head_exactly_once() {
        block_on(async {
            // Ascending arrivals: every insert lands at the growth end, which is
            // the built-in chain's shape. The tail id must never move; the head
            // moves only on the very first split, when head == tail.
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            let tail = chain.tail();
            let first_head = chain.head();

            insert(&store, &mut chain, 1).await;
            let mut moves = 0;
            let mut head = chain.head();
            for v in 2..=40u64 {
                insert(&store, &mut chain, v).await;
                check(&store, &chain).await;
                assert_eq!(chain.tail(), tail, "the tail id moved at {v}");
                if chain.head() != head {
                    moves += 1;
                    head = chain.head();
                }
            }
            assert_eq!(moves, 1, "the head moved {moves} times, not once");
            assert_ne!(
                chain.head(),
                first_head,
                "the first split never happened"
            );
            assert!(segments(&store, &chain).await > 4, "no splits occurred");
        });
    }

    #[test]
    fn an_interior_split_moves_neither_endpoint() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            // Build a chain of several segments first.
            for v in 1..=24u64 {
                insert(&store, &mut chain, v * 10).await;
            }
            let (head, tail) = (chain.head(), chain.tail());
            let before = segments(&store, &chain).await;

            // Now fill one interior segment by inserting between existing keys.
            for v in 1..=9u64 {
                insert(&store, &mut chain, 100 + v).await;
                check(&store, &chain).await;
            }
            assert!(
                segments(&store, &chain).await > before,
                "no interior split happened"
            );
            assert_eq!(chain.head(), head, "an interior split moved the head");
            assert_eq!(chain.tail(), tail, "an interior split moved the tail");
        });
    }

    #[test]
    fn a_stored_payload_comes_back_verbatim() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=30u64 {
                insert(&store, &mut chain, v).await;
            }
            for v in 1..=30u64 {
                assert_eq!(
                    chain.get(&store, &key(v)).await.unwrap(),
                    Some(v.to_be_bytes().to_vec()),
                    "payload for {v} came back wrong"
                );
            }
            assert_eq!(chain.get(&store, &key(999)).await.unwrap(), None);
        });
    }

    #[test]
    fn inserting_an_existing_key_replaces_it_without_growing_the_chain() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=10u64 {
                insert(&store, &mut chain, v).await;
            }
            let before = check(&store, &chain).await;
            let writes =
                chain.plan_insert(&store, key(5), vec![0xFF]).await.unwrap();
            store.apply(&writes).await.unwrap();

            assert_eq!(check(&store, &chain).await, before, "the chain grew");
            assert_eq!(
                chain.get(&store, &key(5)).await.unwrap(),
                Some(vec![0xFF])
            );
        });
    }

    #[test]
    fn a_starved_segment_merges_with_its_neighbour() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=24u64 {
                insert(&store, &mut chain, v).await;
            }
            let before = segments(&store, &chain).await;
            assert!(before >= 3, "need several segments, got {before}");

            // Drain the middle of the chain; segments must fold together rather
            // than linger under the band.
            for v in 9..=16u64 {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            assert!(
                segments(&store, &chain).await < before,
                "nothing merged: still {before} segments"
            );
        });
    }

    #[test]
    fn a_merge_that_would_breach_the_band_redistributes_instead() {
        block_on(async {
            // The starved side and its neighbour must together exceed 2N, or
            // folding them into one is legal and merging is the right answer.
            // Eight ascending keys split into two segments of N; three more land
            // in the second, taking it to 7. Draining the first to N/2 then
            // leaves 2 + 7 = 9 > 2N, so the pair must share out instead of fold.
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=8u64 {
                insert(&store, &mut chain, v * 10).await;
            }
            for v in 1..=3u64 {
                insert(&store, &mut chain, 50 + v).await;
            }
            assert_eq!(segments(&store, &chain).await, 2, "expected one split");

            remove(&store, &mut chain, 10).await;
            check(&store, &chain).await;
            remove(&store, &mut chain, 20).await;
            let keys = check(&store, &chain).await;

            assert_eq!(
                segments(&store, &chain).await,
                2,
                "a redistribute must not change the segment count"
            );
            // `check` already asserts both sides are inside the band; this pins
            // that nothing was lost in the shuffle.
            assert_eq!(keys, vec![30, 40, 50, 51, 52, 53, 60, 70, 80]);
        });
    }

    #[test]
    fn emptying_the_chain_leaves_one_shell_and_no_orphans() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=40u64 {
                insert(&store, &mut chain, v).await;
            }
            for v in 1..=40u64 {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            let observed = check(&store, &chain).await;
            assert!(observed.is_empty(), "{observed:?}");
            assert_eq!(segments(&store, &chain).await, 1, "shell not kept");
            assert_eq!(chain.head(), chain.tail());
            let index = chain.index().expect("an indexed chain");
            assert_eq!(index.total(&store).await.unwrap(), 0);
        });
    }

    #[test]
    fn removing_in_reverse_also_lands_on_one_shell() {
        block_on(async {
            // The mirror of the previous test: draining from the tail exercises
            // the other merge direction (the survivor absorbing leftward).
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=40u64 {
                insert(&store, &mut chain, v).await;
            }
            for v in (1..=40u64).rev() {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
            }
            let observed = check(&store, &chain).await;
            assert!(observed.is_empty(), "{observed:?}");
            assert_eq!(segments(&store, &chain).await, 1);
        });
    }

    #[test]
    fn a_merge_never_deletes_an_endpoint() {
        block_on(async {
            // Draining from the tail end starves the tail over and over, and
            // every one of those merges involves it. The tail must be the
            // survivor: if the absorbed side were chosen by position alone, each
            // merge would move the tail id and force a `Pivot` rewrite — the
            // cost the endpoint discipline exists to avoid.
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=40u64 {
                insert(&store, &mut chain, v).await;
            }
            let (head, tail) = (chain.head(), chain.tail());

            // Stop well before the chain would collapse to a single segment,
            // which is the one case where an endpoint legitimately moves.
            for v in (11..=40u64).rev() {
                remove(&store, &mut chain, v).await;
                check(&store, &chain).await;
                assert_eq!(chain.tail(), tail, "a merge moved the tail at {v}");
                assert_eq!(chain.head(), head, "a merge moved the head at {v}");
            }
            assert!(segments(&store, &chain).await > 1, "collapsed too far");
        });
    }

    #[test]
    fn removing_an_absent_key_plans_nothing() {
        block_on(async {
            let store = MemStore::default();
            let mut chain = fresh(&store).await;
            for v in 1..=10u64 {
                insert(&store, &mut chain, v).await;
            }
            assert!(
                chain.plan_remove(&store, &key(99)).await.unwrap().is_none(),
                "an absent key planned work"
            );
        });
    }

    #[test]
    fn a_dangling_head_is_a_typed_fault_not_a_panic() {
        block_on(async {
            let store = MemStore::default();
            // Never committed: the handle names segments that do not exist.
            let (chain, _) = Chain::<Vec<u8>>::plan_create(
                tenant(),
                TYPE_HASH,
                Lane::Records,
            );
            assert!(matches!(
                chain.get(&store, &key(1)).await,
                Err(crate::error::Error::ChainNodeMissing(_))
            ));
        });
    }
}
