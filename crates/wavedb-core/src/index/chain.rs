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

    fn keys(store: &MemStore, chain: &Chain<Vec<u8>>) -> Vec<u64> {
        block_on(chain.collect(store))
            .unwrap()
            .into_iter()
            .map(|(k, _)| u64::from_be_bytes(k.field.try_into().unwrap()))
            .collect()
    }

    /// A key reads back into the `SecKey` it was written as, anchor flag
    /// and salt included.
    #[test]
    fn an_entry_key_reads_back_into_its_sec_key() {
        let chain = chain(1);
        for value in [0, 1, 2, u64::MAX] {
            let entry = chain.entry_key(&key(value));
            assert_eq!(chain.sec_key(&entry).unwrap(), key(value));
        }
        let other = Chain::<Vec<u8>>::at(owner(2), TYPE_HASH, Lane::Records, 0);
        assert!(other.sec_key(&chain.entry_key(&key(1))).is_err());
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
