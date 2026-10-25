//! How a record chain reaches its segments ([RFC 0050]): find the one
//! covering a key — through the sparse index, or back from the tail for the
//! index-less removal log — read one by id, and stage one written or erased
//! into a plan's batch.
//!
//! [`chain`](super::chain) is the handle; [`chain_insert`](super::chain_insert)
//! and [`chain_remove`](super::chain_remove) are the edits built on these.
//!
//! [RFC 0050]: https://github.com/wavedb/wavedb/blob/main/rfcs/0050-clustered-record-chains.md

use crate::error::{Error, Result};
use crate::local_id::LocalId;
use crate::overlay::Overlay;
use crate::store::{Store, Write};
use crate::wire::WaveWire;

use super::chain::Chain;
use super::node_key::SecKey;
use super::segment::Segment;

impl<P: WaveWire> Chain<P> {
    /// The segment covering `key`, and its id. A key below every separator is
    /// covered by the head — the index has no entry for it by construction.
    pub(super) async fn locate<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<(LocalId, Segment<P>)> {
        let Some(index) = self.index else {
            return self.walk_back_to(store, key).await;
        };
        let id = match index.find(store, key).await? {
            Some(slot) => slot.seg,
            None => self.head,
        };
        Ok((id, self.load(store, id).await?))
    }

    /// [`locate`](Self::locate) without an index: step back from the tail until
    /// a segment's least key no longer sits above `key`.
    ///
    /// The removal log's only writer appends a monotone instant, so this stops
    /// at the tail on the first read; it stays correct, at a read per segment
    /// passed, for anything that does not.
    async fn walk_back_to<S: Store>(
        &self,
        store: &S,
        key: &SecKey,
    ) -> Result<(LocalId, Segment<P>)> {
        let mut id = self.tail;
        loop {
            let seg = self.load(store, id).await?;
            let above = seg.first_key().is_some_and(|first| first > key);
            match seg.prev() {
                Some(prev) if above => id = prev,
                // Either this segment covers the key, or there is nothing
                // before it and the head is where the key belongs.
                _ => return Ok((id, seg)),
            }
        }
    }

    /// Push a segment write and stage it, so later steps of the same plan read
    /// what earlier ones wrote.
    pub(super) fn emit<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        id: LocalId,
        seg: &Segment<P>,
    ) {
        let write = self.put(id, seg);
        view.stage(core::slice::from_ref(&write));
        writes.push(write);
    }

    /// Drop a segment from the store and stage the removal.
    pub(super) fn erase<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        id: LocalId,
    ) {
        let write = Write::Remove(self.lane_hash, id.to_id(self.tenant));
        view.stage(core::slice::from_ref(&write));
        writes.push(write);
    }

    /// The [`Write`] persisting `seg` under `id`.
    pub(super) fn put(&self, id: LocalId, seg: &Segment<P>) -> Write {
        Write::Put(id.to_id(self.tenant), seg.to_bytes(self.lane_hash))
    }

    /// Read one segment by id — how a scan steps along the chain, following
    /// [`Segment::next`] forward or [`Segment::prev`] back.
    ///
    /// # Errors
    /// As [`get`](Self::get).
    pub(crate) async fn segment<S: Store>(
        &self,
        store: &S,
        id: LocalId,
    ) -> Result<Segment<P>> {
        self.load(store, id).await
    }

    /// Read one segment, checking its lane tag.
    pub(super) async fn load<S: Store>(
        &self,
        store: &S,
        id: LocalId,
    ) -> Result<Segment<P>> {
        let bytes = store
            .get_of(self.lane_hash, id.to_id(self.tenant))
            .await?
            .ok_or(Error::ChainNodeMissing(id))?;
        Segment::from_bytes(self.lane_hash, &bytes)
    }

    /// This chain's size band minimum.
    pub(super) const fn min(&self) -> usize {
        self.min
    }

    /// Set the head — `chain_remove` moves it when a merge consumes it.
    pub(super) const fn set_head(&mut self, id: LocalId) {
        self.head = id;
    }

    /// Set the tail — `chain_remove` moves it when a merge consumes it.
    pub(super) const fn set_tail(&mut self, id: LocalId) {
        self.tail = id;
    }
}
