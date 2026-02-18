//! A record chain's **insert** ([RFC 0050]): file a key in the segment that
//! covers it, split that segment 50/50 when it reaches `2N`, and keep the
//! sparse index in step — one [`Write`] batch, staged through an [`Overlay`]
//! so each step reads what the earlier ones wrote.
//!
//! [`chain`](super::chain) is the handle; removal and merging live in
//! [`chain_remove`](super::chain_remove).
//!
//! [RFC 0050]: https://github.com/wavedb/wavedb/blob/main/rfcs/0050-clustered-record-chains.md

use crate::error::Result;
use crate::local_id::LocalId;
use crate::overlay::Overlay;
use crate::store::{Store, Write};
use crate::wire::WaveWire;

use super::chain::Chain;
use super::sec_key::SecKey;
use super::segment::{Segment, mint_lane_id};
use super::sparse::Slot;

impl<P: WaveWire> Chain<P> {
    /// Insert or replace `key`'s entry, splitting the segment when it reaches
    /// `2N` and keeping the sparse index in step.
    ///
    /// Takes `&mut self` only because a chain's **first** split must move one
    /// endpoint (when `head == tail`, one of them becomes the fresh id). After
    /// that the endpoints are frozen, so a caller that compares them before and
    /// after will see a `Pivot` rewrite at most once per chain.
    ///
    /// # Errors
    /// As [`get`](Self::get).
    pub async fn plan_insert<S: Store>(
        &mut self,
        store: &S,
        key: SecKey,
        payload: P,
    ) -> Result<Vec<Write>> {
        let mut view = Overlay::new(store);
        let mut writes = Vec::new();
        let (id, mut seg) = self.locate(&view, &key).await?;
        let was = seg.first_key().cloned();
        seg.insert(key, payload);

        if seg.len() >= self.min.saturating_mul(2) {
            self.split(&mut view, &mut writes, id, seg, was).await?;
        } else {
            self.emit(&mut view, &mut writes, id, &seg);
            self.reindex(&mut view, &mut writes, was, id, &seg).await?;
        }
        Ok(writes)
    }

    /// Split a full segment 50/50 and relink it.
    ///
    /// Three segment writes, not two: the two halves, plus the neighbour on the
    /// far side of the freshly minted one, whose pointer must now name it. Only
    /// a split at a chain end costs two, and that end is where the endpoint
    /// pointer moves instead.
    async fn split<S: Store>(
        &mut self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        id: LocalId,
        mut seg: Segment<P>,
        was: Option<SecKey>,
    ) -> Result<()> {
        let (prev, next) = (seg.prev(), seg.next());
        let mut lower = seg.take_entries();
        let upper = lower.split_off(lower.len() / 2);
        let fresh = mint_lane_id(self.lane_hash);

        // The growth end keeps its id *and* the newer half, so the fresh
        // segment lands behind it; everywhere else the fresh half goes after.
        let (kept, made) = if id == self.tail {
            let made = Segment::with_entries(prev, Some(id), lower);
            let kept = Segment::with_entries(Some(fresh), next, upper);
            match prev {
                Some(outer) => {
                    self.relink(view, writes, outer, Some(fresh), true).await?;
                }
                None => self.head = fresh,
            }
            (kept, made)
        } else {
            let made = Segment::with_entries(Some(id), next, upper);
            let kept = Segment::with_entries(prev, Some(fresh), lower);
            match next {
                Some(outer) => {
                    self.relink(view, writes, outer, Some(fresh), false)
                        .await?;
                }
                None => self.tail = fresh,
            }
            (kept, made)
        };

        self.emit(view, writes, id, &kept);
        self.emit(view, writes, fresh, &made);
        // Whichever half holds the lower keys inherits the old separator; the
        // other needs one of its own.
        let (low_id, low, high_id, high) = if id == self.tail {
            (fresh, &made, id, &kept)
        } else {
            (id, &kept, fresh, &made)
        };
        self.reindex(view, writes, was, low_id, low).await?;
        self.reindex(view, writes, None, high_id, high).await
    }

    /// Repoint one neighbour's `next` (or `prev`) at `to`.
    pub(super) async fn relink<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        id: LocalId,
        to: Option<LocalId>,
        set_next: bool,
    ) -> Result<()> {
        let mut seg: Segment<P> = self.load(view, id).await?;
        if set_next {
            seg.set_next(to);
        } else {
            seg.set_prev(to);
        }
        self.emit(view, writes, id, &seg);
        Ok(())
    }

    /// File `seg` in the index under its current least key, dropping the entry
    /// it used to sit under when that key moved.
    pub(super) async fn reindex<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        was: Option<SecKey>,
        id: LocalId,
        seg: &Segment<P>,
    ) -> Result<()> {
        // An index-less chain (the removal log) has no separators to keep.
        let Some(index) = self.index else {
            return Ok(());
        };
        let now = seg.first_key().cloned();
        if was != now
            && let Some(old) = was
        {
            let batch = index.plan_remove(view, &old).await?;
            view.stage(&batch);
            writes.extend(batch);
        }
        if let Some(first) = now {
            let slot = Slot {
                first,
                seg: id,
                count: seg.len() as u64,
            };
            let batch = index.plan_upsert(view, slot).await?;
            view.stage(&batch);
            writes.extend(batch);
        }
        Ok(())
    }
}
