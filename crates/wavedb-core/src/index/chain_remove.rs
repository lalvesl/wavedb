//! [`Chain`]'s removal half ([RFC 0052]) — take an entry out, and rebalance the
//! segment against a neighbour when it falls to `N/2`.
//!
//! Split from [`chain`](super::chain) and from its insert: the shrinking side
//! carries the harder policy.
//!
//! ## Why a merge rule exists at all
//!
//! On a chain keyed by a domain value, removals are the only thing that empties a
//! segment. On the **built-in** chain they are not: it is keyed by the live
//! version's authoring instant, so every save *relocates* its record to the
//! growth end — which means interior segments **drain** even when nothing is ever
//! deleted. Without a merge rule a write-heavy collection would end up a long run
//! of nearly-empty segments, reading many of them to yield few records, which is
//! precisely what RFC 0050 exists to avoid.
//!
//! ## The two outcomes
//!
//! Given a starved segment and the neighbour it rebalances against:
//!
//! - **combined < 2N — merge.** One segment absorbs the other, the absorbed id is
//!   deleted, and the neighbour beyond it is repointed.
//! - **combined ≥ 2N — redistribute.** Merging would breach the band and the next
//!   insert would split it straight back, so the entries are shared out evenly
//!   instead, leaving both halves at ≥ N. No link changes, no endpoint moves.
//!
//! When an endpoint takes part in a merge it is the survivor, so `head` and `tail`
//! keep their ids. The single exception is a chain collapsing to one segment,
//! where head and tail necessarily become the same id again.
//!
//! [RFC 0052]: https://github.com/wavedb/wavedb/blob/main/rfcs/0052-segment-size-as-the-pagination-unit.md

use crate::error::Result;
use crate::local_id::LocalId;
use crate::overlay::Overlay;
use crate::store::{Store, Write};
use crate::wire::WaveWire;

use super::chain::Chain;
use super::sec_key::SecKey;
use super::segment::Segment;

/// One side of a rebalance: the segment, its id, and the separator it is filed
/// under today.
struct Side<P> {
    id: LocalId,
    seg: Segment<P>,
    was: Option<SecKey>,
}

impl<P: WaveWire> Chain<P> {
    /// Remove `key`'s entry, rebalancing the segment when it falls to `N/2`.
    ///
    /// `None` when the chain did not hold `key` — nothing planned, rather than an
    /// empty batch that looks like work.
    ///
    /// Takes `&mut self` because a merge that consumes an endpoint moves it; see
    /// the module docs for when that can happen.
    ///
    /// # Errors
    /// Propagates a [`Store`] failure, [`Error::ChainNodeMissing`] on a dangling
    /// pointer, or [`Error::LaneBadTag`] when a pointer resolves to a foreign
    /// value.
    ///
    /// [`Error::ChainNodeMissing`]: crate::Error::ChainNodeMissing
    /// [`Error::LaneBadTag`]: crate::Error::LaneBadTag
    pub async fn plan_remove<S: Store>(
        &mut self,
        store: &S,
        key: &SecKey,
    ) -> Result<Option<Vec<Write>>> {
        let mut view = Overlay::new(store);
        let mut writes = Vec::new();
        let (id, mut seg) = self.locate(&view, key).await?;
        let was = seg.first_key().cloned();
        if seg.remove(key).is_none() {
            return Ok(None);
        }

        // A lone segment has no neighbour to rebalance against: it stays as an
        // empty shell rather than leaving the `Pivot` pointing at nothing.
        if seg.len() <= self.min() / 2
            && let Some(other) = seg.next().or_else(|| seg.prev())
        {
            let side = Side { id, seg, was };
            self.rebalance(&mut view, &mut writes, side, other).await?;
        } else {
            self.emit(&mut view, &mut writes, id, &seg);
            self.reindex(&mut view, &mut writes, was, id, &seg).await?;
        }
        Ok(Some(writes))
    }

    /// Merge or redistribute `starved` against the neighbour at `other`.
    async fn rebalance<S: Store>(
        &mut self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        starved: Side<P>,
        other: LocalId,
    ) -> Result<()> {
        let mate = Side {
            id: other,
            was: None,
            seg: self.load(view, other).await?,
        };
        let mate = Side {
            was: mate.seg.first_key().cloned(),
            ..mate
        };
        // Order the pair by key so "left" and "right" mean what they say.
        let (left, right) = if starved.seg.next() == Some(other) {
            (starved, mate)
        } else {
            (mate, starved)
        };

        if left.seg.len() + right.seg.len() >= self.min().saturating_mul(2) {
            self.redistribute(view, writes, left, right).await
        } else {
            self.merge(view, writes, left, right).await
        }
    }

    /// Share the pair's entries out evenly. Links and endpoints are untouched —
    /// only the right side's separator moves.
    async fn redistribute<S: Store>(
        &self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        mut left: Side<P>,
        mut right: Side<P>,
    ) -> Result<()> {
        let mut all = left.seg.take_entries();
        all.extend(right.seg.take_entries());
        let upper = all.split_off(all.len().div_ceil(2));
        left.seg.extend(all);
        right.seg.extend(upper);

        self.emit(view, writes, left.id, &left.seg);
        self.emit(view, writes, right.id, &right.seg);
        self.reindex(view, writes, left.was, left.id, &left.seg)
            .await?;
        self.reindex(view, writes, right.was, right.id, &right.seg)
            .await
    }

    /// Fold the pair into one segment, deleting the absorbed id.
    ///
    /// The survivor is the `head` if it takes part, else the `tail` if it does,
    /// else the left side — so an endpoint is never the one deleted.
    async fn merge<S: Store>(
        &mut self,
        view: &mut Overlay<'_, S>,
        writes: &mut Vec<Write>,
        mut left: Side<P>,
        mut right: Side<P>,
    ) -> Result<()> {
        let keep_left = left.id == self.head() || right.id != self.tail();
        // Whoever survives ends up holding every entry, in key order.
        let entries = {
            let mut all = left.seg.take_entries();
            all.extend(right.seg.take_entries());
            all
        };

        let (mut kept, gone) = if keep_left {
            (left, right)
        } else {
            (right, left)
        };
        kept.seg.extend(entries);

        if keep_left {
            // The right side goes: its `next` becomes the survivor's, and
            // whatever followed it now points back at the survivor.
            kept.seg.set_next(gone.seg.next());
            match gone.seg.next() {
                Some(outer) => {
                    self.relink(view, writes, outer, Some(kept.id), false)
                        .await?;
                }
                None => self.set_tail(kept.id),
            }
        } else {
            kept.seg.set_prev(gone.seg.prev());
            match gone.seg.prev() {
                Some(outer) => {
                    self.relink(view, writes, outer, Some(kept.id), true)
                        .await?;
                }
                None => self.set_head(kept.id),
            }
        }

        self.erase(view, writes, gone.id);
        self.emit(view, writes, kept.id, &kept.seg);
        // The absorbed segment's separator goes; the survivor re-files under
        // whatever its least key is now (which changed if it absorbed leftward).
        if let Some(index) = self.index()
            && let Some(old) = gone.was
        {
            let batch = index.plan_remove(view, &old).await?;
            view.stage(&batch);
            writes.extend(batch);
        }
        self.reindex(view, writes, kept.was, kept.id, &kept.seg)
            .await
    }
}
