//! `Overlay` — a plan-time read view of a [`Store`] with one batch's pending
//! [`Write`]s layered on top.
//!
//! Planning two mutations of the **same** state inside one atomic batch (a
//! `save`'s record write, then a read of the record it wrote) must let the
//! second plan read the first's writes — otherwise both start from the
//! original state and the later write silently undoes the earlier. The
//! overlay is that staging view, over records and ordered lanes alike; it
//! never commits (the real batch commits through the inner store).

use std::collections::{BTreeMap, HashMap};

use crate::error::{Error, Result};
use crate::id::Id;
use crate::store::{Entry, Span, Store, Write};

/// A read view of `inner` with a batch's pending [`Write`]s layered on top.
pub(crate) struct Overlay<'a, S> {
    inner: &'a S,
    /// `Some(bytes)` = pending put, `None` = pending remove.
    pending: HashMap<u128, Option<Vec<u8>>>,
    /// Per ordered lane: `Some(value)` = pending set, `None` = pending unset.
    lanes: HashMap<u64, BTreeMap<Vec<u8>, Option<Vec<u8>>>>,
}

impl<'a, S: Store> Overlay<'a, S> {
    pub(crate) fn new(inner: &'a S) -> Self {
        Self {
            inner,
            pending: HashMap::new(),
            lanes: HashMap::new(),
        }
    }

    /// Layer a planned batch's writes onto the view.
    pub(crate) fn stage(&mut self, writes: &[Write]) {
        for w in writes {
            match w {
                Write::Put(id, bytes) => {
                    self.pending.insert(id.raw(), Some(bytes.clone()));
                }
                Write::Remove(_, id) => {
                    self.pending.insert(id.raw(), None);
                }
                Write::Set(lane, key, value) => {
                    let lane = self.lanes.entry(*lane).or_default();
                    lane.insert(key.clone(), Some(value.clone()));
                }
                Write::Unset(lane, key) => {
                    self.lanes
                        .entry(*lane)
                        .or_default()
                        .insert(key.clone(), None);
                }
                // A guard is not state — nothing to stage.
                Write::Expect(..) => {}
            }
        }
    }

    /// The staged entries of `lane` within `span`'s range.
    fn staged<'s>(
        &'s self,
        lane: u64,
        span: &Span,
    ) -> impl Iterator<Item = (&'s Vec<u8>, &'s Option<Vec<u8>>)> {
        self.lanes
            .get(&lane)
            .into_iter()
            .flat_map(move |l| l.range::<[u8], _>(span.bounds()))
    }
}

impl<S: Store> Store for Overlay<'_, S> {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        match self.pending.get(&id.raw()) {
            Some(slot) => Ok(slot.clone()),
            None => self.inner.get(id).await,
        }
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> Result<Option<Vec<u8>>> {
        match self.pending.get(&id.raw()) {
            Some(slot) => Ok(slot.clone()),
            None => self.inner.get_of(struct_hash, id).await,
        }
    }

    /// The inner store's entries with the staged ones merged in. The inner
    /// read takes as many more as are staged in the range — at most that
    /// many of its entries can be unset — and no skip, which applies to the
    /// merged order.
    async fn scan(&self, lane: u64, span: &Span) -> Result<Vec<Entry>> {
        let staged: BTreeMap<&Vec<u8>, &Option<Vec<u8>>> =
            self.staged(lane, span).collect();
        if staged.is_empty() {
            return self.inner.scan(lane, span).await;
        }
        let skip = usize::try_from(span.skip).unwrap_or(usize::MAX);
        let wide = Span {
            skip: 0,
            limit: skip.saturating_add(span.limit).saturating_add(staged.len()),
            ..span.clone()
        };
        let mut merged: BTreeMap<Vec<u8>, Vec<u8>> =
            self.inner.scan(lane, &wide).await?.into_iter().collect();
        for (key, value) in staged {
            match value {
                Some(value) => {
                    merged.insert(key.clone(), value.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        let read = Span {
            skip: 0,
            limit: usize::MAX,
            ..span.clone()
        };
        // Where the inner read was cut, at least `skip + limit` merged
        // entries lie before its last key — the unsets take at most as many
        // as it read beyond — so what is taken never reaches a staged set
        // past entries it did not read.
        let all = crate::store::read_span(&merged, &read);
        Ok(all.into_iter().skip(skip).take(span.limit).collect())
    }

    async fn count(
        &self,
        lane: u64,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<u64> {
        let span = Span {
            lo: lo.to_vec(),
            hi: hi.map(<[u8]>::to_vec),
            rev: false,
            skip: 0,
            limit: 1,
        };
        let mut count = self.inner.count(lane, lo, hi).await?;
        for (key, value) in self.staged(lane, &span) {
            let probe = Span {
                lo: key.clone(),
                ..span.clone()
            };
            let there = self
                .inner
                .scan(lane, &probe)
                .await?
                .first()
                .is_some_and(|(k, _)| k == key);
            match (there, value.is_some()) {
                (false, true) => count += 1,
                (true, false) => count -= 1,
                _ => {}
            }
        }
        Ok(count)
    }

    fn apply(&self, _batch: &[Write]) -> impl Future<Output = Result<()>> {
        // Plans only read; the real batch commits through the inner store.
        std::future::ready(Err(Error::Backend(
            "overlay store is read-only".into(),
        )))
    }
}
