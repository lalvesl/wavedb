//! `Overlay` — a plan-time read view of a [`Store`] with one batch's pending
//! [`Write`]s layered on top.
//!
//! Planning two tree mutations on the **same** tree inside one atomic batch (a
//! `save`'s old-key removal, then its new-key insert) must let the second plan
//! read the first's node writes — otherwise both rewrite the same node from its
//! original state and the later write silently undoes the earlier. The overlay
//! is that staging view; it never commits (the real batch commits through the
//! inner store).

use std::collections::HashMap;

use crate::error::{Error, Result};
use crate::id::Id;
use crate::store::{Store, Write};

/// A read view of `inner` with a batch's pending [`Write`]s layered on top.
pub(crate) struct Overlay<'a, S> {
    inner: &'a S,
    /// `Some(bytes)` = pending put, `None` = pending remove.
    pending: HashMap<u128, Option<Vec<u8>>>,
}

impl<'a, S: Store> Overlay<'a, S> {
    pub(crate) fn new(inner: &'a S) -> Self {
        Self {
            inner,
            pending: HashMap::new(),
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
                // A guard is not state — nothing to stage.
                Write::Expect(..) => {}
            }
        }
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

    fn apply(&self, _batch: &[Write]) -> impl Future<Output = Result<()>> {
        // Plans only read; the real batch commits through the inner store.
        std::future::ready(Err(Error::Backend(
            "overlay store is read-only".into(),
        )))
    }
}
