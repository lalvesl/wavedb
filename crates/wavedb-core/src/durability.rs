//! [`Durability`] — a store whose batches are durable when asked to be
//! ([RFC 0072] §12).
//!
//! Every mutating operation is one [`Store::apply`] batch, written once in
//! the collection and record layers. A durable operation is the same
//! operation over this wrapper: its batch goes to
//! [`apply_durable`](Store::apply_durable) instead, and nothing else about
//! it changes. So the `_durable` operations of every context — a
//! [`LocalHandle`](crate::LocalHandle), a node's command or `#[server]`
//! operation — need no second copy of any write path.
//!
//! The choice is a field rather than a type, so a node serving both kinds
//! of command runs one monomorphised dispatch, not two.
//!
//! [RFC 0072]: https://github.com/wavedb/wavedb/blob/main/rfcs/0072-the-settle-is-the-durability-point.md

use crate::error::Result;
use crate::id::Id;
use crate::store::{Store, Write};

/// `store`, its batches applied durably when `durable` is set and plainly
/// otherwise. Reads and mutation notices pass straight through.
#[derive(Debug, Clone, Copy)]
pub struct Durability<S> {
    store: S,
    durable: bool,
}

impl<S> Durability<S> {
    /// `store`, durable when `durable` says so.
    #[must_use]
    pub const fn new(store: S, durable: bool) -> Self {
        Self { store, durable }
    }

    /// Whether this store's batches are applied durably.
    #[must_use]
    pub const fn is_durable(&self) -> bool {
        self.durable
    }
}

impl<S: Store> Store for Durability<S> {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        self.store.get(id).await
    }

    async fn get_of(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> Result<Option<Vec<u8>>> {
        self.store.get_of(struct_hash, id).await
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        if self.durable {
            self.store.apply_durable(batch).await
        } else {
            self.store.apply(batch).await
        }
    }

    async fn apply_durable(&self, batch: &[Write]) -> Result<()> {
        self.store.apply_durable(batch).await
    }

    fn note_mutation(
        &self,
        mutation: impl FnOnce() -> crate::notify::Mutation,
    ) {
        self.store.note_mutation(mutation);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;

    use super::Durability;
    use crate::error::Result;
    use crate::id::Id;
    use crate::store::{Store, Write};
    use crate::u48::U48;

    /// Records which of the two applies each batch reached.
    #[derive(Default)]
    struct Recording(RefCell<Vec<bool>>);

    // A test double, written in the trait's own (async) shape.
    #[allow(clippy::unused_async_trait_impl)]
    impl Store for Recording {
        async fn get(&self, _: Id) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        async fn apply(&self, _: &[Write]) -> Result<()> {
            self.0.borrow_mut().push(false);
            Ok(())
        }

        async fn apply_durable(&self, _: &[Write]) -> Result<()> {
            self.0.borrow_mut().push(true);
            Ok(())
        }
    }

    fn batch() -> Vec<Write> {
        vec![Write::Remove(1, Id::new(1, U48::from(1u32), false, 0))]
    }

    #[test]
    fn a_durable_wrapper_sends_every_batch_to_apply_durable() {
        let inner = Recording::default();
        futures::executor::block_on(async {
            let durable = Durability::new(&inner, true);
            durable.apply(&batch()).await.unwrap();
            durable.apply_durable(&batch()).await.unwrap();
            let plain = Durability::new(&inner, false);
            plain.apply(&batch()).await.unwrap();
            plain.apply_durable(&batch()).await.unwrap();
        });
        assert_eq!(*inner.0.borrow(), [true, true, false, true]);
    }

    #[test]
    fn a_forwarding_rc_keeps_the_durable_promise() {
        let inner = std::rc::Rc::new(Recording::default());
        futures::executor::block_on(async {
            Durability::new(std::rc::Rc::clone(&inner), true)
                .apply(&batch())
                .await
                .unwrap();
        });
        assert_eq!(*inner.0.borrow(), [true]);
    }
}
