//! Does a shared `BpTree` node survive two concurrently planned batches?
//!
//! The invariant the RFC 0050 chain inherits, pinned here because it is a
//! property of the **executor model**, not of any structure: `PageStore`'s
//! futures all resolve on first poll (its `get`/`apply` bodies are synchronous),
//! so on a current-thread runtime a collection op never yields between its reads
//! and its `apply` and cannot interleave with another. A `Store` that genuinely
//! suspends — IndexedDB, or anything network-backed — breaks that, and the loss
//! is silent: the record is live at its anchor and absent from the index.
//!
//! Two backends, identical but for whether `get` suspends, driven by the same
//! pair of interleaved inserts.

// The engine's futures are deliberately non-`Send` (current-thread `LocalSet`),
// which is the whole subject here; an integration test is its own crate and so
// does not inherit the crate-root allow.
#![allow(clippy::future_not_send, clippy::significant_drop_tightening)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use wavedb_core::index::BpTree;
use wavedb_core::{Id, LocalId, Result, Store, U48, Write};

/// Ready on the second poll — the minimum an executor can interleave at.
struct YieldOnce(Cell<bool>);

impl Future for YieldOnce {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0.replace(true) {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

async fn yield_once() {
    YieldOnce(Cell::new(false)).await;
}

/// `MemStore` with a switch for whether reads suspend. `YIELDS = false`
/// models `PageStore` (every future resolves on first poll); `YIELDS = true`
/// models any genuinely async backend — IndexedDB, a network store.
#[derive(Default)]
struct Backend<const YIELDS: bool>(Mutex<BTreeMap<u128, Vec<u8>>>);

// A test double, written in the trait's own (async) shape.
#[allow(clippy::unused_async_trait_impl)]
impl<const YIELDS: bool> Store for Backend<YIELDS> {
    async fn get(&self, id: Id) -> Result<Option<Vec<u8>>> {
        // The snapshot is taken here; the suspension is what happens between
        // taking it and the caller acting on it. Yielding *after* the read is
        // what a real async backend does — the value is the state at request
        // time, delivered later.
        let value = self.0.lock().unwrap().get(&id.raw()).cloned();
        if YIELDS {
            yield_once().await;
        }
        Ok(value)
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        let mut m = self.0.lock().unwrap();
        for w in batch {
            if let Write::Expect(_, id, expected) = w
                && m.get(&id.raw()) != expected.as_ref()
            {
                return Err(wavedb_core::Error::Conflict(*id));
            }
        }
        for w in batch {
            match w {
                Write::Put(id, b) => {
                    m.insert(id.raw(), b.clone());
                }
                Write::Remove(_, id) => {
                    m.remove(&id.raw());
                }
                Write::Expect(..) => {}
            }
        }
        Ok(())
    }
}

/// One collection-op shape: read the tree, plan, commit. Exactly the
/// `plan_* … store.apply` sequence every `collection_write` path performs.
async fn insert_one<S: Store>(store: &S, mut tree: BpTree, key: LocalId) {
    let batch = tree.plan_insert(store, key).await.unwrap();
    store.apply(&batch).await.unwrap();
}

/// Both keys inserted concurrently, then: how many survived?
async fn race<const YIELDS: bool>() -> (bool, bool) {
    let store = Backend::<YIELDS>::default();
    let tenant = U48::new(7).unwrap();
    let tree = BpTree::<LocalId>::create(&store, tenant).await.unwrap();

    let a = LocalId::new(100, false, 1);
    let b = LocalId::new(200, false, 2);
    // `BpTree` is `Copy`: each task carries its own handle at the same root,
    // exactly as `self.tree(pivot.current())` hands one out per op.
    futures::future::join(
        insert_one(&store, tree, a),
        insert_one(&store, tree, b),
    )
    .await;

    (
        tree.contains(&store, a).await.unwrap(),
        tree.contains(&store, b).await.unwrap(),
    )
}

#[test]
fn a_non_yielding_store_cannot_interleave_two_batches() {
    let (a, b) = futures::executor::block_on(race::<false>());
    assert!(a && b, "both inserts must survive: got a={a} b={b}");
}

#[test]
fn a_yielding_store_loses_one_of_two_concurrent_inserts() {
    let (a, b) = futures::executor::block_on(race::<true>());
    assert!(
        !(a && b),
        "expected the later batch to clobber the earlier one"
    );
}
