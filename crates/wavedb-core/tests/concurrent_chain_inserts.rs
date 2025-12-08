//! Do two concurrently planned inserts into one chain both survive?
//!
//! They used to not, on a store that suspends: a chain kept its entries in
//! segments, an insert read its segment and wrote it back whole, and two
//! inserts planned from the same read each wrote the segment without the
//! other's entry — the record live at its anchor and absent from the index.
//! `PageStore` hid it only because its futures resolve on first poll, so a
//! current-thread runtime never interleaved them.
//!
//! A chain is now a key range of its lane (RFC 0078 §7) and an insert is one
//! blind `Write::Set`: there is no shared value for two batches to race on.
//! Two backends, identical but for whether reads suspend, driven by the same
//! pair of interleaved inserts.

// The engine's futures are deliberately non-`Send` (current-thread
// executor); an integration test is its own crate and so does not inherit
// the crate-root allow.
#![allow(clippy::future_not_send, clippy::significant_drop_tightening)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};

use wavedb_core::index::{Chain, Lane};
use wavedb_core::store::{Entry, Span, read_count, read_span};
use wavedb_core::{Id, LocalId, Result, SecKey, Store, U48, Write};

/// The type the chain's lane belongs to.
const TYPE_HASH: u64 = 0x0C10_BBE2_0000_0001;

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

/// An in-memory store of lanes with a switch for whether reads suspend.
/// `YIELDS = false` models `PageStore` (every future resolves on first poll);
/// `YIELDS = true` models any genuinely async backend — IndexedDB, a
/// network store.
#[derive(Default)]
struct Backend<const YIELDS: bool>(Mutex<Lanes>);

/// Every ordered lane, by lane hash.
type Lanes = BTreeMap<u64, BTreeMap<Vec<u8>, Vec<u8>>>;

// A test double, written in the trait's own (async) shape.
#[allow(clippy::unused_async_trait_impl)]
impl<const YIELDS: bool> Store for Backend<YIELDS> {
    async fn get(&self, _: Id) -> Result<Option<Vec<u8>>> {
        Ok(None)
    }

    async fn scan(&self, lane: u64, span: &Span) -> Result<Vec<Entry>> {
        // The snapshot is taken here; the suspension is what happens between
        // taking it and the caller acting on it — the value is the state at
        // request time, delivered later.
        let found = self
            .0
            .lock()
            .unwrap()
            .get(&lane)
            .map_or_else(Vec::new, |l| read_span(l, span));
        if YIELDS {
            yield_once().await;
        }
        Ok(found)
    }

    async fn count(
        &self,
        lane: u64,
        lo: &[u8],
        hi: Option<&[u8]>,
    ) -> Result<u64> {
        let lanes = self.0.lock().unwrap();
        Ok(lanes.get(&lane).map_or(0, |l| read_count(l, lo, hi)))
    }

    async fn apply(&self, batch: &[Write]) -> Result<()> {
        let mut lanes = self.0.lock().unwrap();
        for w in batch {
            match w {
                Write::Set(lane, key, value) => {
                    lanes
                        .entry(*lane)
                        .or_default()
                        .insert(key.clone(), value.clone());
                }
                Write::Unset(lane, key) => {
                    if let Some(l) = lanes.get_mut(lane) {
                        l.remove(key);
                    }
                }
                Write::Put(..) | Write::Remove(..) | Write::Expect(..) => {}
            }
        }
        Ok(())
    }
}

/// One collection-op shape: read, plan, commit — the read the floor every
/// insert mints above, then the insert.
async fn insert_one<S: Store>(store: &S, chain: Chain<()>, key: SecKey) {
    chain.last_key(store).await.unwrap();
    store.apply(&[chain.insert(&key, &())]).await.unwrap();
}

fn key(value: u64) -> SecKey {
    SecKey {
        field: value.to_be_bytes().to_vec(),
        rec: LocalId::new(value, false, 1),
    }
}

/// Both keys inserted concurrently, then: how many survived?
async fn race<const YIELDS: bool>() -> (bool, bool) {
    let store = Backend::<YIELDS>::default();
    let owner = Id::new(1, U48::new(7).unwrap(), false, 0);
    let chain = Chain::<()>::at(owner, TYPE_HASH, Lane::Recency, 0);
    let (a, b) = (key(100), key(200));
    futures::future::join(
        insert_one(&store, chain, a.clone()),
        insert_one(&store, chain, b.clone()),
    )
    .await;
    (
        chain.holds(&store, &a).await.unwrap(),
        chain.holds(&store, &b).await.unwrap(),
    )
}

#[test]
fn a_non_yielding_store_keeps_both_concurrent_inserts() {
    let (a, b) = futures::executor::block_on(race::<false>());
    assert!(a && b, "both inserts must survive: got a={a} b={b}");
}

#[test]
fn a_yielding_store_keeps_both_concurrent_inserts() {
    let (a, b) = futures::executor::block_on(race::<true>());
    assert!(a && b, "both inserts must survive: got a={a} b={b}");
}
