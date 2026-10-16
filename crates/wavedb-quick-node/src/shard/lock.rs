//! Per-owner serialisation — the brake the shard model needs.
//!
//! ## What it prevents
//!
//! `concurrent_node_clobber.rs` in `wavedb-core` states the invariant this
//! restores, and states it as a property of the **executor**, not of any
//! structure:
//!
//! > `PageStore`'s futures all resolve on first poll (its `get`/`apply` bodies
//! > are synchronous), so on a current-thread runtime a collection op never
//! > yields between its reads and its `apply` and cannot interleave with
//! > another. A `Store` that genuinely suspends — IndexedDB, or anything
//! > network-backed — breaks that, and the loss is **silent**: the record is
//! > live at its anchor and absent from the index.
//!
//! [`ShardStore`](super::ShardStore) is exactly such a store: every cache miss
//! is a message to the disk actor and a real suspension. Putting it under the
//! node without this module would ship that silent loss.
//!
//! The failure is worth spelling out, because it is not an ordering question.
//! Two inserts into one collection both read the same leaf, both compute a
//! successor from it, and the second overwrites the first's node: the first
//! record is written at its anchor and reachable from nothing. `all()` will
//! not list it, a search will not find it, and the caller was told `Ok(id)`.
//! Declaring a `#[wavedb::list]` makes it worse, not better — one more shared
//! structure to lose the update on.
//!
//! ## Why the key is the type, not the Pivot
//!
//! Ownership is per Pivot ([RFC 0064]), so a Pivot-grained lock is what this
//! wants to be. It cannot be yet: `Get`, `Update` and `Remove` carry only an
//! `Id` on the wire, so their Pivot is not known at the point the lock has to
//! be taken. A **mixed** granularity would be worse than a coarse one — an
//! `Insert` holding a Pivot key and an `Update` holding a type key do not
//! exclude each other, which is the bug with extra steps.
//!
//! So the key is `(tenant, STRUCT_HASH)`: coarser than ownership, uniform, and
//! safe. Two collections of one type serialise where they need not; different
//! types and different tenants do not. It narrows to the Pivot the moment
//! those three commands carry one, which RFC 0064 already lists as separable
//! work worth doing on its own.
//!
//! ## Why one table per thread
//!
//! Only a shard executes, and the router sends every operation on an owner to
//! the shard that owns it, keyed exactly like this lock ([`super::Router`]).
//! A shard runs on one thread, so two operations on one owner always meet on
//! one thread, and a table per thread excludes everything a table for the
//! node would. It is that thread's own state: no atomics, no `Arc`, nothing
//! shared between threads. Today a thread carries one shard, so the table
//! lives in the shard's [`Worker`](super::Worker); once a thread carries
//! several (RFC 0067) they share its one table, and memory follows the
//! threads, not the shards. An owner another thread holds is RFC 0067's
//! business (leases), not this table's.
//!
//! The table is runtime state only: it starts empty with the thread and
//! nothing of it is persisted.
//!
//! ## How a wait works: a chain of waiters
//!
//! The lock is held across the operation's awaits — a `RefCell` borrow could
//! not span one — so waiting has to be a future that suspends:
//!
//! - **Uncontended**, [`acquire`](OwnerLocks::acquire) marks the owner held
//!   and is ready at once; the [`Held`] it returns unmarks it on drop.
//! - **Contended**, the waiter takes a slot, stores its `Waker` there and
//!   joins the owner's **chain**: the owner keeps only the chain's head and
//!   tail, and each slot links to the next waiter. The task returns
//!   `Pending` and leaves the executor's run queue; the shard runs other
//!   owners' work meanwhile.
//! - **On release** the lock is **handed** to the head of the chain, and only
//!   that waiter is woken. Nobody can take it in between, nobody else is
//!   polled for nothing, and waiters are served in arrival order.
//!
//! Links are slot **indices**, not pointers — linking the waiting futures to
//! each other directly would take `unsafe`. The slots are a fixed array per
//! thread, sized at compile time: every waiter is a job, so a thread never
//! has more than [`MAX_WAITERS`](super::MAX_WAITERS) of them, and the
//! held-owner table is fixed the same way. Nothing allocates per wait and
//! nothing grows.
//!
//! A waiter dropped before it got the lock leaves the chain; one dropped after
//! the hand-off passes the lock on.
//!
//! `embassy-sync`'s `Mutex` is not used here: it keeps a single waker, so two
//! waiters on one owner wake each other in a loop until it is released.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use wavedb_core::U48;

use super::waiters::{Asked, Link, Table};

/// The unit operations serialise on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnerKey(u64);

impl OwnerKey {
    /// The key for `struct_hash` under `tenant`.
    ///
    /// The tenant and the type's 15-bit salt, concatenated: 48 + 15 bits fit.
    /// Two types sharing a salt within a tenant share a key — which can only
    /// over-serialise them, never let one owner's operations interleave.
    #[must_use]
    pub fn of(tenant: U48, struct_hash: u64) -> Self {
        Self(
            (tenant.get() << 15)
                | u64::from(wavedb_core::type_salt(struct_hash)),
        )
    }
}

/// One thread's table of held owners and their waiters. Fixed size, built
/// once when the thread starts.
pub struct OwnerLocks {
    table: RefCell<Table>,
}

impl Default for OwnerLocks {
    fn default() -> Self {
        Self::new()
    }
}

impl OwnerLocks {
    #[must_use]
    pub fn new() -> Self {
        Self {
            table: RefCell::new(Table::new()),
        }
    }

    /// Hold `key` until the returned [`Held`] drops.
    pub const fn acquire(&self, key: OwnerKey) -> Acquire<'_> {
        Acquire {
            locks: self,
            key,
            slot: None,
            done: false,
        }
    }

    /// Owners currently held, for tests and introspection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.table.borrow().held()
    }

    /// Whether no owner is held.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn release(&self, key: OwnerKey) {
        let next = self.table.borrow_mut().release(key);
        // Woken outside the borrow: a waker is foreign code.
        if let Some(waker) = next {
            waker.wake();
        }
    }
}

/// Waiting for an owner. Resolves to the [`Held`] that keeps it.
pub struct Acquire<'a> {
    locks: &'a OwnerLocks,
    key: OwnerKey,
    /// Our place in the owner's chain, once we joined it.
    slot: Link,
    /// The lock became a [`Held`]; dropping this is no longer a release.
    done: bool,
}

impl<'a> Future for Acquire<'a> {
    type Output = Held<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Held<'a>> {
        let key = self.key;
        let mut table = self.locks.table.borrow_mut();
        let held = match self.slot {
            None => match table.ask(key, cx.waker()) {
                Asked::Held => true,
                Asked::Waiting(slot) => {
                    self.slot = Some(slot);
                    false
                }
                Asked::Full => {
                    cx.waker().wake_by_ref();
                    false
                }
            },
            Some(slot) => table.check(key, slot, cx.waker()),
        };
        drop(table);
        if !held {
            return Poll::Pending;
        }
        self.slot = None;
        self.done = true;
        Poll::Ready(Held {
            locks: self.locks,
            key,
        })
    }
}

impl Drop for Acquire<'_> {
    fn drop(&mut self) {
        let Some(slot) = self.slot.filter(|_| !self.done) else {
            return;
        };
        let handed = self.locks.table.borrow_mut().leave(self.key, slot);
        // Handed the lock but gone before taking it: pass it on, or the
        // owner would stay held by nobody.
        if handed {
            self.locks.release(self.key);
        }
    }
}

/// An owner, held. Dropping it hands the owner to the next waiter.
pub struct Held<'a> {
    locks: &'a OwnerLocks,
    key: OwnerKey,
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.locks.release(self.key);
    }
}

#[cfg(test)]
mod tests {
    use super::{OwnerKey, OwnerLocks};
    use std::cell::RefCell;

    use wavedb_core::U48;

    fn key(tenant: u32, hash: u64) -> OwnerKey {
        OwnerKey::of(U48::from(tenant), hash)
    }

    #[test]
    fn the_key_separates_tenants_and_types() {
        assert_ne!(key(1, 0xAA), key(2, 0xAA), "tenant must matter");
        assert_ne!(key(1, 0xAA), key(1, 0xBB), "type must matter");
        assert_eq!(key(7, 0xCC), key(7, 0xCC), "and be stable");
    }

    /// One operation: take the lock, do two steps with a yield between them.
    /// The yield is where a real op awaits the disk actor.
    async fn op(
        locks: &OwnerLocks,
        trace: &RefCell<Vec<String>>,
        tag: &str,
        hash: u64,
    ) {
        let _held = locks.acquire(key(1, hash)).await;
        trace.borrow_mut().push(format!("{tag}1"));
        embassy_futures::yield_now().await;
        trace.borrow_mut().push(format!("{tag}2"));
    }

    /// Operations on one owner do not interleave, and run in arrival order.
    ///
    /// Without the lock the bodies interleave at the yield and the trace
    /// reads `a1 b1 c1 a2 …` — the shape in which one collection op reads a
    /// tree node another is about to overwrite.
    #[test]
    fn one_owner_runs_one_operation_at_a_time_in_arrival_order() {
        wavedb_platform::carrier::block_on(async {
            let locks = OwnerLocks::new();
            let trace = RefCell::new(Vec::new());
            embassy_futures::join::join3(
                op(&locks, &trace, "a", 0xAA),
                op(&locks, &trace, "b", 0xAA),
                op(&locks, &trace, "c", 0xAA),
            )
            .await;
            assert_eq!(trace.borrow().join(" "), "a1 a2 b1 b2 c1 c2");
            assert!(locks.is_empty(), "a released owner is forgotten");
        });
    }

    /// Different owners are not held up by each other — the lock is a brake on
    /// one owner, not on the shard.
    #[test]
    fn different_owners_run_concurrently() {
        wavedb_platform::carrier::block_on(async {
            let locks = OwnerLocks::new();
            let trace = RefCell::new(Vec::new());
            embassy_futures::join::join(
                op(&locks, &trace, "a", 0xAA),
                op(&locks, &trace, "b", 0xBB),
            )
            .await;
            // The property is *interleaving*, not a particular order: `b` must
            // start before `a` finishes.
            let seen = trace.borrow().join(" ");
            let (b_start, a_end) = (
                seen.find("b1").expect("b must run"),
                seen.find("a2").expect("a must finish"),
            );
            assert!(b_start < a_end, "distinct owners serialised: {seen}");
            assert!(locks.is_empty());
        });
    }

    /// A waiter is woken once, when the lock is handed to it — never while
    /// it waits, and never by a release meant for another. (`embassy-sync`'s
    /// `Mutex` fails this: two waiters wake each other in a loop.)
    #[test]
    fn a_release_wakes_exactly_the_next_waiter() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::task::{Context, Poll};

        use futures::task::{ArcWake, waker};

        struct Count(AtomicUsize);
        impl ArcWake for Count {
            fn wake_by_ref(this: &Arc<Self>) {
                this.0.fetch_add(1, Ordering::Relaxed);
            }
        }
        let wakes = |c: &Arc<Count>| c.0.load(Ordering::Relaxed);

        let locks = OwnerLocks::new();
        let (second, third) = (
            Arc::new(Count(AtomicUsize::new(0))),
            Arc::new(Count(AtomicUsize::new(0))),
        );
        let (w2, w3) = (waker(Arc::clone(&second)), waker(Arc::clone(&third)));
        let mut cx2 = Context::from_waker(&w2);
        let mut cx3 = Context::from_waker(&w3);

        let mut first = Box::pin(locks.acquire(key(1, 0xAA)));
        let Poll::Ready(held) = first.as_mut().poll(&mut cx2) else {
            panic!("a free owner is taken at once");
        };
        let mut b = Box::pin(locks.acquire(key(1, 0xAA)));
        let mut c = Box::pin(locks.acquire(key(1, 0xAA)));
        assert!(b.as_mut().poll(&mut cx2).is_pending());
        assert!(c.as_mut().poll(&mut cx3).is_pending());
        assert!(b.as_mut().poll(&mut cx2).is_pending(), "still held");
        assert_eq!((wakes(&second), wakes(&third)), (0, 0), "nobody woken");

        drop(held);
        assert_eq!((wakes(&second), wakes(&third)), (1, 0), "only the next");
        let Poll::Ready(held) = b.as_mut().poll(&mut cx2) else {
            panic!("handed to the second waiter");
        };
        drop(held);
        assert_eq!((wakes(&second), wakes(&third)), (1, 1), "then the third");
        assert!(c.as_mut().poll(&mut cx3).is_ready());
    }

    /// A waiter that gives up leaves the queue: the one behind it still gets
    /// the lock — whether it gave up before the hand-off or after it.
    #[test]
    fn a_waiter_that_gives_up_does_not_block_the_next() {
        wavedb_platform::carrier::block_on(async {
            let locks = OwnerLocks::new();
            let first = locks.acquire(key(1, 0xAA)).await;

            // Gives up while waiting.
            let mut quitter = Box::pin(locks.acquire(key(1, 0xAA)));
            assert!(futures::poll!(quitter.as_mut()).is_pending());
            drop(quitter);

            // Handed the lock, gone before taking it.
            let mut handed = Box::pin(locks.acquire(key(1, 0xAA)));
            assert!(futures::poll!(handed.as_mut()).is_pending());
            let mut last = Box::pin(locks.acquire(key(1, 0xAA)));
            assert!(futures::poll!(last.as_mut()).is_pending());
            drop(first); // hands to `handed`
            drop(handed); // which passes it to `last`

            let held = last.await;
            drop(held);
            assert!(locks.is_empty());
        });
    }
}
