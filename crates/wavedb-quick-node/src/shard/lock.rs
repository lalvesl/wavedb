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
//! ## Why an async mutex, and why one table for the whole node
//!
//! The per-owner lock is `tokio`'s because it must be held across the
//! operation's awaits — a `RefCell` cannot span one and a blocking mutex would
//! deadlock on it. Uncontended it is a fast path; contended it parks the task
//! and the shard runs something else, which is the point.
//!
//! The **table** is a plain `std` mutex, and the table is shared by every
//! thread that serves — the accept loop, every shard worker. That is not
//! optional. Once routing exists, two operations on one owner can be picked up
//! by two different threads; a per-thread table would give each its own lock
//! and exclude neither, which is the silent loss above with more machinery in
//! front of it. Exclusion has to be a property of the owner, so the table
//! naming owners has to be one. Its critical section is a map lookup with no
//! await inside, so blocking there costs a few nanoseconds and can never
//! deadlock.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::{Mutex, OwnedMutexGuard};
use wavedb_core::U48;

/// The unit operations serialise on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct OwnerKey(u64);

impl OwnerKey {
    /// The key for `struct_hash` under `tenant`.
    ///
    /// Concatenated rather than combined, for the reason
    /// [`super::route`] concatenates: 48 + 15 bits fit, so the key is
    /// injective and two owners can only be confused by the map's own hashing.
    #[must_use]
    pub fn of(tenant: U48, struct_hash: u64) -> Self {
        Self(
            (tenant.get() << 15)
                | u64::from(wavedb_core::type_salt(struct_hash)),
        )
    }
}

/// One table for the node — every serving thread holds the same `Arc`.
///
/// The inner `Arc<Mutex<()>>` is what `tokio`'s *owned* guard requires, and an
/// owned guard is what lets a lock be held across an operation's awaits
/// without also borrowing this table for the duration. Uncontended, an acquire
/// is one map lookup and one atomic increment against a whole operation's
/// work.
#[derive(Default)]
pub struct OwnerLocks {
    locks: StdMutex<HashMap<OwnerKey, Arc<Mutex<()>>>>,
}

impl OwnerLocks {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold `key` until the returned guard drops.
    ///
    /// The guard is `Owned` so it can be held across the operation's awaits
    /// without borrowing this table for the duration — which would make the
    /// table itself the thing that cannot be shared.
    pub async fn acquire(&self, key: OwnerKey) -> OwnedMutexGuard<()> {
        // Cloned out *before* the await: the table's own lock is never held
        // across one. Doing otherwise would stall every other owner behind
        // whichever one happened to be waiting.
        let lock = self.with(|map| {
            Arc::clone(
                map.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))),
            )
        });
        lock.lock_owned().await
    }

    /// Locks currently tracked, for tests and introspection.
    #[must_use]
    pub fn len(&self) -> usize {
        self.with(|map| map.len())
    }

    /// Whether nothing has been locked yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Run `f` under the table's lock.
    ///
    /// Poisoning is recovered rather than propagated: the map holds
    /// independent `Arc`s and no invariant spanning them, so a panic elsewhere
    /// leaves it perfectly usable — and refusing to hand out locks after one
    /// would turn an unrelated fault into a node that can no longer serve the
    /// owner it was serving.
    fn with<R>(
        &self,
        f: impl FnOnce(&mut HashMap<OwnerKey, Arc<Mutex<()>>>) -> R,
    ) -> R {
        let mut map = self
            .locks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        f(&mut map)
    }
}

/// The brake is only a brake if every serving thread shares one table.
const _: fn() = || {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<OwnerLocks>();
};

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

    /// Two operations on one owner do not interleave; the second's whole body
    /// runs after the first's.
    ///
    /// The recorded order is what the test is about. Without the lock the two
    /// bodies interleave at the yield and the trace reads `a1 b1 a2 b2` — the
    /// shape in which one collection op reads a tree node the other is about
    /// to overwrite.
    /// One operation: take the lock, do two steps with a yield between them.
    /// The yield is where a real op awaits the disk actor.
    async fn op(
        locks: &OwnerLocks,
        trace: &RefCell<Vec<String>>,
        tag: &str,
        hash: u64,
    ) {
        let _guard = locks.acquire(key(1, hash)).await;
        trace.borrow_mut().push(format!("{tag}1"));
        embassy_futures::yield_now().await;
        trace.borrow_mut().push(format!("{tag}2"));
    }

    #[test]
    fn one_owner_runs_one_operation_at_a_time() {
        wavedb_platform::carrier::block_on(async {
            let locks = OwnerLocks::new();
            let trace = RefCell::new(Vec::new());
            embassy_futures::join::join(
                op(&locks, &trace, "a", 0xAA),
                op(&locks, &trace, "b", 0xAA),
            )
            .await;
            let seen = trace.borrow().join(" ");
            assert!(
                seen == "a1 a2 b1 b2" || seen == "b1 b2 a1 a2",
                "operations interleaved: {seen}"
            );
        });
    }

    /// Different owners are not held up by each other — the lock is a brake on
    /// one collection, not on the shard.
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
            // start before `a` finishes. Which of them finishes first afterwards
            // is a wake-up ordering detail and pinning it would make this test
            // fail for a reason it is not about.
            let seen = trace.borrow().join(" ");
            let (b_start, a_end) = (
                seen.find("b1").expect("b must run"),
                seen.find("a2").expect("a must finish"),
            );
            assert!(
                b_start < a_end,
                "distinct owners serialised: {seen} — the brake is on the shard, \
             not on one collection"
            );
            assert_eq!(locks.len(), 2);
        });
    }
}
