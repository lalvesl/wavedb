//! What a worker owns: the engines of its range of data shards ([RFC 0067]
//! step 4).
//!
//! Each data shard is a whole `PageStore`, and a worker holds the ones in its
//! contiguous range **itself** — there is no disk actor in between any more.
//! An operation runs on its owner's store as a function call on the thread
//! that owns it; maintenance runs on the same thread, in bounded steps, when
//! the operations leave room ([`super::background`]).
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::cell::Cell;

use wavedb_storage::{PageStore, StorageError, StorageResult};

use crate::upkeep::Maintenance;

/// Ids one settle or checkpoint round may take before the worker looks at
/// its jobs again.
///
/// This is what makes maintenance share the thread in practice: without a
/// bound, one burst becomes one round. Unmeasured — small enough that a round
/// is short, large enough that a round's fixed cost (one window allocation,
/// one positioned write) is not paid per handful of records.
pub const SETTLE_BUDGET_IDS: usize = 4096;

/// A worker's data shards, and how busy its jobs keep it.
pub struct Owned {
    /// The first data shard in the range.
    first: usize,
    stores: Vec<PageStore>,
    /// Jobs executing right now — the "is DATA busy?" half of the
    /// maintenance valve.
    inflight: Cell<usize>,
}

/// One job's claim on [`Owned::inflight`], released however the job ends.
pub struct Busy<'a>(&'a Cell<usize>);

impl Drop for Busy<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get().saturating_sub(1));
    }
}

impl Owned {
    /// The stores of data shards `first ..` in order.
    #[must_use]
    pub const fn new(first: usize, stores: Vec<PageStore>) -> Self {
        Self {
            first,
            stores,
            inflight: Cell::new(0),
        }
    }

    /// The store of data shard `shard`, if this worker owns it.
    ///
    /// # Errors
    /// [`wavedb_core::Error::Backend`] for a shard outside the range — a
    /// routing fault, answered rather than panicked on.
    pub fn store(&self, shard: usize) -> wavedb_core::Result<&PageStore> {
        shard
            .checked_sub(self.first)
            .and_then(|i| self.stores.get(i))
            .ok_or_else(|| {
                wavedb_core::Error::Backend(format!(
                    "shard {shard} is not on this worker"
                ))
            })
    }

    /// Count a job as executing until the returned guard drops.
    pub fn busy(&self) -> Busy<'_> {
        self.inflight.set(self.inflight.get() + 1);
        Busy(&self.inflight)
    }

    /// Whether any job is executing.
    #[must_use]
    pub const fn working(&self) -> bool {
        self.inflight.get() > 0
    }

    /// Whether any store has stepped work: a queue to settle or a
    /// checkpoint open.
    #[must_use]
    pub fn owes(&self) -> bool {
        self.stores.iter().any(owes)
    }

    /// Committed journal bytes across the range — what the valve weighs.
    #[must_use]
    pub fn journal_bytes(&self) -> u64 {
        self.stores.iter().map(PageStore::journal_len).sum()
    }

    /// One bounded step on the next store from `cursor` that owes one: a
    /// round of its open checkpoint (or that checkpoint's close), else a
    /// settle round. Faults are reported, not propagated — nobody is waiting,
    /// and the journal still holds everything unsettled.
    pub fn step(&self, cursor: &mut usize) {
        let n = self.stores.len();
        let next = (0..n)
            .map(|k| (*cursor + k) % n)
            .find(|&i| self.stores.get(i).is_some_and(owes));
        let Some((i, store)) =
            next.and_then(|i| self.stores.get(i).map(|s| (i, s)))
        else {
            return;
        };
        *cursor = (i + 1) % n;
        let done = if store.is_checkpointing() {
            store.checkpoint_step(SETTLE_BUDGET_IDS).map(|_| ())
        } else {
            store.settle_step(SETTLE_BUDGET_IDS).map(|_| ())
        };
        if let Err(e) = done {
            eprintln!("worker: maintenance step failed: {e}");
        }
    }

    /// The policy's decisions for store `i`, each bounded: open a checkpoint
    /// past this range's share of the journal threshold, evict to its share
    /// of the cache budget, defragment past the extent threshold. The steps
    /// the checkpoint then owes are [`step`](Self::step)'s.
    pub fn upkeep(&self, i: usize, policy: &Maintenance, partitions: usize) {
        let Some(store) = self.stores.get(i) else {
            return;
        };
        let partitions = partitions.max(1);
        let threshold = policy.checkpoint_after_bytes
            / u64::try_from(partitions).unwrap_or(u64::MAX);
        if store.journal_len() > threshold
            && let Err(e) = store.begin_checkpoint()
        {
            eprintln!("worker: checkpoint failed: {e}");
        }
        let share = policy.cache_budget_bytes / partitions;
        store.evict_settled(share);
        if store.largest_free_extent() < policy.defrag_below_blocks
            && let Err(e) = store.defragment(policy.defrag_budget_blocks)
        {
            eprintln!("worker: defragment failed: {e}");
        }
    }

    /// How many stores the range holds.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.stores.len()
    }

    /// Whether the range holds no store at all.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.stores.is_empty()
    }

    /// Close every store cleanly — settle, checkpoint, force the retirement
    /// barrier, so a restart replays nothing — and **drop them before
    /// returning**: a caller that resumes on the answer may reopen the
    /// directories at once, and a lock still held would be `EngineBusy`.
    ///
    /// # Errors
    /// The first store's fault; every store is still attempted and dropped.
    pub fn close(self) -> StorageResult<()> {
        let mut first: Option<StorageError> = None;
        for store in self.stores {
            let closed = store
                .commit_journal()
                .and_then(|()| store.force_retirement());
            if let Err(e) = closed {
                first.get_or_insert(e);
            }
            drop(store);
        }
        first.map_or(Ok(()), Err)
    }
}

fn owes(store: &PageStore) -> bool {
    store.has_pending() || store.is_checkpointing()
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};
    use wavedb_storage::{PageStore, StoreOptions, StructStorage, open_shards};

    use super::Owned;
    use crate::upkeep::Maintenance;

    const SH: u64 = 0x0067_0004_5EED_0001;
    static SLOT: StructStorage = StructStorage::new(SH);

    fn open(dir: &std::path::Path, shards: usize) -> Vec<PageStore> {
        open_shards(dir, &[&SLOT], StoreOptions::default(), shards).unwrap()
    }

    fn put(store: &PageStore, keys: std::ops::Range<u64>) {
        block_on(async {
            for k in keys {
                let id = Id::new(k, U48::from(1u32), false, 1);
                let mut bytes = SH.to_le_bytes().to_vec();
                bytes.extend_from_slice(&k.to_le_bytes());
                store.apply(&[Write::Put(id, bytes)]).await.unwrap();
            }
        });
    }

    /// Steps visit the stores that owe one, round-robin, until none does.
    #[test]
    fn steps_settle_every_store_in_turn() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(0, open(d.path(), 3));
        for i in 0..3 {
            put(owned.store(i).unwrap(), 0..10);
        }
        assert!(owned.owes());
        let mut cursor = 0;
        let mut steps = 0;
        while owned.owes() {
            owned.step(&mut cursor);
            steps += 1;
            assert!(steps <= 3, "one bounded round each was enough");
        }
        assert_eq!(steps, 3);
    }

    /// The policy opens a checkpoint past the range's share of the threshold,
    /// and the steps then close it.
    #[test]
    fn the_policy_opens_a_checkpoint_the_steps_close() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(0, open(d.path(), 1));
        put(owned.store(0).unwrap(), 0..50);
        let policy = Maintenance {
            checkpoint_after_bytes: 1,
            ..Maintenance::default()
        };
        owned.upkeep(0, &policy, 1);
        assert!(owned.store(0).unwrap().is_checkpointing());
        let mut cursor = 0;
        while owned.owes() {
            owned.step(&mut cursor);
        }
        assert!(!owned.store(0).unwrap().is_checkpointing());
    }

    /// A shard outside the range is a refusal, not a panic: routing put the
    /// operation on the wrong worker.
    #[test]
    fn a_shard_outside_the_range_is_refused() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(4, open(d.path(), 2));
        assert!(owned.store(4).is_ok());
        assert!(owned.store(5).is_ok());
        assert!(owned.store(3).is_err());
        assert!(owned.store(6).is_err());
    }

    /// `close` drops the stores before it returns: the directory reopens at
    /// once, with nothing to replay.
    #[test]
    fn close_releases_the_stores_with_nothing_to_replay() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(0, open(d.path(), 2));
        put(owned.store(1).unwrap(), 0..20);
        owned.close().unwrap();
        let reopened = open(d.path(), 2);
        assert!(reopened.iter().all(|s| !s.has_pending()));
    }

    #[test]
    fn a_busy_guard_counts_while_it_lives() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(0, open(d.path(), 1));
        assert!(!owned.working());
        let busy = owned.busy();
        assert!(owned.working());
        drop(busy);
        assert!(!owned.working());
    }
}
