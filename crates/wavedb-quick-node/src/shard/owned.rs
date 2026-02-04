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
use std::time::{Duration, Instant};

use wavedb_storage::{Compacting, PageStore, StorageError, StorageResult};

use crate::upkeep::{Armed, Compaction, Maintenance};

use super::object_policy::{evict_past_ceiling, upload_due};

/// Ids one settle round may take before the worker looks at its jobs
/// again.
///
/// This is what makes maintenance share the thread in practice: without a
/// bound, one burst becomes one round. Unmeasured — small enough that a round
/// is short, large enough that a round's fixed cost (one window allocation,
/// one positioned write) is not paid per handful of records.
pub const SETTLE_BUDGET_IDS: usize = 4096;

/// Evicted buckets one upkeep tick brings back to disk at most — each is a
/// request or two to the object store and one window write.
pub const RESTORES_PER_TICK: usize = 16;

/// How often a store's object store is weighed for compaction (RFC 0070
/// §8): weighing walks every remote directory.
pub const REMOTE_CHECK_EVERY: Duration = Duration::from_secs(60);

/// A worker's data shards, and how busy its jobs keep it.
pub struct Owned {
    /// The first data shard in the range.
    first: usize,
    stores: Vec<PageStore>,
    /// Each store's compaction triggers, by index.
    armed: Vec<Cell<Armed>>,
    /// Each store's object-store compaction triggers, and when they were
    /// last weighed.
    remote_armed: Vec<Cell<(Armed, Option<Instant>)>>,
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
    pub fn new(first: usize, stores: Vec<PageStore>) -> Self {
        Self {
            first,
            armed: stores.iter().map(|_| Cell::default()).collect(),
            remote_armed: stores.iter().map(|_| Cell::default()).collect(),
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

    /// Whether a durable write waits on an open batch of the range: its
    /// steps then go ahead of the jobs, whatever the valve says (RFC 0072).
    #[must_use]
    pub fn urgent(&self) -> bool {
        self.stores.iter().any(PageStore::is_awaited)
    }

    /// Whether any job is executing.
    #[must_use]
    pub const fn working(&self) -> bool {
        self.inflight.get() > 0
    }

    /// Whether any store has stepped work: a settle cycle open.
    #[must_use]
    pub fn owes(&self) -> bool {
        self.stores.iter().any(owes)
    }

    /// Bytes of committed writes not settled yet across the range — what
    /// the valve weighs.
    #[must_use]
    pub fn unsettled_bytes(&self) -> u64 {
        self.stores.iter().map(PageStore::unsettled_bytes).sum()
    }

    /// One bounded step on the next store from `cursor` with a settle cycle
    /// open: a round of it, or its end. Faults are reported, not propagated:
    /// a durable write waiting on the batch hears of it from the store.
    pub async fn step(&self, cursor: &mut usize) {
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
        if let Err(e) = store.settle_step(SETTLE_BUDGET_IDS).await {
            eprintln!("worker: maintenance step failed: {e}");
        }
    }

    /// The policy's decisions for store `i`, each bounded: open a settle
    /// cycle (RFC 0072 §2 — a tick with nothing to settle opens none), evict
    /// to its share of the cache budget, and compact when a trigger says so
    /// (RFC 0069) — one pass per tick until it ends. The steps a cycle then
    /// owes, a compaction's sealing ones included, are
    /// [`step`](Self::step)'s.
    pub async fn upkeep(
        &self,
        i: usize,
        policy: &Maintenance,
        partitions: usize,
    ) {
        let Some(store) = self.stores.get(i) else {
            return;
        };
        let partitions = partitions.max(1);
        if let Err(e) = store.begin_cycle() {
            eprintln!("worker: settle failed: {e}");
        }
        let share = policy.cache_budget_bytes / partitions;
        store.evict_settled(share);
        self.compact(i, store, policy, partitions).await;
    }

    /// Start or continue store `i`'s compaction, if its triggers say so.
    async fn compact(
        &self,
        i: usize,
        store: &PageStore,
        maintenance: &Maintenance,
        partitions: usize,
    ) {
        let policy = &maintenance.compaction;
        let Some(cell) = self.armed.get(i) else {
            return;
        };
        let mut armed = cell.get();
        if !store.is_compacting() {
            evict_past_ceiling(store, maintenance, partitions);
        }
        let now = wavedb_platform::time::unix_secs();
        if store.is_compacting()
            || policy.due(&mut armed, store.space(), partitions, now)
        {
            match store.compact_pass(policy.budget_blocks).await {
                Ok(Compacting::Done { .. }) => {
                    Compaction::finished(&mut armed, store.space());
                }
                Ok(_) => {}
                Err(e) => eprintln!("worker: compaction failed: {e}"),
            }
        }
        cell.set(armed);
    }

    /// Store `i`'s exchange with its object store (RFC 0070): bring back
    /// the evicted buckets reads went looking for, a few per tick, then
    /// upload once the store holds back the policy's bytes, or once its
    /// oldest write has waited the policy's interval — in uploads of a few
    /// files each — and compact the object store when its triggers say so.
    /// The jobs keep the thread while it awaits the object store; a fault is
    /// reported, and what it left undone stays for the next tick.
    pub async fn object_upkeep(
        &self,
        i: usize,
        policy: &Maintenance,
        partitions: usize,
    ) {
        let Some(store) = self.stores.get(i) else {
            return;
        };
        if let Err(e) = store.restore_wanted(RESTORES_PER_TICK).await {
            eprintln!("worker: restore failed: {e}");
        }
        while upload_due(store, policy) {
            match store.upload().await {
                Ok(done) if done.pages > 0 => {}
                Ok(_) => return, // another upload has it, or nothing moved
                Err(e) => {
                    eprintln!("worker: upload failed: {e}");
                    return;
                }
            }
        }
        self.compact_remote(i, store, policy, partitions).await;
    }

    /// Compact store `i`'s object store when the policy's triggers say so,
    /// weighed against its remote space (RFC 0070 §8) once per
    /// [`REMOTE_CHECK_EVERY`]; a size trigger needs
    /// [`object_compact_free_bytes`](Maintenance::object_compact_free_bytes)
    /// free, and as much again after each compaction.
    async fn compact_remote(
        &self,
        i: usize,
        store: &PageStore,
        maintenance: &Maintenance,
        partitions: usize,
    ) {
        let Some(cell) = self.remote_armed.get(i) else {
            return;
        };
        let (mut armed, checked) = cell.get();
        if checked.is_some_and(|at| at.elapsed() < REMOTE_CHECK_EVERY) {
            return;
        }
        let policy = Compaction {
            budget_blocks: maintenance.object_compact_free_bytes
                / wavedb_storage::BLOCK_SIZE as u64,
            ..maintenance.compaction
        };
        let now = wavedb_platform::time::unix_secs();
        if policy.due(&mut armed, store.remote_space(), partitions, now) {
            match store.compact_remote().await {
                Ok(_) => Compaction::finished(&mut armed, store.remote_space()),
                Err(e) => {
                    eprintln!("worker: object store compaction failed: {e}");
                }
            }
        }
        cell.set((armed, Some(Instant::now())));
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

    /// Close every store cleanly — settle everything into a durable batch;
    /// in object mode, upload every write, since nothing else keeps it —
    /// and **drop them before
    /// returning**: a caller that resumes on the answer may reopen the
    /// directories at once, and a lock still held would be `EngineBusy`.
    ///
    /// # Errors
    /// The first store's fault; every store is still attempted and dropped.
    pub async fn close(self) -> StorageResult<()> {
        let mut first: Option<StorageError> = None;
        for store in self.stores {
            let uploaded = if store.is_object_mode() {
                store.upload_all().await
            } else {
                Ok(())
            };
            let closed = uploaded.and_then(|()| store.drain());
            if let Err(e) = closed {
                first.get_or_insert(e);
            }
            drop(store);
        }
        first.map_or(Ok(()), Err)
    }
}

fn owes(store: &PageStore) -> bool {
    store.is_cycling()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};
    use wavedb_storage::{PageStore, StoreOptions, StructStorage, open_shards};

    use super::Owned;
    use crate::upkeep::{Compaction, Maintenance};

    const SH: u64 = 0x0067_0004_5EED_0001;
    static SLOT: StructStorage = StructStorage::new(SH);

    fn open(dir: &std::path::Path, shards: usize) -> Vec<PageStore> {
        open_shards(dir, &[&SLOT], StoreOptions::default(), shards).unwrap()
    }

    fn id(k: u64) -> Id {
        Id::new(k, U48::from(1u32), false, 1)
    }

    fn put(store: &PageStore, keys: std::ops::Range<u64>) {
        block_on(async {
            for k in keys {
                let id = id(k);
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
            block_on(owned.upkeep(i, &Maintenance::default(), 3)); // the tick
        }
        assert!(owned.owes());
        let mut cursor = 0;
        let mut steps = 0;
        while owned.owes() {
            block_on(owned.step(&mut cursor));
            steps += 1;
            assert!(steps <= 3, "one bounded round each was enough");
        }
        assert_eq!(steps, 3);
    }

    /// The tick opens a settle cycle over what was written, the steps end
    /// it; a tick with nothing written opens none.
    #[test]
    fn the_tick_opens_a_cycle_the_steps_end() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(0, open(d.path(), 1));
        let policy = Maintenance::default();
        block_on(owned.upkeep(0, &policy, 1));
        assert!(!owned.owes(), "nothing written, nothing opened");
        put(owned.store(0).unwrap(), 0..50);
        block_on(owned.upkeep(0, &policy, 1));
        assert!(owned.store(0).unwrap().is_cycling());
        let mut cursor = 0;
        while owned.owes() {
            block_on(owned.step(&mut cursor));
        }
        assert!(!owned.store(0).unwrap().has_pending());
    }

    /// A durable write waiting on the open batch makes it urgent, whatever
    /// the valve would say of the bytes behind it.
    #[test]
    fn a_durable_write_waiting_is_urgent() {
        let d = tempfile::tempdir().unwrap();
        let mut stores = open(d.path(), 1);
        let barrier = wavedb_storage::Barrier::spawn("urgent-test").unwrap();
        stores[0].use_barrier(barrier);
        let owned = Owned::new(0, stores);
        let store = owned.store(0).unwrap();
        assert!(!owned.urgent());
        let batch = [Write::Put(id(1), SH.to_le_bytes().to_vec())];
        let mut durable = Box::pin(store.apply_durable(&batch));
        let _ = futures::FutureExt::now_or_never(&mut durable);
        block_on(owned.upkeep(0, &Maintenance::default(), 1));
        assert!(owned.urgent(), "the open batch carries a waiter");
    }

    /// A met trigger starts a compaction, and the ticks carry it to its end:
    /// a pass per upkeep, the batches it waits on ended by the steps.
    #[test]
    fn a_met_trigger_compacts_the_store_to_its_end() {
        let d = tempfile::tempdir().unwrap();
        let owned = Owned::new(0, open(d.path(), 1));
        let store = owned.store(0).unwrap();
        block_on(async {
            for k in 0..600u64 {
                let mut bytes = SH.to_le_bytes().to_vec();
                let mut x = k ^ 0x9E37_79B9_7F4A_7C15;
                for _ in 0..128 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    bytes.extend_from_slice(&x.to_le_bytes());
                }
                store.apply(&[Write::Put(id(k), bytes)]).await.unwrap();
            }
            store.drain().unwrap();
            for k in (0..600u64).filter(|k| k % 3 != 0) {
                store.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
            }
        });
        store.drain().unwrap();
        let before = store.space().file_blocks;
        let policy = Maintenance {
            compaction: Compaction {
                daily_at: None,
                above_bytes: None,
                above_free_percent: Some(20),
                budget_blocks: 8,
            },
            ..Maintenance::default()
        };
        let mut cursor = 0;
        block_on(owned.upkeep(0, &policy, 1));
        assert!(store.is_compacting(), "the free share is past 20%");
        for _ in 0..1_000 {
            block_on(owned.upkeep(0, &policy, 1));
            while owned.owes() {
                block_on(owned.step(&mut cursor));
            }
            if !store.is_compacting() {
                break;
            }
        }
        assert!(!store.is_compacting(), "it ended");
        assert!(store.space().file_blocks < before, "and the file shrank");
        block_on(owned.upkeep(0, &policy, 1));
        assert!(!store.is_compacting(), "nothing new to give back");
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
        wavedb_platform::carrier::block_on(owned.close()).unwrap();
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
    /// An object-backed store uploads what it settled once it holds back
    /// the policy's threshold, and not before.
    #[test]
    fn an_object_backed_store_uploads_past_its_threshold() {
        let d = tempfile::tempdir().unwrap();
        let server = wavedb_platform::s3::test_server::TestServer::start(
            &d.path().join("objects"),
        );
        let options = StoreOptions {
            remote: Some(wavedb_storage::Remote::new(server.bucket(), "")),
            ..StoreOptions::default()
        };
        let stores =
            open_shards(d.path().join("db"), &[&SLOT], options, 1).unwrap();
        let owned = Owned::new(0, stores);
        let store = owned.store(0).unwrap();
        put(store, 0..200);
        store.drain().unwrap();
        let pending = store.pending_upload_bytes();
        assert!(pending > 0);

        let policy = Maintenance {
            upload_after_bytes: pending + 1,
            upload_interval: None,
            ..Maintenance::default()
        };
        wavedb_platform::carrier::block_on(owned.object_upkeep(0, &policy, 1));
        assert_eq!(
            store.pending_upload_bytes(),
            pending,
            "under the threshold"
        );

        let policy = Maintenance {
            upload_after_bytes: pending,
            ..Maintenance::default()
        };
        wavedb_platform::carrier::block_on(owned.object_upkeep(0, &policy, 1));
        assert_eq!(store.pending_upload_bytes(), 0);
        let got =
            wavedb_platform::carrier::block_on(store.read_uploaded(SH, id(7)));
        let mut want = SH.to_le_bytes().to_vec();
        want.extend_from_slice(&7u64.to_le_bytes());
        assert_eq!(got.unwrap(), Some(want));

        // Past the ceiling, the uploaded pages leave the disk and the
        // compaction it triggers cuts the room out of the file; the records
        // still read back, from the object store.
        assert!(store.is_resident(SH, id(7)));
        let policy = Maintenance {
            compaction: Compaction {
                above_bytes: Some(4096),
                above_free_percent: None,
                daily_at: None,
                budget_blocks: 1,
            },
            ..Maintenance::default()
        };
        block_on(owned.upkeep(0, &policy, 1));
        assert!(
            !store.is_resident(SH, id(7)),
            "the uploaded pages left the disk"
        );
        assert!(store.is_compacting(), "and the ceiling compacts the file");
        let got = wavedb_platform::carrier::block_on(store.get_of(SH, id(7)));
        assert!(got.unwrap().is_some());
    }

    /// In object mode the interval is the durability window: a store holding
    /// far less than a file uploads once its oldest write has waited it, and
    /// a close uploads whatever is left — so a reopen finds every write.
    #[test]
    fn an_object_mode_store_uploads_on_its_interval_and_at_close() {
        let d = tempfile::tempdir().unwrap();
        let server = wavedb_platform::s3::test_server::TestServer::start(
            &d.path().join("objects"),
        );
        let options = || StoreOptions {
            remote: Some(wavedb_storage::Remote::new(server.bucket(), "")),
            ..StoreOptions::default()
        };
        let open = || {
            wavedb_storage::open_object_shards(&[&SLOT], options(), 1, 1)
                .unwrap()
        };
        let owned = Owned::new(0, open());
        let store = owned.store(0).unwrap();
        put(store, 0..20);
        let waiting = Maintenance {
            upload_interval: Some(Duration::from_secs(3_600)),
            ..Maintenance::default()
        };
        wavedb_platform::carrier::block_on(owned.object_upkeep(0, &waiting, 1));
        assert!(store.unuploaded_for().is_some(), "not due yet");
        let due = Maintenance {
            upload_interval: Some(Duration::ZERO),
            ..Maintenance::default()
        };
        wavedb_platform::carrier::block_on(owned.object_upkeep(0, &due, 1));
        assert_eq!(store.unuploaded_for(), None, "the interval uploaded it");

        put(store, 20..40);
        wavedb_platform::carrier::block_on(owned.close()).unwrap();
        let reopened = open();
        for k in 0..40 {
            let got = wavedb_platform::carrier::block_on(
                reopened[0].get_of(SH, id(k)),
            );
            assert!(got.unwrap().is_some(), "record {k}");
        }
    }

    /// A met trigger compacts the object store from the upkeep (RFC 0070
    /// §8): what earlier uploads left dead goes, and the records still read
    /// back from the object store.
    #[test]
    fn a_met_trigger_compacts_the_object_store() {
        let d = tempfile::tempdir().unwrap();
        let server = wavedb_platform::s3::test_server::TestServer::start(
            &d.path().join("objects"),
        );
        let options = StoreOptions {
            remote: Some(wavedb_storage::Remote::new(server.bucket(), "")),
            ..StoreOptions::default()
        };
        let stores =
            wavedb_storage::open_object_shards(&[&SLOT], options, 1, 1)
                .unwrap();
        let owned = Owned::new(0, stores);
        let store = owned.store(0).unwrap();
        for _ in 0..3 {
            put(store, 0..200);
            wavedb_platform::carrier::block_on(store.upload_all()).unwrap();
        }
        let before = store.remote_space().file_blocks;
        let policy = Maintenance {
            compaction: Compaction {
                above_bytes: None,
                above_free_percent: Some(1),
                daily_at: None,
                budget_blocks: 1,
            },
            upload_interval: None,
            object_compact_free_bytes: 4096,
            ..Maintenance::default()
        };
        wavedb_platform::carrier::block_on(owned.object_upkeep(0, &policy, 1));
        let after = store.remote_space().file_blocks;
        assert!(after < before, "{before} → {after} blocks");
        store.evict_cold(0).unwrap();
        let got = wavedb_platform::carrier::block_on(store.get_of(SH, id(7)));
        assert!(got.unwrap().is_some());
    }
}
