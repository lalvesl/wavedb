//! WaveDB on the [`Driver`] seam — the engine in the consumer's own thread.
//!
//! ## Why the factory hands over a `PageStore` rather than a built engine
//!
//! One store per consumer: a data shard opened twice is `EngineBusy`. So the
//! factory holds the row's stores — one on `wavedb/single`, one per consumer
//! on `wavedb/multi` — each behind a `Mutex<Option<_>>` slot, and consumer
//! `i`'s `build` **takes** store `i`. A second claim on a store is refused
//! rather than silently sharing.
//!
//! ## How `wavedb/multi` partitions one dataset
//!
//! A collection is indivisible — its chain segments and index nodes belong
//! to the Pivot's owner — so the multi row does what a node does with many
//! owners: **one `Thing` collection per consumer**, each on that consumer's
//! own data shard, and row `n` belongs to consumer `n % N`. Every consumer
//! mints its own Pivot; the dataset, the operations and their order per
//! row are the single row's.
//!
//! The engine is then constructed *on the consumer's thread*, which is the
//! constraint the whole seam is shaped by: an engine built on one thread
//! stays on it.
//!
//! ## What is inside the timed window, and what is not
//!
//! `LocalHandle::new` and `Thing::collection` are built per operation rather
//! than held: both borrow the store, so a struct holding store and handle
//! together would be self-referential — the same wall the SQLite driver's
//! prepared statements hit. Both are a reference and a couple of integers, so
//! the cost is a stack write, not a lookup.
//!
//! The minted-id vector is pushed inside the window and pre-reserved at build
//! time so the push can never reallocate. Maintenance is the opposite: it runs
//! in [`Driver::after_op`], outside the window, because it is this bracket's
//! counterpart to a server's background threads.

use std::sync::Mutex;
use std::time::Instant;

use futures::executor::block_on;
use wavedb_core::{Id, LocalHandle, U48};
use wavedb_storage::PageStore;

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;
use crate::schema::{Thing, ThingPivotId};
use crate::seed::TENANT;
use crate::systems::engine::Engineish;

/// Building one engine from the process's single store.
///
/// One implementation — a `PageStore`, on both rows: the multi row is the
/// same engine, one per consumer — and no `dyn`: a row monomorphises to its
/// concrete engine.
pub trait FromStore: Engineish + Sized {
    fn from_store(store: PageStore) -> Result<Self, String>;
}

impl FromStore for PageStore {
    fn from_store(store: Self) -> Result<Self, String> {
        Ok(store)
    }
}

/// What a consumer needs to build its engine. `Send`: a `PageStore` crosses,
/// the engine made from it does not have to.
pub struct Factory<E> {
    /// Consumer `i`'s store, until it takes it.
    stores: Mutex<Vec<Option<PageStore>>>,
    /// How many consumers, which is how many stores the row opened.
    consumers: usize,
    /// The anchors a seeded row was filled with, and its Pivot. `None` means
    /// the row inserts its own.
    seeded: Option<(Vec<Id>, ThingPivotId)>,
    /// Pre-reserved so an insert's `ids.push` can never reallocate inside a
    /// timed window.
    rows: usize,
    /// Whether every write is made durable before it returns (RFC 0072).
    durable: bool,
    engine: std::marker::PhantomData<fn() -> E>,
}

impl<E> Factory<E> {
    #[must_use]
    pub fn new(
        stores: Vec<PageStore>,
        seeded: Option<(Vec<Id>, ThingPivotId)>,
        rows: usize,
        durable: bool,
    ) -> Self {
        Self {
            consumers: stores.len().max(1),
            stores: Mutex::new(stores.into_iter().map(Some).collect()),
            seeded,
            rows,
            durable,
            engine: std::marker::PhantomData,
        }
    }
}

pub struct WavedbDriver<E: Engineish> {
    engine: E,
    pivot: ThingPivotId,
    /// Minted anchors of this consumer's rows, row `n` at `n / consumers`.
    /// A NonUnique anchor is minted from the clock at insert, so it cannot
    /// be recomputed from the seed — this table is how `n` becomes an `Id`.
    ids: Vec<Id>,
    /// How many consumers share the dataset (row `n` is consumer
    /// `n % consumers`'s).
    consumers: u64,
    /// Whether every write is made durable before it returns.
    durable: bool,
    /// When the store was last settled.
    settled: Instant,
    /// Whether the last op wrote: only a write can leave work to settle or
    /// records to evict.
    wrote: bool,
}

// No `Send` bound on `E`: the engine is built on the consumer's thread and
// never crosses one. `Factory<E>` is `Send` regardless, because it holds a `PageStore` and a `PhantomData<fn() -> E>` —
// a phantom that produces `E` rather than containing one.
impl<E> DriverFactory for Factory<E>
where
    E: Engineish + FromStore,
{
    type Driver = WavedbDriver<E>;

    /// One per store: a collection is indivisible, so each consumer owns a
    /// collection of its own on a data shard of its own.
    fn consumers(&self) -> usize {
        self.consumers
    }

    fn build(&self, shard: usize) -> Result<WavedbDriver<E>, String> {
        if self.seeded.is_some() && self.consumers > 1 {
            return Err("a seed is one collection on one store: a seeded \
                        micro row runs one consumer"
                .into());
        }
        let store = self
            .stores
            .lock()
            .map_err(|_| "the store mutex was poisoned".to_string())?
            .get_mut(shard)
            .and_then(Option::take)
            .ok_or_else(|| {
                format!(
                    "consumer {shard} asked for a second store, or one not \
                     its own: each consumer owns exactly one"
                )
            })?;
        // On this thread: the engine never has to cross one.
        let engine = E::from_store(store)?;

        let (ids, pivot) = match &self.seeded {
            Some((ids, pivot)) => (ids.clone(), *pivot),
            None => {
                let db = LocalHandle::new(engine.store(), U48::from(TENANT));
                let pivot = block_on(Thing::create_pivot(&db))
                    .map_err(|e| format!("create_pivot: {e}"))?;
                (Vec::with_capacity(self.rows / self.consumers + 1), pivot)
            }
        };
        Ok(WavedbDriver {
            engine,
            pivot,
            ids,
            consumers: self.consumers as u64,
            durable: self.durable,
            settled: Instant::now(),
            wrote: false,
        })
    }

    /// Row `n` is consumer `n % N`'s: its collection, its store, its thread.
    fn route(&self, op: &MicroOp) -> usize {
        (op.partition_key() % self.consumers as u64) as usize
    }
}

impl<E: Engineish> WavedbDriver<E> {
    /// The anchor for dataset row `n`.
    fn id_of(&self, n: u64) -> Result<Id, String> {
        self.ids
            .get((n / self.consumers) as usize)
            .copied()
            .ok_or_else(|| format!("row {n} was never inserted"))
    }
}

impl<E: Engineish> Driver for WavedbDriver<E> {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        let db = LocalHandle::new(self.engine.store(), U48::from(TENANT));
        let col = Thing::collection(self.pivot);
        self.wrote = !matches!(op, MicroOp::Read { .. });
        match op {
            // A durable row writes through the `_durable` twin (RFC 0072
            // §12): it returns once its batch is durable — on a store nobody
            // ticks, settled and synced on the spot, inside the timed window.
            MicroOp::Insert { n, row } => {
                let id = if self.durable {
                    block_on(col.insert_durable(&db, &row))
                } else {
                    block_on(col.insert(&db, &row))
                }
                .map_err(|e| format!("insert {n}: {e}"))?;
                // Inserts arrive in row order, so this consumer's rows land
                // at `n / N` — checked, since a gap would misfile every read.
                debug_assert_eq!(self.ids.len() as u64, n / self.consumers);
                self.ids.push(id);
            }
            MicroOp::Read { n } => {
                let id = self.id_of(n)?;
                let got = block_on(col.get(&db, id))
                    .map_err(|e| format!("get {n}: {e}"))?;
                // A `get` that found nothing is a fast operation and a wrong
                // one.
                if got.is_none() {
                    return Err(format!("row {n} came back empty"));
                }
            }
            MicroOp::Update { n, row } => {
                let id = self.id_of(n)?;
                if self.durable {
                    block_on(col.save_durable(&db, id, &row))
                } else {
                    block_on(col.save(&db, id, &row))
                }
                .map_err(|e| format!("save {n}: {e}"))?;
            }
        }
        Ok(())
    }

    /// After a write, never a read — a read adds nothing to settle and
    /// nothing to evict. A durable write has settled already, so it only
    /// evicts; a relaxed one settles (never waiting for the `fsync`, which
    /// a helper takes), then evicts, once the window has
    /// elapsed (a node's tick, outside the timed window) or once what is
    /// not settled has passed the store's pressure line (a node's valve).
    /// Both evict to the store's budget: its data shard's share of the
    /// database's.
    fn after_op(&mut self) -> Result<(), String> {
        if !std::mem::take(&mut self.wrote) {
            return Ok(());
        }
        if !self.durable
            && (self.settled.elapsed() >= crate::RELAXED_WINDOW
                || self.engine.under_pressure())
        {
            self.engine.settle()?;
            self.settled = Instant::now();
        }
        self.engine.evict_to_budget();
        Ok(())
    }

    /// Quiesce before the reads, and empty the cache before the cold ones.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        self.engine.probe_mark(done);
        match done {
            // Settle, so the read phase does not measure the insert phase's
            // deferred work.
            "insert" => self.engine.drain(),
            // Empty the engine's own cache — the counterpart of reopening the
            // SQLite connection. The OS page cache stays warm on both sides.
            "read_hot" => {
                self.engine.evict(0);
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Settle and release, in that order.
    ///
    /// Required rather than tidy: the directory's lock has to be free before
    /// anything opens the same directory again, and a footprint read over
    /// an undrained settle queue misses what is still in RAM.
    fn close(self) -> Result<(), String> {
        self.engine.probe_mark("last");
        self.engine.drain()?;
        self.engine.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use wavedb_storage::{PageStore, StoreOptions};

    use super::{Factory, FromStore};
    use crate::harness::micro::MicroWorkload;
    use crate::harness::{DriverFactory, run};
    use crate::schema::Thing;

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "wavedb-bench-engine-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("scratch");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn open(dir: &PathBuf) -> PageStore {
        PageStore::open_with(
            dir,
            &Thing::storage_entries(),
            StoreOptions::default(),
        )
        .expect("open")
    }

    fn workload() -> MicroWorkload {
        MicroWorkload {
            rows: 120,
            reads: 60,
            updates: 60,
            seed: 42,
            seeded: false,
        }
    }

    fn run_one<E>(scratch: &Scratch) -> Vec<crate::harness::PhaseResult>
    where
        E: crate::systems::engine::Engineish + FromStore,
    {
        let factory: Factory<E> =
            Factory::new(vec![open(&scratch.0)], None, 120, false);
        run(&factory, workload()).expect("run")
    }

    /// The single-thread row, end to end: insert, read, reopen-equivalent,
    /// update, all through the seam.
    #[test]
    fn a_direct_micro_row_runs_through_the_harness() {
        let scratch = Scratch::new();
        let phases = run_one::<PageStore>(&scratch);

        assert_eq!(phases.len(), 4);
        assert_eq!(phases[0].samples.len(), 120, "insert");
        assert_eq!(phases[1].samples.len(), 60, "read_hot");
        assert_eq!(phases[3].samples.len(), 60, "update");
        assert!(phases.iter().all(|p| p.wall_ns > 0));
    }

    /// A relaxed row settles on pressure, not only on its window: with a
    /// record budget a few inserts fill, what is unsettled never stays past
    /// the store's line once the op is done — long before a 1 s window
    /// elapses — and a read leaves the store as it found it.
    #[test]
    fn a_relaxed_row_settles_on_pressure_and_never_after_a_read() {
        use crate::harness::Driver as _;
        use crate::plan::op::MicroOp;
        use crate::schema::thing;

        let scratch = Scratch::new();
        let options = StoreOptions {
            record_cache_bytes: 64 << 10,
            ..StoreOptions::default()
        };
        let line = options.pressure_bytes();
        let store = PageStore::open_with(
            &scratch.0,
            &Thing::storage_entries(),
            options,
        )
        .expect("open");
        let factory: Factory<PageStore> =
            Factory::new(vec![store], None, 400, false);
        let mut driver = factory.build(0).expect("build");
        let mut settled_by_pressure = false;
        for n in 0..400 {
            driver
                .execute(MicroOp::Insert {
                    n,
                    row: thing(n, 42),
                })
                .expect("insert");
            let owed = driver.engine.unsettled_bytes();
            driver.after_op().expect("after insert");
            settled_by_pressure |= owed > line;
            assert!(driver.engine.unsettled_bytes() <= line, "row {n}");
        }
        assert!(settled_by_pressure, "the inserts never reached the line");
        let owed = driver.engine.unsettled_bytes();
        let held = driver.engine.cache_bytes();
        driver.execute(MicroOp::Read { n: 7 }).expect("read");
        driver.after_op().expect("after read");
        assert_eq!(driver.engine.unsettled_bytes(), owed, "a read settled");
        assert_eq!(driver.engine.cache_bytes(), held, "a read evicted");
        drop(driver);
    }

    /// A relaxed store settles without waiting for its `fsync`: the
    /// helper takes the barrier, nothing is left unsettled when the call
    /// returns, and the records are there after a reopen.
    #[test]
    fn an_unwaited_settle_leaves_nothing_owed_and_survives_a_reopen() {
        use wavedb_core::{LocalHandle, U48};

        use crate::schema::thing;
        use crate::seed::TENANT;
        use crate::systems::engine::settle_unwaited;

        let scratch = Scratch::new();
        let mut stores = vec![open(&scratch.0)];
        crate::systems::wavedb::relax(&mut stores).expect("relax");
        let store = stores.pop().expect("one store");
        let (pivot, ids) = {
            let db = LocalHandle::new(&store, U48::from(TENANT));
            let pivot = futures::executor::block_on(Thing::create_pivot(&db))
                .expect("pivot");
            let col = Thing::collection(pivot);
            let ids: Vec<_> = (0..50)
                .map(|n| {
                    futures::executor::block_on(col.insert(&db, &thing(n, 42)))
                        .expect("insert")
                })
                .collect();
            (pivot, ids)
        };
        settle_unwaited(&store).expect("settle");
        assert_eq!(store.unsettled_bytes(), 0, "the round owed nothing");
        drop(store);
        let store = open(&scratch.0);
        let db = LocalHandle::new(&store, U48::from(TENANT));
        let col = Thing::collection(pivot);
        for (n, id) in ids.into_iter().enumerate() {
            let got =
                futures::executor::block_on(col.get(&db, id)).expect("get");
            assert!(got.is_some(), "row {n} after the reopen");
        }
    }

    /// The rule that forces the `Mutex<Option<_>>`: a second consumer asking
    /// for the store must be refused, not quietly handed a share of it.
    #[test]
    fn a_second_build_is_refused_rather_than_sharing_the_store() {
        let scratch = Scratch::new();
        let factory: Factory<PageStore> =
            Factory::new(vec![open(&scratch.0)], None, 10, false);

        let first = factory.build(0);
        assert!(first.is_ok());
        let err = factory.build(1).err().expect("must refuse");
        assert!(err.contains("second store"), "{err}");
        // Release the claim so the next test can open the directory.
        drop(first);
    }

    /// One consumer per store: the single row has one, the multi row as
    /// many as data shards.
    #[test]
    fn a_consumer_per_store() {
        let scratch = Scratch::new();
        let direct: Factory<PageStore> =
            Factory::new(vec![open(&scratch.0)], None, 1, false);
        assert_eq!(direct.consumers(), 1);
    }

    /// The multi row end to end: three data shards, three consumers, each
    /// its own collection, every read finding its row on its own shard.
    #[test]
    fn a_multi_micro_row_partitions_rows_across_consumers() {
        let scratch = Scratch::new();
        let stores = wavedb_storage::open_shards(
            &scratch.0,
            &Thing::storage_entries(),
            StoreOptions {
                storage_shards: 1,
                ..StoreOptions::default()
            },
            3,
        )
        .expect("open");
        let factory: Factory<PageStore> =
            Factory::new(stores, None, 120, false);
        assert_eq!(factory.consumers(), 3);
        let phases = run(&factory, workload()).expect("run");
        assert_eq!(phases[0].samples.len(), 120, "insert");
        assert_eq!(phases[1].samples.len(), 60, "read_hot");
        assert_eq!(phases[3].samples.len(), 60, "update");
    }
}
