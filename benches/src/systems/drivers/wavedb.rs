//! WaveDB on the [`Driver`] seam — the engine in the consumer's own thread.
//!
//! ## Why the factory hands over a `PageStore` rather than a built engine
//!
//! One store per process: `StructStorage`'s statics are process-global and a
//! second open is `EngineBusy`. So the factory holds the one store behind a
//! `Mutex<Option<_>>` and the first `build` **takes** it. That is sound here
//! because the micro workload runs one consumer — a collection is indivisible,
//! so `wavedb/multi` is a `shop` row only (RFC 0065 §3) — and a second `build`
//! is refused rather than silently sharing.
//!
//! The engine is then constructed *on the consumer's thread*, which is the
//! constraint the whole seam is shaped by: [`Sharded`] holds an
//! `Rc<ShardStore>` and is not `Send`, so it could not have been built
//! anywhere else and moved.
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

use futures::executor::block_on;
use wavedb_core::{Id, LocalHandle, U48};
use wavedb_storage::PageStore;

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;
use crate::schema::{Thing, ThingPivotId};
use crate::seed::TENANT;
use crate::systems::engine::{Engineish, Sharded};

/// Journal bytes above which a row checkpoints, between operations.
const CHECKPOINT_AFTER_BYTES: u64 = 64 << 20;

/// Cache budget the checkpoint evicts down to.
const CACHE_BUDGET_BYTES: usize = 96 << 20;

/// Building one engine from the process's single store.
///
/// Two implementations, one per row of the single/multi axis, and no `dyn`:
/// each row monomorphises to its own concrete engine.
pub trait FromStore: Engineish + Sized {
    fn from_store(store: PageStore) -> Result<Self, String>;
}

impl FromStore for PageStore {
    fn from_store(store: Self) -> Result<Self, String> {
        Ok(store)
    }
}

impl FromStore for Sharded {
    fn from_store(store: PageStore) -> Result<Self, String> {
        Self::new(store)
    }
}

/// What a consumer needs to build its engine. `Send`: a `PageStore` crosses,
/// the engine made from it does not have to.
pub struct Factory<E> {
    store: Mutex<Option<PageStore>>,
    /// The anchors a seeded row was filled with, and its Pivot. `None` means
    /// the row inserts its own.
    seeded: Option<(Vec<Id>, ThingPivotId)>,
    /// Pre-reserved so an insert's `ids.push` can never reallocate inside a
    /// timed window.
    rows: usize,
    engine: std::marker::PhantomData<fn() -> E>,
}

impl<E> Factory<E> {
    #[must_use]
    pub fn new(
        store: PageStore,
        seeded: Option<(Vec<Id>, ThingPivotId)>,
        rows: usize,
    ) -> Self {
        Self {
            store: Mutex::new(Some(store)),
            seeded,
            rows,
            engine: std::marker::PhantomData,
        }
    }
}

pub struct WavedbDriver<E: Engineish> {
    engine: E,
    pivot: ThingPivotId,
    /// Minted anchors, indexed by the dataset's logical row number. A
    /// NonUnique anchor is minted from the clock at insert, so it cannot be
    /// recomputed from the seed — this table is how `n` becomes an `Id`.
    ids: Vec<Id>,
}

// No `Send` bound on `E`, and that is the point: `Sharded` holds an
// `Rc<ShardStore>` and never crosses a thread. `Factory<E>` is `Send`
// regardless, because it holds a `PageStore` and a `PhantomData<fn() -> E>` —
// a phantom that produces `E` rather than containing one.
impl<E> DriverFactory for Factory<E>
where
    E: Engineish + FromStore,
{
    type Driver = WavedbDriver<E>;

    /// One. The micro workload lives in a single collection, and a collection
    /// is indivisible: its B+tree nodes and chain segments belong to the
    /// Pivot's owner, so a second consumer would contend on shared structure.
    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, shard: usize) -> Result<WavedbDriver<E>, String> {
        let store = self
            .store
            .lock()
            .map_err(|_| "the store mutex was poisoned".to_string())?
            .take()
            .ok_or_else(|| {
                format!(
                    "consumer {shard} asked for a second store: one \
                     `PageStore` per process, and the micro workload has one \
                     consumer"
                )
            })?;
        // On this thread, and it has to be: `Sharded` holds an `Rc`.
        let engine = E::from_store(store)?;

        let (ids, pivot) = match &self.seeded {
            Some((ids, pivot)) => (ids.clone(), *pivot),
            None => {
                let db = LocalHandle::new(engine.store(), U48::from(TENANT));
                let pivot = block_on(Thing::create_pivot(&db))
                    .map_err(|e| format!("create_pivot: {e}"))?;
                (Vec::with_capacity(self.rows), pivot)
            }
        };
        Ok(WavedbDriver { engine, pivot, ids })
    }

    fn route(&self, _op: &MicroOp) -> usize {
        0
    }
}

impl<E: Engineish> WavedbDriver<E> {
    /// The anchor for dataset row `n`.
    fn id_of(&self, n: u64) -> Result<Id, String> {
        self.ids
            .get(n as usize)
            .copied()
            .ok_or_else(|| format!("row {n} was never inserted"))
    }
}

impl<E: Engineish> Driver for WavedbDriver<E> {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        let db = LocalHandle::new(self.engine.store(), U48::from(TENANT));
        let col = Thing::collection(self.pivot);
        match op {
            MicroOp::Insert { n, row } => {
                let id = block_on(col.insert(&db, &row))
                    .map_err(|e| format!("insert {n}: {e}"))?;
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
                block_on(col.save(&db, id, &row))
                    .map_err(|e| format!("save {n}: {e}"))?;
            }
        }
        Ok(())
    }

    /// Checkpoint and evict once the journal is large enough to matter.
    ///
    /// A byte threshold rather than an operation count: RFC 0060's adapter
    /// used `MAINTAIN_EVERY = 5000` operations and never fired while 649 MB of
    /// journal accumulated, because a count says nothing about how much log it
    /// produced.
    fn after_op(&mut self) -> Result<(), String> {
        if self.engine.journal_len() <= CHECKPOINT_AFTER_BYTES {
            return Ok(());
        }
        self.engine.checkpoint()?;
        self.engine.evict(CACHE_BUDGET_BYTES);
        Ok(())
    }

    /// Quiesce before the reads, and empty the cache before the cold ones.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        match done {
            // Settle and frame the journal, so the read phase does not measure
            // the insert phase's deferred work.
            "insert" => {
                self.engine.drain()?;
                self.engine.checkpoint()
            }
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
    /// Required rather than tidy: the process-wide engine claim has to be free
    /// before anything opens the same directory again, and a footprint read
    /// over an undrained settle queue counts a retained journal as stored data.
    fn close(self) -> Result<(), String> {
        self.engine.drain()?;
        self.engine.checkpoint()?;
        self.engine.close();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use wavedb_storage::{PageStore, StoreOptions};

    use super::{Factory, FromStore};
    use crate::harness::micro::MicroWorkload;
    use crate::harness::{DriverFactory, run};
    use crate::schema::Thing;
    use crate::systems::engine::Sharded;

    /// One `PageStore` per **process**, so these tests may not overlap.
    /// `StructStorage`'s statics are process-global and a second open is
    /// `EngineBusy` — the same rule the storage crate's own tests serialise
    /// against.
    fn engine_gate() -> MutexGuard<'static, ()> {
        static GATE: OnceLock<Mutex<()>> = OnceLock::new();
        GATE.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

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
        let factory: Factory<E> = Factory::new(open(&scratch.0), None, 120);
        run(&factory, workload()).expect("run")
    }

    /// The single-thread row, end to end: insert, read, reopen-equivalent,
    /// update, all through the seam.
    #[test]
    fn a_direct_micro_row_runs_through_the_harness() {
        let _gate = engine_gate();
        let scratch = Scratch::new();
        let phases = run_one::<PageStore>(&scratch);

        assert_eq!(phases.len(), 4);
        assert_eq!(phases[0].samples.len(), 120, "insert");
        assert_eq!(phases[1].samples.len(), 60, "read_hot");
        assert_eq!(phases[3].samples.len(), 60, "update");
        assert!(phases.iter().all(|p| p.wall_ns > 0));
    }

    /// The sharded row — and the one that proves the seam's whole shape: a
    /// `Sharded` holds an `Rc<ShardStore>` and is **not** `Send`, so it can
    /// only exist because `build` runs on the consumer's own thread.
    #[test]
    fn a_sharded_micro_row_runs_on_the_consumers_thread() {
        let _gate = engine_gate();
        let scratch = Scratch::new();
        let phases = run_one::<Sharded>(&scratch);

        assert_eq!(phases.len(), 4);
        assert_eq!(phases[0].samples.len(), 120);
        assert!(phases.iter().all(|p| !p.samples.is_empty()));
    }

    /// The rule that forces the `Mutex<Option<_>>`: a second consumer asking
    /// for the store must be refused, not quietly handed a share of it.
    #[test]
    fn a_second_build_is_refused_rather_than_sharing_the_store() {
        let _gate = engine_gate();
        let scratch = Scratch::new();
        let factory: Factory<PageStore> =
            Factory::new(open(&scratch.0), None, 10);

        let first = factory.build(0);
        assert!(first.is_ok());
        let err = factory.build(1).err().expect("must refuse");
        assert!(err.contains("second store"), "{err}");
        // Release the claim so the next test can open the directory.
        drop(first);
    }

    /// Micro is one consumer for both engines, because a collection is
    /// indivisible — the constraint that makes `wavedb/multi` a `shop` row.
    #[test]
    fn both_engines_ask_for_exactly_one_consumer() {
        let _gate = engine_gate();
        let scratch = Scratch::new();
        let direct: Factory<PageStore> =
            Factory::new(open(&scratch.0), None, 1);
        assert_eq!(direct.consumers(), 1);
        drop(direct);

        let scratch2 = Scratch::new();
        let sharded: Factory<Sharded> =
            Factory::new(open(&scratch2.0), None, 1);
        assert_eq!(sharded.consumers(), 1);
    }
}
