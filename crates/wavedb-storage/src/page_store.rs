//! [`PageStore`] — the node's authoritative [`Store`](wavedb_core::Store) backend.
//!
//! This is the disk-optimised key→value store the `Store`-generic
//! `Pivot`/`BpTree` layer in [`wavedb_core`] runs over. It ties together the
//! pieces built below it:
//!
//! - [`Journal`](crate::journal::Journal) — the write-ahead log;
//!   **durability is here** (`apply` fsyncs before it returns).
//! - one [`StructStorage`] **static per type** — that type's own in-memory
//!   cache (reads serve from it) and its own [`Directory`] over the shared
//!   [`BlockFile`]. The statics are `#[wavedb]`-generated and handed in as an
//!   explicit registry at [`open`](PageStore::open): compile-time state, no
//!   runtime `STRUCT_HASH → state` map.
//!
//! ## Locking
//!
//! There is no store-wide state lock. Each type's cache is its own `RwLock`
//! and its directory its own `Mutex`, so operations on different types run
//! concurrently and a cached read never waits on the settle path. The narrow
//! globals:
//!
//! - `journal` (`Mutex`) — appends must be ordered; the cache commit happens
//!   under it so cache order always equals journal (= replay) order;
//! - `alloc` (`Mutex`) — block space is one shared resource.
//!
//! Lock order: `journal → pending → cache` on the commit path (the pending
//! push stays under the journal so eviction's "queue empty ⇒ everything
//! settled" check can't race a commit); `pending → dir → cache(read) →
//! alloc` on the settle drain; `dir → dict` on the read-through path. No
//! path takes them in a conflicting order, so the graph is acyclic.
//!
//! The commit path takes no directory at all since RFC 0063: every write
//! carries the STRUCT_HASH it routes by, so committing a batch is memory
//! work and cannot touch a page.
//!
//! `retiring` sits outside that order entirely, by rule: it is **never held
//! across another lock**. A checkpoint publishes under it (`alloc → retiring`)
//! and claims it before rotating; `force_retirement` takes it after syncing the
//! journal and releases it before `dispose` takes `alloc`. Since it is only
//! ever held alone, it can join no cycle (`crate::retire`).
//!
//! ## Reads
//!
//! The cache is a **cache**, not the dataset: a read serves from the type's
//! cache when it holds the id and falls back to the settled page
//! ([`Directory::get_record`]) otherwise. Settle writes the cache's current
//! bytes before anything is evicted, so the fallback can only ever see the
//! newest settled state — never a version the cache has superseded. An
//! absent id costs one page probe; that's fine until a keyed filter earns
//! its place.
//!
//! ## Recovery model
//!
//! `data.bin` is a **deterministic projection of the journal**; recovery
//! roots in the newest valid `Commit` frame (journal-rooted — see
//! [`PageStore::commit_journal`] and the `commit` module docs). On
//! [`open`](PageStore::open): with a durable `Commit`, the
//! directories/dictionaries/allocator replay from the addressing log it
//! names, the caches start
//! **empty** (reads fall through to the pages), and only the uncovered
//! `Batch` frames replay; without one (first generation), the data file is
//! truncated back to its superblock and every committed batch replays
//! through the same commit + settle path a live `apply` uses. Either way a
//! crash loses nothing that was acked. Settle writes the **cache's current
//! bytes** for a touched id (not the batch's), which makes it
//! order-independent and replay-idempotent: a late or repeated settle can
//! only write newer state.
//!
//! The settle into pages is **deferred**: `apply` commits to the journal +
//! caches and queues the touched ids; [`drain`](PageStore::drain) (driven by
//! the node's maintenance task) writes the pages later. An unsettled remove
//! is tombstoned so the read fallback never resurrects the page's stale
//! bytes. [`PageStore::commit_journal`] drains first, so the frame it
//! appends always covers everything committed.

use std::path::Path;
use std::time::Duration;

use parking_lot::Mutex;
use wavedb_core::Id;

use crate::block::BlockAllocator;
use crate::block_file::BlockFile;
use crate::directory::Directory;
use crate::error::StorageResult;
use crate::struct_storage::{BPTREE_NODE_STORAGE, EngineClaim, StructStorage};

/// How many blocks a bucket should hold. It is a **target**, not a limit: a
/// bucket over it is split when its turn comes round, and simply spans more
/// blocks until then (RFC 0049). Tunable.
const DEFAULT_TARGET_BLOCKS_PER_BUCKET: u64 = 8; // 32 KiB

/// Ids one batch touched, grouped by registry slot — what the settle consumes.
pub(crate) type Touched = Vec<(usize, Vec<Id>)>;

/// How a store answers the durability question and how much RAM it may spend
/// answering reads — chosen once at [`open_with`](PageStore::open_with)
/// (RFCs 0061, 0044, 0065 §6).
///
/// Opened, never inferred: a durability mode that changes by itself is a
/// guarantee nobody can reason about, and a cache budget that drifts is a
/// memory ceiling nobody can hold a process to.
///
/// **None of these fields reaches stored bytes**, so none folds into a
/// `STRUCT_HASH`: a window and two cache budgets change when work happens and
/// what is held in RAM while it does, never what is written. Two stores opened
/// with different options hold byte-identical files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOptions {
    /// Longest a `Batch` may sit unsynced. **Zero (the default) is one
    /// barrier per batch** — the promise the engine was built around, and
    /// what every existing caller keeps.
    ///
    /// Non-zero trades it for group commit: `apply` appends without a barrier
    /// and syncs only once the window has elapsed, so a burst inside one
    /// window amortises to a single `fsync`. `Ok` from a write then means *in
    /// the journal, ordered, and durable within the window unless the process
    /// dies first* — a crash loses a **suffix** of acknowledged writes and
    /// never corrupts (frames are crc-framed, replay stops at the first torn
    /// one). [`flush`](PageStore::flush) forces the barrier when a particular
    /// write does need the stronger answer.
    pub relax_window: Duration,

    /// Bytes of **page images** the block file may hold (RFC 0044) — whole
    /// runs, still compressed, exactly as they sit on disk.
    ///
    /// Zero disables the cache. Read once at open: a budget that changed
    /// under a running store would make "how much RAM does this hold" a
    /// question with no answer.
    pub page_cache_bytes: usize,

    /// Bytes of **decoded records** the caches should be evicted down to —
    /// the budget [`evict_to_budget`](PageStore::evict_to_budget) targets.
    ///
    /// **The store never applies it on its own.** Eviction stays where it
    /// already is, in the caller's hands (the node's upkeep loop, a
    /// benchmark's phase boundary), because a `PageStore` that dropped cache
    /// entries on a timer of its own would be doing work no caller asked for
    /// inside a window someone is timing. What this field changes is only
    /// *where the number lives*: opened with the store, instead of a constant
    /// remembered separately at every call site.
    pub record_cache_bytes: usize,
}

/// Bytes of decoded records [`StoreOptions::record_cache_bytes`] defaults to.
///
/// The number callers were already passing by hand, so the default changes
/// nothing; it is advisory in the strict sense, since nothing evicts until a
/// caller asks.
pub const DEFAULT_RECORD_CACHE_BYTES: usize = 96 << 20;

impl Default for StoreOptions {
    /// Exactly today's engine: one barrier per batch, a 64 MiB page cache,
    /// and a record-cache budget nothing applies unless asked.
    fn default() -> Self {
        Self {
            relax_window: Duration::ZERO,
            page_cache_bytes: crate::page_cache::DEFAULT_BUDGET_BYTES,
            record_cache_bytes: DEFAULT_RECORD_CACHE_BYTES,
        }
    }
}

/// The native, page-backed [`Store`](wavedb_core::Store).
///
/// One instance per process (the node model: one process, one `data.bin`);
/// state lives in the per-type [`StructStorage`] statics registered at
/// [`open`](Self::open).
#[derive(Debug)]
pub struct PageStore {
    pub(crate) file: BlockFile,
    /// The directory holding `data.bin` + the journal chain (rotation
    /// creates new journal files here).
    pub(crate) data_dir: std::path::PathBuf,
    pub(crate) journal: Mutex<crate::journal::Journal>,
    pub(crate) alloc: Mutex<BlockAllocator>,
    /// The addressing log written into the settle windows (RFC 0046): the
    /// last full snapshot chunk and every delta chunk after it.
    pub(crate) meta: Mutex<crate::meta_log::MetaLog>,
    /// A checkpoint whose `Commit` frame is written but not yet durable —
    /// the retired journal and the protection roll it authorises (RFC 0046).
    pub(crate) retiring: Mutex<Option<crate::retire::Retiring>>,
    /// The registered slots, sorted by `STRUCT_HASH` (deduped) — a lock-free,
    /// read-only route table after open.
    pub(crate) types: Vec<&'static StructStorage>,
    pub(crate) seed: [u64; 4],
    pub(crate) target_blocks_per_bucket: u64,
    /// Touched ids committed to the caches but not yet settled into pages —
    /// what [`drain`](Self::drain) consumes.
    pub(crate) pending: Mutex<Touched>,
    /// The durability window this store was opened with (RFC 0061); zero is
    /// one barrier per batch.
    pub(crate) relax_window: Duration,
    /// The record-cache budget this store was opened with — carried, not
    /// applied (see [`StoreOptions::record_cache_bytes`]).
    pub(crate) record_cache_bytes: usize,
    _claim: EngineClaim,
}

impl PageStore {
    /// Open (or create) a node store rooted at directory `dir`, holding
    /// `data.bin` and `journal.log`, serving exactly the listed `types` (each
    /// a `#[wavedb]`-generated static — `T::storage_entries()`). The reserved
    /// `BpTree`-node slot is always included. Replays the journal to rebuild
    /// every slot's cache and pages.
    ///
    /// The registry is an allowlist: a write whose `STRUCT_HASH` has no listed
    /// slot fails with [`StorageError::UnregisteredStructHash`](crate::StorageError::UnregisteredStructHash).
    ///
    /// # Errors
    /// [`StorageError::EngineBusy`](crate::StorageError::EngineBusy) if this process already has an open store;
    /// otherwise any filesystem / corruption fault during open or replay.
    pub fn open(
        dir: impl AsRef<Path>,
        types: &[&'static StructStorage],
    ) -> StorageResult<Self> {
        Self::open_with(dir, types, StoreOptions::default())
    }

    /// [`open`](Self::open), with the durability question answered explicitly
    /// (RFC 0061). `StoreOptions::default()` is exactly [`open`](Self::open).
    ///
    /// # Errors
    /// The same faults as [`open`](Self::open).
    pub fn open_with(
        dir: impl AsRef<Path>,
        types: &[&'static StructStorage],
        options: StoreOptions,
    ) -> StorageResult<Self> {
        let claim = EngineClaim::acquire()?;
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;

        let data_bin_existed = dir.join("data.bin").exists();
        let file = BlockFile::open_with(
            dir.join("data.bin"),
            options.page_cache_bytes,
        )?;
        let seed = file.seed();

        // Generated Pivot types of identical shape may share a STRUCT_HASH;
        // they also share pages today, so one slot serves the hash — dedup.
        let mut types: Vec<&'static StructStorage> = types.to_vec();
        types.push(&BPTREE_NODE_STORAGE);
        types.sort_by_key(|s| s.struct_hash());
        types.dedup_by_key(|s| s.struct_hash());
        for slot in &types {
            slot.reset(); // a prior run's state (same process) must not leak in
        }

        // Recover from the journal chain: the newest `Commit` frame roots
        // the directories/dictionaries/allocator (caches stay empty — reads
        // fall through to the pages); every uncovered `Batch` replays below.
        let recovered =
            crate::commit::restore(dir, data_bin_existed, &file, &types)?;

        let store = Self {
            file,
            data_dir: dir.to_path_buf(),
            journal: Mutex::new(recovered.journal),
            alloc: Mutex::new(recovered.alloc),
            meta: Mutex::new(recovered.meta),
            retiring: Mutex::new(None),
            types,
            seed,
            target_blocks_per_bucket: DEFAULT_TARGET_BLOCKS_PER_BUCKET,
            pending: Mutex::new(Vec::new()),
            relax_window: options.relax_window,
            record_cache_bytes: options.record_cache_bytes,
            _claim: claim,
        };
        for batch in &recovered.replay {
            store.route_batch(batch)?; // rebuild caches + pages, no re-journal
            let touched = store.commit_to_caches(batch);
            store.settle(&touched)?;
        }
        Ok(store)
    }

    /// Force the barrier now: every batch acknowledged so far is on the
    /// platter when this returns.
    ///
    /// A no-op-shaped call on a zero-window store (each batch already synced,
    /// so this just takes one more barrier over nothing) and the escape hatch
    /// on a windowed one — the reason a window is a setting rather than a
    /// wager. An application can be relaxed for the cart and strict for the
    /// receipt (RFC 0061).
    ///
    /// # Errors
    /// [`StorageError::Io`](crate::StorageError::Io) if the sync fails.
    pub fn flush(&self) -> StorageResult<()> {
        self.journal.lock().sync()
    }

    /// Number of live records currently cached across every registered type.
    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.types.iter().map(|s| s.cached_len()).sum()
    }

    /// Bucket count of one type's directory (`0` while nothing settled).
    #[must_use]
    pub fn bucket_count(&self, struct_hash: u64) -> usize {
        self.slot_of(struct_hash).map_or(0, |slot| {
            slot.directory().lock().as_ref().map_or(0, Directory::len)
        })
    }

    /// The registered slot for `struct_hash`, if listed at open.
    pub(crate) fn slot_of(
        &self,
        struct_hash: u64,
    ) -> Option<&'static StructStorage> {
        self.types
            .binary_search_by_key(&struct_hash, |s| s.struct_hash())
            .ok()
            .map(|i| self.types[i])
    }
}

#[cfg(test)]
mod tests {
    use super::{PageStore, StoreOptions};
    use crate::Progress;
    use crate::error::StorageError;
    use crate::struct_storage::StructStorage;
    use futures::executor::block_on;
    use parking_lot::{Mutex, MutexGuard};
    use std::time::Duration;
    use wavedb_core::{Id, Store, U48, Write};

    const SH: u64 = 0x1122_3344_5566_7788;
    const SH2: u64 = 0x99AA_BBCC_DDEE_FF00;

    /// The test slot every raw record routes to (`SH`-headed).
    static TEST_SLOT: StructStorage = StructStorage::new(SH);
    /// A second registered type, for the multi-slot paths.
    static OTHER_SLOT: StructStorage = StructStorage::new(SH2);

    /// The per-struct slots are process-global statics, so only one store may
    /// live at a time — serialise the tests that open one.
    fn engine_gate() -> MutexGuard<'static, ()> {
        static GATE: Mutex<()> = Mutex::new(());
        GATE.lock()
    }

    fn open(path: &std::path::Path) -> PageStore {
        PageStore::open(path, &[&TEST_SLOT]).unwrap()
    }

    /// A wire record: `[STRUCT_HASH (8 LE)][body]`.
    fn rec(struct_hash: u64, body: &[u8]) -> Vec<u8> {
        let mut v = struct_hash.to_le_bytes().to_vec();
        v.extend_from_slice(body);
        v
    }

    fn nonunique(key: u64) -> Id {
        Id::new(key, U48::from(1u32), false, (key & 0x7FFF) as u16)
    }

    /// `Default` is today's engine, spelled out.
    ///
    /// A canary rather than a tautology: these three numbers are what every
    /// caller that never asked already runs on, so changing one is changing
    /// every deployment's engine — which should cost a failing test.
    #[test]
    fn default_options_are_todays_engine() {
        let o = StoreOptions::default();
        assert_eq!(o.relax_window, Duration::ZERO, "one barrier per batch");
        assert_eq!(o.page_cache_bytes, 64 << 20);
        assert_eq!(o.record_cache_bytes, 96 << 20);
    }

    /// Each budget reaches the layer that spends it, and `open` still means
    /// the default — a knob is only a knob where it lands.
    #[test]
    fn the_budgets_reach_the_layers_that_spend_them() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        {
            let s = PageStore::open_with(
                d.path(),
                &[&TEST_SLOT],
                StoreOptions {
                    page_cache_bytes: 128 << 10,
                    record_cache_bytes: 4 << 10,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            assert_eq!(s.file.page_cache().budget(), 128 << 10);
            assert_eq!(s.record_cache_bytes(), 4 << 10);
        }
        let s = open(d.path());
        assert_eq!(s.file.page_cache().budget(), 64 << 20);
        assert_eq!(s.record_cache_bytes(), 96 << 20);
    }

    /// The record-cache budget is **carried, not applied** (RFC 0065 §6): a
    /// store opened with a one-byte budget still holds every settled record
    /// until a caller asks it not to.
    ///
    /// Asserted rather than assumed, because eviction takes the journal lock.
    /// A store that evicted on a schedule of its own would be quiescing
    /// writers at a moment nobody chose — inside a timed window, for the
    /// callers this budget exists for.
    #[test]
    fn the_store_never_evicts_records_until_asked() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = PageStore::open_with(
            d.path(),
            &[&TEST_SLOT],
            StoreOptions {
                record_cache_bytes: 1, // anything held is over budget
                ..StoreOptions::default()
            },
        )
        .unwrap();
        block_on(async {
            for k in 0..64u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"payload"))])
                    .await
                    .unwrap();
            }
        });
        s.drain().unwrap();
        let cached = |s: &PageStore| -> usize {
            s.types.iter().map(|t| t.cached_bytes()).sum()
        };
        assert!(
            cached(&s) > 1,
            "settling 64 records must not evict them: the budget is the \
             caller's to apply"
        );
        s.evict_to_budget();
        assert!(
            cached(&s) <= 1,
            "asked, the store must evict to the budget it was opened with, \
             not to one remembered at the call site"
        );
    }

    /// Cache budgets are RAM, never bytes on disk. A store opened with **no**
    /// caches at all writes a file the default store reads back unchanged —
    /// which is why neither field folds into a `STRUCT_HASH`.
    #[test]
    fn a_store_opened_without_caches_writes_the_same_file() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        {
            let s = PageStore::open_with(
                d.path(),
                &[&TEST_SLOT],
                StoreOptions {
                    page_cache_bytes: 0,
                    record_cache_bytes: 0,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            block_on(async {
                for k in 0..32u64 {
                    s.apply(&[Write::Put(nonunique(k), rec(SH, b"nocache"))])
                        .await
                        .unwrap();
                }
            });
            s.drain().unwrap();
            s.commit_journal().unwrap();
        }
        let s = open(d.path()); // default budgets, same bytes
        block_on(async {
            for k in 0..32u64 {
                assert_eq!(
                    s.get(nonunique(k)).await.unwrap(),
                    Some(rec(SH, b"nocache")),
                    "record {k} must survive a store that cached nothing"
                );
            }
        });
    }

    /// RFC 0041's accounting: whatever a round touched, settling it is **one**
    /// positioned write — plus one read per page it had to modify, and no
    /// barrier at all (durability is the journal's until a checkpoint).
    #[test]
    fn a_settle_round_is_one_write_and_one_read_per_touched_page() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            for k in 0..40u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"first"))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap(); // the pages exist from here on

            let (reads, writes, syncs) = s.file.io().snapshot();
            for k in 0..40u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"second"))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap();
            let (r, w, sy) = s.file.io().snapshot();

            assert_eq!(w - writes, 1, "40 records must settle in ONE write");
            assert_eq!(sy - syncs, 0, "a settle round takes no barrier");
            let buckets = s.bucket_count(SH) as u64;
            assert!(
                r - reads <= buckets,
                "read {} pages for {buckets} buckets — one per touched page \
                 is the floor",
                r - reads
            );
        });
    }

    /// A checkpoint costs **one** write and **one** barrier, counting both
    /// files: the pages, the dictionary and the round's descriptor delta share
    /// the window write and its `data.bin` sync, and the `Commit` frame takes
    /// no barrier of its own — the next ordinary write carries it (RFC 0046).
    ///
    /// And nothing pays a barrier on that checkpoint's behalf afterwards: the
    /// retirement it authorises is disposed of by the *following* checkpoint,
    /// which is also one write and one barrier (RFC 0047).
    #[test]
    fn a_checkpoint_is_one_write_and_one_barrier() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            for k in 0..20u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"payload"))])
                    .await
                    .unwrap();
            }
            let (_, writes, syncs) = s.file.io().snapshot();
            s.commit_journal().unwrap();
            let (_, w, sy) = s.file.io().snapshot();
            assert_eq!(w - writes, 1, "pages + delta ride one window write");
            assert_eq!(sy - syncs, 1, "one `data.bin` barrier…");
            assert_eq!(
                s.journal.lock().barriers(),
                0,
                "…and none on the fresh journal: the frame is deferred"
            );
            assert!(s.is_retiring());

            // The next ordinary write pays a barrier it was paying anyway,
            // and that fsync carries the frame — no extra IO at all, and no
            // retirement work on the write path (RFC 0047).
            s.apply(&[Write::Put(nonunique(99), rec(SH, b"carrier"))])
                .await
                .unwrap();
            assert_eq!(s.journal.lock().barriers(), 1, "the batch's own fsync");
            assert!(s.is_retiring(), "disposal belongs to the next checkpoint");
            assert_eq!(journals(d.path()).len(), 2);

            // Which disposes of it for free: the carrier's barrier count moved
            // past the frame, so nothing needs syncing to prove it.
            let (_, w, sy) = s.file.io().snapshot();
            s.commit_journal().unwrap();
            let (_, w2, sy2) = s.file.io().snapshot();
            assert_eq!(w2 - w, 1, "one window write again");
            assert_eq!(sy2 - sy, 1, "one `data.bin` barrier again");
            assert_eq!(
                s.journal.lock().barriers(),
                0,
                "the fresh journal took none: no barrier for housekeeping"
            );
            assert_eq!(journals(d.path()).len(), 2, "at most two generations");
        });
        // And the commit is a real recovery root.
        drop(s);
        let s = open(d.path());
        block_on(async {
            assert_eq!(
                s.get(nonunique(7)).await.unwrap(),
                Some(rec(SH, b"payload"))
            );
        });
    }

    /// A checkpoint's runs are protected from the moment it **publishes**, not
    /// from the next write (RFC 0047). Its frame may become the recovery root
    /// at any instant after that, so a settle round in between must not free —
    /// and hand back for reuse — a run that frame still names.
    #[test]
    fn a_settle_after_a_checkpoint_cannot_reuse_its_runs() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            for k in 0..20u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"v1"))])
                    .await
                    .unwrap();
            }
            s.commit_journal().unwrap();
            assert_eq!(
                s.alloc.lock().deferred_blocks(),
                0,
                "nothing held yet — this checkpoint is the first root"
            );

            // Rewriting the same records supersedes the very pages the
            // checkpoint just named, so the settle frees their runs.
            for k in 0..20u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"v2"))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap();
            assert!(
                s.alloc.lock().deferred_blocks() > 0,
                "runs the live frame names must be held, not pooled"
            );
        });
    }

    /// The one case where disposal is not free (RFC 0047): two checkpoints
    /// with no write in between. Nothing flushed the journal carrying the
    /// first frame, so the second must sync it before deleting the journal
    /// that frame retires — a barrier paid by a checkpoint that had no work.
    #[test]
    fn a_checkpoint_with_no_writes_syncs_the_carrier_before_disposing() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            s.apply(&[Write::Put(nonunique(1), rec(SH, b"acked"))])
                .await
                .unwrap();
            s.commit_journal().unwrap();
            assert_eq!(journals(d.path()).len(), 2);

            // Not a single append since the frame — so its carrier has taken
            // no barrier, and the next checkpoint cannot delete on its word.
            s.commit_journal().unwrap();
            assert_eq!(
                journals(d.path()).len(),
                2,
                "one generation disposed of, this one's taken its place"
            );
            let carrier =
                s.retiring.lock().as_ref().map(|r| r.journal.barriers());
            assert_eq!(
                carrier,
                Some(1),
                "no batch touched that journal, so the barrier is the \
                 fallback's — the proof the delete was authorised"
            );
        });
        drop(s);
        let s = open(d.path());
        block_on(async {
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"acked"))
            );
        });
    }

    /// RFC 0046: the descriptors ride the settle window, so the `Commit` frame
    /// only *names* the addressing log. Its cost must therefore stay below the
    /// 8 bytes per bucket RFC 0043 spent carrying the directory itself — every
    /// checkpoint, for every type, forever.
    #[test]
    fn a_commit_frame_tracks_the_log_not_the_directory() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let mut s = open(d.path());
        // One block per bucket: a wide directory for little data, which is
        // exactly the shape this claim is about.
        s.target_blocks_per_bucket = 1;
        block_on(async {
            for chunk in (0..200u64).step_by(50) {
                // Batched applies keep the fsync count sane.
                let batch: Vec<Write> = (chunk..chunk + 50)
                    .map(|k| Write::Put(nonunique(k), rec(SH, &noise(k))))
                    .collect();
                s.apply(&batch).await.unwrap();
                s.drain().unwrap();
            }
            s.commit_journal().unwrap();

            // The fresh journal holds exactly the Commit frame, so its length
            // *is* the frame's cost.
            let (buckets, frame) = (s.bucket_count(SH), s.journal_len());
            assert!(buckets >= 16, "expected a wide directory, got {buckets}");
            assert!(
                frame < 8 * buckets as u64,
                "a {buckets}-bucket directory framed in {frame} bytes — \
                 carrying the descriptors would have cost {} plus overhead",
                8 * buckets
            );

            // RFC 0048: and it does not grow with the log either. Each round
            // adds a chunk to the chain; the frame keeps naming just the head,
            // so every checkpoint costs the same handful of bytes.
            for chunk in (200..600u64).step_by(50) {
                let batch: Vec<Write> = (chunk..chunk + 50)
                    .map(|k| Write::Put(nonunique(k), rec(SH, &noise(k))))
                    .collect();
                s.apply(&batch).await.unwrap();
                s.drain().unwrap();
                s.commit_journal().unwrap();
                assert_eq!(
                    s.journal_len(),
                    frame,
                    "the frame must not grow as the chain does"
                );
            }
        });
    }

    /// Compaction keeps the log bounded: without it every checkpoint would
    /// add another chunk address to the frame forever. And the whole chain —
    /// snapshot plus the deltas folded over it — must still restore.
    ///
    /// A second type that only ever writes *before* the compaction guards the
    /// snapshot's other half: a full chunk must carry the resident state of
    /// the types its round never touched, or that type is lost at the next
    /// reopen.
    #[test]
    fn compaction_bounds_the_log_and_the_chain_still_restores() {
        const ROUNDS: u64 = 120;
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let open2 = |path: &std::path::Path| {
            PageStore::open(path, &[&TEST_SLOT, &OTHER_SLOT]).unwrap()
        };
        block_on(async {
            let s = open2(d.path());
            s.apply(&[Write::Put(nonunique(7), rec(SH2, b"untouched"))])
                .await
                .unwrap();
            s.drain().unwrap();

            let mut widest = 0;
            for k in 0..ROUNDS {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"round"))])
                    .await
                    .unwrap();
                s.commit_journal().unwrap(); // drains, then frames the log
                widest = widest.max(s.journal_len());
            }
            // Unbounded growth would reach ~8 bytes per round on top of the
            // frame's fixed head; compaction caps it far below that.
            assert!(
                widest < 8 * ROUNDS,
                "the log must compact: widest frame was {widest} bytes over \
                 {ROUNDS} checkpoints"
            );
            drop(s);

            // Reopen: the snapshot chunk plus every delta after it.
            let s = open2(d.path());
            assert_eq!(s.cache_len(), 0, "a committed open starts cold");
            for k in 0..ROUNDS {
                assert_eq!(
                    s.get(nonunique(k)).await.unwrap(),
                    Some(rec(SH, b"round")),
                    "record {k} lost across the delta chain"
                );
            }
            assert_eq!(
                s.get_of(SH2, nonunique(7)).await.unwrap(),
                Some(rec(SH2, b"untouched")),
                "a snapshot must carry the types its round did not touch"
            );
        });
    }

    /// RFC 0042: relocation moves page bytes verbatim to fresh tail blocks and
    /// repoints the buckets — the records must survive it, including once the
    /// caches are gone and every read comes off the moved pages.
    #[test]
    fn defragment_relocates_pages_without_losing_records() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            // Enough noisy records to split into several buckets…
            for k in 0..200u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, &noise(k)))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap();
            s.commit_journal().unwrap();
            // …then rewrite half of them: every settled page is written
            // copy-on-write, so the runs they vacate pock the file.
            for k in (0..200u64).step_by(2) {
                s.apply(&[Write::Put(nonunique(k), rec(SH, &noise(k + 7)))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap();
            s.commit_journal().unwrap();

            let report = s.defragment(4096).unwrap();
            assert!(
                report.largest_after >= report.largest_before,
                "a pass must not fragment further: {report:?}"
            );

            // Force every read through the relocated pages.
            s.commit_journal().unwrap();
            s.evict_settled(0);
            assert_eq!(s.cache_len(), 0);
            for k in 0..200u64 {
                let want = if k % 2 == 0 { k + 7 } else { k };
                assert_eq!(
                    s.get(nonunique(k)).await.unwrap(),
                    Some(rec(SH, &noise(want))),
                    "record {k} lost across relocation"
                );
            }
        });
        // And the relocation survives a reopen from the commit.
        drop(s);
        let s = open(d.path());
        block_on(async {
            assert_eq!(
                s.get(nonunique(101)).await.unwrap(),
                Some(rec(SH, &noise(101)))
            );
        });
    }

    #[test]
    fn put_get_remove() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            assert_eq!(s.get(nonunique(1)).await.unwrap(), None);
            s.apply(&[Write::Put(nonunique(1), rec(SH, b"alpha"))])
                .await
                .unwrap();
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"alpha"))
            );
            // The typed path reaches the same bytes through the slot directly.
            assert_eq!(
                s.get_of(SH, nonunique(1)).await.unwrap(),
                Some(rec(SH, b"alpha"))
            );
            assert_eq!(s.get_of(SH ^ 1, nonunique(1)).await.unwrap(), None);
            s.apply(&[Write::Remove(SH, nonunique(1))]).await.unwrap();
            assert_eq!(s.get(nonunique(1)).await.unwrap(), None);
        });
    }

    #[test]
    fn batch_is_atomic_multi_record() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            s.apply(&[
                Write::Put(nonunique(1), rec(SH, b"a")),
                Write::Put(nonunique(2), rec(SH, b"b")),
            ])
            .await
            .unwrap();
            assert_eq!(s.get(nonunique(1)).await.unwrap(), Some(rec(SH, b"a")));
            assert_eq!(s.get(nonunique(2)).await.unwrap(), Some(rec(SH, b"b")));
        });
    }

    #[test]
    fn survives_reopen() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(7), rec(SH, b"durable"))])
                    .await
                    .unwrap();
            } // drop — no graceful flush beyond the journal fsync
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(7)).await.unwrap(),
                Some(rec(SH, b"durable")),
                "journal must survive a reopen"
            );
        });
    }

    #[test]
    fn reopen_reflects_removals() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"x"))])
                    .await
                    .unwrap();
                s.apply(&[Write::Remove(SH, nonunique(1))]).await.unwrap();
            }
            let s = open(d.path());
            assert_eq!(s.get(nonunique(1)).await.unwrap(), None);
            assert_eq!(s.cache_len(), 0);
        });
    }

    // The `Expect` guard: a satisfied guard commits its batch minus the
    // guard (the journal never holds one, so replay re-applies only the
    // effective writes); a violated guard refuses the whole batch as a
    // typed conflict *before* the durability point.
    #[test]
    fn expect_guard_commits_clean_and_conflicts_before_journaling() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"v1"))])
                    .await
                    .unwrap();
                // Satisfied guards (present + absent) ride with a Put.
                s.apply(&[
                    Write::Expect(SH, nonunique(1), Some(rec(SH, b"v1"))),
                    Write::Expect(SH, nonunique(2), None),
                    Write::Put(nonunique(1), rec(SH, b"v2")),
                ])
                .await
                .unwrap();
                // A stale expectation refuses the whole batch, typed.
                let err = s
                    .apply(&[
                        Write::Expect(SH, nonunique(1), Some(rec(SH, b"v1"))),
                        Write::Put(nonunique(1), rec(SH, b"lost-race")),
                    ])
                    .await
                    .unwrap_err();
                assert!(matches!(
                    err,
                    wavedb_core::Error::Conflict(at) if at == nonunique(1)
                ));
            }
            // Replay: the guarded commit landed, the refused one never
            // reached the log.
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"v2")),
                "the guarded batch must survive a reopen"
            );
        });
    }

    /// RFC 0063's A2. An `Expect` guard used to resolve its id's **type** by
    /// probing every registered slot in turn — so the guard on a first save,
    /// which matches nothing, paid a page read per declared type, under the
    /// journal lock that serializes all writers. The guard now carries the
    /// STRUCT_HASH it routes by, so its cost is one lookup and cannot grow
    /// with the schema.
    #[test]
    fn a_guard_costs_the_same_whatever_the_schemas_width() {
        let _g = engine_gate();
        // The same batch, the same guard, against a one-type registry and a
        // two-type one. Sequential: the slots are process-global statics.
        let narrow = guard_page_reads(&[&TEST_SLOT]);
        let wide = guard_page_reads(&[&TEST_SLOT, &OTHER_SLOT]);
        assert_eq!(
            narrow, wide,
            "a guard's page reads must not grow with the registry: {narrow} \
             with one type, {wide} with two"
        );
        assert!(
            wide <= 1,
            "a guard is one routed lookup — at most the target type's own \
             page, not a scan; charged {wide}"
        );
    }

    /// Page reads charged by one absent-expectation guard against a store
    /// whose every registered type holds settled, evicted pages — the state
    /// in which a scan would actually have to read them.
    fn guard_page_reads(types: &[&'static StructStorage]) -> u64 {
        let d = tempfile::tempdir().unwrap();
        let s = PageStore::open(d.path(), types).unwrap();
        block_on(async {
            for slot in types {
                for k in 0..64u64 {
                    s.apply(&[Write::Put(
                        nonunique(k),
                        rec(slot.struct_hash(), b"settled"),
                    )])
                    .await
                    .unwrap();
                }
            }
            s.drain().unwrap();
            s.evict_settled(0); // the caches must not answer the guard
            let before = s.file.io().snapshot().0;
            // Absent everywhere, so the old probe fell through every slot.
            s.apply(&[
                Write::Expect(SH, nonunique(9_999), None),
                Write::Put(nonunique(9_999), rec(SH, b"first version")),
            ])
            .await
            .unwrap();
            s.file.io().snapshot().0 - before
        })
    }

    /// The same fix on the other write that named no type: routing a
    /// `Remove` meant finding the id's owner by probing every cache and then
    /// every settled page. It routes by its own hash now, so committing one
    /// is memory work — which is what lets the whole commit section promise
    /// it cannot touch a page (RFC 0063).
    #[test]
    fn a_remove_reads_no_page() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = PageStore::open(d.path(), &[&TEST_SLOT, &OTHER_SLOT]).unwrap();
        block_on(async {
            for k in 0..64u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"settled"))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap();
            s.evict_settled(0);

            let before = s.file.io().snapshot().0;
            s.apply(&[Write::Remove(SH, nonunique(5))]).await.unwrap();
            assert_eq!(
                s.file.io().snapshot().0,
                before,
                "committing a Remove must read no page"
            );
            assert_eq!(
                s.get_of(SH, nonunique(5)).await.unwrap(),
                None,
                "the removal must still take effect"
            );
        });
    }

    #[test]
    fn unregistered_struct_hash_is_refused_before_journaling() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            let err = s
                .apply(&[Write::Put(nonunique(1), rec(SH ^ 0xFF, b"stray"))])
                .await
                .unwrap_err();
            assert!(err.to_string().contains("no StructStorage registered"));
            // Nothing journaled: a reopen replays cleanly and stays empty.
        });
        drop(s);
        let s = open(d.path());
        assert_eq!(s.cache_len(), 0, "refused write must not reach the log");
    }

    /// A bounded settle round takes its budget and leaves the rest queued —
    /// RFC 0063's interruptible drain, and the property the disk actor's
    /// priority valve needs in order to have anything to interleave between.
    ///
    /// The budget has to bite **within** one slot, not only across slots: a
    /// burst of one type is one slot, and that is the common case. A version
    /// that only split at slot boundaries would pass every other assertion
    /// here and settle all 20 records in one round.
    #[test]
    fn a_bounded_settle_step_leaves_the_rest_queued() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            for n in 1..=20u64 {
                s.apply(&[Write::Put(nonunique(n), rec(SH, b"x"))])
                    .await
                    .unwrap();
            }
            assert!(s.has_pending(), "20 writes must queue a settle");

            // One bounded round: some settled, the remainder still queued.
            assert_eq!(s.settle_step(5).unwrap(), Progress::Settled);
            assert!(
                s.has_pending(),
                "a budget of 5 must not have settled all 20"
            );

            // Driving it to exhaustion reaches the same place `drain` would.
            let mut rounds = 1;
            while s.settle_step(5).unwrap() == Progress::Settled {
                rounds += 1;
                assert!(rounds < 100, "step made no progress");
            }
            assert!(
                rounds >= 4,
                "20 ids at 5 per round is at least 4 rounds, took {rounds}"
            );
            assert!(!s.has_pending(), "the queue must end empty");

            // And the records are all readable from their pages.
            TEST_SLOT.mem_cache().write().clear();
            for n in 1..=20u64 {
                assert_eq!(
                    s.get_of(SH, nonunique(n)).await.unwrap(),
                    Some(rec(SH, b"x")),
                    "record {n} lost across the stepped settle"
                );
            }
        });
    }

    /// The step reports `Done` on an empty queue rather than looping.
    #[test]
    fn a_step_on_an_empty_queue_is_done() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        assert_eq!(s.settle_step(usize::MAX).unwrap(), Progress::Done);
        assert_eq!(s.settle_step(1).unwrap(), Progress::Done);
    }

    #[test]
    fn evicted_records_read_through_from_pages() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            s.apply(&[
                Write::Put(nonunique(1), rec(SH, b"settled")),
                Write::Put(nonunique(2), rec(SH, b"also")),
            ])
            .await
            .unwrap();
            s.drain().unwrap(); // settle into pages
            // Simulate eviction: drop the cache; the settled pages must serve.
            TEST_SLOT.mem_cache().write().clear();
            assert_eq!(
                s.get_of(SH, nonunique(1)).await.unwrap(),
                Some(rec(SH, b"settled")),
                "typed read must fall through to the page"
            );
            assert_eq!(
                s.get(nonunique(2)).await.unwrap(),
                Some(rec(SH, b"also")),
                "untyped read must fall through to the page"
            );
            // Absent ids stay absent through the fallback.
            assert_eq!(s.get(nonunique(99)).await.unwrap(), None);
            assert_eq!(s.get_of(SH, nonunique(99)).await.unwrap(), None);
        });
    }

    #[test]
    fn remove_of_evicted_record_reaches_its_page() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(3), rec(SH, b"gone soon"))])
                    .await
                    .unwrap();
                s.drain().unwrap();
                // Evict, then remove: the owner must be found on the page.
                TEST_SLOT.mem_cache().write().clear();
                s.apply(&[Write::Remove(SH, nonunique(3))]).await.unwrap();
                // Before the settle lands, the tombstone must hide the
                // (still-present) page bytes…
                assert_eq!(s.get(nonunique(3)).await.unwrap(), None);
                assert_eq!(s.get_of(SH, nonunique(3)).await.unwrap(), None);
                // …and after it, the page itself agrees.
                s.drain().unwrap();
                assert_eq!(s.get(nonunique(3)).await.unwrap(), None);
            }
            // And the journal agrees on replay.
            let s = open(d.path());
            assert_eq!(s.get(nonunique(3)).await.unwrap(), None);
            assert_eq!(s.cache_len(), 0);
        });
    }

    #[test]
    fn second_open_in_process_is_engine_busy() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let first = open(d.path());
        let d2 = tempfile::tempdir().unwrap();
        assert!(matches!(
            PageStore::open(d2.path(), &[&TEST_SLOT]).unwrap_err(),
            StorageError::EngineBusy
        ));
        drop(first);
        // Slot released on drop — opening again succeeds.
        let _second = open(d.path());
    }

    #[test]
    fn unsettled_writes_read_correctly_and_survive_reopen() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"queued"))])
                    .await
                    .unwrap();
                assert!(s.has_pending(), "settle must be deferred");
                assert_eq!(
                    s.get(nonunique(1)).await.unwrap(),
                    Some(rec(SH, b"queued")),
                    "the cache serves an unsettled write"
                );
                assert_eq!(s.bucket_count(SH), 0, "no page written yet");
            } // dropped with the queue never drained
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"queued")),
                "the journal covers unsettled writes"
            );
        });
    }

    #[test]
    fn tombstone_hides_stale_page_until_settle() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            s.apply(&[Write::Put(nonunique(9), rec(SH, b"v1"))])
                .await
                .unwrap();
            s.drain().unwrap(); // page holds v1
            s.apply(&[Write::Remove(SH, nonunique(9))]).await.unwrap();
            // Unsettled remove: the page still holds v1; reads must not
            // resurrect it.
            assert_eq!(s.get(nonunique(9)).await.unwrap(), None);
            // A re-put supersedes the tombstone immediately.
            s.apply(&[Write::Put(nonunique(9), rec(SH, b"v2"))])
                .await
                .unwrap();
            assert_eq!(
                s.get(nonunique(9)).await.unwrap(),
                Some(rec(SH, b"v2"))
            );
            s.drain().unwrap();
            assert_eq!(
                s.get(nonunique(9)).await.unwrap(),
                Some(rec(SH, b"v2"))
            );
        });
    }

    #[test]
    fn evict_settled_respects_budget_and_pending() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            s.apply(&[Write::Put(nonunique(1), rec(SH, b"resident"))])
                .await
                .unwrap();
            // Pending queue non-empty ⇒ eviction must refuse.
            s.evict_settled(0);
            assert!(s.cache_len() > 0, "unsettled entries must not evict");
            s.drain().unwrap();
            s.evict_settled(0);
            assert_eq!(s.cache_len(), 0, "settled entries evict to budget");
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"resident")),
                "evicted reads fall through to the page"
            );
        });
    }

    /// The journal files under a store dir, sorted by ts.
    fn journals(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        crate::journal::scan(dir)
            .unwrap()
            .into_iter()
            .map(|(_, p)| p)
            .collect()
    }

    #[test]
    fn commit_retires_the_old_journal_and_reopen_is_cold() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"kept"))])
                    .await
                    .unwrap();
                s.apply(&[
                    Write::Put(nonunique(2), rec(SH, b"doomed")),
                    Write::Put(nonunique(3), rec(SH, b"kept too")),
                ])
                .await
                .unwrap();
                s.apply(&[Write::Remove(SH, nonunique(2))]).await.unwrap();
                s.commit_journal().unwrap();
                // The frame is written but unsynced, so the retirement it
                // authorises is still pending (RFC 0046) — the old journal
                // stays until something makes the frame durable.
                assert!(s.is_retiring());
                assert_eq!(journals(d.path()).len(), 2);
                s.force_retirement().unwrap();
                assert!(!s.is_retiring());
                assert_eq!(
                    journals(d.path()).len(),
                    1,
                    "the retired journal must be deleted once the frame is \
                     durable"
                );
            }
            let s = open(d.path());
            assert_eq!(s.cache_len(), 0, "a committed open starts cold");
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"kept"))
            );
            assert_eq!(
                s.get_of(SH, nonunique(3)).await.unwrap(),
                Some(rec(SH, b"kept too"))
            );
            assert_eq!(
                s.get(nonunique(2)).await.unwrap(),
                None,
                "a pre-commit remove must hold after restore"
            );
        });
    }

    #[test]
    fn writes_after_commit_replay_over_it() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"old"))])
                    .await
                    .unwrap();
                s.commit_journal().unwrap();
                // Post-commit traffic lands in the fresh journal.
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"new"))])
                    .await
                    .unwrap();
                s.apply(&[Write::Put(nonunique(4), rec(SH, b"tail"))])
                    .await
                    .unwrap();
            }
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"new")),
                "the journal tail must win over committed state"
            );
            assert_eq!(
                s.get(nonunique(4)).await.unwrap(),
                Some(rec(SH, b"tail"))
            );
        });
    }

    #[test]
    fn covered_journal_left_behind_is_skipped_and_cleaned() {
        // Simulates a crash between the Commit frame and the old journal's
        // delete: the covered journal is still on disk; recovery must skip
        // it (its frames are already in data.bin) and clean it up.
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            let (old_path, old_bytes) = {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"v1"))])
                    .await
                    .unwrap();
                s.apply(&[Write::Put(nonunique(1), rec(SH, b"v2"))])
                    .await
                    .unwrap();
                s.apply(&[Write::Put(nonunique(5), rec(SH, b"other"))])
                    .await
                    .unwrap();
                s.apply(&[Write::Remove(SH, nonunique(5))]).await.unwrap();
                let old = journals(d.path()).pop().unwrap();
                let bytes = std::fs::read(&old).unwrap();
                s.commit_journal().unwrap();
                (old, bytes)
            };
            // "Un-delete" the covered journal.
            std::fs::write(&old_path, &old_bytes).unwrap();
            assert_eq!(journals(d.path()).len(), 2);
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"v2"))
            );
            assert_eq!(s.get(nonunique(5)).await.unwrap(), None);
            assert_eq!(
                journals(d.path()).len(),
                1,
                "recovery must clean the covered leftover"
            );
        });
    }

    /// The deferral's safety net (RFC 0046): a `Commit` frame written without
    /// a barrier may simply never reach the disk. Because the retirement waits
    /// for it, the retired journal is still there — and every acked write
    /// replays out of it.
    #[test]
    fn a_deferred_commit_that_never_lands_replays_the_retained_journal() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                for k in 0..5u64 {
                    s.apply(&[Write::Put(nonunique(k), rec(SH, b"acked"))])
                        .await
                        .unwrap();
                }
                s.commit_journal().unwrap();
                assert!(s.is_retiring(), "the frame must still be pending");
            }
            // The unsynced frame never made it: the new journal comes back
            // empty, exactly as a crash in that window would leave it.
            let mut files = journals(d.path());
            assert_eq!(files.len(), 2, "the retired journal must be retained");
            let newest = files.pop().unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&newest)
                .unwrap()
                .set_len(0)
                .unwrap();

            let s = open(d.path());
            for k in 0..5u64 {
                assert_eq!(
                    s.get(nonunique(k)).await.unwrap(),
                    Some(rec(SH, b"acked")),
                    "record {k} lost with the deferred commit"
                );
            }
        });
    }

    #[test]
    fn torn_commit_frame_falls_back_to_the_old_journal() {
        // Simulates a crash mid-Commit-append: the frame is torn (crc
        // fails) and the retired journal was never deleted — recovery must
        // ignore the torn commit and rebuild from the old journal's frames.
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            let (old_path, old_bytes) = {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(7), rec(SH, b"survives"))])
                    .await
                    .unwrap();
                let old = journals(d.path()).pop().unwrap();
                let bytes = std::fs::read(&old).unwrap();
                s.commit_journal().unwrap();
                (old, bytes)
            };
            // Tear the Commit frame (drop its last byte) and un-delete the
            // old journal — the exact crash-mid-append disk state.
            let new_path = journals(d.path()).pop().unwrap();
            let len = std::fs::metadata(&new_path).unwrap().len();
            std::fs::OpenOptions::new()
                .write(true)
                .open(&new_path)
                .unwrap()
                .set_len(len - 1)
                .unwrap();
            std::fs::write(&old_path, &old_bytes).unwrap();
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(7)).await.unwrap(),
                Some(rec(SH, b"survives")),
                "the old journal must rule when the commit is torn"
            );
        });
    }

    #[test]
    fn missing_journal_with_data_refuses_to_open() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            let s = open(d.path());
            s.apply(&[Write::Put(nonunique(1), rec(SH, b"x"))])
                .await
                .unwrap();
        });
        for j in journals(d.path()) {
            std::fs::remove_file(j).unwrap();
        }
        assert!(matches!(
            PageStore::open(d.path(), &[&TEST_SLOT]).unwrap_err(),
            StorageError::Corrupt(_)
        ));
    }

    #[test]
    fn commit_restores_dictionary_compressed_pages() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        // Repetitive bodies → the dictionary warms and pages store Zstd, so
        // reading them back after restore needs the persisted dictionary.
        let body = |k: u64| {
            let mut v = vec![b'A'; 1024];
            v.extend_from_slice(&k.to_le_bytes());
            rec(SH, &v)
        };
        block_on(async {
            {
                let s = open(d.path());
                for k in 0..50u64 {
                    s.apply(&[Write::Put(nonunique(k), body(k))])
                        .await
                        .unwrap();
                }
                s.commit_journal().unwrap();
            }
            let s = open(d.path());
            assert_eq!(s.cache_len(), 0);
            for k in 0..50u64 {
                assert_eq!(
                    s.get_of(SH, nonunique(k)).await.unwrap(),
                    Some(body(k)),
                    "record {k} must decompress against the restored dict"
                );
            }
            // The store keeps working: settle + a second commit over the
            // restored state (untouched roots carry forward).
            s.apply(&[Write::Put(nonunique(60), body(60))])
                .await
                .unwrap();
            s.commit_journal().unwrap();
        });
        // Third generation open still serves everything.
        block_on(async {
            let s = open(d.path());
            assert!(s.get_of(SH, nonunique(60)).await.unwrap().is_some());
            assert_eq!(
                s.get_of(SH, nonunique(0)).await.unwrap(),
                Some(body(0))
            );
        });
    }

    /// ~1 KiB of pseudo-random (zstd-incompressible) bytes — pages must grow
    /// past the split threshold by their **stored** (compressed) size, so
    /// compressible filler would never trigger a split.
    fn noise(k: u64) -> Vec<u8> {
        let mut state = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..1024)
            .map(|_| {
                // xorshift64
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    /// `noise`, at an arbitrary length — bytes the dictionary cannot collapse
    /// as long as each call gets a distinct seed.
    fn incompressible(len: usize, seed: u64) -> Vec<u8> {
        let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..len)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect()
    }

    /// Splitting distributes whole records; it cannot divide one. A record
    /// larger than the target leaves its bucket permanently over target, and
    /// RFC 0049 makes that a supported outcome — the page spans more blocks —
    /// rather than a reason to keep splitting.
    ///
    /// Under the pre-0049 trigger ("any planned page over threshold") this
    /// burned the whole 64-split budget on *every* round that touched the
    /// bucket, growing the directory by 64 buckets each time and never
    /// relieving anything.
    #[test]
    fn a_record_larger_than_the_target_does_not_drive_splits() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let mut s = open(d.path());
        s.target_blocks_per_bucket = 1; // 4 KiB
        let mut big = Vec::new();
        block_on(async {
            for round in 0..4u64 {
                // A fresh body each round, so the dictionary cannot learn it
                // and shrink the page out of the interesting range.
                big = rec(SH, &incompressible(32 * 1024, round + 7));
                s.apply(&[Write::Put(nonunique(1), big.clone())])
                    .await
                    .unwrap();
                s.drain().unwrap();
                assert!(
                    s.bucket_count(SH) < 16,
                    "round {round}: the directory grew to {} buckets chasing \
                     a record no split can divide",
                    s.bucket_count(SH)
                );
            }
            assert_eq!(s.get(nonunique(1)).await.unwrap(), Some(big.clone()));
        });
    }

    /// The other half of RFC 0049's rule, unchanged by it: a bucket genuinely
    /// over target *is* split when its turn comes, so ordinary growth still
    /// grows the table and every record survives the repartition.
    #[test]
    fn many_records_trigger_split_and_stay_readable() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            // Each record ~1 KiB; enough of them overflow a bucket and split.
            for k in 0..200u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, &noise(k)))])
                    .await
                    .unwrap();
            }
            s.drain().unwrap(); // splits happen on the settle path
            assert!(
                s.bucket_count(SH) > 1,
                "expected at least one bucket split"
            );
            for k in 0..200u64 {
                assert_eq!(
                    s.get(nonunique(k)).await.unwrap(),
                    Some(rec(SH, &noise(k))),
                    "record {k} lost"
                );
            }
        });
        drop(s);
        // And it all survives a rebuild from the journal.
        let s = open(d.path());
        block_on(async {
            assert_eq!(s.cache_len(), 200);
            assert_eq!(
                s.get(nonunique(199)).await.unwrap().unwrap().len(),
                8 + 1024
            );
        });
    }

    /// The default is the promise the engine was built around, and the one
    /// thing a durability window must never quietly change (RFC 0061).
    #[test]
    fn a_zero_window_takes_one_barrier_per_batch() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path()); // StoreOptions::default()
        let before = s.journal.lock().barriers();
        block_on(async {
            for k in 0..8u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"strict"))])
                    .await
                    .unwrap();
            }
        });
        assert_eq!(s.journal.lock().barriers(), before + 8);
    }

    /// The whole point of the window: a burst inside one costs no barrier,
    /// and `flush` buys the strong answer back for exactly one.
    #[test]
    fn a_burst_inside_one_window_costs_one_barrier() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        let s = PageStore::open_with(
            d.path(),
            &[&TEST_SLOT],
            StoreOptions {
                // Long enough that nothing elapses during the test — the
                // window's *timing* is the clock's business, not this test's.
                relax_window: Duration::from_hours(1),
                ..StoreOptions::default()
            },
        )
        .unwrap();
        let before = s.journal.lock().barriers();
        block_on(async {
            for k in 0..32u64 {
                s.apply(&[Write::Put(nonunique(k), rec(SH, b"windowed"))])
                    .await
                    .unwrap();
            }
        });
        assert_eq!(
            s.journal.lock().barriers(),
            before,
            "32 batches inside one window must take no barrier"
        );
        s.flush().unwrap();
        assert_eq!(s.journal.lock().barriers(), before + 1);
    }

    /// Elapsed window ⇒ the barrier is taken, and what was acked is on the
    /// platter: a reopen replays it.
    #[test]
    fn an_elapsed_window_syncs_and_the_batch_replays() {
        let _g = engine_gate();
        let d = tempfile::tempdir().unwrap();
        {
            let s = PageStore::open_with(
                d.path(),
                &[&TEST_SLOT],
                // Elapsed before the first append finishes writing.
                StoreOptions {
                    relax_window: Duration::from_nanos(1),
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            let before = s.journal.lock().barriers();
            block_on(s.apply(&[Write::Put(nonunique(7), rec(SH, b"elapsed"))]))
                .unwrap();
            assert_eq!(s.journal.lock().barriers(), before + 1);
        }
        let s = open(d.path());
        assert_eq!(
            block_on(s.get(nonunique(7))).unwrap(),
            Some(rec(SH, b"elapsed"))
        );
    }
}
