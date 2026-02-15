//! [`PageStore`] — the node's authoritative [`Store`](wavedb_core::Store) backend.
//!
//! This is the disk-optimised key→value store the `Store`-generic
//! `Pivot`/`BpTree` layer in [`wavedb_core`] runs over. It ties together the
//! pieces built below it:
//!
//! - one slot **per registered type** — that type's own [`Directory`] over
//!   the shared [`BlockFile`] and its dictionary. The types are the
//!   `#[wavedb]`-generated [`StructStorage`] declarations handed in as an
//!   explicit registry at [`open`](PageStore::open): a compile-time list,
//!   routed once at open, no runtime registration. The slots belong to this
//!   store, so several stores can be open in one process, each over its own
//!   directory.
//! - the **record cache** (`record_cache`, RFC 0068) — one log of
//!   committed records per type, in chunks, and one index over them; reads
//!   serve from it, the settle writes from it, eviction drops whole chunks
//!   of it.
//! - the **settle cycle** (`crate::cycle`, RFC 0072) — every write is
//!   committed to the record cache and nothing else; a cycle settles what
//!   was committed into one window per round at the end of the active data
//!   file, each ending with a header naming the addresses it changed, and
//!   the batch is durable once its barrier is taken.
//!
//! ## Cells, not locks
//!
//! A store is used from one thread (RFC 0068), so its state sits in plain
//! cells. What used to be lock order is now borrow discipline: no step holds
//! a borrow across a call that borrows the same cell, and nothing that
//! suspends — an object-store request (RFC 0070) — holds one at all. The
//! commit path touches no directory (RFC 0063): every write carries the
//! STRUCT_HASH it routes by, so committing a batch is memory work.
//!
//! ## Reads
//!
//! The record cache answers when it indexes the id (bytes, or `None` for a
//! remove not yet settled); otherwise the settled page does
//! ([`Directory::get_record`]), and in an object-backed store an evicted
//! page's copy in the object store (`crate::read_through`). Only settled
//! chunks leave the cache, so a fallback sees the newest settled state.
//!
//! ## Durability and reconstruction
//!
//! A write is durable once the batch carrying it is: at the next settle — a
//! [`apply_durable`](PageStore::apply_durable) waits for it, an ordinary
//! `apply` does not. There is no journal: on [`open`](PageStore::open) the
//! newest whole batch's header, at the end of the newest data file, roots a
//! walk back through the headers that rebuilds every directory
//! (`crate::reopen`), the caches start empty, and nothing is replayed. A
//! store dropped settles what it holds first; a crash loses the batch not
//! yet durable.

pub use crate::store_options::{DEFAULT_RECORD_CACHE_BYTES, StoreOptions};
use std::path::Path;

use std::cell::RefCell;

use crate::block_file::BlockFile;
use crate::directory::Directory;
use crate::error::StorageResult;
use crate::manifest::Manifest;
use crate::struct_storage::{StructStorage, TypeSlot};

/// How many blocks a bucket should hold. It is a **target**, not a limit: a
/// bucket over it is split when its turn comes round, and simply spans more
/// blocks until then (RFC 0049). Tunable.
const DEFAULT_TARGET_BLOCKS_PER_BUCKET: u64 = 8; // 32 KiB

/// The native, page-backed [`Store`](wavedb_core::Store).
///
/// One instance per data directory: the directory's files are locked
/// while it is open. Per-type state lives in the store's own slots, one per
/// [`StructStorage`] registered at [`open`](Self::open), so stores over
/// different directories share nothing and can be open side by side.
#[derive(Debug)]
pub struct PageStore {
    /// The data files — in RAM in object mode, which has no disk
    /// (RFC 0070 §9).
    pub(crate) file: BlockFile,
    /// Where its windows go: its own files, or a storage thread's
    /// (RFC 0073 §2).
    pub(crate) storage: crate::link::Storage,
    /// Where the disk's chain of headers stands (RFC 0072 §4).
    pub(crate) chain: RefCell<crate::disk_chain::DiskChain>,
    /// This data shard's position in its storage shard — its place in the
    /// roots block and in its headers (RFC 0073 §5).
    pub(crate) position: u32,
    /// One round at a time through the directories (`crate::wait`).
    pub(crate) gate: crate::wait::Gate,
    /// What no header names yet (RFC 0072 §3).
    pub(crate) unrecorded: RefCell<crate::disk_chain::Unrecorded>,
    /// Where the batches stand (RFC 0072 §2).
    pub(crate) cycle: RefCell<crate::cycle::Cycle>,
    /// The registered types' state, sorted by `STRUCT_HASH` (deduped) — a
    /// lock-free, read-only route table after open.
    pub(crate) types: Vec<TypeSlot>,
    pub(crate) seed: [u64; 4],
    pub(crate) target_blocks_per_bucket: u64,
    /// Committed records, per type, in commit order — what reads serve from
    /// and a cycle settles ([`crate::record_cache`]).
    pub(crate) cache: RefCell<crate::record_cache::RecordCache>,
    /// The record-cache budget this store was opened with — carried, not
    /// applied (see [`StoreOptions::record_cache_bytes`]).
    pub(crate) record_cache_bytes: usize,
    /// The thread's `fsync` helper, once the store has one: batch barriers
    /// are then taken there ([`crate::barrier`]).
    pub(crate) barrier: Option<crate::barrier::Barrier>,
    /// Where a compaction is between its steps (RFC 0069).
    pub(crate) compaction: std::cell::Cell<crate::compact::Phase>,
    /// Through a storage thread: the files the running compaction empties
    /// (RFC 0073 §7).
    pub(crate) victims: RefCell<Vec<(u64, u64)>>,
    /// Blocks cut off the data files since open ([`reclaim`](Self::reclaim)).
    pub(crate) reclaimed: std::cell::Cell<u64>,
    /// The free share that makes a data file worth emptying (RFC 0071 §7).
    pub(crate) victim_free_percent: u8,
    /// The object store's side, when the pages live there (RFC 0070).
    pub(crate) object: Option<crate::remote::ObjectState>,
}

impl PageStore {
    /// Open (or create) a node store rooted at directory `dir`, holding
    /// its `MANIFEST` and `storage/00`, serving exactly the listed `types` (each
    /// a `#[wavedb]`-generated declaration — `T::storage_entries()`). The
    /// reserved `BpTree`-node slot is always included.
    ///
    /// The registry is an allowlist: a write whose `STRUCT_HASH` has no listed
    /// slot fails with [`StorageError::UnregisteredStructHash`](crate::StorageError::UnregisteredStructHash).
    ///
    /// # Errors
    /// [`StorageError::EngineBusy`](crate::StorageError::EngineBusy) if the
    /// directory is already open, in this process or another; otherwise any
    /// filesystem / corruption fault during open.
    pub fn open(
        dir: impl AsRef<Path>,
        types: &[&'static StructStorage],
    ) -> StorageResult<Self> {
        Self::open_with(dir, types, StoreOptions::default())
    }

    /// [`open`](Self::open), with the options answered explicitly.
    /// `StoreOptions::default()` is exactly [`open`](Self::open).
    ///
    /// `dir` is a database of **one** shard: its `MANIFEST` and
    /// `storage/00` ([`crate::shard_set`]), created when missing.
    ///
    /// # Errors
    /// The same faults as [`open`](Self::open);
    /// [`StorageError::Corrupt`](crate::StorageError::Corrupt) for a database
    /// of more shards.
    pub fn open_with(
        dir: impl AsRef<Path>,
        types: &[&'static StructStorage],
        options: StoreOptions,
    ) -> StorageResult<Self> {
        let dir = dir.as_ref();
        let facts = crate::manifest::resolve(
            Some(dir),
            options.remote.as_ref(),
            (1, 1),
        )?;
        let options = crate::shard_set::only_shard(&facts, options)?;
        Self::open_shard(
            &crate::shard_set::storage_dir(dir, 0),
            types,
            options,
            &facts,
        )
    }

    /// Open (or create) one shard of the database `facts` describe, in
    /// `dir`: reconstruct it from its data files' headers (RFC 0072 §6), and
    /// from the object store when the disk holds nothing of an object-backed
    /// one.
    pub(crate) fn open_shard(
        dir: &Path,
        types: &[&'static StructStorage],
        options: StoreOptions,
        facts: &Manifest,
    ) -> StorageResult<Self> {
        let file = BlockFile::open(
            dir,
            facts.seed(),
            facts.object_backed(),
            options.page_cache_bytes,
        )?;
        let had_files = file.has_files();
        let types = crate::struct_storage::slots_of(types);
        let registered: Vec<u64> =
            types.iter().map(TypeSlot::struct_hash).collect();
        // An object-backed store with no disk yet may have a database in its
        // object store already: it is recovered, never created over.
        let found =
            crate::recover::found_for(had_files, &options, &registered)?;
        let reopened = crate::reopen::reopen(
            &file,
            &types,
            options.file_bytes,
            facts.object_backed(),
        )?;
        let own = crate::link::Storage::Own(RefCell::new(reopened.alloc));
        let store = Self::assemble(file, types, own, options);
        *store.chain.borrow_mut() = reopened.chain;
        if let Some(object) = &store.object
            && let Some(record) = &reopened.remote_chain
        {
            *object.chain.borrow_mut() =
                crate::header::Chain::from_record(record);
        }
        store.prepare_object(had_files)?;
        if !facts.object_backed() {
            store.forget_remote(); // converted to the disk (`crate::convert`)
        }
        store.hold_directory()?;
        if let Some(found) = found {
            store.adopt(found)?;
        }
        Ok(store)
    }

    /// Keep a data file in the directory, empty if nothing was written yet:
    /// the data files' locks are what make a second open of it
    /// [`EngineBusy`](crate::StorageError::EngineBusy).
    fn hold_directory(&self) -> StorageResult<()> {
        if let Some(alloc) = self.own()
            && !self.file.has_files()
        {
            self.file.create(alloc.borrow().next_start())?;
        }
        Ok(())
    }

    /// A store over `file` and its reconstructed allocator, caches empty —
    /// what both opens build on.
    pub(crate) fn assemble(
        file: BlockFile,
        types: Vec<TypeSlot>,
        storage: crate::link::Storage,
        options: StoreOptions,
    ) -> Self {
        let type_count = types.len();
        Self {
            seed: file.seed(),
            file,
            storage,
            chain: RefCell::default(),
            position: 0,
            gate: crate::wait::Gate::default(),
            unrecorded: RefCell::default(),
            cycle: RefCell::default(),
            types,
            target_blocks_per_bucket: DEFAULT_TARGET_BLOCKS_PER_BUCKET,
            cache: RefCell::new(crate::record_cache::RecordCache::new(
                type_count,
                crate::block_file::random_seed(),
            )),
            record_cache_bytes: options.record_cache_bytes,
            barrier: None,
            compaction: std::cell::Cell::default(),
            victims: RefCell::default(),
            reclaimed: std::cell::Cell::default(),
            victim_free_percent: options.victim_free_percent,
            object: options.remote.map(crate::remote::ObjectState::new),
        }
    }

    /// Whether the pages live on a disk — every mode but object mode.
    pub(crate) const fn has_disk(&self) -> bool {
        !self.file.is_in_memory()
    }

    /// Number of records the cache indexes across every registered type —
    /// live ones, and removes not yet settled.
    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.cache.borrow().len()
    }

    /// Bytes the record cache holds — its index and every type's log, the
    /// number [`evict_settled`](Self::evict_settled) compares its budget to.
    #[must_use]
    pub fn cache_bytes(&self) -> usize {
        self.cache.borrow().footprint()
    }

    /// Bucket (page) count of one type's directory (`0` while nothing
    /// settled) — pages, not entries: aliases count once.
    #[must_use]
    pub fn bucket_count(&self, struct_hash: u64) -> usize {
        self.slot_of(struct_hash).map_or(0, |slot| {
            slot.directory()
                .borrow_mut()
                .as_ref()
                .map_or(0, Directory::pages)
        })
    }

    /// The registered slot for `struct_hash`, if listed at open.
    pub(crate) fn slot_of(&self, struct_hash: u64) -> Option<&TypeSlot> {
        self.types
            .binary_search_by_key(&struct_hash, TypeSlot::struct_hash)
            .ok()
            .and_then(|i| self.types.get(i))
    }
}

/// A store dropped settles what it holds and waits for it to be durable —
/// a close, not a crash. A fault then is lost with the store: what it held
/// is what a crash would have lost.
impl Drop for PageStore {
    fn drop(&mut self) {
        if self.has_disk() {
            let _ = self.drain();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PageStore, StoreOptions};
    use crate::Progress;
    use crate::error::StorageError;
    use crate::struct_storage::StructStorage;
    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};

    const SH: u64 = 0x1122_3344_5566_7788;
    const SH2: u64 = 0x99AA_BBCC_DDEE_FF00;

    /// The test slot every raw record routes to (`SH`-headed).
    static TEST_SLOT: StructStorage = StructStorage::new(SH);
    /// A second registered type, for the multi-slot paths.
    static OTHER_SLOT: StructStorage = StructStorage::new(SH2);

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
    /// A canary rather than a tautology: these two numbers are what every
    /// caller that never asked already runs on, so changing one is changing
    /// every deployment's engine — which should cost a failing test.
    #[test]
    fn default_options_are_todays_engine() {
        let o = StoreOptions::default();
        assert_eq!(o.page_cache_bytes, 64 << 20);
        assert_eq!(o.record_cache_bytes, 96 << 20);
    }

    /// Each budget reaches the layer that spends it, and `open` still means
    /// the default — a knob is only a knob where it lands.
    #[test]
    fn the_budgets_reach_the_layers_that_spend_them() {
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
    /// Asserted rather than assumed: a store that evicted on a schedule of
    /// its own would be quiescing
    /// writers at a moment nobody chose — inside a timed window, for the
    /// callers this budget exists for.
    #[test]
    fn the_store_never_evicts_records_until_asked() {
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
        assert!(
            s.cache_len() == 64,
            "settling 64 records must not evict them: the budget is the \
             caller's to apply"
        );
        s.evict_to_budget();
        assert!(
            s.cache_len() == 0,
            "asked, the store must evict to the budget it was opened with, \
             not to one remembered at the call site"
        );
    }

    /// Cache budgets are RAM, never bytes on disk. A store opened with **no**
    /// caches at all writes a file the default store reads back unchanged —
    /// which is why neither field folds into a `STRUCT_HASH`.
    #[test]
    fn a_store_opened_without_caches_writes_the_same_file() {
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

    /// RFC 0072's accounting: whatever a batch touched, settling it is
    /// **one** positioned write — pages and header together — plus one read
    /// per page it had to modify, and **one** barrier.
    #[test]
    fn a_batch_is_one_write_one_barrier_and_one_read_per_touched_page() {
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
            assert_eq!(sy - syncs, 1, "and the batch takes ONE barrier");
            let buckets = s.bucket_count(SH) as u64;
            assert!(
                r - reads <= buckets,
                "read {} pages for {buckets} buckets — one per touched page \
                 is the floor",
                r - reads
            );
        });
    }

    #[test]
    fn put_get_remove() {
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
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(7), rec(SH, b"durable"))])
                    .await
                    .unwrap();
            } // a drop settles what the store holds
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(7)).await.unwrap(),
                Some(rec(SH, b"durable")),
                "a closed store's writes survive a reopen"
            );
        });
    }

    #[test]
    fn reopen_reflects_removals() {
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
    // guard (the logs never hold one); a violated guard refuses the whole
    // batch as a typed conflict before anything is committed.
    #[test]
    fn expect_guard_commits_clean_and_conflicts_before_committing() {
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
    /// which matches nothing, paid a page read per declared type, on the
    /// path that serializes all writers. The guard now carries the
    /// STRUCT_HASH it routes by, so its cost is one lookup and cannot grow
    /// with the schema.
    #[test]
    fn a_guard_costs_the_same_whatever_the_schemas_width() {
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

    /// A round holds the pages it writes, decoded, and the next round takes
    /// them out instead of reading them back (RFC 0044): settling twice and
    /// reading every record once the record cache let it go costs no disk
    /// read at all.
    #[test]
    fn a_settled_page_is_read_from_ram() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            let before = s.file.io().snapshot().0;
            for round in 0..2u8 {
                for k in 0..64u64 {
                    let bytes = rec(SH, &[round; 8]);
                    s.apply(&[Write::Put(nonunique(k), bytes)]).await.unwrap();
                }
                s.drain().unwrap();
            }
            s.evict_settled(0);
            for k in 0..64u64 {
                assert_eq!(
                    s.get_of(SH, nonunique(k)).await.unwrap(),
                    Some(rec(SH, &[1; 8]))
                );
            }
            assert_eq!(
                s.file.io().snapshot().0,
                before,
                "the settles and the reads must find their pages in RAM"
            );
        });
    }

    #[test]
    fn unregistered_struct_hash_is_refused_before_committing() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        block_on(async {
            let err = s
                .apply(&[Write::Put(nonunique(1), rec(SH ^ 0xFF, b"stray"))])
                .await
                .unwrap_err();
            assert!(err.to_string().contains("no StructStorage registered"));
            // Nothing committed: a reopen stays empty.
        });
        drop(s);
        let s = open(d.path());
        assert_eq!(s.cache_len(), 0, "refused write must not reach the log");
    }

    /// A bounded cycle step takes its budget and leaves the rest queued —
    /// RFC 0063's interruptible drain, and the property a worker's
    /// priority valve needs in order to have anything to interleave between.
    ///
    /// The budget has to bite **within** one slot, not only across slots: a
    /// burst of one type is one slot, and that is the common case. A version
    /// that only split at slot boundaries would pass every other assertion
    /// here and settle all 20 records in one round.
    #[test]
    fn a_bounded_cycle_step_leaves_the_rest_queued() {
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
            assert!(s.begin_cycle().unwrap());
            assert_eq!(s.cycle_step(5).unwrap(), Progress::Settled);
            assert!(
                s.has_pending(),
                "a budget of 5 must not have settled all 20"
            );

            // Driving it to exhaustion reaches the same place `drain` would.
            // The round that ends the batch reports `Done`.
            let mut rounds = 1;
            loop {
                rounds += 1;
                assert!(rounds < 100, "step made no progress");
                if s.cycle_step(5).unwrap() == Progress::Done {
                    break;
                }
            }
            assert!(
                rounds >= 4,
                "20 ids at 5 per round is at least 4 rounds, took {rounds}"
            );
            assert!(!s.has_pending(), "the queue must end empty");

            // And the records are all readable from their pages.
            s.evict_settled(0);
            for n in 1..=20u64 {
                assert_eq!(
                    s.get_of(SH, nonunique(n)).await.unwrap(),
                    Some(rec(SH, b"x")),
                    "record {n} lost across the stepped settle"
                );
            }
        });
    }

    /// A tick with nothing to settle opens nothing — no window, no header,
    /// no barrier — and a step with no batch open is done.
    #[test]
    fn an_idle_tick_writes_nothing() {
        let d = tempfile::tempdir().unwrap();
        let s = open(d.path());
        let before = s.file.io().snapshot();
        assert!(!s.begin_cycle().unwrap());
        assert_eq!(s.cycle_step(usize::MAX).unwrap(), Progress::Done);
        s.drain().unwrap();
        assert_eq!(s.file.io().snapshot(), before);
    }

    #[test]
    fn evicted_records_read_through_from_pages() {
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
            // Evict: the settled pages must serve.
            s.evict_settled(0);
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
        let d = tempfile::tempdir().unwrap();
        block_on(async {
            {
                let s = open(d.path());
                s.apply(&[Write::Put(nonunique(3), rec(SH, b"gone soon"))])
                    .await
                    .unwrap();
                s.drain().unwrap();
                // Evict, then remove: the owner must be found on the page.
                s.evict_settled(0);
                s.apply(&[Write::Remove(SH, nonunique(3))]).await.unwrap();
                // Before the settle lands, the tombstone must hide the
                // (still-present) page bytes…
                assert_eq!(s.get(nonunique(3)).await.unwrap(), None);
                assert_eq!(s.get_of(SH, nonunique(3)).await.unwrap(), None);
                // …and after it, the page itself agrees.
                s.drain().unwrap();
                assert_eq!(s.get(nonunique(3)).await.unwrap(), None);
            }
            // And the reconstruction agrees.
            let s = open(d.path());
            assert_eq!(s.get(nonunique(3)).await.unwrap(), None);
            assert_eq!(s.cache_len(), 0);
        });
    }

    /// One directory, one store: a second open of it is refused while the
    /// first lives, and allowed once it drops.
    #[test]
    fn a_directory_open_twice_is_engine_busy() {
        let d = tempfile::tempdir().unwrap();
        let first = open(d.path());
        assert!(matches!(
            PageStore::open(d.path(), &[&TEST_SLOT]).unwrap_err(),
            StorageError::EngineBusy
        ));
        drop(first);
        let _second = open(d.path());
    }

    /// Two stores over two directories, open side by side, share nothing —
    /// not even for one declared type (RFC 0067: one store per shard).
    #[test]
    fn stores_over_two_directories_are_independent() {
        let (d1, d2) =
            (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        block_on(async {
            {
                let (a, b) = (open(d1.path()), open(d2.path()));
                a.apply(&[Write::Put(nonunique(1), rec(SH, b"in a"))])
                    .await
                    .unwrap();
                b.apply(&[Write::Put(nonunique(2), rec(SH, b"in b"))])
                    .await
                    .unwrap();
                a.drain().unwrap();
                assert_eq!(a.get(nonunique(2)).await.unwrap(), None);
                assert_eq!(b.get(nonunique(1)).await.unwrap(), None);
                assert_eq!(
                    b.get(nonunique(2)).await.unwrap(),
                    Some(rec(SH, b"in b"))
                );
            }
            // And each directory replays only its own.
            let (a, b) = (open(d1.path()), open(d2.path()));
            assert_eq!(
                a.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"in a"))
            );
            assert_eq!(a.get(nonunique(2)).await.unwrap(), None);
            assert_eq!(
                b.get(nonunique(2)).await.unwrap(),
                Some(rec(SH, b"in b"))
            );
        });
    }

    #[test]
    fn unsettled_writes_read_correctly_and_survive_reopen() {
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
            } // dropped with the queue never drained: the drop settles it
            let s = open(d.path());
            assert_eq!(
                s.get(nonunique(1)).await.unwrap(),
                Some(rec(SH, b"queued")),
                "a closed store settled its unsettled writes"
            );
        });
    }

    #[test]
    fn tombstone_hides_stale_page_until_settle() {
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

    #[test]
    fn a_reopen_restores_dictionary_compressed_pages() {
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
                s.drain().unwrap();
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
            // The store keeps working: a second batch over the restored
            // state (untouched addresses carry forward).
            s.apply(&[Write::Put(nonunique(60), body(60))])
                .await
                .unwrap();
            s.drain().unwrap();
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
        // And it all survives a reconstruction.
        let s = open(d.path());
        block_on(async {
            assert_eq!(s.cache_len(), 0, "a reconstruction starts cold");
            assert_eq!(
                s.get(nonunique(199)).await.unwrap().unwrap().len(),
                8 + 1024
            );
        });
    }
}
