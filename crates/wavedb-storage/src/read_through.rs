//! The read path (split from [`crate::page_store`] for the file budget):
//! the record cache, then the settled page on disk, then — for a bucket
//! evicted from disk (RFC 0070 §7) — the object store.
//!
//! The first two are synchronous and run when the read is *called*, so a
//! disk-only store's read future is ready at once and never suspends. Only
//! the third awaits, and only in an object-backed store, which a node uses
//! under its owner lock (RFC 0070 §14).
//!
//! There used to be an `owner_of` here too — "which slot holds this id?",
//! answered by probing every one of them. It went with RFC 0063: `Remove`
//! and `Expect` now carry the STRUCT_HASH they route by, so nothing has to
//! search for an id's type any more.

use wavedb_core::Id;

use crate::block::Run;
use crate::error::{StorageError, StorageResult};
use crate::page::SlotPage;
use crate::page_cache::Lane;
use crate::page_store::PageStore;
use crate::pages::{EVICTED, Located, key_of_id, locate, with_page};
use crate::record_cache::Lookup;

/// What the synchronous half of a read found.
pub enum Settled {
    /// The answer: the bytes, or absent.
    Found(Option<Vec<u8>>),
    /// The key's disk page (of slot `idx`, found by `page`) was evicted:
    /// the object store holds the answer.
    Evicted { idx: usize, page: Vec<u8> },
}

/// What the synchronous half of an untyped read found.
pub enum SettledAny {
    /// The answer: the bytes, or absent from every type.
    Found(Option<Vec<u8>>),
    /// Absent from every page on disk; these `(slot, page key)`s were
    /// evicted.
    Evicted(Vec<(usize, Vec<u8>)>),
}

impl PageStore {
    /// Read `key` from slot `idx`'s settled page — the fallback when the
    /// cache does not hold it. Absent when the type has never settled
    /// anything or the page does not hold the key. A remove not yet settled
    /// never reaches here: its tombstone is in the cache, which answers
    /// first.
    pub(crate) fn read_from_pages(
        &self,
        idx: usize,
        key: &[u8],
    ) -> StorageResult<Settled> {
        let slot = &self.types[idx];
        let index = slot.directory().borrow();
        let Some(index) = index.as_ref() else {
            return Ok(Settled::Found(None));
        };
        let Located::Page { run, flags, at } = locate(index, key) else {
            return Ok(Settled::Found(None));
        };
        if flags & EVICTED != 0 {
            let page = index.key_of(at).unwrap_or_default();
            return Ok(Settled::Evicted { idx, page });
        }
        self.touch(idx, run.start);
        let dict = slot.dictionary().borrow();
        let bytes = with_page(&self.file, run, &dict, |p| {
            p.get(key).map(<[u8]>::to_vec)
        })?;
        Ok(Settled::Found(bytes))
    }

    /// The synchronous half of a typed read: the record cache, then the
    /// settled page — one binary search on the route table and at most one
    /// page read, whatever the schema's width.
    ///
    /// An unregistered `struct_hash` reads as absent rather than faulting:
    /// [`route_batch`](Self::route_batch) has already refused any batch that
    /// would reach here with one.
    pub(crate) fn read_now(
        &self,
        struct_hash: u64,
        key: &[u8],
    ) -> StorageResult<Settled> {
        #[cfg(feature = "probe")]
        let start = std::time::Instant::now();
        let Some(idx) = self.slot_index(struct_hash) else {
            return Ok(Settled::Found(None));
        };
        let cached = self.cache.borrow().get(idx, key);
        #[cfg(feature = "probe")]
        crate::probe::record_lookup(&cached);
        let found = match cached {
            Lookup::Hit(bytes) => Ok(Settled::Found(Some(bytes))),
            Lookup::Removed => Ok(Settled::Found(None)),
            Lookup::Miss => self.read_from_pages(idx, key),
        };
        #[cfg(feature = "probe")]
        crate::probe::read_done(
            u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX),
            || self.probe_census(),
        );
        found
    }

    /// The caches as the probe reports them: the record cache's census and
    /// the page cache's size.
    #[cfg(feature = "probe")]
    fn probe_census(&self) -> String {
        let pages = self.file.page_cache();
        format!(
            "{} | page_cache KiB={} pages={}",
            self.cache.borrow().census(),
            pages.bytes() >> 10,
            pages.len(),
        )
    }

    /// Close the probe's read counters at a phase boundary, naming the
    /// phase that just ended (the `probe` feature only).
    #[cfg(feature = "probe")]
    pub fn probe_mark(&self, phase: &str) {
        crate::probe::mark(phase, || self.probe_census());
    }

    /// Finish a read [`read_now`](Self::read_now) began: at the object store
    /// when the bucket was evicted.
    pub(crate) async fn read_rest(
        &self,
        now: StorageResult<Settled>,
        key: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        match now? {
            Settled::Found(bytes) => Ok(bytes),
            Settled::Evicted { idx, page } => {
                self.read_evicted(idx, page, key).await
            }
        }
    }

    /// The read the `Expect` guard makes, under the commit: synchronous, so
    /// a batch reaching it has had every evicted bucket it writes restored
    /// first (`Store::apply`).
    pub(crate) fn read_of(
        &self,
        struct_hash: u64,
        key: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        match self.read_now(struct_hash, key)? {
            Settled::Found(bytes) => Ok(bytes),
            Settled::Evicted { .. } => {
                Err(StorageError::Corrupt("a guard read an evicted bucket"))
            }
        }
    }

    /// Read `id` without knowing its type: the record cache, then every
    /// settled page, then the object store for every type whose bucket was
    /// evicted — the body of [`Store::get`](wavedb_core::Store::get).
    ///
    /// The cost is a page read per registered type, so nothing on a write
    /// path may call this (RFC 0063). `Store::get` keeps it because an
    /// untyped read is what that method *means*; typed callers use
    /// `get_of`.
    pub(crate) fn read_any_now(&self, key: &[u8]) -> StorageResult<SettledAny> {
        let cached = self.cache.borrow().get_any(key);
        match cached {
            Lookup::Hit(bytes) => return Ok(SettledAny::Found(Some(bytes))),
            Lookup::Removed => return Ok(SettledAny::Found(None)),
            Lookup::Miss => {}
        }
        let mut evicted = Vec::new();
        for idx in 0..self.types.len() {
            match self.read_from_pages(idx, key)? {
                Settled::Found(Some(bytes)) => {
                    return Ok(SettledAny::Found(Some(bytes)));
                }
                Settled::Found(None) => {}
                Settled::Evicted { idx, page } => evicted.push((idx, page)),
            }
        }
        Ok(if evicted.is_empty() {
            SettledAny::Found(None)
        } else {
            SettledAny::Evicted(evicted)
        })
    }

    /// Finish an untyped read at the object store, type by type.
    pub(crate) async fn read_any_rest(
        &self,
        now: StorageResult<SettledAny>,
        key: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        let evicted = match now? {
            SettledAny::Found(bytes) => return Ok(bytes),
            SettledAny::Evicted(evicted) => evicted,
        };
        for (idx, page) in evicted {
            if let Some(bytes) = self.read_evicted(idx, page, key).await? {
                return Ok(Some(bytes));
            }
        }
        Ok(None)
    }
}

impl PageStore {
    /// `id`'s bytes as the object store holds them — the uploaded page
    /// covering it, filtered to it. `None` when no uploaded page holds it.
    ///
    /// Nothing is borrowed across the request: the descriptor is copied out
    /// of the remote directory first, and the dictionary is borrowed only to
    /// decode the bytes that came back (§14).
    ///
    /// # Errors
    /// An object-store request failing, the object or range missing, or a
    /// page that fails to decode; [`StorageError::NotObjectBacked`] for a
    /// disk-only store.
    pub async fn read_uploaded(
        &self,
        struct_hash: u64,
        id: Id,
    ) -> StorageResult<Option<Vec<u8>>> {
        self.object()?;
        match self.slot_index(struct_hash) {
            Some(idx) => self.read_remote(idx, &key_of_id(id)).await,
            None => Ok(None),
        }
    }

    /// `key`'s bytes in the uploaded page of slot `idx` that covers it.
    ///
    /// A compaction may carry the page forward and delete its file while
    /// the request is out: a fetch that fails when the descriptor has moved
    /// meanwhile is made again at the new one.
    pub(crate) async fn read_remote(
        &self,
        idx: usize,
        key: &[u8],
    ) -> StorageResult<Option<Vec<u8>>> {
        let mut run = self.remote_run(idx, key);
        loop {
            let Some(at) = run else {
                return Ok(None);
            };
            let read = |page: &SlotPage| page.get(key).map(<[u8]>::to_vec);
            match self.with_remote_page(idx, at, read).await {
                Ok(bytes) => return Ok(bytes),
                Err(e) => {
                    let now = self.remote_run(idx, key);
                    if now == run {
                        return Err(e);
                    }
                    run = now;
                }
            }
        }
    }

    /// Lend the uploaded page of slot `idx` at `run` to `read` —
    /// held by the page cache in the object store's lane, or fetched,
    /// decoded and then held, decoded (RFC 0044). Nothing is borrowed across
    /// the request.
    ///
    /// # Errors
    /// The fetch's faults, and a page that fails to decode.
    pub(crate) async fn with_remote_page<R>(
        &self,
        idx: usize,
        run: Run,
        read: impl FnOnce(&SlotPage) -> R,
    ) -> StorageResult<R> {
        let cache = self.file.page_cache();
        let read = match cache.with(Lane::Remote, run, read) {
            Ok(done) => return Ok(done),
            Err(read) => read,
        };
        let image = self.fetch_image(run).await?;
        let page = self.decode_remote(idx, &image)?;
        let done = read(&page);
        cache.insert(Lane::Remote, run, page);
        Ok(done)
    }

    /// The run of the uploaded page of slot `idx` covering `key`, if one
    /// does.
    fn remote_run(&self, idx: usize, key: &[u8]) -> Option<Run> {
        let remote = self.types[idx].remote_directory().borrow();
        match locate(remote.as_ref()?, key) {
            Located::Page { run, .. } => Some(run),
            Located::Absent => None,
        }
    }
}
