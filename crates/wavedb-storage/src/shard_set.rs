//! A sharded database ([RFC 0067] §2): a `MANIFEST` and `shards/00` …
//! `shards/xx`, each shard a whole [`PageStore`] — its own data files,
//! headers, allocator, directories and dictionaries.
//!
//! The shard count is fixed when the database is created and read back from
//! the `MANIFEST` on every open ([`crate::manifest`]), because it is part of
//! every record's address. A caller's count is only the count a **new**
//! database is created with. A database of one shard has the same shape —
//! it is what [`PageStore::open`] opens.
//!
//! The shards share nothing, so each one is reconstructed from its own
//! headers and nothing about it ever spans two of them.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::path::{Path, PathBuf};

use crate::error::{StorageError, StorageResult};
use crate::manifest::{self, Manifest};
use crate::page_store::{PageStore, StoreOptions};
use crate::struct_storage::StructStorage;

/// Where the shards live, under the database's root.
pub const SHARDS_DIR: &str = "shards";

/// Open the sharded database at `dir`, creating it with `shards` shards if
/// it does not exist yet. Returns one store per shard, in shard order.
///
/// Recovery runs on as many threads as the machine offers
/// ([`open_shards_with`]).
///
/// # Errors
/// As [`open_shards_with`].
pub fn open_shards(
    dir: impl AsRef<Path>,
    types: &[&'static StructStorage],
    options: StoreOptions,
    shards: usize,
) -> StorageResult<Vec<PageStore>> {
    let threads = std::thread::available_parallelism().map_or(1, usize::from);
    open_shards_with(dir, types, options, shards, threads)
}

/// [`open_shards`], recovering on `threads` threads.
///
/// The shards share nothing, so each is reconstructed on its own: thread
/// `t` opens the contiguous range of shards `p` with `p · threads / P = t` —
/// the ranges a node's workers will own — and the database is open once the
/// slowest range is. The stores are returned in shard order whichever thread
/// opened them; they move to whoever owns them next.
///
/// The budgets in `options` are the database's: each shard gets an equal
/// share of the page cache and of the record-cache budget.
///
/// An object-backed database keeps a copy of the `MANIFEST` in its object
/// store as well, so a database whose disk is gone — or one reopened in
/// object mode — still knows its facts.
///
/// # Errors
/// [`StorageError::Corrupt`] for an unreadable `MANIFEST`, a shard count out
/// of `1..=256` or two `MANIFEST`s that disagree; the `MANIFEST`'s other
/// faults ([`manifest::resolve`]); otherwise the first fault, in shard order,
/// that opening a shard raised (a shard already open is
/// [`StorageError::EngineBusy`]). Every store that did open is dropped again
/// before this returns, so a retry finds them closed.
pub fn open_shards_with(
    dir: impl AsRef<Path>,
    types: &[&'static StructStorage],
    options: StoreOptions,
    shards: usize,
    threads: usize,
) -> StorageResult<Vec<PageStore>> {
    let dir = dir.as_ref();
    let facts = manifest::resolve(Some(dir), options.remote.as_ref(), shards)?;
    open_each(&facts, options, threads, |i, options| {
        PageStore::open_shard(&shard_dir(dir, i), types, options, &facts)
    })
}

/// [`open_shards_with`] in object mode ([`PageStore::open_object`]): no
/// disk, the `MANIFEST` in the object store alone.
///
/// # Errors
/// [`StorageError::NotObjectBacked`] without `options.remote`; otherwise
/// as [`open_shards_with`].
pub fn open_object_shards(
    types: &[&'static StructStorage],
    options: StoreOptions,
    shards: usize,
    threads: usize,
) -> StorageResult<Vec<PageStore>> {
    let remote = options
        .remote
        .as_ref()
        .ok_or(StorageError::NotObjectBacked)?;
    let facts = manifest::resolve(None, Some(remote), shards)?;
    open_each(&facts, options, threads, |_, options| {
        PageStore::open_object_shard(types, options, &facts)
    })
}

/// Shard 0 of the database `facts` describe, which must have exactly one —
/// what [`PageStore::open`] and [`PageStore::open_object`] open.
///
/// # Errors
/// [`StorageError::Corrupt`] for a database of more shards.
pub(crate) fn only_shard(
    facts: &Manifest,
    mut options: StoreOptions,
) -> StorageResult<StoreOptions> {
    if facts.shards() != 1 {
        return Err(StorageError::Corrupt("a sharded database: open_shards"));
    }
    options.remote = options.remote.map(|r| r.shard(0));
    Ok(options)
}

/// Open shards `0..facts.shards()` on `threads` threads with `open`, each
/// with its share of the budgets and its own prefix in the object store.
fn open_each(
    facts: &Manifest,
    options: StoreOptions,
    threads: usize,
    open: impl Fn(usize, StoreOptions) -> StorageResult<PageStore> + Sync,
) -> StorageResult<Vec<PageStore>> {
    let count = facts.shards();
    let share = |bytes: usize| bytes / count;
    let options = StoreOptions {
        page_cache_bytes: share(options.page_cache_bytes),
        record_cache_bytes: share(options.record_cache_bytes),
        ..options
    };
    let (options, open) = (&options, &open);
    let threads = threads.clamp(1, count);
    // Each shard keeps its objects under a prefix of its own, as it keeps
    // its files in a directory of its own.
    let open_range = |range: std::ops::Range<usize>| -> Vec<_> {
        range
            .map(|i| {
                let options = StoreOptions {
                    remote: options.remote.as_ref().map(|r| r.shard(i)),
                    ..options.clone()
                };
                open(i, options)
            })
            .collect()
    };
    let opened: Vec<StorageResult<PageStore>> = std::thread::scope(|s| {
        let ranges = (0..threads).map(|t| {
            (t * count).div_ceil(threads)..((t + 1) * count).div_ceil(threads)
        });
        // Collected on purpose: every range must be spawned before the first
        // join, or the lazy chain would spawn and join one range at a time.
        #[allow(clippy::needless_collect)]
        let running: Vec<_> = ranges
            .map(|range| s.spawn(move || open_range(range)))
            .collect();
        running
            .into_iter()
            .flat_map(|thread| {
                thread.join().unwrap_or_else(|_| {
                    vec![Err(StorageError::Corrupt(
                        "a shard's recovery panicked",
                    ))]
                })
            })
            .collect()
    });
    opened.into_iter().collect()
}

/// The directory of shard `i` under the database root `dir`.
#[must_use]
pub fn shard_dir(dir: &Path, i: usize) -> PathBuf {
    dir.join(SHARDS_DIR).join(format!("{i:02x}"))
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};

    use super::{open_shards, open_shards_with, shard_dir};
    use crate::error::StorageError;
    use crate::manifest::MANIFEST;
    use crate::{PageStore, StoreOptions, StructStorage};

    const SH: u64 = 0x0067_0005_4EC0_0E21;
    static SLOT: StructStorage = StructStorage::new(SH);

    fn id(k: u64) -> Id {
        Id::new(k, U48::from(1u32), false, 1)
    }

    fn put(k: u64) -> Write {
        let mut bytes = SH.to_le_bytes().to_vec();
        bytes.extend_from_slice(&k.to_le_bytes());
        Write::Put(id(k), bytes)
    }

    fn open_on(dir: &std::path::Path, threads: usize) -> Vec<PageStore> {
        open_shards_with(dir, &[&SLOT], StoreOptions::default(), 8, threads)
            .unwrap()
    }

    /// Reconstruction on three threads hands back every shard, in shard
    /// order, each from its own headers and no other.
    #[test]
    fn parallel_reconstruction_puts_every_shard_in_its_place() {
        let dir = tempfile::tempdir().unwrap();
        for (k, store) in (0u64..).zip(open_on(dir.path(), 1)) {
            block_on(store.apply(&[put(k)])).unwrap();
        } // dropped: each settles its own batch
        let stores = open_on(dir.path(), 3);
        assert_eq!(stores.len(), 8);
        for (i, store) in stores.iter().enumerate() {
            assert_eq!(
                store.file.dir.as_deref(),
                Some(shard_dir(dir.path(), i).as_path())
            );
            for k in 0..8u64 {
                let found = block_on(store.get_of(SH, id(k))).unwrap();
                assert_eq!(
                    found.is_some(),
                    k == i as u64,
                    "shard {i}, key {k}"
                );
            }
        }
    }

    /// One shard failing to open fails the database — and every shard that
    /// did open is closed again, so a retry finds them free.
    #[test]
    fn a_shard_that_fails_releases_the_others() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_on(dir.path(), 4));
        // Another process holding shard 5: its data file locked.
        let file = std::fs::read_dir(shard_dir(dir.path(), 5))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let held = std::fs::File::open(file).unwrap();
        held.try_lock().unwrap();
        let err = open_shards_with(
            dir.path(),
            &[&SLOT],
            StoreOptions::default(),
            8,
            4,
        )
        .unwrap_err();
        assert!(matches!(err, StorageError::EngineBusy), "{err:?}");
        drop(held);
        assert_eq!(open_on(dir.path(), 4).len(), 8);
    }

    #[test]
    fn a_new_database_records_its_shard_count() {
        let dir = tempfile::tempdir().unwrap();
        let stores =
            open_shards(dir.path(), &[], StoreOptions::default(), 4).unwrap();
        assert_eq!(stores.len(), 4);
        assert!(dir.path().join(MANIFEST).exists());
        for i in 0..4 {
            assert!(shard_dir(dir.path(), i).is_dir());
        }
    }

    /// The count is part of every record's address: a reopen keeps the one
    /// the database was created with, whatever the caller asks for.
    #[test]
    fn a_reopen_keeps_the_created_count() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_shards(dir.path(), &[], StoreOptions::default(), 4).unwrap());
        let stores =
            open_shards(dir.path(), &[], StoreOptions::default(), 64).unwrap();
        assert_eq!(stores.len(), 4);
    }

    #[test]
    fn a_torn_manifest_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_shards(dir.path(), &[], StoreOptions::default(), 2).unwrap());
        std::fs::write(dir.path().join(MANIFEST), [1, 2, 3]).unwrap();
        let err = open_shards(dir.path(), &[], StoreOptions::default(), 2)
            .unwrap_err();
        assert!(matches!(err, StorageError::BadMagic), "{err:?}");
    }

    /// One store is a database of one shard, the same shape on disk — and a
    /// database of more is not opened as one.
    #[test]
    fn a_single_store_is_a_one_shard_database() {
        let dir = tempfile::tempdir().unwrap();
        drop(PageStore::open(dir.path(), &[]).unwrap());
        assert!(dir.path().join(MANIFEST).exists());
        assert!(shard_dir(dir.path(), 0).is_dir());
        assert_eq!(
            open_shards(dir.path(), &[], StoreOptions::default(), 8)
                .unwrap()
                .len(),
            1
        );
        let sharded = tempfile::tempdir().unwrap();
        drop(open_shards(sharded.path(), &[], StoreOptions::default(), 2));
        let err = PageStore::open(sharded.path(), &[]).unwrap_err();
        assert!(matches!(err, StorageError::Corrupt(_)), "{err:?}");
    }

    #[test]
    fn a_shard_count_out_of_range_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        for bad in [0, 257] {
            let err =
                open_shards(dir.path(), &[], StoreOptions::default(), bad)
                    .unwrap_err();
            assert!(matches!(err, StorageError::Corrupt(_)), "{err:?}");
        }
    }

    /// Two opens of one database: the second finds its shards locked.
    #[test]
    fn a_database_open_twice_is_engine_busy() {
        let dir = tempfile::tempdir().unwrap();
        let _held =
            open_shards(dir.path(), &[], StoreOptions::default(), 2).unwrap();
        let err = open_shards(dir.path(), &[], StoreOptions::default(), 2)
            .unwrap_err();
        assert!(matches!(err, StorageError::EngineBusy), "{err:?}");
    }
}
