//! A sharded database ([RFC 0067] §2): a `MANIFEST` and `shards/00` …
//! `shards/xx`, each shard a whole [`PageStore`] — its own `data.bin`,
//! journal, allocator, directories and dictionaries.
//!
//! The shard count is fixed when the database is created and read back from
//! the `MANIFEST` on every open, because it is part of every record's address:
//! a record lives in shard `seahash(owner) % P`
//! ([`wavedb_core::placement`]), so opening with another `P` would look for
//! every record in the wrong place. A caller's count is only the count a
//! **new** database is created with.
//!
//! The shards share nothing, so each one replays its own journal and nothing
//! about recovery ever spans two of them.
//!
//! [RFC 0067]: ../../../../rfcs/0067-thread-per-core-shards-WIP.md

use std::fs;
use std::io::Write as _;
use std::path::Path;

use wavedb_wire::WaveWire;

use crate::error::{StorageError, StorageResult};
use crate::page_store::{PageStore, StoreOptions};
use crate::struct_storage::StructStorage;

/// The file naming the shard count, at the database's root.
pub const MANIFEST: &str = "MANIFEST";

/// Where the shards live, under the database's root.
pub const SHARDS_DIR: &str = "shards";

/// The most shards a database may have: a shard's directory is two hex
/// digits.
pub const MAX_SHARDS: usize = 256;

/// What the `MANIFEST` holds, as checked wire bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, WaveWire)]
struct Manifest {
    shards: u32,
}

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
/// The shards share nothing, so their journals replay independently: thread
/// `t` opens the contiguous range of shards `p` with `p · threads / P = t` —
/// the ranges a node's workers will own — and the database is open once the
/// slowest range is. The stores are returned in shard order whichever thread
/// opened them; they move to whoever owns them next.
///
/// The budgets in `options` are the database's: each shard gets an equal
/// share of the page cache and of the record-cache budget.
///
/// # Errors
/// [`StorageError::Corrupt`] for an unreadable `MANIFEST`, a shard count out
/// of `1..=256`, or a directory holding an unsharded store; otherwise the
/// first fault, in shard order, that opening a shard raised (a shard already
/// open is [`StorageError::EngineBusy`]). Every store that did open is
/// dropped again before this returns, so a retry finds them closed.
pub fn open_shards_with(
    dir: impl AsRef<Path>,
    types: &[&'static StructStorage],
    options: StoreOptions,
    shards: usize,
    threads: usize,
) -> StorageResult<Vec<PageStore>> {
    let dir = dir.as_ref();
    let count = match read_manifest(dir)? {
        Some(count) => count,
        None => create(dir, shards)?,
    };
    let share = |bytes: usize| bytes / count;
    let options = StoreOptions {
        page_cache_bytes: share(options.page_cache_bytes),
        record_cache_bytes: share(options.record_cache_bytes),
        ..options
    };
    let options = &options;
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
                PageStore::open_with(shard_dir(dir, i), types, options)
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
pub fn shard_dir(dir: &Path, i: usize) -> std::path::PathBuf {
    dir.join(SHARDS_DIR).join(format!("{i:02x}"))
}

/// The shard count the `MANIFEST` records, or `None` when there is none.
fn read_manifest(dir: &Path) -> StorageResult<Option<usize>> {
    let bytes = match fs::read(dir.join(MANIFEST)) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let manifest: Manifest = wavedb_wire::from_wire_checked(&bytes)
        .map_err(|_| StorageError::Corrupt("unreadable MANIFEST"))?;
    let count = usize::try_from(manifest.shards)
        .map_err(|_| StorageError::Corrupt("MANIFEST shard count"))?;
    checked(count).map(Some)
}

/// Create the database: the shard directories are created by opening them;
/// the `MANIFEST` is written last and atomically, so a crash part-way leaves
/// a directory that is created again from scratch rather than half a
/// database.
fn create(dir: &Path, shards: usize) -> StorageResult<usize> {
    let count = checked(shards)?;
    fs::create_dir_all(dir)?;
    if dir.join("data.bin").exists() {
        // An unsharded store from before RFC 0067 — unsupported, like any
        // older layout, and not to be silently shadowed by a new database.
        return Err(StorageError::Corrupt("unsharded data directory"));
    }
    let manifest = Manifest {
        shards: u32::try_from(count)
            .map_err(|_| StorageError::Corrupt("MANIFEST shard count"))?,
    };
    let tmp = dir.join(format!("{MANIFEST}.tmp"));
    let mut file = fs::File::create(&tmp)?;
    file.write_all(&wavedb_wire::to_wire_checked(&manifest))?;
    file.sync_all()?;
    fs::rename(&tmp, dir.join(MANIFEST))?;
    fs::File::open(dir)?.sync_all()?;
    Ok(count)
}

const fn checked(count: usize) -> StorageResult<usize> {
    if count == 0 || count > MAX_SHARDS {
        Err(StorageError::Corrupt("shard count out of 1..=256"))
    } else {
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use wavedb_core::{Id, Store, U48, Write};

    use super::{MANIFEST, open_shards, open_shards_with, shard_dir};
    use crate::error::StorageError;
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

    /// Recovery on three threads hands back every shard, in shard order,
    /// each having replayed its own journal and no other.
    #[test]
    fn parallel_recovery_replays_every_shard_into_its_place() {
        let dir = tempfile::tempdir().unwrap();
        for (k, store) in (0u64..).zip(open_on(dir.path(), 1)) {
            block_on(store.apply(&[put(k)])).unwrap();
        } // dropped unsettled: only the journals hold the records
        let stores = open_on(dir.path(), 3);
        assert_eq!(stores.len(), 8);
        for (i, store) in stores.iter().enumerate() {
            assert_eq!(store.data_dir, shard_dir(dir.path(), i));
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
        let held = PageStore::open(shard_dir(dir.path(), 5), &[&SLOT]).unwrap();
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
            assert!(shard_dir(dir.path(), i).join("data.bin").exists());
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
    fn a_torn_manifest_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        drop(open_shards(dir.path(), &[], StoreOptions::default(), 2).unwrap());
        std::fs::write(dir.path().join(MANIFEST), [1, 2, 3]).unwrap();
        let err = open_shards(dir.path(), &[], StoreOptions::default(), 2)
            .unwrap_err();
        assert!(matches!(err, StorageError::Corrupt(_)), "{err:?}");
    }

    #[test]
    fn an_unsharded_store_is_not_shadowed() {
        let dir = tempfile::tempdir().unwrap();
        drop(crate::PageStore::open(dir.path(), &[]).unwrap());
        let err = open_shards(dir.path(), &[], StoreOptions::default(), 2)
            .unwrap_err();
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
