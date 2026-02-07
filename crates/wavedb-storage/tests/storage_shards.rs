//! Data shards writing through shared storage shards ([RFC 0073]), proved
//! through the stores: every data shard of a storage shard settles into its
//! files through one thread, and a reopen — or a crash — finds every data
//! shard's root in the newest roots block, an idle one's included.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use std::path::{Path, PathBuf};

use common::copy_dir;
use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{
    BLOCK_SIZE, PageStore, StoreOptions, StructStorage, open_shards_with,
};

const SH: u64 = 0x0073_0003_5A4E_D001;
static SLOT: StructStorage = StructStorage::new(SH);

/// Four data shards through two storage shards.
fn open_sized(dir: &Path, file_blocks: u64) -> Vec<PageStore> {
    let options = StoreOptions {
        storage_shards: 2,
        file_bytes: file_blocks * BLOCK_SIZE as u64,
        ..StoreOptions::default()
    };
    open_shards_with(dir, &[&SLOT], options, 4, 2).expect("open")
}

fn open(dir: &Path) -> Vec<PageStore> {
    open_sized(dir, 1 << 20)
}

fn id(k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, 1)
}

fn record(k: u64, version: u8, len: usize) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.resize(16 + len, version);
    bytes
}

/// A record of 1 KiB nothing compresses: pages, not headers, fill the
/// files.
fn noise(k: u64) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    let mut x = k ^ 0x9E37_79B9_7F4A_7C15;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

fn put(s: &PageStore, k: u64, version: u8) {
    block_on(s.apply(&[Write::Put(id(k), record(k, version, 200))])).unwrap();
}

fn read(s: &PageStore, k: u64) -> Option<Vec<u8>> {
    block_on(s.get_of(SH, id(k))).unwrap()
}

/// Blocks the data files of every storage shard take.
fn disk_blocks(db: &Path) -> u64 {
    let mut blocks = 0;
    for shard in std::fs::read_dir(db.join("storage")).unwrap() {
        for file in std::fs::read_dir(shard.unwrap().path()).unwrap() {
            blocks += file.unwrap().metadata().unwrap().len() / 4096;
        }
    }
    blocks
}

/// The crash: the database as it is on disk now.
fn crash(dir: &Path) -> PathBuf {
    let to = dir.with_extension("crashed");
    copy_dir(dir, &to);
    to
}

/// Each data shard keeps its own records, through two storage shards'
/// files — and finds them again after a reopen.
#[test]
fn data_shards_share_storage_shards_and_reopen() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    {
        let stores = open(&db);
        for (i, s) in stores.iter().enumerate() {
            for k in 0..50 {
                put(s, i as u64 * 1_000 + k, 1);
            }
            s.drain().unwrap();
        }
    }
    let storage = std::fs::read_dir(db.join("storage")).unwrap().count();
    assert_eq!(storage, 2, "one directory per storage shard");
    assert!(!db.join("shards").exists());
    let stores = open(&db);
    for (i, s) in stores.iter().enumerate() {
        for k in 0..50 {
            let k = i as u64 * 1_000 + k;
            assert_eq!(read(s, k), Some(record(k, 1, 200)), "shard {i} {k}");
        }
        assert_eq!(read(s, (i as u64 + 1) % 4 * 1_000), None, "not its own");
    }
}

/// A data shard that wrote once and then idled while its neighbour wrote
/// batch after batch still roots in the newest roots block.
#[test]
fn an_idle_data_shards_root_is_in_the_newest_roots_block() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    {
        let stores = open(&db);
        put(&stores[1], 7, 9);
        stores[1].drain().unwrap();
        for round in 0..30u8 {
            for k in 0..20 {
                put(&stores[0], k, round);
            }
            stores[0].drain().unwrap();
        }
    }
    let stores = open(&db);
    assert_eq!(read(&stores[1], 7), Some(record(7, 9, 200)));
    for k in 0..20 {
        assert_eq!(read(&stores[0], k), Some(record(k, 29, 200)), "{k}");
    }
}

/// A durable write resolves once the storage write holding its batch is
/// durable; a crash right after keeps it, and every data shard's earlier
/// durable writes with it.
#[test]
fn a_durable_write_survives_a_crash() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let stores = open(&db);
    put(&stores[2], 1, 1);
    stores[2].drain().unwrap();
    let batch = [Write::Put(id(2), record(2, 1, 200))];
    block_on(stores[3].apply_durable(&batch)).unwrap();
    put(&stores[3], 3, 1); // not durable: in RAM only
    let crashed = open(&crash(&db));
    assert_eq!(read(&crashed[2], 1), Some(record(1, 1, 200)));
    assert_eq!(read(&crashed[3], 2), Some(record(2, 1, 200)));
    assert_eq!(read(&crashed[3], 3), None, "never settled");
}

/// Every data shard of a storage shard compacts: the evacuated files go,
/// and every record reads back, then and after a reopen.
#[test]
fn data_shards_evacuate_their_storage_shards_files() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let file_blocks = 64;
    let before;
    {
        let stores = open_sized(&db, file_blocks);
        for s in &stores {
            for k in 0..300 {
                let w = Write::Put(id(k), noise(k));
                block_on(s.apply(&[w])).unwrap();
            }
            s.drain().unwrap();
            for k in (0..300).filter(|k| k % 4 != 0) {
                block_on(s.apply(&[Write::Remove(SH, id(k))])).unwrap();
            }
            s.drain().unwrap();
        }
        before = disk_blocks(&db);
        for s in &stores {
            s.compact().unwrap();
        }
        // The storage threads reclaim with their next durable write.
        for s in &stores {
            put(s, 9_999, 1);
            s.drain().unwrap();
        }
        let after = disk_blocks(&db);
        assert!(after < before, "the files shrank: {before} → {after}");
        for s in &stores {
            for k in (0..300).step_by(4) {
                assert_eq!(read(s, k), Some(noise(k)), "{k}");
            }
        }
    }
    let stores = open_sized(&db, file_blocks);
    for s in &stores {
        for k in 0..300 {
            let want = (k % 4 == 0).then(|| noise(k));
            assert_eq!(read(s, k), want, "{k}");
        }
    }
}

/// The newest storage write torn — a page of one data shard's batch lost
/// before its `fsync` — gives way to the roots block before it, for every
/// data shard: the other one's root, durable then, still opens.
#[test]
fn a_torn_storage_write_gives_way_to_the_one_before() {
    use std::os::unix::fs::FileExt;
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let stores = open(&db);
    let file = db.join("storage/00/0000000000000000");
    put(&stores[0], 1, 1);
    stores[0].drain().unwrap();
    let first_end = std::fs::metadata(&file).unwrap().len();
    put(&stores[1], 2, 1);
    stores[1].drain().unwrap();
    let crashed = crash(&db);
    let torn = std::fs::OpenOptions::new()
        .write(true)
        .open(crashed.join("storage/00/0000000000000000"))
        .unwrap();
    torn.write_all_at(&[0; 4096], first_end).unwrap(); // its first page
    drop(torn);
    let reopened = open(&crashed);
    assert_eq!(read(&reopened[0], 1), Some(record(1, 1, 200)));
    assert_eq!(read(&reopened[1], 2), None, "its batch was never durable");
}

/// Dropping the last data shard of a storage shard returns once its thread
/// let the files go: the database opens again at once, every time.
#[test]
fn closing_frees_the_files_at_once() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    for round in 0..20u8 {
        let stores = open(&db);
        put(&stores[round as usize % 4], 1, round);
        drop(stores);
    }
    let stores = open(&db);
    assert_eq!(read(&stores[3], 1), Some(record(1, 19, 200)));
}
