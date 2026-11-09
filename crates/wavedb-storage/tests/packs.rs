//! A round's small pages of several types in one run (RFC 0081 Â§1, Â§5):
//! what a durable round of two types costs, a pack outliving the sections
//! that left it, and a pack through a reopen, a compaction and a crash.

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use common::copy_dir;
use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{BLOCK_SIZE, PageStore, StoreOptions, StructStorage};

const A: u64 = 0x0081_0005_9AC4_0001;
const B: u64 = 0x0081_0005_9AC4_0002;
static SLOT_A: StructStorage = StructStorage::new(A);
static SLOT_B: StructStorage = StructStorage::new(B);
// Uncompressed: a round writes no dictionary, so the blocks counted are
// the pages'.
static PLAIN_A: StructStorage = StructStorage::without_compression(A);
static PLAIN_B: StructStorage = StructStorage::without_compression(B);

fn open(dir: &Path) -> PageStore {
    PageStore::open(dir, &[&SLOT_A, &SLOT_B]).expect("open")
}

fn id(hash: u64, k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, (hash & 0x7FFF) as u16)
}

fn record(hash: u64, k: u64, version: u8) -> Vec<u8> {
    let mut bytes = hash.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.resize(16 + 200, version);
    bytes
}

fn put(hash: u64, k: u64, version: u8) -> Write {
    Write::Put(id(hash, k), record(hash, k, version))
}

fn read(s: &PageStore, hash: u64, k: u64) -> Option<Vec<u8>> {
    block_on(s.get_of(hash, id(hash, k))).unwrap()
}

fn data_file(dir: &Path) -> PathBuf {
    wavedb_storage::shard_set::storage_dir(dir, 0).join("0000000000000000")
}

fn blocks(dir: &Path) -> u64 {
    std::fs::metadata(data_file(dir)).unwrap().len() / BLOCK_SIZE as u64
}

/// Blocks every data file of the one-shard database at `dir` takes.
fn all_blocks(dir: &Path) -> u64 {
    let shard = wavedb_storage::shard_set::storage_dir(dir, 0);
    std::fs::read_dir(shard)
        .unwrap()
        .map(|f| f.unwrap().metadata().unwrap().len() / BLOCK_SIZE as u64)
        .sum()
}

/// A durable round of one small record in each of two types: one block for
/// both pages, its header, its roots block â where each page took a block.
#[test]
fn a_round_of_two_types_writes_their_pages_in_one_block() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let plain = || PageStore::open(&db, &[&PLAIN_A, &PLAIN_B]).unwrap();
    let s = plain();
    let before = blocks(&db);
    block_on(s.apply_durable(&[put(A, 1, 1), put(B, 1, 1)])).unwrap();
    assert_eq!(blocks(&db) - before, 2, "pack, header");
    drop(s);
    let s = plain();
    assert_eq!(read(&s, A, 1), Some(record(A, 1, 1)));
    assert_eq!(read(&s, B, 1), Some(record(B, 1, 1)));
}

/// One type rewrites its page and leaves the pack; the other still reads
/// from it â then, and after a reopen, which counts the names again â and
/// leaves it too, after which a compaction gives the space back.
#[test]
fn a_pack_lasts_as_long_as_a_section_is_named() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    {
        let s = open(&db);
        block_on(s.apply_durable(&[put(A, 1, 1), put(B, 1, 1)])).unwrap();
        block_on(s.apply_durable(&[put(A, 1, 2)])).unwrap();
        assert_eq!(read(&s, B, 1), Some(record(B, 1, 1)));
    }
    {
        let s = open(&db);
        assert_eq!(read(&s, A, 1), Some(record(A, 1, 2)));
        assert_eq!(read(&s, B, 1), Some(record(B, 1, 1)));
        for v in 2..6 {
            block_on(s.apply_durable(&[put(B, 1, v), put(A, 2, v)])).unwrap();
        }
        assert_eq!(read(&s, B, 1), Some(record(B, 1, 5)));
    }
    let s = open(&db);
    assert_eq!(read(&s, A, 1), Some(record(A, 1, 2)));
    assert_eq!(read(&s, A, 2), Some(record(A, 2, 5)));
    assert_eq!(read(&s, B, 1), Some(record(B, 1, 5)));
}

/// Packs carried out of an emptied file by a compaction, a section at a
/// time: every record reads back, then and after a reopen.
#[test]
fn a_compaction_carries_a_pack_out_section_by_section() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let options = StoreOptions {
        file_bytes: 64 * BLOCK_SIZE as u64,
        ..StoreOptions::default()
    };
    let opened = || {
        PageStore::open_with(&db, &[&SLOT_A, &SLOT_B], options.clone()).unwrap()
    };
    {
        let s = opened();
        // Packs of two sections; then most of A's records go, so the early
        // files are mostly free and B's sections still live in their packs.
        for k in 0..40 {
            block_on(s.apply_durable(&[put(A, k, 1), put(B, k, 1)])).unwrap();
        }
        for k in 0..40 {
            block_on(s.apply_durable(&[Write::Remove(A, id(A, k))])).unwrap();
        }
        let before = all_blocks(&db);
        s.compact().unwrap();
        let after = all_blocks(&db);
        assert!(after < before, "files emptied: {before} → {after}");
        for k in 0..40 {
            assert_eq!(read(&s, A, k), None, "{k}");
            assert_eq!(read(&s, B, k), Some(record(B, k, 1)), "{k}");
        }
    }
    let s = opened();
    for k in 0..40 {
        assert_eq!(read(&s, B, k), Some(record(B, k, 1)), "{k} after a reopen");
    }
}

/// A batch whose pack the crash tore gives way to the batch before it.
#[test]
fn a_torn_pack_gives_way_to_the_batch_before() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    // Plain types: the window holds no dictionary before the pack.
    let plain =
        |dir: &Path| PageStore::open(dir, &[&PLAIN_A, &PLAIN_B]).unwrap();
    let s = plain(&db);
    block_on(s.apply_durable(&[put(A, 1, 1), put(B, 1, 1)])).unwrap();
    let first_end = std::fs::metadata(data_file(&db)).unwrap().len();
    block_on(s.apply_durable(&[put(A, 1, 2), put(B, 1, 2)])).unwrap();
    let crashed = db.with_extension("crashed");
    copy_dir(&db, &crashed);
    // The second round's window starts with its pack.
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(data_file(&crashed))
        .unwrap();
    file.write_all_at(&[0xFF; 64], first_end + 100).unwrap();
    drop(file);
    let s = plain(&crashed);
    assert_eq!(read(&s, A, 1), Some(record(A, 1, 1)));
    assert_eq!(read(&s, B, 1), Some(record(B, 1, 1)));
}
