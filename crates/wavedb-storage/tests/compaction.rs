//! Data files and their compaction ([RFC 0071]), proved through the store:
//! writes roll over into files named by address, compaction carries the live
//! runs of the emptiest files forward and deletes what they leave, and every
//! record reads back — from the moved pages, after a reopen, and after a
//! store dropped in the middle of it.
//!
//! [RFC 0071]: ../../../rfcs/0071-data-files-by-address.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::path::{Path, PathBuf};

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};

use wavedb_storage::{
    BLOCK_SIZE, Compacting, PageStore, Progress, Space, StoreOptions,
    StructStorage,
};

const SH: u64 = 0x5EED_0069_C0A4_0001;
static SLOT: StructStorage = StructStorage::new(SH);

const RECORDS: u64 = 1_500;

fn open_sized(dir: &Path, file_blocks: u64) -> PageStore {
    let options = StoreOptions {
        file_bytes: file_blocks * BLOCK_SIZE as u64,
        ..StoreOptions::default()
    };
    PageStore::open_with(dir, &[&SLOT], options).unwrap()
}

/// Files of 64 blocks: the pocked store spans several.
fn open(dir: &Path) -> PageStore {
    open_sized(dir, 64)
}

/// The data files of the one-shard store at `dir`: `(first block, bytes)`.
fn data_files(dir: &Path) -> Vec<(u64, u64)> {
    let shard: PathBuf = wavedb_storage::shard_set::shard_dir(dir, 0);
    let mut out: Vec<(u64, u64)> = std::fs::read_dir(shard)
        .unwrap()
        .filter_map(|e| {
            let e = e.unwrap();
            let name = e.file_name().into_string().unwrap();
            let start = u64::from_str_radix(&name, 16).ok()?;
            (name.len() == 16).then(|| (start, e.metadata().unwrap().len()))
        })
        .collect();
    out.sort_unstable();
    out
}

/// Copy the directory tree `from` to `to` — what a crash leaves on disk,
/// taken while the store holds it open.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            copy_dir(&path, &to.join(entry.file_name()));
        } else {
            std::fs::copy(&path, to.join(entry.file_name())).unwrap();
        }
    }
}

fn id(key: u64) -> Id {
    Id::new(key, U48::from(1u32), false, (SH & 0x7FFF) as u16)
}

/// A record of ~1 KiB that does not compress, so pages take real room.
fn record(key: u64, version: u64) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    bytes.extend_from_slice(&key.to_le_bytes());
    let mut x = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version ^ 1;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

/// Fill the store, then remove two records in three: the pages shrink,
/// copy-on-write, and the runs they leave are holes all through the files.
fn pock(s: &PageStore) {
    block_on(async {
        for k in 0..RECORDS {
            s.apply(&[Write::Put(id(k), record(k, 0))]).await.unwrap();
            if k % 100 == 99 {
                s.drain().unwrap(); // a window each, files apart
            }
        }
        for k in (0..RECORDS).filter(|k| k % 3 != 0) {
            s.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
        }
        s.drain().unwrap();
    });
}

fn assert_records(s: &PageStore) {
    s.evict_settled(0);
    assert_eq!(s.cache_len(), 0, "every read must come off a page");
    block_on(async {
        for k in 0..RECORDS {
            let want = (k % 3 == 0).then(|| record(k, 0));
            assert_eq!(s.get_of(SH, id(k)).await.unwrap(), want, "record {k}");
        }
    });
}

const fn unused(space: Space) -> u64 {
    space.free_blocks + space.deferred_blocks
}

/// The bytes the allocator says the files hold are the bytes on disk.
fn assert_matches_disk(dir: &Path, s: &PageStore) {
    let on_disk: u64 = data_files(dir).iter().map(|f| f.1).sum();
    assert_eq!(on_disk, s.space().file_blocks * BLOCK_SIZE as u64);
}

/// The headline: the pocked files are emptied and deleted, what they held
/// lives on in fewer blocks, and nothing is lost — before and after a
/// reopen.
#[test]
fn compaction_empties_the_holed_files_and_keeps_every_record() {
    let d = tempfile::tempdir().unwrap();
    let s = open(d.path());
    pock(&s);
    let before = s.space();
    let files_before = data_files(d.path());
    assert!(files_before.len() > 3, "{files_before:?}");
    assert!(unused(before) * 3 > before.file_blocks, "{before:?}");

    let done = s.compact().unwrap();
    let after = s.space();
    assert!(done.moved_images > 0, "{done:?}");
    assert!(
        after.file_blocks < before.file_blocks,
        "{before:?} → {after:?}"
    );
    assert!(done.reclaimed_blocks >= before.file_blocks - after.file_blocks);
    assert!(unused(after) < unused(before), "{before:?} → {after:?}");
    let files_after = data_files(d.path());
    assert!(
        files_before.iter().any(|f| !files_after.contains(f)),
        "old files were cut or deleted: {files_before:?} → {files_after:?}"
    );
    assert_matches_disk(d.path(), &s);
    assert!(!s.is_compacting());
    assert_records(&s);

    drop(s);
    let s = open(d.path());
    assert_matches_disk(d.path(), &s);
    assert_records(&s);
}

/// A store smaller than one file: its only file is the active one, which
/// compaction closes first and then empties — or it would never shrink.
#[test]
fn a_store_of_one_file_compacts_too() {
    let d = tempfile::tempdir().unwrap();
    let s = open_sized(d.path(), 1 << 20);
    pock(&s);
    let [(first, _)] = data_files(d.path())[..] else {
        panic!("one file: {:?}", data_files(d.path()));
    };
    let before = s.space();
    s.compact().unwrap();
    assert!(s.space().file_blocks < before.file_blocks);
    let files = data_files(d.path());
    assert!(
        files.iter().all(|f| f.0 != first),
        "{first} went: {files:?}"
    );
    assert_records(&s);
    drop(s);
    assert_records(&open_sized(d.path(), 1 << 20));
}

/// Stepped as a node steps it — small passes, writes in between, the
/// sealing batch settled by the caller — it ends the same way.
#[test]
fn compaction_in_steps_lets_writes_through() {
    let d = tempfile::tempdir().unwrap();
    let s = open(d.path());
    pock(&s);
    let before = s.space();
    let mut extra = RECORDS;
    let reclaimed = loop {
        match s.compact_step(16).unwrap() {
            Compacting::Moved { .. } => {}
            Compacting::Sealing => {
                s.begin_cycle().unwrap();
                while s.cycle_step(64).unwrap() == Progress::Settled {}
            }
            Compacting::Done { reclaimed_blocks } => break reclaimed_blocks,
        }
        block_on(s.apply(&[Write::Put(id(extra), record(extra, 0))])).unwrap();
        extra += 1;
    };
    assert!(reclaimed > 0);
    s.drain().unwrap();
    assert!(s.space().file_blocks < before.file_blocks + (extra - RECORDS));
    assert_records(&s);
    drop(s);
    let s = open(d.path());
    assert_records(&s);
    for k in RECORDS..extra {
        let got = block_on(s.get_of(SH, id(k))).unwrap();
        assert_eq!(got, Some(record(k, 0)), "write {k} during compaction");
    }
}

/// A crash part-way — runs moved, no batch over them durable — reopens
/// from the batch before, whose runs the moves never cut.
#[test]
fn a_crash_mid_compaction_reopens_whole() {
    let d = tempfile::tempdir().unwrap();
    let crashed = d.path().join("crashed");
    {
        let s = open(&d.path().join("db"));
        pock(&s);
        for _ in 0..3 {
            s.compact_step(8).unwrap();
        }
        assert!(s.is_compacting());
        // A reclaim meanwhile cuts nothing the durable batch names.
        s.reclaim().unwrap();
        copy_dir(&d.path().join("db"), &crashed);
    }
    let s = open(&crashed);
    assert!(!s.is_compacting(), "a reopen starts idle");
    assert_records(&s);
    // And it can still compact from there.
    assert!(s.compact().unwrap().reclaimed_blocks > 0);
    assert_records(&s);
}

/// What a crash leaves of a cut or a deletion it interrupted — a file longer
/// than its last run, a file no run names — reopens clean.
#[test]
fn a_crash_s_leftovers_are_cut_on_reopen() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = open(d.path());
        pock(&s);
    }
    let shard = wavedb_storage::shard_set::shard_dir(d.path(), 0);
    let files = data_files(d.path());
    let (last, len) = *files.last().unwrap();
    let path = shard.join(format!("{last:016x}"));
    let longer = len + 5 * BLOCK_SIZE as u64;
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(longer)
        .unwrap();
    let stray = last + 1_000_000;
    std::fs::write(shard.join(format!("{stray:016x}")), [0u8; 4096]).unwrap();

    let s = open(d.path());
    let after = data_files(d.path());
    assert!(after.iter().all(|f| f.0 != stray), "{after:?}");
    assert!(
        after.iter().all(|f| f.1 < longer || f.0 != last),
        "{after:?}"
    );
    assert_matches_disk(d.path(), &s);
    assert_records(&s);
}

/// A window is never split: one larger than the ceiling lands whole, the
/// next one opens a file named after where it ended, and a reopen with
/// another ceiling reads both.
#[test]
fn a_window_past_the_ceiling_lands_whole() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = open_sized(d.path(), 4);
        block_on(async {
            let batch: Vec<Write> =
                (0..40).map(|k| Write::Put(id(k), record(k, 0))).collect();
            s.apply(&batch).await.unwrap();
            s.drain().unwrap();
            s.apply(&[Write::Put(id(40), record(40, 0))]).await.unwrap();
            s.drain().unwrap();
        });
    }
    let files = data_files(d.path());
    assert!(files.len() >= 2, "{files:?}");
    assert!(files[0].1 > 4 * BLOCK_SIZE as u64, "{files:?}");
    for pair in files.windows(2) {
        let end = pair[0].0 + pair[0].1 / BLOCK_SIZE as u64;
        assert!(pair[1].0 >= end, "named past the one before: {files:?}");
    }
    let s = open_sized(d.path(), 1 << 20);
    s.evict_settled(0);
    for k in 0..=40 {
        assert_eq!(block_on(s.get_of(SH, id(k))).unwrap(), Some(record(k, 0)));
    }
}
