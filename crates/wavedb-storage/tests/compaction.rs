//! Compaction ([RFC 0069]), proved through the store: live runs move down
//! into the holes, the free tail leaves `data.bin`, and every record reads
//! back — from the moved pages, after a reopen, and after a store dropped in
//! the middle of it.
//!
//! [RFC 0069]: ../../../rfcs/0069-compaction-into-the-holes.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};

use wavedb_storage::{
    Compacting, PageStore, Space, StoreOptions, StructStorage,
};

const SH: u64 = 0x5EED_0069_C0A4_0001;
static SLOT: StructStorage = StructStorage::new(SH);

const RECORDS: u64 = 1_500;

/// Batches share barriers (RFC 0061): these tests are about space, and a
/// barrier per write would make them about the disk. Everything a reopen
/// checks was covered by a checkpoint first.
fn open(dir: &std::path::Path) -> PageStore {
    let options = StoreOptions {
        relax_window: std::time::Duration::from_secs(3_600),
        ..StoreOptions::default()
    };
    PageStore::open_with(dir, &[&SLOT], options).unwrap()
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
/// copy-on-write, and the runs they leave are holes all through the file.
fn pock(s: &PageStore) {
    block_on(async {
        for k in 0..RECORDS {
            s.apply(&[Write::Put(id(k), record(k, 0))]).await.unwrap();
        }
        s.commit_journal().unwrap();
        for k in (0..RECORDS).filter(|k| k % 3 != 0) {
            s.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
        }
        // Two checkpoints: the second releases what the first superseded.
        s.commit_journal().unwrap();
        s.commit_journal().unwrap();
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

/// The headline: a pocked file comes out shorter, its free space mostly gone
/// with the tail, and nothing lost — before and after a reopen.
#[test]
fn compaction_shrinks_the_file_and_keeps_every_record() {
    let d = tempfile::tempdir().unwrap();
    let s = open(d.path());
    pock(&s);
    let before = s.space();
    assert!(unused(before) * 3 > before.file_blocks, "{before:?}");

    let done = s.compact().unwrap();
    let after = s.space();
    assert!(done.moved_images > 0, "{done:?}");
    assert_eq!(
        done.reclaimed_blocks,
        before.file_blocks - after.file_blocks,
        "every block the file lost is counted: {done:?}"
    );
    assert!(
        after.file_blocks < before.file_blocks,
        "{before:?} → {after:?}"
    );
    assert!(unused(after) < unused(before), "{before:?} → {after:?}");
    let on_disk = std::fs::metadata(d.path().join("data.bin")).unwrap().len();
    assert_eq!(on_disk, after.file_blocks * 4096, "the file itself shrank");
    assert!(!s.is_compacting());
    assert_records(&s);

    drop(s);
    let s = open(d.path());
    assert_records(&s);
}

/// Stepped as a node steps it — small passes, writes in between, the
/// sealing checkpoints closed by the caller — it ends the same way.
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
                while s.checkpoint_step(64).unwrap()
                    == wavedb_storage::Checkpointing::Settling
                {}
            }
            Compacting::Done { reclaimed_blocks } => break reclaimed_blocks,
        }
        block_on(s.apply(&[Write::Put(id(extra), record(extra, 0))])).unwrap();
        extra += 1;
    };
    assert!(reclaimed > 0);
    assert!(s.space().file_blocks < before.file_blocks);
    s.commit_journal().unwrap();
    assert_records(&s);
    drop(s);
    let s = open(d.path());
    assert_records(&s);
    for k in RECORDS..extra {
        let got = block_on(s.get_of(SH, id(k))).unwrap();
        assert_eq!(got, Some(record(k, 0)), "write {k} during compaction");
    }
}

/// A store dropped part-way — runs moved, nothing sealed — reopens from its
/// last commit, whose runs the moves never touched.
#[test]
fn a_store_dropped_mid_compaction_reopens_whole() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = open(d.path());
        pock(&s);
        for _ in 0..3 {
            s.compact_step(8).unwrap();
        }
        assert!(s.is_compacting());
    }
    let s = open(d.path());
    assert!(!s.is_compacting(), "a reopen starts idle");
    assert_records(&s);
    // And it can still compact from there.
    assert!(s.compact().unwrap().reclaimed_blocks > 0);
    assert_records(&s);
}
