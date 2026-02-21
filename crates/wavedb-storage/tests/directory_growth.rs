//! The extendible directory across a reopen ([RFC 0074]): the headers name
//! buckets only, and carry each type's depth, so the walk has to fill every
//! alias from the depth in a page's descriptor — after doublings, after a
//! round that split one page many times, and never from a batch the crash
//! cut.
//!
//! [RFC 0074]: ../../../rfcs/0074-extendible-hashing.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use std::path::{Path, PathBuf};

use common::copy_dir;
use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{PageStore, Progress, StructStorage};

const SH: u64 = 0x0074_0002_D1EC_0001;
static SLOT: StructStorage = StructStorage::new(SH);

fn open(dir: &Path) -> PageStore {
    PageStore::open(dir, &[&SLOT]).expect("open")
}

fn id(k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, 1)
}

/// ~300 incompressible bytes: about a hundred to a 32 KiB page, whatever
/// the dictionary learns.
fn record(k: u64, version: u8) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    let mut x = k.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ u64::from(version) | 1;
    while bytes.len() < 16 + 300 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

fn write(s: &PageStore, keys: std::ops::Range<u64>, version: u8) {
    for k in keys {
        block_on(s.apply(&[Write::Put(id(k), record(k, version))])).unwrap();
    }
}

fn assert_reads(s: &PageStore, keys: std::ops::Range<u64>, version: u8) {
    for k in keys {
        let got = block_on(s.get_of(SH, id(k))).unwrap();
        assert_eq!(got, Some(record(k, version)), "record {k}");
    }
}

/// The crash: the database as it is on disk now, at `<dir>-crashed`.
fn crash(dir: &Path) -> PathBuf {
    let to = dir.with_extension("crashed");
    copy_dir(dir, &to);
    to
}

/// Batch after batch doubles the directory; a cold reopen routes every
/// record to its page again.
#[test]
fn a_reopen_after_doublings_finds_every_record() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let pages = {
        let s = open(&db);
        for batch in 0..8 {
            write(&s, batch * 500..(batch + 1) * 500, 1);
            s.drain().unwrap();
        }
        s.bucket_count(SH)
    };
    assert!(pages >= 32, "the directory doubled several times: {pages}");
    let s = open(&db);
    assert_eq!(s.cache_len(), 0, "a cold start");
    assert_eq!(s.bucket_count(SH), pages, "every page found again");
    assert_reads(&s, 0..4_000, 1);
}

/// One round from a single page: it splits again and again before anything
/// is written, and the reopen sees exactly that shape.
#[test]
fn a_round_of_cascading_splits_reopens_whole() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let pages = {
        let s = open(&db);
        write(&s, 0..3_000, 1);
        // One round takes every id: the page splits before it is written.
        assert!(s.begin_cycle().unwrap());
        while s.cycle_step(usize::MAX).unwrap() != Progress::Done {}
        s.bucket_count(SH)
    };
    assert!(pages >= 16, "one round split its page many times: {pages}");
    let s = open(&db);
    assert_eq!(s.bucket_count(SH), pages);
    assert_reads(&s, 0..3_000, 1);
    // And it carries on from that shape.
    write(&s, 0..3_000, 2);
    s.drain().unwrap();
    drop(s);
    assert_reads(&open(&db), 0..3_000, 2);
}

/// A batch the crash cut after a round that doubled the directory comes
/// back not at all: the reopen has the batch before it, at its depth.
#[test]
fn a_cut_batch_whose_round_doubled_comes_back_not_at_all() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = open(&db);
    write(&s, 0..1_000, 1);
    s.drain().unwrap();
    let before = s.bucket_count(SH);
    write(&s, 0..6_000, 2);
    assert!(s.begin_cycle().unwrap());
    assert_eq!(s.cycle_step(5_000).unwrap(), Progress::Settled);
    assert!(s.bucket_count(SH) > 2 * before, "the round doubled");
    let reopened = open(&crash(&db));
    assert_eq!(reopened.bucket_count(SH), before);
    assert_reads(&reopened, 0..1_000, 1);
    let absent = block_on(reopened.get_of(SH, id(5_999))).unwrap();
    assert_eq!(absent, None, "nothing of the cut batch");
}
