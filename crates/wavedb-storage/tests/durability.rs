//! Durability without a journal ([RFC 0072]), end to end. A crash is the
//! database's files copied while the store holds them open — every window it
//! wrote is there, what it holds in RAM is not — and reopened.
//!
//! - a write is in the record cache only, until a settle cycle carries it;
//!   a durable write waits for that cycle's barrier, on the helper or
//!   inline, and everything committed before it is durable with it;
//! - a batch a crash cut before its end comes back not at all, and one
//!   whose page or header the crash tore gives way to the batch before it;
//! - nothing is replayed: a reopen starts cold.
//!
//! [RFC 0072]: ../../../rfcs/0072-the-settle-is-the-durability-point-WIP.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use common::copy_dir;
use futures::FutureExt;
use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{Barrier, PageStore, Progress, StructStorage};

const SH: u64 = 0x0072_0002_D0AB_1E00;
static SLOT: StructStorage = StructStorage::new(SH);
/// A second type, which only the first batch writes.
const SH2: u64 = 0x0072_0002_D0AB_1E01;
static QUIET: StructStorage = StructStorage::new(SH2);

fn open(dir: &Path) -> PageStore {
    PageStore::open(dir, &[&SLOT, &QUIET]).expect("open")
}

fn with_helper(dir: &Path) -> PageStore {
    let mut store = open(dir);
    store.use_barrier(Barrier::spawn("durability-test").expect("helper"));
    store
}

fn id(k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, 1)
}

fn record(hash: u64, k: u64, version: u8) -> Vec<u8> {
    let mut bytes = hash.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.resize(16 + 300, version);
    bytes
}

fn put(k: u64, version: u8) -> Write {
    Write::Put(id(k), record(SH, k, version))
}

fn read(s: &PageStore, hash: u64, k: u64) -> Option<Vec<u8>> {
    block_on(s.get_of(hash, id(k))).unwrap()
}

/// The crash: the database as it is on disk now, at `<dir>-crashed`.
fn crash(dir: &Path) -> PathBuf {
    let to = dir.with_extension("crashed");
    copy_dir(dir, &to);
    to
}

/// The one data file of the one-shard database at `dir`.
fn data_file(dir: &Path) -> PathBuf {
    wavedb_storage::shard_set::shard_dir(dir, 0).join("0000000000000000")
}

fn len(path: &Path) -> u64 {
    std::fs::metadata(path).unwrap().len()
}

/// Zero `blocks` blocks of `path` from byte `at`.
fn zero(path: &Path, at: u64, blocks: usize) {
    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.write_all_at(&vec![0; blocks * 4096], at).unwrap();
}

/// A write is in RAM until a cycle carries it: a crash before loses it, a
/// crash after keeps it.
#[test]
fn a_write_is_durable_once_a_cycle_carries_it() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = open(&db);
    block_on(s.apply(&[put(1, 1)])).unwrap();
    assert_eq!(read(&s, SH, 1), Some(record(SH, 1, 1)), "the cache has it");
    assert_eq!(read(&open(&crash(&db)), SH, 1), None, "the disk does not");

    assert!(s.begin_cycle().unwrap());
    while s.cycle_step(usize::MAX).unwrap() == Progress::Settled {}
    let reopened = open(&crash(&db));
    assert_eq!(read(&reopened, SH, 1), Some(record(SH, 1, 1)));
    assert_eq!(reopened.cache_len(), 0, "nothing replayed: a cold start");
}

/// With a helper, a durable write suspends until its batch's barrier is
/// taken there — and every write committed before it is durable with it.
#[test]
fn a_durable_write_waits_for_its_batch() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = with_helper(&db);
    block_on(s.apply(&[put(1, 1)])).unwrap();
    let batch = [put(2, 1)];
    let mut durable = Box::pin(s.apply_durable(&batch));
    assert!((&mut durable).now_or_never().is_none(), "waits for a cycle");
    // The node's tick.
    assert!(s.begin_cycle().unwrap());
    while s.cycle_step(64).unwrap() == Progress::Settled {}
    block_on(durable).unwrap();
    let reopened = open(&crash(&db));
    assert_eq!(read(&reopened, SH, 1), Some(record(SH, 1, 1)), "before it");
    assert_eq!(read(&reopened, SH, 2), Some(record(SH, 2, 1)));
}

/// A store nobody ticks settles a durable write's batch itself.
#[test]
fn a_durable_write_without_a_helper_settles_its_batch() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = open(&db);
    block_on(s.apply_durable(&[put(3, 1)])).unwrap();
    assert!(!s.has_pending());
    assert_eq!(read(&open(&crash(&db)), SH, 3), Some(record(SH, 3, 1)));
}

/// A batch the crash cut before its end — rounds written, no header saying
/// it ended — comes back not at all; the batch before it does, whole.
#[test]
fn a_batch_cut_before_its_end_comes_back_not_at_all() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = open(&db);
    for k in 0..40 {
        block_on(s.apply(&[put(k, 1)])).unwrap();
    }
    s.drain().unwrap();
    for k in 0..40 {
        block_on(s.apply(&[put(k, 2)])).unwrap();
    }
    assert!(s.begin_cycle().unwrap());
    for _ in 0..3 {
        assert_eq!(s.cycle_step(5).unwrap(), Progress::Settled);
    }
    let reopened = open(&crash(&db));
    for k in 0..40 {
        assert_eq!(read(&reopened, SH, k), Some(record(SH, k, 1)), "{k}");
    }
}

/// A batch whose page the crash tore — its header on disk, a page block of
/// it not — gives way to the batch before it.
#[test]
fn a_batch_with_a_torn_page_gives_way_to_the_one_before() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = open(&db);
    block_on(s.apply(&[put(1, 1), put(2, 1)])).unwrap();
    s.drain().unwrap();
    let first_end = len(&data_file(&db));
    block_on(s.apply(&[put(1, 2), put(2, 2)])).unwrap();
    s.drain().unwrap();
    let crashed = crash(&db);
    zero(&data_file(&crashed), first_end, 1); // the window's first page
    let reopened = open(&crashed);
    assert_eq!(read(&reopened, SH, 1), Some(record(SH, 1, 1)));
    assert_eq!(read(&reopened, SH, 2), Some(record(SH, 2, 1)));
    // And it carries on from there.
    block_on(reopened.apply(&[put(1, 3)])).unwrap();
    drop(reopened);
    assert_eq!(read(&open(&crashed), SH, 1), Some(record(SH, 1, 3)));
}

/// A batch whose header the crash tore is found past: the header before it
/// is read back from the blocks behind.
#[test]
fn a_torn_header_is_read_past() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    let s = open(&db);
    block_on(s.apply(&[put(1, 1)])).unwrap();
    s.drain().unwrap();
    block_on(s.apply(&[put(1, 2)])).unwrap();
    s.drain().unwrap();
    let crashed = crash(&db);
    let file = data_file(&crashed);
    zero(&file, len(&file) - 8 * 4096, 8);
    assert_eq!(read(&open(&crashed), SH, 1), Some(record(SH, 1, 1)));
}

/// Many batches of one type: the other type, written once, is still found —
/// by the spare room restating it, or by the walk down — and the record
/// first written survives every later batch.
#[test]
fn a_type_written_once_survives_many_batches() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    {
        let s = open(&db);
        let quiet = Write::Put(id(9), record(SH2, 9, 7));
        block_on(s.apply(&[quiet])).unwrap();
        s.drain().unwrap();
        for round in 0..50u8 {
            for k in 0..20 {
                block_on(s.apply(&[put(k, round)])).unwrap();
            }
            s.drain().unwrap();
        }
    }
    let s = open(&db);
    assert_eq!(read(&s, SH2, 9), Some(record(SH2, 9, 7)));
    for k in 0..20 {
        assert_eq!(read(&s, SH, k), Some(record(SH, k, 49)), "{k}");
    }
}
