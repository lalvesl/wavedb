//! The record cache as a log ([RFC 0068]), proved through the store: what
//! eviction may drop, what it must keep, what a reopen rebuilds, and that a
//! budget holds under steady writes — which the per-record cache it replaced
//! never did, since it refused to evict while anything was pending.
//!
//! [RFC 0068]: ../../../rfcs/0068-record-cache-as-a-log-WIP.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};

use wavedb_storage::{PageStore, Progress, StructStorage};

const SH: u64 = 0x5EED_0068_CAC4_E001;
static SLOT: StructStorage = StructStorage::new(SH);

fn open(dir: &std::path::Path) -> PageStore {
    PageStore::open(dir, &[&SLOT]).unwrap()
}

/// One bounded round of a settle cycle, opening one if none is open — a
/// node's step.
fn step(s: &PageStore, budget: usize) -> Progress {
    if !s.is_cycling() {
        s.begin_cycle().unwrap();
    }
    s.cycle_step(budget).unwrap()
}

fn id(key: u64) -> Id {
    Id::new(key, U48::from(1u32), false, (SH & 0x7FFF) as u16)
}

fn record(key: u64, version: u8, len: usize) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    bytes.extend_from_slice(&key.to_le_bytes());
    bytes.resize(16 + len, version);
    bytes
}

fn put(s: &PageStore, key: u64, version: u8, len: usize) {
    block_on(s.apply(&[Write::Put(id(key), record(key, version, len))]))
        .unwrap();
}

fn read(s: &PageStore, key: u64) -> Option<Vec<u8>> {
    block_on(s.get_of(SH, id(key))).unwrap()
}

/// Settle part of a burst and evict: the settled records leave the cache and
/// read back from their pages, the unsettled ones stay — their pages do not
/// hold them yet.
#[test]
fn eviction_drops_the_settled_and_keeps_the_unsettled() {
    let d = tempfile::tempdir().unwrap();
    let s = open(d.path());
    for k in 0..2_000 {
        put(&s, k, 1, 200);
    }
    assert_eq!(step(&s, 1_000), Progress::Settled);
    let before = s.cache_len();
    s.evict_settled(0);
    assert!(s.cache_len() < before, "settled chunks must go");
    assert!(s.cache_len() >= 1_000, "the unsettled half must stay");
    assert!(s.has_pending());
    for k in (0..2_000).step_by(41) {
        assert_eq!(read(&s, k), Some(record(k, 1, 200)), "record {k}");
    }
    s.drain().unwrap();
    s.evict_settled(0);
    assert_eq!(s.cache_len(), 0);
    for k in (0..2_000).step_by(41) {
        assert_eq!(read(&s, k), Some(record(k, 1, 200)), "record {k}");
    }
}

/// A record rewritten many times inside the window is settled at its newest
/// version, and a remove settled after a rewrite reads as gone — from the
/// cache first, and from the page once the cache lets go.
#[test]
fn rewrites_settle_at_their_newest_and_removes_stay_removed() {
    let d = tempfile::tempdir().unwrap();
    let s = open(d.path());
    for v in 0..50u8 {
        put(&s, 7, v, 40);
    }
    put(&s, 8, 0, 40);
    block_on(s.apply(&[Write::Remove(SH, id(8))])).unwrap();
    assert_eq!(read(&s, 8), None, "an unsettled remove hides nothing");
    s.drain().unwrap();
    s.evict_settled(0);
    assert_eq!(read(&s, 7), Some(record(7, 49, 40)));
    assert_eq!(read(&s, 8), None, "the page agrees after the settle");
}

/// Unsettled and evicted alike come back on reopen: a close settles what
/// was not, and the reconstruction finds both, so what was read before is
/// read after.
#[test]
fn a_reopen_rebuilds_what_eviction_and_the_settle_left() {
    let d = tempfile::tempdir().unwrap();
    {
        let s = open(d.path());
        for k in 0..500 {
            put(&s, k, 1, 100);
        }
        step(&s, 200);
        s.evict_settled(0);
        for k in 0..250 {
            put(&s, k, 2, 100);
        }
    }
    let s = open(d.path());
    assert!(!s.has_pending(), "a reopen replays nothing");
    for k in 0..500 {
        let version = if k < 250 { 2 } else { 1 };
        assert_eq!(read(&s, k), Some(record(k, version, 100)), "record {k}");
    }
}

/// Under steady writes, a store that settles and evicts as it goes stays
/// inside its budget, give or take what is still unsettled — the per-record
/// cache it replaced grew without bound here, since one pending id was enough
/// to refuse every eviction.
#[test]
fn steady_writes_stay_inside_the_budget() {
    const BUDGET: usize = 512 << 10;
    let d = tempfile::tempdir().unwrap();
    let s = open(d.path());
    let mut peak = 0;
    for round in 0..20u64 {
        for k in 0..500 {
            put(&s, round * 500 + k, 1, 300);
        }
        // The node's shape: a bounded settle step, then the upkeep evicts.
        // The step scans past this round's writes, so what stays unsettled
        // between steps is never more than one round of them.
        step(&s, 600);
        s.evict_settled(BUDGET);
        peak = peak.max(s.cache_bytes());
    }
    s.drain().unwrap();
    s.evict_settled(BUDGET);
    assert!(s.cache_bytes() <= BUDGET, "{} > {BUDGET}", s.cache_bytes());
    // 10 000 records of ~340 bytes is ~3.4 MB written; what stays resident
    // is the budget, one round of unsettled writes (~170 KB) and the index.
    assert!(peak < BUDGET + (512 << 10), "peak {peak} bytes");
    for k in (0..10_000).step_by(997) {
        assert_eq!(read(&s, k), Some(record(k, 1, 300)), "record {k}");
    }
}
