//! Spans over cold pages (RFC 0081 §3): a page written long ago lies past
//! what its range's window reaches from where the next rounds land, so an
//! update's span over it gets a range of its own, and cuts the range it
//! came from in two. The write pays its span and no more; the ranges are
//! the cleaner's to give back — its regroup moves each cut range side by
//! side, the page named either side of a span once, and its merge
//! consolidates the page with its spans. Every record reads back, before
//! and after a reopen.

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::collections::BTreeMap;
use std::path::Path;

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{PageStore, StoreOptions, StructStorage};

const A: u64 = 0x5EED_0081_0003_0001;
const B: u64 = 0x5EED_0081_0003_0002;
static SLOT_A: StructStorage = StructStorage::new(A);
static SLOT_B: StructStorage = StructStorage::new(B);

const RECORDS: u64 = 2_000;
const UPDATES: usize = 666;

fn open(dir: &Path, options: StoreOptions) -> PageStore {
    PageStore::open_with(dir, &[&SLOT_A, &SLOT_B], options).unwrap()
}

fn id(sh: u64, k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, (sh & 0x7FFF) as u16)
}

/// ~1 KiB that does not compress away.
fn record(sh: u64, k: u64, version: u64) -> Vec<u8> {
    let mut bytes = sh.to_le_bytes().to_vec();
    let mut x =
        (k ^ sh ^ version << 40).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

/// `A`'s ranges, pages and index bytes, by the census.
fn census_a(store: &PageStore) -> (u64, u64, u64) {
    let census = store.census().unwrap();
    let a = census.iter().find(|c| c.struct_hash == A).unwrap();
    (a.ranges, a.pages, a.index_bytes)
}

/// `A` written, then 24 MiB of `B` after it: `A`'s pages lie past what a
/// 16 MiB window reaches from where the next rounds land. Then durable
/// updates of `A` at random, each a batch; the versions they left.
fn cold_updates(store: &PageStore) -> BTreeMap<u64, u64> {
    let put = |sh, k, v| Write::Put(id(sh, k), record(sh, k, v));
    let mut versions = BTreeMap::new();
    block_on(async {
        for k in 0..RECORDS {
            store.apply(&[put(A, k, 0)]).await.unwrap();
        }
        store.drain().unwrap();
        for k in 0..24_000 {
            store.apply(&[put(B, k, 0)]).await.unwrap();
        }
        store.drain().unwrap();
        let mut x = 7u64;
        for _ in 0..UPDATES {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            let k = x % RECORDS;
            let version = versions.get(&k).copied().unwrap_or(0) + 1;
            versions.insert(k, version);
            store.apply_durable(&[put(A, k, version)]).await.unwrap();
        }
    });
    versions
}

fn assert_a(store: &PageStore, versions: &BTreeMap<u64, u64>, when: &str) {
    store.evict_settled(0);
    block_on(async {
        for k in 0..RECORDS {
            let version = versions.get(&k).copied().unwrap_or(0);
            let got = store.get_of(A, id(A, k)).await.unwrap();
            assert_eq!(got, Some(record(A, k, version)), "record {k} {when}");
        }
    });
}

/// Each update writes its span, not its page — a few blocks — and leaves
/// a range or two more in the index. A compaction gives every one back.
#[test]
fn a_compaction_gives_back_the_ranges_cold_spans_cut() {
    let d = tempfile::tempdir().unwrap();
    let store = open(d.path(), StoreOptions::default());
    let versions = cold_updates(&store);
    let (cut, _, cut_bytes) = census_a(&store);
    assert!(cut > UPDATES as u64 / 2, "{cut} ranges after the updates");
    assert_a(&store, &versions, "cut");

    store.compact().unwrap();
    let (ranges, _, bytes) = census_a(&store);
    assert!(ranges <= 8, "{cut} ranges → {ranges}");
    assert!(bytes * 20 < cut_bytes, "{cut_bytes} B → {bytes} B");
    assert!(!store.wants_regroup());
    assert_a(&store, &versions, "compacted");

    drop(store);
    let store = open(d.path(), StoreOptions::default());
    assert_eq!(census_a(&store).0, ranges);
    assert_a(&store, &versions, "reopened");
}

/// Past the index budget, the ranges spans cut are the cleaner's: the
/// store asks for it rather than making a write pay, and a compaction's
/// regroup answers it.
#[test]
fn past_the_budget_cut_ranges_ask_for_the_cleaner() {
    let d = tempfile::tempdir().unwrap();
    let options = StoreOptions {
        index_budget_bytes: 16 << 10,
        ..StoreOptions::default()
    };
    let store = open(d.path(), options.clone());
    let versions = cold_updates(&store);
    assert!(store.wants_regroup(), "{:?}", census_a(&store));
    store.compact().unwrap();
    assert!(!store.wants_regroup());
    assert!(census_a(&store).0 <= 8);
    assert_a(&store, &versions, "compacted");
    drop(store);
    let store = open(d.path(), options);
    assert_a(&store, &versions, "reopened");
}
