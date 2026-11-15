//! The cleaner's choices ([RFC 0081] §7–§8), proved through a compaction:
//! an address is an age, so a range written long ago is cold and merged
//! into full pages, while a recent one keeps the half-page bound a write
//! would cut again; and the holes emptied pages leave go with the merge.
//! Every record reads back, before and after a reopen.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::path::Path;

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{PageStore, StructStorage};

const A: u64 = 0x5EED_0081_0008_0001;
const B: u64 = 0x5EED_0081_0008_0002;
static SLOT_A: StructStorage = StructStorage::new(A);
static SLOT_B: StructStorage = StructStorage::new(B);

const RECORDS: u64 = 3_000;

fn open(dir: &Path) -> PageStore {
    PageStore::open(dir, &[&SLOT_A, &SLOT_B]).unwrap()
}

fn id(sh: u64, k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, (sh & 0x7FFF) as u16)
}

/// ~1 KiB that does not compress away.
fn record(sh: u64, k: u64) -> Vec<u8> {
    let mut bytes = sh.to_le_bytes().to_vec();
    let mut x = (k ^ sh).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

fn put_all(s: &PageStore, sh: u64, keys: impl Iterator<Item = u64>) {
    block_on(async {
        for k in keys {
            s.apply(&[Write::Put(id(sh, k), record(sh, k))])
                .await
                .unwrap();
        }
    });
    s.drain().unwrap();
}

fn remove_all(s: &PageStore, keys: impl Iterator<Item = u64>) {
    block_on(async {
        for k in keys {
            s.apply(&[Write::Remove(A, id(A, k))]).await.unwrap();
        }
    });
    s.drain().unwrap();
}

/// `A`'s pages and holes, by the census.
fn census_a(s: &PageStore) -> (u64, u64) {
    let c = s.census().unwrap();
    let a = c.iter().find(|c| c.struct_hash == A).unwrap();
    (a.pages, a.holes)
}

fn assert_a(s: &PageStore, kept: impl Fn(u64) -> bool, when: &str) {
    s.evict_settled(0);
    block_on(async {
        for k in 0..RECORDS {
            let want = kept(k).then(|| record(A, k));
            let got = s.get_of(A, id(A, k)).await.unwrap();
            assert_eq!(got, want, "record {k} {when}");
        }
    });
}

/// Seven records in ten removed leave `A`'s pages under half a page each,
/// too big for the half-page bound to pair. Recent, they stay as they are;
/// once `B`'s writes put them behind the newest quarter of the files, the
/// same compaction merges them into full pages.
#[test]
fn a_cold_range_is_merged_into_full_pages() {
    let d = tempfile::tempdir().unwrap();
    let kept = |k: u64| k % 10 < 3;
    let pocked = |dir: &Path, later: u64| {
        let s = open(dir);
        put_all(&s, A, 0..RECORDS);
        remove_all(&s, (0..RECORDS).filter(|&k| !kept(k)));
        put_all(&s, B, 0..later);
        s
    };
    let hot = pocked(&d.path().join("hot"), 0);
    let (hot_before, _) = census_a(&hot);
    hot.compact().unwrap();
    let (hot_after, _) = census_a(&hot);
    assert_a(&hot, kept, "recent, compacted");

    let cold_dir = d.path().join("cold");
    let cold = pocked(&cold_dir, 4 * RECORDS);
    let (cold_before, _) = census_a(&cold);
    assert_eq!(cold_before, hot_before, "the same pages to start from");
    cold.compact().unwrap();
    let (cold_after, _) = census_a(&cold);
    assert!(
        cold_after * 3 < hot_after * 2,
        "cold merged full: {cold_before} → {cold_after}; recent {hot_before} → {hot_after}"
    );
    assert_a(&cold, kept, "cold, compacted");
    drop(cold);
    let cold = open(&cold_dir);
    assert_eq!(census_a(&cold).0, cold_after);
    assert_a(&cold, kept, "cold, reopened");
}

/// Removing whole runs of keys empties whole pages; their entries stay as
/// holes while their ranges hold others. A compaction's merge takes the
/// holes beside each other, and they do not come back on a reopen.
#[test]
fn a_compaction_takes_the_holes_emptied_pages_left() {
    let d = tempfile::tempdir().unwrap();
    let kept = |k: u64| k % 300 >= 120;
    let s = open(d.path());
    put_all(&s, A, 0..RECORDS);
    remove_all(&s, (0..RECORDS).filter(|&k| !kept(k)));
    let (_, holes_before) = census_a(&s);
    assert!(holes_before > 10, "{holes_before} holes");
    s.compact().unwrap();
    let (_, holes_after) = census_a(&s);
    assert!(
        holes_after * 2 < holes_before,
        "holes merged: {holes_before} → {holes_after}"
    );
    assert_a(&s, kept, "compacted");
    drop(s);
    let s = open(d.path());
    assert_eq!(census_a(&s).1, holes_after);
    assert_a(&s, kept, "reopened");
}
