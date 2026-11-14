//! The index budget ([RFC 0081] §6), proved through the store: durable
//! updates in the middle of pages leave each page named either side of a
//! span — entries a clean index would not hold — and past the budget the
//! batches consolidate them inline, with no compaction. Every record reads
//! back, through a reopen and through a crash in the middle of a batch
//! that consolidated.
//!
//! [RFC 0081]: ../../../rfcs/0081-commit-blocks-slices-and-the-cleaner-PLANNED.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::path::Path;

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{PageStore, Progress, StoreOptions, StructStorage};

const SH: u64 = 0x5EED_0081_0007_0001;
static SLOT: StructStorage = StructStorage::new(SH);

const RECORDS: u64 = 3_000;

fn open(dir: &Path, index_budget_bytes: usize) -> PageStore {
    let options = StoreOptions {
        index_budget_bytes,
        ..StoreOptions::default()
    };
    PageStore::open_with(dir, &[&SLOT], options).unwrap()
}

fn id(k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, (SH & 0x7FFF) as u16)
}

/// ~1 KiB that does not compress away; `version` changes every byte.
fn record(k: u64, version: u64) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    let mut x = (k ^ version << 32).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

const fn updated(k: u64) -> bool {
    k % 97 == 13
}

/// Every record, then a durable update of one in 97: each a span in the
/// middle of its page.
fn fill_and_update(s: &PageStore) {
    block_on(async {
        for k in 0..RECORDS {
            s.apply(&[Write::Put(id(k), record(k, 0))]).await.unwrap();
        }
        s.drain().unwrap();
        for k in (0..RECORDS).filter(|&k| updated(k)) {
            let put = Write::Put(id(k), record(k, 1));
            s.apply_durable(&[put]).await.unwrap();
        }
    });
}

fn assert_records(s: &PageStore, when: &str) {
    s.evict_settled(0);
    block_on(async {
        for k in 0..RECORDS {
            let want = record(k, u64::from(updated(k)));
            let got = s.get_of(SH, id(k)).await.unwrap();
            assert_eq!(got, Some(want), "record {k} {when}");
        }
    });
}

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

/// Under the default budget the spans stay, for a compaction to take; with
/// none, every batch's first round consolidates, and the index ends smaller
/// for the same records — read back before and after a reopen.
#[test]
fn past_the_budget_the_batches_consolidate_the_spans() {
    let d = tempfile::tempdir().unwrap();
    let roomy = open(
        &d.path().join("roomy"),
        StoreOptions::default().index_budget_bytes,
    );
    fill_and_update(&roomy);
    let tight_dir = d.path().join("tight");
    let tight = open(&tight_dir, 0);
    fill_and_update(&tight);

    assert!(
        tight.index_bytes() < roomy.index_bytes(),
        "consolidated: {} against {}",
        tight.index_bytes(),
        roomy.index_bytes()
    );
    assert!(
        tight.bucket_count(SH) < roomy.bucket_count(SH),
        "fewer entries: {} against {}",
        tight.bucket_count(SH),
        roomy.bucket_count(SH)
    );
    assert_records(&roomy, "with the spans");
    assert_records(&tight, "consolidated");

    let entries = tight.bucket_count(SH);
    drop(tight);
    let tight = open(&tight_dir, 0);
    assert_eq!(tight.bucket_count(SH), entries);
    assert_records(&tight, "reopened");
}

/// A crash after a batch's consolidation round, before the batch ends,
/// reopens from the batch before: the merged pages are named by no header
/// yet, and the runs they replace were not freed.
#[test]
fn a_crash_after_a_consolidation_round_reopens_whole() {
    let d = tempfile::tempdir().unwrap();
    let crashed = d.path().join("crashed");
    {
        let s = open(&d.path().join("db"), 0);
        fill_and_update(&s);
        let last = RECORDS + 1;
        block_on(s.apply(&[Write::Put(id(last), record(last, 0))])).unwrap();
        // Spans the batches had no round left to consolidate: a compaction
        // never ran, and the last updates' spans are still there.
        let spans = s.index_bytes();
        assert!(s.begin_cycle().unwrap());
        // The consolidation round: a window, the batch still open.
        assert_eq!(s.cycle_step(64).unwrap(), Progress::Settled);
        assert!(s.is_cycling());
        assert!(s.index_bytes() < spans, "the round merged");
        copy_dir(&d.path().join("db"), &crashed);
    }
    let s = open(&crashed, 0);
    assert_records(&s, "after the crash");
    // And it goes on consolidating from there.
    fill_and_update(&s);
    assert_records(&s, "written again");
}
