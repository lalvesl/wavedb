//! Eviction from disk ([RFC 0070] phase 4), proved against a real S3 server:
//! the disk of an object-backed store is a cache, its coldest uploaded pages
//! leave it, and everything they held still reads back — from the object
//! store, until a write (or a read, through the upkeep) brings the bucket
//! back. A settle never meets an evicted bucket, so a page on disk stays the
//! whole truth about its bucket.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{
    Fixture, RECORDS, SH, assert_uploaded, fill, id, record, sampled,
    upload_all,
};
use wavedb_core::{Store, Write};
use wavedb_platform::carrier::block_on;
use wavedb_storage::PageStore;

/// Blocks in use: the file, less what is free or held back.
fn live(s: &PageStore) -> u64 {
    let space = s.space();
    space.file_blocks - space.free_blocks - space.deferred_blocks
}

/// Fill, settle and upload everything, and empty the record cache, so
/// every read comes off a page.
fn uploaded(fx: &Fixture) -> PageStore {
    let s = fx.open();
    fill(&s, 0..RECORDS, 0);
    upload_all(&s);
    s.evict_settled(0);
    s
}

/// The sampled keys read through the store as `want(key)` says.
fn assert_reads(s: &PageStore, want: impl Fn(u64) -> Option<Vec<u8>>) {
    block_on(async {
        for k in sampled() {
            let got = s.get_of(SH, id(k)).await.unwrap();
            assert_eq!(got, want(k), "record {k}");
        }
    });
}

#[test]
fn evicted_pages_read_back_from_the_object_store() {
    let fx = Fixture::new();
    let s = uploaded(&fx);
    let before = live(&s);
    let evicted = s.evict_cold(0).unwrap();
    assert!(evicted > 0 && live(&s) < before, "{evicted} of {before}");
    assert!(sampled().all(|k| !s.is_resident(SH, id(k))));
    assert_reads(&s, |k| Some(record(k, 0)));
}

/// A page still to upload never leaves: the disk holds the only copy.
#[test]
fn a_page_not_uploaded_stays() {
    let fx = Fixture::new();
    let s = fx.open();
    fill(&s, 0..RECORDS, 0);
    assert_eq!(s.evict_cold(0).unwrap(), 0);
    assert!(sampled().all(|k| s.is_resident(SH, id(k))));
}

/// The least recently used pages leave first: a bucket just read stays.
#[test]
fn the_coldest_pages_leave_first() {
    let fx = Fixture::new();
    let s = uploaded(&fx);
    let hot = [3, 700, 1_400];
    block_on(async {
        for k in hot {
            s.get_of(SH, id(k)).await.unwrap();
        }
    });
    s.evict_cold(live(&s) / 2).unwrap();
    for k in hot {
        assert!(s.is_resident(SH, id(k)), "hot record {k} was evicted");
    }
    assert!(
        sampled().any(|k| !s.is_resident(SH, id(k))),
        "half the pages left"
    );
}

/// A write brings its bucket back before it commits — puts, removals and
/// guarded saves alike — and the result reaches the object store again.
#[test]
fn a_write_brings_its_bucket_back() {
    let fx = Fixture::new();
    let s = uploaded(&fx);
    s.evict_cold(0).unwrap();
    block_on(async {
        s.apply(&[Write::Put(id(7), record(7, 1))]).await.unwrap();
        s.apply(&[Write::Remove(SH, id(14))]).await.unwrap();
        // A guard reads the evicted bucket too, and still tells a stale
        // expectation from a current one.
        let stale = s
            .apply(&[
                Write::Expect(SH, id(21), Some(record(21, 9))),
                Write::Put(id(21), record(21, 1)),
            ])
            .await;
        assert!(
            matches!(stale, Err(wavedb_core::Error::Conflict(_))),
            "{stale:?}"
        );
        s.apply(&[
            Write::Expect(SH, id(21), Some(record(21, 0))),
            Write::Put(id(21), record(21, 1)),
        ])
        .await
        .unwrap();
    });
    for k in [7, 14, 21] {
        assert!(s.is_resident(SH, id(k)), "record {k}'s bucket came back");
    }
    s.drain().unwrap();
    upload_all(&s);
    s.evict_settled(0);
    let want = |k: u64| match k {
        7 | 21 => Some(record(k, 1)),
        14 => None,
        _ => Some(record(k, 0)),
    };
    assert_reads(&s, want);
    assert_uploaded(&s, want);
}

/// A read of an evicted bucket marks it wanted; the upkeep brings it back.
#[test]
fn a_bucket_read_from_the_object_store_comes_back() {
    let fx = Fixture::new();
    let s = uploaded(&fx);
    s.evict_cold(0).unwrap();
    block_on(async {
        assert_eq!(s.get_of(SH, id(35)).await.unwrap(), Some(record(35, 0)));
        assert_eq!(s.restore_wanted(16).await.unwrap(), 1);
    });
    assert!(s.is_resident(SH, id(35)));
    assert_eq!(s.pending_upload_bytes(), 0, "what came back is uploaded");
}

/// A batch records the eviction, so a reopen keeps it — and compaction then
/// gives the room back to the filesystem.
#[test]
fn a_reopen_keeps_what_was_evicted_and_compaction_shrinks_the_file() {
    let fx = Fixture::new();
    {
        let s = uploaded(&fx);
        s.evict_cold(0).unwrap();
        s.drain().unwrap();
    }
    let s = fx.open();
    assert!(sampled().all(|k| !s.is_resident(SH, id(k))));
    assert_reads(&s, |k| Some(record(k, 0)));

    let before = s.space().file_blocks;
    let done = s.compact().unwrap();
    assert!(done.reclaimed_blocks > 0, "{done:?}");
    assert!(s.space().file_blocks < before);
    assert_reads(&s, |k| Some(record(k, 0)));
}

/// An eviction no batch recorded is simply undone by a crash: the last
/// durable batch still names the pages, and their runs were held back.
#[test]
fn an_eviction_lost_in_a_crash_loses_nothing() {
    let fx = Fixture::new();
    let crashed = {
        let s = uploaded(&fx);
        s.drain().unwrap();
        s.evict_cold(0).unwrap();
        s.reclaim().unwrap(); // cuts nothing the durable batch names
        fx.crashed() // before any window records it
    };
    let s = fx.open_at(&crashed);
    assert!(sampled().all(|k| s.is_resident(SH, id(k))));
    assert_reads(&s, |k| Some(record(k, 0)));
}

/// A write into an evicted bucket brings the bucket back before it
/// commits, and the close settles both.
#[test]
fn a_write_brings_its_evicted_bucket_back() {
    let fx = Fixture::new();
    {
        let s = uploaded(&fx);
        s.evict_cold(0).unwrap();
        s.drain().unwrap();
        block_on(s.apply(&[Write::Put(id(42), record(42, 1))])).unwrap();
        assert!(s.is_resident(SH, id(42)));
    }
    let s = fx.open();
    assert!(s.is_resident(SH, id(42)));
    assert_reads(&s, |k| Some(record(k, u64::from(k == 42))));
}
