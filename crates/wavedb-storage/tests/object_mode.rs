//! Object mode ([RFC 0070] §9 phase 6), proved against a real S3 server: a
//! store with no disk at all. A write is durable once an upload carried it;
//! every open is a recovery from the files; a crash loses only what no
//! upload carried, and never part of a batch; a second writer on the prefix
//! is fenced, not silent.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{
    Fixture, RECORDS, SH, SLOT, big, big_id, fill, id, record, sampled,
    upload_all,
};
use wavedb_core::{Store, Write};
use wavedb_platform::carrier::block_on;
use wavedb_storage::{
    PageStore, StorageError, StoreOptions, StructStorage, open_object_shards,
    open_shards_with,
};

/// The sampled keys read through the store as `want(key)` says.
fn assert_reads(s: &PageStore, want: impl Fn(u64) -> Option<Vec<u8>>) {
    block_on(async {
        for k in sampled() {
            let got = s.get_of(SH, id(k)).await.unwrap();
            assert_eq!(got, want(k), "record {k}");
        }
    });
}

impl Fixture {
    /// The newest file's number, by the one-key listing.
    fn newest_number(&self) -> Option<u64> {
        let key = self.newest_file()?;
        let hex = key.rsplit('/').next()?;
        Some(!u64::from_str_radix(hex, 16).ok()?)
    }
}

#[test]
fn a_database_lives_in_the_object_store_alone() {
    let fx = Fixture::new();
    {
        let s = fx.open_object();
        assert!(s.is_object_mode());
        assert_eq!(fx.newest_file(), None, "an open creates nothing");
        fill(&s, 0..RECORDS, 0);
        assert_reads(&s, |k| Some(record(k, 0)));
        upload_all(&s);
        assert_eq!(s.unuploaded_for(), None, "everything went up");
    }
    assert!(!fx.db().exists(), "no disk was ever touched");

    let s = fx.open_object();
    assert_reads(&s, |k| Some(record(k, 0)));
    // It carries on: writes land, go up, and survive another reopen.
    block_on(async {
        s.apply(&[Write::Put(id(5), record(5, 1))]).await.unwrap();
        s.apply(&[Write::Remove(SH, id(7))]).await.unwrap();
    });
    upload_all(&s);
    drop(s);
    let s = fx.open_object();
    assert_reads(&s, |k| match k {
        5 => Some(record(5, 1)),
        7 => None,
        _ => Some(record(k, 0)),
    });
}

/// What no upload carried is what a crash loses — and the upload interval
/// is measured from the oldest such write.
#[test]
fn a_crash_loses_only_what_no_upload_carried() {
    let fx = Fixture::new();
    {
        let s = fx.open_object();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
        assert_eq!(s.unuploaded_for(), None);
        fill(&s, (0..RECORDS).filter(|k| k % 2 == 0), 1);
        assert!(s.unuploaded_for().is_some(), "the clock runs");
        // Dropped without an upload: the crash.
    }
    let s = fx.open_object();
    assert_reads(&s, |k| Some(record(k, 0)));
}

/// An upload of part of a backlog is not a cut: a crash after it reopens at
/// the last cut, so no batch is ever half there. The files it left are
/// stepped over, never written again.
#[test]
fn a_part_of_a_backlog_is_never_recovered() {
    let fx = Fixture::new();
    {
        let s = fx.open_object();
        block_on(async {
            for k in 0..200 {
                s.apply(&[Write::Put(big_id(k), big(k, 0))]).await.unwrap();
            }
            // 12 MiB settled: one upload carries 8 of them.
            let done = s.upload().await.unwrap();
            assert!(done.files > 0);
        });
        assert!(s.pending_upload_bytes() > 0, "a part is still waiting");
    }
    let leftover = fx.newest_number().unwrap();
    {
        let s = fx.open_object();
        block_on(async {
            for k in 0..200 {
                let got = s.get_of(SH, big_id(k)).await.unwrap();
                assert_eq!(got, None, "big record {k} came back in part");
            }
            s.apply(&[Write::Put(big_id(0), big(0, 1))]).await.unwrap();
        });
        upload_all(&s);
    }
    assert!(fx.newest_number().unwrap() > leftover, "past the leftovers");
    let s = fx.open_object();
    block_on(async {
        let got = s.get_of(SH, big_id(0)).await.unwrap();
        assert_eq!(got, Some(big(0, 1)));
    });
}

/// Two stores on one prefix: the one that meets the other's file is fenced
/// and takes no more writes; nothing either wrote is overwritten.
#[test]
fn a_second_writer_is_fenced() {
    let fx = Fixture::new();
    let first = fx.open_object();
    fill(&first, 0..10, 0);
    upload_all(&first);

    let second = fx.open_object();
    fill(&first, 10..20, 0);
    upload_all(&first);
    fill(&second, 20..30, 0);
    let refused = block_on(second.upload_all());
    assert!(matches!(refused, Err(StorageError::Fenced)), "{refused:?}");
    assert!(second.is_fenced());
    let write = block_on(second.apply(&[Write::Put(id(40), record(40, 0))]));
    assert!(write.is_err(), "a fenced store takes no writes");

    drop((first, second));
    let s = fx.open_object();
    block_on(async {
        for k in 0..30 {
            let want = (k < 20).then(|| record(k, 0));
            assert_eq!(s.get_of(SH, id(k)).await.unwrap(), want, "{k}");
        }
    });
}

/// The RAM a store holds is a cache too: uploaded pages leave it, and the
/// room they leave goes back once compacted.
#[test]
fn evicted_pages_give_their_ram_back() {
    let fx = Fixture::new();
    let s = fx.open_object();
    fill(&s, 0..RECORDS, 0);
    upload_all(&s);
    let before = s.space().file_blocks;
    s.evict_cold(0).unwrap();
    s.compact().unwrap();
    let after = s.space().file_blocks;
    assert!(after * 4 < before, "{before} blocks, then {after}");
    assert_reads(&s, |k| Some(record(k, 0)));
}

/// What navigation reads stays resident (§10): eviction takes record pages
/// only, so a walk never waits on the object store.
#[test]
fn navigation_is_never_evicted() {
    const NAV: u64 = 0x5EED_0070_0B1E_C702;
    static NAV_SLOT: StructStorage = StructStorage::new(NAV).resident();
    let nav = |k: u64| {
        let mut bytes = NAV.to_le_bytes().to_vec();
        bytes.extend_from_slice(&record(k, 0)[8..]);
        bytes
    };
    let fx = Fixture::new();
    let options = StoreOptions {
        remote: Some(fx.remote()),
        ..StoreOptions::default()
    };
    let s = PageStore::open_object(&[&SLOT, &NAV_SLOT], options).unwrap();
    fill(&s, 0..RECORDS, 0);
    block_on(async {
        for k in 0..RECORDS {
            s.apply(&[Write::Put(id(k), nav(k))]).await.unwrap();
        }
    });
    upload_all(&s);
    s.evict_cold(0).unwrap();
    assert!(
        sampled().all(|k| s.is_resident(NAV, id(k))),
        "navigation stays"
    );
    assert!(
        sampled().all(|k| !s.is_resident(SH, id(k))),
        "records leave"
    );
}

/// A sharded database keeps its `MANIFEST` in the object store too: a
/// database in disk + object mode, closed with everything uploaded,
/// reopens in object mode — the conversion is a close and an open — with
/// its own count, whatever count the open asks for.
#[test]
fn a_sharded_database_moves_to_object_mode_by_reopening() {
    let fx = Fixture::new();
    let options = || StoreOptions {
        remote: Some(fx.remote()),
        ..StoreOptions::default()
    };
    {
        let stores =
            open_shards_with(fx.db(), &[&SLOT], options(), 4, 2).unwrap();
        for (k, s) in (0u64..).zip(&stores) {
            block_on(s.apply(&[Write::Put(id(k), record(k, 0))])).unwrap();
            upload_all(s);
        }
    }
    let stores = open_object_shards(&[&SLOT], options(), 64, 2).unwrap();
    assert_eq!(stores.len(), 4, "the count the database was created with");
    for (k, s) in (0u64..).zip(&stores) {
        assert!(s.is_object_mode());
        let got = block_on(s.get_of(SH, id(k))).unwrap();
        assert_eq!(got, Some(record(k, 0)), "shard {k}");
    }
}
