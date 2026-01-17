//! Recovery from the object store alone ([RFC 0070] phase 5), proved
//! against a real S3 server: the disk of an object-backed database is
//! deleted, and the database opens from its files — every record reads
//! back, writes carry on, and what a crashed upload or a stray object left
//! in the prefix is never taken for the newest state.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{Fixture, RECORDS, SH, fill, id, record, sampled, upload_all};
use wavedb_core::{Id, Store, Write};
use wavedb_platform::carrier::block_on;
use wavedb_platform::s3::When;
use wavedb_storage::PageStore;

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
    /// The disk is gone: only the object store is left.
    fn lose_the_disk(&self) {
        std::fs::remove_dir_all(self.db()).unwrap();
    }

    /// The newest file's number, by the one-key listing.
    fn newest_number(&self) -> u64 {
        let key = self.newest_file().unwrap();
        let hex = key.rsplit('/').next().unwrap();
        !u64::from_str_radix(hex, 16).unwrap()
    }
}

#[test]
fn a_database_whose_disk_is_gone_opens_from_its_files() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
    }
    fx.lose_the_disk();

    let s = fx.open();
    assert!(sampled().all(|k| !s.is_resident(SH, id(k))), "all evicted");
    assert_reads(&s, |k| Some(record(k, 0)));

    // It carries on: writes land, go up, and survive losing the disk again.
    block_on(async {
        s.apply(&[Write::Put(id(5), record(5, 1))]).await.unwrap();
        s.apply(&[Write::Remove(SH, id(7))]).await.unwrap();
    });
    s.drain().unwrap();
    let before = fx.newest_number();
    upload_all(&s);
    assert!(fx.newest_number() > before, "new files, not file 0 again");
    drop(s);
    fx.lose_the_disk();

    let s = fx.open();
    assert_reads(&s, |k| match k {
        5 => Some(record(5, 1)),
        7 => None,
        _ => Some(record(k, 0)),
    });
}

/// The newest version of every record wins, however many uploads rewrote
/// it — the walk takes each bucket's newest entry.
#[test]
fn the_newest_upload_of_each_record_wins() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
        fill(&s, (0..RECORDS).filter(|k| k % 2 == 0), 1);
        upload_all(&s);
        fill(&s, (0..RECORDS).filter(|k| k % 3 == 0), 2);
        upload_all(&s);
    }
    fx.lose_the_disk();
    let s = fx.open();
    assert_reads(&s, |k| {
        let version = if k % 3 == 0 { 2 } else { u64::from(k % 2 == 0) };
        Some(record(k, version))
    });
}

/// An object in the prefix that is not a file of the chain — a copy of a
/// file under another number, or bytes that are no header — is skipped.
#[test]
fn a_stray_object_is_never_the_newest_file() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
    }
    let bucket = fx.server.bucket();
    let newest = fx.newest_number();
    let key = |n: u64| format!("db/1seg/{:016x}", !n);
    block_on(async {
        let copy = bucket.get(&key(newest), None).await.unwrap().unwrap();
        bucket
            .put(&key(newest + 2), &copy, When::Always)
            .await
            .unwrap();
        bucket
            .put(&key(newest + 5), b"no header", When::Always)
            .await
            .unwrap();
    });
    fx.lose_the_disk();
    let s = fx.open();
    assert_reads(&s, |k| Some(record(k, 0)));
}

/// A record of ~64 KiB that does not compress: a few hundred make several
/// files of an upload.
fn big(key: u64, version: u64) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    let mut x = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version ^ 1;
    for _ in 0..8_192 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

fn big_id(key: u64) -> Id {
    id(1_000_000 + key)
}

/// Uploads whose files all went up but which a crash kept off the disk are
/// redone after the reopen, smaller, past them: the first attempt's files
/// stay in the prefix, a branch no chain names. A recovery must take the
/// redo, not them — else the records removed before it would come back.
#[test]
fn a_crashed_uploads_leftover_files_are_not_the_newest() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        block_on(async {
            for k in 0..200 {
                s.apply(&[Write::Put(big_id(k), big(k, 0))]).await.unwrap();
            }
        });
        s.drain().unwrap();
        s.commit_journal().unwrap();
        upload_all(&s);
        // Dropped: nothing records the upload.
    }
    let crashed = fx.newest_number();
    assert!(crashed >= 1, "the first attempt wrote several files");
    {
        let s = fx.open();
        block_on(async {
            for k in 20..200 {
                s.apply(&[Write::Remove(SH, big_id(k))]).await.unwrap();
            }
        });
        s.drain().unwrap();
        upload_all(&s);
        s.commit_journal().unwrap();
    }
    assert!(
        fx.newest_number() > crashed,
        "the redo went past the leftovers"
    );
    fx.lose_the_disk();

    let s = fx.open();
    block_on(async {
        for k in 0..200 {
            let want = (k < 20).then(|| big(k, 0));
            let got = s.get_of(SH, big_id(k)).await.unwrap();
            assert_eq!(got, want, "big record {k}");
        }
    });
}
