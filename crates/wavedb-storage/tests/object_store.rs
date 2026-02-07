//! A store whose pages live in an object store, its disk caching them
//! ([RFC 0070] phase 3), proved against a real S3 server — the dev shell's
//! versitygw, one per test over its own directory.
//!
//! Every record settled to disk is uploaded, reassembled into the object
//! store's pages, and reads back from there; a removal leaves the object
//! store too; the remote directories survive a reopen; an upload a crash
//! kept off the disk is simply made again.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{
    Fixture, RECORDS, SH, SLOT, assert_uploaded, fill, id, record, upload_all,
};
use wavedb_core::{Store, Write};
use wavedb_platform::carrier::block_on;
use wavedb_storage::{PageStore, StorageError, StoreOptions};

#[test]
fn every_settled_record_reads_back_from_the_object_store() {
    let fx = Fixture::new();
    let s = fx.open();
    fill(&s, 0..RECORDS, 0);
    let pending = s.pending_upload_bytes();
    assert!(
        pending > 1 << 20,
        "the settle flags what it writes: {pending}"
    );

    let done = block_on(s.upload()).unwrap();
    assert!(done.files >= 1 && done.bytes > 0, "{done:?}");
    upload_all(&s);
    assert_uploaded(&s, |k| Some(record(k, 0)));
    assert_eq!(
        fx.newest_file(),
        Some(format!("db/storage/00/1seg/{:016x}", !0u64))
    );
}

/// The flagged disk page is the whole truth about its bucket, so a record
/// removed from it leaves the object store's page too, and a rewritten one
/// replaces its old bytes there.
#[test]
fn removals_and_rewrites_reach_the_object_store() {
    let fx = Fixture::new();
    let s = fx.open();
    fill(&s, 0..RECORDS, 0);
    upload_all(&s);

    block_on(async {
        for k in (0..RECORDS).filter(|k| k % 3 == 0) {
            s.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
        }
    });
    fill(&s, (0..RECORDS).filter(|k| k % 3 == 1), 1);
    upload_all(&s);
    assert_uploaded(&s, |k| match k % 3 {
        0 => None,
        1 => Some(record(k, 1)),
        _ => Some(record(k, 0)),
    });
}

/// What an upload changed rides the next window's header, so a reopen
/// restores the remote directories and the flags — and the
/// next upload continues after the last file instead of over it.
#[test]
fn a_reopen_restores_what_was_uploaded() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
        // A later batch records the upload.
        fill(&s, 0..10, 1);
        s.drain().unwrap();
    }
    let s = fx.open();
    let pending = s.pending_upload_bytes();
    assert!(pending > 0, "the ten rewritten records are still to go up");
    assert!(pending < 1 << 20, "the rest went up before: {pending}");
    assert_uploaded(&s, |k| Some(record(k, 0)));

    let first = fx.newest_file();
    upload_all(&s);
    assert_ne!(fx.newest_file(), first, "a new file, not the old one again");
    assert_uploaded(&s, |k| Some(record(k, u64::from(k < 10))));
}

/// An upload a crash kept off the disk is made again: the flags are still
/// set — and the new files go past the lost ones, which stay where they
/// are: a file number once written is never written again.
#[test]
fn an_upload_lost_in_a_crash_is_made_again() {
    let fx = Fixture::new();
    let crashed = {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        s.drain().unwrap();
        upload_all(&s);
        fx.crashed() // no window after the upload: nothing records it
    };
    let s = fx.open_at(&crashed);
    assert!(
        s.pending_upload_bytes() > 1 << 20,
        "every flag is still set"
    );
    block_on(async {
        assert_eq!(s.read_uploaded(SH, id(0)).await.unwrap(), None);
    });
    let lost = fx.newest_file();
    upload_all(&s);
    assert!(fx.newest_file() < lost, "past the lost upload's files");
    assert_uploaded(&s, |k| Some(record(k, 0)));
}

/// Compaction moves pages without changing them: a page the object store
/// has not received keeps its flag where it lands.
#[test]
fn compaction_keeps_what_is_still_to_upload() {
    let fx = Fixture::new();
    let s = fx.open();
    fill(&s, 0..RECORDS, 0);
    s.drain().unwrap();
    block_on(async {
        for k in (0..RECORDS).filter(|k| k % 3 != 0) {
            s.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
        }
    });
    s.drain().unwrap();
    let pending = s.pending_upload_bytes();
    let done = s.compact().unwrap();
    assert!(done.reclaimed_blocks > 0, "{done:?}");
    assert_eq!(s.pending_upload_bytes(), pending);

    upload_all(&s);
    assert_uploaded(&s, |k| (k % 3 == 0).then(|| record(k, 0)));
}

/// The mode is fixed when the database is created.
#[test]
fn the_mode_is_fixed_at_creation() {
    let fx = Fixture::new();
    drop(fx.open());
    let err = PageStore::open(fx.db(), &[&SLOT]).unwrap_err();
    assert!(
        matches!(
            err,
            StorageError::ModeMismatch {
                object_backed: true
            }
        ),
        "{err:?}"
    );

    let disk = fx.dir.path().join("disk");
    drop(PageStore::open(&disk, &[&SLOT]).unwrap());
    let options = StoreOptions {
        remote: Some(fx.remote()),
        ..StoreOptions::default()
    };
    let err = PageStore::open_with(&disk, &[&SLOT], options).unwrap_err();
    assert!(
        matches!(
            err,
            StorageError::ModeMismatch {
                object_backed: false
            }
        ),
        "{err:?}"
    );
    let disk_only = PageStore::open(&disk, &[&SLOT]).unwrap();
    assert_eq!(disk_only.pending_upload_bytes(), 0);
    assert!(matches!(
        block_on(disk_only.upload()),
        Err(StorageError::NotObjectBacked)
    ));
}
