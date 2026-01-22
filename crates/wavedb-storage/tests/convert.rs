//! Converting a database between disk-only and object-backed ([RFC 0070]
//! §9), proved against a real S3 server: a disk database moves its pages
//! up, an object-store database brings them down, and each opens in its
//! new mode with every record — and refuses the old one.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{
    Fixture, RECORDS, SH, SLOT, assert_uploaded, fill, id, record, upload_all,
};
use wavedb_core::Store;
use wavedb_platform::carrier::block_on;
use wavedb_storage::{Backend, PageStore, StorageError, StoreOptions, convert};

impl Fixture {
    /// The options a conversion takes: this fixture's object store.
    fn remote_options(&self) -> StoreOptions {
        StoreOptions {
            remote: Some(self.remote()),
            ..StoreOptions::default()
        }
    }

    /// The database opened disk-only.
    fn open_disk(&self) -> PageStore {
        PageStore::open(self.db(), &[&SLOT]).unwrap()
    }

    fn convert(&self, to: Backend) -> Result<(), StorageError> {
        convert(self.db(), &[&SLOT], self.remote_options(), to)
    }
}

/// Every record reads back through `s`, its caches emptied first.
fn assert_reads(s: &PageStore, version: u64) {
    s.evict_settled(0);
    block_on(async {
        for k in 0..RECORDS {
            let got = s.get_of(SH, id(k)).await.unwrap();
            assert_eq!(got, Some(record(k, version)), "record {k}");
        }
    });
}

#[test]
fn a_disk_database_moves_to_the_object_store() {
    let fx = Fixture::new();
    {
        let s = fx.open_disk();
        fill(&s, 0..RECORDS, 0);
        s.commit_journal().unwrap();
    }
    fx.convert(Backend::ObjectStore).unwrap();
    fx.convert(Backend::ObjectStore).unwrap(); // done is done
    let s = fx.open();
    upload_all(&s);
    assert_uploaded(&s, |k| Some(record(k, 0)));
    drop(s);
    assert!(matches!(
        PageStore::open(fx.db(), &[&SLOT]).unwrap_err(),
        StorageError::ModeMismatch {
            object_backed: true
        }
    ));
    // The disk gone, the database opens from its files alone.
    std::fs::remove_dir_all(fx.db()).unwrap();
    assert_reads(&fx.open_object(), 0);
}

/// An object store already holding a database is never written over.
#[test]
fn a_prefix_holding_a_database_is_refused() {
    let fx = Fixture::new();
    let other = fx.open_object();
    fill(&other, 0..10, 0);
    upload_all(&other);
    drop(other);
    drop(fx.open_disk());
    let err = fx.convert(Backend::ObjectStore).unwrap_err();
    assert!(matches!(err, StorageError::ObjectStoreInUse), "{err:?}");
    assert!(fx.open_disk().space().file_blocks == 0, "still disk-only");
}

/// A database that lived in the object store alone comes down to a disk it
/// never had, every page brought back; the files it leaves stay.
#[test]
fn an_object_store_database_comes_down_to_the_disk() {
    let fx = Fixture::new();
    {
        let s = fx.open_object();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
    }
    fx.convert(Backend::Disk).unwrap();
    fx.convert(Backend::Disk).unwrap(); // done is done
    assert_reads(&fx.open_disk(), 0);
    assert!(fx.newest_file().is_some(), "the files are kept");
    assert!(matches!(
        fx.open_with_remote().unwrap_err(),
        StorageError::ModeMismatch {
            object_backed: false
        }
    ));
}

/// A disk + object database whose cold pages left the disk brings them
/// back before it stops being object-backed.
#[test]
fn evicted_pages_come_back_before_the_mode_changes() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
        s.evict_cold(0).unwrap();
        s.commit_journal().unwrap();
    }
    fx.convert(Backend::Disk).unwrap();
    let s = fx.open_disk();
    assert_reads(&s, 0);
    // And it carries on as a disk database.
    fill(&s, 0..RECORDS, 1);
    s.commit_journal().unwrap();
    drop(s);
    assert_reads(&fx.open_disk(), 1);
}

#[test]
fn nothing_to_convert_is_no_database() {
    let fx = Fixture::new();
    for to in [Backend::ObjectStore, Backend::Disk] {
        let err = fx.convert(to).unwrap_err();
        assert!(matches!(err, StorageError::NoDatabase), "{err:?}");
    }
}

impl Fixture {
    fn open_with_remote(&self) -> Result<PageStore, StorageError> {
        PageStore::open_with(self.db(), &[&SLOT], self.remote_options())
    }
}
