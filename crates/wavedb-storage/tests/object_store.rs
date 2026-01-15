//! A store whose pages live in an object store, its disk caching them
//! ([RFC 0070] phase 3), proved against a real S3 server — the dev shell's
//! versitygw, one per test over its own directory.
//!
//! Every record settled to disk is uploaded, reassembled into the object
//! store's pages, and reads back from there; a removal leaves the object
//! store too; the remote directories survive a reopen; an upload a crash
//! kept off the disk is simply made again.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend-WIP.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::path::PathBuf;

use wavedb_core::{Id, Store, U48, Write};
use wavedb_platform::carrier::block_on;
use wavedb_platform::s3::test_server::TestServer;
use wavedb_storage::{
    PageStore, Remote, StorageError, StoreOptions, StructStorage,
};

const SH: u64 = 0x5EED_0070_0B1E_C701;
static SLOT: StructStorage = StructStorage::new(SH);

const RECORDS: u64 = 1_500;

/// A database directory and the object store behind it.
struct Fixture {
    server: TestServer,
    dir: tempfile::TempDir,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let server = TestServer::start(&dir.path().join("objects"));
        Self { server, dir }
    }

    fn db(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    fn remote(&self) -> Remote {
        Remote::new(self.server.bucket(), "db/")
    }

    /// Batches share barriers (RFC 0061): what a reopen checks was covered
    /// by a checkpoint first.
    fn open(&self) -> PageStore {
        let options = StoreOptions {
            relax_window: std::time::Duration::from_secs(3_600),
            remote: Some(self.remote()),
            ..StoreOptions::default()
        };
        PageStore::open_with(self.db(), &[&SLOT], options).unwrap()
    }

    /// The newest file's key, by the one-key listing recovery will use.
    fn newest_file(&self) -> Option<String> {
        block_on(self.server.bucket().first_key("db/1seg/")).unwrap()
    }
}

fn id(key: u64) -> Id {
    Id::new(key, U48::from(1u32), false, (SH & 0x7FFF) as u16)
}

/// A record of ~1 KiB that does not compress, so pages take real room.
fn record(key: u64, version: u64) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    bytes.extend_from_slice(&key.to_le_bytes());
    let mut x = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ version ^ 1;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

fn fill(s: &PageStore, keys: impl Iterator<Item = u64>, version: u64) {
    block_on(async {
        for k in keys {
            s.apply(&[Write::Put(id(k), record(k, version))])
                .await
                .unwrap();
        }
    });
    s.drain().unwrap();
}

/// Upload until nothing is pending.
fn upload_all(s: &PageStore) {
    while s.pending_upload_bytes() > 0 {
        let done = block_on(s.upload()).unwrap();
        assert!(done.pages > 0, "an upload with work pending moved nothing");
    }
}

/// The keys read back from the object store: the first ten, and one in
/// seven after them — every residue the tests set apart, without a whole
/// page decoded per key over the full range in a debug build.
fn sampled() -> impl Iterator<Item = u64> {
    (0..RECORDS).filter(|k| *k < 10 || k % 7 == 0)
}

/// The sampled keys read back from the object store as `want(key)` says.
fn assert_uploaded(s: &PageStore, want: impl Fn(u64) -> Option<Vec<u8>>) {
    block_on(async {
        for k in sampled() {
            let got = s.read_uploaded(SH, id(k)).await.unwrap();
            assert_eq!(got, want(k), "record {k} in the object store");
        }
    });
}

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
    assert_eq!(fx.newest_file(), Some(format!("db/1seg/{:016x}", !0u64)));
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

/// What an upload changed rides the next window into the disk's addressing
/// log, so a reopen restores the remote directories and the flags — and the
/// next upload continues after the last file instead of over it.
#[test]
fn a_reopen_restores_what_was_uploaded() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
        // A later round records the upload; the checkpoint roots it.
        fill(&s, 0..10, 1);
        s.commit_journal().unwrap();
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
/// set, the end is still where it was, and the file is written over.
#[test]
fn an_upload_lost_in_a_crash_is_made_again() {
    let fx = Fixture::new();
    {
        let s = fx.open();
        fill(&s, 0..RECORDS, 0);
        s.commit_journal().unwrap();
        upload_all(&s);
        // Dropped with no window after the upload: nothing records it.
    }
    let s = fx.open();
    assert!(
        s.pending_upload_bytes() > 1 << 20,
        "every flag is still set"
    );
    block_on(async {
        assert_eq!(s.read_uploaded(SH, id(0)).await.unwrap(), None);
    });
    upload_all(&s);
    assert_eq!(
        fx.newest_file(),
        Some(format!("db/1seg/{:016x}", !0u64)),
        "the same file numbers again"
    );
    assert_uploaded(&s, |k| Some(record(k, 0)));
}

/// Compaction moves pages without changing them: a page the object store
/// has not received keeps its flag where it lands.
#[test]
fn compaction_keeps_what_is_still_to_upload() {
    let fx = Fixture::new();
    let s = fx.open();
    fill(&s, 0..RECORDS, 0);
    s.commit_journal().unwrap();
    block_on(async {
        for k in (0..RECORDS).filter(|k| k % 3 != 0) {
            s.apply(&[Write::Remove(SH, id(k))]).await.unwrap();
        }
    });
    s.commit_journal().unwrap();
    s.commit_journal().unwrap();
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
