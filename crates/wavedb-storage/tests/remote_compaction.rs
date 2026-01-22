//! Compaction of the object store ([RFC 0070] §8, phase 7), proved against
//! a real S3 server: files nothing needs are deleted, the mostly free ones
//! are carried forward and deleted after, and every record still reads —
//! through the store, after a reopen, and from the object store alone. A
//! file the last cut still names is never deleted.
//!
//! [RFC 0070]: ../../../rfcs/0070-object-storage-backend.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{
    Fixture, RECORDS, SH, SLOT, big, big_id, fill, id, record, sampled,
    upload_all,
};
use wavedb_core::{Store, Write};
use wavedb_platform::carrier::block_on;
use wavedb_storage::{PageStore, StoreOptions};

/// The sampled keys read through the store as `want(key)` says, every
/// uploaded page out of RAM or off the disk first, so the reads go to the
/// object store.
fn assert_reads(s: &PageStore, want: impl Fn(u64) -> Option<Vec<u8>>) {
    s.evict_cold(0).unwrap();
    block_on(async {
        for k in sampled() {
            let got = s.get_of(SH, id(k)).await.unwrap();
            assert_eq!(got, want(k), "record {k}");
        }
    });
}

impl Fixture {
    /// The numbers of the files the object store holds, up to the newest.
    fn files(&self) -> Vec<u64> {
        let remote = self.remote().shard(0);
        let Some(key) = self.newest_file() else {
            return Vec::new();
        };
        let hex = key.rsplit('/').next().unwrap();
        let newest = !u64::from_str_radix(hex, 16).unwrap();
        (0..=newest)
            .filter(|&n| {
                let key = remote.file_key(n);
                block_on(self.server.bucket().get(&key, Some(0..1)))
                    .unwrap()
                    .is_some()
            })
            .collect()
    }
}

/// Every record rewritten twice, each version uploaded: the first two
/// versions' files hold nothing live, and go.
fn rewrite_three_times(s: &PageStore) {
    for version in 0..3 {
        fill(s, 0..RECORDS, version);
        upload_all(s);
    }
}

#[test]
fn rewritten_files_are_deleted_and_every_record_reads() {
    let fx = Fixture::new();
    let s = fx.open_object();
    rewrite_three_times(&s);
    let before = fx.files();
    let done = block_on(s.compact_remote()).unwrap();
    let after = fx.files();
    assert!(done.deleted_files > 0, "{done:?}");
    assert!(after.len() < before.len(), "{before:?} → {after:?}");
    assert_reads(&s, |k| Some(record(k, 2)));
    drop(s);
    let s = fx.open_object();
    assert_reads(&s, |k| Some(record(k, 2)));
}

/// With a disk, the files leave once the disk's roots stop naming them —
/// and the database still opens from the disk, and from its files alone.
#[test]
fn a_disk_and_object_store_compacts_and_reopens_both_ways() {
    let fx = Fixture::new();
    let s = fx.open();
    rewrite_three_times(&s);
    s.commit_journal().unwrap();
    s.commit_journal().unwrap();
    let before = fx.files();
    let done = block_on(s.compact_remote()).unwrap();
    assert!(done.deleted_files > 0, "{done:?}");
    assert!(fx.files().len() < before.len());
    assert_reads(&s, |k| Some(record(k, 2)));
    drop(s);
    let s = fx.open();
    assert_reads(&s, |k| Some(record(k, 2)));
    drop(s);
    std::fs::remove_dir_all(fx.db()).unwrap();
    let s = fx.open();
    assert_reads(&s, |k| Some(record(k, 2)));
}

/// A record rewritten per upload leaves small files, each still holding a
/// page or two the later ones did not rewrite — and the first file most of
/// the pages. Compaction carries what they hold forward into an upload of
/// its own and deletes them.
#[test]
fn small_files_are_carried_forward_and_deleted() {
    let fx = Fixture::new();
    let s = fx.open_object();
    fill(&s, 0..RECORDS, 0);
    upload_all(&s);
    for k in 0..10 {
        fill(&s, (k * 97)..=(k * 97), 1);
        upload_all(&s);
    }
    let before = fx.files();
    let done = block_on(s.compact_remote()).unwrap();
    let after = fx.files();
    assert!(done.moved_pages > 0, "{done:?}");
    assert!(after.len() * 2 < before.len(), "{before:?} → {after:?}");
    let want =
        |k: u64| Some(record(k, u64::from(k.is_multiple_of(97) && k < 970)));
    assert_reads(&s, want);
    drop(s);
    assert_reads(&fx.open_object(), want);
}

/// A second type, so an upload can leave the first one's page where it is.
const SH2: u64 = 0x5EED_0070_0B1E_C702;
static SLOT2: wavedb_storage::StructStorage =
    wavedb_storage::StructStorage::new(SH2);

fn other(k: u64) -> (wavedb_core::Id, Vec<u8>) {
    let id = wavedb_core::Id::new(k, wavedb_core::U48::from(1u32), false, 2);
    let mut bytes = SH2.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    (id, bytes)
}

/// An upload of part of a backlog is no cut: a recovery still takes the
/// cut before it. Here that cut names a page in an old file, below its
/// horizon, which the part just uploaded rewrote — so the store's own
/// directories no longer name the file, and only the cut does. It stays,
/// and a crash after the compaction recovers the cut whole.
#[test]
fn a_file_the_last_cut_names_is_kept() {
    let fx = Fixture::new();
    {
        let options = StoreOptions {
            remote: Some(fx.remote()),
            victim_free_percent: 100,
            ..StoreOptions::default()
        };
        let s = PageStore::open_object(&[&SLOT, &SLOT2], options).unwrap();
        block_on(async {
            // One small page of the first type, in the first file.
            for k in 0..10 {
                s.apply(&[Write::Put(id(k), record(k, 0))]).await.unwrap();
            }
        });
        upload_all(&s);
        // Two cuts of the other type: the first type's page stays in the
        // first file, restated, and the horizon passes that file.
        for k in 0..2 {
            let (id, bytes) = other(k);
            block_on(s.apply(&[Write::Put(id, bytes)])).unwrap();
            upload_all(&s);
        }
        block_on(async {
            for k in 0..200 {
                s.apply(&[Write::Put(big_id(k), big(k, 0))]).await.unwrap();
            }
            // 12 MiB of the first type: one upload carries 8 of them, no
            // cut, and rewrites its one remote page out of the first file.
            assert!(s.upload().await.unwrap().files > 0);
            s.compact_remote().await.unwrap();
        });
        assert!(s.pending_upload_bytes() > 0, "a part is still waiting");
    } // the crash: what waited is lost
    let s = PageStore::open_object(
        &[&SLOT, &SLOT2],
        StoreOptions {
            remote: Some(fx.remote()),
            ..StoreOptions::default()
        },
    )
    .unwrap();
    block_on(async {
        for k in 0..10 {
            assert_eq!(s.get_of(SH, id(k)).await.unwrap(), Some(record(k, 0)));
        }
        assert_eq!(s.get_of(SH, big_id(0)).await.unwrap(), None);
    });
}

/// No dictionary, so nothing but pages keeps the old files alive.
static PLAIN: wavedb_storage::StructStorage =
    wavedb_storage::StructStorage::without_compression(SH);

/// With a disk, the newest checkpoint is what a crash reopens from, and its
/// remote directories name files the store has since moved past. They stay
/// until two checkpoints say otherwise: reopened from the disk, the store
/// uploads on from those directories and every record reads.
#[test]
fn a_file_the_disk_s_checkpoint_names_is_kept() {
    let fx = Fixture::new();
    let options = || StoreOptions {
        relax_window: std::time::Duration::from_secs(3_600),
        remote: Some(fx.remote()),
        victim_free_percent: 100,
        ..StoreOptions::default()
    };
    {
        let s = PageStore::open_with(fx.db(), &[&PLAIN], options()).unwrap();
        fill(&s, 0..RECORDS, 0);
        upload_all(&s);
        s.commit_journal().unwrap();
        s.commit_journal().unwrap();
        for version in 1..3 {
            fill(&s, 0..RECORDS, version);
            upload_all(&s);
        }
        s.flush().unwrap();
        block_on(s.compact_remote()).unwrap();
    } // the crash: the disk reopens from the checkpoint of version 0
    let s = PageStore::open_with(fx.db(), &[&PLAIN], options()).unwrap();
    upload_all(&s);
    assert_reads(&s, |k| Some(record(k, 2)));
}
