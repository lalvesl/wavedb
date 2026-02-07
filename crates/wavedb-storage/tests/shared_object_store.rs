//! Data shards sharing a storage shard's chain of files in the object store
//! ([RFC 0073] §8), proved against a real S3 server: their uploads go up
//! in one file — one PUT — and the database comes back from the disk, from
//! the object store alone, and in object mode.
//!
//! [RFC 0073]: ../../../rfcs/0073-data-shards-and-storage-shards.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

mod common;

use common::{Fixture, SH, SLOT, id, record};
use futures::future::join_all;
use wavedb_core::{Store, Write};
use wavedb_platform::carrier::block_on;
use wavedb_storage::{
    PageStore, StoreOptions, open_object_shards, open_shards_with,
};

/// Four data shards through two storage shards.
const SHARDS: usize = 4;

impl Fixture {
    fn shared_options(&self) -> StoreOptions {
        StoreOptions {
            remote: Some(self.remote()),
            storage_shards: 2,
            ..StoreOptions::default()
        }
    }

    /// The database, disk + object.
    fn open_shared(&self) -> Vec<PageStore> {
        let options = self.shared_options();
        open_shards_with(self.db(), &[&SLOT], options, SHARDS, 2).unwrap()
    }

    /// The same database in object mode.
    fn open_shared_object(&self) -> Vec<PageStore> {
        let options = self.shared_options();
        open_object_shards(&[&SLOT], options, SHARDS, 2).unwrap()
    }

    /// Whether storage shard 0's file `file` is in the object store.
    fn has_file(&self, file: u64) -> bool {
        let key = format!("db/storage/00/1seg/{:016x}", !file);
        block_on(self.server.bucket().get(&key, Some(0..1)))
            .unwrap()
            .is_some()
    }
}

/// Every data shard's own keys: `100 · position + k`.
const fn key(shard: usize, k: u64) -> u64 {
    shard as u64 * 100 + k
}

fn write(stores: &[PageStore], version: u64) {
    for (i, s) in stores.iter().enumerate() {
        block_on(async {
            for k in 0..20 {
                let w = Write::Put(id(key(i, k)), record(key(i, k), version));
                s.apply(&[w]).await.unwrap();
            }
        });
        s.drain().unwrap();
    }
}

fn assert_reads(stores: &[PageStore], version: u64) {
    for (i, s) in stores.iter().enumerate() {
        for k in 0..20 {
            let got = block_on(s.get_of(SH, id(key(i, k)))).unwrap();
            assert_eq!(got, Some(record(key(i, k), version)), "{i}/{k}");
        }
    }
}

/// The data shards of a storage shard uploading together go up in one
/// file: one PUT where each used to make its own.
#[test]
fn data_shards_upload_together_in_one_file() {
    let fx = Fixture::new();
    let stores = fx.open_shared();
    write(&stores, 1);
    let uploads = join_all(stores.iter().map(PageStore::upload));
    for up in block_on(uploads) {
        assert!(up.unwrap().files > 0, "every data shard sent its section");
    }
    assert!(fx.has_file(0), "one file for storage shard 0");
    assert!(!fx.has_file(1), "and only one");
    for s in &stores {
        assert_eq!(s.pending_upload_bytes(), 0);
    }
    for (i, s) in stores.iter().enumerate() {
        let k = key(i, 3);
        let got = block_on(s.read_uploaded(SH, id(k))).unwrap();
        assert_eq!(got, Some(record(k, 1)), "{i} read from the object store");
    }
}

/// A database whose disk is gone comes back from its files alone: every
/// data shard roots in the storage shard's roots block.
#[test]
fn the_object_store_alone_brings_every_data_shard_back() {
    let fx = Fixture::new();
    {
        let stores = fx.open_shared();
        write(&stores, 1);
        for s in &stores {
            block_on(s.upload_all()).unwrap();
        }
        write(&stores, 2);
        for s in &stores {
            block_on(s.upload_all()).unwrap();
        }
    }
    std::fs::remove_dir_all(fx.db().join("storage")).unwrap();
    let stores = fx.open_shared();
    assert_reads(&stores, 2);
    // And it carries on.
    write(&stores, 3);
    for s in &stores {
        block_on(s.upload_all()).unwrap();
    }
    drop(stores);
    assert_reads(&fx.open_shared(), 3);
}

/// Object mode: every data shard in RAM, uploading through the storage
/// shard's thread, back after every reopen.
#[test]
fn object_mode_shares_the_storage_shards_files() {
    let fx = Fixture::new();
    {
        let stores = fx.open_shared_object();
        write(&stores, 1);
        let uploads = join_all(stores.iter().map(PageStore::upload_all));
        for up in block_on(uploads) {
            up.unwrap();
        }
    }
    assert!(!fx.db().exists(), "no disk was ever touched");
    let stores = fx.open_shared_object();
    assert_reads(&stores, 1);
    write(&stores, 2);
    for s in &stores {
        block_on(s.upload_all()).unwrap();
    }
    drop(stores);
    assert_reads(&fx.open_shared_object(), 2);
}

/// A file goes once none of the storage shard's data shards needs it: each
/// compaction reports, and the round that has every report deletes — and
/// the database still comes back from what is left.
#[test]
fn compaction_deletes_what_no_data_shard_needs() {
    let fx = Fixture::new();
    let stores = fx.open_shared();
    for version in 1..=3 {
        write(&stores, version);
        for s in &stores {
            block_on(s.upload_all()).unwrap();
        }
    }
    assert!(fx.has_file(0));
    let mut deleted = 0;
    for _ in 0..2 {
        for s in &stores {
            deleted += block_on(s.compact_remote()).unwrap().deleted_files;
        }
    }
    assert!(deleted > 0, "the superseded files went");
    assert!(!fx.has_file(0), "the first, superseded by every data shard");
    assert_reads(&stores, 3);
    drop(stores);
    std::fs::remove_dir_all(fx.db().join("storage")).unwrap();
    assert_reads(&fx.open_shared(), 3);
}
