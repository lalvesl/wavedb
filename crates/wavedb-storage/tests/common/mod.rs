//! What the object-store tests share: a database directory with a versitygw
//! behind it, records that take real room, and the upload loop.

// Each test binary uses its own subset.
#![allow(dead_code)]

use std::path::PathBuf;

use wavedb_core::{Id, Store, U48, Write};
use wavedb_platform::carrier::block_on;
use wavedb_platform::s3::test_server::TestServer;
use wavedb_storage::{PageStore, Remote, StoreOptions, StructStorage};

pub const SH: u64 = 0x5EED_0070_0B1E_C701;
pub static SLOT: StructStorage = StructStorage::new(SH);

pub const RECORDS: u64 = 1_500;

/// A database directory and the object store behind it.
pub struct Fixture {
    pub server: TestServer,
    pub dir: tempfile::TempDir,
}

impl Fixture {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let server = TestServer::start(&dir.path().join("objects"));
        Self { server, dir }
    }

    pub fn db(&self) -> PathBuf {
        self.dir.path().join("db")
    }

    pub fn remote(&self) -> Remote {
        Remote::new(self.server.bucket(), "db/")
    }

    /// Batches share barriers (RFC 0061): what a reopen checks was covered
    /// by a checkpoint first.
    pub fn open(&self) -> PageStore {
        let options = StoreOptions {
            relax_window: std::time::Duration::from_secs(3_600),
            remote: Some(self.remote()),
            ..StoreOptions::default()
        };
        PageStore::open_with(self.db(), &[&SLOT], options).unwrap()
    }

    /// The newest file's key, by the one-key listing recovery will use.
    pub fn newest_file(&self) -> Option<String> {
        block_on(self.server.bucket().first_key("db/1seg/")).unwrap()
    }
}

pub fn id(key: u64) -> Id {
    Id::new(key, U48::from(1u32), false, (SH & 0x7FFF) as u16)
}

/// A record of ~1 KiB that does not compress, so pages take real room.
pub fn record(key: u64, version: u64) -> Vec<u8> {
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

pub fn fill(s: &PageStore, keys: impl Iterator<Item = u64>, version: u64) {
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
pub fn upload_all(s: &PageStore) {
    while s.pending_upload_bytes() > 0 {
        let done = block_on(s.upload()).unwrap();
        assert!(done.pages > 0, "an upload with work pending moved nothing");
    }
}

/// The keys read back from the object store: the first ten, and one in
/// seven after them — every residue the tests set apart, without a whole
/// page decoded per key over the full range in a debug build.
pub fn sampled() -> impl Iterator<Item = u64> {
    (0..RECORDS).filter(|k| *k < 10 || k % 7 == 0)
}

/// The sampled keys read back from the object store as `want(key)` says.
pub fn assert_uploaded(s: &PageStore, want: impl Fn(u64) -> Option<Vec<u8>>) {
    block_on(async {
        for k in sampled() {
            let got = s.read_uploaded(SH, id(k)).await.unwrap();
            assert_eq!(got, want(k), "record {k} in the object store");
        }
    });
}
