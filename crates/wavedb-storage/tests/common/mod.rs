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

    /// The database, disk + object (RFC 0070).
    pub fn open(&self) -> PageStore {
        self.open_at(&self.db())
    }

    /// The database at `dir` — this fixture's, or a crash's copy of it.
    pub fn open_at(&self, dir: &std::path::Path) -> PageStore {
        let options = StoreOptions {
            remote: Some(self.remote()),
            ..StoreOptions::default()
        };
        PageStore::open_with(dir, &[&SLOT], options).unwrap()
    }

    /// What a crash would leave of the database: its files as they are now,
    /// copied while the store holds them open — the windows it wrote are
    /// there, what it holds in RAM is not.
    pub fn crashed(&self) -> PathBuf {
        let to = self.dir.path().join("crashed");
        copy_dir(&self.db(), &to);
        to
    }

    /// The same database in object mode: no disk at all (RFC 0070 §9).
    pub fn open_object(&self) -> PageStore {
        let options = StoreOptions {
            remote: Some(self.remote()),
            ..StoreOptions::default()
        };
        PageStore::open_object(&[&SLOT], options).unwrap()
    }

    /// The newest file's key, by the one-key listing recovery will use.
    pub fn newest_file(&self) -> Option<String> {
        block_on(self.server.bucket().first_key("db/storage/00/1seg/")).unwrap()
    }
}

/// Copy the directory tree `from` to `to`.
pub fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
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

/// Upload until every committed write is in the object store.
pub fn upload_all(s: &PageStore) {
    block_on(s.upload_all()).unwrap();
    assert_eq!(s.pending_upload_bytes(), 0);
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

/// A record of ~64 KiB that does not compress: a few hundred make several
/// files of an upload.
pub fn big(key: u64, version: u64) -> Vec<u8> {
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

pub fn big_id(key: u64) -> Id {
    id(1_000_000 + key)
}
