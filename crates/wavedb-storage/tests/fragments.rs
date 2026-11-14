//! Appends past a page are a fragment, not the page written again
//! (RFC 0081 §2): what a durable append costs, and that every record reads
//! back, before and after a reopen.

use std::path::Path;

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{BLOCK_SIZE, PageStore, StructStorage};

const SH: u64 = 0x0081_0005_F4A6_0001;
// Uncompressed: what a page takes is what it holds, so the blocks counted
// are the records'.
static SLOT: StructStorage = StructStorage::without_compression(SH);

/// Ids by creation: every new one past the last.
fn id(k: u64) -> Id {
    Id::new(1_000 + k, U48::from(1u32), false, 1)
}

/// A record of 1 KiB nothing compresses.
fn noise(k: u64) -> Vec<u8> {
    let mut bytes = SH.to_le_bytes().to_vec();
    let mut x = k ^ 0x9E37_79B9_7F4A_7C15;
    for _ in 0..128 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    bytes
}

/// Blocks every file under `dir` takes.
fn blocks(dir: &Path) -> u64 {
    let mut total = 0;
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let meta = entry.metadata().unwrap();
        total += if meta.is_dir() {
            blocks(&entry.path())
        } else {
            meta.len() / BLOCK_SIZE as u64
        };
    }
    total
}

fn put(store: &PageStore, k: u64) {
    block_on(store.apply_durable(&[Write::Put(id(k), noise(k))])).unwrap();
}

fn read(store: &PageStore, k: u64) -> Option<Vec<u8>> {
    block_on(store.get(id(k))).unwrap()
}

/// A tail page of ~40 records takes ten blocks; each durable append past it
/// wrote it again whole. Now it writes a fragment — a block or two, then a
/// new fragment once one passes a block — with its header and roots block.
#[test]
fn a_durable_append_does_not_write_the_page_before_it_again() {
    const FILLED: u64 = 40;
    const APPENDS: u64 = 60;
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("db");
    {
        let store = PageStore::open(&dir, &[&SLOT]).unwrap();
        for k in 0..FILLED {
            block_on(store.apply(&[Write::Put(id(k), noise(k))])).unwrap();
        }
        store.drain().unwrap();
        let before = blocks(&dir);
        for k in FILLED..FILLED + APPENDS {
            put(&store, k);
        }
        let written = blocks(&dir) - before;
        assert!(
            written < 5 * APPENDS,
            "{written} blocks for {APPENDS} durable appends"
        );
        for k in 0..FILLED + APPENDS {
            assert_eq!(read(&store, k), Some(noise(k)), "{k}");
        }
    }
    let store = PageStore::open(&dir, &[&SLOT]).unwrap();
    for k in 0..FILLED + APPENDS {
        assert_eq!(read(&store, k), Some(noise(k)), "{k} after a reopen");
    }
}

/// A round that changes a page as well as appending past it rewrites it,
/// the appends with it; a later round's appends are a fragment again —
/// and an update of a record a fragment holds finds it there.
#[test]
fn an_update_inside_the_page_takes_the_appends_with_it() {
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("db");
    {
        let store = PageStore::open(&dir, &[&SLOT]).unwrap();
        for k in 0..40 {
            block_on(store.apply(&[Write::Put(id(k), noise(k))])).unwrap();
        }
        store.drain().unwrap();
        for k in 40..50 {
            put(&store, k);
        }
        // An update of a record in the page and one in a fragment, and an
        // append, in one batch.
        let batch = [
            Write::Put(id(3), noise(1_003)),
            Write::Put(id(45), noise(1_045)),
            Write::Put(id(50), noise(50)),
        ];
        block_on(store.apply_durable(&batch)).unwrap();
        block_on(store.apply_durable(&[Write::Remove(SH, id(41))])).unwrap();
    }
    let store = PageStore::open(&dir, &[&SLOT]).unwrap();
    for k in 0..=50 {
        let want = match k {
            3 | 45 => Some(noise(1_000 + k)),
            41 => None,
            _ => Some(noise(k)),
        };
        assert_eq!(read(&store, k), want, "{k}");
    }
}

/// Durable updates in the middle of a ten-block page write the span they
/// change, not the page; the page, named either side of each span, reads
/// back through reopens — which count its names again — and a compaction.
#[test]
fn a_durable_update_in_a_page_writes_its_span() {
    const FILLED: u64 = 40;
    let d = tempfile::tempdir().unwrap();
    let dir = d.path().join("db");
    let version = |k: u64, v: u64| noise(k + 1_000 * v);
    {
        let store = PageStore::open(&dir, &[&SLOT]).unwrap();
        for k in 0..FILLED {
            block_on(store.apply(&[Write::Put(id(k), noise(k))])).unwrap();
        }
        store.drain().unwrap();
        let before = blocks(&dir);
        for k in [5, 17, 29] {
            block_on(store.apply_durable(&[Write::Put(id(k), version(k, 1))]))
                .unwrap();
        }
        let written = blocks(&dir) - before;
        assert!(written < 5 * 3, "{written} blocks for 3 durable updates");
    }
    {
        let store = PageStore::open(&dir, &[&SLOT]).unwrap();
        for k in [4, 6, 18, 30, 5] {
            block_on(store.apply_durable(&[Write::Put(id(k), version(k, 2))]))
                .unwrap();
        }
        block_on(store.apply_durable(&[Write::Remove(SH, id(16))])).unwrap();
    }
    let want = |k: u64| match k {
        4 | 5 | 6 | 18 | 30 => Some(version(k, 2)),
        17 | 29 => Some(version(k, 1)),
        16 => None,
        _ => Some(noise(k)),
    };
    let store = PageStore::open(&dir, &[&SLOT]).unwrap();
    for k in 0..FILLED {
        assert_eq!(read(&store, k), want(k), "{k}");
    }
    store.compact().unwrap();
    for k in 0..FILLED {
        assert_eq!(read(&store, k), want(k), "{k} compacted");
    }
    drop(store);
    let store = PageStore::open(&dir, &[&SLOT]).unwrap();
    for k in 0..FILLED {
        assert_eq!(read(&store, k), want(k), "{k} compacted, reopened");
    }
}
