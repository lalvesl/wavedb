//! A header in its round's last block, after the page there (RFC 0081 §1):
//! a durable round of small pages writes one block, the pack and the header
//! naming it. The block is named twice over — by the pack's sections and by
//! the header while a reconstruction may read it — and goes with the last
//! of those names, whichever it is, before and after a reopen.

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::path::Path;

use futures::executor::block_on;
use wavedb_core::{Id, Store, U48, Write};
use wavedb_storage::{PageStore, StructStorage};

const A: u64 = 0x0081_0001_5EA1_0001;
const B: u64 = 0x0081_0001_5EA1_0002;
const C: u64 = 0x0081_0001_5EA1_0003;
// Uncompressed: what a page takes is what it holds.
static PLAIN_A: StructStorage = StructStorage::without_compression(A);
static PLAIN_B: StructStorage = StructStorage::without_compression(B);
static PLAIN_C: StructStorage = StructStorage::without_compression(C);

fn open(dir: &Path) -> PageStore {
    PageStore::open(dir, &[&PLAIN_A, &PLAIN_B, &PLAIN_C]).unwrap()
}

fn id(hash: u64, k: u64) -> Id {
    Id::new(k, U48::from(1u32), false, (hash & 0x7FFF) as u16)
}

fn record(hash: u64, k: u64, version: u8) -> Vec<u8> {
    let mut bytes = hash.to_le_bytes().to_vec();
    bytes.extend_from_slice(&k.to_le_bytes());
    bytes.resize(16 + 200, version);
    bytes
}

/// A durable round of record `k` of `A` and `B`, at `version`: one block.
fn round(s: &PageStore, k: u64, version: u8) {
    let puts = [A, B].map(|h| Write::Put(id(h, k), record(h, k, version)));
    one_block(s, &puts);
}

fn one_block(s: &PageStore, writes: &[Write]) {
    let before = s.space().file_blocks;
    block_on(s.apply_durable(writes)).unwrap();
    assert_eq!(s.space().file_blocks - before, 1, "the page and its header");
}

/// A durable round of `C` alone, which leaves `A`'s and `B`'s pages be.
fn other(s: &PageStore, version: u8) {
    one_block(s, &[Write::Put(id(C, 1), record(C, 1, version))]);
}

fn assert_record(s: &PageStore, k: u64, version: u8) {
    for h in [A, B] {
        let got = block_on(s.get_of(h, id(h, k))).unwrap();
        assert_eq!(got, Some(record(h, k, version)), "{h:x}/{k}");
    }
}

/// Blocks no page or header names: free, or freed once a batch is durable.
fn unused(s: &PageStore) -> u64 {
    let space = s.space();
    space.free_blocks + space.deferred_blocks
}

/// The pack's sections go first: the header, still read by a
/// reconstruction, keeps the block — counted again by the reopen in
/// between — until the batch after it ends.
#[test]
fn a_shared_block_outlives_its_pack_while_its_header_is_live() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    round(&open(&db), 1, 1);
    let s = open(&db);
    round(&s, 1, 2);
    assert_eq!(unused(&s), 0, "the first header still names its block");
    round(&s, 1, 3);
    assert_eq!(unused(&s), 1, "the first block, once");
    assert_record(&s, 1, 3);
    drop(s);
    let s = open(&db);
    assert_record(&s, 1, 3);
    assert_eq!(unused(&s), 1);
}

/// The header goes first: the pack, still named by its sections, keeps the
/// block — through a reopen, which no longer counts the header — until its
/// records are written again.
#[test]
fn a_shared_block_outlives_its_header_while_its_pack_is_named() {
    let d = tempfile::tempdir().unwrap();
    let db = d.path().join("db");
    {
        let s = open(&db);
        round(&s, 1, 1);
        for v in 1..=4 {
            other(&s, v);
        }
        // `C`'s first blocks went with their headers, and the pack stays.
        assert_eq!(unused(&s), 2, "the first two of C's blocks");
    }
    let s = open(&db);
    assert_record(&s, 1, 1);
    assert_eq!(unused(&s), 2);
    round(&s, 1, 2);
    other(&s, 5);
    other(&s, 6);
    assert_eq!(unused(&s), 2 + 3, "the pack's block, once, and C's");
    s.evict_settled(0);
    assert_record(&s, 1, 2);
    drop(s);
    let s = open(&db);
    assert_record(&s, 1, 2);
    let c = block_on(s.get_of(C, id(C, 1))).unwrap();
    assert_eq!(c, Some(record(C, 1, 6)));
}
