//! Ordered lanes in the page store ([RFC 0078] §7): entries set and unset
//! by key, read back by span — the pages and the record cache merged, in
//! either order — the same before a settle, after it, and after a reopen.
//!
//! Every read is checked against a sorted map holding what was written,
//! read by the same span (`read_span`), so a scan here means what a scan of
//! any in-memory store means.
//!
//! [RFC 0078]: ../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

// The engine is used from one thread; its futures are not `Send` (lib.rs).
#![allow(clippy::future_not_send)]

use std::collections::BTreeMap;
use std::path::Path;

use futures::executor::block_on;
use wavedb_core::store::{Span, read_count, read_span};
use wavedb_core::{Store, Write};
use wavedb_storage::{PageStore, StructStorage};

const LANE: u64 = 0x0078_0004_1A4E_0001;
static SLOT: StructStorage = StructStorage::new(LANE);

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn open(dir: &Path) -> PageStore {
    PageStore::open(dir, &[&SLOT]).expect("open")
}

/// A collection's key: its 16-byte prefix, then a big-endian position.
fn key(collection: u8, at: u64) -> Vec<u8> {
    let mut key = vec![collection; 16];
    key.extend_from_slice(&at.to_be_bytes());
    key
}

fn value(at: u64, version: u8) -> Vec<u8> {
    let mut bytes = at.to_le_bytes().to_vec();
    bytes.resize(200, version);
    bytes
}

/// Apply `writes` to the store and the model alike.
fn write(store: &PageStore, model: &mut Model, writes: &[Write]) {
    for w in writes {
        match w {
            Write::Set(_, k, v) => {
                model.insert(k.clone(), v.clone());
            }
            Write::Unset(_, k) => {
                model.remove(k);
            }
            _ => {}
        }
    }
    block_on(store.apply(writes)).expect("apply");
}

/// Every span worth checking over `collection`'s keys: whole, bounded,
/// skipped, limited, both ways.
fn spans(collection: u8) -> Vec<Span> {
    let whole = Span::prefix(&[collection; 16], usize::MAX);
    let mut out = vec![whole.clone(), whole.clone().reversed()];
    for (skip, limit) in [(0, 1), (0, 7), (5, 3), (40, 25), (1_000, 10)] {
        let span = Span {
            skip,
            limit,
            ..whole.clone()
        };
        out.push(span.clone().reversed());
        out.push(span);
    }
    let bounded = Span {
        lo: key(collection, 300),
        hi: Some(key(collection, 700)),
        rev: false,
        skip: 0,
        limit: usize::MAX,
    };
    out.push(bounded.clone().reversed());
    out.push(bounded);
    out
}

/// Every span over both collections reads what the model holds, and every
/// count counts it.
fn agrees(store: &PageStore, model: &Model) {
    for collection in [1, 2] {
        for span in spans(collection) {
            let got = block_on(store.scan(LANE, &span)).expect("scan");
            assert_eq!(got, read_span(model, &span), "{span:?}");
            let counted =
                block_on(store.count(LANE, &span.lo, span.hi.as_deref()))
                    .expect("count");
            assert_eq!(
                counted,
                read_count(model, &span.lo, span.hi.as_deref()),
                "{span:?}"
            );
        }
    }
}

/// Two collections share the lane, each read alone, from the cache before
/// a settle, from the pages after, from both once more is written, and
/// from the pages alone after a reopen.
#[test]
fn a_lane_reads_the_same_from_cache_pages_and_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let mut model = Model::new();
    {
        let store = open(dir.path());
        // Interleaved, so the two collections' keys share no page order
        // the writes would have given them.
        let mut writes = Vec::new();
        for at in 0..1_000u64 {
            writes.push(Write::Set(LANE, key(1, at * 2), value(at, 1)));
            writes.push(Write::Set(LANE, key(2, at), value(at, 2)));
        }
        write(&store, &mut model, &writes);
        agrees(&store, &model);

        block_on(store.apply_durable(&[])).expect("settle");
        agrees(&store, &model);

        // Over the pages: removals, rewrites and keys between pages.
        let mut writes = Vec::new();
        for at in (0..1_000u64).step_by(3) {
            writes.push(Write::Unset(LANE, key(1, at * 2)));
            writes.push(Write::Set(LANE, key(1, at * 2 + 1), value(at, 3)));
            writes.push(Write::Set(LANE, key(2, at), value(at, 4)));
        }
        write(&store, &mut model, &writes);
        agrees(&store, &model);
        block_on(store.apply_durable(&[])).expect("settle");
    }
    let store = open(dir.path());
    agrees(&store, &model);
}

/// Unsetting a key never written, or one already unset, changes nothing;
/// an emptied collection reads empty from either end.
#[test]
fn an_emptied_collection_reads_empty() {
    let dir = tempfile::tempdir().unwrap();
    let store = open(dir.path());
    let mut model = Model::new();
    let sets = (0..300).map(|at| Write::Set(LANE, key(1, at), value(at, 1)));
    write(&store, &mut model, &sets.collect::<Vec<_>>());
    block_on(store.apply_durable(&[])).expect("settle");
    let unsets = (0..300).map(|at| Write::Unset(LANE, key(1, at)));
    write(&store, &mut model, &unsets.collect::<Vec<_>>());
    write(&store, &mut model, &[Write::Unset(LANE, key(1, 9_999))]);
    agrees(&store, &model);
    block_on(store.apply_durable(&[])).expect("settle");
    agrees(&store, &model);
    assert!(model.is_empty());
}
