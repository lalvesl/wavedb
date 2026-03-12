//! A page read from its bytes, and the records read off it (RFC 0076 §4).
//!
//! The page is the micro workload's: 391 records of its `Thing`, each
//! behind a head of the size a stored record carries, zstd'd against a
//! dictionary sampled from them as the store samples it. `from_bytes` is
//! the whole read — length, crc, envelope, decompression, the body checked;
//! `get` and `iter` are what a reader does with the page next.
//!
//! Uses nothing newer than the page's own API, so the same file runs at an
//! older commit to compare against: `cargo bench -p wavedb-storage --bench
//! page`, the clock pinned.

use std::hint::black_box;

use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use wavedb_core::{Id, U48};
use wavedb_storage::{Dictionary, SlotPage};
use wavedb_wire::to_wire;

/// The records a micro page holds.
const RECORDS: u64 = 391;

/// Bytes ahead of a record's body: its `STRUCT_HASH`, the metadata length
/// and a metadata of the usual size.
const HEAD: usize = 8 + 4 + 48;

const STRUCT_HASH: u64 = 0x5448_494E_4700_0001;

const WORDS: [&str; 16] = [
    "record", "anchor", "segment", "chain", "instant", "tenant", "page",
    "journal", "commit", "barrier", "cache", "index", "pivot", "wire", "hash",
    "slot",
];

/// Record `n`'s stored bytes: a head, then a `Thing`'s wire.
fn record(n: u64) -> Vec<u8> {
    let mut x = n.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut body = String::with_capacity(224);
    for _ in 0..32 {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        body.push_str(WORDS[(x % 16) as usize]);
        body.push(' ');
    }
    // Tuples compose flattened, so nesting the scalars keeps the layout.
    let thing = (
        ((n % 64) as u32, x >> 1),
        format!("name-{n:010}"),
        format!("tag-{:04}", n % 1024),
        body,
    );
    let mut bytes = vec![0u8; HEAD];
    bytes[..8].copy_from_slice(&STRUCT_HASH.to_le_bytes());
    bytes[8..16].copy_from_slice(&n.to_le_bytes());
    bytes.extend_from_slice(&to_wire(&thing));
    bytes
}

fn id(n: u64) -> Id {
    Id::new(
        1_700_000_000_000_000_000 + n * 1_000,
        U48::from(1u32),
        false,
        0,
    )
}

/// The page, its dictionary, and its bytes, compressed or not.
fn page(compress: bool) -> (Dictionary, Vec<u8>) {
    let mut dict = Dictionary::new();
    let mut page = SlotPage::new(STRUCT_HASH);
    for n in 0..RECORDS {
        let bytes = record(n);
        dict.sample(&bytes);
        page.upsert(id(n), bytes);
    }
    let bytes = page.to_bytes(&dict, compress).expect("page encodes");
    (dict, bytes)
}

fn reads(c: &mut Criterion) {
    let mut group = c.benchmark_group("page");
    group.throughput(Throughput::Elements(RECORDS));
    for (name, compress) in [("zstd", true), ("raw", false)] {
        let (dict, bytes) = page(compress);
        group.bench_function(format!("{name}/from_bytes"), |b| {
            b.iter(|| SlotPage::from_bytes(black_box(&bytes), &dict));
        });
        let page = SlotPage::from_bytes(&bytes, &dict).expect("page decodes");
        group.bench_function(format!("{name}/get_all"), |b| {
            b.iter(|| {
                (0..RECORDS)
                    .filter_map(|n| page.get(black_box(id(n))))
                    .map(<[u8]>::len)
                    .sum::<usize>()
            });
        });
        group.bench_function(format!("{name}/iter"), |b| {
            b.iter(|| {
                black_box(&page).iter().map(|(_, r)| r.len()).sum::<usize>()
            });
        });
    }
    group.finish();
}

criterion_group!(benches, reads);
criterion_main!(benches);
