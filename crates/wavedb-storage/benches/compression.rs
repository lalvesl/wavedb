//! How a page compresses (RFC 0081 §9, phase 0): with the type's
//! dictionary, with none, and raw, over pages of 4, 16, 32 and 128 KiB of
//! the micro workload's records — the ratio, the encode and the decode.
//!
//! The dictionary is sampled from other records of the same shape, as the
//! store samples one from what it settled before; a page's own records
//! would flatter it. The ratios print once, before the timings: stored
//! bytes over the records' bytes.
//!
//! `cargo bench -p wavedb-storage --bench compression`, the clock pinned.

use std::hint::black_box;
use std::time::Duration;

use criterion::{
    BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use wavedb_core::{Id, U48};
use wavedb_storage::{Dictionary, SlotPage};
use wavedb_wire::to_wire;

/// Bytes ahead of a record's body: its `STRUCT_HASH`, the metadata length
/// and a metadata of the usual size.
const HEAD: usize = 8 + 4 + 48;

const STRUCT_HASH: u64 = 0x5448_494E_4700_0001;

const WORDS: [&str; 16] = [
    "record", "anchor", "segment", "chain", "instant", "tenant", "page",
    "journal", "commit", "barrier", "cache", "index", "pivot", "wire", "hash",
    "slot",
];

/// Page sizes, in KiB of records.
const SIZES: [usize; 4] = [4, 16, 32, 128];

/// Record `n`'s stored bytes: a head, then a `Thing`'s wire (as
/// `benches/page.rs`).
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

fn key(n: u64) -> [u8; 16] {
    Id::new(
        1_700_000_000_000_000_000 + n * 1_000,
        U48::from(1u32),
        false,
        0,
    )
    .raw()
    .to_be_bytes()
}

/// A dictionary sampled from records the pages do not hold.
fn trained() -> Dictionary {
    let mut dict = Dictionary::new();
    for n in 1_000_000..1_004_000 {
        dict.sample(&record(n));
    }
    dict
}

/// A page of about `kib` KiB of records, and their bytes.
fn page_of(kib: usize) -> (SlotPage, usize) {
    let mut page = SlotPage::new(STRUCT_HASH);
    let (mut n, mut bytes) = (0, 0);
    while bytes < kib * 1024 {
        let r = record(n);
        bytes += r.len() + 16;
        page.upsert(&key(n), r);
        n += 1;
    }
    (page, bytes)
}

/// The three ways a page is stored: its dictionary (`None` raw) and
/// whether it compresses.
fn modes(dict: &Dictionary) -> [(&'static str, Dictionary, bool); 3] {
    [
        ("dict", dict.clone(), true),
        ("no-dict", Dictionary::new(), true),
        ("raw", Dictionary::new(), false),
    ]
}

fn compression(c: &mut Criterion) {
    let dict = trained();
    eprintln!("stored/records — the dictionary {} bytes", dict.len());
    for kib in SIZES {
        let (mut page, bytes) = page_of(kib);
        let line: Vec<String> = modes(&dict)
            .iter()
            .map(|(name, d, compress)| {
                let stored = page.to_bytes(d, *compress).expect("encodes");
                // Stored over the records' bytes, to three places.
                let milli = stored.len() * 1000 / bytes;
                format!("{name} {}.{:03}", milli / 1000, milli % 1000)
            })
            .collect();
        eprintln!("{kib:>4} KiB: {}", line.join("  "));
    }
    encode_group(c, &dict);
    let mut decode = c.benchmark_group("decode");
    decode.measurement_time(Duration::from_secs(3));
    for kib in SIZES {
        let (mut page, bytes) = page_of(kib);
        decode.throughput(Throughput::Bytes(bytes as u64));
        for (name, d, compress) in modes(&dict) {
            let stored = page.to_bytes(&d, compress).expect("encodes");
            decode.bench_function(BenchmarkId::new(name, kib), |b| {
                b.iter(|| SlotPage::from_bytes(black_box(&stored), &d));
            });
        }
    }
    decode.finish();
}

/// The encodes, a group of their own: the group holds the criterion it
/// borrows only as long as it measures.
fn encode_group(c: &mut Criterion, dict: &Dictionary) {
    let mut encode = c.benchmark_group("encode");
    encode.measurement_time(Duration::from_secs(3));
    for kib in SIZES {
        let (page, bytes) = page_of(kib);
        encode.throughput(Throughput::Bytes(bytes as u64));
        for (name, d, compress) in modes(dict) {
            encode.bench_function(BenchmarkId::new(name, kib), |b| {
                b.iter(|| {
                    // A copy with its records as changes, as a round seals.
                    let mut fresh = SlotPage::new(STRUCT_HASH);
                    for (k, v) in page.iter() {
                        fresh.upsert(k, v.to_vec());
                    }
                    fresh.to_bytes(black_box(&d), compress)
                });
            });
        }
    }
    encode.finish();
}

criterion_group!(benches, compression);
criterion_main!(benches);
