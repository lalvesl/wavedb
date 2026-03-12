//! Decode against view (RFC 0076): what a read pays to own a value, and what
//! it pays to borrow one.
//!
//! Every view here is checked as a decode is — UTF-8, lengths, every element
//! of a list walked — so the two columns do the same validation and differ
//! only in the copies. Where a view defers work (a list of numbers is a
//! division, not a walk), a second column reads every element back, so the
//! cheaper one is not cheaper by reading less.
//!
//! Run with `cargo bench -p wavedb-wire --bench views`, the clock pinned.

use std::hint::black_box;

use criterion::{
    BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
};
use wavedb_wire::{WaveWire, Wire, from_wire, to_wire, view_wire};

/// The micro workload's record (`benches/gen/src/schema.rs`): two scalars,
/// two short strings, ~200 bytes of text.
#[derive(WaveWire, Debug, Clone, PartialEq, Eq)]
struct Thing {
    kind: u32,
    score: u64,
    name: String,
    tag: String,
    body: String,
}

/// A record with every kind of heap a view borrows: a string, bytes, and a
/// list of strings.
#[derive(WaveWire, Debug, Clone, PartialEq, Eq)]
struct Doc {
    id: u64,
    title: String,
    blob: Vec<u8>,
    tags: Vec<String>,
}

fn thing(n: u64) -> Thing {
    const WORDS: [&str; 8] = [
        "record", "anchor", "segment", "chain", "instant", "tenant", "page",
        "wire",
    ];
    let body = (0..32)
        .map(|i| WORDS[((n + i) % 8) as usize])
        .collect::<Vec<_>>()
        .join(" ");
    Thing {
        kind: (n % 64) as u32,
        score: n.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 1,
        name: format!("name-{n:010}"),
        tag: format!("tag-{:04}", n % 1024),
        body,
    }
}

fn doc() -> Doc {
    Doc {
        id: 7,
        title: "a document of some length".into(),
        blob: (0..=u8::MAX).cycle().take(1024).collect(),
        tags: (0..16).map(|i| format!("tag-{i:04}")).collect(),
    }
}

fn records(c: &mut Criterion) {
    let mut group = c.benchmark_group("record");
    let thing = to_wire(&thing(42));
    group.throughput(Throughput::Bytes(thing.len() as u64));
    group.bench_function("thing/decode", |b| {
        b.iter(|| from_wire::<Thing>(black_box(&thing)));
    });
    group.bench_function("thing/view", |b| {
        b.iter(|| view_wire::<Thing>(black_box(&thing)).map(black_box));
    });
    let doc = to_wire(&doc());
    group.throughput(Throughput::Bytes(doc.len() as u64));
    group.bench_function("doc/decode", |b| {
        b.iter(|| from_wire::<Doc>(black_box(&doc)));
    });
    group.bench_function("doc/view", |b| {
        b.iter(|| view_wire::<Doc>(black_box(&doc)).map(black_box));
    });
    group.finish();
}

fn blobs(c: &mut Criterion) {
    let mut group = c.benchmark_group("bytes");
    for size in [4 * 1024, 64 * 1024] {
        let wire = to_wire(&vec![0xA5u8; size]);
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(
            BenchmarkId::new("decode", size),
            &wire,
            |b, wire| b.iter(|| from_wire::<Vec<u8>>(black_box(wire))),
        );
        group.bench_with_input(
            BenchmarkId::new("view", size),
            &wire,
            |b, wire| {
                b.iter(|| view_wire::<Vec<u8>>(black_box(wire)).map(black_box));
            },
        );
    }
    group.finish();
}

fn lists(c: &mut Criterion) {
    let mut group = c.benchmark_group("list");
    let numbers = to_wire(&(0..1000u32).collect::<Vec<_>>());
    group.throughput(Throughput::Elements(1000));
    group.bench_function("u32x1000/decode", |b| {
        b.iter(|| from_wire::<Vec<u32>>(black_box(&numbers)));
    });
    group.bench_function("u32x1000/view", |b| {
        b.iter(|| view_wire::<Vec<u32>>(black_box(&numbers)).map(black_box));
    });
    group.bench_function("u32x1000/view+sum", |b| {
        b.iter(|| {
            view_wire::<Vec<u32>>(black_box(&numbers))
                .map(|l| l.iter().fold(0u32, u32::wrapping_add))
        });
    });
    group.bench_function("u32x1000/view+get", |b| {
        b.iter(|| {
            view_wire::<Vec<u32>>(black_box(&numbers)).map(|l| {
                (0..l.len())
                    .filter_map(|i| l.get(i))
                    .fold(0u32, u32::wrapping_add)
            })
        });
    });
    let strings =
        to_wire(&(0..100).map(|i| format!("tag-{i:04}")).collect::<Vec<_>>());
    group.throughput(Throughput::Elements(100));
    group.bench_function("stringx100/decode", |b| {
        b.iter(|| from_wire::<Vec<String>>(black_box(&strings)));
    });
    group.bench_function("stringx100/view", |b| {
        b.iter(|| view_wire::<Vec<String>>(black_box(&strings)).map(black_box));
    });
    group.bench_function("stringx100/view+len", |b| {
        b.iter(|| {
            view_wire::<Vec<String>>(black_box(&strings))
                .map(|l| l.iter().map(str::len).sum::<usize>())
        });
    });
    group.finish();
}

/// What a reply of records costs to receive: a `Vec<Thing>` decoded, or the
/// same bytes as a `Vec<Wire<Thing>>` — each record checked and kept as a
/// buffer of its own — and then each one viewed.
fn replies(c: &mut Criterion) {
    let mut group = c.benchmark_group("reply");
    let reply = to_wire(&(0..100).map(thing).collect::<Vec<_>>());
    group.throughput(Throughput::Elements(100));
    group.bench_function("thingx100/decode", |b| {
        b.iter(|| from_wire::<Vec<Thing>>(black_box(&reply)));
    });
    group.bench_function("thingx100/wires", |b| {
        b.iter(|| from_wire::<Vec<Wire<Thing>>>(black_box(&reply)));
    });
    group.bench_function("thingx100/wires+view", |b| {
        b.iter(|| {
            from_wire::<Vec<Wire<Thing>>>(black_box(&reply))
                .map(|wires| wires.iter().filter_map(|w| w.view().ok()).count())
        });
    });
    let wire = Wire::encode(&thing(42));
    group.throughput(Throughput::Elements(1));
    group.bench_function("thing/wire.decode", |b| {
        b.iter(|| black_box(&wire).decode());
    });
    group.bench_function("thing/wire.view", |b| {
        b.iter(|| black_box(&wire).view().map(black_box));
    });
    group.finish();
}

criterion_group!(benches, records, blobs, lists, replies);
criterion_main!(benches);
