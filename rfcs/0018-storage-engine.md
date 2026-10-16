# RFC 0018 — The storage engine

- **Status:** Implemented. **Amended by
  [0067](0067-thread-per-core-shards-WIP.md) step 1 (2026-10-08):** the
  `StructStorage` statics became declarations and the per-type state moved
  into the open store, so the one-store-per-process rule below is now one
  store per **data directory** (`data.bin` is locked while open).
- **Crates:** `wavedb-storage`
- **Code:** `crates/wavedb-storage/src/` (`data.bin`, page directory, `SlotPage`)

## Summary

`wavedb-storage` is the native node engine behind `Store`
([RFC 0008](0008-store-trait-and-atomic-batch.md)): a single `data.bin` of
4 KiB blocks (superblock in block 0), **per-`STRUCT_HASH` linear-hashed page
directories**, `SlotPage`s carrying checked-wire envelopes, and **per-type zstd
dictionaries**. Per-type state is compile-time (`StructStorage` statics), which
forces **one open `PageStore` per process**.

## Motivation

The layout must serve an application read in *few IOs* by keeping a type's (and
a tenant's) bytes together ([RFC 0001](0001-vision-and-non-goals.md)), and it
must compress well (the CPU saved from joins is spent here). Per-`STRUCT_HASH`
directories give locality by type; per-type dictionaries make zstd effective on
small records that would otherwise be incompressible alone.

## Design

- **`data.bin`, 4 KiB blocks.** Block 0 is the superblock (`FORMAT_VERSION`
  pinned at 1, [RFC 0002 §8](0002-architectural-hard-rules.md); the per-DB random
  hash seed for runtime id/page routing).
- **Per-`STRUCT_HASH` page directories, linear-hashed.** Each type's records
  live in their own growable directory of pages; linear hashing grows it a bucket
  at a time without a full rehash.
- **`SlotPage`.** A page of slots, each a checked-wire envelope
  ([RFC 0003](0003-wavewire-wire-format.md) `validation`); decode verifies the
  `STRUCT_HASH` head ([RFC 0010](0010-metadata-and-record-envelopes.md)).
- **Per-type zstd dictionaries**, versioned by prefix length — records of one
  type compress against a dictionary trained on that type. Compression is
  per-type opt-out via `#[wavedb(compress = false)]` (a storage config, never
  folded into `STRUCT_HASH`); hot B+tree node pages disable it by default.
- **Block descriptor** = `start u40 · count u20 · occupation u4` — one 64-bit
  format for pages **and** dictionaries ([RFC 0005](0005-composite-ids-and-bit-budgets.md)).
- **Reads are page-backed** (the cache is a *cache*): `get_of` serves the
  per-type cache and falls through to the page directory on a miss.

## The one-store-per-process rule

Because per-type state is **compile-time statics** (`StructStorage`), a process
can hold only one open `PageStore` — a second open is `EngineBusy`. Consequences
that ripple through the whole test suite:

- storage tests serialise via an `engine_gate()` mutex;
- node integration tests use a single `#[tokio::test]`;
- a `Db::open` client and a node can't share one engine, so a
  cache-and-node test runs the node as a **child process**
  ([RFC 0024](0024-client-db-and-cache.md)).

## Recovery

Durability and open-time recovery are their own idea —
[RFC 0019](0019-journal-rooted-recovery.md) (journal-first WAL, journal-rooted
commit).

## Deferred / dropped

- **Per-value (string/blob) heap compression** — page-level zstd exists;
  per-value is future work, measure first.
- **A dedicated one-node-per-page B+tree format** — dropped
  ([RFC 0031](0031-node-per-page-bptree-DEPRECATED.md)).
- **A cold/history tier (slow-node)** — removed
  ([RFC 0033](0033-cold-history-slow-node-tier-DEPRECATED.md)); history is a
  single tier in `data.bin`, unbounded growth accepted.
