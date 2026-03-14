# RFC 0077 — XXH3 for the runtime hash

- **Status:** Planned (opened 2026-10-24)
- **Grows out of:** the `docs/improvements.md` entry "The runtime hash is
  SeaHash, ~7× a mixer on a 16-byte key" (2026-10-24, found measuring
  [RFC 0076](0076-zero-copy-views-WIP.md))
- **Builds on:** [RFC 0074](0074-extendible-hashing.md) (the page
  directories), [RFC 0068](0068-record-cache-as-a-log-WIP.md) (the record
  index), [RFC 0067](0067-thread-per-core-shards-WIP.md) (placement)
- **Crates:** a new vendored `crates/xxh3`; `wavedb-storage` (`directory`,
  `record_index`, `record_cache`, `split`); `wavedb-core` (`placement`)
- **Code (now):**
  - `crates/wavedb-storage/src/directory.rs` (`hash_of`, SeaHash seeded)
  - `crates/wavedb-storage/src/record_index.rs` (`RecordIndex::hash`)
  - `crates/wavedb-core/src/placement.rs` (`Owner::placement_hash`, SeaHash
    unseeded)

## Summary

The hashes WaveDB takes **per operation** — an `Id`'s bucket in the record
index and in its type's page directory, and an owner's shard — become
**XXH3-64**, the function Dragonfly hashes its keys with. Only its 9–16-byte
path is needed: every input is 10, 14 or 16 bytes. It costs ~4.8 ns on a
`u128` where the vendored SeaHash costs ~36 ns, and a read takes one to
three of these hashes, a write more.

The hashes that are **identities** stay SeaHash 4.1.0, bit for bit:
`STRUCT_HASH`, `fn_identity`, the chain lane hashes, `natural_key_hash`.
They are computed at build time or once, and they are what stored bytes
are named by.

The seed stays per database and random (the `MANIFEST`'s), folded once to
the `u64` XXH3 takes. Records land in other buckets and other shards,
which is a layout change: a database created before it is unsupported, as
every layout change is before release.

## Motivation

### The cost

Criterion, one thread, the clock pinned at 2.3 GHz, 1024 distinct `u128`
keys (a throwaway bench, 2026-10-24; step 1 keeps it as
`crates/xxh3/benches`). Throughput hashes independent keys; latency chains
each hash into the next key:

| a `u128` through | throughput | latency |
|---|---:|---:|
| SeaHash (vendored, seeded) | 35.9 ns | 35.5 ns |
| **XXH3-64, the 9–16-byte path, seeded** | **4.8 ns** | **7.6 ns** |
| wyhash (final, 16 bytes) | 4.6 ns | 6.4 ns |
| wyhash, protected multiply | 6.3 ns | 7.7 ns |
| murmur3 `fmix64` per half, chained | 5.6 ns | 6.8 ns |
| `fmix64` once over `lo ^ rot(hi)` | 4.4 ns | 6.7 ns |
| xorshift64\* over `lo ^ rot(hi)` | 4.0 ns | 5.6 ns |

SeaHash is a stream hash. On 16 bytes it still runs its four-lane state, a
diffusion per lane and a finalisation with the length — some six
multiplies where XXH3 makes two — and the vendored one, safe and
`const fn`, reads its input a byte at a time. How much of its 36 ns each
of those costs was not measured. Nothing in WaveDB needs a stream hash at
runtime: every runtime input has a fixed width.

### Where they are taken

- **The record index** (`RecordIndex::hash`): every `get` that consults
  the record cache, every put into it, and every key a segment split
  rehashes.
- **The page directory** (`Directory::hash`): every record-cache miss,
  every page a settle routes a record to, and every record a page split
  partitions (`split.rs`).
- **Placement** (`Owner::placement_hash`): every operation a node routes
  to its owner's shard.

The micro row's timed read is ~0.9 µs (`benches/results/index.md`); one to
three hashes at ~31 ns saved each is 3–10% of it, before a write's
several.

### Why not keep the heavy hash for safety

Rust's `HashMap` defaults to SipHash-1-3 under a random key because the
standard library cannot know where its keys come from, and hash flooding
(2011) turned colliding keys into O(n) inserts. A random seed alone did
not stop it: in 2012 Aumasson and Bernstein found seed-independent
collisions in MurmurHash2/3 and CityHash, and SipHash — a PRF, keyed — was
the answer.

**SeaHash is not a PRF either.** WaveDB's seeded SeaHash buys what a
seeded XXH3 buys: an attacker who does not know the 64-bit seed cannot
precompute buckets, and no proof stands behind either. What bounds a
flood is structural, and it stays:

- a page directory stops splitting at `D_MAX` (32): past it a page stays
  over target, and the directory does not grow without bound;
- the record index stops at `DIR_SLACK` doublings past its segment count
  and spills to an overflow list. That list is scanned linearly, so a
  flood of full 64-bit collisions would slow lookups on that store. It
  would today, under SeaHash, just the same.

And the lever is narrow. A NonUnique `Id` is minted by the node, an
instant and a counter; a client chooses bytes only through a
`#[wavedb::key]`, whose `KEY` is a SeaHash of its fields, so steering it
means grinding a 64-bit hash per insert, each insert a write the node
pays for and the tenant answers for.

## Design

### 1. `crates/xxh3`: the 9–16-byte path, vendored

A crate of its own, as `crates/seahash` is: safe, `const fn`, no
dependencies, edition 2024, crediting Yann Collet's xxHash (BSD-2-Clause)
in its README.

```rust
/// XXH3-64 of `bytes` under `seed` — the reference's
/// `XXH3_64bits_withSeed`, for 9 to 16 bytes, checked when compiled.
pub const fn hash<const N: usize>(bytes: &[u8; N], seed: u64) -> u64 {
    const { assert!(N >= 9 && N <= 16) };
    // bitflip1 = (secret[24..32] ^ secret[32..40]) + seed
    // bitflip2 = (secret[40..48] ^ secret[48..56]) - seed
    // lo = read64(bytes[0..8]) ^ bitflip1
    // hi = read64(bytes[N-8..N]) ^ bitflip2
    // acc = N + swap64(lo) + hi + fold64(lo × hi)
    // avalanche(acc): acc ^= acc >> 37; acc *= 0x165667919E3779F9; acc ^ acc >> 32
}
```

- **The length is a type parameter.** An input outside 9–16 bytes does not
  compile, so the function has no runtime branch and nothing to panic on.
  Every caller hashes a fixed-size array already.
- **Golden vectors from the reference C library** (`nixpkgs#xxHash`),
  seeds included, in `tests/golden.rs`, as SeaHash's are pinned to the
  published 4.1.0. The scratch implementation already matches `xxhsum -H3`
  at 10, 14 and 16 bytes.
- **Not the general XXH3.** Inputs past 16 bytes take other paths (and
  above 240 a seed-derived secret). None is needed, and the crate says so
  rather than carrying them untested.

### 2. The directory and the record index

`directory::hash_of(id: u128, seed: u64) -> u64` becomes
`xxh3::hash(&id.to_le_bytes(), seed)`. `Directory` and `RecordIndex` hold
the folded `u64` instead of the `[u64; 4]` (24 bytes less each).

The `MANIFEST` keeps its `[u64; 4]`. The store folds it once when it
opens (`seed[0] ^ seed[1] ^ seed[2] ^ seed[3]`, random words xored are a
random word), so the manifest's format does not move. The directory's
module docs lose their SeaHash paragraph and gain this one.

### 3. Placement

`Owner::placement_hash` becomes `xxh3::hash` over the same frozen bytes
(10 for a collection's `LocalId`, 14 for a Unique's tenant and
`STRUCT_HASH`), under a **fixed** seed (0), as it is unseeded today.

Placement is not a table a client can flood. Its inputs are node-minted
Pivot ids and a (tenant, type) pair, and the most a skew could do is load
one shard. Seeding it would put the database seed into every router
(`route.rs`, `router.rs`, `seed.rs`, `worker.rs`) for no protection worth
having.

### 4. What stays SeaHash

| hash | why it stays |
|---|---|
| `STRUCT_HASH` (`wavedb-macros`) | type identity, at build time, on disk in every record |
| `fn_identity` | a fn-type's identity, the same hash space |
| chain lane hashes (`index/segment.rs`) | a chain's identity in its segments' heads |
| `natural_key_hash` (`mint.rs`) | it **is** the anchor's `KEY`: an address the client computes, over variable-length field bytes |

`crates/seahash` stays pinned and golden-tested. Its README's "every shard
placement in WaveDB is a SeaHash value" becomes untrue and is corrected.

### 5. The layout moves

Every record lands in another directory bucket and another shard. A
database written before this does not open correctly. It is unsupported,
as before release every layout change is (no format versioning, `CLAUDE.md`).
That includes a developer's native client caches (`~/.cache/<app>`), which
are deleted. The wasm cache (IndexedDB, keyed by `Id`) holds no
directory and is unaffected.

### 6. Steps

1. `crates/xxh3` with its golden tests and the bench above, in the
   workspace.
2. The directory, the record index, the record cache and the split on it;
   the seed folded at open. The storage tests and a reopen test — a
   durability angle — pass unchanged, since nothing in them pins a bucket.
3. Placement. `shard_workers.rs` and `convert.rs` compute shards through
   the same function and follow.
4. Docs: `directory.rs`, `placement.rs`, `CLAUDE.md` ("A record lives in
   shard `seahash(owner bytes) % P`"), the SeaHash README, this RFC's
   progress.
5. Measure: the micro row through `scripts/bench.sh`, the clock pinned,
   with the deferred rerun that measures RFC 0076.

## Alternatives

- **wyhash.** As fast (4.6 ns), and it would do. Its multiply has a known
  blind spot: an operand xored to zero zeroes the product, and the
  constants it xors with are public. The protected variant closes that at
  6.3 ns. The spec also moved between versions (`final3`, `final4`), and
  XXH3's has been frozen since 0.8.0.
- **rapidhash**, wyhash's successor: newer, less deployed, the same shape.
- **A murmur3 `fmix64` or xorshift mixer.** The fastest folds (4.0–4.4 ns)
  join the halves by `lo ^ rot(hi)` first, a linear map, and an `Id`'s
  structure makes its collisions easy to build. `fmix64` per half (5.6 ns)
  avoids that but is an ad-hoc construction with no reference to pin
  against.
- **SipHash-1-3**, a PRF: the one choice that would *prove* flooding
  resistance. Rust's `DefaultHasher` is not usable as is — its algorithm
  is not promised stable across releases and stored layouts need that — so
  it would be vendored. Not measured here; it costs several times XXH3 on
  16 bytes, and the floods it would stop are already bounded by structure
  (Motivation).
- **SeaHash with a fixed-width path.** Same output, pinned by the golden
  tests, no layout change. It cuts the byte-at-a-time reads but not the
  algorithm's multiplies, so it stays well behind XXH3. Worth taking only
  if moving the layout were not free, and before release it is.

## Open questions

1. **Seed placement after all?** A skewed shard map loads one shard; it
   does not degrade a table. If tenants or Pivots ever become cheap for a
   client to mint in bulk, revisit.
2. **The record index's overflow list** is a linear scan. Bounded memory,
   unbounded time under a full-collision flood, the same as today. A
   second, independently seeded hash for the overflow would bound it too.
   Not needed while the seed is secret.
