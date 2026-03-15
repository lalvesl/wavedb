# RFC 0077 — XXH3 for the runtime hash

- **Status:** Deprecated (opened 2026-10-24, dropped 2026-10-24) — the
  runtime hash stays SeaHash
- **Grew out of:** the `docs/improvements.md` entry "The runtime hash is
  SeaHash, ~7× a mixer on a 16-byte key" (2026-10-24, found measuring
  [RFC 0076](0076-zero-copy-views-WIP.md)), removed with this RFC
- **Crates (as proposed):** a vendored `crates/xxh3`; `wavedb-storage`
  (`directory::hash_of`, the record index); `wavedb-core` (`placement`)

## What it proposed

Hash the fixed-width keys WaveDB hashes on every operation — an `Id`'s
16 bytes into the record index and the page directories, an owner's 10 or
14 bytes into its shard — with XXH3-64's 9–16-byte path, the function
Dragonfly hashes its keys with, instead of SeaHash. The identity hashes
(`STRUCT_HASH`, `fn_identity`, the lane hashes, `natural_key_hash`) were to
stay SeaHash. It was built in full, with the reference C library's output
pinned by golden tests, and then not landed.

## Why it lost

- **The cost it answered was a bench artifact.** The ~36 ns SeaHash cost
  on a `u128` came from a scratch bench built without LTO, where the call
  into the `seahash` crate is neither inlined nor specialised to the key's
  length. WaveDB builds release and bench with `lto = "fat"`, and there
  SeaHash costs ~7 ns on 16 bytes (XXH3 ~3.9 ns) and ~16.5 ns on 10 bytes
  (XXH3 ~3.8 ns). One to three hashes per ~0.9 µs read: under 2%, not the
  3–10% this RFC first claimed.
- **Trust.** SeaHash is the project's hash already, vendored, golden-tested
  against the published 4.1.0, with provable statistical guarantees
  (avalanche, bit independence). A second vendored hash for a gain this
  small was not worth carrying.

Neither hash is a PRF, so neither proves resistance to chosen-key
flooding. What bounds a flood is the per-database secret seed and the
structures' own limits — `D_MAX` in the page directories, `DIR_SLACK` and
the overflow list in the record index — which stay as they are.

## If it comes back

A faster runtime hash would be measured inside the workspace, under its
release profile (`cargo bench` in a member crate), not in a crate outside
it. The numbers above are where that comparison stands.
