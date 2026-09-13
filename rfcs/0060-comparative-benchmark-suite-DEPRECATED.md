# RFC 0060 — Comparative benchmark suite (WaveDB vs MongoDB, PostgreSQL, MySQL, SQLite)

- **Status:** Deprecated 2026-09-04 — superseded by
  [RFC 0065](0065-benchmark-suite-ii-the-row-as-the-unit.md). Much of what it
  built **still runs and still stands**; it is the *shape* that was replaced,
  not the methodology. See [What survives](#what-survives) below.
- **Superseded by:** [RFC 0065](0065-benchmark-suite-ii-the-row-as-the-unit.md)
- **Was implemented in:** `benches/` (excluded from the workspace),
  `benches/nix/`, `benches/results/`
- **Opened:** 2026-08-12 · phases 1, 1b, 2, 3 landed 2026-08-13/14 ·
  the e-commerce workload 2026-08-14 · the cage 2026-08-22/24

## What it proposed

The repository had no measurements at all — `crates/wavedb-net/benches/` was
empty and `criterion` was unused — so every performance claim in the corpus was
an argument about code rather than a number. RFC 0060 proposed a reproducible
**insert / read-by-key / update** comparison of WaveDB against **MongoDB**
(the reference peer, being the closest model: a document *is* a record, `_id`
*is* an anchor, both encode client-side, and the oplog is structurally the
recency chain), PostgreSQL, MySQL and SQLite — with durability as a row
dimension rather than a setting, two never-merged transport brackets
(in-process vs over a socket), storage footprint as a first-class decomposed
metric, and every run committed to `benches/results/` under a host fingerprint
so a regression is bisectable.

It ran through `nix run .#bench`, so `flake.lock` pinned the competitors'
versions alongside the toolchain, and the **filled datasets were themselves
derivations** — a fill being a pure function of (system, version, size, seed).

A second, e-commerce workload (`shop`) landed beside the three-operation
`micro` one, reporting latency of composed operations with one tenant per user.

## Why it was superseded

Not by being wrong. By making **the run** its unit — one process, every system
in sequence, one JSON per run — which turned out to bind in three places at
once:

1. **Peers were remeasured on every pass.** A ~50-minute run spent almost all
   of it producing, by construction, the numbers PostgreSQL, MySQL and MongoDB
   produced last time: none of their inputs is the WaveDB commit. An
   hour-long benchmark is run before releases; a four-minute one is run before
   commits.
2. **There was no multi-threaded WaveDB row, and could not be.**
   `benches/src/systems/engine.rs:184` reads `Shards::start(store, 1)`, and its
   comment is right about why — a harness driving `Store` directly has no
   requests to route, so passing `N` would spawn threads with no work and
   describe a configuration that is not running. The missing piece was never a
   parameter; it was a *source of requests*.
3. **The harness put the systems on unequal CPU terms.** Data generation sat
   outside the stopwatch but ran *serially with* the operation it fed, while
   the servers' WAL writers, page cleaners and eviction threads ran
   *concurrently with* the timed window on CPUs the embedded pair never used.

RFC 0065 makes the **row** the unit — one
`(system, variant, durability, workload, tier)` measurement, with its own
identity, its own cage, its own process and its own stored file — and all three
resolve as consequences: peers are reused because the WaveDB SHA is not in
their identity; a generator thread feeding N = 3 consumers is finally a source
of requests; and everything competes inside one budget.

## What survives

Carried into RFC 0065 §9 unchanged, and still the reason the suite is
trustworthy:

- **The cage, and why it is three tools.** `systemd-run` is the only one that
  can bound memory, and it bounds the **page cache** too, which is what makes a
  cold read cold. `taskset` supplies the CPU budget because `cpuset` is not
  among the controllers delegated to a user scope. `bwrap` caps *nothing* — it
  is there for a private PID namespace and one uniform filesystem shape.
  `--dev-bind / /` is deliberate: the databases must write to a real disk.
- **Pinned server caches, 256 MB each.** Every server sizes its cache from the
  *machine's* RAM rather than the cgroup's, so an unpinned MongoDB under the
  cage asks for gigabytes it cannot have and is OOM-killed while the other two
  quietly take different fractions of a machine none of them can see. 256 MB is
  not tuning: it is MongoDB's floor, so the least-adjustable server sets the
  number for all three. Equal budgets are what make a row a comparison.
- **The cage is part of the lane identity**, and an uncaged run refuses to
  record — such a row is not a worse measurement but a measurement of *another
  machine*.
- **Footprint at four points** — baseline / hot / settled / compacted — with
  **payload split from log capacity**, because a server's preallocated log is a
  configuration default and the same size at 20 rows as at 200 000; and with
  compression stated per system.
- **Seeds as Nix derivations** rather than a `~/.cache` directory: version
  binding is free and correct (a `flake.lock` bump invalidates the seed, where
  a cache would hand a PG 18 datadir to PG 19), the output is cached by inputs
  rather than content (a datadir holds timestamps and a random system id, so it
  can never be fixed-output), the server is shut down **cleanly** in the builder
  so the first measured operation does not pay recovery, and each run
  materialises a writable copy by reflink with the cost reported outside the
  window.
- **`mongod` cannot start in the Nix build sandbox** — its bundled tcmalloc
  `CHECK`-fails reading `/sys/devices/system/cpu/possible` before parsing a
  single argument — so the Mongo seed fills inside a nested `bubblewrap` with a
  synthetic `/sys` carrying a **fixed** CPU mask. And `mongodb` in nixpkgs is
  unfree and therefore not in `cache.nixos.org`: `mongodb-ce` is the prebuilt
  tarball, and the difference is hours of C++ per version bump.
- **Why MongoDB is the reference peer**, and why FerretDB is not (it speaks the
  Mongo protocol over PostgreSQL, so benchmarking it measures PostgreSQL).
- **Whole-record updates on every system** (`$set` patches and field-level
  updates would flatter the others for free), the **typed** WaveDB read path,
  and the retention annotation beside every update column.
- **No headline number, no CI gate, no fairness certificate**, and the
  predictions of §9 recorded before the first run.

## Two findings worth keeping

**The seed footprints at 200 000 rows** (apparent size, as loaded, before any
adapter-driven compaction) — the measurement that forced payload and log
capacity apart:

| Seed | Total | Payload (data + indexes) | Log capacity | Build |
|---|---|---|---|---|
| wavedb | 23 MB | 20.9 MB `data.bin` (+3.2 MB id sidecar) | 58 B (two retired journals) | ~11 min |
| sqlite | 54 MB | 54 MB (post-`VACUUM`) | 0 (checkpointed + truncated) | seconds |
| postgres | 167 MB | 87 MB `base` | 80 MB `pg_wal` | seconds |
| mongodb | 227 MB | 22 MB collection + 4.2 MB indexes | 200 MB WT journal | seconds |
| mysql | 309 MB | 72 MB `bench` (+26 MB `mysql.ibd`) | 100 MB redo + 50 MB binlog | seconds |

WaveDB's payload is the **smallest of the five while retaining history**; the
build column is the group-commit gap seen from the build side.

**The suite's characteristic failure mode**, recorded because it is the one to
watch for: write bytes were read from the server's own `/proc/<pid>/io`, which
is right for MySQL and MongoDB and **wrong for PostgreSQL**, which is
process-per-connection — the postmaster writes essentially nothing. It reported
a confident `0.0 kB/insert`. A wrong number, not a missing one. The counter now
sums the process tree (`benches/src/systems/server.rs`).

## Open questions inherited by RFC 0065

0060's open question 4 — *how is the "exceeds RAM" size first built, and is a
multi-hour derivation acceptable?* — was never answered and is now load-bearing
for 0065's `huge` tier (0065 open question 1). Its question 5 (concurrency
sweep now or later) is answered by 0065 §3: the sweep is not a later phase but
the harness's default shape. Questions 1–3, 6–9 (seed size limits, a shared
binary cache, overlayfs vs reflink, the client-cache bracket, variance
thresholds, history-share separability, and the single-tenant limitation) carry
forward unchanged and are not restated in 0065.

The full original text is in this file's git history.
