# RFC 0065 — implementation progress

Working state for the benchmark restructure
([RFC 0065](../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md)). The RFC is
the *design*; this file is *where the work stands* and, more importantly, what
has been **learned along the way that is not obvious from the code**.

It exists because a plan that lives only in a chat log is a plan that dies with
it. Update it as steps land; delete it when the restructure is done and the
RFC's own status header can carry the answer.

---

## Where this stands

**Phases 1 and 2 complete.** Every row of both workloads on all five systems
runs through the harness end to end — real wall clock, pooled percentiles,
per-phase `bytes_written` attributed to the right process, and the footprint
points. Proven by running one row of each system and workload against live
servers and reading the stored JSON back.

**The RFC 0060 bridge is gone.** `src/row/bridge.rs` is deleted, and so are
the ten `run` functions that each owned their own loop, timing and phase
boundaries. What survives of `systems/` is the part that was never a
measurement: the preloads, the DDL and the open helpers.

Run it: `nix run .#bench -- --dry-run` (the plan), then without `--dry-run`.
Live-server tests need the pinned peers on `PATH` — see
`tests/server_drivers.rs`, which **skips and says so** rather than failing.

---

## Steps

Legend: ✅ done · 🔧 in progress · ⬜ not started · ⏸ waiting on a decision

### Phase 0 — preparation

| # | step | file | |
|---|---|---|---|
| 0.1 | Map generation vs execution across the 10 adapters | (survey) | ✅ |
| 0.2 | `WorkloadOp` — the descriptor the generator emits | `src/plan/op.rs` | ✅ |
| 0.3 | `RowRecord` — the JSON v2 row format, encode + decode | `src/corpus/{record,decode}.rs` | ✅ |

### Phase 1 — the row as the unit

| # | step | file | |
|---|---|---|---|
| 1.1 | `RowKey` | `src/corpus/record.rs` | ✅ |
| 1.2 | `RowKey::digest()` — SHA only for WaveDB | `src/corpus/record.rs` | ✅ |
| 1.3 | `Corpus::{load, store, has, rows_for_host}` | `src/corpus/store.rs` | ✅ |
| 1.4 | `Plan::resolve()` — measure vs reuse | `src/plan/resolve.rs` | ✅ |
| 1.5 | `bench-row` — one row, one JSON | `src/bin/bench-row.rs` | ✅ |
| 1.6 | `bench` — the supervisor | `src/bin/bench.rs` | ✅ |
| 1.7 | `cage::exec_row()` | `src/cage/exec.rs` | ✅ |
| 1.8 | `cage::verify()` runs in the child | `src/cage/verify.rs` | ✅ |
| 1.9 | `--refresh` / `--refresh-all` | `src/bin/bench.rs` | ✅ |
| 1.10 | Nix: `cage.nix` declares, `apps.nix` runs the supervisor | `nix/` | ✅ |
| 1.11 | Retire `wavedb-bench.rs`, `cli.rs`, `index.rs`, `tables.rs` | — | ✅ |
| 1.12 | The pre-0065 corpus: transcribe or keep as archaeology | `results/` | ⏸ |

Also restored in 1.11: the **noise and space guards**, which lived inside the
deleted runner (`src/guard.rs`). They now run in the supervisor, before any row
is spawned — a refusal there costs nothing, the same refusal at the end of a
pass costs the pass.

### Phase 2 — the producer/consumer harness

| # | step | file | |
|---|---|---|---|
| 2.1 | Generator (streaming, runs on the orchestrating thread) | `src/harness/run.rs` | ✅ |
| 2.2 | Partition / routing (`DriverFactory::route`) | `src/harness/driver.rs` | ✅ |
| 2.3 | Consumer threads + message protocol | `src/harness/consumer.rs` | ✅ |
| 2.4 | `Driver` / `DriverFactory` seam | `src/harness/driver.rs` | ✅ |
| 2.5a | `micro` workload generator | `src/harness/micro.rs` | ✅ |
| 2.5b | sqlite driver | `src/systems/drivers/sqlite.rs` | ✅ |
| 2.5c | wavedb driver (Direct + Sharded) | `src/systems/drivers/wavedb.rs` | ✅ |
| 2.5d | postgres / mysql / mongodb drivers + lifecycle | `src/systems/drivers/` | ✅ |
| 2.5e | Live-server integration tests | `tests/server_drivers.rs` | ✅ |
| 2.6 | `shop` workload generator + 5 shop drivers | `harness/shop.rs`, `drivers/shop/` | ✅ |
| 2.7 | `Shards::start(store, N)` for the sharded shop row | `drivers/shop/engine.rs` | ✅ |
| 2.8 | Debug assertion: a consumer only ever sees its own partition | `drivers/shop/wavedb.rs` | ✅ |
| 2.9 | Wall-clock throughput vs per-op percentiles | `src/harness/run.rs` | ✅ |
| 2.10 | `Phase` carries `wall_ns` + `consumers`; `ops_per_sec` removed | `src/corpus/record.rs` | ✅ |
| 2.11 | `consumers` in the identity digest | `src/corpus/record.rs` | ✅ |
| 2.12 | `--consumers N` (default 3) | `src/bin/bench.rs` | ✅ |
| 2.13 | **Wire `bench-row` to the harness** for the embedded bracket | `src/row/micro.rs` | ✅ |
| 2.14 | Footprints + server lifecycle inside the row | `src/row/{points,embedded,server}.rs` | ✅ |

### Phase 3 — engine knobs and dataset tiers

| # | step | |
|---|---|---|
| 3.1 | `StoreOptions.{page_cache_bytes, record_cache_bytes}` | ✅ |
| 3.2 | `BlockFile` takes the budget instead of `DEFAULT_BUDGET_BYTES` | ✅ |
| 3.3 | How `record_cache_bytes` applies (auto-evict vs caller default) | ✅ |
| 3.4 | Storage tests: budget honoured, `Default` unchanged, durability | ✅ |
| 3.5 | `params.nix`: four tiers, each with its own `rev` | ✅ |
| 3.6 | `gen.nix`: filtered source, so an unrelated edit stops invalidating seeds | ✅ |
| 3.7 | `dataset.nix` / `seeds.nix` parameterised by tier | ✅ |
| 3.8 | Fill profile (uncaged, big caches, max relax) | ✅ |
| 3.9 | Build the `large` tier and time the fill — **gates phase 5** | 🔧 |
| 3.10 | `.bench-seeds/` GC roots | ✅ |

3.1 and 3.2 landed as one step, deliberately: a `page_cache_bytes` the
`BlockFile` does not read is not a budget, it is a field that reports a
ceiling nobody enforces. A step boundary in the middle of that is a knob that
lies for the length of one iteration.

3.3 is answered the conservative way — `record_cache_bytes` is **carried, not
applied**. The store hands out the number (`record_cache_bytes()`,
`evict_to_budget()`) and evicts only when a caller asks, exactly as today.
Eviction takes the journal lock, so a `PageStore` that dropped entries on a
schedule of its own would be stalling writers inside a window someone else is
timing — the one thing this suite cannot have. Reversible in one commit if the
node ever wants a self-managing ceiling.

3.4 is seven tests, each one **falsified before being believed**: hardcoding
the record budget back to 96 MiB fails `the_store_never_evicts_records_until_asked`,
and ignoring `open_with`'s argument fails all three page-cache tests. A green
test over a knob is worth exactly what it costs to make it red.

3.5 makes the tier the single source of the row count. Both apps now pass
`--tier`, `--rows` and `--dataset-revision` from one attribute, derivations are
named by the tier's tag (`bench-seed-sqlite-small-200000-r1`), and a `rev` bump
is proven to change that name. Verified by building `bench-dataset`: the
manifest names the tier and revision and the TSV holds exactly 200 000 rows.

3.6 narrows `bench-gen`'s source to the Rust and manifests it compiles
from. Measured in both directions: an RFC, a README, `PROGRESS.md` and a
corpus row now leave the store path alone, while `benches/src/schema.rs`, a
crate `.rs`, either `Cargo.toml` and the workspace root still change it. The
`bench-seed-sqlite` seed builds from the filtered binary.

Its blast radius is smaller, not zero: `bench-gen` is one binary, so editing
the **fill** (`seed.rs`) still invalidates the **dataset** (the TSV, which
`emit_tsv` produces and the fill never touches) and therefore the four
bulk-loaded seeds built from it. Measured: appending a comment to `seed.rs`
moves `bench-dataset-smoke` and `bench-seed-postgres-smoke` to new paths.
Splitting `bench-gen` into two binaries with separate filesets would fix it;
until then, a fill change costs a dataset rebuild.

3.7 instantiates **every** tier, not just the selected one: the dataset and
the five seeds become `bench-dataset-<tier>` / `bench-seed-<system>-<tier>`,
and each tier gets its own app pair (`bench-large`, `bench-seeded-large`).
The unsuffixed names are the default tier and resolve to the identical store
path, so nothing that already worked changed. Instantiating four costs
nothing — evaluation is lazy, so `huge` is an expression until something asks
for it. Proven by building `bench-dataset-smoke` (1 000 rows) and
`bench-seed-wavedb-smoke` (`ids.bin` = 16 000 bytes = 1 000 anchors), and by
reading the generated `bench-seeded-smoke` script: it exports the smoke seed
paths and passes `--tier smoke --rows 1000`.

3.10 is `scripts/bench_seeds.sh`, which pins a tier's six outputs as GC
roots under a gitignored `.bench-seeds/<tier>/`. Verified end to end:
`nix-store --gc --print-roots` lists the link, `--list` reports it with its
size, and `--unpin` removes both. It **pins what is already built** and only
reports what is missing — building is opt-in (`--build`), because at `large`
that is hours and a script that starts them by accident is a script nobody
runs twice.

The tier is a **suffix, not a flag**, deliberately: it decides both what is
measured and what the row is filed as, so an app that took it as an argument
would be an app you can point at the wrong dataset — which is the bug 3.5
just fixed, reintroduced one level up.

3.8 gives the fill three declared numbers (`params.nix`'s `fill`): a 6 GiB
page cache, a 4 GiB record cache, and a 4 GiB journal threshold. It also
fixes a defect the profile exposed — `fill_wavedb` had **no maintenance at
all**, so both the journal and the record cache grew for the entire fill. See
the method finding below for why the threshold is 4 GiB and not the 64 MiB a
running node uses.

### Phase 4 — read counters

| # | step | |
|---|---|---|
| 4.1 | `read_bytes` + `rchar` from `/proc/self/io` | ⬜ |
| 4.2 | Same for the server process tree | ⬜ |
| 4.3 | `PhaseRecord` fields (already in the format, still written as 0) | 🔧 |
| 4.4 | `read amp` column and the `read_bytes / rchar` ratio | ⬜ |

### Phase 5 — the `huge` tier

Blocked on 3.9.

### Phase 6 — the three tables

| # | step | |
|---|---|---|
| 6.1 | Table A — variants at one commit | ⬜ |
| 6.2 | Table B — evolution across commits | ⬜ |
| 6.3 | Table C — comparison, peers from the corpus | ⬜ |
| 6.4 | `index.md` regenerated from the corpus | ⬜ |
| 6.5 | `benches/README.md` for the new architecture | ⬜ |
| 6.6 | RFC 0065 status → Partial / Implemented | ⬜ |

---

## Measurement findings

The reason this file matters more than the checklist above.

### These invalidate numbers already in the corpus

- **MySQL's update column is inflated by roughly 21%.** RFC 0060 drew update
  keys uniformly **with replacement** and built each new value as
  `thing_v2(n, seed)` — a pure function of the row and the seed. The second
  update of a row therefore wrote byte-identical values, and InnoDB turns an
  all-columns-unchanged `UPDATE` into a no-op. At 50 000 updates over 100 000
  rows that is ~21% of the phase MySQL skipped and its peers did. Found because
  the new driver *checks* that an update touched a row; the old adapter used
  `.expect()` and did not look. Fixed by `schema::thing_v2_at`, which salts by
  the draw ordinal. **Update columns from before and after this change are not
  comparable.**
- **`wavedb-sharded` in the corpus is not multi-threaded.** It is
  `Shards::start(store, 1)` — one thread plus a disk actor. The row measures
  *what the actor costs*, which is a real result under a misleading name. A
  genuinely sharded row needs a concurrent source of requests, which is what
  the phase-2 harness supplies, and it can only exist on `shop` (below).
- **No MongoDB, PostgreSQL or MySQL numbers exist in `results/` at all.** The
  one recorded run that attempted them skipped all six server rows with
  `spawn mongod: No such file or directory` — it was not launched through the
  Nix runtime. Any table quoting peer numbers came from a run that was never
  recorded.
- **`ops_per_sec` divided by the summed timed windows.** Correct at one
  consumer and wrong by roughly `N` under concurrency, because summing
  overlapping windows counts concurrency as duration. Split into wall-clock
  throughput and per-operation percentiles (RFC 0065 §4).

### Method findings

- **The sharded shop row is the first measurement of actual parallelism in
  this project, and it is ~2.3×.** Three consumers over one disk actor, 300
  users (a smoke shape, uncaged): `profile` 6 951/s against `single`'s 3 021,
  `order_page` 2 047 against 818, `order_detail` 619 against 286. `checkout`
  is 443 against 323 — writes serialise at the actor, reads do not.
  **Read it with the same caveat as the micro row**: the sharded consumers
  each hold a `ShardStore` read cache the direct row has no equivalent of, so
  part of that gap is RFC 0044's missing read cache rather than concurrency.
  Separating the two needs a direct row with a read cache, which does not
  exist to be measured.

- **A collection is indivisible, so `micro` cannot be sharded.** Its B+tree
  nodes and chain segments carry ids of their own and belong to the *Pivot's*
  owner, so two consumers writing disjoint records would still contend on
  shared structure — silent index loss, not a cache miss
  (`wavedb-quick-node/src/shard/route.rs`). `wavedb/multi` is therefore a
  `shop` row only, enforced by `MicroOp::partition_key()` returning a constant
  and by `plan::resolve::consumers_for`.
- **The read phases were seeded by their own name's length** (`seed ^ 8`,
  `seed ^ 9`), so renaming a phase silently changed the data it read. Now named
  constants (`harness::micro`).
- **Reads and updates were not verified.** `.expect("select")` accepted a query
  that found nothing as success — a fast operation and a wrong one. Every
  driver now fails the row instead.
- **Nothing measures reads.** `bytes_written` is recorded and no read counter
  is, which is why a `mongod` observed reading ~1 TB against a 27.5 MB dataset
  could not be classified from the corpus. Phase 4.
- **Generation ran serially with the operation it fed**, on the same CPU, while
  the servers' WAL writers, page cleaners and eviction threads run
  *concurrently with* a timed window. That asymmetry is the whole motivation
  for the producer/consumer split.
- **A maintenance threshold must be in bytes, not operations.** RFC 0060's
  adapter used `MAINTAIN_EVERY = 5000` operations, which never fired while
  649 MB of journal accumulated: a count of operations says nothing about how
  much log they produced.
- **WaveDB's direct row has no read cache, and the measured gap is 540×.**
  A release smoke run (20 000 rows, uncaged, so a shape rather than a corpus
  number) put `wavedb/single` at 752 480 read_hot/s and **1 393 read_cold/s**;
  `wavedb/multi` over the same phases sat at 105 951 and 98 556. The direct
  row's per-type cache is a *write* cache that `evict(0)` empties and reads
  never repopulate, so every cold read walks the tree from pages; the sharded
  row's `ShardStore` memoises, so it has a read cache the direct row does not.
  RFC 0044 is that gap, and the pair is why `read_cold` is not comparable
  across the two variants — the row's notes already say so, and this is the
  ratio behind the sentence.
- **WaveDB's `compacted` footprint is LARGER than its `settled` one.** On a
  5 000-row release row the store settled at 1 933 312 payload bytes and came
  out of `defragment` at 2 060 288 — **127 KB bigger**, log column unchanged
  at 8 192 both times, so it is `data.bin` that grew. The sequence is the one
  RFC 0060 used (settle → defragment → settle), so this is not new; it had
  simply never been read as a pair before. A compaction that grows the file is
  worth its own measurement before anyone attributes it to a mechanism.
- **`allocated_bytes` alone misreads the `compacted` point.** PostgreSQL's
  `VACUUM FULL` shrank the heap from 25.2 MB to 23.9 MB *and* wrote a second
  16 MB WAL segment, so the total went 42.0 → 57.4 MB. Anyone reading the
  total would conclude the rewrite made it bigger. Read `payload_bytes`
  (= allocated − log) across that point; the `is_log` split exists for exactly
  this.
- **The preallocated-log column is most of some peers.** MongoDB's baseline is
  209.8 MB of which **209.76 MB is WiredTiger journal** — the dataset at that
  point is ~102 KB. MySQL's baseline is 210 MB with 155 MB of log. Counting
  those as stored data would say the peers are enormous and WaveDB is tiny,
  which is a statement about default preallocation rather than about storage.
- **Each system empties its cache differently**, and the phase boundary is the
  only place that difference is allowed to live: postgres, mysql and mongodb
  **restart the process**, sqlite reopens the connection, wavedb calls
  `evict(0)`. Five mechanisms, one `Driver::between_phases`, so `read_cold`
  means the same thing on all five.

- **Checkpointing often makes a fill worse, and the fill had no checkpoint at
  all.** `seed::fill_wavedb` inserted every row and settled once at the end,
  so the journal grew for the whole fill — **4.8 GB at 200 000 rows**, which
  extrapolates to ~120 GB at `large` and ~1.2 TB at `huge`, with the record
  cache growing beside it. Adding a checkpoint fixed that and made everything
  else worse; the interesting part is by how much. All three at 200 000 rows,
  same machine, uncaged:

  | fill | wall | peak RSS | store |
  |---|--:|--:|--:|
  | no maintenance (the old code) | 2:16.9 | 351 MB | 20 MB |
  | checkpoint every 64 MiB (~75 rounds) | 4:25.8 | 561 MB | 303 MB |
  | checkpoint every 4 GiB (1 round) | 2:22.4 | 269 MB | 46 MB |

  Page writes are copy-on-write (RFC 0041), so every intermediate settle
  rewrites pages that a later settle rewrites again: 75 rounds nearly doubled
  the wall clock and left a store **15× larger**. The end-of-fill settle
  writes each page once, and that is what a bulk fill actually wants.

  So the threshold is a **safety valve, not a schedule** — sized so it fires
  rarely and only exists to stop the journal and the cache running away
  before the final settle. At 4 GiB the fill is within 4% of the unbounded
  one's time, uses *less* RAM than it (the one checkpoint evicts), and pays
  26 MB of store bloat that is mostly freed-but-unreturned blocks a
  `defragment` reclaims.

  The node's own 64 MiB (`quick-node`'s `checkpoint_after_bytes`, which the
  shop preload also uses) is right for a *running* node and wrong here by
  three orders of magnitude. The two are now separate constants with the
  reason written at both.

- **The fill needs more RAM than the cage allows, which is why it is
  uncaged.** Peak RSS 561 MB against a 500 MB cage, at the smallest of the
  four tiers. Not a new decision — RFC 0065 §6 already says a fill is a build
  — but this is the number that makes it not a preference.

- **The safety valve works at scale, and the `large` fill is a multi-hour
  build.** A first `nix build .#bench-seed-wavedb-large` ran **2h47m without
  finishing** the 5 million inserts before its session was torn down. Two
  things are already established from it, and neither needs the run to
  complete:

  - **Memory is bounded, by the mechanism 3.8 added.** Peak RSS **3.28 GB**
    against the 10 GiB declared ceiling, and the checkpoint-plus-evict fired
    **ten times**, each dropping the process from 2–3 GB back to 0.3–0.5 GB
    (at 1992 s, 3193 s, 3254 s, 3676 s, 4521 s, 5183 s, 5670 s, 5912 s,
    6393 s, 8136 s). Without it the fill has no ceiling at all — that is what
    `fill_wavedb` did before this phase.
  - **The insert rate degrades with the tree.** 200 000 rows measured
    1 408 rows/s; 5 000 000 rows did **not** complete in 10 004 s, so the
    average there is under 500 rows/s — a fall of at least 2.8×. Extrapolating
    the *small* number to `huge` was therefore optimistic by nearly 3×: at
    under 500 rows/s, 50 million rows is **28 hours or more**.

  That last number is the answer RFC 0065 open question 1 was waiting for: at
  this rate `huge` is not a build you run, it is a build you import. Phase 5
  should assume the build-outside-and-import path rather than a derivation.

### Declared concessions



- **`--work-dir` cannot be deeper than ~57 characters.** PostgreSQL caps a
  Unix-domain socket path at 107 bytes, and the row appends
  `<digest>/postgres-<durability>/.s.PGSQL.5432` (50 characters) to it. The
  default (`/tmp/wavedb-bench-row`) is far inside that; a deep scratch
  directory is not, and the failure is a `FATAL` in the server log rather than
  anything the row says.

- **SQLite pays a statement-cache lookup the SQL servers do not.** A
  `rusqlite::CachedStatement` borrows its `Connection`, so a driver cannot hold
  prepared statements the way `postgres::Statement` and `mysql::Statement`
  (owned handles) allow. It is tens of nanoseconds: unmeasurable against a
  ~4 ms durable insert, about **1%** against a ~10 µs hot read. Identical
  across every SQLite row, so it cannot bias `durable` against `relaxed`; a
  real, small handicap against the in-process WaveDB rows. Equalising it by
  removing the servers' hoisted statements would mean making a peer slower on
  purpose.
- **Phase-1 rows record `wall_ns` as the sum of the timed windows.** Identical
  to what RFC 0060 recorded, and every such row carries `row::PHASE1_NOTE`
  saying so in its own `notes`.

### Bugs the restructure found in the suite it replaced

Not introduced here — they were live in the RFC 0060 suite and only became
visible because phase 3 started building the datasets instead of assuming
them.

- **Recording a measurement invalidated the datasets the next measurement
  needs.** `bench-gen` was built with `src = repoSrc` — the whole checkout —
  and it is a build input of *every* seed. `benches/results/` is tracked, so
  storing a row changed the flake source and rebuilt all five seeds; so did
  editing an RFC. Measured, not inferred: touching one corpus JSON moved
  `bench-seed-mongodb` from `d477s7…` to `2c3lcn…`. At `small` that is
  minutes of rebuild for a file the fill never reads; at `large` it is the
  afternoon, which is what makes it a phase-3 blocker rather than a wart.

- **The two apps disagreed about what `small` means.** `nix run .#bench`
  passed no `--rows` at all, so every unseeded run measured `bench-row`'s own
  default while `nix run .#bench-seeded` measured `params.nix`'s 200 000 —
  and both filed under `tier: small`. Under RFC 0065 the tier **name** is the
  identity and the row count is not, so the two are indistinguishable in the
  corpus: same digest, different dataset. It is not hypothetical, it is what
  `results/` holds — three rows at 100 000 and one at 10, none at the declared
  200 000. Fixed by making the tier the only source of the count and having
  every app pass name, count and revision together.

### Bugs introduced during this restructure, and fixed

Kept because they are the shape of mistake this design invites.

- **`bench-gen` had not built since phase 2, and nothing said so.**
  `drivers/shop/mod.rs` declared `pub mod mysql;` without the
  `#[cfg(feature = "servers")]` its four siblings carry, so the crate compiled
  under default features and failed under `--no-default-features` — which is
  exactly how `bench-gen` is built, and `bench-gen` is a build input of
  **every seed**. `cargo clippy --all-targets` never sees it because the
  default feature set turns `servers` on. One missing attribute; the whole
  seed tree unbuildable. `tests/server_drivers.rs` needed the same gate at
  file level. `cargo clippy --no-default-features --all-targets` is now part
  of the bar below.
- **A failed row leaked its server, and the leak corrupted the next run.**
  `stop` only runs on the success path, so a row that failed mid-way unwound
  past it and left `mongod` holding the data directory. The scratch is named
  by the row's **digest**, so the next attempt at that row cleared a directory
  another process was still writing to: the second `mongod` died on
  `failed to read 4096 bytes at offset 77824` in `WiredTiger.wt`. `Server`
  now has a `Drop` that kills an unstopped child — a kill rather than the
  system's own shutdown, because that path only has to guarantee the process
  is gone.
- **`direct()` was not a direct connection.** The shop MongoDB row starts a
  `mongod` with `--replSet` and initiates it, and the client used to do that
  had no `directConnection=true`. An uninitiated replica-set member is a
  server an ordinary client will not select, so the readiness ping never got
  an answer and the row died on a 300-second timeout beside a perfectly
  healthy server. The connection that *initiates* a set cannot require the
  set to exist.
- **A swallowed error became a timeout with no cause.** `replSetInitiate`'s
  result was discarded as "idempotent on a restart", so the failure above
  reported `not ready after 300s` and nothing else. It is now kept and folded
  into the wait's error; only `already initialized` is ignored.
- **`compact` refuses on a replica set primary.** It needs `force: true`,
  which is right here: the objection is that it slows down other running
  operations, and there are none — the row is between its phases and its
  footprint.
- **A row could file a consumer count it did not use.** `wavedb/single` on
  shop was asked for three consumers, ran on one, and stored `consumers: 3`
  in its identity beside `consumers: "1"` in its settings. The count is a
  row-identity field, so the row now refuses rather than measuring.
- **The lane key named a machine no number ran on.** The supervisor runs
  uncaged, so `Host::probe` fingerprinted 8 CPUs and 15 835 MB while every row
  it scheduled measured at 4 CPUs and 500 MB. Fixed with
  `Host::probe_with_budget`.
- **A remeasured row measured a rewrite.** Rows shared one work directory, so
  `--refresh` re-ran `insert` against a populated database. SQLite shouted
  (`table thing already exists`); a WaveDB store would have reopened silently
  and reported rewrites as inserts. Each row now gets a scratch of its own,
  named by its digest, cleared before and removed on success.
- **A built driver was moved across a thread boundary** — the exact thing the
  factory seam exists to prevent. The compiler refused it; `thread::scope` and
  building inside the consumer thread is the fix.
- **`systemd-run` rejects `-p=NAME=VALUE`.** Ten unit tests asserting the argv
  all passed; the first real execution did not. Unit tests over an argv check
  *shape*, never *acceptance*.
- **The MongoDB driver never created the `tag` index.** Every SQL peer
  declares `idx_thing_tag` in its DDL and SQLite's driver does too; the
  MongoDB `Factory` written in step 2.5d had no `create` flag at all, so the
  reference peer was the only row not paying for a secondary index on every
  write. Nothing would have failed — it is a flattering measurement, not a
  broken one, which is why it survived a live-server test. Fixed with a
  `create` flag matching the other four.
- **A green test that proved nothing.** The first live-server run passed
  through the skip path, and `nix develop` does not have `initdb` either — the
  peers live in the bench app's `runtimeInputs`, not the dev shell. The skip
  now prints.

---

## Open decisions

1. **1.12 — the pre-0065 corpus.** Four WaveDB rows in
   `results/…-4c-500m-…-e636/`, measured at `N = 1`, without read counters, and
   before `cage_revision` existed. Transcribe into the row format, or keep as
   archaeology? Recommendation: **keep**, since the lane name changes with the
   cage revision anyway.
2. ~~**3.3 — how `record_cache_bytes` applies.**~~ **Taken**: the conservative
   one (carried, not applied — see phase 3's note). Recorded here because it
   was a decision, not a derivation, and reversing it is one commit.
3. **The off-standard `8c-15835m` lane** — uncaged, `--force`, irreproducible,
   and holding no peer numbers. Keep or drop?
4. **Whether one sharded workload is enough** (RFC 0065 open question 5).

---

## Conventions this work follows

- No commits. The tree is dirty by instruction.
- Every file under 350 non-test lines; `scripts/check_file_length.sh` does not
  cover `benches/`, so this is a choice rather than a gate.
- `cargo fmt --all` + `cargo clippy --all-targets` at zero warnings + the full
  unit suite green, every step.
- **And `cargo clippy --no-default-features --all-targets`**, because that is
  the feature set `bench-gen` — and therefore every seed derivation — is built
  with. The default set hides a missing `#[cfg(feature = "servers")]`
  completely.
- No `dyn`, no serde — the workspace's rules apply here too.
