# RFC 0060 — Comparative benchmark suite (WaveDB vs MongoDB, PostgreSQL, MySQL, SQLite)

- **Status:** Partial — opened 2026-08-12; **phase 1 implemented 2026-08-13**
  (embedded bracket, footprints, results corpus, `nix run .#bench`),
  **phases 1b, 2 and 3 implemented 2026-08-14** (all five seed derivations, and
  the server bracket: MongoDB, PostgreSQL and MySQL adapters, each starting its
  own server, both durability rows — nine rows in all), and the **e-commerce
  workload of §3.1 implemented 2026-08-14** (a second nine-row table, reported
  as latency of composed operations, with one tenant per user). Phases 4–5 are
  still design only. See *Implementation status* below.
- **Crates:** the `benches/` package, **excluded** from the workspace
  (`Cargo.toml`); `flake.nix` (the runner and the seed derivations); no change to
  any shipped crate
- **Code:** `benches/src/{cli,schema,shop,metrics,footprint,host,json,report,seed}.rs`,
  `benches/src/systems/{wavedb,sqlite,mongodb,postgres,mysql,server}.rs`,
  `benches/src/systems/shop/` (the e-commerce workload, one adapter per system
  plus its phases),
  `benches/src/bin/{wavedb-bench,bench-gen}.rs`, `benches/results/`,
  `flake.nix` (`apps.bench`, `apps.bench-seeded`, `packages.bench-seed-*`),
  `Cargo.toml` (`workspace.exclude`). Still target-only: the history
  comparison (§8) and the concurrency sweep.
- **Related:** [RFC 0041](0041-single-barrier-checkpoint.md) and
  [RFC 0047](0047-generational-journal-retirement.md) (the barrier accounting
  this measures), [RFC 0009](0009-anchors-succession-and-history.md) (the
  history an update writes), [RFC 0022](0022-live-sync-navigation-catchup.md)
  (the recency structure MongoDB's oplog is the natural peer of),
  [RFC 0054](0054-no-duplication-by-default.md) /
  [RFC 0051](0051-ordered-record-lists.md) (how many copies a write touches)

## Summary

A reproducible three-operation comparison — **insert**, **read by key**,
**update** — between WaveDB and the four systems a developer would otherwise
reach for: **MongoDB**, PostgreSQL, MySQL, SQLite. The suite runs under **Nix**,
so the competitor versions are pinned by `flake.lock` alongside the toolchain;
the **filled datasets are themselves derivations**, so the expensive part — the
fill — happens once per (system, size, version) and is served from the Nix store
on every later run; and **every run is committed** to `benches/results/`, making
the corpus a performance history a regression can be bisected against.

The rest of the design is what makes the numbers mean anything. Two axes are
load-bearing and are not settings but **row dimensions**: durability and
transport. A third — that a WaveDB `save` retains every superseded version and
the other four retain none — is out of scope for now by decision: this pass
compares raw operation cost with **no history on the SQL/document side**, and
records that asymmetry as a stated caveat rather than pretending it away. The
history comparison (both a `+history` control and the version-walk read) is
designed here and **deferred to phase 4**.

## Motivation

There is no benchmark in this repository. `crates/wavedb-net/benches/` is an
empty directory and `criterion` sits in the workspace dependency table
(`Cargo.toml:67`) unused. Every performance claim the design record makes — one
IOp per mutation, one barrier per checkpoint
([0041](0041-single-barrier-checkpoint.md)), a rendered page in one segment read
([0052](0052-segment-size-as-the-pagination-unit.md)) — is an argument about the
code, not a measurement. Those arguments are probably right, and that is exactly
the problem: an unmeasured argument stays right forever.

The question worth answering is not "is WaveDB fast?" It is:

> For the operations WaveDB actually offers, on a workload it was designed for,
> where does it sit relative to the systems it would replace — and where does it
> lose?

A benchmark that cannot produce the second half is marketing. This one is built
so that losing is a normal, recordable outcome, with predictions written down
**before** the first run (§9) so results cannot be rationalised afterwards.

### Why MongoDB is the reference peer

MongoDB, not PostgreSQL, is the closest thing to WaveDB's model, and it should
be read as the primary comparison:

- **A document is a record.** One self-describing value, written and read whole —
  no decomposition into rows and columns, no join to reassemble it. WaveDB's
  `[STRUCT_HASH][meta][body]` and a BSON document answer the same question.
- **`_id` is an anchor.** The primary access path is a point lookup by an id the
  system mints, and both encode on the *client* — BSON in the driver, WaveWire in
  the generated code. In both, the schema lives in the application.
- **The oplog is a recency chain.** An ordered log of what changed, which is what
  change streams read — structurally what WaveDB's sync navigates
  ([0022](0022-live-sync-navigation-catchup.md)). That comparison belongs to a
  later phase, but it is why the two systems feel alike.

Where they diverge is worth stating in the same breath: Mongo retains no version
history (the oplog is a window, not a record's chain), has no compile-time type
identity, and will accept concurrent field updates that WaveDB refuses with
`Error::Conflict`.

### Why the obvious benchmark is wrong

The obvious harness inserts a million rows into each system, times it, and
prints five numbers. It would be wrong three times over:

1. **Durability.** WaveDB's `apply` `fsync`s before it returns
   (`crates/wavedb-storage/src/apply.rs:50`, `journal.rs:174`) and every
   collection op is exactly one batch — so **one write op is one barrier**, with
   no group commit and no way to relax it. All four competitors ship a knob that
   trades that barrier away — `synchronous_commit=off` (PostgreSQL),
   `innodb_flush_log_at_trx_commit=2` (MySQL), `writeConcern j:false` (MongoDB),
   `PRAGMA synchronous=NORMAL` (SQLite) — and those settings are common in
   published benchmarks. On an NVMe a barrier costs tens to hundreds of
   microseconds against a few for the unbarriered write, so a table mixing the two
   ranks by *who is durable*, not by who is fast. Comparing a durable engine to
   four relaxed ones is not a measurement, it is a category error.
2. **Transport.** SQLite runs in the caller's process. The other three run behind
   a socket. WaveDB can do **either** — the engine directly, or over HTTP through
   `wavedb-quick-node`. Put all five in one table and the loopback socket is a
   large fraction of what is being timed.
3. **Retained work.** A WaveDB update writes the new version, archives the old
   one at its derived slot, re-keys the recency chain, and rewrites the record in
   every declared `#[wavedb::list]`. The others do not retain anything: MVCC
   garbage is vacuumed, InnoDB undo is purged, SQLite overwrites in place, and
   WiredTiger's old versions age out. **This pass accepts that asymmetry** — see
   §2 — but it must be printed next to the update row every time, or the number
   invites exactly the wrong conclusion.

## Design

### 1. What is comparable at all

WaveDB has no SQL, no query planner, no joins, and no ad-hoc predicates — a
filtered read is a `#[server]` function, and filtering *is* application code. The
honest intersection of all five systems is exactly the three operations
requested:

| Operation | WaveDB | MongoDB | SQL systems |
|---|---|---|---|
| **Create** | `insert` (NonUnique) / `save` (Unique) | `insertOne` | `INSERT` |
| **Read** | `T::get` / `Collection::get` by anchor | `findOne({_id})` | `SELECT … WHERE pk = ?` |
| **Update** | `save` at a known anchor | `replaceOne({_id})` | `UPDATE … WHERE pk = ?` |

Update uses `replaceOne`, not `$set`: WaveDB writes a whole record, and a
partial-field update is a different operation with a different cost. Comparing
whole-value writes to field patches would flatter Mongo for no reason.

One more row is included because it is the operation WaveDB is *shaped for*, and
excluding it would understate the engine as badly as including joins would
overstate it:

- **Recency listing** — `all()` (recency-ordered by construction, one segment
  read) versus `find().sort({updated_at: -1}).limit(n)` / `ORDER BY … LIMIT n`,
  which need an index the others maintain on every update.

Deferred to phase 4, designed in §8: the **version-history walk**
(`record_history` versus an audit table / a versioned-document collection) and
the **`+history` control** that would make the update row an apples-to-apples
comparison. Everything else is out of scope and says so in the results file. The
suite must never grow a "join emulation" row; the answer to a join in WaveDB is
that you did not model it that way.

### 2. The two matching axes (and the one recorded caveat)

**Durability** is a row dimension, not a setting. Every operation is measured
twice:

| Row | WaveDB | MongoDB | PostgreSQL | MySQL | SQLite |
|---|---|---|---|---|---|
| **durable** | (only mode) | `w:1, j:true` | `synchronous_commit=on` | `innodb_flush_log_at_trx_commit=1` | `journal_mode=WAL`, `synchronous=FULL` |
| **relaxed** | (only mode — *still barriered*) | `w:1, j:false` | `synchronous_commit=off` | `…=2` | `synchronous=NORMAL` |

The relaxed row's asymmetry is deliberate and is itself a result: **WaveDB has
no relaxed mode to offer.** `Journal::append_deferred` exists (`journal.rs:186`)
but is reserved for the checkpoint's `Commit` frame
([RFC 0046](0046-directory-deltas-in-the-window.md)); nothing exposes "batch my
writes and sync later" to a caller. If the relaxed row shows a large gap, that
is the measured price of the missing group commit — a design input, recorded
rather than hidden.

**Transport** is a bracket. Two tables, never merged:

- **Embedded** — WaveDB engine in-process (`PageStore` via the typed API) vs
  SQLite. Same process, same thread, no socket.
- **Server** — WaveDB through `quick-node` over HTTP vs MongoDB vs PostgreSQL vs
  MySQL over their own protocols, all on loopback, all from the same harness
  process.

WaveDB appears in both, which is the point: the difference between its two
numbers is the cost of its own transport, measured on the same machine in the
same run.

**Retained work** is, for this pass, a **caveat and not a control**. The others
run with no history table and no versioned documents; WaveDB runs as it is,
retaining every version. Consequently:

- every update row carries a fixed annotation — *WaveDB retains all versions; the
  others retain none* — in both the JSON record and `index.md`;
- the **storage footprint** (§4.1) is mandatory on the update run, because it is
  where that asymmetry becomes visible rather than rhetorical — an update
  benchmark that reports only latency has hidden the entire trade;
- no headline may compare update *latency* without both.

This is the honest form of "compare without history": state the difference, keep
it in the data, and do not launder it into a speed claim in either direction.

### 3. The workload

A single fixed schema, compiled into the bench binary (per-type storage is
compile-time — `StructStorage` statics — so the schema cannot be a runtime
parameter). One `#[wavedb]` NonUnique struct of realistic shape: a handful of
scalars, two short strings, one longer text field; one `#[wavedb::pivot]`; a
`#[wavedb::key]` variant as a second binary. The SQL side gets a table of the
same columns with a primary key and one index matching the pivot; Mongo gets a
collection with one secondary index on the same field.

Row content comes from a **fixed-seed PRNG**, so every system receives logically
identical data and a re-run of a seed derivation (§6) reproduces the same
dataset.

Dataset sizes cross the two boundaries that matter, since RAM and IOps are the
scarce resources:

1. **Fits the record cache** — everything served from memory on every side.
2. **Exceeds the cache, fits page cache/RAM** — WaveDB reads through to settled
   pages; the others serve from their buffer pools.
3. **Exceeds RAM** — the only size where the on-disk layout is actually under
   test. Reported separately, and the suite must refuse to *claim* anything at
   sizes 1–2 that it only measured there.

Read measurements are taken **cold and hot**: hot as loaded, cold after
reopening the store and dropping the OS page cache. A read benchmark that only
reads back what it just wrote is measuring `mem_cache`
(`crates/wavedb-storage/src/read_through.rs:19`) — and the equivalent buffer pool
on the other side — not the read path.

Access distribution: uniform random over the key space, plus a Zipfian pass
(skewed, the realistic shape) so the caches are exercised the way a real
workload exercises them. Single-threaded first; a concurrency sweep only after
the single-client numbers are trusted (see §9 — it is where WaveDB is predicted
to lose hardest, and [0058](0058-per-type-actors-DEPRECATED.md) is parked).

### 3.1 The second workload: an e-commerce shop

Everything above measures **one operation on one flat type**, which is what a
micro benchmark is for and is also its limit: no application asks a database for
one row of one table. So there is a second workload, reported in its own table,
whose phases are the things a customer actually waits on — and reported as
**latency, not throughput**, because a rate hides exactly the tail a page render
lives or dies by.

Three types, and every one of the modelling decisions is a claim being priced:

```rust
#[wavedb]                                  // Unique: one per tenant
struct User { name, address, city, email, shoppings: ShoppingPivotId }

#[wavedb(NonUnique, page = 32)]
struct Shopping {
    #[wavedb::list(page = 10)] bought_at: u64,
    discount_cents: u64, transport_cents: u64,
    items: ProductPivotId,
}

#[wavedb(NonUnique, page = 32)]
struct Product { #[wavedb::list(page = 10)] name: String, quantity, unit_cents }
```

- **A user is a tenant.** The tenant is 48 bits *of the `Id`*, so "one shop,
  many customers" is what tenancy is for, and this is the first workload here
  that has more than one of them — it is the answer to open question 9, at least
  for the access paths. `User::get(&db)` takes **no key**: the identity is in the
  handle. The other four carry a `user_id` column plus an index on it, and that
  difference is what the profile row prices.
- **Records hold pivots**, which is how trees nest: the user holds its order
  collection's `PivotId`, each order holds its line items'. The SQL schema's two
  foreign keys are the same relationship from the other side.
- **The lists are the read path.** `Shopping` is listed by `bought_at`,
  `Product` by `name`, both at `page = 10`, so rendering a page of ten is one
  segment read holding whole records — against `ORDER BY … LIMIT 10 OFFSET n`
  over an index everywhere else. This is where RFC 0051's duplication either
  pays for itself or does not.

The five phases:

| Phase | What it is | WaveDB | The other four |
|---|---|---|---|
| `signup` | create a user | order pivot + `save` | one `INSERT` |
| `checkout` | one order **and its line items** | `2 + items` batches, one barrier each | **one transaction**, one barrier |
| `profile` | the user's own record | `User::get` — no key | `SELECT … WHERE id = ?` |
| `order_page` | user + ten orders | one page descent on the declared list | `ORDER BY … LIMIT 10 OFFSET n` |
| `order_detail` | one order's line items | a second list's page | a second indexed range |

**The checkout is the point.** An e-commerce checkout is inherently several
records, and the other four commit them together for one barrier because that is
how an application would write it. WaveDB has no multi-record transaction —
every collection op is its own atomic `Store::apply` batch — so it pays one
barrier per record. Pricing that is worth more than any number in the micro
table, because it is not a tuning gap: it is the data model, and no amount of
tuning changes it.

Two deliberate non-choices, stated so they are not mistaken for oversights.
MongoDB gets the **reference model, not embedded line items** — embedding is
what a Mongo application would really do, and it would be one document write per
checkout and no join for a detail; holding the shape equal across all five keeps
the row about access paths rather than about modelling, and the embedded variant
deserves its own row rather than a silent substitution. And the preload is
**bulk** wherever a system offers it (one transaction, `insert_many`), because
it is never timed and the stored form is identical either way — WaveDB, which
has no bulk path, therefore takes minutes where the others take seconds, and
that shows up in wall clock rather than in a measured phase.

Everything is measured **after a reopen or restart**, uniformly: the preload's
writes would otherwise still be warm, and WaveDB's per-type cache is a *write*
cache, so it would be reading back what it had just written.

### 4. What gets measured

Throughput alone is the least interesting column. Every run records, per
operation and per configuration:

- **Latency distribution** — p50 / p95 / p99 / max, not just the mean. A mean
  hides exactly the checkpoint and settle pauses worth knowing about.
- **Barriers per operation.** WaveDB can *self-report* this:
  `Journal::barriers()` already counts its own `fsync`s (`journal.rs:214`,
  asserted on in `page_store.rs:324`). For the others, count at the process
  boundary. This is a first-class result, not a footnote.
- **Bytes written to disk** per operation (`/proc/self/io` for the embedded
  bracket; filesystem delta for the server bracket).
- **Storage footprint** — a first-class result with its own subsection below.
- **RSS** at end of run.
- **Error counts** — notably `Error::Conflict`, which WaveDB returns where the
  others block. A blocked lock and a refused write are different outcomes and
  must not be averaged together.

#### 4.1 Storage footprint

Space is a headline metric, not a footnote, and WaveDB is expected to look bad
at it — it retains every version by design and has never been tuned for size.
The point of measuring it is not the verdict but the **decomposition**: "WaveDB
uses 3× PostgreSQL" is unactionable, while "1.4× is live data, 1.2× is retained
history, 0.4× is page slack" tells you where to work, and which part is a bug
rather than a design choice.

**When it is measured.** A single number is meaningless because every system
defers different work. Three points, all three reported, never conflated:

| Point | Meaning | WaveDB | MongoDB | PostgreSQL | MySQL | SQLite |
|---|---|---|---|---|---|---|
| **0. baseline** | the system running, with none of this dataset in it | superblock | empty `--dbpath` | post-`initdb` cluster | post-`--initialize` datadir | empty file |
| **a. hot** | right after the run, deferred work still pending | settle queue may be undrained | — | — | — | — |
| **b. settled** | after each system's own natural quiescence | checkpoint + settle drained | clean shutdown (WT checkpoint) | clean shutdown (checkpoints and closes) | clean shutdown (buffer pool flushed, purge complete) | `wal_checkpoint(TRUNCATE)` |
| **c. compacted** | after explicitly asking for the smallest form | defrag ([0042](0042-free-space-defragmentation.md)) | `compact` | `VACUUM FULL` | `OPTIMIZE TABLE` | `VACUUM` |

Point **b** is the fair comparison and the one a headline may quote. Point **a**
exposes who is hiding work; **c** shows the floor — and for WaveDB it is a floor
with history still in it, which is exactly the honest picture.

Point **0** is not a comparison row; it is a **correction term**, and the
implementation added it because without it the small-scale numbers lie. An
empty PostgreSQL cluster measures ~24 MB of system catalogs and an empty MySQL
datadir ~55 MB, so at 2 000 rows their amplification comes out at 49× and 101× —
figures that describe `initdb`, not the dataset. Recording the baseline lets a
reader see the furniture instead of inferring it. Note that for the two servers
the settled measurement covers a **clean shutdown**: that is the strongest
quiescence available and the only one that is the same operation on all three
servers, where "checkpoint and wait for purge" is three different guesses.

**What counts.** Everything the system needs to serve the data after a restart:
data files, journal/WAL, indexes, and dictionaries. Both `du --apparent-size`
and allocated blocks are recorded, since WaveDB allocates in 4 KiB block runs and
leaves free runs behind — the gap between the two *is* fragmentation.

**Split into payload and log capacity, or the metric measures configuration
defaults.** The seeds settled this empirically at 200 000 rows: MongoDB's data
directory is 227 MB, of which **200 MB is the WiredTiger journal** against a
22 MB collection; MySQL's is 309 MB, of which 100 MB is preallocated
`#innodb_redo` and 50 MB is a binlog, against 72 MB of table; PostgreSQL's 167 MB
is 87 MB of `base` beside 80 MB of preallocated `pg_wal`. Those log areas are
**fixed capacities chosen by the server's defaults**, not functions of the
dataset — they are the same size at 200 000 rows and at 20 rows. Counting them
into one total would make the footprint headline a comparison of default log
settings, and it would flatter WaveDB for a reason that has nothing to do with
WaveDB: its journals are 29 bytes each after a checkpoint retires them
([0047](0047-generational-journal-retirement.md)), because it preallocates
nothing. So the table reports **payload** (data files + indexes) and **log
capacity** as separate columns, sums them for completeness, and quotes payload in
any headline. The same rule is what keeps the WaveDB row honest in the other
direction: a `data.bin` measured before the settle queue drains counts a whole
retained journal as though it were stored data.

**Derived numbers**, which is where the metric becomes comparable at all:

- **bytes per live record** — total ÷ live count, the readable headline.
- **amplification** = on-disk bytes ÷ logical payload bytes, where the logical
  size is the summed wire size of the live records. Because all five systems hold
  the identical dataset (§3, fixed-seed generator), this is one number on one
  scale for everybody.
- **history share** — for WaveDB, the fraction attributable to archived versions
  rather than live ones. Today archives and live anchors are mixed in the same
  pages, so this must be derived by walking the chains; if
  [0059](0059-object-storage-capacity-tier-PLANNED.md)'s phase 1 lands, the
  archive lane makes it a direct per-lane read — and this benchmark becomes that
  phase's acceptance test.
- **page slack** — WaveDB self-reports it almost for free: `BlockDescriptor`
  carries a 4-bit occupation gauge, "a coarse 1/16th fill gauge the directory can
  read **without** touching the page" (`block.rs:73,140`), so a descriptor walk
  yields a fill histogram at no IO cost. No competitor exposes anything this
  cheap; report it as a WaveDB-only diagnostic, never as a comparison row.

**Compression must be stated per system or the whole metric is noise.** The
defaults are not aligned: WiredTiger compresses collections with snappy and
prefix-compresses indexes **by default**; InnoDB and PostgreSQL heap data are
effectively uncompressed (TOAST only kicks in for large values); SQLite has none;
WaveDB applies per-type zstd dictionaries. So the footprint table records each
system's compression setting as a column, and MongoDB is measured **twice** —
snappy (its default, the honest real-world figure) and `none` (the like-for-like
figure against the uncompressed SQL pair).

### 5. Harness shape, and the engine constraints it must respect

The bench crate lives at `benches/` and is **excluded from the workspace**.
`Cargo.toml` has no `exclude` key today (its comment at line 4 refers to one);
this RFC adds it. The reason is hard: the suite depends on `mongodb`,
`tokio-postgres`, `mysql_async` and `rusqlite`, and none of those may enter the
shipped dependency graph or `cargo deny check`'s audit surface. The bench crate
depends on the WaveDB crates by path, never the reverse.

Four engine facts the harness has to be built around:

1. **One store per process** — `StructStorage`'s statics are process-global and a
   second open fails with `StorageError::EngineBusy`
   (`struct_storage.rs:221,233`). One process per WaveDB configuration; the
   server bracket runs the node as a child process, the pattern
   `examples/contact-book/tests/local_cache_e2e.rs` already uses.
2. **Bench the typed path.** `Store::get` is documented as an untyped fallback
   that probes *every* slot — "typed callers use `get_of` and skip this scan
   entirely" (`apply.rs:142–147`). A read benchmark that calls `get` measures a
   path no generated code takes. All reads go through the typed API.
3. **Non-`Send` futures.** The engine is a current-thread `LocalSet` model.
   Criterion's async executor must be built on a current-thread runtime; if that
   fights the harness, the fallback is a hand-rolled timing loop, which for
   whole-operation latency percentiles is barely a loss.
4. **Steady state.** Measure after the fill, never during, and never across a
   checkpoint boundary without saying so. A run that straddles one and reports
   the mean has measured a checkpoint and called it an update.

#### The cage: every system gets the same machine

A comparison in which each system sees a different machine is not a comparison,
and by default each of them *does* see a different machine — because each infers
its budget from the host rather than being told one. So the whole run executes
inside a fixed cage, and every server's cache is pinned rather than inferred.

```sh
systemd-run --user --scope -p MemoryMax=500M -p MemorySwapMax=0 \
    -p Delegate=yes -- \
  taskset -c 0-3 \
  bwrap --dev-bind / / --unshare-pid --proc /proc -- wavedb-bench …
```

Three tools, because no one of them does all three jobs, and it is worth being
precise about which does what:

- **`systemd-run` — the memory cap.** cgroup v2's `MemoryMax`, and the only one
  of the three that can bound memory at all. It bounds the **page cache** as
  well as anonymous memory, which is the point: without it a 900 MB dataset
  simply lives in RAM and a "cold" read is a memory read. This is also the
  mechanism the exceeds-RAM question (open question 4) needs, at a size that
  fits in an afternoon.
- **`taskset` — the CPU budget.** `AllowedCPUs` on the scope would be tidier,
  but `cpuset` is not among the controllers delegated to a user scope here
  (`cpu io memory pids`), so affinity it is. Affinity is also what `nproc`
  reports, so the servers size their thread pools from the budget rather than
  from the host.
- **`bwrap` — the namespace, and *nothing else*.** Bubblewrap caps no resource
  whatsoever; it is here for a private PID namespace, so a killed run cannot
  leave a `mongod` behind (it did, twice, during development), and for one
  uniform filesystem shape. `--dev-bind / /` is deliberate: the databases must
  write to the real disk, and a tmpfs would put them in RAM and measure the
  wrong thing.

**Each server's cache is pinned to the same budget** — MongoDB
`--wiredTigerCacheSizeGB 0.25`, MySQL `--innodb-buffer-pool-size=256M`,
PostgreSQL `-c shared_buffers=256MB` — and that is not tidiness either. Every
one of them sizes its cache from the *machine's* RAM, not the cgroup's, so under
the cage an unpinned MongoDB asks for gigabytes it cannot have and is
OOM-killed, while MySQL and PostgreSQL quietly take different fractions of a
machine none of them can see. Equal budgets are what make the row a comparison.
256 MB is not a tuning choice: it is MongoDB's floor (`--wiredTigerCacheSizeGB`
refuses less than 0.25), so the least-adjustable server sets the number for all
three. Only one server runs at a time, so the cgroup holds one server plus the
benchmark process.

**One configuration, and the run may not record outside it.** Two exceptions
existed and both are closed. The scope used to be created loose (10 G) and
tightened at startup so that *fills* got the machine; that measured **worse** —
8 000 shop users filled in 27 s at 500 MB and 483 s at 10 G, because a fill is
write-bound and the extra RAM only defers the writeback into one cliff — so the
scope is now created at the budget and the fill runs inside it. And an uncaged
process (a plain `cargo run`) used to record perfectly happily into a host lane
of its own; it now prints and refuses, because such a row is not a worse
measurement but a measurement of **another machine**, comparable with nothing
in the corpus. `--force` overrides, and marks the record `"forced": true`
beside `"caged"`.
The embedded pair has no equivalent knob: SQLite's page cache defaults to a
couple of megabytes and WaveDB's per-type cache is unbounded, which under the
cage is itself a thing worth watching.

**The cage is part of the lane identity.** `Host` records the *effective*
budget, not the physical machine: CPUs from `Cpus_allowed_list` (which
`/proc/cpuinfo` would not show) and memory from the cgroup's `memory.max` (which
`/proc/meminfo` would not show), and both fold into the host key. A caged run
and an uncaged one on the same hardware are different lanes, because as far as
any number here is concerned they are different machines.

One measurement bug the cage's arrival exposed, recorded because it is the
suite's characteristic failure mode: write bytes were being read from the
server's own `/proc/<pid>/io`, which is right for MySQL and MongoDB and **wrong
for PostgreSQL**, which is process-per-connection — the postmaster writes
essentially nothing and every byte comes from a backend, the WAL writer or the
checkpointer. It reported a confident `0.0 kB/insert`. The counter now sums the
process tree.

### 6. Seeded datasets are Nix derivations

Refilling ten million rows into four servers before every run is the single
most tedious cost in the suite, and it is pure waste: the dataset is a pure
function of (system, version, size, generator seed). So it becomes a
derivation, and the Nix store is the cache.

```nix
packages.bench-seed-pg-1e7 = stdenv.mkDerivation {
  name = "bench-seed-pg-1e7";
  nativeBuildInputs = [ postgresql_18 bench-gen ];
  buildPhase = ''
    initdb -D "$out/data" --no-locale --encoding=UTF8
    pg_ctl -D "$out/data" -o "-k $TMPDIR -c listen_addresses=" -w start
    bench-gen --system pg --rows 1e7 --seed 42 --socket "$TMPDIR"
    pg_ctl -D "$out/data" -w stop        # clean shutdown: no recovery on first use
  '';
};
```

One per (system × size), plus the SQLite file and the Mongo `--dbpath`. Five
properties make this the right tool rather than a `~/.cache` directory:

- **Version binding is free and correct.** A data directory is locked to its
  server's major version. Because the seed derivation takes the server package as
  an input, bumping `postgresql` in `flake.lock` invalidates the seed
  automatically. An ad-hoc cache would cheerfully hand a PostgreSQL 18 data
  directory to PostgreSQL 19.
- **Cached by inputs, not by content.** A data directory holds timestamps, WAL,
  and a random system identifier — it is **not** bit-reproducible, and the
  derivation must not be marked fixed-output or claim otherwise. Nix caches it by
  input hash, which is all this needs.
- **Clean shutdown inside the builder.** Without it the first measured operation
  pays crash recovery and the number is garbage.
- **The store is read-only; databases write.** Each run materialises a working
  copy: `cp --reflink=auto -r` (near-instant on btrfs/xfs, a full copy elsewhere),
  or overlayfs with the store path as `lowerdir` (no copy at all, needs user
  namespaces). Reflink is the default because it needs no privileges. The
  materialisation cost sits **outside** the measurement window and is reported as
  its own field, so a machine where it is slow is visible rather than mysterious.
- **GC roots.** A 40 GB seed that `nix-collect-garbage` eats between runs is
  worse than no cache: `nix build --out-link .bench-seeds/<name>` (gitignored)
  pins them.

Two honest limits:

- **The insert benchmark never uses a seed.** The fill *is* the measurement.
  Seeds serve the read, update and listing phases only.
- **The WaveDB seed rarely pays.** The same trick works (a filled `data.bin` plus
  its journal), but its inputs include the crate sources, and the on-disk layout
  changes freely between commits under the pre-release no-versioning policy — so
  nearly every commit invalidates it. The caching is a large win for the four
  competitors and a small one for WaveDB. That asymmetry is stated, not
  engineered around; scoping the seed's inputs to "only the storage crates" would
  be a clever way to serve a stale `data.bin` to changed code.

And one implementation constraint worth recording, because it is invisible until
it isn't: **`mongod` cannot start in the Nix build sandbox.** Its bundled
tcmalloc reads `/sys/devices/system/cpu/possible` to size the per-CPU caches and
`CHECK`-fails — `SIGABRT`, before a single command-line argument is parsed —
when the file is absent. The sandbox does not mount `/sys`, and the builder
cannot create it either, because the sandbox root is read-only. The Mongo seed
therefore runs its whole fill inside a `bubblewrap` namespace whose root is a
fresh tmpfs with a synthetic `/sys` (nested unprivileged user namespaces are
permitted inside the sandbox; `bwrap --dev-bind / /` is not, since that root
inherits the read-only mount). The CPU mask written there is a **fixed** value:
a seed must be a pure function of its declared inputs, and reading the builder's
real core count would make it a function of the machine as well.

The package choice has the same flavour of hidden cost. `mongodb` in nixpkgs is
unfree, and unfree packages are not distributed by `cache.nixos.org` — pointing
at it silently compiles the whole server locally, hours of C++ at every version
bump, more than the rest of the suite combined. `mongodb-ce` is the official
prebuilt tarball plus `autoPatchelf`: the licence then costs a download. It
ships `mongod`/`mongos` only, so `mongosh` and `mongoimport` come from their own
(free) packages.

### 7. Nix is the runner, and the results corpus lives in git

The suite runs as a flake app, mirroring `apps.fmt` / `apps.real_example`
(`flake.nix:337,185`):

```sh
nix run .#bench              # full suite, writes one results file
nix run .#bench -- --quick   # smoke pass, does not record
```

`writeShellApplication` with `runtimeInputs = [ rustToolchain postgresql_18
mysql84 sqlite mongodb ]`; the app materialises the seeds, starts `postgres`,
`mysqld` and `mongod` on unix sockets under a temp dir, runs the harness, tears
them down, and writes the results file. No Docker, no ambient service, no "make
sure Postgres is running" step in a README. Two nixpkgs facts already checked and
load-bearing: **`mysql80` no longer exists** (removed at its 2026-04-30 EOL), so
the pin is `mysql84` (8.4.10 LTS); and **MongoDB is unfree** (`meta.unfree =
true`, SSPL — 7.0.37 as `mongodb`, 8.2.11 as `mongodb-ce`), so the flake needs a
narrowly-scoped `allowUnfreePredicate` for that one package, which is a
deliberate, documented exception rather than a blanket `allowUnfree`.

Every recorded run appends to `benches/results/`, and the corpus is
**append-only** — the same rule the RFC numbering follows. A past number is never
edited to look better; a run that was wrong is superseded by a later run and
annotated, not rewritten.

```
benches/results/
  index.md                                  # one line per run, append-only
  <host-key>/2026-08-14T09-31Z-b7b281b.json # the raw record
```

The **host key** is what makes the corpus usable. Performance numbers are not
portable across machines, so each run records a fingerprint — CPU model and core
count, RAM, kernel, filesystem type of the data dir, whether the disk is
rotational, whether the host is virtualised — and derives a short stable alias
from it (`ryzen7840u-nvme-ext4`). The rule that keeps the history honest:

> **Two rows are comparable only when their host keys match.** Different host
> key ⇒ different lane, never a trend line.

Each JSON record carries: the host fingerprint; the WaveDB git SHA and whether
the tree was dirty; the `flake.lock` revision; every system's version string;
**the store path of every seed used**, so a row names the exact dataset it ran
against; the full configuration of each system (not a profile name — the actual
settings); the workload parameters; and the per-operation metrics of §4 including
repeat count and observed variance.

Two guards against the corpus filling with noise:

- **A dirty tree records, but is marked.** A run from uncommitted code is a data
  point about code that does not exist; it stays flagged forever.
- **A noisy machine refuses to record.** The runner samples load average and
  refuses if the box is busy. Better a missing row than a slow row that a future
  bisect blames on a commit.

JSON is the stored form because it diffs and machines read it; `index.md` gets
one appended human line per run so `git log` on that file is a readable history.
No regenerated summary file — a rewritten table would churn the diff and defeat
the point of committing the numbers at all.

### 8. Deferred: the history comparison (phase 4)

Designed now so the deferral is a schedule and not an omission.

**The `+history` control.** The three SQL systems and Mongo are made to retain
old versions the way WaveDB does — the simplest faithful form being a second
table (or collection) written in the same transaction as the update:

```sql
BEGIN;
  INSERT INTO thing_history SELECT * FROM thing WHERE id = ?;
  UPDATE thing SET … WHERE id = ?;
COMMIT;
```

This is the only apples-to-apples update comparison there is: with it, the
question stops being "who is faster" and becomes "which way of retaining history
costs less", which has a real answer. Without it — the state of phase 1–3 — the
update row compares two different jobs, which is why §2 makes the annotation and
the on-disk-size column mandatory.

**The version walk.** `record_history` / `unique_history` walking the chain
versus `SELECT … WHERE pk = ? ORDER BY version` over the audit table. Only
meaningful once the control exists, since otherwise there is nothing on the other
side to read.

### 9. Predictions, written before the first run

Recorded here so the results cannot be reinterpreted after the fact.

**WaveDB should lose:**

- **Bulk insert, by a lot.** One op is one batch is one `fsync`. `COPY`,
  `LOAD DATA`, `insertMany` and SQLite's many-inserts-one-transaction all
  amortise the barrier over thousands of rows; WaveDB has no multi-record
  transaction to amortise with. This is a real gap, not an artifact — group
  commit is the fix, and this benchmark exists partly to size it.
- **Concurrent writes against MongoDB, hardest of all.** Mongo's journal
  group-commits across concurrent writers, so its durable throughput scales with
  clients while WaveDB's per-op barrier does not. Single-client latency may look
  comparable; the gap should open with the client count, and that is precisely
  the shape of the missing group commit.
- **Update, on latency, against everyone** — since it retains and they do not
  (§2). The footprint columns are where that trade should be readable.
- **Storage footprint, against everyone, at every measurement point.** It
  retains all history, has never been tuned for size, allocates in 4 KiB runs,
  and pays a chain segment per collection. The interesting result is not the
  ratio but its decomposition (§4.1): how much of the gap is *retained history*
  (a design choice, and the price of a feature nobody else offers) versus *slack
  and overhead* (not a design choice, and therefore work items). A large slack
  share would be the most actionable finding the whole suite could produce.
- **Anything with more than a handful of declared `#[wavedb::list]`s** — each one
  is a full extra copy of the record per write, by design
  ([RFC 0051](0051-ordered-record-lists.md)).

**WaveDB should win:**

- **Point read of a hot record in the embedded bracket** — no parse, no plan, no
  protocol, a cache hit keyed by id.
- **The recency listing**, which is one segment read against an index the others
  maintain on every single update.
- **Point read against Mongo in the server bracket**, where both do a whole-value
  fetch by id and the difference is protocol weight — the closest, and therefore
  most informative, single comparison in the suite.

If a prediction is wrong, the results file says so in a line of prose. That
sentence is the most valuable thing the suite can produce.

## Implementation status

**Landed (phase 1, 2026-08-13).** `benches/` builds outside the workspace,
`nix run .#bench` links the `flake.lock`-pinned system SQLite (never
`rusqlite`'s bundled copy), and one run produces the append-only record of §7 —
host key, provenance, per-phase percentiles, and the three footprint points.
The noise guard and the dirty-tree marking both work: a machine above load 1.0
refuses to record.

**The seed derivations of §6 are built** (2026-08-13), for **all five systems**,
including the three whose Rust adapters do not exist yet. That is possible
because of one change to the plan: `bench-gen emit-tsv` writes the dataset once
in a portable form, and each seed loads that same file with its own bulk tool
(`.import`, `\copy`, `LOAD DATA`, `mongoimport`) inside the builder. So a
PostgreSQL, MySQL or MongoDB seed is version-pinned and verified **now**, and
phases 2–3 only have to write the client code. WaveDB is the exception: it has
no bulk path, so it fills through the engine, and the sidecar it writes
(`ids.bin`, `pivot.bin`) is not optional — a NonUnique anchor id is minted from
the clock and cannot be recomputed from the seed.

All five verify their own row count in the builder, and at 200 000 rows they
measure (apparent size, as loaded, before any adapter-driven compaction):

| Seed | Total | Payload (data + indexes) | Log capacity | Build |
|---|---|---|---|---|
| wavedb | 23 MB | 20.9 MB `data.bin` (+3.2 MB id sidecar) | 58 B (two retired journals) | ~11 min |
| sqlite | 54 MB | 54 MB (post-`VACUUM`) | 0 (checkpointed + truncated) | seconds |
| postgres | 167 MB | 87 MB `base` | 80 MB `pg_wal` | seconds |
| mongodb | 227 MB | 22 MB collection + 4.2 MB indexes | 200 MB WT journal | seconds |
| mysql | 309 MB | 72 MB `bench` (+26 MB `mysql.ibd`) | 100 MB redo + 50 MB binlog | seconds |

Three things to read out of that table before any adapter exists. The log column
is why §4.1 now insists on the split — it is configuration, not data. The WaveDB
payload is the **smallest of the five while retaining history**, which is the
same doubt §9's storage prediction already collected, now at real scale. And the
build column is the group-commit gap seen from the build side: one insert is one
batch is one `fsync`, so WaveDB's fill takes minutes where a bulk loader takes
seconds.

**The server bracket landed (phases 2–3, 2026-08-14).** MongoDB, PostgreSQL and
MySQL adapters, each in both durability rows, bringing the table to nine rows.
Four decisions in them are worth stating because they are what make the rows
mean anything:

- **Each adapter starts its own server**, in the run's scratch directory, on a
  unix socket (MongoDB: a port derived from the process id). Nothing is measured
  against a machine-wide instance, because the durability column has to describe
  a server *this run* configured or it is a guess. `pg_ctl` and `--fork` are
  both avoided for the same reason: they hand back the pid of a process that
  exits, and the write-bytes column would read zero.
- **Write bytes come from the server's `/proc/<pid>/io`**, not ours. In the
  embedded bracket the engine writes in the benchmark's own process and
  `/proc/self/io` is exactly right; in the server bracket our process writes
  socket traffic and nothing else.
- **The synchronous drivers.** `postgres`, `mysql` and `mongodb`'s `sync`
  feature. The workload is one connection issuing one operation at a time (§3),
  so an async client would add a runtime and buy nothing — and MongoDB's pool is
  pinned to one connection so a row cannot quietly measure concurrency the other
  adapters do not have.
- **A `servers` cargo feature**, on by default. `bench-gen` needs no database
  client and is a build input of *every* seed derivation, so without the gate a
  seed rebuild would compile three drivers before writing a TSV.

**The e-commerce workload landed (§3.1, 2026-08-14).** A second nine-row table
under the first, selected with `--workload micro|shop|both`. It reports
**milliseconds, p50 beside p99** rather than a rate, because its phases are
composed operations a customer waits on and a rate hides the tail. It is also
the first workload with more than one tenant — one per user — which is a partial
answer to open question 9: the access paths are exercised, the directory
behaviour under interleaved tenants still is not.

**Not built yet.** The history comparison (§8, phase 4), the recency-listing
row, the exceeds-RAM size, and the concurrency sweep (phase 5).

**One gap found while building.** The barrier count of §4 is not recorded:
`PageStore` exposes no public accessor for its `IoCounts`, and this RFC forbids
changing a shipped crate to serve the benchmark. A one-line accessor would close
it, which is a decision for the engine, not for the harness. Bytes-written *is*
recorded, from `/proc/self/io`.

### What the first measurements said

None of this is recorded — one machine, and a load average the guard rightly
refused (1.66 on the embedded-only pass, 7.30 on the nine-row one). The figures
below are the default pass: 100 000 rows, 50 000 reads, 50 000 updates, one
i5-8300H on btrfs. They are an estimate, not a corpus row — the second pass ran
under enough load to halve every rate, so read them **within** a row and not
against the first pass. They are here because several of them bear on the RFC's
own claims.

```
system/row          bracket   insert/s  read_hot/s read_cold/s  update/s  payload     log    amp
wavedb/durable     embedded        172     808079         120        49    23.7M    0.0M  0.93×
sqlite/durable     embedded        259      94579       78891       248    30.5M    0.0M  1.20×
sqlite/relaxed     embedded       5165      94319       78209      3410    30.5M    0.0M  1.20×
mongodb/durable      server        228       2149        2134       279    27.5M  210.0M  1.08×
mongodb/relaxed      server       2095       2132        2137      1877    24.6M  209.9M  0.97×
postgres/durable     server        274      12376       11588       264    59.4M   67.1M  2.34×
postgres/relaxed     server       9500      11980       11455      7879    59.0M   67.1M  2.32×
mysql/durable        server        142       7333        6629       159    83.7M  241.6M  3.30×
mysql/relaxed        server        245       7041        6752       318    83.7M  241.6M  3.30×

empty-system baseline (payload): mongodb 0.1M, postgres 24.0M, mysql 55.1M
```

- **A methodology trap, caught before it entered the corpus.** Journal
  retirement is generational ([0047](0047-generational-journal-retirement.md)):
  the journal a checkpoint supersedes is deleted by the *next* one. Quiescing
  with a single checkpoint therefore counts a whole retained journal as stored
  data, and reported a 1.4 MB database as **34 MB** — a 26× amplification that
  was pure harness error. `quiesce` now checkpoints until the footprint stops
  moving. Every future adapter will meet the same class of bug, which is the
  argument for §4.1's three measurement points existing at all.
- **The storage prediction in §9 is wrong.** WaveDB measured *below* SQLite's
  settled footprint **while retaining every superseded version** — the opposite
  of "expected to lose it outright". At 100 000 rows plus 50 000 updates it
  settles at 23.7 MB against SQLite's 30.5 MB, an amplification of **0.93×**
  against 1.20×: less than the logical payload, with 150 000 record versions in
  it. The per-type zstd dictionaries pay for the history and then some.
- **Cold read is the outlier, it is CPU, and it gets worse with scale.** Hot
  reads run 1.4 M/s against SQLite's 95 K/s — **14× faster**, as predicted.
  Cold reads run 206/s against SQLite's 196 K/s: a **~950× loss**, on an NVMe
  with the whole database in the OS page cache, which rules out IO. It was
  ~100× at 2 000 rows, so the miss path degrades as pages fill — consistent
  with per-page decode (decompress a whole page, keep one record). The gap is
  not "reads are slow"; it is entirely in the miss path, and it is the single
  most valuable thing the suite has produced so far.
- **The server bracket made that finding worse, and it is the headline.** On
  the nine-row pass WaveDB's cold read is **120/s while MongoDB's is 2 134/s,
  PostgreSQL's 11 588/s and MySQL's 6 629/s** — every one of them paying a
  connection round trip per read that WaveDB, in-process, does not. An
  in-process engine losing 18× to a remote document store on a point lookup is
  not a tuning gap; it is a defect in the read-through path, and it is the same
  defect as the update row. Formally these are different brackets and the
  numbers are not comparable — which is exactly why this comparison is worth
  making anyway: the bracket rule exists to stop a *round trip* being mistaken
  for a database, and here the in-process side loses regardless.
- **The storage result holds against all four.** WaveDB's settled payload is
  23.7 MB against MongoDB's 24.6 MB (snappy, relaxed row), PostgreSQL's 59.4 MB
  and MySQL's 83.7 MB — the smallest of the five, retaining 150 000 record
  versions where the others retain 100 000. The peer it barely beats is the one
  that also compresses by default, which is the honest reading: this is the
  zstd dictionaries, not the layout.
- **MySQL's relaxed row barely moves** (142 → 245/s, where PostgreSQL goes 274 →
  9 500 and SQLite 259 → 5 165), because `innodb_flush_log_at_trx_commit=2`
  leaves `sync_binlog=1` in place and the binlog is still `fsync`ed per commit.
  A durability row is a *set* of settings, not one knob — which is why §2 pins
  them by name rather than by profile.
- **Insert is at parity; update is not, and the difference is the miss path.**
  Insert runs 222/s against SQLite's durable 259/s — both `fsync`-bound, both
  scale-stable (222/s at 100 000 rows against 236/s at 2 000). Update runs 65/s
  against 375/s, and that gap *grew* with scale (1.4× at 2 000 rows, 5.8× here)
  — as it must, since an update reads before it writes and the read it does is
  a cold one. Fixing the miss path is the same work as fixing the update row.
- **The relaxed row behaves as predicted.** SQLite relaxed reaches 7 486/s
  insert and 4 623/s update, ~29× and ~71× its own durable row — the
  group-commit gap of §9, and the reason WaveDB has no relaxed row to offer.
- **The cache is a write cache.** A read that misses does not populate it, so a
  store nobody has just written to has no warm state at all: on a seeded run
  `read_hot` does not exist as a distinct measurement. That is
  [RFC 0044](0044-page-cache-PLANNED-LOW.md), currently *Planned-low*, and it
  is the other half of the cold-read finding above.

### Two bugs the seeds found, both in this suite

Worth recording because both are the kind that produce a *plausible number*
rather than an error, which is the failure mode a benchmark must be built
against.

1. **A green build with an empty database.** `psql` returns 0 after a failed
   `\copy` unless `ON_ERROR_STOP` is set, so the first PostgreSQL seed built
   successfully and held **zero rows** — a benchmark run against it would have
   measured an empty table and reported the result. Every seed now verifies its
   own row count inside the builder and fails loudly.
2. **The dataset was not the same on every system.** `score` was a full `u64`,
   which is above `i64::MAX`: PostgreSQL's `COPY` rejected the row outright,
   and SQLite silently stored it as a **float**, losing precision. Two systems
   holding different values breaks the one property the shared dataset exists
   to provide, and the amplification denominator with it. The generator now
   keeps `score` inside signed-64-bit range, and the reason is a comment in
   `schema.rs` rather than folklore.

A third, smaller: a seed copied out of the store is world-readable, and
**PostgreSQL refuses to start** on a data directory group- or other-readable, so
materialising is `chmod -R u+w,go-rwx`, not `u+w`.

## What this deliberately does not do

- **Not a CI gate.** Benchmarks in CI are noise generators; the machine varies,
  the numbers drift, and the gate gets disabled within a month. Runs are manual
  and deliberate.
- **Not a fairness certificate.** Every one of these systems can be tuned for
  years. The suite pins *stated, published* configurations and reports them in
  full; it does not claim to have found anyone's best case, and it must never
  tune WaveDB while leaving the others at defaults.
- **Not a feature comparison.** Nothing here says WaveDB replaces PostgreSQL or
  MongoDB. It measures three operations they all have.
- **No headline number.** A ratio without the durability row, the transport
  bracket, and the retention annotation is not a result.

## Alternatives

- **Adopt an existing suite (YCSB, sysbench, TPC-*).** Rejected: all of them bind
  to SQL or to a client driver model WaveDB has no adapter for, and writing that
  adapter is most of the work anyway — with the added cost that the workload
  would then be shaped by the harness rather than by what WaveDB is for. Their
  *methodology* (fixed distributions, steady state, percentiles) is borrowed;
  their code is not.
- **Re-fill the datasets on every run.** Rejected — it is the tedium that stops a
  benchmark from being run at all, and a filled data directory is a pure function
  of its inputs, which is exactly what a derivation is for.
- **A plain `~/.cache/wavedb-bench/` directory instead of derivations.**
  Rejected: it has no version binding, so a nixpkgs bump silently pairs an old
  data directory with a new server — the failure mode is either a refusal or,
  worse, a number nobody can explain.
- **Marking the seeds fixed-output.** Rejected as impossible: a data directory
  contains timestamps and a random system identifier, so there is no content hash
  to state.
- **FerretDB instead of MongoDB** (Apache-2.0, in nixpkgs at 1.24.0), to avoid
  the SSPL unfree exception. Rejected: it speaks the Mongo wire protocol on top
  of PostgreSQL, so benchmarking it measures PostgreSQL with a translation layer
  — the opposite of the point.
- **`$set` field patches for the Mongo update row.** Rejected: WaveDB writes
  whole records; a partial update is a different operation.
- **Benchmark only against SQLite.** Tempting — it is the honest peer for an
  embedded engine, and needs no server bracket at all. Rejected because MongoDB
  is the model's real peer and the SQL pair is what a developer actually weighs
  WaveDB against; refusing the comparison reads as avoiding it.
- **Results in a separate repository.** Rejected: the value of the corpus is that
  a result names a commit *in this history*, so a regression is bisectable with
  the tools already at hand.
- **Regenerate a summary table on every run.** Rejected — churn in the diff, and
  a rewritten past.
- **Docker Compose for the competitor databases.** Rejected: the project already
  builds and tests through Nix, `flake.lock` gives version pinning Compose tags
  do not, seeds-as-derivations has no Compose equivalent, and a second toolchain
  is a second thing to keep working.

## Open questions

1. **How large may a seed be before the cache stops being worth it?** The
   exceeds-RAM seeds are tens of gigabytes each, times five systems. Options: cap
   the cached sizes at 1–2 and fill size 3 on demand, or accept the disk cost with
   an explicit `nix store gc` story. Needs a first measurement of the actual
   footprint.
2. **Should seeds be pushed to a shared binary cache** (attic/cachix) so a second
   machine skips the fill? Attractive for sizes 1–2; for size 3 the transfer may
   cost more than refilling. Probably: yes for the small ones, never for the big.
3. **Overlayfs or reflink for materialisation?** Reflink needs no privileges but
   silently degrades to a full copy on ext4; overlayfs never copies but needs user
   namespaces. Leaning: reflink by default, overlay behind a flag, and always
   report the materialisation time so the degradation is visible.
4. **How is the "exceeds RAM" size *first* built** — the seed derivation itself
   still has to do that fill once, inside a builder, possibly for hours. Is a
   sandboxed multi-hour derivation acceptable, or does size 3 need a
   build-outside-and-import path?
5. **Concurrency sweep now or later?** WaveDB's concurrency design
   ([0058](0058-per-type-actors-DEPRECATED.md)) is parked, so a multi-client
   sweep would measure a placeholder — but §9 predicts that is exactly where the
   MongoDB gap appears. Leaning: single-client in phases 1–3, and the sweep as its
   own phase rather than never.
6. **Should the harness measure the client cache path** (`Db::open`, the
   write-through cache)? It is the configuration a real app uses and has no
   counterpart in the other four — interesting and incomparable at once. Probably
   a separate, clearly-labelled WaveDB-only table.
7. **How much variance is too much to record?** A threshold has to exist or the
   noise guard is decoration. Needs a first calibration run to set.
8. **Is the WaveDB history share cleanly separable before
   [0059](0059-object-storage-capacity-tier-PLANNED.md) phase 1?** Archives and
   live anchors share pages today, so the decomposition in §4.1 has to be derived
   by walking chains and summing wire sizes — accurate for *logical* bytes but
   unable to attribute *page slack* to one side or the other. If that turns out to
   be too coarse to be useful, the archive lane becomes a prerequisite rather than
   a nice-to-have.
9. **Every row runs under one tenant (`TENANT = 1`), and nothing here measures a
   second one.** That is defensible for the three operations measured: the tenant
   is 48 bits *of the `Id`*, a partition of the key space rather than a filter
   applied at read, so a second tenant changes which anchor a lookup lands on and
   not how many it touches — and none of the four competitors has the concept at
   all, so a tenant column would be a constant on their side and would stop the
   datasets from being byte-identical, which is what makes amplification one
   number on one scale. What it leaves unmeasured is real, though: whether the
   linear-hashed page directory degrades when many tenants interleave inside one
   type's lane, and what a cross-tenant listing costs. Neither is visible in an
   insert/read/update row, so it wants its own workload rather than a column here.

## Phasing

| Phase | Content | Priority |
|---|---|---|
| **1** | The embedded bracket: WaveDB engine vs SQLite, three operations, both durability rows, the results corpus + host key + `nix run .#bench`. Stands alone and is where the methodology gets proven. | **done 2026-08-13** |
| **1b** | The seed derivations of §6 — **all five systems**, via one portable dataset each loads with its own bulk tool, so the three server seeds exist before their adapters. | **done 2026-08-13** |
| **2** | **MongoDB** in the server bracket — the reference peer, and the comparison worth getting right before any other server lands. Landed against `mongod` directly; against `quick-node` it is the same adapter with a different peer, and that pairing is still to come. | **done 2026-08-14** |
| **3** | PostgreSQL and MySQL in the server bracket. Landed. The recency-listing row and the exceeds-RAM size did **not** land with them. | **partly done 2026-08-14** |
| **4** | The `+history` control and the version-walk row (§8) — deferred by decision, not by oversight. | later |
| **5** | Concurrency sweep and the client-cache bracket — gated on open questions 5 and 6. | low |
