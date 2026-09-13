# RFC 0065 — Benchmark suite II: the row as the unit

- **Status:** Planned — opened 2026-09-04
- **Supersedes:** [RFC 0060](0060-comparative-benchmark-suite-DEPRECATED.md)
- **Crates:** the `benches/` package, **excluded** from the workspace; plus two
  new fields on `wavedb_storage::StoreOptions` (behaviour only — see §6)
- **Code (target):** `benches/nix/{params,gen,dataset,seeds,cage,runtime,apps}.nix`,
  `benches/src/bin/{bench,bench-row,bench-gen}.rs`,
  `benches/src/{plan,corpus,cage,metrics,report}/`,
  `benches/src/systems/`, `benches/results/rows/`
- **Related:** [RFC 0064](0064-pivot-owned-concurrency-PLANNED.md) (the sharded
  shape the multi-consumer row finally exercises),
  [RFC 0044](0044-page-cache-PLANNED-LOW.md) and
  [RFC 0061](0061-relaxed-durability-window.md) (the two knobs the fill turns up),
  [RFC 0041](0041-single-barrier-checkpoint.md) /
  [RFC 0047](0047-generational-journal-retirement.md) (the barrier accounting
  this measures)

## Summary

[RFC 0060](0060-comparative-benchmark-suite-DEPRECATED.md) built a working
comparison and proved the methodology — the cage, the footprint decomposition,
seeds as derivations — but it made **the run** the unit: one process, every
system in sequence, one JSON per run. Three consequences follow from that one
choice, and all three are now the binding constraints.

This RFC makes **the row** the unit. A row is one
`(system, variant, durability, workload, tier)` measurement. It has an
identity, it gets its own cage, it is measured by its own process, and it is
stored and reused on its own. From that single change:

- **peers stop being remeasured** — a `postgres/durable` row is a function of
  its inputs, and none of those inputs is the WaveDB commit;
- **WaveDB gets a real multi-threaded row** — a generator thread feeding
  N = 3 consumers is the first configuration in which `Shards::start(store, N)`
  has anything to route;
- **WaveDB against WaveDB across commits becomes the primary table**, because
  individually-addressable rows are what a trend line is made of.

## Motivation

### What 0060 established, and keeps

Nothing below revisits these. They were argued, built, and proven, and §9
records them as carried forward verbatim: the cage and why it is three tools;
the footprint decomposition at four points with payload split from log
capacity; why MongoDB is the reference peer; seeds as Nix derivations rather
than a `~/.cache` directory; the results corpus living in git so a regression
is bisectable; durability as a row dimension rather than a setting; and the
refusal to publish a headline ratio.

### The three things the run-as-unit cannot express

**1. Peers are remeasured every time, and the cost is the suite's adoption.**
A full pass is ~50 minutes, of which the overwhelming majority is
PostgreSQL, MySQL and MongoDB producing — by construction — the same numbers
they produced last time. Their inputs are the `flake.lock` version, the
workload parameters, the seed, the host, and the cage. The WaveDB commit is
not among them. A benchmark that costs an hour is a benchmark that gets run
before releases; a benchmark that costs four minutes is one that gets run
before commits, and only the second one catches a regression while its cause
is still on screen.

**2. There is no multi-threaded WaveDB row, and the code says so.**
`benches/src/systems/engine.rs:184` reads `Shards::start(store, 1)`, and its
comment is right about why:

> The count is 1 because it is the truth rather than a setting: this thread
> *is* the shard […] and `Shards::start` spawns only the disk actor. The
> per-shard worker threads are the node's `Router`'s, and a benchmark driving
> `Store` directly has no requests to route. Passing `N` here would spawn
> nothing and describe a configuration that is not running.

So today's `wavedb-sharded` row is *single thread plus a disk actor*, which is
a real configuration but not the one [0064](0064-pivot-owned-concurrency-PLANNED.md)
is about. The missing piece is not a parameter — it is a **source of
requests**. A harness that generates work on one thread and consumes it on
several is exactly that source, and it is the smallest change that makes the
sharded row mean what its name says.

**3. Nothing in the harness makes the systems compete for CPU on equal terms.**
Data generation sits outside `lat.time()` in all five adapters, which keeps it
out of the *stopwatch* — but it still runs, on the same CPUs, in the gaps
between timed windows, and it runs **serially with** the operation it feeds.
Meanwhile PostgreSQL's WAL writer, MySQL's page cleaners and WiredTiger's
eviction threads run *concurrently with* the timed window on CPUs the client
is not using. The measured gap between the durable and relaxed PostgreSQL rows
(274 → 9 500 insert/s) is largely that: the barrier leaving the critical path
onto threads the embedded pair does not have. Making the generator a
**thread** rather than a call in the loop puts every system in the same shape —
work is produced concurrently with its own consumption, inside one budget —
and the difference that remains is the systems, not the harness.

There is a fourth, smaller motivation that the corpus itself supplies: today
`benches/results/` contains **no MongoDB, PostgreSQL or MySQL numbers at all**.
The one recorded run that attempted them skipped all six server rows with
`spawn mongod: No such file or directory`, because it was not launched through
the Nix runtime. A row that can be measured, stored and reused independently
is also a row that can *fail* independently, instead of taking a fifty-minute
pass down with it.

## Design

### 1. The row, and its identity

A **row** is one measurement:

```
row = (system, variant, durability, workload, tier)
```

`system` ∈ {wavedb, sqlite, postgres, mysql, mongodb}; `variant` is
`single` | `multi` for WaveDB and `-` elsewhere; `durability` ∈
{durable, relaxed}; `workload` ∈ {micro, shop}; `tier` names the dataset size
(§5).

Its **identity** is a digest over everything that can change the number:

```
digest = fnv1a(
    system, system_version, variant, durability,
    workload, tier, dataset_revision, generator_seed,
    consumers,                       // N
    host_key,                        // cpu model · cpu budget · mem budget · fs
    cage_revision,                   // the cgroup/affinity/namespace recipe
    wavedb_git_sha,                  // ← only present for system == wavedb
)
```

The last field is the whole mechanism. For a peer it is absent, so the digest
is stable across WaveDB commits and the stored row is reused. For WaveDB it is
present, so every commit is a new row and nothing stale is ever served. There
is no cache-invalidation policy to get wrong; the identity is the policy.

Rows are stored one per file:

```
benches/results/rows/<host-key>/<digest>.json
```

committed to git, exactly as 0060's per-run records were, and for the same
reason: a number that names a commit *in this history* is bisectable with the
tools already at hand.

The digest is **FNV-1a**, the same function the host fingerprint already uses
(`benches/src/json`), rather than a second hash to keep consistent — this is a
lane identity, not a security hash. Fields are folded with a `\x1f` separator
so neighbouring values cannot trade characters and collide, and `None` for the
SHA folds as an empty field rather than being skipped.

**Why the store, and not a Nix derivation.** The datasets are derivations
because a filled data directory is a pure function of its inputs. A *timing* is
not. A Nix builder runs under `nix-daemon`'s sandbox, which chooses its own
cgroup and CPU affinity — a duration measured inside one was not measured in
the cage this suite defines, and could not be made to be. So the division of
labour is: **Nix owns what is pure (the data), git owns what is observed (the
measurement).** Attempting otherwise would produce numbers that look
reproducible and are not, which is the failure mode the whole suite exists
against.

`--refresh <selector>` remeasures matching rows regardless of what is stored;
`--refresh-all` is the blunt form. Nothing else discards a stored row.

### 2. One cage per row

The runner splits in two:

- **`bench`** — the supervisor. Resolves the plan (which rows are wanted, which
  are already stored), materialises seeds, spawns one caged child per row it
  must measure, then renders the corpus.
- **`bench-row`** — measures exactly one row and writes exactly one JSON. It
  runs *inside* the cage; it is what `bench` execs.

```
bench                                    (uncaged supervisor)
 └── systemd-run --user --scope
       -p MemoryMax=500M -p MemorySwapMax=0 -p Delegate=yes --
     taskset -c 0-3
     bwrap --dev-bind / / --unshare-pid --proc /proc --
       bench-row --system postgres --durability durable --tier large …
         ├── generator thread
         ├── consumer thread × N
         └── postgres  (child process — same cgroup, same mask, same PID ns)
```

Four things this buys, none cosmetic:

- **A row fails alone.** An OOM kill, a startup timeout or a panic costs one
  row, not the pass. 0060 lost a fifty-minute run to a single `mysqld` startup
  timeout, which is why `STARTUP_SECS` is 300.
- **Clean accounting per row.** A fresh cgroup means the memory budget starts
  empty. Today row *k* inherits whatever page cache row *k−1* left behind, and
  since `MemoryMax` bounds page cache, that inheritance is not neutral.
- **The one-store-per-process rule stops shaping the plan.**
  `StructStorage`'s statics are process-global and a second open is
  `EngineBusy`; with one process per row the constraint is satisfied by
  construction rather than by ordering the run carefully.
- **No leaked servers.** A private PID namespace per row means a killed row
  cannot leave a `mongod` behind — 0060 records this happening twice.

The supervisor is deliberately *outside* the cage: it builds nothing and
measures nothing, and putting the orchestration inside the budget would charge
the row for work that is not the row's.

### 3. The producer/consumer harness

Inside one row's cage:

```
   ┌──────────────── bwrap · 500 MB · cpus 0-3 ─────────────────┐
   │                                                            │
   │   [generator] ──── bounded channel ────► [consumer 0] ──►  │
   │        │                                 [consumer 1] ──►  engine / driver
   │        └── routes by owner (user)          [consumer 2] ──►  │
   │                                                            │
   └────────────────────────────────────────────────────────────┘
```
| system | workload | generators | consumers | database |
|---|---|--:|--:|---|
| `wavedb/single` | both | 1 | 1 | same binary, `PageStore` in the consumer thread |
| `wavedb/multi` | **`shop` only** | 1 | **3** | same binary, `ShardStore` per consumer over one disk actor |
| `sqlite` | both | 1 | 1 | same binary, one `Connection` |
| `postgres` / `mysql` / `mongodb` | both | 1 | 1 | child process, **same cage** |

**`wavedb/multi` is a `shop` row only, and the reason is structural.** The
`micro` workload lives in exactly one collection (`systems/wavedb.rs` mints a
single `Thing::create_pivot`), and a collection is **indivisible**: its B+tree
nodes and chain segments carry ids of their own and belong to the *Pivot's*
owner, not to any record's — so two consumers writing disjoint records would
still contend on shared structure. That is silent index loss, not a cache miss.
`wavedb-quick-node/src/shard/route.rs` states the rule, and
[RFC 0064](0064-pivot-owned-concurrency-PLANNED.md) is built on it: the Pivot
instance *is* the unit of concurrency, so a workload with one Pivot has no
concurrency to measure.

Giving `micro` N collections was considered and rejected: it would stop being
the micro workload (`all()` would no longer be one recency order, and the row
would no longer be comparable with the existing corpus), and it would be
measuring a partition invented for the benchmark rather than one an application
has. `shop` partitions for free and for a real reason — a user *is* a tenant
*is* one `Shopping` collection, and every shop phase already picks its user
first — so it is where a sharded engine gets priced.

`MicroOp::partition_key()` returns a constant `0`, and its test asserts that,
so the constraint is enforced in code rather than remembered from a plan.

The generator builds `thing(n, seed)` / the shop's operation stream and pushes
descriptors down a bounded channel; consumers pop, execute, and time only the
execution. The channel is bounded so the generator cannot run arbitrarily far
ahead and turn into a memory experiment — one that must fit inside the row's
500 MB alongside the engine.

**Everything in that box shares the budget**, and that is the standardisation
the shape is for. The generator competes for the same four CPUs everywhere;
the server, when there is one, competes for them too. That a `sqlite` row
leaves two CPUs idle while a `postgres` row saturates four is a **result**, not
an imbalance: the budget is identical, and what a system does with it is the
thing being measured.

#### Why N = 3

Four CPUs, one generator thread, three consumers. `N = 4` oversubscribes and
measures the scheduler; `N = 2` leaves a core to the background threads the
embedded pair does not have, which quietly hands it to the servers. Three is
the number that spends the budget exactly once. It is a **row-identity field**
(§1), so a sweep over `--consumers` produces different rows rather than
overwriting one, and `N` is reported in every table — a throughput without its
consumer count is not a result.

#### The generator is the router

Multi-consumer WaveDB inherits a correctness condition from `ShardStore`,
recorded at `engine.rs`:

> `ShardStore` remembers absence, which holds only while a record is reached
> by exactly one holder.

So the consumers may not share keys. The generator therefore **partitions the
key space** and routes each descriptor to the consumer that owns it — which is
precisely the node's `Router`, and the reason this shape is worth building
rather than approximating with a thread pool over a shared queue. The harness
ends up mirroring the deployed architecture instead of inventing a
benchmark-only one.

The `shop` workload survives, and it is the workload where this is natural
rather than imposed: a `Product` collection is already one per `Shopping`
holder, so the Pivot instance *is* the partition
([0064](0064-pivot-owned-concurrency-PLANNED.md)'s unit of concurrency), and a
user is a tenant is one collection. It is also the only workload that can be
partitioned at all — see the table above.

### 4. What concurrency does to the metric, and why it has to break

`metrics.rs:71` computes

```rust
ops_per_sec = count as f64 * 1e9 / total_ns as f64
```

where `total_ns` is the **sum of the timed windows**. With one consumer that is
exactly right, and it was chosen deliberately: it excludes the untimed gaps
between operations, so generation and maintenance never inflate a rate. With
three consumers it is wrong by roughly 3×, because it sums windows that
overlapped in wall time as though they had been taken in sequence.

So the phase result splits into two numbers that stop pretending to be one:

- **throughput** = `count / wall_elapsed(phase)` — the only form that survives
  concurrency, and the number a comparison quotes;
- **latency** = p50 / p95 / p99 / max over the per-operation windows, pooled
  across consumers — unchanged in meaning, and still the column that exposes
  checkpoint and settle pauses, which is why 0060 keeps every sample rather
  than bucketing.

Under `N = 1` the two agree to within the untimed gap, and reporting both is
what makes that gap visible instead of assumed. Every existing recorded row was
measured at `N = 1`; the change does not retro-falsify them, but it does mean
throughput and latency stop being derivable from each other, which they never
honestly were.

### 5. Datasets: tiers, and a hash you control

`benches/nix/params.nix` gains explicit tiers, each with its own revision:

```nix
tiers = {
  smoke = { rows =     1000; rev = 1; };  # correctness of the harness
  small = { rows =   200000; rev = 1; };  # today's size
  large = { rows =  5000000; rev = 1; };  # the working size
  huge  = { rows = 50000000; rev = 1; };  # exceeds RAM under the cage
};
```

One dataset serves every row at its tier. In particular **one WaveDB seed
serves all four WaveDB rows** — `(durable | relaxed) × (single | multi)` differ
in how they are *driven*, never in what they are driven against — and each row
materialises its own writable copy with `cp --reflink=auto` (near-free on
btrfs), so a row may mutate or destroy its copy freely while the store path
stays pristine.

**The rebuild is yours to trigger.** Today it is not: `benches/nix/gen.nix`
builds `bench-gen` with `src = repoSrc`, the whole repository, so editing any
file — `benches/src/systems/mysql.rs`, a crate README — rebuilds `bench-gen`
and invalidates **every seed** downstream. At the `huge` tier that discards
hours. Two changes fix it in the two directions that matter:

- `bench-gen`'s source is **filtered** to what it actually compiles from
  (`benches/src/{schema,seed}.rs`, `benches/src/bin/bench-gen.rs`,
  `benches/Cargo.{toml,lock}`, and the WaveDB crates it links). Unrelated edits
  stop invalidating anything.
- each tier's `rev` is a declared input. Bumping the integer — **and nothing
  else** — forces that tier's derivations to rebuild.

Stable by default, forced on demand. The pairing is deliberate: filtering alone
would still rebuild on a genuine `schema.rs` change, which is correct in
principle and unaffordable in practice at 50 million rows; the revision is the
override that keeps the decision human.

`nix build --out-link .bench-seeds/<name>` (gitignored) pins them as GC roots,
as 0060 §6 already requires.

### 6. The fill profile: uncaged, and as much RAM as we can give it

Filling `huge` through an engine whose default is one barrier per batch is the
suite's largest single cost — at the ~228 inserts/s the small tier measured,
50 million rows is days. But **a fill is a build, not a measurement**, and the
constraints that make a measurement honest do not apply to it:

- it runs **outside the cage** (it is a derivation; `nix-daemon` sandboxes it,
  and no timing is recorded from it);
- it runs at the maximum relax window
  ([0061](0061-relaxed-durability-window.md)) — already the case via
  `benches/src/lib.rs`'s `FILL_WINDOW`;
- and it gets **both** in-memory layers turned up, which is the part that does
  not exist yet.

WaveDB holds two distinct caches, and only one of them is reachable today:

| layer | what it holds | where | today |
|---|---|---|---|
| record cache | decoded records, per `StructStorage` slot | `settle.rs:150` `evict_settled(budget)` | caller-driven; the bench passes 96 MB, a fill that never calls it grows unbounded |
| page cache | whole block runs, still compressed, `Arc<[u8]>` | `page_cache.rs:49`, built at `block_file.rs:142,153` | **hardcoded** `DEFAULT_BUDGET_BYTES = 64 MiB`; not reachable from `StoreOptions` |

So `StoreOptions` gains the two budgets:

```rust
pub struct StoreOptions {
    pub relax_window: Duration,
    pub page_cache_bytes: usize,    // was: 64 MiB, hardcoded in block_file.rs
    pub record_cache_bytes: usize,  // was: whatever the caller remembered to evict to
}
```

Two properties make this a safe change rather than a knob to be suspicious of.
Neither field **ever reaches stored bytes**, so neither folds into the
`STRUCT_HASH` — the rule is "behaviour that never reaches disk doesn't fold",
and a cache budget is the cleanest possible example. And `Default` keeps
today's numbers exactly, so every existing caller is unchanged.

The fill then runs at roughly:

| knob | measured row | fill |
|---|--:|--:|
| `relax_window` | `0` (durable) / window (relaxed) | `FILL_WINDOW` |
| `page_cache_bytes` | 64 MiB | ~6 GiB |
| `record_cache_bytes` | 96 MiB | ~4 GiB |
| cage | 500 MB · 4 cpu | none |

Both numbers live in `params.nix` and sum to a declared total, because ~10 GiB
on a 16.6 GiB machine with a desktop running is tight enough that it should be
one edit to change, not a hunt through builders.

`record_cache_bytes` also closes a real hazard rather than only buying speed: a
fill that simply never evicts is not "using memory well", it is unbounded, and
the difference between 10 GiB and OOM is currently nothing but the row count.

### 7. The corpus: three tables, and WaveDB against itself

Because rows are addressable, the report stops being a transcript of a run and
becomes three queries over the corpus. All three filter on
`host_key + cage_revision + tier + workload + consumers`, so nothing
incomparable is ever placed in one table.

**A — the variant table** (one commit, WaveDB only). `single` vs `multi` ×
`durable` vs `relaxed`, four rows. What the sharding and the window each buy,
in isolation.

**B — the evolution table** (one variant, many commits). One line per recorded
WaveDB commit, oldest first, with the delta against the previous. This is the
table the whole restructure is for: it is where a regression shows up as a
number next to the SHA that caused it, and it is only possible because a row
survives the run that produced it.

**C — the comparison table** (one commit, everybody). WaveDB's rows beside the
peers', peers served from the corpus without being remeasured. Embedded and
server brackets stay separate, as 0060 §2 requires — a server row carries a
socket round trip inside its timed window that no embedded row pays.

`benches/results/index.md` is regenerated from the corpus rather than appended
to. 0060 rejected regeneration ("churn in the diff, and a rewritten past"), and
that rejection was right *when the file was the record*. It no longer is: the
rows are the record, `index.md` is a rendering, and a rendering that cannot be
rebuilt from its source is a liability. The past is not rewritten, because the
row files are append-only and immutable.

### 8. Read counters

The suite records `bytes_written` per phase (`metrics.rs`, and
`server.rs::write_bytes` summing the process tree, since PostgreSQL is
process-per-connection). It records **no read counter at all**, and that gap
has already cost an investigation: a `mongod` observed reading ~1 TB against a
27.5 MB dataset could not be classified from the corpus, because the corpus had
nothing to say about reads.

Both counters are added in the same two functions, and both forms are kept:

- `read_bytes` — what reached the block layer;
- `rchar` — what the process asked for, page-cache hits included.

Their **ratio is the finding**. `read_bytes ≈ rchar` is genuine thrashing —
under a 500 MB cgroup that also bounds page cache, the plausible failure. `
read_bytes ≪ rchar` is a working cache and costs no IOps. A `read amp` column
joins `amp` in the tables. This is the smallest change in the RFC and probably
the highest information per line of it.

### 9. Carried over from 0060 unchanged

Stated explicitly so the deprecation does not read as a repudiation:

- **The cage recipe** — `systemd-run` for the memory bound (page cache
  included), `taskset` for the CPU budget (`cpuset` is not delegated to user
  scopes), `bwrap` for the PID namespace and nothing else; `--dev-bind / /` so
  the databases write to a real disk. Now applied per row rather than per run.
- **Pinned server caches** — 256 MB each, MongoDB's floor setting the number
  for all three, because each server otherwise sizes its cache from the
  machine's RAM rather than the cgroup's.
- **The cage is part of the lane identity**, and a run may not record outside
  it (`--force` marks the record). The recipe now carries an explicit
  `revision` (`benches/nix/cage.nix`) that is a field of every row's digest,
  so changing the cgroup, affinity or namespace retires the rows measured
  under the old one instead of silently comparing across them.
- **Footprint at four points** — baseline / hot / settled / compacted — with
  payload split from log capacity, and compression stated per system.
- **Seeds as derivations**, with clean shutdown inside the builder, reflink
  materialisation reported outside the measurement window, and the `mongod`
  tcmalloc `/sys` workaround.
- **Why MongoDB is the reference peer**, and why FerretDB is not.
- **Durability as a row dimension**, whole-record updates on every system, the
  typed WaveDB path, and the retention annotation beside every update column.
- **No headline number, no CI gate, no fairness certificate.**
- **The predictions of 0060 §9** stand unamended and unre-scored; the
  concurrency prediction ("the gap should open with the client count") is the
  one this RFC finally makes testable.

## Alternatives

- **Keep the run as the unit and add a peer-result cache beside it.** Rejected:
  the cache key would have to be derived from a run record that mixes every
  system's inputs together, so it would either over-invalidate (and buy
  nothing) or under-invalidate (and serve a MySQL row measured under a
  different cage). The identity has to live on the row because the inputs do.
- **Peer results as Nix derivations.** Rejected — see §1. A duration measured
  inside `nix-daemon`'s sandbox was not measured in this suite's cage, and
  presenting it as reproducible because it came from a derivation is worse than
  not caching at all.
- **A thread pool over one shared queue instead of key-partitioned
  consumers.** Rejected: `ShardStore` remembers absence, which is only sound
  while a record has exactly one holder. A shared queue would be measuring an
  engine configuration that is not merely untuned but incorrect.
- **`N = 4` consumers (one per CPU), with the generator sharing a core.**
  Rejected: it makes the generator's scheduling latency part of every timed
  window, and the phase then measures the harness under contention.
- **Drop the `shop` workload to halve the matrix.** Rejected explicitly by
  decision: `shop` is the workload with a natural partition (one `Product`
  collection per `Shopping`) and therefore the one where the multi-consumer row
  is meaningful rather than synthetic.
- **Generate the whole dataset into RAM before each phase, instead of a
  generator thread.** Rejected: ~25 MB at the small tier is affordable and
  ~600 MB at `large` is not — it would not fit the 500 MB budget, so the
  technique does not survive the sizes the suite exists to reach.
- **Let the fill run inside the cage for consistency.** Rejected on evidence:
  0060 measured a fill as *faster* at 500 MB than at 10 G, because a fill is
  write-bound and extra RAM only defers writeback into one cliff. But that
  finding was taken with a 64 MiB page cache and a caller-driven record cache;
  §6 changes both, and the fill is a build either way. The honest statement is
  that the fill's configuration is chosen for build throughput and is
  deliberately **not** a measured configuration.

## A measurement bug this restructure found

RFC 0060.s update phase drew keys uniformly **with replacement** and built each
new value as `thing_v2(n, seed)` — a pure function of the row and the seed. So
the second update of a row wrote byte-identical values, and InnoDB turns an
all-columns-unchanged `UPDATE` into a no-op. At 50 000 updates over 100 000
rows about **21%** of MySQL.s update phase was work it skipped and its peers
did, and the recorded `mysql` update numbers are flattered by exactly that
much.

It surfaced because the RFC 0065 driver checks that an update touched a row,
where the old adapter did not look. The fix is in the generator: values are
salted by the draw ordinal (`schema::thing_v2_at`), so every update is a real
write on every system. Update columns measured before and after this change
are not comparable, which is one more thing `dataset_revision` exists to say.

## Open questions

1. **How long does `huge` actually take to fill, and does it fit a derivation
   at all?** This is 0060's open question 4, unanswered and now load-bearing.
   §6's caches should move it by a large factor; if a `huge` WaveDB fill still
   runs into days, the tier needs a build-outside-and-import path rather than a
   builder.
2. **Does `page_cache_bytes` at ~6 GiB actually help a fill?** The page cache
   holds *settled* images and a fill is almost pure write; the record cache is
   the layer that should dominate. Worth measuring at `small` before either
   number is cast into `params.nix`.
3. **What is the right `record_cache_bytes` for the measured rows?** The bench
   currently passes 96 MB against a 500 MB cage. Once it is a `StoreOptions`
   field rather than a call site, the question "is 96 the right number" becomes
   answerable by sweeping it — and a sweep produces rows, which is exactly what
   §1 is for.
4. **Should the corpus be pruned?** Table B grows one row per commit per
   configuration forever. Probably not for a long time (a row file is ~2 KB),
   but the answer should be "no, and here is the arithmetic" rather than
   silence.
5. **Is one sharded workload enough?** `shop` is now the only workload that
   can produce a `wavedb/multi` row (§3), so the sharded engine is priced
   against exactly one access pattern — a realistic one, but one. A second
   multi-collection workload would triangulate; whether that is worth a third
   adapter set is not decided here.
6. **The off-standard `8c-15835m` lane** (uncaged, 8 CPUs, 16.6 GB, `--force`)
   is now irreproducible and holds no peer numbers. Keep it as history or drop
   it? Not decided here.

## Phasing

| Phase | Content | Priority |
|---|---|---|
| **1** | The split: `bench` supervisor + `bench-row`, one cage per row, row identity and the `results/rows/` corpus. Peers reused. Existing adapters unchanged, `N = 1` throughout. This alone delivers the adoption win. | **first** |
| **2** | The producer/consumer harness, `N = 3`, the generator-as-router, and the throughput/latency split in `metrics.rs`. The first genuine `wavedb/multi` row. | **first** |
| **3** | `StoreOptions.{page_cache_bytes, record_cache_bytes}`, the filtered `bench-gen` source, tier revisions, and the `large` tier built. | high |
| **4** | Read counters (`read_bytes` + `rchar`) and the `read amp` column. Small; can land with any phase. | high |
| **5** | The `huge` tier, gated on open question 1. | later |
| **6** | The three rendered tables, with B (evolution) as the headline. Needs a few commits' worth of rows to be worth looking at. | after 1–3 |
