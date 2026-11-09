# Benchmark results

Append-only (RFC 0060 §7). One section per recorded run — the table it printed,
under the machine it ran on; the JSON record beside it carries the full
configuration. **Rows are comparable only within one host key**: a different
machine is a different lane, never a trend line, and every heading names its
lane for that reason.

`update` columns: WaveDB retains every superseded version and the others retain
none, so read them beside `payload` and `amp`.

The entries below the header and above the first `##` heading are older runs in
the previous one-line-per-run format. They are left exactly as they were
recorded: the format changed, the history did not.

- `2026-08-22T22-30Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-500m-btrfs-e636 · `3e171bf` **(dirty tree)** — wavedb/durable: insert 221/s, read_cold 774/s, update 140/s, 6.81× space; wavedb/relaxed: insert 1053/s, read_cold 775/s, update 327/s, 7.07× space
- `2026-08-23T02-50Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-500m-btrfs-e636 · `3e171bf` **(dirty tree)** — wavedb/durable: insert 206/s, read_cold 778/s, update 155/s, 6.57× space; wavedb/relaxed: insert 984/s, read_cold 778/s, update 326/s, 6.35× space; wavedb-sharded/durable: insert 230/s, read_cold 919/s, update 156/s, 6.58× space; wavedb-sharded/relaxed: insert 726/s, read_cold 917/s, update 296/s, 6.59× space

## `2026-10-17T09-26Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-104a · `4590276`

`micro`, `small` tier (200 000 records, 50.8 MB logical), 1 consumer, caged.
WaveDB only (`--only wavedb`); the four `shop` rows were OOM-killed in the
cage (exit 137) and recorded nothing. Rows `3b043ed6c9d2ee70`, `5c0ceeb0e5609510`.

| system | durability | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb | durable | 101.6 | 1 648.8 | 1 673.1 | 72.5 | 75.0 ms | 272.9 | 262.2 | 48 819.1 | 28.9 |
| wavedb | relaxed | 921.9 | 1 647.0 | 1 689.9 | 263.4 | 1.3 ms | 7.5 | 27.8 | 2 949.3 | 28.1 |

## `2026-10-17T16-21Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-104a · `7adaa11` **(dirty tree)**

`micro`, `small` tier (200 000 records, 50.8 MB logical), 1 consumer, caged,
every system. The tree was dirty by one storage test file, which no bench
builds. Every WaveDB row here is one store (`PageStore::open`): one data
shard writing its own files, so one storage shard (RFC 0073). `peak` is the
payload at the end of the load (`hot`), `compacted` after the row's own
compaction; `log` is preallocated recovery capacity, apart on purpose.

Embedded:

| system | durability | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb | durable | 87.6 | 1 661.9 | 1 701.3 | 64.8 | 81.2 ms | 305.5 | 276.4 | 55 641.2 | 28.8 | 0 |
| wavedb | relaxed | 907.8 | 1 666.9 | 1 674.4 | 254.3 | 1.0 ms | 7.4 | 28.1 | 2 954.6 | 27.7 | 0 |
| sqlite | durable | 267.2 | 107 808.9 | 109 675.4 | 249.9 | 68.7 ms | 92.0 | 98.8 | 68.2 | 56.8 | 4.2 |
| sqlite | relaxed | 5 367.1 | 108 754.2 | 107 525.0 | 3 366.3 | 0.1 ms | 12.3 | 19.7 | 68.2 | 56.8 | 4.2 |

Server:

| system | durability | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| postgres | durable | 258.5 | 21 530.6 | 20 420.8 | 269.1 | 72.1 ms | 80.8 | 81.6 | 194.6 | 190.4 | 100.7 |
| postgres | relaxed | 17 569.1 | 20 611.7 | 19 377.1 | 14 840.6 | 0.1 ms | 1.4 | 0.7 | 211.1 | 240.6 | 151.0 |
| mysql | durable | 266.0 | 13 867.4 | 11 121.1 | 245.6 | 66.8 ms | 83.4 | 125.2 | 290.0 | 280.8 | 155.2 |
| mysql | relaxed | 7 464.1 | 13 824.4 | 11 942.2 | 4 707.2 | 0.2 ms | 2.3 | 9.6 | 290.0 | 280.8 | 155.2 |
| mongodb | durable | 263.5 | 5 423.4 | 5 304.4 | 250.2 | 66.4 ms | 78.9 | 81.5 | 262.8 | 242.0 | 210.2 |
| mongodb | relaxed | 5 321.3 | 5 423.3 | 5 277.0 | 3 904.9 | 0.3 ms | 0.6 | 1.4 | 258.0 | 258.1 | 209.9 |
| valkey | durable | 392.7 | 39 365.0 | 39 500.2 | 486.2 | 48.7 ms | 80.7 | 82.0 | 113.2 | 45.4 | 0 |
| valkey | relaxed | 18 518.2 | 39 368.6 | 39 156.3 | 12 572.7 | 0.1 ms | 0.4 | 0.5 | 113.2 | 45.4 | 0 |

Rows `6f188bef81bf2229`, `868d0b03b49e9e49` (wavedb), `b7e1fe26d6b4a968`,
`988c76e18478bb4c` (sqlite), `3c22146ed4065ace`, `4b3bc88b27967abe`
(postgres), `7aa21981b1df61d4`, `db1d919c8c5fe638` (mysql),
`899ba828e288f8d7`, `cd4b7d2ceadce067` (mongodb), `8f2fd54af265036e`,
`010d54c97ac3ebde` (valkey).

## `2026-10-18T06-54Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-104a · `e63a9d0`

`micro`, `small` tier (200 000 records, 50.8 MB logical), 1 consumer, caged,
WaveDB only (`--only wavedb`), clock pinned at 2.3 GHz like the `7adaa11` run
above, so the two compare row for row. The first run with RFC 0074
(extendible hashing: a page over target splits in the round that rewrites
it) and RFC 0075 phases 1–4 (every secondary and fuzzy index a chain, the
B+tree deleted). One store, so one storage shard (RFC 0073).

| system | durability | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb | durable | 94.3 | 1 621.8 | 1 622.1 | 71.3 | 77.7 ms | 201.6 | 240.2 | 33 393.9 | 25.5 | 0 |
| wavedb | relaxed | 1 031.1 | 1 631.2 | 1 654.7 | 302.7 | 1.0 ms | 5.4 | 23.2 | 2 303.1 | 24.8 | 0 |

Against `7adaa11`, durable: insert +7.7%, update +10.0%, **KiB written per
insert 305.5 → 201.6 (−34%)**, per update 276.4 → 240.2 (−13%), peak
55 641.2 → 33 393.9 MB (−40%), compacted 28.8 → 25.5 MB; reads −2.4%.
Relaxed: insert +13.6%, update +19.0%, KiB per insert 7.4 → 5.4, peak
2 954.6 → 2 303.1 MB. A durable insert is still one fsync, so its rate moves
far less than its bytes.

What the bytes are, from the settle-round probe (a `probe` build, which
records no row; `micro` at the `smoke` tier, durable inserts, KiB written
per round = per insert, by thousands of records): before RFC 0074 (`0a87229`)
a round grew with the database, 127.6 KiB at 0–10 k to 265.7 at 90–100 k,
the B+tree's share alone 37.5 → 158.8 and the recency chain's 21 → 57; now
it holds flat at 110–126 KiB, the secondary chain 21–25 KiB of it and the
recency chain 20–29. The fixed 32 KiB header is now about a quarter of every
round, and the largest single cost left.

Rows `a34996ed82d3e90f`, `13fdaca239500caf`.

## `2026-10-18T17-30Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-104a · `1fd1c9f`

`micro`, `small` tier (200 000 records, 50.8 MB logical), caged, WaveDB only
(`--only wavedb`), clock pinned at 2.3 GHz like the `e63a9d0` run above.
The first run with the bulk wire codec (`7ee2042`: a `Vec<u8>` and a vector of
fixed-width scalars encode and decode as one copy, not per element), the
compacting record cache (`7f03357`, RFC 0068 amendment: eviction rewrites
settled chunks that are mostly superseded copies before it drops any), and
the first `wavedb/multi` micro rows (`1fd1c9f`, RFC 0065 revision): three
consumers, each its own thread owning one data shard and one `Thing`
collection on it, row `n` consumer `n % 3`'s, every data shard writing
through one storage shard. `wavedb/single` is the row that compares across
commits; `multi` is the same dataset split by owner, so its rates are the
three threads together.

| system | durability | consumers | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb/single | durable | 1 | 110.4 | 185 914.4 | 21 413.6 | 87.2 | 76.0 ms | 202.7 | 241.3 | 33 569.4 | 25.5 | 0 |
| wavedb/single | relaxed | 1 | 1 829.5 | 21 381.9 | 23 815.3 | 817.1 | 0.6 ms | 3.8 | 12.6 | 1 420.7 | 24.6 | 0 |
| wavedb/multi | durable | 3 | 208.2 | 664 070.8 | 85 018.4 | 163.6 | 82.8 ms | 187.6 | 217.0 | 9 296.6 | — | 0 |
| wavedb/multi | relaxed | 3 | — | — | — | — | — | — | — | — | — | — |

`wavedb/multi relaxed` was OOM-killed in the cage (exit 137) and recorded
nothing. Each of its three stores opens with the default caches, a 64 MiB
page cache and a 96 MiB record cache, so the budgets alone are 480 MiB of the
cage's 500 MiB; a relaxed store also holds its unsettled span, which the
record cache cannot evict, and the shared storage thread holds each window
in RAM until it lands. The durable row survives because its unsettled span
is near zero. The `multi` rows take no compacted footprint (never seeded,
RFC 0065), so `peak` is the settled one.

Against `e63a9d0`, `wavedb/single` durable: **read_hot 1 621.8 → 185 914.4
(×115, p50 1.06 µs)**, read_cold 1 622.1 → 21 413.6 (×13), insert +17%,
update +22%; bytes written, peak and compacted footprint unchanged. Relaxed:
read_hot 1 631.2 → 21 381.9 (×13), read_cold 1 654.7 → 23 815.3 (×14),
**insert +77%, update ×2.7**, KiB per insert 5.4 → 3.8 and per update
23.2 → 12.6, peak 2 303.1 → 1 420.7 MB (−38%).

Where it comes from, from the read-path probe (a `probe` build, which records
no row; `micro` relaxed at this tier, in the cage). Before, a hot read was
605 µs. The record cache answered 0.8% of reads, because its 96 MiB were
almost all dead copies of chain segments and index nodes, rewritten whole by
every write, and eviction dropped whole chunks, records and all. Every miss
decoded a page in 1.39 ms, 1.1 ms of it WaveWire walking the page's
`Vec<u8>` records one byte at a time. The bulk codec cut the decode to
0.34 ms, of which zstd is now 0.27 ms. Exact capacities also let the 64 MiB
page cache hold 467 pages instead of 297, so its hit rate went from 58% to
90%. Compaction is what lifts the durable read_hot: each durable write
settles at once, so nearly the whole log is compactable and the records stay
in the cache. On the relaxed row the 1 s window keeps up to ~70 MB of the
96 MiB unsettled, which compaction cannot touch (RFC 0068 amendment), so its
read_hot is the page path, level with read_cold. The relaxed write gain is
the codec on the settle side (every rewritten page encodes its records). A
faster insert also likely carries more records per 1 s window, so each
round's segment rewrites spread over more of them; that is plausible but not
measured.

`multi` durable against `single` durable: insert ×1.89 and update ×1.88 on
three threads, each fsync-bound on its own batches; read_hot ×3.6 and
read_cold ×4.0, more than the thread count, since each store's caches hold a
third of the dataset. Bytes written per insert are close (187.6 against
202.7 KiB), yet the end-of-load footprint is 9.3 GB against 33.6 GB. That gap
is not yet explained.

Rows `20bdf2d53b48a0c3`, `1c569a761bf08be3`, `94408997a9d9ae54`.

## `2026-10-18T20-29Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-104a · `da2ce57`

`shop`, `small` tier (200 000 users preloaded, each a tenant with their orders
and line items), caged, every system, clock pinned at 2.3 GHz. Each row
preloads, untimed, then times 100 signups, 200 checkouts, 10 000 profile
reads, 1 000 order pages and 1 000 order details. `peak` is the footprint at
the end of the load, `compacted` after the row's own compaction.

| system | durability | signup/s | checkout/s | profile/s | order_page/s | order_detail/s | checkout p99 | peak MB | compacted MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb | durable | — | — | — | — | — | — | — | — |
| wavedb | relaxed | — | — | — | — | — | — | — | — |
| wavedb/multi | durable | — | — | — | — | — | — | — | — |
| wavedb/multi | relaxed | — | — | — | — | — | — | — | — |
| sqlite | durable | 564.9 | 565.5 | 148 591.3 | 21 241.4 | 22 144.2 | 2.3 ms | 46.7 | 44.1 |
| sqlite | relaxed | 1 317.3 | 9 816.3 | 147 368.4 | 24 120.8 | 21 738.3 | 0.2 ms | 46.7 | 44.1 |
| postgres | durable | 520.1 | 488.8 | 20 351.2 | 5 314.3 | 4 054.2 | 3.2 ms | 373.1 | 355.1 |
| postgres | relaxed | 7 794.9 | 3 843.1 | 20 850.4 | 5 322.2 | 4 010.9 | 0.5 ms | 373.1 | 355.1 |
| mysql | durable | 228.8 | 163.8 | 13 852.4 | 3 583.9 | 2 180.7 | 59.2 ms | 359.0 | 345.6 |
| mysql | relaxed | 8 854.3 | 2 528.9 | 13 459.3 | 2 270.2 | 2 249.2 | 0.6 ms | 359.0 | 345.6 |
| mongodb | durable | 194.5 | 141.0 | 4 423.7 | 1 293.7 | 429.5 | 66.7 ms | 337.2 | 337.3 |
| mongodb | relaxed | 1 606.1 | 464.3 | 4 336.7 | 1 308.8 | 564.5 | 2.8 ms | 337.1 | 337.3 |
| valkey | durable | 579.2 | 544.1 | 33 855.5 | 12 615.6 | 10 280.9 | 2.2 ms | 177.6 | 97.8 |
| valkey | relaxed | 23 628.4 | 11 342.8 | 33 978.2 | 12 586.4 | 10 224.5 | 0.2 ms | 177.6 | 97.8 |

All four WaveDB rows were OOM-killed in the cage (exit 137), 12–16 s in,
still in the untimed preload, durable and relaxed alike. They recorded
nothing, as in the `4590276` run. A heaptrack profile of the
`wavedb/single relaxed` row run outside the cage puts the heap peak at
788.8 MB, in the preload's last `drain()`:

- 474 MB in one settle round's plan:
  - every page the round touches decoded at once (`plan::staged_page`,
    222 MB);
  - their images (64 MB);
  - the rest of the round.
- 230 MB in the record cache, which the fill evicts to 192 MiB.
- 84.5 MB in the assembled window.

`drain()` steps the settle with an unbounded round (`usize::MAX` ids), so a
round is everything unsettled, up to the fill's 64 MiB. A plan holds every
page it stages until it has applied the whole round, because each image must
bind one dictionary state and splits are decided last. A round's memory is
therefore the number of distinct pages it touches. A node's 4 096-id bound
does not cap it either, since 4 096 ids spread over a nesting shape can
touch as many pages.

Rows `881aa4504b0ab49a`, `80a3b4fbaa2a71be`, `b2038921c693c8f0`,
`bbcfb8007bf8d0a0`, `fc50563a10570306`, `e298462eaa825aea`,
`5bd33ad5241e1e27`, `4fe8309ca0fa7af7`, `17654b00eb0c4610`,
`a2b3d67de3091ec0`.

## `2026-10-23T19-07Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-cee3 · `5fc77d4`

`micro`, `small` tier (200 000 records, 50.8 MB logical), caged, WaveDB only,
clock pinned at 2.3 GHz. The lane key differs from the `104a` runs above. Its
hash covers CPU, cores, CPU and memory budgets, total memory, kernel,
filesystem, disk class, virtualisation and clock. The code computing it is
unchanged, the CPU, clock, cage and kernel (6.18.38) are the same, and the
NixOS system was reconfigured on 2026-10-19 and rebooted since. Total memory
is the likely field; this is unconfirmed. The comparison below with `1fd1c9f`
is therefore across lanes, on the same hardware.

What changed (`da2ce57`, `5fc77d4`):

- The bench drivers evict to the store's own record budget, a data shard's
  share, instead of a 96 MiB constant per consumer.
- They act only after a write: a durable write evicts, and a relaxed write
  settles and evicts on its window or once `PageStore::under_pressure()`
  says the unsettled bytes passed half the record budget (RFC 0068,
  amendment 2026-10-23).
- Before, they evicted after every operation, reads included, each time
  summing every chunk of the record cache.

| system | durability | consumers | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb/single | durable | 1 | 115.2 | 616 707.2 | 21 963.0 | 87.5 | 74.4 ms | 201.2 | 238.8 | 33 191.5 | 25.5 | 0 |
| wavedb/single | relaxed | 1 | 1 376.0 | 52 674.0 | 23 846.4 | 671.0 | 0.6 ms | 7.5 | 18.7 | 2 477.5 | 24.5 | 0 |
| wavedb/multi | durable | 3 | 208.5 | 2 102 350.8 | 101 423.8 | 165.7 | 81.0 ms | 188.1 | 215.4 | 9 405.3 | — | 0 |
| wavedb/multi | relaxed | 3 | 5 463.5 | 204 311.3 | 107 096.9 | 2 379.1 | 0.4 ms | 5.0 | 14.3 | 1 705.7 | — | 0 |

`wavedb/multi relaxed`, OOM-killed on `1fd1c9f`, now completes. Its three
stores together evicted to 288 MiB of records before the budget fix, and to
96 MiB after it.

**The per-read eviction was most of a hot read.** On `wavedb/single`
durable, read_hot went from 185 914.4 to **616 707.2/s (×3.3)**. Its wall time
per read fell from 5.38 to 1.62 µs, while the timed read stayed at ~0.9 µs.
About 3.8 µs of every read was the driver's eviction walk. The ~0.7 µs left
outside the timed window is the generator-to-consumer hand-off.
`wavedb/multi` durable reads **2 102 350.8/s** on three threads, against
664 070.8. Writes are unchanged on the durable rows: insert 110.4 → 115.2,
update 87.2 → 87.5, multi 208.2 → 208.5.

**Pressure helps relaxed reads and costs relaxed writes, as built.**

Reads: `wavedb/single` relaxed read_hot went from 21 381.9 to **52 674.0/s
(×2.5)**. Settling at half the budget leaves the other half settled,
which compaction can keep.

Writes: insert fell 1 829.5 → 1 376.0 (−25%) and update 817.1 → 671.0
(−18%). Bytes written rose, 3.8 → 7.5 KiB per insert and 12.6 → 18.7 per
update, and peak rose 1 420.7 → 2 477.5 MB. Settles are more frequent and
smaller, and each one is the driver's `drain()`, which waits for its barrier
on the consumer thread and is counted in wall time. A relaxed insert's
wall time is 727 µs against 367 µs timed. A node settles with
`settle_step` and takes the barrier on a helper (`use_barrier`), never
waiting. The relaxed driver should do the same: settle without awaiting
the barrier.

Rows `873ba15255870fa9`, `a78ee165b302c1c9`, `974caf2ecc391932`,
`d67b6af9a022d936`.

## `2026-10-31T22-14Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-cee3 · `404a19b`

`micro`, `small` tier (200 000 records, 50.8 MB logical), caged, WaveDB only,
clock pinned at 2.3 GHz, same lane as the `5fc77d4` run above. Clean tree, but
every row records `forced`: the run bypassed a bench precondition, so read it
as provisional. The first run with the RFC 0078 engine (quantized page index,
ordered lanes, folding, regrouping and merging in compaction), the bounded
drain (`778f502e`) and the shop preload fix (`404a19b`).

| system | durability | consumers | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb/single | durable | 1 | 63.7 | 328 489.8 | 105 630.9 | 98.3 | 80.2 ms | 190.3 | 185.4 | 28 525.0 | 24.5 | 0 |
| wavedb/single | relaxed | 1 | 185.8 | 464 105.0 | 231 525.0 | 503.7 | 4.0 ms | 1.4 | 15.8 | 1 050.7 | 36.7 | 0 |
| wavedb/multi | durable | 3 | 159.0 | 903 727.2 | 314 403.1 | 157.1 | 85.2 ms | 186.2 | 179.3 | 6 697.4 | — | 0 |
| wavedb/multi | relaxed | 3 | 820.8 | 1 571 846.3 | 760 263.1 | 805.6 | 4.2 ms | 1.0 | 14.3 | 915.7 | — | 0 |

Against `5fc77d4`:

- **Cold reads**: ×4.8 (single durable, 21 963.0 → 105 630.9), ×9.7 (single
  relaxed, 23 846.4 → 231 525.0), ×3.1 (multi durable) and ×7.1 (multi
  relaxed).
- **Bytes written**: per update 238.8 → 185.4 KiB (single durable, −22%) and
  215.4 → 179.3 (multi durable, −17%); per insert 201.2 → 190.3 (−5%) and
  188.1 → 186.2. Relaxed per insert 7.5 → 1.4 (single) and 5.0 → 1.0 (multi).
- **Peak footprint**: single durable 33 191.5 → 28 525.0 MB, multi durable
  9 405.3 → 6 697.4, single relaxed 2 477.5 → 1 050.7, multi relaxed
  1 705.7 → 915.7.
- **Compacted footprint**: single durable 25.5 → 24.5 MB, single relaxed
  24.5 → 36.7 MB (larger).
- **Hot reads** are mixed: single relaxed 52 674.0 → 464 105.0 and multi
  relaxed 204 311.3 → 1 571 846.3, but single durable 616 707.2 → 328 489.8
  and multi durable 2 102 350.8 → 903 727.2.
- **Insert rates fell** in all four rows: single durable 115.2 → 63.7
  (−45%), single relaxed 1 376.0 → 185.8 (−87%), multi durable 208.5 → 159.0
  (−24%), multi relaxed 5 463.5 → 820.8 (−85%). Updates: single durable
  87.5 → 98.3 (+12%), single relaxed 671.0 → 503.7 (−25%), multi durable
  165.7 → 157.1, multi relaxed 2 379.1 → 805.6 (−66%). The cause is not
  measured here; the relaxed insert drop is the first thing to profile.

The `multi` rows take no compacted footprint (never seeded, RFC 0065).

Rows `ff5ea958a153ece6`, `8d824e079bbc2c06`, `60130dccb750ffc1`,
`6706afb63db423b5`.

## `2026-11-08T03-45Z` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-btrfs-afab · `32f83e5` **(dirty tree)**

`micro` and `shop`, `small` tier, caged, WaveDB only, clock pinned at 2.3 GHz,
not forced. The tree was dirty only by the run's own log file, untracked in
the repository root; the code is `32f83e5`. The first run with the lane-walk
fix (`af53801d`) and RFC 0079 phases 1–4 (headers sized to their change).

The host fingerprint changed since the `404a19b` run, so this is the `-afab`
lane, not `-cee3`: same CPU, clock and cage. The comparisons below cross
the two lanes and are indicative, not a trend line.

| system | durability | consumers | insert/s | read_hot/s | read_cold/s | update/s | insert p99 | KiB written/insert | KiB written/update | peak MB | compacted MB | log MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb/single | durable | 1 | 129.0 | 354 807.5 | 141 817.3 | 142.4 | 70.6 ms | 159.1 | 163.6 | 20 721.2 | 33.1 | 0 |
| wavedb/single | relaxed | 1 | 11 609.1 | 551 705.1 | 223 936.8 | 2 654.4 | 0.2 ms | 0.1 | 3.0 | 174.1 | 17.9 | 0 |
| wavedb/multi | durable | 3 | 232.3 | 1 215 736.7 | 428 672.8 | 217.3 | 81.8 ms | 156.7 | 155.5 | 9 979.6 | — | 0 |
| wavedb/multi | relaxed | 3 | 41 078.0 | 1 865 164.2 | 715 342.9 | 8 473.3 | 0.1 ms | 0.1 | 1.4 | 87.0 | — | 0 |

Against `404a19b`:

- **Bytes written, durable** (RFC 0079): per insert 190.3 → 159.1 KiB
  (single, −16%) and 186.2 → 156.7 (multi, −16%); per update 185.4 → 163.6
  (−12%) and 179.3 → 155.5 (−13%). About 30 KiB a durable round, the 32 KiB
  header become one block plus its share of restating headers.
- **Bytes written, relaxed**: per insert 1.4 → 0.1 KiB (single) and 1.0 →
  0.1 (multi); per update 15.8 → 3.0 and 14.3 → 1.4.
- **Insert rates**: single durable 63.7 → 129.0 (×2.0), multi durable
  159.0 → 232.3 (+46%), single relaxed 185.8 → 11 609.1 (×62), multi
  relaxed 820.8 → 41 078.0 (×50). The relaxed regression of the `404a19b`
  run is gone: it was the lane walk (`af53801d`).
- **Updates**: single durable 98.3 → 142.4 (+45%), multi durable 157.1 →
  217.3 (+38%), single relaxed 503.7 → 2 654.4 (×5.3), multi relaxed
  805.6 → 8 473.3 (×10.5).
- **Reads**: cold +34% (single durable), −3% (single relaxed), +36% (multi
  durable), −6% (multi relaxed); hot +8%, +19%, +35%, +19%.
- **Peak footprint**: single durable 28 525.0 → 20 721.2 MB, multi durable
  6 697.4 → 9 979.6 (larger), single relaxed 1 050.7 → 174.1, multi relaxed
  915.7 → 87.0. Compacted: single durable 24.5 → 33.1 MB (larger), single
  relaxed 36.7 → 17.9.

The first WaveDB `shop` rows recorded: every earlier attempt was OOM-killed
in the preload (the `da2ce57` run). The other systems' rows are in that run,
on the `-104a` lane.

| system | durability | consumers | signup/s | checkout/s | profile/s | order_page/s | order_detail/s | checkout p99 | peak MB | compacted MB |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| wavedb/single | durable | 1 | 214.6 | 116.1 | 528 723.9 | 14 783.5 | 3 325.1 | 15.9 ms | 98.6 | — |
| wavedb/single | relaxed | 1 | 101 442.0 | 1 184.1 | 529 378.2 | 14 851.7 | 3 428.2 | 2.0 ms | 75.9 | — |
| wavedb/multi | durable | 3 | 193.8 | 119.4 | 1 219 419.4 | 46 278.6 | 9 021.0 | 102.7 ms | 114.8 | — |
| wavedb/multi | relaxed | 3 | 140 882.0 | 4 028.9 | 1 765 117.9 | 27 481.4 | 9 234.4 | 1.9 ms | 77.7 | — |

The `multi` micro rows take no compacted footprint (never seeded, RFC 0065);
no `shop` row recorded one.

Rows `3b16318e0152eaf1`, `43a1243fb8608f11`, `d68c7324f15ef342`,
`9efa278f0718f566`, `46b4c0986de612b1`, `913c247891b70d51`,
`5ede10e2c99d02e4`, `b070b078a2aa2638`.

## `2026-11-13` · intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-kingston-sa400s3-crypt4096-xfs-9485 · `60d8d88` and `54a7aad`

The first run on the dedicated SATA disk (KINGSTON SA400S37240G, LUKS2 4096
sectors, XFS; the host key names it, so this is the `-9485` lane, not
comparable with the earlier btrfs lanes). `micro` and `shop`, `small` tier,
caged (500 MB), clock pinned at 2.3 GHz, not forced. Two code states, one
disk, one night:

- **`60d8d88`**, before RFC 0081: every system, 29 rows (the baseline for
  the new lane). Postgres, MySQL and Valkey failed their first pass, a Unix
  socket path over 107 characters under the long mount path; they were run
  again through a short symlink to the same mount (same lane key).
- **`54a7aad`**, RFC 0081 phases 0, 2–6 (sub-header in bits, clip by
  coverage, packs, spans as entries, merge consolidation, Own writes no
  roots block): WaveDB only, 8 rows. Phase 9 (no dictionaries) is not in
  it.

Two rows are missing, both killed by the cage's memory bound (exit 137,
SIGKILL under the 500 MB `memory.max`): `wavedb/object durable micro` on
both code states (the baseline's left an orphan `bwrap` for 2 h 22 until it
was killed by pid), and **`wavedb/single durable micro` on `54a7aad` only**,
which fits on `60d8d88`. That one is a lead for RFC 0081, not yet explained:
the probe build is the way to see what the row holds in RAM.

Peak MB is the largest footprint the row recorded (`hot` or `settled`),
compacted MB the one at the compacted point; `—` where the code did not
record one (multi and shop on `60d8d88`, RFC 0081 phase 0).

### micro

| system | durability | code | row | insert/s | read_hot/s | read_cold/s | update/s | KiB/insert | KiB/update | peak MB | compacted MB |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| mongodb/- | durable | — | 7b91f024 | 344.1 | 5333.1 | 5275.8 | 533.4 | 4.5 | 821.3 | 270 | 239.5 |
| mongodb/- | relaxed | — | 857262be | 5198.2 | 5310.7 | 5243.7 | 3815.3 | 0.5 | 2.6 | 264 | 257.5 |
| mysql/- | durable | — | ddc2d881 | 510.5 | 14102 | 9950.7 | 467.5 | 10.3 | 40.8 | 290 | 280.8 |
| mysql/- | relaxed | — | 9ba47903 | 7890.8 | 14026.8 | 10321.9 | 5482.4 | 2 | 7 | 290 | 280.8 |
| postgres/- | durable | — | f507b0c3 | 772.8 | 21730.6 | 19519.9 | 774 | 895.5 | 905.2 | 221 | 208.3 |
| postgres/- | relaxed | — | 7ae0797a | 17762.7 | 21050.8 | 19991.5 | 14911 | 1.6 | 0.7 | 283 | 243.9 |
| sqlite/- | durable | — | 871e20d5 | 764.4 | 109077.8 | 108202.3 | 768.1 | 15.3 | 20.8 | 68 | 56.8 |
| sqlite/- | relaxed | — | 7cfbbd85 | 12554.3 | 109523.1 | 111634.6 | 9175.1 | 11.3 | 16.8 | 68 | 56.8 |
| valkey/- | durable | — | eca5ee46 | 424.6 | 38166.1 | 38834.1 | 375.5 | 4.4 | 4.5 | 241 | 45.4 |
| valkey/- | relaxed | — | 04ad5dd1 | 20747.7 | 35489.6 | 38484.7 | 13444.8 | 0.4 | 0.5 | 223 | 45.4 |
| wavedb/multi | durable | `54a7aad` | e86e0b5d | 373.4 | 529493.5 | 106004 | 313.1 | 12.8 | 21.3 | 3713 | 40.3 |
| wavedb/multi | durable | `60d8d88` | 0de03365 | 267.3 | 970029.9 | 284883.8 | 271.3 | 79 | 83.7 | 9771 | — |
| wavedb/multi | relaxed | `54a7aad` | 9019e69e | 38225 | 1962234.3 | 720505.2 | 8599.1 | 0.1 | 1.6 | 95 | 19.1 |
| wavedb/multi | relaxed | `60d8d88` | 504d0b27 | 34066.8 | 1852694.3 | 740987.5 | 7855.4 | 0.1 | 1.8 | 110 | — |
| wavedb/object | relaxed | `54a7aad` | 75a687f6 | 8792.8 | 543241.9 | 217535.3 | 984 | 0.1 | 4.4 | 245 | 24.4 |
| wavedb/object | relaxed | `60d8d88` | ea99b6ec | 8980.1 | 541981.4 | 215501.9 | 1097.4 | 0.1 | 4.3 | 241 | 18 |
| wavedb/single | durable | `60d8d88` | 5f38023a | 111.6 | 325657.3 | 104333.8 | 116.2 | 80.5 | 85.3 | 20858 | 36.3 |
| wavedb/single | relaxed | `54a7aad` | 2d827785 | 9741.7 | 561007.9 | 223037.3 | 2289.5 | 0.1 | 3.9 | 220 | 21.1 |
| wavedb/single | relaxed | `60d8d88` | ef31117e | 11706.8 | 551954.7 | 223555.8 | 2606.8 | 0.1 | 3.6 | 203 | 17.7 |

Against `60d8d88`, durable (WaveDB multi, the only durable pair with both
rows): insert 267.3 → 373.4 /s (+40%), update 271.3 → 313.1 /s (+15%), KiB
written per insert 79 → 12.8 and per update 83.7 → 21.3, peak footprint
9 771 → 3 713 MB. Relaxed is within noise (single insert 11 707 → 9 742 /s,
multi 34 067 → 38 225 /s): the target of RFC 0081 is the durable round.

### shop

| system | durability | code | row | signup/s | checkout/s | profile/s | order_page/s | order_detail/s | KiB/checkout | peak MB | compacted MB |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| mongodb/- | durable | — | f3c1c7fc | 452.8 | 359.5 | 4183.9 | 1226.7 | 555.1 | 4.9 | 337 | 337.2 |
| mongodb/- | relaxed | — | 681b2078 | 2869.1 | 513.7 | 4225.9 | 1300.7 | 644.7 | 0.7 | 337 | 337.3 |
| mysql/- | durable | — | 5b05e737 | 745.7 | 402.5 | 13114.8 | 2915.6 | 1858 | 88.9 | 359 | 345.8 |
| mysql/- | relaxed | — | 092773e5 | 9048.3 | 2484 | 13465.3 | 3039.2 | 1767.2 | 1.1 | 359 | 345.8 |
| postgres/- | durable | — | 7d89d0c2 | 681.9 | 684.5 | 21875.5 | 5300.3 | 4370.9 | 1024.4 | 418 | 358.4 |
| postgres/- | relaxed | — | 9e8d0158 | 9005.9 | 3925.9 | 21195.1 | 5039.6 | 4252.8 | 0.4 | 418 | 358.5 |
| sqlite/- | durable | — | 6264ccf8 | 161.3 | 239.3 | 141813.6 | 23750.4 | 22038.7 | 25.9 | 47 | 44.1 |
| sqlite/- | relaxed | — | 6fc8cc21 | 1064.1 | 15816.7 | 144830.7 | 23863.9 | 21417.9 | 21.9 | 47 | 44.1 |
| valkey/- | durable | — | b642aa89 | 421.3 | 426.1 | 33937.8 | 12560 | 10181.9 | 4.8 | 178 | 97.8 |
| valkey/- | relaxed | — | 34fcf3e4 | 26863.3 | 13330.3 | 33673.8 | 12598.9 | 10210.4 | 0 | 178 | 97.8 |
| wavedb/multi | durable | `54a7aad` | 81be1baa | 281.5 | 247 | 1536946.8 | 45159.6 | 8860.7 | 41.2 | 82 | 89.5 |
| wavedb/multi | durable | `60d8d88` | 775f2686 | 237.2 | 187.5 | 1796210.2 | 49175.2 | 9363.7 | 116.9 | 112 | — |
| wavedb/multi | relaxed | `54a7aad` | 6df41395 | 59230.8 | 9219.1 | 1472065.7 | 26719.1 | 9122.1 | 9.8 | 67 | 73.7 |
| wavedb/multi | relaxed | `60d8d88` | 8db90b82 | 140733.9 | 3194.4 | 1838865.7 | 26929.7 | 9204.6 | 24.9 | 81 | — |
| wavedb/single | durable | `54a7aad` | 27608490 | 189.2 | 102.1 | 456327.6 | 13739 | 3169 | 32.7 | 68 | 71.9 |
| wavedb/single | durable | `60d8d88` | 6352cea3 | 149.9 | 104.4 | 506868.5 | 14288.2 | 3312.2 | 90.4 | 97 | — |
| wavedb/single | relaxed | `54a7aad` | ba5951f2 | 156040.6 | 2992.6 | 461293.2 | 16114.1 | 3362.4 | 21 | 65 | 68.3 |
| wavedb/single | relaxed | `60d8d88` | 5489cbd7 | 102543.4 | 1160.2 | 465410.9 | 14960.1 | 3396.5 | 21.1 | 76 | — |

Against `60d8d88`, durable checkout: single 104.4 → 102.1 /s (equal), multi
187.5 → 247.0 /s (+32%); KiB written per checkout 90.4 → 32.7 (single) and
116.9 → 41.2 (multi). Reads move by up to ~15% either way (WaveDB multi profile 1.80 M → 1.54 M
/s, order page 49 175 → 45 160 /s); the run is one pass, so that is
indicative, not a trend.

Rows: every digest in the tables above (`row`), under
`benches/results/rows/intel-r-core-tm-i5-8300h-cpu-2-30ghz-4c-2300mhz-500m-kingston-sa400s3-crypt4096-xfs-9485/`.
