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
