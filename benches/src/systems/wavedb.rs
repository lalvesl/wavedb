//! The WaveDB adapter — engine in-process, the embedded bracket.
//!
//! Three constraints from RFC 0060 §5 shape this file:
//!
//! - **One store per process.** Every open is scoped so the `EngineClaim`
//!   drops before the next one; the cold-read phase depends on it.
//! - **The typed path only.** Reads go through `CollectionHandle::get`, which
//!   routes by `STRUCT_HASH`. `Store::get` is an untyped fallback that probes
//!   every slot — a path no generated code takes.
//! - **Steady state.** The settle queue is drained and the journal checkpointed
//!   before the "settled" footprint, so the number is not just work postponed.

use std::path::Path;

use futures::executor::block_on;
use wavedb_core::{CollectionHandle, LocalHandle, U48};
use wavedb_storage::PageStore;

use super::engine::{Engine, Engineish, Sharded};
use super::{Cfg, Durability, SystemReport};
use crate::RELAXED_WINDOW;
use crate::footprint::{Footprint, Point};
use crate::metrics::{self, Phase};
use crate::schema::{Rng, Thing, ThingPivotId, logical_bytes, thing, thing_v2};

const TENANT: u32 = 1;

/// Journal bytes that trigger a maintenance pass — `quick-node`'s own default
/// (`Maintenance::checkpoint_after_bytes`).
///
/// Byte-driven for the reason that default is byte-driven: an **operation
/// count cannot bound a log whose per-operation size depends on the data**.
/// Here it emphatically does. A first attempt checkpointed every 5 000 inserts
/// and never fired once: under 5 000 inserts of this schema had already
/// written **649 MB** of journal — on the order of 130 KB each, against a
/// ~400-byte record — while `data.bin` was still 4 KB.
///
/// That ratio is a finding in its own right and is **not explained here**; it
/// wants its own measurement before anyone attributes it to a mechanism.
const CHECKPOINT_AFTER_BYTES: u64 = 64 << 20;

/// What the write cache is evicted **down to** — not to zero.
///
/// Evicting to zero drops the hot B+tree interior nodes along with everything
/// else, so the next write descends through the page store instead of RAM; in
/// the shop adapter that made a fill 6× slower. This has to sit far enough
/// under the cage to leave room for the process itself.
const CACHE_BUDGET_BYTES: usize = 96 << 20;

/// One maintenance pass: checkpoint (which settles the queue into pages and
/// retires the journal) and bring the write cache back under budget. Exactly
/// what `quick-node`'s maintenance loop does, on the benchmark's own schedule.
///
/// Called from **inside** a timed phase but **outside** `Latencies::time`, so
/// it changes no latency and no `ops_per_sec` — those sum the individually
/// timed operations. It does land in the phase's `bytes_written`, which spans
/// the whole closure, and that is right: those bytes are really written.
fn maintain<E: Engineish>(engine: &E) {
    if engine.journal_len() <= CHECKPOINT_AFTER_BYTES {
        return;
    }
    engine.checkpoint().expect("checkpoint");
    engine.evict(CACHE_BUDGET_BYTES);
}
/// Defrag budget: generous enough that one pass is the compaction, since the
/// point of the "compacted" footprint is the floor, not a partial move.
const DEFRAG_BUDGET_BLOCKS: u64 = 1 << 20;

pub fn run(
    cfg: &Cfg,
    d: Durability,
    engine: Engine,
) -> Result<SystemReport, String> {
    // Per row: no two rows may share a store, or the second would inherit the
    // first's pages and its `insert` phase would measure a rewrite.
    let dir =
        cfg.work_dir
            .join(format!("wavedb-{}-{}", engine.name(), d.name()));
    // A seed arrives as `<store>/data` plus its `ids.bin`/`pivot.bin` sidecar;
    // the sidecar is what makes it usable at all, since a NonUnique anchor id
    // is minted from the clock and cannot be recomputed.
    let mut materialise_ms = 0;
    let seeded = if let Some(src) = &cfg.seed_wavedb {
        materialise_ms =
            crate::seed::materialise(src, &dir)?.as_millis() as u64;
        Some(crate::seed::load_wavedb_sidecar(&dir)?)
    } else {
        std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
        None
    };

    let data = dir.join("data");
    // The engine choice is made **once**, here, and everything downstream is
    // generic over it: one workload, two seams, no duplicated phase code.
    let (phases, footprints) = match engine {
        Engine::Direct => drive(cfg, &data, seeded, || open(&data, d))?,
        Engine::Sharded => {
            drive(cfg, &data, seeded, || Sharded::new(open(&data, d)?))?
        }
    };

    Ok(SystemReport {
        system: match engine {
            Engine::Direct => "wavedb",
            Engine::Sharded => "wavedb-sharded",
        },
        bracket: "embedded",
        workload: "micro",
        durability: d,
        version: env!("CARGO_PKG_VERSION").to_string(),
        settings: vec![
            ("mode".into(), engine.mode().into()),
            // Counted, not assumed. `Shards::start` spawns the **disk actor**
            // and nothing else — the per-shard worker threads belong to the
            // node's `Router`, which a benchmark driving `Store` directly never
            // builds. So the sharded row is this thread acting as the one
            // shard, plus the actor's: two threads, not N+1.
            (
                "threads".into(),
                match engine {
                    Engine::Direct => "1 (everything here)".to_string(),
                    Engine::Sharded => {
                        "2 — this thread is the shard, + 1 disk actor"
                            .to_string()
                    }
                },
            ),
            (
                "barrier".into(),
                match d {
                    Durability::Durable => "fsync per apply batch".to_string(),
                    Durability::Relaxed => {
                        format!("one fsync per elapsed {RELAXED_WINDOW:?}")
                    }
                },
            ),
            ("block_size".into(), "4096".into()),
        ],
        compression: "zstd (per-type dictionaries)",
        retains_history: true,
        phases,
        footprints,
        live_records: cfg.rows,
        logical_bytes: logical_bytes(cfg.rows, cfg.seed),
        notes: notes(cfg.seed_wavedb.is_some(), engine),
        seed_path: cfg.seed_wavedb.as_ref().map(|p| p.display().to_string()),
        materialise_ms,
    })
}

/// Everything one row produces: its timed phases and its footprint points.
type Row = (Vec<Phase>, Vec<(Point, Footprint)>);

/// The whole row, generic over which engine seam is under it.
///
/// `open` is a factory rather than a value because the sequence **reopens**:
/// the cold-read phase depends on a store with empty caches, and the
/// process-wide `EngineClaim` means the previous one must be closed first.
fn drive<E, F>(
    cfg: &Cfg,
    data: &Path,
    seeded: Option<(Vec<wavedb_core::Id>, ThingPivotId)>,
    open: F,
) -> Result<Row, String>
where
    E: Engineish,
    F: Fn() -> Result<E, String>,
{
    let (mut phases, ids, pivot) = match seeded {
        Some((ids, pivot)) => (read_only(cfg, &open, pivot, &ids)?, ids, pivot),
        None => fill_and_read(cfg, data, &open)?,
    };
    let (update, footprints) =
        update_and_measure(cfg, data, &open, &ids, pivot)?;
    phases.push(update);
    Ok((phases, footprints))
}

pub fn notes(seeded: bool, engine: Engine) -> Vec<String> {
    let mut notes = vec![
        "Every collection op is one apply batch. The durable row takes one \
         fsync per batch; the relaxed row takes one per elapsed window \
         (RFC 0061), the counterpart of the others' relaxed knobs."
            .into(),
        "Retains every superseded version; the other systems retain none. Read \
         the update row beside the footprint, never alone."
            .into(),
        "Barrier count is not recorded: PageStore exposes no public IoCounts \
         accessor and RFC 0060 forbids changing a shipped crate."
            .into(),
    ];
    if seeded {
        notes.push(
            "Seeded run: no insert phase (the insert benchmark IS the fill), \
             and no read_hot phase — the per-type cache is a write cache that \
             reads never populate, so on a store nobody has just written to, \
             hot and cold are the same measurement. RFC 0044 is that gap."
                .into(),
        );
    }
    if engine == Engine::Sharded {
        notes.push(
            "Sharded row: the engine is owned by a disk actor on its own \
             thread and reached by message through a ShardStore. It is the \
             single/multi-thread axis, and it measures the cost of that \
             boundary — NOT parallelism. This benchmark issues one operation \
             at a time, so only one shard ever has work; and the brake keys \
             on (tenant, STRUCT_HASH), so one type under one tenant would be \
             one owner even under a concurrent client."
                .into(),
        );
        notes.push(
            "read_cold is NOT comparable to the direct row. ShardStore \
             memoises on read; the engine's per-type cache is a write cache \
             that reads never populate. So the sharded row has a read cache \
             the direct row does not — the gap RFC 0044 names, filled here as \
             a side effect of the shard owning its own cache. A faster \
             read_cold on this row is that difference, not a faster read path."
                .into(),
        );
        notes.push(
            "Measured cost of a ShardStore MISS: the same row run with a \
             16 MiB shard budget against a ~40 MB working set reported \
             read_hot at 96 175/s where the direct row reported 1 269 423/s. \
             The round trip is roughly an order of magnitude over an \
             in-process hit, so the row's result is governed by the shard's \
             hit rate — and ShardStore bounds itself by clearing the whole \
             cache rather than evicting (RFC 0044 is that gap). This row uses \
             the shipped 64 MiB default, which holds this working set."
                .into(),
        );
    }
    notes
}

/// A seeded store's read phase. Only `read_cold` exists here, and the reason is
/// itself a finding: a read that misses the cache does not populate it, so a
/// store opened over prefilled pages has no warm state to measure.
fn read_only<E: Engineish>(
    cfg: &Cfg,
    open: &impl Fn() -> Result<E, String>,
    pivot: ThingPivotId,
    ids: &[wavedb_core::Id],
) -> Result<Vec<Phase>, String> {
    let engine = open()?;
    let db = LocalHandle::new(engine.store(), U48::from(TENANT));
    let col = Thing::collection(pivot);
    let phase = read_phase("read_cold", cfg, &db, col, ids);
    engine.close();
    Ok(vec![phase])
}

/// Insert every row, then read back hot (engine caches warm) and cold (store
/// reopened, so the caches are empty and reads fall through to settled pages).
fn fill_and_read<E: Engineish>(
    cfg: &Cfg,
    dir: &Path,
    open: &impl Fn() -> Result<E, String>,
) -> Result<(Vec<Phase>, Vec<wavedb_core::Id>, ThingPivotId), String> {
    let _ = dir;
    let mut ids = Vec::with_capacity(cfg.rows as usize);
    let mut phases = Vec::new();

    let pivot = {
        let engine = open()?;
        let db = LocalHandle::new(engine.store(), U48::from(TENANT));
        let pivot = block_on(Thing::create_pivot(&db))
            .map_err(|e| format!("create_pivot: {e}"))?;
        let col = Thing::collection(pivot);

        phases.push(metrics::phase(
            "insert",
            |lat| {
                for n in 0..cfg.rows {
                    let t = thing(n, cfg.seed);
                    let id = lat.time(|| {
                        block_on(col.insert(&db, &t)).expect("insert")
                    });
                    ids.push(id);
                    maintain(&engine);
                }
            },
            cfg.rows as usize,
        ));

        // Quiesce before reading so the read phase measures the read path and
        // not a settle that happened to land inside it.
        engine.drain()?;
        phases.push(read_phase("read_hot", cfg, &db, col, &ids));
        engine.checkpoint()?;
        engine.close();
        pivot
    };

    // Reopen: engine caches are empty, so this is a genuine read-through to
    // pages. The OS page cache is still warm — see the note in `run`.
    {
        let engine = open()?;
        let db = LocalHandle::new(engine.store(), U48::from(TENANT));
        let col = Thing::collection(pivot);
        phases.push(read_phase("read_cold", cfg, &db, col, &ids));
        engine.close();
    }

    Ok((phases, ids, pivot))
}

/// Whole-record saves at known anchors, then the three footprint points.
fn update_and_measure<E: Engineish>(
    cfg: &Cfg,
    dir: &Path,
    open: &impl Fn() -> Result<E, String>,
    ids: &[wavedb_core::Id],
    pivot: ThingPivotId,
) -> Result<(Phase, Vec<(Point, Footprint)>), String> {
    let engine = open()?;
    let db = LocalHandle::new(engine.store(), U48::from(TENANT));
    // The same pivot the fill used: a collection's chains hang off it, so a
    // fresh one would save into a different collection entirely.
    let col = Thing::collection(pivot);

    let mut rng = Rng::new(cfg.seed ^ 0x0DDB_A11B_EEF0_0D15);
    let update = metrics::phase(
        "update",
        |lat| {
            for _ in 0..cfg.updates {
                let n = rng.below(cfg.rows);
                let id = ids[n as usize];
                let t = thing_v2(n, cfg.seed);
                lat.time(|| block_on(col.save(&db, id, &t)).expect("save"));
                // A save archives the superseded version, so this loop grows
                // the cache and the journal faster than the fill does.
                maintain(&engine);
            }
        },
        cfg.updates as usize,
    );

    let mut points = vec![(Point::Hot, measure(dir)?)];
    points.push((Point::Settled, quiesce(&engine, dir)?));
    engine.defragment(DEFRAG_BUDGET_BLOCKS)?;
    points.push((Point::Compacted, quiesce(&engine, dir)?));
    engine.close();
    Ok((update, points))
}

/// Drain the settle queue and checkpoint **until the footprint stops moving**.
///
/// One checkpoint is not quiescence here: journal retirement is generational
/// (RFC 0047), so the journal a checkpoint supersedes is deleted by the one
/// after it. Measuring after a single round therefore counts a whole retained
/// journal as though it were stored data — which is how a 1.4 MB database
/// first measured as 34 MB. Looping until stable is the honest reading of
/// "the system's own natural quiescence", and it costs a few barriers.
fn quiesce<E: Engineish>(engine: &E, dir: &Path) -> Result<Footprint, String> {
    const MAX_ROUNDS: usize = 6;
    // `u64::MAX` forces at least two rounds: the first checkpoint supersedes
    // the journal, the second is the one that may delete it, so a size that
    // merely failed to grow proves nothing yet.
    let mut last = u64::MAX;
    for _ in 0..MAX_ROUNDS {
        engine.drain()?;
        engine.checkpoint()?;
        let now = measure(dir)?;
        if now.allocated_bytes == last && !engine.has_pending() {
            return Ok(now);
        }
        last = now.allocated_bytes;
    }
    measure(dir)
}

/// A retired journal is 29 bytes and nothing here is preallocated, so the log
/// column exists on this row only to keep the comparison symmetric: every
/// system's recovery area is separated from its data (RFC 0060 §4.1).
fn measure(dir: &Path) -> Result<Footprint, String> {
    Footprint::split(dir, is_log).map_err(io)
}

fn is_log(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("journal_"))
}

fn read_phase<S: wavedb_core::Store>(
    name: &'static str,
    cfg: &Cfg,
    db: &LocalHandle<'_, S>,
    col: CollectionHandle<Thing>,
    ids: &[wavedb_core::Id],
) -> Phase {
    let mut rng = Rng::new(cfg.seed ^ u64::from(name.len() as u32));
    metrics::phase(
        name,
        |lat| {
            for _ in 0..cfg.reads {
                let id = ids[rng.below(ids.len() as u64) as usize];
                let got = lat.time(|| block_on(col.get(db, id)).expect("get"));
                assert!(got.is_some(), "{name}: missing record");
            }
        },
        cfg.reads as usize,
    )
}

/// Opened at the row's own durability (RFC 0061). Unlike the shop workload
/// there is no untimed preload to exempt here — the fill **is** the `insert`
/// phase — so every open in this adapter takes the row's window.
pub fn open(dir: &Path, d: Durability) -> Result<PageStore, String> {
    PageStore::open_with(
        dir,
        &Thing::storage_entries(),
        wavedb_storage::StoreOptions {
            relax_window: match d {
                Durability::Durable => std::time::Duration::ZERO,
                Durability::Relaxed => crate::RELAXED_WINDOW,
            },
        },
    )
    .map_err(|e| format!("open: {e}"))
}

fn io(e: std::io::Error) -> String {
    format!("footprint: {e}")
}
