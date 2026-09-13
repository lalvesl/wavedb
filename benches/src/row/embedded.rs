//! The two in-process rows, with their footprints ([RFC 0065] §7).
//!
//! ## The order the four points are taken in
//!
//! 1. `hot` — inside [`run_with`](crate::harness::run_with)'s window, after
//!    the last phase and **before** any driver closes. Every `close` here
//!    quiesces something, so this is the only place the deferral a system is
//!    carrying is still visible.
//! 2. `settled` — after the row's own quiescence, which each system spells
//!    differently.
//! 3. `compacted` — after a rewrite the system does not do on its own.
//!
//! There is no `baseline`: an empty SQLite file and an empty WaveDB store are
//! a few kilobytes, and a point that is always ~0 is a column of noise. The
//! server rows take one because a running, empty PostgreSQL is ~40 MB.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::cell::RefCell;

use super::RowRun;
use super::micro::{Meta, materialise, path_of, record, workload};
use super::points::{Points, sqlite_is_log, wavedb_is_log};
use crate::corpus::RowRecord;
use crate::footprint::Point;
use crate::systems::drivers;
use crate::systems::engine::{Engine, Engineish};
use crate::systems::{Durability, wavedb as wavedb_adapter};

/// # Errors
/// The database could not be opened, or an operation was refused.
pub fn sqlite(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    let cfg = &run.cfg;
    let dir = cfg.work_dir.join(format!("sqlite-{}", d.name()));
    let (seeded, materialise_ms) = materialise(&dir, cfg.seed_sqlite.as_ref())?;

    let sync = match d {
        Durability::Durable => "FULL",
        Durability::Relaxed => "NORMAL",
    };
    let path = dir.join("bench.db");
    // A seeded row arrives with the table already loaded by `sqlite3 .import`
    // in the builder, so there is no schema left to create.
    let factory = drivers::sqlite::Factory {
        path: path.clone(),
        sync,
        create: !seeded,
    };

    let points = RefCell::new(Points::of(&dir, sqlite_is_log));
    let phases =
        crate::harness::run_with(&factory, workload(cfg, seeded), || {
            points.borrow_mut().take(Point::Hot)
        })?;
    let mut points = points.into_inner();

    // The driver checkpointed on the way out; this connection takes the
    // reading and then asks for the rewrite SQLite never does on its own.
    let conn = rusqlite::Connection::open(&path)
        .map_err(|e| format!("sqlite: {e}"))?;
    points.take(Point::Settled)?;
    conn.execute_batch("VACUUM")
        .map_err(|e| format!("sqlite: vacuum: {e}"))?;
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .map_err(|e| format!("sqlite: checkpoint: {e}"))?;
    drop(conn);
    points.take(Point::Compacted)?;

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "embedded",
            settings: vec![
                ("journal_mode".into(), "WAL".into()),
                ("synchronous".into(), sync.into()),
                (
                    "transaction".into(),
                    "one per operation (autocommit)".into(),
                ),
                ("statements".into(), "prepare_cached".into()),
            ],
            compression: "none",
            retains_history: false,
            notes: vec![
            "Retains no superseded versions: an update overwrites in place \
             and the previous row is unrecoverable."
                .into(),
        ],
            seed_path: path_of(cfg.seed_sqlite.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    ))
}

/// # Errors
/// The store could not be opened, or an operation was refused.
pub fn wavedb<E>(run: &RowRun, d: Durability) -> Result<RowRecord, String>
where
    E: Engineish + drivers::wavedb::FromStore,
{
    let cfg = &run.cfg;
    let engine = super::engine_of(&run.key.variant)?;
    // Per row: no two rows may share a store, or the second would inherit the
    // first's pages and its `insert` phase would measure a rewrite.
    let dir =
        cfg.work_dir
            .join(format!("wavedb-{}-{}", engine.name(), d.name()));
    let (seeded, materialise_ms) = materialise(&dir, cfg.seed_wavedb.as_ref())?;
    // A seed arrives as `<store>/data` plus its `ids.bin`/`pivot.bin`
    // sidecar; the sidecar is what makes it usable at all, since a NonUnique
    // anchor id is minted from the clock and cannot be recomputed.
    let sidecar = if seeded {
        Some(crate::seed::load_wavedb_sidecar(&dir)?)
    } else {
        None
    };

    let data = dir.join("data");
    let store = wavedb_adapter::open(&data, d)?;
    let factory =
        drivers::wavedb::Factory::<E>::new(store, sidecar, cfg.rows as usize);

    let points = RefCell::new(Points::of(&data, wavedb_is_log));
    let phases =
        crate::harness::run_with(&factory, workload(cfg, seeded), || {
            points.borrow_mut().take(Point::Hot)
        })?;
    let mut points = points.into_inner();

    // Reopened as a plain `PageStore`, whichever seam the row measured. The
    // footprint is bytes on disk, and the actor changes none of them — while
    // going back through it would ask a second engine to exist for work that
    // is not being timed. The reopen also proves the store replays.
    let engine_after = wavedb_adapter::open(&data, d)?;
    quiesce(&engine_after, &points)?;
    points.take(Point::Settled)?;
    Engineish::defragment(&engine_after, DEFRAG_BUDGET_BLOCKS)?;
    quiesce(&engine_after, &points)?;
    points.take(Point::Compacted)?;
    Engineish::close(engine_after);

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "embedded",
            settings: settings(engine, d),
            compression: "zstd (per-type dictionaries)",
            retains_history: true,
            notes: wavedb_adapter::notes(seeded, engine),
            seed_path: path_of(cfg.seed_wavedb.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    ))
}

/// Defrag budget: generous enough that one pass is the compaction, since the
/// point of the `compacted` footprint is the floor, not a partial move.
const DEFRAG_BUDGET_BLOCKS: u64 = 1 << 20;

/// Drain and checkpoint **until the footprint stops moving**.
///
/// One checkpoint is not quiescence: journal retirement is generational
/// (RFC 0047), so the journal a checkpoint supersedes is deleted by the one
/// after it. Measuring after a single round counts a whole retained journal as
/// stored data — which is how a 1.4 MB database first measured as 34 MB.
fn quiesce<E: Engineish>(engine: &E, points: &Points) -> Result<(), String> {
    const MAX_ROUNDS: usize = 6;
    // `u64::MAX` forces at least two rounds: the first checkpoint supersedes
    // the journal, the second is the one that may delete it, so a size that
    // merely failed to grow proves nothing yet.
    let mut last = u64::MAX;
    for _ in 0..MAX_ROUNDS {
        engine.drain()?;
        engine.checkpoint()?;
        let now = points.probe()?;
        if now == last && !engine.has_pending() {
            return Ok(());
        }
        last = now;
    }
    Ok(())
}

fn settings(engine: Engine, d: Durability) -> Vec<(String, String)> {
    vec![
        ("mode".into(), engine.mode().into()),
        // Counted, not assumed. `Shards::start` spawns the **disk actor** and
        // nothing else — the per-shard worker threads belong to the node's
        // `Router`, which a benchmark driving `Store` directly never builds.
        (
            "threads".into(),
            match engine {
                Engine::Direct => "1 consumer (everything there)".to_string(),
                Engine::Sharded => {
                    "1 consumer, which is the shard, + 1 disk actor".to_string()
                }
            },
        ),
        (
            "barrier".into(),
            match d {
                Durability::Durable => "fsync per apply batch".to_string(),
                Durability::Relaxed => {
                    format!("one fsync per elapsed {:?}", crate::RELAXED_WINDOW)
                }
            },
        ),
        ("block_size".into(), "4096".into()),
    ]
}
