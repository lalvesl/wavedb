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
    let multi = engine == Engine::Sharded;
    // A seed is one collection on one store; the multi row fills itself
    // (the planner never seeds it).
    let seed = if multi {
        None
    } else {
        cfg.seed_wavedb.as_ref()
    };
    let (seeded, materialise_ms) = materialise(&dir, seed)?;
    // A seed arrives as `<store>/data` plus its `ids.bin`/`pivot.bin`
    // sidecar; the sidecar is what makes it usable at all, since a NonUnique
    // anchor id is minted from the clock and cannot be recomputed.
    let sidecar = if seeded {
        Some(crate::seed::load_wavedb_sidecar(&dir)?)
    } else {
        None
    };

    let data = dir.join("data");
    let consumers = run.key.consumers.max(1) as usize;
    let stores = if multi {
        wavedb_adapter::open_shards(&data, consumers)?
    } else {
        vec![wavedb_adapter::open(&data)?]
    };
    let durable = d == Durability::Durable;
    let factory = drivers::wavedb::Factory::<E>::new(
        stores,
        sidecar,
        cfg.rows as usize,
        durable,
    );
    let used = crate::harness::DriverFactory::consumers(&factory);
    // The consumer count is part of the row's identity: a row asked for
    // three that ran on one would file the wrong number under the right name.
    if used != consumers {
        return Err(format!(
            "wavedb/{} on micro was asked for {consumers} consumers and runs \
             {used}",
            run.key.variant
        ));
    }

    let points = RefCell::new(Points::of(&data, wavedb_is_log));
    let phases =
        crate::harness::run_with(&factory, workload(cfg, seeded), || {
            points.borrow_mut().take(Point::Hot)
        })?;
    let mut points = points.into_inner();
    drop(factory);
    if multi {
        // Every consumer drained and closed its shard. Compaction of a
        // sharded database is the storage shard's to direct (RFC 0073), so
        // the multi row takes `settled` and no `compacted` — as the shop's.
        points.take(Point::Settled)?;
        return Ok(record(
            run,
            phases,
            Meta {
                bracket: "embedded",
                settings: settings(engine, d, used),
                compression: "zstd (per-type dictionaries)",
                retains_history: true,
                notes: multi_notes(used),
                seed_path: None,
                materialise_ms,
                footprints: points.into_vec(),
            },
        ));
    }

    // Reopened as a plain `PageStore`, whichever seam the row measured. The
    // footprint is bytes on disk, and the actor changes none of them — while
    // going back through it would ask a second engine to exist for work that
    // is not being timed. The reopen also proves the store reconstructs.
    let engine_after = wavedb_adapter::open(&data)?;
    quiesce(&engine_after, &points)?;
    points.take(Point::Settled)?;
    Engineish::compact(&engine_after)?;
    quiesce(&engine_after, &points)?;
    points.take(Point::Compacted)?;
    Engineish::close(engine_after);

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "embedded",
            settings: settings(engine, d, used),
            compression: "zstd (per-type dictionaries)",
            retains_history: true,
            notes: wavedb_adapter::notes(seeded),
            seed_path: path_of(cfg.seed_wavedb.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    ))
}

/// Settle **until the footprint stops moving**.
///
/// One settle is not quiescence: what a batch frees becomes a hole only once
/// the next one is durable (RFC 0072 §8), so the footprint can still move
/// after the first.
fn quiesce<E: Engineish>(engine: &E, points: &Points) -> Result<(), String> {
    const MAX_ROUNDS: usize = 6;
    // `u64::MAX` forces at least two rounds, so a size that merely failed to
    // grow proves nothing yet.
    let mut last = u64::MAX;
    for _ in 0..MAX_ROUNDS {
        engine.drain()?;
        let now = points.probe()?;
        if now == last && !engine.has_pending() {
            return Ok(());
        }
        last = now;
    }
    Ok(())
}

/// The multi row's notes: the single row's, and how it splits the dataset.
fn multi_notes(consumers: usize) -> Vec<String> {
    let mut notes = wavedb_adapter::notes(false);
    notes.push(format!(
        "Multi: {consumers} consumers, each its own thread owning one data \
         shard and one `Thing` collection on it; row n is consumer \
         n % {consumers}'s. A collection is indivisible (its chains belong to \
         its owner), so the dataset is split by owner, as a node splits its \
         owners across workers. Every data shard writes through one storage \
         shard (RFC 0073)."
    ));
    notes
}

fn settings(
    engine: Engine,
    d: Durability,
    consumers: usize,
) -> Vec<(String, String)> {
    vec![
        ("mode".into(), engine.mode().into()),
        ("consumers".into(), consumers.to_string()),
        // Counted, not assumed.
        (
            "threads".into(),
            match engine {
                Engine::Direct => "1 consumer (everything there)".to_string(),
                Engine::Sharded => {
                    "one consumer per data shard, each its own thread"
                        .to_string()
                }
            },
        ),
        (
            "barrier".into(),
            match d {
                Durability::Durable => {
                    "`_durable` writes: a settle and its fsync each (RFC 0072)"
                        .to_string()
                }
                Durability::Relaxed => {
                    format!(
                        "standard writes: a settle per elapsed {:?}",
                        crate::RELAXED_WINDOW
                    )
                }
            },
        ),
        ("block_size".into(), "4096".into()),
    ]
}
