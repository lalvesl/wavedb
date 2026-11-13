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
//! WaveDB's row also times its store's **opens** ([`super::opens`]): as the
//! workload left it, before anything else touches it, and after the
//! compaction — cold and warm each.
//!
//! There is no `baseline`: an empty SQLite file and an empty WaveDB store are
//! a few kilobytes, and a point that is always ~0 is a column of noise. The
//! server rows take one because a running, empty PostgreSQL is ~40 MB.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::cell::RefCell;

use wavedb_storage::PageStore;

use super::RowRun;
use super::micro::{Meta, materialise, path_of, record, workload};
use super::points::{Points, sqlite_is_log, wavedb_is_log};
use crate::corpus::{CensusRecord, RowRecord};
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
    let mut stores = if multi {
        wavedb_adapter::open_shards(&data, consumers)?
    } else {
        vec![wavedb_adapter::open(&data)?]
    };
    let durable = d == Durability::Durable;
    if !durable {
        wavedb_adapter::relax(&mut stores)?;
    }
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
    let mut opens = Vec::new();
    if multi {
        // Every consumer drained and closed its shard; the reopen is the
        // open timed as written, and its stores are the ones compacted.
        let stores = super::opens::timed("written", &data, &mut opens, || {
            wavedb_adapter::open_shards(&data, consumers)
        })?;
        let census = compact_all(stores, &mut points)?;
        drop(super::opens::timed("compacted", &data, &mut opens, || {
            wavedb_adapter::open_shards(&data, consumers)
        })?);
        let mut row = record(
            run,
            phases,
            Meta {
                bracket: "embedded",
                settings: settings(engine, d, used),
                compression: "zstd (per page)",
                retains_history: true,
                notes: multi_notes(used),
                seed_path: None,
                materialise_ms,
                footprints: points.into_vec(),
            },
        );
        row.opens = opens;
        row.census = census;
        return Ok(row);
    }

    // Reopened as a plain `PageStore`, whichever seam the row measured. The
    // footprint is bytes on disk, and the actor changes none of them — while
    // going back through it would ask a second engine to exist for work that
    // is not being timed. The reopen also proves the store reconstructs,
    // and it is the open timed as written.
    let engine_after =
        super::opens::timed("written", &data, &mut opens, || {
            wavedb_adapter::open(&data)
        })?;
    let census = compact_all(vec![engine_after], &mut points)?;
    let compacted =
        super::opens::timed("compacted", &data, &mut opens, || {
            wavedb_adapter::open(&data)
        })?;
    Engineish::close(compacted);

    let mut row = record(
        run,
        phases,
        Meta {
            bracket: "embedded",
            settings: settings(engine, d, used),
            compression: "zstd (per page)",
            retains_history: true,
            notes: wavedb_adapter::notes(seeded),
            seed_path: path_of(cfg.seed_wavedb.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    );
    row.opens = opens;
    row.census = census;
    Ok(row)
}

/// Settle **until the footprint stops moving**.
///
/// One settle is not quiescence: what a batch frees becomes a hole only once
/// the next one is durable (RFC 0072 §8), so the footprint can still move
/// after the first.
pub(super) fn quiesce<E: Engineish>(
    engines: &[E],
    points: &Points,
) -> Result<(), String> {
    const MAX_ROUNDS: usize = 6;
    // `u64::MAX` forces at least two rounds, so a size that merely failed to
    // grow proves nothing yet.
    let mut last = u64::MAX;
    for _ in 0..MAX_ROUNDS {
        for engine in engines {
            engine.drain()?;
        }
        let now = points.probe()?;
        if now == last && !engines.iter().any(Engineish::has_pending) {
            return Ok(());
        }
        last = now;
    }
    Ok(())
}

/// `settled`, then `compacted`: every store quiesced, then compacted and
/// quiesced again, then closed.
///
/// A sharded database compacts the same way, one data shard after another:
/// the storage shard directs the evacuation (RFC 0073 §7) and cuts its files
/// with the durable write over each shard's moves, so nothing is written to
/// get the space back.
pub(super) fn compact_all(
    stores: Vec<PageStore>,
    points: &mut Points,
) -> Result<Vec<CensusRecord>, String> {
    quiesce(&stores, points)?;
    points.take(Point::Settled)?;
    for store in &stores {
        Engineish::compact(store)?;
    }
    quiesce(&stores, points)?;
    points.take(Point::Compacted)?;
    let census = census_of(&stores)?;
    for store in stores {
        Engineish::close(store);
    }
    Ok(census)
}

/// The census of `stores` (RFC 0081 phase 0), each slot's summed over
/// them — a sharded row's data shards hold the same slots.
fn census_of(stores: &[PageStore]) -> Result<Vec<CensusRecord>, String> {
    let mut by_slot: std::collections::BTreeMap<u64, CensusRecord> =
        std::collections::BTreeMap::new();
    for store in stores {
        for c in store.census().map_err(|e| format!("census: {e}"))? {
            let sum =
                by_slot
                    .entry(c.struct_hash)
                    .or_insert_with(|| CensusRecord {
                        slot: format!("{:016x}", c.struct_hash),
                        ..CensusRecord::default()
                    });
            sum.ranges += c.ranges;
            sum.pages += c.pages;
            sum.holes += c.holes;
            sum.evicted += c.evicted;
            sum.runs += c.runs;
            sum.shared_runs += c.shared_runs;
            sum.blocks += c.blocks;
            sum.records += c.records;
            sum.record_bytes += c.record_bytes;
            sum.index_bytes += c.index_bytes;
        }
    }
    Ok(by_slot.into_values().collect())
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

pub(super) fn settings(
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
                        "standard writes: a settle per elapsed {:?} or past half the \
                         record budget (pressure), its fsync on a helper, never \
                         awaited",
                        crate::RELAXED_WINDOW
                    )
                }
            },
        ),
        ("block_size".into(), "4096".into()),
    ]
}
