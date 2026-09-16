//! The `shop` workload on the [RFC 0065] harness.
//!
//! ## The preload is the row's, and it is untimed
//!
//! Every shop row starts from a filled database: `users` users, each with
//! their orders and line items. That fill is bulk, single-threaded, and done
//! in whatever shape each system's own bulk loader wants — one transaction at
//! `synchronous = OFF` on SQLite, a relaxed window on WaveDB. It is never
//! measured, and the stored form it produces is the same either way.
//!
//! It runs **here**, before the harness starts, so a driver only ever sees the
//! measured window and the generator never has to know a database exists.
//!
//! ## Why the read phases come after a settle
//!
//! `signup` and `checkout` write. Without a boundary between them and the
//! three read phases, a read would measure the write cache. Each system
//! empties it in its own spelling — SQLite reopens, WaveDB evicts to zero, the
//! three servers restart — and
//! [`SETTLE_AFTER`](crate::systems::drivers::shop::SETTLE_AFTER) is the one
//! place that names when.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::cell::RefCell;

use wavedb_storage::PageStore;

use super::RowRun;
use super::micro::{Meta, record};
use super::points::{Points, sqlite_is_log, wavedb_is_log};
use crate::corpus::RowRecord;
use crate::footprint::Point;
use crate::harness::shop::ShopWorkload;
use crate::shop::PAGE;
use crate::systems::Durability;
use crate::systems::drivers::shop as drv;
use crate::systems::engine::Engine;
use crate::systems::shop::ShopCfg;

/// Run `run` on the harness, or answer `None` if this is not a `shop` row.
///
/// # Errors
/// The preload failed, a server would not start, or an operation was refused.
pub fn measure(
    run: &RowRun,
    d: Durability,
) -> Result<Option<RowRecord>, String> {
    if run.key.workload != "shop" {
        return Ok(None);
    }
    match run.key.system.as_str() {
        "sqlite" => sqlite(run, d).map(Some),
        "wavedb" => match super::engine_of(&run.key.variant)? {
            Engine::Direct => {
                wavedb::<PageStore>(run, d, consumers(run)).map(Some)
            }
            Engine::Sharded => {
                wavedb::<drv::engine::ShopShard>(run, d, consumers(run))
                    .map(Some)
            }
        },
        #[cfg(feature = "servers")]
        "postgres" => super::shop_server::postgres(run, d).map(Some),
        #[cfg(feature = "servers")]
        "mysql" => super::shop_server::mysql(run, d).map(Some),
        #[cfg(feature = "servers")]
        "mongodb" => super::shop_server::mongodb(run, d).map(Some),
        #[cfg(not(feature = "servers"))]
        "postgres" | "mysql" | "mongodb" => Err(format!(
            "{}: built without the `servers` feature",
            run.key.system
        )),
        other => Err(format!("no shop adapter for {other}")),
    }
}

/// The consumer count the row was scheduled with. It is a **row-identity
/// field**, so it arrives on the command line rather than being decided here
/// — a row that chose its own would be filed under a digest that does not
/// describe it.
fn consumers(run: &RowRun) -> usize {
    run.key.consumers.max(1) as usize
}

fn sqlite(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    let cfg = &run.shop;
    let dir = cfg.work_dir.join(format!("shop-sqlite-{}", d.name()));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    let path = dir.join("shop.db");

    {
        // `OFF` for the fill only: it is never timed, and the stored form it
        // produces is the same either way.
        let mut conn = rusqlite::Connection::open(&path).map_err(sqlite_err)?;
        conn.pragma_update(None, "synchronous", "OFF")
            .map_err(sqlite_err)?;
        conn.execute_batch(crate::systems::shop::sqlite::DDL)
            .map_err(sqlite_err)?;
        crate::systems::shop::sqlite::preload(cfg, &mut conn)?;
    }

    let sync = match d {
        Durability::Durable => "FULL",
        Durability::Relaxed => "NORMAL",
    };
    let factory = drv::sqlite::Factory {
        path: path.clone(),
        sync,
    };
    let points = RefCell::new(Points::of(&dir, sqlite_is_log));
    let phases = crate::harness::run_with(
        &factory,
        ShopWorkload::new(clone_cfg(cfg)),
        || points.borrow_mut().take(Point::Hot),
    )?;
    let mut points = points.into_inner();

    let conn = rusqlite::Connection::open(&path).map_err(sqlite_err)?;
    points.take(Point::Settled)?;
    conn.execute_batch("VACUUM").map_err(sqlite_err)?;
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
                ("tenancy".into(), "user_id column + index".into()),
                ("transaction".into(), "one per checkout".into()),
            ],
            compression: "none",
            retains_history: false,
            notes: vec![
            "The order page is ORDER BY … LIMIT 10 OFFSET n over an index, \
             not a materialised list."
                .into(),
        ],
            seed_path: None,
            materialise_ms: 0,
            footprints: points.into_vec(),
        },
    ))
}

fn wavedb<E>(
    run: &RowRun,
    d: Durability,
    asked: usize,
) -> Result<RowRecord, String>
where
    E: drv::engine::ShopEngine,
    E::Shared: Send + Sync,
{
    let cfg = &run.shop;
    let engine = super::engine_of(&run.key.variant)?;
    let dir = cfg.work_dir.join(format!(
        "shop-wavedb-{}-{}",
        engine.name(),
        d.name()
    ));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;

    crate::systems::shop::wavedb::preload(cfg, &dir)?;
    // Reopened: the per-type cache is a *write* cache, so everything the
    // preload just wrote would otherwise still be warm and the read phases
    // would measure RAM. The reopen is also where the measured durability is
    // chosen — the fill's window never survives it.
    let store = crate::systems::shop::wavedb::open_measured(&dir, d)?;
    let factory = drv::wavedb::Factory::<E>::new(store, asked)?;
    let used = crate::harness::DriverFactory::consumers(&factory);
    // The consumer count is a **row-identity field**: it is in the digest, and
    // every table prints it. A row that was asked for three and ran on one
    // would be filed under a name that does not describe it, and nothing
    // downstream could tell. `plan::resolve::consumers_for` already clamps
    // what the supervisor asks; this is the same rule where it can be
    // enforced rather than trusted.
    if used != asked {
        return Err(format!(
            "wavedb/{} on shop was asked for {asked} consumers and can \
             honour {used}: the count is part of the row's identity, so \
             measuring it anyway would file the wrong number",
            run.key.variant
        ));
    }

    let points = RefCell::new(Points::of(&dir, wavedb_is_log));
    let phases = crate::harness::run_with(
        &factory,
        ShopWorkload::new(clone_cfg(cfg)),
        || points.borrow_mut().take(Point::Hot),
    )?;
    let mut points = points.into_inner();
    // The engine goes down with the factory: with three consumers over one
    // disk actor, a driver's own `close` cannot be the thing that stops it.
    drop(factory);
    points.take(Point::Settled)?;

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "embedded",
            settings: vec![
                ("tenancy".into(), "one tenant per user".into()),
                (
                    "list".into(),
                    format!("Shopping by bought_at, page = {PAGE}"),
                ),
                ("transaction".into(), "none: one op is one batch".into()),
                ("consumers".into(), used.to_string()),
                ("mode".into(), engine.mode().into()),
                (
                    "relax_window".into(),
                    match d {
                        Durability::Durable => {
                            "0 — one barrier per batch".to_string()
                        }
                        Durability::Relaxed => {
                            format!("{:?}", crate::RELAXED_WINDOW)
                        }
                    },
                ),
            ],
            compression: "zstd (per-type dictionaries)",
            retains_history: true,
            notes: vec![
            "A checkout is one order plus its line items, and WaveDB has no \
             multi-record transaction: it costs one batch — one barrier — per \
             record, where the other four commit the whole checkout once."
                .into(),
            "Retains every superseded version; the other four retain none. \
             Read the write phases beside the footprint, never alone."
                .into(),
        ],
            seed_path: None,
            materialise_ms: 0,
            footprints: points.into_vec(),
        },
    ))
}

/// `ShopCfg` is not `Clone` and the workload wants it by value.
pub(super) fn clone_cfg(cfg: &ShopCfg) -> ShopCfg {
    ShopCfg {
        users: cfg.users,
        signups: cfg.signups,
        checkouts: cfg.checkouts,
        profile_reads: cfg.profile_reads,
        page_reads: cfg.page_reads,
        detail_reads: cfg.detail_reads,
        orders_max: cfg.orders_max,
        items_max: cfg.items_max,
        seed: cfg.seed,
        work_dir: cfg.work_dir.clone(),
    }
}

fn sqlite_err(e: rusqlite::Error) -> String {
    format!("sqlite: {e}")
}
