//! The three shop rows that own a process.
//!
//! Same lifecycle rule as [`server`](super::server): the row starts the
//! server, preloads through it, hands the harness a factory that only ever
//! connects, and stops it after the last driver has closed.
//!
//! The one difference worth naming is MongoDB's: this row starts a **one-node
//! replica set**, because a checkout is a multi-document transaction and a
//! standalone `mongod` has none. The `micro` MongoDB row is standalone. Both
//! declare which they are in their stored settings.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use super::RowRun;
use super::micro::{Meta, record};
use super::points::{Points, mongodb_is_log, mysql_is_log, postgres_is_log};
use super::shop::clone_cfg;
use crate::corpus::RowRecord;
use crate::footprint::Point;
use crate::harness::shop::ShopWorkload;
use crate::systems::drivers::shop as drv;
use crate::systems::server::{CACHE_GB, CACHE_MYSQL, CACHE_POSTGRES, Server};
use crate::systems::{Durability, drivers};

/// # Errors
/// The cluster would not initialise or start, or an operation was refused.
pub fn postgres(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use drivers::postgres as pg;

    let cfg = &run.shop;
    let dir = cfg.work_dir.join(format!("shop-postgres-{}", d.name()));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    pg::init(&dir)?;
    let sync = match d {
        Durability::Durable => "on",
        Durability::Relaxed => "off",
    };
    let data = dir.join("data");
    let mut points = Points::of(&data, postgres_is_log);

    let held = share(pg::start(&dir, sync)?);
    points.take(Point::Baseline)?;
    {
        let mut client = pg::connect_at(&dir)?;
        client
            .batch_execute(drv::postgres::DDL)
            .map_err(|e| format!("postgres: {e}"))?;
        crate::systems::shop::postgres::preload(cfg, &mut client)?;
    }

    let factory = drv::postgres::Factory {
        socket_dir: dir.clone(),
        sync,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, cfg, &mut points)?;

    stop(&held, |s| pg::stop(s, &dir))?;
    points.take(Point::Settled)?;

    let running = share(pg::start(&dir, sync)?);
    pg::compact(&dir)?;
    stop(&running, |s| pg::stop(s, &dir))?;
    points.take(Point::Compacted)?;

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "server",
            settings: vec![
                ("synchronous_commit".into(), sync.into()),
                ("shared_buffers".into(), CACHE_POSTGRES.into()),
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

/// # Errors
/// The datadir would not initialise, or an operation was refused.
pub fn mysql(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use drivers::mysql as mydrv;
    use drivers::mysql_server as my;
    use mysql::prelude::Queryable as _;

    let cfg = &run.shop;
    let dir = cfg.work_dir.join(format!("shop-mysql-{}", d.name()));
    std::fs::create_dir_all(&dir).map_err(|e| format!("mkdir: {e}"))?;
    my::init(&dir)?;
    let flush = match d {
        Durability::Durable => "1",
        Durability::Relaxed => "2",
    };
    let data = dir.join("data");
    let mut points = Points::of(&data, mysql_is_log);

    let held = share(my::start(&dir, flush)?);
    points.take(Point::Baseline)?;
    {
        let mut conn = my::connect_at(&dir, None)?;
        conn.query_drop("CREATE DATABASE shop")
            .map_err(|e| format!("mysql: {e}"))?;
        conn.query_drop("USE shop")
            .map_err(|e| format!("mysql: {e}"))?;
        for ddl in drv::mysql::DDL {
            conn.query_drop(ddl).map_err(|e| format!("mysql: {e}"))?;
        }
        crate::systems::shop::mysql::preload(cfg, &mut conn)?;
    }

    let factory = drv::mysql::Factory {
        dir: dir.clone(),
        database: "shop".into(),
        flush,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, cfg, &mut points)?;

    stop(&held, |s| my::stop(s, &dir))?;
    points.take(Point::Settled)?;

    let running = share(my::start(&dir, flush)?);
    mydrv::compact(&dir, "shop")?;
    stop(&running, |s| my::stop(s, &dir))?;
    points.take(Point::Compacted)?;

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "server",
            settings: vec![
                ("innodb_flush_log_at_trx_commit".into(), flush.into()),
                ("innodb_buffer_pool_size".into(), CACHE_MYSQL.into()),
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

/// # Errors
/// The replica set would not come up, or an operation was refused.
pub fn mongodb(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use drv::mongo_server as mgs;
    use drv::mongodb as mg;

    let cfg = &run.shop;
    let dir = cfg.work_dir.join(format!("shop-mongodb-{}", d.name()));
    let data = dir.join("data");
    std::fs::create_dir_all(&data).map_err(|e| format!("mkdir: {e}"))?;
    let journal = d == Durability::Durable;
    let port = mgs::port();
    let mut points = Points::of(&data, mongodb_is_log);

    let held = share(mgs::start(&dir, port)?);
    points.take(Point::Baseline)?;
    {
        let client = mgs::connect(port, journal)?;
        let db = client.database("shop");
        mg::indexes(&db)?;
        crate::systems::shop::mongodb::preload(cfg, &db)?;
    }

    let factory = mg::Factory {
        dir: dir.clone(),
        port,
        journal,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, cfg, &mut points)?;

    stop(&held, |s| mgs::stop(s, &dir))?;
    points.take(Point::Settled)?;

    let running = share(mgs::start(&dir, port)?);
    compact_mongo(port)?;
    stop(&running, |s| mgs::stop(s, &dir))?;
    points.take(Point::Compacted)?;

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "server",
            settings: vec![
                ("writeConcern".into(), format!("{{ w: 1, j: {journal} }}")),
                ("wiredTigerCacheSizeGB".into(), CACHE_GB.into()),
                ("topology".into(), "one-node replica set".into()),
                ("tenancy".into(), "user_id field + index".into()),
                ("transaction".into(), "one per checkout".into()),
            ],
            compression: "snappy (WiredTiger default)",
            retains_history: false,
            notes: vec![
            "References, not an embedded line-item array: the shape of the \
             data is held equal across all five so the numbers compare \
             storage and access paths rather than modelling choices."
                .into(),
            "A one-node replica set, because a multi-document transaction \
             needs one. The micro MongoDB row is standalone."
                .into(),
        ],
            seed_path: None,
            materialise_ms: 0,
            footprints: points.into_vec(),
        },
    ))
}

/// The three shop collections, compacted one at a time.
fn compact_mongo(port: u16) -> Result<(), String> {
    let client = drv::mongo_server::connect(port, false)?;
    let db = client.database("shop");
    for name in ["users", "shopping", "product"] {
        // `force` because this node is a replica set primary, which
        // `compact` refuses by default "as this will slow down other running
        // operations". There are no other operations: the row is between its
        // phases and its footprint, and slowing down a benchmark nobody is
        // measuring is the entire point of the compacted point.
        db.run_command(mongodb::bson::doc! { "compact": name, "force": true })
            .run()
            .map_err(|e| format!("mongodb: compact {name}: {e}"))?;
    }
    Ok(())
}

/// Run the harness, taking the `hot` point after the last phase and before any
/// driver closes.
fn drive<F>(
    factory: &F,
    cfg: &crate::systems::shop::ShopCfg,
    points: &mut Points,
) -> Result<Vec<crate::harness::PhaseResult>, String>
where
    F: crate::harness::DriverFactory + Sync,
    F::Driver: crate::harness::Driver<Op = crate::plan::op::ShopOp>,
{
    let cell = RefCell::new(points);
    crate::harness::run_with(factory, ShopWorkload::new(clone_cfg(cfg)), || {
        cell.borrow_mut().take(Point::Hot)
    })
}

fn share(server: Server) -> Arc<Mutex<Option<Server>>> {
    Arc::new(Mutex::new(Some(server)))
}

fn stop<F>(held: &Arc<Mutex<Option<Server>>>, how: F) -> Result<(), String>
where
    F: FnOnce(Server) -> Result<(), String>,
{
    let server = held
        .lock()
        .map_err(|_| "the server mutex was poisoned".to_string())?
        .take()
        .ok_or_else(|| "the server is already gone".to_string())?;
    how(server)
}
