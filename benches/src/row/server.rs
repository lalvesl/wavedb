//! The three rows that own a process ([RFC 0065] Â§2, Â§7).
//!
//! ## The lifecycle belongs to the row, not to the driver
//!
//! A server outlives every connection made to it. The row starts it, hands
//! the harness a factory that only ever *connects*, and stops it after the
//! last driver has closed â because a data directory read while a server
//! still holds it measures that server's deferral rather than its storage.
//!
//! The one thing a driver does to the server is **restart** it, at a phase
//! boundary, to empty the buffer pool before `read_cold`. That is why the
//! handle is shared (`Arc<Mutex<Option<Server>>>`): `Server::stop` consumes
//! the value, so a restart has to take it out and put a new one back, and the
//! row has to be able to find whatever is running when the phases end.
//!
//! ## The four points
//!
//! `baseline` is taken here and nowhere else: an initialised, running, empty
//! cluster is ~40 MB of PostgreSQL and ~200 MB of WiredTiger journal before a
//! single record exists. Without it the storage column would be read as a
//! statement about the dataset. A seeded row skips it â the seed arrived
//! already filled, so there is no empty state left to measure.
//!
//! `settled` is taken after a **clean shutdown**, which for all three *is*
//! their quiescence: it checkpoints and closes, so nothing measured afterwards
//! is work the server still owed.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

use super::RowRun;
use super::micro::{Meta, materialise, path_of, record, workload};
use super::points::{Points, mongodb_is_log, mysql_is_log, postgres_is_log};
use crate::corpus::RowRecord;
use crate::footprint::Point;
use crate::systems::Durability;
use crate::systems::drivers;
use crate::systems::server::{CACHE_GB, CACHE_MYSQL, CACHE_POSTGRES, Server};

/// # Errors
/// The cluster would not initialise or start, or an operation was refused.
pub fn postgres(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use drivers::postgres as pg;

    let cfg = &run.cfg;
    let dir = cfg.work_dir.join(format!("postgres-{}", d.name()));
    let (seeded, materialise_ms) =
        materialise(&dir, cfg.seed_postgres.as_ref())?;
    if !seeded {
        pg::init(&dir)?;
    }
    let sync = match d {
        Durability::Durable => "on",
        Durability::Relaxed => "off",
    };
    let data = dir.join("data");
    let mut points = Points::of(&data, postgres_is_log);

    let held = share(pg::start(&dir, sync)?);
    if !seeded {
        points.take(Point::Baseline)?;
    }
    let factory = pg::Factory {
        socket_dir: dir.clone(),
        create: !seeded,
        sync,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, workload(cfg, seeded), &mut points)?;

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
                ("transport".into(), "unix socket".into()),
                ("statements".into(), "server-side prepared".into()),
            ],
            compression: "none",
            retains_history: false,
            notes: vec![
            "Retains no superseded versions once vacuumed: an UPDATE writes a \
             new tuple and the dead one is reclaimed."
                .into(),
        ],
            seed_path: path_of(cfg.seed_postgres.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    ))
}

/// # Errors
/// The datadir would not initialise, the server would not start, or an
/// operation was refused.
pub fn mysql(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use drivers::mysql as mydrv;
    use drivers::mysql_server as my;

    let cfg = &run.cfg;
    let dir = cfg.work_dir.join(format!("mysql-{}", d.name()));
    let (seeded, materialise_ms) = materialise(&dir, cfg.seed_mysql.as_ref())?;
    if !seeded {
        my::init(&dir)?;
    }
    // `1` is a flush per commit; `2` hands the write to the OS and flushes
    // once a second â MySQL's own name for relaxed.
    let flush = match d {
        Durability::Durable => "1",
        Durability::Relaxed => "2",
    };
    let data = dir.join("data");
    let mut points = Points::of(&data, mysql_is_log);

    let held = share(my::start(&dir, flush)?);
    if !seeded {
        points.take(Point::Baseline)?;
    }
    let factory = mydrv::Factory {
        dir: dir.clone(),
        database: "bench".into(),
        create: !seeded,
        flush,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, workload(cfg, seeded), &mut points)?;

    stop(&held, |s| my::stop(s, &dir))?;
    points.take(Point::Settled)?;

    let running = share(my::start(&dir, flush)?);
    mydrv::compact(&dir, "bench")?;
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
                ("transport".into(), "unix socket".into()),
                ("statements".into(), "server-side prepared".into()),
            ],
            compression: "none",
            retains_history: false,
            notes: vec![
            "Retains no superseded versions: an UPDATE rewrites in place and \
             the undo record is purged."
                .into(),
        ],
            seed_path: path_of(cfg.seed_mysql.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    ))
}

/// # Errors
/// The server would not start, or an operation was refused.
pub fn mongodb(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use drivers::mongodb as mg;

    let cfg = &run.cfg;
    let dir = cfg.work_dir.join(format!("mongodb-{}", d.name()));
    let (seeded, materialise_ms) =
        materialise(&dir, cfg.seed_mongodb.as_ref())?;
    let data = dir.join("data");
    if !seeded {
        std::fs::create_dir_all(&data).map_err(|e| format!("dbpath: {e}"))?;
    }
    // The write concern's `j` flag is per-operation rather than per-server, so
    // it travels with the write: the durable row really does wait for the
    // journal on every single insert.
    let journal = d == Durability::Durable;
    let port = mg::port();
    let mut points = Points::of(&data, mongodb_is_log);

    let held = share(mg::start(&dir, port)?);
    if !seeded {
        points.take(Point::Baseline)?;
    }
    let factory = mg::Factory {
        dir: dir.clone(),
        port,
        journal,
        create: !seeded,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, workload(cfg, seeded), &mut points)?;

    stop(&held, |s| mg::stop(s, &dir))?;
    points.take(Point::Settled)?;

    let running = share(mg::start(&dir, port)?);
    mg::compact(port)?;
    stop(&running, |s| mg::stop(s, &dir))?;
    points.take(Point::Compacted)?;

    Ok(record(
        run,
        phases,
        Meta {
            bracket: "server",
            settings: vec![
                ("writeConcern".into(), format!("{{ w: 1, j: {journal} }}")),
                ("storage_engine".into(), "WiredTiger".into()),
                ("wiredTigerCacheSizeGB".into(), CACHE_GB.into()),
                ("transport".into(), "loopback TCP".into()),
                ("operation".into(), "one document per request".into()),
            ],
            compression: "snappy (WiredTiger default)",
            retains_history: false,
            notes: vec![
            "Retains no superseded versions: `replace_one` overwrites the \
             document and the previous one is unrecoverable."
                .into(),
            "The only peer that compresses its data by default, which is why \
             the compression column exists at all."
                .into(),
        ],
            seed_path: path_of(cfg.seed_mongodb.as_ref()),
            materialise_ms,
            footprints: points.into_vec(),
        },
    ))
}

/// Run the harness, taking the `hot` point in the one window where it means
/// anything â after the last phase, before any driver closes and before the
/// server is shut down.
fn drive<F, W>(
    factory: &F,
    workload: W,
    points: &mut Points,
) -> Result<Vec<crate::harness::PhaseResult>, String>
where
    F: crate::harness::DriverFactory + Sync,
    W: crate::harness::Workload<Op = <F::Driver as crate::harness::Driver>::Op>,
    W::Op: Send,
{
    let cell = RefCell::new(points);
    crate::harness::run_with(factory, workload, || {
        cell.borrow_mut().take(Point::Hot)
    })
}

fn share(server: Server) -> Arc<Mutex<Option<Server>>> {
    Arc::new(Mutex::new(Some(server)))
}

/// Take the running server out of the shared slot and stop it.
///
/// It is taken rather than borrowed because `Server::stop` consumes the value,
/// and it is read from the slot rather than remembered because a phase
/// boundary may have replaced it with a different process.
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

#[cfg(test)]
mod tests {
    use super::stop;

    /// Stopping a slot that is already empty is a named refusal rather than a
    /// panic — which is what a row failing on its way out would hit, and the
    /// point where an `.expect()` would turn one bad row into no corpus entry
    /// at all.
    #[test]
    fn a_server_that_is_already_gone_is_a_typed_refusal() {
        let held = std::sync::Arc::new(std::sync::Mutex::new(None));
        let err = stop(&held, |_| Ok(())).expect_err("must refuse");
        assert!(err.contains("already gone"), "{err}");
    }
}
