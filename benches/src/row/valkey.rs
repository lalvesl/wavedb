//! The two Valkey rows, `micro` and `shop` — the server bracket's in-memory
//! peer.
//!
//! Same lifecycle as the three disk-backed servers ([`super::server`]): the
//! row starts the process, drivers only connect, a phase boundary restarts
//! it, and the four points are taken around a clean `SHUTDOWN`. Two things
//! differ, and both are recorded rather than smoothed over:
//!
//! - **there is no cache budget.** Every key is resident; the 500 MB cage is
//!   the only bound, and a dataset that does not fit is OOM-killed;
//! - **a restart does not make a read cold.** It replays the AOF into memory,
//!   so `read_cold` (and the shop's read phases after the settle) read a
//!   resident dataset. The restart still happens, so the phase boundary is
//!   the same one every other row crosses.
//!
//! The AOF is Valkey's only copy on disk, so all of it is data: no part of
//! the directory is set aside as log.

use std::sync::Arc;

use super::RowRun;
use super::micro::{Meta, record, workload};
use super::points::{Points, valkey_is_log};
use super::server::{drive, share, stop};
use crate::corpus::RowRecord;
use crate::footprint::Point;
use crate::systems::Durability;
use crate::systems::drivers::valkey_server as vk;

const RESIDENT: &str = "Holds the whole dataset in memory; there is no cache \
     budget to equalise. A restart replays the append-only file, so the \
     phases after it read a resident dataset, not a cold one.";

/// # Errors
/// The server would not start, or an operation was refused.
pub fn micro(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use crate::systems::drivers::valkey as drv;

    let cfg = &run.cfg;
    let dir = cfg.work_dir.join(format!("valkey-{}", d.name()));
    let data = dir.join("data");
    std::fs::create_dir_all(&data).map_err(|e| format!("mkdir: {e}"))?;
    let mut points = Points::of(&data, valkey_is_log);

    let held = share(vk::start(&dir, d)?);
    points.take(Point::Baseline)?;
    let factory = drv::Factory {
        dir: dir.clone(),
        durability: d,
        server: Arc::clone(&held),
    };
    let phases = drive(&factory, workload(cfg, false), &mut points)?;
    settle_and_compact(&held, &dir, d, &mut points)?;

    let mut settings = vk::settings(d);
    settings.push(("index".into(), "a set per tag value, client-kept".into()));
    Ok(record(
        run,
        phases,
        Meta {
            bracket: "server",
            settings,
            compression: "none",
            retains_history: false,
            notes: vec![
                "Retains no superseded versions: HSET overwrites the fields \
                 in place."
                    .into(),
                "An update reads the old tag first (to move the row between \
                 tag sets), so it is two round trips; insert is one."
                    .into(),
                RESIDENT.into(),
            ],
            seed_path: None,
            materialise_ms: 0,
            footprints: points.into_vec(),
        },
    ))
}

/// # Errors
/// The server would not start, the preload failed, or an operation was
/// refused.
pub fn shop(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    use crate::systems::drivers::shop::valkey as drv;

    let cfg = &run.shop;
    let dir = cfg.work_dir.join(format!("shop-valkey-{}", d.name()));
    let data = dir.join("data");
    std::fs::create_dir_all(&data).map_err(|e| format!("mkdir: {e}"))?;
    let mut points = Points::of(&data, valkey_is_log);

    let held = share(vk::start(&dir, d)?);
    points.take(Point::Baseline)?;
    crate::systems::shop::valkey::preload(cfg, &dir)?;
    let factory = drv::Factory {
        dir: dir.clone(),
        durability: d,
        server: Arc::clone(&held),
    };
    let phases = super::shop_server::drive(&factory, cfg, &mut points)?;
    settle_and_compact(&held, &dir, d, &mut points)?;

    let mut settings = vk::settings(d);
    settings.extend([
        ("tenancy".into(), "user_id field + sorted-set index".into()),
        ("transaction".into(), "one MULTI/EXEC per checkout".into()),
    ]);
    Ok(record(
        run,
        phases,
        Meta {
            bracket: "server",
            settings,
            compression: "none",
            retains_history: false,
            notes: vec![
                "The order page is ZRANGE over the user's bought_at index, \
                 then one pipelined HGETALL per order: two round trips. The \
                 detail is three (user + first order, its item index, the \
                 items)."
                    .into(),
                RESIDENT.into(),
            ],
            seed_path: None,
            materialise_ms: 0,
            footprints: points.into_vec(),
        },
    ))
}

/// `settled` after a clean `SHUTDOWN`, then `compacted` after the AOF is
/// rewritten down to the live dataset.
fn settle_and_compact(
    held: &vk::Shared,
    dir: &std::path::Path,
    d: Durability,
    points: &mut Points,
) -> Result<(), String> {
    stop(held, |s| vk::stop(s, dir))?;
    points.take(Point::Settled)?;
    let running = share(vk::start(dir, d)?);
    vk::compact(dir)?;
    stop(&running, |s| vk::stop(s, dir))?;
    points.take(Point::Compacted)
}
