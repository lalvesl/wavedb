//! `wavedb/object` — the single row's `micro` workload on a store kept in an
//! object store, its disk caching it (RFC 0070 phase 3), and its opens over
//! the object store ([RFC 0065] §12).
//!
//! ## What it measures
//!
//! The phases are the single row's, with a node's uploads in the driver's
//! upkeep (8 MiB or 10 s, `systems::engine::UPLOAD_AFTER_BYTES`), so the
//! object store fills the way a node's does: many uploads, a long chain of
//! headers. The gateway is a local versitygw on the scratch disk
//! (`systems::object_store`): the requests are what S3 would be asked, the
//! times a local disk's.
//!
//! ## Opens, each on a fork
//!
//! At each point — `written`, everything committed uploaded, and
//! `compacted`, after the disk's compaction and the object store's
//! (`compact_remote`) — the store is opened twice, cold, counting the
//! object store's requests:
//!
//! - **from its disk**, the object store beside it (a node's restart);
//! - **from the object store alone**, in object mode (a node whose disk is
//!   gone, or an object-mode database).
//!
//! Each open runs on a **fork** of the point's state — a copy of the data
//! directory and of the gateway's — with a gateway of its own, so the two
//! start from the same files, neither changes what the other or the next
//! point sees, and a gateway's log holds its open's requests alone. On btrfs
//! the copy is a reflink (`copy_file_range`), so a fork costs metadata.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::cell::RefCell;
use std::path::Path;

use futures::executor::block_on;
use wavedb_storage::{PageStore, StoreOptions};

use super::RowRun;
use super::embedded::{quiesce, settings};
use super::micro::{Meta, record, workload};
use super::opens::cold;
use super::points::{Points, wavedb_is_log};
use crate::corpus::{OpenRecord, RowRecord};
use crate::footprint::Point;
use crate::schema::Thing;
use crate::systems::engine::{Engine, Engineish};
use crate::systems::object_store::ObjectStore;
use crate::systems::{Durability, drivers, wavedb as wavedb_adapter};

/// # Errors
/// The gateway or the store could not be opened, an operation was refused,
/// or an upload or a compaction failed.
pub fn wavedb(run: &RowRun, d: Durability) -> Result<RowRecord, String> {
    let cfg = &run.cfg;
    let dir = cfg.work_dir.join(format!("wavedb-object-{}", d.name()));
    let (data, s3) = (dir.join("data"), dir.join("s3"));
    let log = dir.join("access.log");
    let durable = d == Durability::Durable;

    let server = ObjectStore::start(&s3, &log)?;
    let mut stores = vec![open(&data, server.options())?];
    if !durable {
        wavedb_adapter::relax(&mut stores)?;
    }
    let factory = drivers::wavedb::Factory::<PageStore>::new(
        stores,
        None,
        cfg.rows as usize,
        durable,
    );
    let points = RefCell::new(Points::of(&data, wavedb_is_log));
    let phases =
        crate::harness::run_with(&factory, workload(cfg, false), || {
            points.borrow_mut().take(Point::Hot)
        })?;
    let mut points = points.into_inner();
    drop(factory);

    // Written: what the workload committed, all of it uploaded.
    let store = open(&data, server.options())?;
    block_on(store.upload_all()).map_err(|e| format!("upload: {e}"))?;
    quiesce(std::slice::from_ref(&store), &points)?;
    points.take(Point::Settled)?;
    Engineish::close(store);
    drop(server);
    let mut opens = Vec::new();
    measure(&dir, "written", &mut opens)?;

    // Compacted: the disk's compaction, then the object store's.
    let server = ObjectStore::start(&s3, &log)?;
    let store = open(&data, server.options())?;
    Engineish::compact(&store)?;
    block_on(store.compact_remote())
        .map_err(|e| format!("compact_remote: {e}"))?;
    block_on(store.upload_all()).map_err(|e| format!("upload: {e}"))?;
    quiesce(std::slice::from_ref(&store), &points)?;
    points.take(Point::Compacted)?;
    Engineish::close(store);
    drop(server);
    measure(&dir, "compacted", &mut opens)?;

    let mut settings = settings(Engine::Direct, d, 1);
    settings.push((
        "object_store".into(),
        "versitygw over the row's scratch disk; uploads at 8 MiB or 10 s"
            .into(),
    ));
    let mut notes = wavedb_adapter::notes(false);
    notes.push(
        "Object: the single row's store kept in an object store, its disk \
         caching it (RFC 0070 phase 3), uploading as a node's upkeep does. \
         The object store is a local versitygw: its requests are what S3 \
         would be asked, its times a local disk's."
            .into(),
    );
    let mut row = record(
        run,
        phases,
        Meta {
            bracket: "embedded",
            settings,
            compression: "zstd (per-type dictionaries)",
            retains_history: true,
            notes,
            seed_path: None,
            materialise_ms: 0,
            footprints: points.into_vec(),
        },
    );
    row.opens = opens;
    Ok(row)
}

/// The store at `data`, kept in the object store `options` names.
fn open(data: &Path, options: StoreOptions) -> Result<PageStore, String> {
    PageStore::open_with(data, &Thing::storage_entries(), options)
        .map_err(|e| format!("open: {e}"))
}

/// Open `dir`'s store cold from its disk, then from its object store alone,
/// each on a fork of its own (see the module docs), recording both under
/// `point`.
fn measure(
    dir: &Path,
    point: &str,
    opens: &mut Vec<OpenRecord>,
) -> Result<(), String> {
    for (from, slug) in [("disk", "disk"), ("object store", "object")] {
        let fork = dir.join(format!("fork-{point}-{slug}"));
        let (data, s3) = (fork.join("data"), fork.join("s3"));
        copy_tree(&dir.join("s3"), &s3)?;
        if slug == "disk" {
            copy_tree(&dir.join("data"), &data)?;
        }
        let server = ObjectStore::start(&s3, &fork.join("access.log"))?;
        let options = server.options();
        let (store, record) = if slug == "disk" {
            cold(point, from, &[&data, &s3], Some(&server), || {
                open(&data, options.clone())
            })?
        } else {
            cold(point, from, &[&s3], Some(&server), || {
                PageStore::open_object(
                    &Thing::storage_entries(),
                    options.clone(),
                )
                .map_err(|e| format!("open_object: {e}"))
            })?
        };
        opens.push(record);
        Engineish::close(store);
        drop(server);
        std::fs::remove_dir_all(&fork)
            .map_err(|e| format!("{}: {e}", fork.display()))?;
    }
    Ok(())
}

/// Copy the tree at `from` to `to` — a reflink per file where the
/// filesystem makes one (`std::fs::copy` is `copy_file_range` on Linux).
fn copy_tree(from: &Path, to: &Path) -> Result<(), String> {
    std::fs::create_dir_all(to)
        .map_err(|e| format!("{}: {e}", to.display()))?;
    let entries = std::fs::read_dir(from)
        .map_err(|e| format!("{}: {e}", from.display()))?;
    for entry in entries {
        let path = entry
            .map_err(|e| format!("{}: {e}", from.display()))?
            .path();
        let target = to.join(path.file_name().unwrap_or_default());
        if path.is_dir() {
            copy_tree(&path, &target)?;
        } else {
            std::fs::copy(&path, &target)
                .map_err(|e| format!("{}: {e}", path.display()))?;
        }
    }
    Ok(())
}
