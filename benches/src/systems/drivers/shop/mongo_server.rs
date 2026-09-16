//! The shop MongoDB row's server lifecycle.
//!
//! Separate from the driver because it **is** separate: a server outlives
//! every connection made to it, so the row starts it, preloads through it, and
//! stops it after the last driver has closed. The driver's only claim on it is
//! the restart at a phase boundary, which is how this system makes a read
//! cold.
//!
//! It starts a **one-node replica set** rather than a standalone `mongod`,
//! because a checkout is a multi-document transaction and a standalone has
//! none. The `micro` MongoDB row is standalone; both say which they are in
//! their stored settings.

use std::path::Path;

use mongodb::bson::doc;
use mongodb::options::{Acknowledgment, ClientOptions, WriteConcern};
use mongodb::sync::Client;

use crate::systems::server::{self, CACHE_GB, Server};

use super::mongodb::Shared;

/// A port derived from this process, so two runs on one machine do not fight
/// — and neither touches a `mongod` the user is running.
#[must_use]
pub fn port() -> u16 {
    27_600 + u16::try_from(std::process::id() % 300).unwrap_or(0)
}

/// # Errors
/// The connection options were rejected.
pub fn connect(port: u16, journal: bool) -> Result<Client, String> {
    let mut opts = ClientOptions::parse(format!("mongodb://127.0.0.1:{port}"))
        .run()
        .map_err(drv)?;
    opts.write_concern = Some(
        WriteConcern::builder()
            .w(Acknowledgment::Nodes(1))
            .journal(journal)
            .build(),
    );
    opts.max_pool_size = Some(1);
    Client::with_options(opts).map_err(drv)
}

/// A client that talks to **this process**, not to whatever topology it
/// claims to belong to.
///
/// `directConnection=true` is load-bearing rather than a hint. A `mongod`
/// started with `--replSet` but not yet initiated reports itself as a replica
/// set member with no configuration, and an ordinary client then finds no
/// server it is willing to select — so the ping used to decide the server is
/// up never gets an answer, and the row dies on a 300-second timeout while a
/// perfectly healthy `mongod` sits there. It is the connection used to
/// *initiate* the set, so by definition it cannot require the set to exist.
fn direct(port: u16) -> Result<Client, String> {
    ClientOptions::parse(format!(
        "mongodb://127.0.0.1:{port}/?directConnection=true"
    ))
    .run()
    .and_then(Client::with_options)
    .map_err(drv)
}

/// Start a **one-node replica set**: multi-document transactions need one, and
/// the checkout is a transaction.
///
/// # Errors
/// The process would not start, or would not become primary.
pub fn start(dir: &Path, port: u16) -> Result<Server, String> {
    let log = dir.join("mongod.log");
    let mongo = Server::spawn(
        "mongod",
        &[
            "--dbpath",
            &s(&dir.join("data")),
            "--bind_ip",
            "127.0.0.1",
            "--port",
            &port.to_string(),
            // Pinned, not inferred: WiredTiger sizes its cache from the HOST's
            // RAM, not the cgroup's, so an unpinned mongod under the cage asks
            // for gigabytes it cannot have and is OOM-killed.
            "--wiredTigerCacheSizeGB",
            CACHE_GB,
            "--replSet",
            "bench",
            "--logpath",
            &s(&log),
        ],
        &dir.join("mongod.out"),
    )?;
    server::wait_for("mongod", server::STARTUP_SECS, || {
        direct(port).is_ok_and(|c| {
            c.database("admin")
                .run_command(doc! { "ping": 1 })
                .run()
                .is_ok()
        })
    })
    .map_err(|e| format!("{e}\n{}", server::log_tail(&log, 10)))?;

    // Idempotent across a restart, where the node is already initiated and
    // answers `AlreadyInitialized`. Every *other* refusal is kept and folded
    // into the primary wait's error, because a swallowed one turns a real
    // failure into a 300-second timeout with no cause attached — which is
    // exactly what it did the first time this row ran.
    let initiate = direct(port).and_then(|c| {
        c.database("admin")
            .run_command(doc! {
                "replSetInitiate": doc! {
                    "_id": "bench",
                    "members": [
                        doc! { "_id": 0, "host": format!("127.0.0.1:{port}") },
                    ],
                }
            })
            .run()
            .map(|_| ())
            .or_else(|e| {
                if e.to_string().contains("already initialized") {
                    Ok(())
                } else {
                    Err(drv(e))
                }
            })
    });
    // Wait for primary, or the first write refuses.
    server::wait_for("mongod primary", server::STARTUP_SECS, || {
        direct(port).is_ok_and(|c| {
            c.database("admin")
                .run_command(doc! { "hello": 1 })
                .run()
                .is_ok_and(|d| d.get_bool("isWritablePrimary").unwrap_or(false))
        })
    })
    .map_err(|e| {
        let why = initiate
            .as_ref()
            .err()
            .map_or_else(String::new, |i| format!("\nreplSetInitiate: {i}"));
        format!("{e}{why}\n{}", server::log_tail(&log, 10))
    })?;
    Ok(mongo)
}

/// # Errors
/// The shutdown command failed.
pub fn stop(mongo: Server, dir: &Path) -> Result<(), String> {
    mongo.stop("mongod", &["--dbpath", &s(&dir.join("data")), "--shutdown"])
}

/// # Errors
/// The slot was empty or poisoned, or the server would not come back.
pub fn restart_server(
    held: &Shared,
    dir: &Path,
    port: u16,
) -> Result<(), String> {
    let mut slot = held
        .lock()
        .map_err(|_| "the server mutex was poisoned".to_string())?;
    let old = slot
        .take()
        .ok_or_else(|| "the server is already gone".to_string())?;
    stop(old, dir)?;
    *slot = Some(start(dir, port)?);
    Ok(())
}

fn s(p: &Path) -> String {
    p.display().to_string()
}

fn drv(e: mongodb::error::Error) -> String {
    format!("mongodb: {e}")
}
