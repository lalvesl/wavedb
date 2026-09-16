//! The MySQL server's lifecycle — the row's, not the driver's.
//!
//! A server outlives every connection made to it: the row initialises the
//! datadir, starts it, and stops it after the last driver has closed, because
//! a data directory read while a server still holds it measures that server's
//! deferral rather than its storage. The driver's only claim on it is the
//! restart at a phase boundary, which is how this system empties the InnoDB
//! buffer pool.

use std::path::{Path, PathBuf};

use mysql::prelude::Queryable as _;
use mysql::{Conn, OptsBuilder};

use crate::systems::server::{self, Server};

use super::mysql::{Shared, sql};

/// The socket path, kept short on purpose: a unix socket is capped at ~107
/// bytes, which a nested temp directory can reach on its own.
#[must_use]
pub fn sock(dir: &Path) -> PathBuf {
    dir.join("s")
}

/// Create an empty datadir.
///
/// # Errors
/// `mysqld --initialize-insecure` refusing.
pub fn init(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir: {e}"))?;
    server::run(
        "mysqld",
        &[
            "--initialize-insecure",
            &format!("--datadir={}", dir.join("data").display()),
            &format!("--log-error={}", dir.join("init.log").display()),
        ],
    )
    .map(|_| ())
}

/// Start a server in `dir` and wait until it answers.
///
/// `flush` is `innodb_flush_log_at_trx_commit`, passed at startup rather than
/// `SET GLOBAL` so the value is what the server ran under from its first
/// write.
///
/// # Errors
/// The process failing to spawn, or never becoming connectable.
pub fn start(dir: &Path, flush: &'static str) -> Result<Server, String> {
    let log = dir.join("mysqld.log");
    let my = Server::spawn(
        "mysqld",
        &[
            &format!("--datadir={}", dir.join("data").display()),
            &format!("--socket={}", sock(dir).display()),
            &format!("--pid-file={}", dir.join("mysqld.pid").display()),
            &format!("--log-error={}", log.display()),
            &format!("--innodb-flush-log-at-trx-commit={flush}"),
            &format!("--innodb-buffer-pool-size={}", server::CACHE_MYSQL),
            // No TCP: the socket is in the run's own directory, so two rows
            // cannot reach each other's server even by accident.
            "--skip-networking",
        ],
        &dir.join("mysqld.out"),
    )?;
    server::wait_for("mysqld", server::STARTUP_SECS, || {
        Conn::new(
            OptsBuilder::new()
                .socket(Some(sock(dir).display().to_string()))
                .user(Some("root")),
        )
        .is_ok()
    })
    .map_err(|e| format!("{e}\n{}", server::log_tail(&log, 10)))?;
    Ok(my)
}

/// Stop with `mysqladmin shutdown`, not a signal: InnoDB's quiescence *is* a
/// clean shutdown — it flushes the buffer pool and completes purge.
///
/// # Errors
/// `mysqladmin` refusing, or the process failing to reap.
pub fn stop(my: Server, dir: &Path) -> Result<(), String> {
    my.stop(
        "mysqladmin",
        &[
            "--socket",
            &sock(dir).display().to_string(),
            "-u",
            "root",
            "shutdown",
        ],
    )
}

/// Connect to an already-running server in `dir`, optionally selecting a
/// database. The shop driver's entry point into the same settings the micro
/// driver uses.
///
/// # Errors
/// The server refused the connection.
pub fn connect_at(dir: &Path, database: Option<&str>) -> Result<Conn, String> {
    let mut conn = Conn::new(
        OptsBuilder::new()
            .socket(Some(sock(dir).display().to_string()))
            .user(Some("root")),
    )
    .map_err(sql)?;
    if let Some(db) = database {
        conn.query_drop(format!("USE {db}")).map_err(sql)?;
    }
    Ok(conn)
}

/// Stop whatever is in `held` and start a replacement under the same
/// durability. The restart is what empties the InnoDB buffer pool.
///
/// # Errors
/// The slot was empty or poisoned, or the server would not come back.
pub fn restart_server(
    held: &Shared,
    dir: &Path,
    flush: &'static str,
) -> Result<(), String> {
    let mut slot = held
        .lock()
        .map_err(|_| "the server mutex was poisoned".to_string())?;
    let old = slot
        .take()
        .ok_or_else(|| "the server is already gone".to_string())?;
    stop(old, dir)?;
    *slot = Some(start(dir, flush)?);
    Ok(())
}
