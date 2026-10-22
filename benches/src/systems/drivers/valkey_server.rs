//! The Valkey server's lifecycle — the row's, not the driver's.
//!
//! Valkey keeps the whole dataset in memory and its only copy on disk is the
//! **append-only file**: there is no cache budget to pin, because there is no
//! cache — every key is resident. What it shares with the other servers is
//! the shape: the row starts it, drivers only connect, a phase boundary
//! restarts it (which here reloads the AOF rather than emptying a cache, so
//! `read_cold` is resident too — said in the row's notes, not hidden), and
//! the row stops it with the server's own `SHUTDOWN`.
//!
//! ## The persistence it runs with
//!
//! - `appendonly yes`, `appendfsync always` (durable) or `everysec` (relaxed
//!   — Valkey's own name for the one-second window the others' relaxed rows
//!   use);
//! - `save ""`: no RDB snapshots. A snapshot forks, and a fork in a 500 MB
//!   cage is a copy-on-write experiment rather than a measurement;
//! - `auto-aof-rewrite-percentage 0`: the AOF is rewritten when the row asks
//!   ([`compact`]), never by a background fork inside a measured phase.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::resp::{Reply, Resp};
use crate::systems::Durability;
use crate::systems::server::{self, Server};

/// A running server, shared so a phase boundary can restart it.
pub type Shared = Arc<Mutex<Option<Server>>>;

/// The socket path, kept short on purpose: a unix socket is capped at ~107
/// bytes, which a nested temp directory can reach on its own.
#[must_use]
pub fn sock(dir: &Path) -> PathBuf {
    dir.join("s")
}

/// The `appendfsync` policy a durability row runs under.
#[must_use]
pub const fn fsync(d: Durability) -> &'static str {
    match d {
        Durability::Durable => "always",
        Durability::Relaxed => "everysec",
    }
}

/// What a row records about this server, beside its own.
#[must_use]
pub fn settings(d: Durability) -> Vec<(String, String)> {
    [
        ("appendonly", "yes"),
        ("appendfsync", fsync(d)),
        ("save", "\"\" (no RDB snapshots)"),
        (
            "auto-aof-rewrite-percentage",
            "0 (rewritten by the row only)",
        ),
        ("maxmemory", "unset (the whole dataset is resident)"),
        ("transport", "unix socket"),
        ("client", "hand-rolled RESP2, pipelined"),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v.into()))
    .collect()
}

/// Start a server in `dir` (its AOF under `dir/data`) and wait until it
/// answers — which, after a restart, is after it has replayed the AOF.
///
/// # Errors
/// The process failing to spawn, or never becoming ready.
pub fn start(dir: &Path, d: Durability) -> Result<Server, String> {
    let data = dir.join("data");
    std::fs::create_dir_all(&data).map_err(|e| format!("mkdir: {e}"))?;
    let log = dir.join("valkey.log");
    let owned = [
        sock(dir).display().to_string(),
        data.display().to_string(),
        log.display().to_string(),
    ];
    let mut v = Server::spawn(
        "valkey-server",
        &[
            // No TCP: the socket is in the run's own directory, so two rows
            // cannot reach each other's server even by accident.
            "--port",
            "0",
            "--unixsocket",
            &owned[0],
            "--unixsocketperm",
            "700",
            "--dir",
            &owned[1],
            "--logfile",
            &owned[2],
            "--daemonize",
            "no",
            "--appendonly",
            "yes",
            "--appendfsync",
            fsync(d),
            "--save",
            "",
            "--auto-aof-rewrite-percentage",
            "0",
        ],
        &dir.join("valkey.out"),
    )?;
    // `PING` answers `-LOADING` while the AOF replays; only `+PONG` is ready.
    v.wait_ready("valkey-server", server::STARTUP_SECS, || {
        Resp::connect(&sock(dir))
            .and_then(|mut c| c.call(&[b"PING"]))
            .is_ok_and(|r| r == Reply::Status("PONG".into()))
    })
    .map_err(|e| format!("{e}\n{}", server::log_tail(&log, 10)))?;
    Ok(v)
}

/// Stop with `SHUTDOWN`, the server's own: it flushes and fsyncs the AOF
/// before exiting, which is as quiesced as Valkey gets.
///
/// # Errors
/// `valkey-cli` refusing, or the process failing to reap.
pub fn stop(v: Server, dir: &Path) -> Result<(), String> {
    v.stop(
        "valkey-cli",
        &["-s", &sock(dir).display().to_string(), "SHUTDOWN"],
    )
}

/// Stop whatever is in `held` and start a replacement under the same
/// durability.
///
/// # Errors
/// The slot was empty or poisoned, or the server would not come back.
pub fn restart_server(
    held: &Shared,
    dir: &Path,
    d: Durability,
) -> Result<(), String> {
    let mut slot = held
        .lock()
        .map_err(|_| "the server mutex was poisoned".to_string())?;
    let old = slot
        .take()
        .ok_or_else(|| "the server is already gone".to_string())?;
    stop(old, dir)?;
    *slot = Some(start(dir, d)?);
    Ok(())
}

/// `BGREWRITEAOF`, waited out: the AOF becomes one base file holding the
/// live dataset, which is the floor a `compacted` footprint is asking for.
///
/// # Errors
/// The server refused the rewrite, or never reported it finished.
pub fn compact(dir: &Path) -> Result<(), String> {
    let mut conn = Resp::connect(&sock(dir))?;
    conn.call(&[b"BGREWRITEAOF"])?;
    let deadline = std::time::Instant::now()
        + std::time::Duration::from_secs(server::STARTUP_SECS);
    while std::time::Instant::now() < deadline {
        let info = conn.call(&[b"INFO", b"persistence"])?.bulk()?;
        let info = String::from_utf8_lossy(&info);
        let busy = |field: &str| {
            info.lines().any(|l| l.trim() == format!("{field}:1"))
        };
        if !busy("aof_rewrite_in_progress") && !busy("aof_rewrite_scheduled") {
            return Ok(());
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    Err("valkey: BGREWRITEAOF did not finish".into())
}

/// The **server's** disk writes, read from the shared handle at the start of
/// each phase, so a restart hands the next phase the pid actually running.
#[must_use]
pub fn meter(held: &Shared) -> crate::io_counters::Meter {
    match held.lock() {
        Ok(g) => g.as_ref().map_or(crate::io_counters::Meter::Current, |s| {
            crate::io_counters::Meter::Pid(s.pid)
        }),
        Err(_) => crate::io_counters::Meter::Current,
    }
}
