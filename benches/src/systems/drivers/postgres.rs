//! PostgreSQL on the [`Driver`] seam.
//!
//! The durability knob is a **server** flag (`synchronous_commit`), passed at
//! startup rather than `SET` by a client, so the value is what the server ran
//! under from its first write. The driver therefore carries no durability of
//! its own: it connects, and the socket it connects to already decided.
//!
//! Statements are prepared once and held. `postgres::Statement` is an owned
//! handle rather than a borrow of the client, so — unlike the SQLite driver —
//! nothing per-operation lands inside the timed window. Both are the honest
//! shape for their API; the difference is stated in `drivers::sqlite`.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use postgres::{Client, NoTls, Statement};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;
use crate::systems::server::{self, Server};

pub const DDL: &str = "
CREATE TABLE thing (
  id    BIGINT PRIMARY KEY,
  kind  INTEGER NOT NULL,
  score BIGINT  NOT NULL,
  name  TEXT    NOT NULL,
  tag   TEXT    NOT NULL,
  body  TEXT    NOT NULL
);
CREATE INDEX idx_thing_tag ON thing(tag);
";

const INSERT: &str = "INSERT INTO thing (id, kind, score, name, tag, body) \
                      VALUES ($1, $2, $3, $4, $5, $6)";
const SELECT: &str = "SELECT name FROM thing WHERE id = $1";
/// Whole-row rewrite, not a partial: WaveDB writes whole records.
const UPDATE: &str = "UPDATE thing SET kind = $2, score = $3, name = $4, \
                      tag = $5, body = $6 WHERE id = $1";

/// A running server, and the handle that lets a phase boundary restart it.
///
/// The row starts the server and stops it; the driver only ever **restarts**
/// it, and only at a phase boundary. Sharing it through an `Arc<Mutex<_>>` is
/// what makes both true at once: `Server::stop` consumes the value, so a
/// restart has to take it out and put a new one back.
pub type Shared = Arc<Mutex<Option<Server>>>;

/// The socket directory of an already-running server.
pub struct Factory {
    pub socket_dir: PathBuf,
    pub create: bool,
    /// `on` or `off` — kept so a restart brings the server back under the
    /// same durability it was measured with.
    pub sync: &'static str,
    pub server: Shared,
}

pub struct PostgresDriver {
    client: Client,
    insert: Statement,
    select: Statement,
    update: Statement,
    socket_dir: PathBuf,
    sync: &'static str,
    server: Shared,
}

impl DriverFactory for Factory {
    type Driver = PostgresDriver;

    /// One. The workload is sequential, and a second connection would measure
    /// concurrency the other adapters do not have.
    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<PostgresDriver, String> {
        let mut client = connect(&self.socket_dir)?;
        if self.create {
            client.batch_execute(DDL).map_err(sql)?;
        }
        Ok(PostgresDriver {
            insert: client.prepare(INSERT).map_err(sql)?,
            select: client.prepare(SELECT).map_err(sql)?,
            update: client.prepare(UPDATE).map_err(sql)?,
            client,
            socket_dir: self.socket_dir.clone(),
            sync: self.sync,
            server: Arc::clone(&self.server),
        })
    }

    fn route(&self, _op: &MicroOp) -> usize {
        0
    }

    /// The **server's** disk writes, not this process's. Read from the shared
    /// handle at the start of each phase, so a restart at a phase boundary
    /// hands the next phase the pid that is actually running.
    fn writer(&self) -> crate::metrics::Writer {
        match self.server.lock() {
            Ok(g) => g.as_ref().map_or(crate::metrics::Writer::Current, |s| {
                crate::metrics::Writer::Pid(s.pid)
            }),
            // A poisoned mutex means a consumer panicked mid-restart; the row
            // is already failing, and a wrong pid would not make it clearer.
            Err(_) => crate::metrics::Writer::Current,
        }
    }
}

impl Driver for PostgresDriver {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        match op {
            MicroOp::Insert { n, row } => {
                self.client
                    .execute(
                        &self.insert,
                        &[
                            &(n as i64),
                            &(row.kind as i32),
                            &(row.score as i64),
                            &row.name,
                            &row.tag,
                            &row.body,
                        ],
                    )
                    .map_err(sql)?;
            }
            MicroOp::Read { n } => {
                let got = self.client.query_opt(&self.select, &[&(n as i64)]);
                let row = got.map_err(sql)?;
                // A `SELECT` that found nothing is a fast operation and a
                // wrong one.
                if row.is_none() {
                    return Err(format!("row {n} came back empty"));
                }
            }
            MicroOp::Update { n, row } => {
                let touched = self
                    .client
                    .execute(
                        &self.update,
                        &[
                            &(n as i64),
                            &(row.kind as i32),
                            &(row.score as i64),
                            &row.name,
                            &row.tag,
                            &row.body,
                        ],
                    )
                    .map_err(sql)?;
                if touched != 1 {
                    return Err(format!("update {n} touched {touched} rows"));
                }
            }
        }
        Ok(())
    }

    /// The boundaries PostgreSQL needs, and one of them is a **server**
    /// restart rather than a client reopen.
    ///
    /// `shared_buffers` lives in the server, not in the connection, so
    /// reconnecting would leave the cache exactly as hot as it was. Emptying
    /// it means stopping and starting the process — which is the counterpart
    /// of reopening the WaveDB store and of SQLite reconnecting, and is why
    /// `read_cold` means the same thing on all three. The OS page cache stays
    /// warm throughout, on every system.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        match done {
            // Force the checkpoint the read phase would otherwise pay for.
            "insert" => self.client.batch_execute("CHECKPOINT").map_err(sql),
            "read_hot" => self.restart(),
            _ => Ok(()),
        }
    }

    /// The client is dropped, which closes the backend. The **server** is
    /// stopped by whoever started it: it outlives every driver, because a
    /// clean shutdown *is* PostgreSQL's quiescence and the footprint is
    /// measured after it.
    fn close(self) -> Result<(), String> {
        drop(self.client);
        Ok(())
    }
}

fn sql(e: postgres::Error) -> String {
    format!("postgres: {e}")
}

impl PostgresDriver {
    /// Stop the server, start it again, and reconnect.
    ///
    /// Takes the `Server` out of the shared slot because `Server::stop`
    /// consumes it, and puts the replacement back — so whoever started it can
    /// still stop it at the end of the row.
    fn restart(&mut self) -> Result<(), String> {
        let mut slot = self
            .server
            .lock()
            .map_err(|_| "the server mutex was poisoned".to_string())?;
        let old = slot
            .take()
            .ok_or_else(|| "the server is already gone".to_string())?;
        stop(old, &self.socket_dir)?;
        *slot = Some(start(&self.socket_dir, self.sync)?);
        drop(slot);

        self.client = connect(&self.socket_dir)?;
        self.insert = self.client.prepare(INSERT).map_err(sql)?;
        self.select = self.client.prepare(SELECT).map_err(sql)?;
        self.update = self.client.prepare(UPDATE).map_err(sql)?;
        Ok(())
    }
}

/// Start a server in `dir`, and wait until it answers.
///
/// # Errors
/// The process failing to spawn, or never becoming connectable.
pub fn start(dir: &Path, sync: &'static str) -> Result<Server, String> {
    let log = dir.join("postgres.log");
    let pg = Server::spawn(
        "postgres",
        &[
            "-D",
            &dir.join("data").display().to_string(),
            "-k",
            &dir.display().to_string(),
            // No TCP: the socket is in the run's own directory, so two rows
            // cannot reach each other's server even by accident.
            "-c",
            "listen_addresses=",
            "-c",
            &format!("synchronous_commit={sync}"),
            "-c",
            &format!("shared_buffers={}", server::CACHE_POSTGRES),
        ],
        &log,
    )?;
    server::wait_for("postgres", server::STARTUP_SECS, || connect(dir).is_ok())
        .map_err(|e| format!("{e}\n{}", server::log_tail(&log, 10)))?;
    Ok(pg)
}

/// Stop with `pg_ctl`, not a signal: it means "checkpoint and close", and a
/// signal only means "die".
///
/// # Errors
/// `pg_ctl` refusing, or the process failing to reap.
pub fn stop(pg: Server, dir: &Path) -> Result<(), String> {
    pg.stop(
        "pg_ctl",
        &["-D", &dir.join("data").display().to_string(), "-w", "stop"],
    )
}

/// Create an empty cluster.
///
/// # Errors
/// `initdb` refusing.
pub fn init(dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("mkdir: {e}"))?;
    server::run(
        "initdb",
        &[
            "-D",
            &dir.join("data").display().to_string(),
            "--no-locale",
            "--encoding=UTF8",
            "-U",
            "bench",
        ],
    )
    .map(|_| ())
}

fn connect(dir: &Path) -> Result<Client, String> {
    Client::connect(
        &format!("host={} user=bench dbname=postgres", dir.display()),
        NoTls,
    )
    .map_err(sql)
}

/// `VACUUM FULL` — rewrite the heap into a fresh file, which is the floor a
/// `compacted` footprint is asking for. Needs the table to itself, so it runs
/// with no driver connected.
///
/// # Errors
/// The server refused the connection or the rewrite.
pub fn compact(dir: &Path) -> Result<(), String> {
    let mut client = connect(dir)?;
    client.batch_execute("VACUUM FULL").map_err(sql)
}
