//! MySQL on the [`Driver`] seam.
//!
//! The durability knob is `innodb_flush_log_at_trx_commit`, passed at startup
//! rather than `SET GLOBAL`, so the value is what the server ran under from
//! its first write and not from its first client. The driver carries none of
//! it.
//!
//! Statements are prepared once and held: `mysql::Statement` is an owned
//! handle, so nothing per-operation enters the timed window.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use mysql::prelude::Queryable;
use mysql::{Conn, OptsBuilder, Statement, params};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;
use crate::systems::server::{self, Server};

pub const DDL: &str = "
CREATE TABLE thing (
  id    BIGINT PRIMARY KEY,
  kind  INT     NOT NULL,
  score BIGINT  NOT NULL,
  name  TEXT    NOT NULL,
  tag   VARCHAR(64) NOT NULL,
  body  TEXT    NOT NULL
) ENGINE=InnoDB
";

const INSERT: &str = "INSERT INTO thing (id, kind, score, name, tag, body) \
                      VALUES (:id, :kind, :score, :name, :tag, :body)";
const SELECT: &str = "SELECT name FROM thing WHERE id = :id";
const UPDATE: &str = "UPDATE thing SET kind = :kind, score = :score, \
                      name = :name, tag = :tag, body = :body WHERE id = :id";

/// A running server, shared so a phase boundary can restart it.
pub type Shared = Arc<Mutex<Option<Server>>>;

/// The directory of an already-running server.
pub struct Factory {
    pub dir: PathBuf,
    pub database: String,
    pub create: bool,
    /// `1` or `2` — kept so a restart brings the server back under the same
    /// durability it was measured with.
    pub flush: &'static str,
    pub server: Shared,
}

pub struct MysqlDriver {
    conn: Conn,
    insert: Statement,
    select: Statement,
    update: Statement,
    dir: PathBuf,
    database: String,
    flush: &'static str,
    server: Shared,
}

impl DriverFactory for Factory {
    type Driver = MysqlDriver;

    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<MysqlDriver, String> {
        // Connected **without** a database, then `USE`. A row that is not
        // seeded has no `bench` schema yet, so naming it in the connection
        // would fail before there was anything able to create it.
        let mut conn = Conn::new(
            OptsBuilder::new()
                .socket(Some(sock(&self.dir).display().to_string()))
                .user(Some("root")),
        )
        .map_err(sql)?;
        if self.create {
            conn.query_drop(format!("CREATE DATABASE {}", self.database))
                .map_err(sql)?;
        }
        conn.query_drop(format!("USE {}", self.database))
            .map_err(sql)?;
        if self.create {
            conn.query_drop(DDL).map_err(sql)?;
            conn.query_drop("CREATE INDEX idx_thing_tag ON thing(tag)")
                .map_err(sql)?;
        }
        Ok(MysqlDriver {
            insert: conn.prep(INSERT).map_err(sql)?,
            select: conn.prep(SELECT).map_err(sql)?,
            update: conn.prep(UPDATE).map_err(sql)?,
            conn,
            dir: self.dir.clone(),
            database: self.database.clone(),
            flush: self.flush,
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

impl Driver for MysqlDriver {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        match op {
            MicroOp::Insert { n, row } => {
                self.conn
                    .exec_drop(
                        &self.insert,
                        params! {
                            "id" => n,
                            "kind" => row.kind,
                            "score" => row.score,
                            "name" => row.name,
                            "tag" => row.tag,
                            "body" => row.body,
                        },
                    )
                    .map_err(sql)?;
            }
            MicroOp::Read { n } => {
                let got: Option<String> = self
                    .conn
                    .exec_first(&self.select, params! { "id" => n })
                    .map_err(sql)?;
                if got.is_none() {
                    return Err(format!("row {n} came back empty"));
                }
            }
            MicroOp::Update { n, row } => {
                self.conn
                    .exec_drop(
                        &self.update,
                        params! {
                            "id" => n,
                            "kind" => row.kind,
                            "score" => row.score,
                            "name" => row.name,
                            "tag" => row.tag,
                            "body" => row.body,
                        },
                    )
                    .map_err(sql)?;
                if self.conn.affected_rows() != 1 {
                    return Err(format!(
                        "update {n} touched {} rows",
                        self.conn.affected_rows()
                    ));
                }
            }
        }
        Ok(())
    }

    /// The boundary that matters is a **server** restart: the InnoDB buffer
    /// pool lives in the server, not the connection, so reconnecting would
    /// leave the cache exactly as hot as it was.
    fn between_phases(
        &mut self,
        done: &str,
        _next: &str,
    ) -> Result<(), String> {
        match done {
            "insert" => self.conn.query_drop("FLUSH TABLES").map_err(sql),
            "read_hot" => self.restart(),
            _ => Ok(()),
        }
    }

    fn close(self) -> Result<(), String> {
        drop(self.conn);
        Ok(())
    }
}

fn sql(e: mysql::Error) -> String {
    format!("mysql: {e}")
}

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

impl MysqlDriver {
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
        stop(old, &self.dir)?;
        *slot = Some(start(&self.dir, self.flush)?);
        drop(slot);

        self.conn = Conn::new(
            OptsBuilder::new()
                .socket(Some(sock(&self.dir).display().to_string()))
                .user(Some("root")),
        )
        .map_err(sql)?;
        self.conn
            .query_drop(format!("USE {}", self.database))
            .map_err(sql)?;
        self.insert = self.conn.prep(INSERT).map_err(sql)?;
        self.select = self.conn.prep(SELECT).map_err(sql)?;
        self.update = self.conn.prep(UPDATE).map_err(sql)?;
        Ok(())
    }
}

/// `OPTIMIZE TABLE` — InnoDB rebuilds the tablespace, which is the floor a
/// `compacted` footprint is asking for.
///
/// # Errors
/// The server refused the connection or the rebuild.
pub fn compact(dir: &Path, database: &str) -> Result<(), String> {
    let mut conn = Conn::new(
        OptsBuilder::new()
            .socket(Some(sock(dir).display().to_string()))
            .user(Some("root"))
            .db_name(Some(database.to_string())),
    )
    .map_err(sql)?;
    conn.query_drop("OPTIMIZE TABLE thing").map_err(sql)
}
