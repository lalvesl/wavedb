//! SQLite on the [`Driver`] seam — the embedded bracket's peer.
//!
//! Every operation runs in its own implicit transaction (rusqlite's autocommit
//! default), which is the fair match for "one WaveDB collection op is one
//! apply batch": both pay their barrier per operation. Batching inserts into
//! one transaction would compare a bulk load against a per-record engine,
//! which is a real difference but a different measurement.
//!
//! ## `prepare_cached`, and what it costs
//!
//! RFC 0060 hoisted one prepared statement out of each phase loop. That cannot
//! survive the move to a driver: a `Statement` borrows its `Connection`, so a
//! struct holding both is self-referential. `prepare_cached` is rusqlite's
//! answer — the statement is still compiled once, and what lands inside the
//! timed window is a hash lookup on the SQL string, tens of nanoseconds.
//!
//! Stated rather than waved away, because it is not free everywhere: against a
//! ~4 ms durable insert it is unmeasurable, against a ~10 µs hot read it is
//! about a percent. It applies identically to every SQLite row, so it cannot
//! bias `durable` against `relaxed`; it is a real, small handicap against the
//! in-process WaveDB rows, and that is the honest way to hold it.

use std::path::PathBuf;

use rusqlite::{Connection, params};

use crate::harness::{Driver, DriverFactory};
use crate::plan::op::MicroOp;

const DDL: &str = "
CREATE TABLE thing (
  id    INTEGER PRIMARY KEY,
  kind  INTEGER NOT NULL,
  score INTEGER NOT NULL,
  name  TEXT    NOT NULL,
  tag   TEXT    NOT NULL,
  body  TEXT    NOT NULL
);
CREATE INDEX idx_thing_tag ON thing(tag);
";

const INSERT: &str = "INSERT INTO thing (id, kind, score, name, tag, body) \
                      VALUES (?1, ?2, ?3, ?4, ?5, ?6)";
const SELECT: &str =
    "SELECT kind, score, name, tag, body FROM thing WHERE id = ?1";
/// Whole-row rewrite, not a `$set`-style partial: WaveDB writes whole records,
/// and comparing a field patch to that would flatter the SQL side for free.
const UPDATE: &str = "UPDATE thing SET kind = ?2, score = ?3, name = ?4, \
                      tag = ?5, body = ?6 WHERE id = ?1";

/// Everything a consumer needs to open its own connection. `Send`, and the
/// only half that crosses.
pub struct Factory {
    pub path: PathBuf,
    /// `FULL` or `NORMAL` — the durability row, in SQLite's own spelling.
    pub sync: &'static str,
    /// True when the schema still has to be created. A seeded row arrives with
    /// the table already loaded by `sqlite3 .import` in the builder.
    pub create: bool,
}

pub struct SqliteDriver {
    conn: Connection,
    path: PathBuf,
    sync: &'static str,
}

impl DriverFactory for Factory {
    type Driver = SqliteDriver;

    /// One, always. SQLite has no shard model, and a second connection would
    /// measure its locking rather than its storage.
    fn consumers(&self) -> usize {
        1
    }

    fn build(&self, _shard: usize) -> Result<SqliteDriver, String> {
        let conn = connect(&self.path, self.sync)?;
        if self.create {
            conn.execute_batch(DDL).map_err(sql)?;
        }
        Ok(SqliteDriver {
            conn,
            path: self.path.clone(),
            sync: self.sync,
        })
    }

    fn route(&self, _op: &MicroOp) -> usize {
        0
    }
}

impl Driver for SqliteDriver {
    type Op = MicroOp;

    fn execute(&mut self, op: MicroOp) -> Result<(), String> {
        match op {
            MicroOp::Insert { n, row } => {
                let mut stmt = self.conn.prepare_cached(INSERT).map_err(sql)?;
                stmt.execute(params![
                    n as i64,
                    row.kind,
                    row.score as i64,
                    row.name,
                    row.tag,
                    row.body
                ])
                .map_err(sql)?;
            }
            MicroOp::Read { n } => {
                let mut stmt = self.conn.prepare_cached(SELECT).map_err(sql)?;
                let name: String = stmt
                    .query_row(params![n as i64], |r| r.get(2))
                    .map_err(sql)?;
                // The read has to prove it read: a `SELECT` that returned no
                // row is a fast operation and a wrong one.
                if name.is_empty() {
                    return Err(format!("row {n} came back empty"));
                }
            }
            MicroOp::Update { n, row } => {
                let mut stmt = self.conn.prepare_cached(UPDATE).map_err(sql)?;
                let touched = stmt
                    .execute(params![
                        n as i64,
                        row.kind,
                        row.score as i64,
                        row.name,
                        row.tag,
                        row.body
                    ])
                    .map_err(sql)?;
                if touched != 1 {
                    return Err(format!("update {n} touched {touched} rows"));
                }
            }
        }
        Ok(())
    }

    fn between_phases(&mut self, done: &str, next: &str) -> Result<(), String> {
        match (done, next) {
            // Quiesce before reading, matching what the WaveDB row does.
            ("insert", _) => checkpoint(&self.conn),
            // Reopen so the connection's own page cache is empty — the
            // counterpart of reopening the WaveDB store. The OS page cache
            // stays warm on both sides, which is why this is `read_cold` and
            // not `read_from_disk`.
            ("read_hot", _) => {
                self.conn = connect(&self.path, self.sync)?;
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Checkpoint on the way out so the footprint that follows measures a
    /// settled database rather than an undrained WAL.
    fn close(self) -> Result<(), String> {
        checkpoint(&self.conn)
    }
}

fn connect(path: &std::path::Path, sync: &str) -> Result<Connection, String> {
    let conn = Connection::open(path).map_err(sql)?;
    // `journal_mode` answers with a row, so it cannot go through
    // `pragma_update`.
    conn.query_row("PRAGMA journal_mode = WAL", [], |r| r.get::<_, String>(0))
        .map_err(sql)?;
    conn.pragma_update(None, "synchronous", sync).map_err(sql)?;
    Ok(conn)
}

fn checkpoint(conn: &Connection) -> Result<(), String> {
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(()))
        .map_err(sql)
}

fn sql(e: rusqlite::Error) -> String {
    format!("sqlite: {e}")
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::Factory;
    use crate::harness::micro::MicroWorkload;
    use crate::harness::{percentiles, run, throughput};

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "wavedb-bench-sqlite-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("scratch");
            Self(dir)
        }

        fn db(&self) -> PathBuf {
            self.0.join("bench.db")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn workload() -> MicroWorkload {
        MicroWorkload {
            rows: 200,
            reads: 100,
            updates: 100,
            seed: 42,
            seeded: false,
        }
    }

    /// The whole seam, end to end: generator, one consumer, four phases, the
    /// boundaries between them, and a real database on disk.
    #[test]
    fn a_full_micro_row_runs_through_the_harness() {
        let scratch = Scratch::new();
        let factory = Factory {
            path: scratch.db(),
            sync: "NORMAL",
            create: true,
        };
        let phases = run(&factory, workload()).expect("run");

        assert_eq!(phases.len(), 4);
        assert_eq!(phases[0].name, "insert");
        assert_eq!(phases[0].samples.len(), 200);
        assert_eq!(phases[1].samples.len(), 100);
        assert_eq!(phases[3].samples.len(), 100);
        assert!(phases.iter().all(|p| p.wall_ns > 0));
    }

    /// Every phase produces a rate and a distribution, and they are the two
    /// different numbers RFC 0065 §4 insists on keeping apart.
    #[test]
    fn a_phase_yields_both_a_rate_and_a_distribution() {
        let scratch = Scratch::new();
        let factory = Factory {
            path: scratch.db(),
            sync: "NORMAL",
            create: true,
        };
        let mut phases = run(&factory, workload()).expect("run");

        let insert = &mut phases[0];
        let rate = throughput(insert.samples.len() as u64, insert.wall_ns);
        assert!(rate > 0.0, "no throughput");
        let (p50, p95, p99, max) = percentiles(&mut insert.samples);
        assert!(p50 > 0 && p50 <= p95 && p95 <= p99 && p99 <= max);
    }

    /// The reopen between the read phases actually happens: the file is still
    /// readable afterwards, and the row completes rather than dying on a
    /// closed connection.
    #[test]
    fn the_row_survives_the_reopen_between_the_read_phases() {
        let scratch = Scratch::new();
        let factory = Factory {
            path: scratch.db(),
            sync: "NORMAL",
            create: true,
        };
        run(&factory, workload()).expect("run");

        let conn = rusqlite::Connection::open(scratch.db()).expect("open");
        let n: i64 = conn
            .query_row("SELECT count(*) FROM thing", [], |r| r.get(0))
            .expect("count");
        assert_eq!(n, 200, "the dataset did not survive the row");
    }

    /// `close` checkpoints, so the footprint measured after a row sees a
    /// settled database rather than an undrained WAL.
    #[test]
    fn the_wal_is_truncated_when_the_row_ends() {
        let scratch = Scratch::new();
        let factory = Factory {
            path: scratch.db(),
            sync: "NORMAL",
            create: true,
        };
        run(&factory, workload()).expect("run");

        let wal = scratch.0.join("bench.db-wal");
        let size = std::fs::metadata(&wal).map_or(0, |m| m.len());
        assert_eq!(size, 0, "{} bytes of WAL left behind", size);
    }

    /// SQLite answers one, and it has to: a second connection would measure
    /// its locking rather than its storage.
    #[test]
    fn sqlite_asks_for_exactly_one_consumer() {
        use crate::harness::DriverFactory;
        let scratch = Scratch::new();
        let factory = Factory {
            path: scratch.db(),
            sync: "FULL",
            create: true,
        };
        assert_eq!(factory.consumers(), 1);
    }
}
