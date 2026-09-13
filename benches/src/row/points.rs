//! The four footprint points, per system ([RFC 0065] §7).
//!
//! ## Why the row owns them and the driver does not
//!
//! A footprint is a property of a **data directory**, not of a connection.
//! Three of the four points can only be read when no driver holds the store:
//! `settled` after a clean shutdown, `compacted` after a rewrite that needs
//! the database to itself. The fourth, `hot`, can only be read *before* any
//! driver closes — every `close` in this suite quiesces something, so a
//! reading taken afterwards is `settled` wearing the wrong name.
//!
//! So the sequence belongs to the row, and the harness only lends it the one
//! window it cannot take alone
//! ([`run_with`](crate::harness::run_with)).
//!
//! ## What `is_log` separates, and why every system needs one
//!
//! Each system preallocates recovery capacity that has nothing to do with the
//! dataset: PostgreSQL's `pg_wal` is ~80 MB whether the table holds 200 000
//! rows or 20, WiredTiger's journal is 200 MB beside a 22 MB collection.
//! Counting that as stored data would say the peers are enormous and WaveDB is
//! tiny, which is a statement about default preallocation rather than about
//! storage. Every predicate here separates the same thing under each system's
//! own spelling.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::path::Path;

use crate::corpus::FootprintRecord;
use crate::footprint::{Footprint, IsLog, Point};

/// The points one row collected, in the order they were taken.
pub struct Points {
    taken: Vec<(String, FootprintRecord)>,
    dir: std::path::PathBuf,
    is_log: IsLog,
}

impl Points {
    /// Measure `dir`, separating recovery capacity with `is_log`.
    #[must_use]
    pub fn of(dir: &Path, is_log: IsLog) -> Self {
        Self {
            taken: Vec::new(),
            dir: dir.to_path_buf(),
            is_log,
        }
    }

    /// Take one reading now.
    ///
    /// # Errors
    /// The directory could not be walked.
    pub fn take(&mut self, point: Point) -> Result<(), String> {
        let f = Footprint::split(&self.dir, self.is_log)
            .map_err(|e| format!("footprint at {}: {e}", point.name()))?;
        self.taken.push((point.name().to_string(), record(&f)));
        Ok(())
    }

    /// Read the directory **without** recording a point.
    ///
    /// What a quiescence loop needs: it has to watch the size settle, and
    /// every intermediate reading it took would otherwise become a column in
    /// the corpus.
    ///
    /// # Errors
    /// The directory could not be walked.
    pub fn probe(&self) -> Result<u64, String> {
        Footprint::split(&self.dir, self.is_log)
            .map(|f| f.allocated_bytes)
            .map_err(|e| format!("footprint probe: {e}"))
    }

    #[must_use]
    pub fn into_vec(self) -> Vec<(String, FootprintRecord)> {
        self.taken
    }
}

fn record(f: &Footprint) -> FootprintRecord {
    FootprintRecord {
        apparent_bytes: f.apparent_bytes,
        allocated_bytes: f.allocated_bytes,
        log_bytes: f.log_bytes,
        files: f.files,
    }
}

/// SQLite: the WAL and its shared-memory index.
#[must_use]
pub fn sqlite_is_log(path: &Path) -> bool {
    path.file_name().is_some_and(|n| {
        let n = n.to_string_lossy();
        n.ends_with("-wal") || n.ends_with("-shm")
    })
}

/// WaveDB: `journal_*`. A retired journal is 29 bytes and nothing here is
/// preallocated, so this column exists on this row only to keep the
/// comparison symmetric — every system's recovery area is separated from its
/// data.
#[must_use]
pub fn wavedb_is_log(path: &Path) -> bool {
    path.file_name()
        .is_some_and(|n| n.to_string_lossy().starts_with("journal_"))
}

/// PostgreSQL: `pg_wal`, preallocated recovery capacity.
#[must_use]
pub fn postgres_is_log(path: &Path) -> bool {
    path.components().any(|c| c.as_os_str() == "pg_wal")
}

/// MySQL: the redo log, the binary log and InnoDB's doublewrite buffer — all
/// preallocated, none of them this dataset.
#[must_use]
pub fn mysql_is_log(path: &Path) -> bool {
    let named = |n: &str| {
        n.starts_with("ib_logfile")
            || n.starts_with("#ib_")
            || n.starts_with("binlog")
            || n.starts_with("undo_")
            || n == "ib_buffer_pool"
    };
    path.components().any(|c| c.as_os_str() == "#innodb_redo")
        || path
            .file_name()
            .is_some_and(|n| named(&n.to_string_lossy()))
}

/// MongoDB: WiredTiger's journal (200 MB preallocated) and its FTDC
/// telemetry, which is diagnostics rather than stored data.
#[must_use]
pub fn mongodb_is_log(path: &Path) -> bool {
    path.components().any(|c| {
        c.as_os_str() == "journal" || c.as_os_str() == "diagnostic.data"
    })
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{
        Points, mongodb_is_log, mysql_is_log, postgres_is_log, sqlite_is_log,
        wavedb_is_log,
    };
    use crate::footprint::Point;

    #[test]
    fn sqlite_separates_the_wal_and_its_index() {
        assert!(sqlite_is_log(Path::new("/d/bench.db-wal")));
        assert!(sqlite_is_log(Path::new("/d/bench.db-shm")));
        assert!(!sqlite_is_log(Path::new("/d/bench.db")));
    }

    #[test]
    fn wavedb_separates_its_journals_from_data_bin() {
        assert!(wavedb_is_log(Path::new("/d/journal_0001")));
        assert!(!wavedb_is_log(Path::new("/d/data.bin")));
    }

    /// The 80 MB that is there whether the table holds 200 000 rows or 20.
    #[test]
    fn postgres_separates_pg_wal_wherever_it_appears() {
        assert!(postgres_is_log(Path::new("/d/pg_wal/000000010000")));
        assert!(!postgres_is_log(Path::new("/d/base/16384/2619")));
    }

    #[test]
    fn mysql_separates_every_preallocated_log_it_has() {
        for p in [
            "/d/#innodb_redo/#ib_redo1",
            "/d/ib_logfile0",
            "/d/binlog.000001",
            "/d/undo_001",
            "/d/ib_buffer_pool",
        ] {
            assert!(mysql_is_log(Path::new(p)), "{p}");
        }
        assert!(!mysql_is_log(Path::new("/d/bench/thing.ibd")));
    }

    #[test]
    fn mongodb_separates_the_journal_and_the_telemetry() {
        assert!(mongodb_is_log(Path::new("/d/journal/WiredTigerLog.0001")));
        assert!(mongodb_is_log(Path::new("/d/diagnostic.data/metrics")));
        assert!(!mongodb_is_log(Path::new("/d/collection-0-123.wt")));
    }

    /// The names are what a reader's tooling keys on, and they arrive in the
    /// order the points were taken — `hot` before `settled` is the whole
    /// point of the pair.
    #[test]
    fn points_come_out_named_and_in_order() {
        let dir = std::env::temp_dir()
            .join(format!("wavedb-bench-points-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch");
        std::fs::write(dir.join("bench.db"), [0u8; 4096]).expect("data");
        std::fs::write(dir.join("bench.db-wal"), [0u8; 8192]).expect("wal");

        let mut p = Points::of(&dir, sqlite_is_log);
        p.take(Point::Hot).expect("hot");
        p.take(Point::Settled).expect("settled");
        let taken = p.into_vec();
        let names: Vec<&str> = taken.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["hot", "settled"]);
        // And the WAL landed in the log column rather than in the data one,
        // which is the whole reason the predicate is passed in.
        assert_eq!(taken[0].1.log_bytes, 8192);
        assert_eq!(taken[0].1.files, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_directory_that_is_not_there_is_an_error_naming_its_point() {
        let mut p = Points::of(Path::new("/nonexistent/bench"), wavedb_is_log);
        let err = p.take(Point::Baseline).expect_err("must fail");
        assert!(err.contains("baseline"), "{err}");
    }
}
