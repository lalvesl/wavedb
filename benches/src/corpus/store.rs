//! Where rows live on disk, and how they get there safely.
//!
//! `benches/results/rows/<host-key>/<digest>.json` — the host lane is a
//! directory because rows only ever compare within one, and the digest is the
//! file name because it *is* the identity ([RFC 0065] §1). Nothing else about
//! a row needs to be in its path: the file says what it is.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::path::{Path, PathBuf};

use super::record::{RowKey, RowRecord};

/// The stored rows, rooted at `results/rows`.
pub struct Corpus {
    root: PathBuf,
}

/// A row that could not be read, and why.
///
/// Kept beside the rows that *did* read rather than aborting the load: one
/// unreadable file must not hide fifty good ones from the evolution table.
/// But it is returned rather than swallowed, because a corrupt row is a fact
/// about the corpus and silently remeasuring it would launder that.
#[derive(Debug, Clone)]
pub struct Unreadable {
    pub path: PathBuf,
    pub why: String,
}

impl Corpus {
    /// The corpus under `results_dir` (typically `benches/results`).
    #[must_use]
    pub fn at(results_dir: &Path) -> Self {
        Self {
            root: results_dir.join("rows"),
        }
    }

    /// Where `key`'s row is, or would be.
    #[must_use]
    pub fn path_of(&self, key: &RowKey) -> PathBuf {
        self.root
            .join(&key.host_key)
            .join(format!("{}.json", key.digest()))
    }

    /// Whether a row for `key` is stored.
    ///
    /// Presence only — a file that exists but does not parse answers `true`
    /// here and fails in [`load`](Self::load), which is the split the planner
    /// wants: "is there something to reuse" and "is it usable" are different
    /// questions and only the second one should be able to stop a run.
    #[must_use]
    pub fn has(&self, key: &RowKey) -> bool {
        self.path_of(key).is_file()
    }

    /// Read `key`'s row.
    ///
    /// `Ok(None)` means **absent** — measure it. `Err` means present and
    /// unusable, which is not the same thing and must not be treated as one:
    /// a truncated or hand-edited row that quietly remeasured would erase the
    /// evidence that it was ever wrong.
    ///
    /// # Errors
    /// The file exists but cannot be read, parsed, or does not match its own
    /// digest.
    pub fn load(&self, key: &RowKey) -> Result<Option<RowRecord>, String> {
        let path = self.path_of(key);
        if !path.is_file() {
            return Ok(None);
        }
        read_row(&path).map(Some)
    }

    /// Write `record` to its own path, creating the host lane if needed.
    ///
    /// **Atomic**: the bytes go to a temporary file in the same directory and
    /// are then renamed over the target, so a run killed mid-write leaves
    /// either the old row or the new one and never half of either. This is not
    /// caution for its own sake — a partially written row is exactly the input
    /// [`load`](Self::load) would have to reject, and a benchmark that can be
    /// interrupted by an OOM kill (which is the point of the cage) will be.
    ///
    /// An existing row **is replaced**. That is what `--refresh` means; a run
    /// that is not refreshing never reaches here for a row it already has,
    /// because the planner filtered it out first.
    ///
    /// # Errors
    /// Any filesystem fault along the way.
    pub fn store(&self, record: &RowRecord) -> Result<PathBuf, String> {
        let path = self.path_of(&record.key);
        let dir = path.parent().ok_or("row path has no directory")?;
        std::fs::create_dir_all(dir)
            .map_err(|e| format!("mkdir {}: {e}", dir.display()))?;

        // Same directory, so the rename cannot cross a filesystem — a rename
        // across one is a copy, and a copy is not atomic.
        let tmp = dir.join(format!(
            ".{}.{}.tmp",
            record.key.digest(),
            std::process::id()
        ));
        std::fs::write(&tmp, record.to_json())
            .map_err(|e| format!("write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &path).map_err(|e| {
            let _ = std::fs::remove_file(&tmp);
            format!("rename into {}: {e}", path.display())
        })?;
        Ok(path)
    }

    /// Every row in one host lane, for the tables that read across commits.
    ///
    /// Unreadable files are collected rather than thrown: see [`Unreadable`].
    /// A lane that does not exist yet is an empty corpus, not an error — the
    /// first run on a new machine has to start somewhere.
    ///
    /// # Errors
    /// The lane exists but cannot be listed.
    pub fn rows_for_host(
        &self,
        host_key: &str,
    ) -> Result<(Vec<RowRecord>, Vec<Unreadable>), String> {
        let lane = self.root.join(host_key);
        if !lane.is_dir() {
            return Ok((Vec::new(), Vec::new()));
        }
        let entries = std::fs::read_dir(&lane)
            .map_err(|e| format!("read {}: {e}", lane.display()))?;

        let mut rows = Vec::new();
        let mut bad = Vec::new();
        for entry in entries.flatten() {
            let path = entry.path();
            // `.tmp` files are a `store` that was interrupted; skipping them
            // is right, and reporting them would be noise about a write that
            // already failed loudly.
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            match read_row(&path) {
                Ok(row) => rows.push(row),
                Err(why) => bad.push(Unreadable { path, why }),
            }
        }
        // Deterministic order regardless of what `read_dir` felt like, so a
        // rendered table does not shuffle between runs.
        rows.sort_by(|a, b| {
            a.timestamp
                .cmp(&b.timestamp)
                .then_with(|| a.key.digest().cmp(&b.key.digest()))
        });
        Ok((rows, bad))
    }

    /// Every host lane present, for a report that spans machines by naming
    /// them rather than by mixing them.
    ///
    /// # Errors
    /// The corpus root exists but cannot be listed.
    pub fn hosts(&self) -> Result<Vec<String>, String> {
        if !self.root.is_dir() {
            return Ok(Vec::new());
        }
        let entries = std::fs::read_dir(&self.root)
            .map_err(|e| format!("read {}: {e}", self.root.display()))?;
        let mut hosts: Vec<String> = entries
            .flatten()
            .filter(|e| e.path().is_dir())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        hosts.sort();
        Ok(hosts)
    }
}

fn read_row(path: &Path) -> Result<RowRecord, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read {}: {e}", path.display()))?;
    super::decode::from_json(&text)
        .map_err(|e| format!("{}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::Corpus;
    use crate::corpus::{RowKey, RowRecord};

    /// A scratch directory of this test's own, removed on drop so a failing
    /// assertion does not leak one into `/tmp` on every run.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "wavedb-bench-corpus-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&dir).expect("scratch");
            Self(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn key(sha: Option<&str>, tier: &str) -> RowKey {
        RowKey {
            system: if sha.is_some() { "wavedb" } else { "postgres" }.into(),
            system_version: "0.1.0".into(),
            variant: "single".into(),
            durability: "durable".into(),
            workload: "micro".into(),
            tier: tier.into(),
            dataset_revision: 1,
            generator_seed: 42,
            consumers: 1,
            host_key: "i5-8300h-4c-500m-btrfs-e636".into(),
            cage_revision: 1,
            wavedb_git_sha: sha.map(Into::into),
        }
    }

    fn record(key: RowKey, timestamp: &str) -> RowRecord {
        RowRecord {
            key,
            timestamp: timestamp.into(),
            dirty: false,
            caged: true,
            forced: false,
            bracket: "embedded".into(),
            compression: "none".into(),
            retains_history: false,
            settings: Vec::new(),
            phases: Vec::new(),
            footprints: Vec::new(),
            live_records: 200_000,
            logical_bytes: 27_500_000,
            notes: Vec::new(),
            seed_path: None,
            materialise_ms: 0,
        }
    }

    #[test]
    fn a_stored_row_reads_back_identical() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let row = record(key(Some("04085ec"), "large"), "2026-09-04T04-39Z");

        assert!(!corpus.has(&row.key), "empty corpus claimed a row");
        assert_eq!(corpus.load(&row.key).expect("load"), None);

        corpus.store(&row).expect("store");
        assert!(corpus.has(&row.key));
        assert_eq!(corpus.load(&row.key).expect("load"), Some(row));
    }

    /// The path is the identity: lane by host, file by digest, nothing else.
    #[test]
    fn a_row_is_filed_by_host_lane_and_digest() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let k = key(Some("04085ec"), "large");
        let path = corpus.path_of(&k);
        assert!(path.ends_with(format!("{}.json", k.digest())));
        assert_eq!(
            path.parent().and_then(|p| p.file_name()),
            Some(std::ffi::OsStr::new(&k.host_key))
        );
    }

    /// Absent and broken are different answers, and the planner acts on them
    /// differently: absent means measure, broken means tell someone.
    #[test]
    fn a_corrupt_row_fails_rather_than_reading_as_absent() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let row = record(key(Some("04085ec"), "large"), "2026-09-04T04-39Z");
        let path = corpus.store(&row).expect("store");

        std::fs::write(&path, "{\"schema\": \"wavedb-bench/2\", ")
            .expect("truncate");
        assert!(corpus.has(&row.key), "the file is still there");
        let err = corpus.load(&row.key).expect_err("must not read as absent");
        assert!(err.contains("end of input"), "{err}");
    }

    /// A refresh replaces the row at that identity — same digest, new
    /// measurement.
    #[test]
    fn storing_again_replaces_the_row() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let k = key(Some("04085ec"), "large");
        corpus
            .store(&record(k.clone(), "2026-09-04T01-00Z"))
            .expect("first");
        corpus
            .store(&record(k.clone(), "2026-09-04T02-00Z"))
            .expect("second");

        let back = corpus.load(&k).expect("load").expect("present");
        assert_eq!(back.timestamp, "2026-09-04T02-00Z");
        let (rows, bad) = corpus.rows_for_host(&k.host_key).expect("lane");
        assert_eq!(rows.len(), 1, "a replace must not leave two files");
        assert!(bad.is_empty());
    }

    /// No half-written file survives a store: the temporary is renamed into
    /// place, so the lane only ever holds complete rows.
    #[test]
    fn a_store_leaves_no_temporary_behind() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let k = key(Some("04085ec"), "large");
        corpus
            .store(&record(k.clone(), "2026-09-04T04-39Z"))
            .expect("store");

        let lane = scratch.0.join("rows").join(&k.host_key);
        let names: Vec<String> = std::fs::read_dir(&lane)
            .expect("lane")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(names[0].ends_with(".json"), "{names:?}");
    }

    /// One unreadable file must not hide the rows that did read — the
    /// evolution table would otherwise go blank over a single bad write.
    #[test]
    fn a_bad_file_is_reported_beside_the_rows_that_read() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let good = key(Some("04085ec"), "large");
        corpus
            .store(&record(good.clone(), "2026-09-04T01-00Z"))
            .expect("a");
        corpus
            .store(&record(key(Some("e16f415"), "large"), "2026-09-04T02-00Z"))
            .expect("b");

        let lane = scratch.0.join("rows").join(&good.host_key);
        std::fs::write(lane.join("deadbeefdeadbeef.json"), "not json at all")
            .expect("corrupt");

        let (rows, bad) = corpus.rows_for_host(&good.host_key).expect("lane");
        assert_eq!(rows.len(), 2, "good rows were lost");
        assert_eq!(bad.len(), 1);
        assert!(bad[0].path.ends_with("deadbeefdeadbeef.json"));
    }

    /// Rows come back oldest-first regardless of directory order, so a
    /// rendered evolution table does not shuffle between runs.
    #[test]
    fn a_lane_reads_back_in_timestamp_order() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let host = key(None, "large").host_key.clone();
        for (sha, ts) in [
            ("cccccc1", "2026-09-04T03-00Z"),
            ("aaaaaa1", "2026-09-04T01-00Z"),
            ("bbbbbb1", "2026-09-04T02-00Z"),
        ] {
            corpus
                .store(&record(key(Some(sha), "large"), ts))
                .expect("store");
        }
        let (rows, _) = corpus.rows_for_host(&host).expect("lane");
        let stamps: Vec<&str> =
            rows.iter().map(|r| r.timestamp.as_str()).collect();
        assert_eq!(
            stamps,
            [
                "2026-09-04T01-00Z",
                "2026-09-04T02-00Z",
                "2026-09-04T03-00Z"
            ]
        );
    }

    /// A machine with no corpus yet is empty, not broken.
    #[test]
    fn an_absent_lane_is_an_empty_corpus() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let (rows, bad) = corpus.rows_for_host("never-seen").expect("lane");
        assert!(rows.is_empty() && bad.is_empty());
        assert!(corpus.hosts().expect("hosts").is_empty());
    }

    #[test]
    fn hosts_lists_the_lanes_that_exist() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        for host in ["zeta-8c", "alpha-4c"] {
            let mut k = key(Some("04085ec"), "large");
            k.host_key = host.into();
            corpus
                .store(&record(k, "2026-09-04T04-39Z"))
                .expect("store");
        }
        assert_eq!(corpus.hosts().expect("hosts"), ["alpha-4c", "zeta-8c"]);
    }
}
