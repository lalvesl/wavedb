//! What a stored row holds, and the identity it is filed under.

use crate::json::{Json, fnv1a};

/// The record format's name and version, written into every file.
///
/// `2` because the shape is not an extension of `wavedb-bench/1`: that schema
/// described a **run** holding many systems, this one describes a single row.
/// A reader can tell them apart without guessing, which matters while both
/// exist in `benches/results/`.
pub const SCHEMA: &str = "wavedb-bench/2";

/// Everything the digest is taken over — the inputs that can change the
/// number.
///
/// The last field is the whole reuse mechanism ([RFC 0065] §1). For a peer it
/// is `None`, so the digest is stable across WaveDB commits and the stored row
/// is served instead of remeasured; for WaveDB it is `Some`, so every commit
/// is a new row and nothing stale is ever served. There is no
/// cache-invalidation policy to get wrong: the identity *is* the policy.
///
/// [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowKey {
    pub system: String,
    /// The measured system's own version string, from the server or driver
    /// rather than from `flake.lock`, so the record names what actually ran.
    pub system_version: String,
    /// `single` / `multi` for WaveDB, `-` for everyone else.
    pub variant: String,
    pub durability: String,
    pub workload: String,
    pub tier: String,
    /// Bumped by hand to force a dataset rebuild ([RFC 0065] §5).
    pub dataset_revision: u64,
    pub generator_seed: u64,
    /// How many consumer threads fed the driver. In the identity because a
    /// throughput without its consumer count is not a result.
    pub consumers: u32,
    pub host_key: String,
    /// The cgroup/affinity/namespace recipe's revision. A row measured under a
    /// different cage is a row from another machine.
    pub cage_revision: u32,
    /// Present **only** for WaveDB rows.
    pub wavedb_git_sha: Option<String>,
}

impl RowKey {
    /// The row's identity, as the 16-hex-digit name its file is stored under.
    ///
    /// FNV-1a, the same function the host fingerprint uses
    /// ([`crate::json::fnv1a`]): this is a lane identity, not a security hash,
    /// and a second hash in the crate would only be a second thing to keep
    /// consistent.
    ///
    /// Fields are folded with a separator that cannot occur inside one, so
    /// `("ab", "c")` and `("a", "bc")` cannot collide by concatenation — a
    /// digest whose parts run together is not an identity.
    #[must_use]
    pub fn digest(&self) -> String {
        let mut text = String::with_capacity(256);
        for part in [
            self.system.as_str(),
            self.system_version.as_str(),
            self.variant.as_str(),
            self.durability.as_str(),
            self.workload.as_str(),
            self.tier.as_str(),
            &self.dataset_revision.to_string(),
            &self.generator_seed.to_string(),
            &self.consumers.to_string(),
            self.host_key.as_str(),
            &self.cage_revision.to_string(),
            // `None` folds as an empty field rather than being skipped: a peer
            // row and a WaveDB row built at the empty SHA must still differ.
            self.wavedb_git_sha.as_deref().unwrap_or(""),
        ] {
            text.push_str(part);
            text.push('\u{1f}');
        }
        format!("{:016x}", fnv1a(text.as_bytes()))
    }

    /// Whether this row is remeasured on every commit.
    #[must_use]
    pub const fn is_wavedb(&self) -> bool {
        self.wavedb_git_sha.is_some()
    }
}

/// One measured phase.
///
/// `wall_ns` and `total_ns` are both here, and keeping both is the point:
/// `total_ns` sums the per-operation windows and `wall_ns` measures the phase,
/// so under one consumer they agree to within the untimed gaps and under
/// three they differ by roughly three. Throughput divides by `wall_ns` —
/// the only form that survives concurrency ([RFC 0065] §4).
///
/// [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhaseRecord {
    pub name: String,
    pub count: u64,
    pub wall_ns: u64,
    pub total_ns: u64,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
    pub max_ns: u64,
    pub bytes_written: u64,
    /// Bytes that reached the block layer, and bytes the process asked for.
    /// The **ratio** is the finding: equal means genuine thrashing, and
    /// `read_bytes` far below `rchar` means a working cache costing no IOps.
    pub read_bytes: u64,
    pub rchar: u64,
}

impl PhaseRecord {
    /// Operations per second, by wall clock.
    #[must_use]
    pub fn throughput(&self) -> f64 {
        if self.wall_ns == 0 {
            return 0.0;
        }
        self.count as f64 * 1e9 / self.wall_ns as f64
    }
}

/// A footprint measurement at one point in the run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FootprintRecord {
    pub apparent_bytes: u64,
    pub allocated_bytes: u64,
    pub log_bytes: u64,
    pub files: u64,
}

/// One row: its identity, how it was taken, and what it measured.
#[derive(Debug, Clone, PartialEq)]
pub struct RowRecord {
    pub key: RowKey,
    pub timestamp: String,
    /// The tree was dirty when this ran — the row is not reproducible from a
    /// commit alone.
    pub dirty: bool,
    pub caged: bool,
    /// Recorded outside the cage by explicit override. A `forced` row is
    /// comparable with nothing, and says so in its own file.
    pub forced: bool,
    pub bracket: String,
    pub compression: String,
    pub retains_history: bool,
    pub settings: Vec<(String, String)>,
    pub phases: Vec<PhaseRecord>,
    pub footprints: Vec<(String, FootprintRecord)>,
    pub live_records: u64,
    pub logical_bytes: u64,
    pub notes: Vec<String>,
    pub seed_path: Option<String>,
    pub materialise_ms: u64,
}

impl RowRecord {
    /// The file name this row is stored under, digest and all.
    #[must_use]
    pub fn file_name(&self) -> String {
        format!("{}.json", self.key.digest())
    }

    /// The phase named `name`, if this row has one.
    #[must_use]
    pub fn phase(&self, name: &str) -> Option<&PhaseRecord> {
        self.phases.iter().find(|p| p.name == name)
    }

    /// Encode to the stored form.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut j = Json::new();
        j.obj(None, |j| {
            j.str("schema", SCHEMA);
            j.str("digest", &self.key.digest());
            j.str("timestamp", &self.timestamp);
            self.write_key(j);
            j.obj(Some("provenance"), |j| {
                j.boolean("dirty", self.dirty);
                j.boolean("caged", self.caged);
                j.boolean("forced", self.forced);
            });
            j.str("bracket", &self.bracket);
            j.str("compression", &self.compression);
            j.boolean("retains_history", self.retains_history);
            j.num("live_records", self.live_records);
            j.num("logical_bytes", self.logical_bytes);
            j.num("materialise_ms", self.materialise_ms);
            match &self.seed_path {
                Some(p) => j.str("seed_path", p),
                None => j.str("seed_path", ""),
            }
            j.obj(Some("settings"), |j| {
                for (k, v) in &self.settings {
                    j.str(k, v);
                }
            });
            j.arr(Some("phases"), |j| {
                for p in &self.phases {
                    write_phase(j, p);
                }
            });
            j.arr(Some("footprints"), |j| {
                for (point, f) in &self.footprints {
                    j.obj(None, |j| {
                        j.str("point", point);
                        j.num("apparent_bytes", f.apparent_bytes);
                        j.num("allocated_bytes", f.allocated_bytes);
                        j.num("log_bytes", f.log_bytes);
                        j.num("files", f.files);
                    });
                }
            });
            j.arr(Some("notes"), |j| {
                for n in &self.notes {
                    j.elem(n);
                }
            });
        });
        j.finish()
    }

    fn write_key(&self, j: &mut Json) {
        j.obj(Some("key"), |j| {
            j.str("system", &self.key.system);
            j.str("system_version", &self.key.system_version);
            j.str("variant", &self.key.variant);
            j.str("durability", &self.key.durability);
            j.str("workload", &self.key.workload);
            j.str("tier", &self.key.tier);
            j.num("dataset_revision", self.key.dataset_revision);
            j.num("generator_seed", self.key.generator_seed);
            j.num("consumers", u64::from(self.key.consumers));
            j.str("host_key", &self.key.host_key);
            j.num("cage_revision", u64::from(self.key.cage_revision));
            j.str(
                "wavedb_git_sha",
                self.key.wavedb_git_sha.as_deref().unwrap_or(""),
            );
        });
    }
}

fn write_phase(j: &mut Json, p: &PhaseRecord) {
    j.obj(None, |j| {
        j.str("name", &p.name);
        j.num("count", p.count);
        j.num("wall_ns", p.wall_ns);
        j.num("total_ns", p.total_ns);
        j.num("p50_ns", p.p50_ns);
        j.num("p95_ns", p.p95_ns);
        j.num("p99_ns", p.p99_ns);
        j.num("max_ns", p.max_ns);
        j.num("bytes_written", p.bytes_written);
        j.num("read_bytes", p.read_bytes);
        j.num("rchar", p.rchar);
        j.ratio("ops_per_sec", p.throughput());
    });
}
