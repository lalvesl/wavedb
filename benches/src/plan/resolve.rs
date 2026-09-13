//! Which rows a run must measure, and which it already has
//! ([RFC 0065] §1).
//!
//! ## Why this is pure
//!
//! Resolving takes no filesystem decision of its own beyond reading the
//! corpus, and in particular it does **not** probe versions. A row's identity
//! includes `system_version`, and a server's version is knowable only by
//! asking the binary — which is a plan-time `--version` call, milliseconds and
//! no server start, but still an effect. It belongs to the supervisor, which
//! fills [`Request::versions`] before calling here. Keeping the split means
//! the interesting logic — what gets reused — is testable without a database
//! on the machine.
//!
//! Asking the *binary* rather than the running server is also what keeps the
//! reuse correct: `flake.lock` pins the package, so a nixpkgs bump changes the
//! version string, which changes the digest, which retires every peer row that
//! was measured against the old one. That invalidation is the property a
//! `~/.cache` directory could never have.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use std::collections::BTreeMap;

use crate::corpus::{Corpus, RowKey, RowRecord, Unreadable};

/// The systems in report order. `wavedb` twice, because its two engine seams
/// are two rows rather than one row with a footnote.
pub const SYSTEMS: [(&str, &str); 6] = [
    ("wavedb", "single"),
    ("wavedb", "multi"),
    ("sqlite", "-"),
    ("postgres", "-"),
    ("mysql", "-"),
    ("mongodb", "-"),
];

pub const DURABILITIES: [&str; 2] = ["durable", "relaxed"];
pub const WORKLOADS: [&str; 2] = ["micro", "shop"];

/// What a run was asked to produce.
pub struct Request {
    /// Empty means every system; otherwise only these.
    pub systems: Vec<String>,
    /// Empty means both workloads.
    pub workloads: Vec<String>,
    pub tier: String,
    /// Consumers for the one configuration that can use more than one.
    pub consumers: u32,
    pub refresh: Refresh,
    pub host_key: String,
    pub cage_revision: u32,
    pub dataset_revision: u64,
    pub generator_seed: u64,
    /// The commit being measured. Folded into WaveDB rows only.
    pub wavedb_git_sha: String,
    /// `system → version`, probed by the supervisor before planning.
    pub versions: BTreeMap<String, String>,
}

/// How much of the corpus a run is willing to ignore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refresh {
    /// Reuse everything that is stored. The default, and the reason a pass
    /// costs minutes rather than an hour.
    None,
    /// Remeasure everything, stored or not.
    All,
    /// Remeasure only these systems.
    Systems(Vec<String>),
}

impl Refresh {
    fn covers(&self, system: &str) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Systems(names) => names.iter().any(|n| n == system),
        }
    }
}

/// The answer: what to run, what to read, and what is wrong.
pub struct Plan {
    /// Rows to measure, in the order they should run.
    pub measure: Vec<RowKey>,
    /// Rows served from the corpus. Never remeasured.
    pub reuse: Vec<RowRecord>,
    /// Stored rows that could not be read. **Reported, and also scheduled for
    /// measurement** — a corrupt row must not block a run, and must not be
    /// silently replaced either.
    pub broken: Vec<Unreadable>,
}

impl Plan {
    /// Minutes saved, roughly: how much of the matrix came from the corpus.
    #[must_use]
    pub fn reused(&self) -> usize {
        self.reuse.len()
    }
}

/// How many consumers a given configuration runs with.
///
/// One, except for sharded WaveDB on the `shop` workload — and that exception
/// is the whole shape of [RFC 0065] §3. The `micro` workload lives in a single
/// collection, and a collection is indivisible (its B+tree nodes and chain
/// segments belong to the Pivot's owner), so asking for three consumers there
/// would not be a slower measurement but a wrong one. The clamp is here, at
/// the point the matrix is built, so no row can be *created* with a count it
/// may not have.
///
/// [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md
#[must_use]
pub const fn consumers_for(
    system: &str,
    variant: &str,
    workload: &str,
    asked: u32,
) -> u32 {
    // `const fn` has no string equality, so compare bytes.
    let sharded = matches!(system.as_bytes(), b"wavedb")
        && matches!(variant.as_bytes(), b"multi");
    let shop = matches!(workload.as_bytes(), b"shop");
    if sharded && shop { asked } else { 1 }
}

/// Whether this configuration is a row at all.
///
/// `wavedb/multi` exists only on `shop`: with one consumer it would be an
/// expensive re-run of `wavedb/single` plus a disk actor, which is a row the
/// corpus already has under a name that says so.
#[must_use]
fn is_a_row(system: &str, variant: &str, workload: &str) -> bool {
    !(system == "wavedb" && variant == "multi" && workload != "shop")
}

/// Build the requested matrix and split it against the corpus.
///
/// # Errors
/// The corpus root exists but cannot be listed or read.
pub fn resolve(req: &Request, corpus: &Corpus) -> Result<Plan, String> {
    let mut plan = Plan {
        measure: Vec::new(),
        reuse: Vec::new(),
        broken: Vec::new(),
    };

    for (system, variant) in SYSTEMS {
        if !req.systems.is_empty() && !req.systems.iter().any(|s| s == system) {
            continue;
        }
        for workload in WORKLOADS {
            if !req.workloads.is_empty()
                && !req.workloads.iter().any(|w| w == workload)
            {
                continue;
            }
            if !is_a_row(system, variant, workload) {
                continue;
            }
            for durability in DURABILITIES {
                let key = req.key(system, variant, workload, durability);
                classify(req, corpus, key, system, &mut plan)?;
            }
        }
    }
    Ok(plan)
}

fn classify(
    req: &Request,
    corpus: &Corpus,
    key: RowKey,
    system: &str,
    plan: &mut Plan,
) -> Result<(), String> {
    if req.refresh.covers(system) {
        plan.measure.push(key);
        return Ok(());
    }
    match corpus.load(&key) {
        Ok(Some(row)) => plan.reuse.push(row),
        Ok(None) => plan.measure.push(key),
        // Present and unusable: say so, then measure it anyway. Refusing the
        // whole run over one bad file would make a corrupt row a blocker; not
        // reporting it would make the replacement look routine.
        Err(why) => {
            plan.broken.push(Unreadable {
                path: corpus.path_of(&key),
                why,
            });
            plan.measure.push(key);
        }
    }
    Ok(())
}

impl Request {
    /// The identity of one row of this request.
    #[must_use]
    pub fn key(
        &self,
        system: &str,
        variant: &str,
        workload: &str,
        durability: &str,
    ) -> RowKey {
        RowKey {
            system: system.to_string(),
            system_version: self
                .versions
                .get(system)
                .cloned()
                .unwrap_or_default(),
            variant: variant.to_string(),
            durability: durability.to_string(),
            workload: workload.to_string(),
            tier: self.tier.clone(),
            dataset_revision: self.dataset_revision,
            generator_seed: self.generator_seed,
            consumers: consumers_for(system, variant, workload, self.consumers),
            host_key: self.host_key.clone(),
            cage_revision: self.cage_revision,
            // The one field that decides reuse: present for WaveDB, absent for
            // everyone else.
            wavedb_git_sha: (system == "wavedb")
                .then(|| self.wavedb_git_sha.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{Plan, Refresh, Request, consumers_for, resolve};
    use crate::corpus::{Corpus, RowKey, RowRecord};

    struct Scratch(PathBuf);

    impl Scratch {
        fn new() -> Self {
            static N: AtomicU32 = AtomicU32::new(0);
            let dir = std::env::temp_dir().join(format!(
                "wavedb-bench-plan-{}-{}",
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

    fn request() -> Request {
        let mut versions = BTreeMap::new();
        for (s, v) in [
            ("wavedb", "0.1.0"),
            ("sqlite", "3.50.0"),
            ("postgres", "18.1"),
            ("mysql", "8.4.0"),
            ("mongodb", "8.0.4"),
        ] {
            versions.insert(s.to_string(), v.to_string());
        }
        Request {
            systems: Vec::new(),
            workloads: Vec::new(),
            tier: "large".into(),
            consumers: 3,
            refresh: Refresh::None,
            host_key: "i5-8300h-4c-500m-btrfs-e636".into(),
            cage_revision: 1,
            dataset_revision: 1,
            generator_seed: 42,
            wavedb_git_sha: "04085ec".into(),
            versions,
        }
    }

    fn record(key: RowKey) -> RowRecord {
        RowRecord {
            key,
            timestamp: "2026-09-04T05-22Z".into(),
            dirty: false,
            caged: true,
            forced: false,
            bracket: "server".into(),
            compression: "none".into(),
            retains_history: false,
            settings: Vec::new(),
            phases: Vec::new(),
            footprints: Vec::new(),
            live_records: 0,
            logical_bytes: 0,
            notes: Vec::new(),
            seed_path: None,
            materialise_ms: 0,
        }
    }

    fn plan_of(req: &Request, scratch: &Scratch) -> Plan {
        resolve(req, &Corpus::at(&scratch.0)).expect("resolve")
    }

    /// 10 micro rows + 12 shop rows. The asymmetry is `wavedb/multi`, which
    /// exists on `shop` only.
    #[test]
    fn the_full_matrix_is_twenty_two_rows() {
        let scratch = Scratch::new();
        let plan = plan_of(&request(), &scratch);
        assert_eq!(plan.measure.len(), 22);
        assert!(plan.reuse.is_empty());

        let micro = plan.measure.iter().filter(|k| k.workload == "micro");
        assert_eq!(micro.count(), 10);
    }

    /// The constraint from `MicroOp::partition_key`, enforced where rows are
    /// created rather than trusted downstream.
    #[test]
    fn no_micro_row_ever_asks_for_more_than_one_consumer() {
        let scratch = Scratch::new();
        for key in plan_of(&request(), &scratch).measure {
            if key.workload == "micro" {
                assert_eq!(key.consumers, 1, "{}/{}", key.system, key.variant);
            }
        }
    }

    #[test]
    fn only_sharded_wavedb_on_shop_gets_three_consumers() {
        let scratch = Scratch::new();
        let three: Vec<_> = plan_of(&request(), &scratch)
            .measure
            .into_iter()
            .filter(|k| k.consumers == 3)
            .collect();
        assert_eq!(three.len(), 2, "durable and relaxed, nothing else");
        for key in three {
            assert_eq!(
                (
                    key.system.as_str(),
                    key.variant.as_str(),
                    key.workload.as_str()
                ),
                ("wavedb", "multi", "shop")
            );
        }
    }

    #[test]
    fn sharded_wavedb_is_not_a_micro_row() {
        let scratch = Scratch::new();
        assert!(
            !plan_of(&request(), &scratch)
                .measure
                .iter()
                .any(|k| { k.variant == "multi" && k.workload == "micro" })
        );
    }

    /// The point of the whole restructure: a stored peer row is served, not
    /// remeasured, **even though the WaveDB commit moved**.
    #[test]
    fn a_stored_peer_row_survives_a_new_wavedb_commit() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let req = request();
        for workload in ["micro", "shop"] {
            for durability in ["durable", "relaxed"] {
                let key = req.key("postgres", "-", workload, durability);
                corpus.store(&record(key)).expect("store");
            }
        }

        let mut later = request();
        later.wavedb_git_sha = "deadbee".into();
        let plan = plan_of(&later, &scratch);

        assert_eq!(plan.reused(), 4, "postgres should have been reused");
        assert!(!plan.measure.iter().any(|k| k.system == "postgres"));
        assert_eq!(plan.measure.len(), 18);
    }

    /// And the other half: a WaveDB row is *not* reusable across commits.
    #[test]
    fn a_stored_wavedb_row_is_remeasured_on_a_new_commit() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let req = request();
        corpus
            .store(&record(req.key("wavedb", "single", "micro", "durable")))
            .expect("store");

        // Same commit: served.
        assert_eq!(plan_of(&req, &scratch).reused(), 1);

        // Moved commit: measured again.
        let mut later = request();
        later.wavedb_git_sha = "deadbee".into();
        assert_eq!(plan_of(&later, &scratch).reused(), 0);
    }

    /// A nixpkgs bump must retire the peer rows measured against the old
    /// server — the invalidation an ad-hoc cache could not have.
    #[test]
    fn a_version_bump_retires_the_rows_it_invalidates() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let req = request();
        corpus
            .store(&record(req.key("postgres", "-", "micro", "durable")))
            .expect("store");
        assert_eq!(plan_of(&req, &scratch).reused(), 1);

        let mut bumped = request();
        bumped.versions.insert("postgres".into(), "19.0".into());
        assert_eq!(plan_of(&bumped, &scratch).reused(), 0);
    }

    #[test]
    fn refresh_all_ignores_a_full_corpus() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let req = request();
        for key in plan_of(&req, &scratch).measure {
            corpus.store(&record(key)).expect("store");
        }
        assert_eq!(plan_of(&req, &scratch).measure.len(), 0);

        let mut forced = request();
        forced.refresh = Refresh::All;
        let plan = plan_of(&forced, &scratch);
        assert_eq!(plan.measure.len(), 22);
        assert_eq!(plan.reused(), 0);
    }

    #[test]
    fn refresh_by_system_leaves_the_others_reused() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let req = request();
        for key in plan_of(&req, &scratch).measure {
            corpus.store(&record(key)).expect("store");
        }
        let mut some = request();
        some.refresh = Refresh::Systems(vec!["mysql".into()]);
        let plan = plan_of(&some, &scratch);

        assert_eq!(plan.measure.len(), 4, "mysql: 2 workloads × 2 rows");
        assert!(plan.measure.iter().all(|k| k.system == "mysql"));
        assert_eq!(plan.reused(), 18);
    }

    #[test]
    fn a_system_filter_narrows_the_matrix() {
        let scratch = Scratch::new();
        let mut req = request();
        req.systems = vec!["sqlite".into()];
        req.workloads = vec!["micro".into()];
        let plan = plan_of(&req, &scratch);
        assert_eq!(plan.measure.len(), 2);
        assert!(plan.measure.iter().all(|k| k.system == "sqlite"));
    }

    /// A corrupt row is reported **and** scheduled: it must not block the run,
    /// and it must not be replaced quietly either.
    #[test]
    fn a_broken_row_is_reported_and_also_measured() {
        let scratch = Scratch::new();
        let corpus = Corpus::at(&scratch.0);
        let req = request();
        let key = req.key("mongodb", "-", "micro", "durable");
        let path = corpus.store(&record(key.clone())).expect("store");
        std::fs::write(&path, "{ truncated").expect("corrupt");

        let plan = plan_of(&req, &scratch);
        assert_eq!(plan.broken.len(), 1);
        assert_eq!(plan.broken[0].path, path);
        assert!(plan.measure.contains(&key), "must still be measured");
    }

    /// The clamp itself, at its edges.
    #[test]
    fn the_consumer_clamp_only_opens_for_one_configuration() {
        assert_eq!(consumers_for("wavedb", "multi", "shop", 3), 3);
        assert_eq!(consumers_for("wavedb", "multi", "micro", 3), 1);
        assert_eq!(consumers_for("wavedb", "single", "shop", 3), 1);
        assert_eq!(consumers_for("postgres", "-", "shop", 3), 1);
    }
}
