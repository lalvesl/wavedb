//! The three-operation workload, as a [`Workload`].
//!
//! ## The RNG streams are named, not derived from a length
//!
//! RFC 0060 seeded the read phases with `cfg.seed ^ name.len()` — 8 for
//! `read_hot` and 9 for `read_cold`. It worked, and it meant **renaming a
//! phase silently changed the data it read**. The streams are constants here,
//! so a phase's identity and its name are independent, which is what a
//! reproducible dataset requires.

use crate::plan::op::MicroOp;
use crate::schema::{Rng, thing, thing_v2_at};

use super::Workload;

/// Phase names, in run order.
pub const PHASES: [&str; 4] = ["insert", "read_hot", "read_cold", "update"];

/// The phases a **seeded** row runs, and the two absences are findings
/// rather than shortcuts.
///
/// `insert` is gone because the insert benchmark *is* the fill: a row served
/// from a prefilled seed has nothing left to insert, and re-inserting over it
/// would measure a rewrite.
///
/// `read_hot` is gone because on WaveDB there is nothing hot to measure — the
/// per-type cache is a **write** cache that reads never populate, so on a
/// store nobody has just written to, hot and cold are the same number. It is
/// dropped for every system rather than only for WaveDB, because a phase that
/// means one thing on four rows and another on the fifth is worse than a
/// missing phase. RFC 0044 is that gap.
pub const SEEDED_PHASES: [&str; 2] = ["read_cold", "update"];

/// One salt per phase that draws keys. Arbitrary but **fixed**: two phases
/// sharing a salt would read the same key sequence, and `read_cold` would
/// then be `read_hot` against a cold cache rather than an independent draw.
const SALT_READ_HOT: u64 = 0x5EED_0000_0000_1001;
const SALT_READ_COLD: u64 = 0x5EED_0000_0000_1002;
const SALT_UPDATE: u64 = 0x0DDB_A11B_EEF0_0D15;

/// Sizes for one micro row.
pub struct MicroWorkload {
    pub rows: u64,
    pub reads: u64,
    pub updates: u64,
    pub seed: u64,
    /// The store arrived already filled, so the phase list is
    /// [`SEEDED_PHASES`].
    pub seeded: bool,
}

impl Workload for MicroWorkload {
    type Op = MicroOp;

    fn phases(&self) -> &'static [&'static str] {
        if self.seeded { &SEEDED_PHASES } else { &PHASES }
    }

    fn len(&self, phase: &str) -> u64 {
        match phase {
            "insert" => self.rows,
            "read_hot" | "read_cold" => self.reads,
            "update" => self.updates,
            _ => 0,
        }
    }

    fn generate<E>(&mut self, phase: &str, mut emit: E) -> Result<(), String>
    where
        E: FnMut(MicroOp) -> Result<(), String>,
    {
        match phase {
            // Sequential, so `n` is both the row and the order it arrives in.
            "insert" => {
                for n in 0..self.rows {
                    emit(MicroOp::Insert {
                        n,
                        row: thing(n, self.seed),
                    })?;
                }
            }
            "read_hot" | "read_cold" => {
                let salt = if phase == "read_hot" {
                    SALT_READ_HOT
                } else {
                    SALT_READ_COLD
                };
                let mut rng = Rng::new(self.seed ^ salt);
                for _ in 0..self.reads {
                    emit(MicroOp::Read {
                        n: rng.below(self.rows.max(1)),
                    })?;
                }
            }
            "update" => {
                let mut rng = Rng::new(self.seed ^ SALT_UPDATE);
                for i in 0..self.updates {
                    let n = rng.below(self.rows.max(1));
                    // Salted by the draw ordinal: two updates of one row must
                    // write different bytes, or MySQL turns the second into a
                    // no-op and its update column measures work it skipped.
                    emit(MicroOp::Update {
                        n,
                        row: thing_v2_at(n, self.seed, i),
                    })?;
                }
            }
            other => return Err(format!("unknown micro phase {other:?}")),
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{MicroWorkload, PHASES, SEEDED_PHASES};
    use crate::harness::Workload;
    use crate::plan::op::MicroOp;

    fn workload() -> MicroWorkload {
        MicroWorkload {
            rows: 100,
            reads: 50,
            updates: 25,
            seed: 42,
            seeded: false,
        }
    }

    fn collect(phase: &str) -> Vec<MicroOp> {
        let mut out = Vec::new();
        workload()
            .generate(phase, |op| {
                out.push(op);
                Ok(())
            })
            .expect("generate");
        out
    }

    #[test]
    fn each_phase_emits_the_count_it_declared() {
        let w = workload();
        for phase in PHASES {
            assert_eq!(collect(phase).len() as u64, w.len(phase), "{phase}");
        }
    }

    #[test]
    fn inserts_arrive_in_row_order() {
        let keys: Vec<u64> =
            collect("insert").iter().map(MicroOp::key).collect();
        assert_eq!(keys, (0..100).collect::<Vec<_>>());
    }

    /// The bug RFC 0060's `name.len()` seeding hid: two read phases must draw
    /// **different** key sequences, or `read_cold` is `read_hot` again.
    #[test]
    fn the_two_read_phases_draw_different_keys() {
        let hot: Vec<u64> =
            collect("read_hot").iter().map(MicroOp::key).collect();
        let cold: Vec<u64> =
            collect("read_cold").iter().map(MicroOp::key).collect();
        assert_ne!(hot, cold);
    }

    /// And the property that makes a seed a seed.
    #[test]
    fn a_phase_is_reproducible_from_its_seed() {
        for phase in PHASES {
            let a: Vec<u64> = collect(phase).iter().map(MicroOp::key).collect();
            let b: Vec<u64> = collect(phase).iter().map(MicroOp::key).collect();
            assert_eq!(a, b, "{phase}");
        }
    }

    #[test]
    fn every_drawn_key_is_inside_the_dataset() {
        for phase in ["read_hot", "read_cold", "update"] {
            for op in collect(phase) {
                assert!(op.key() < 100, "{phase} drew {}", op.key());
            }
        }
    }

    /// A seeded row runs neither `insert` nor `read_hot`, and the phase
    /// list is what says so — a workload that still declared them would ask
    /// a prefilled store to insert over itself.
    #[test]
    fn a_seeded_row_runs_neither_insert_nor_read_hot() {
        let w = MicroWorkload {
            seeded: true,
            ..workload()
        };
        assert_eq!(w.phases(), SEEDED_PHASES);
        assert!(!w.phases().contains(&"insert"));
        assert!(!w.phases().contains(&"read_hot"));
    }

    #[test]
    fn an_unknown_phase_is_refused() {
        let err = workload()
            .generate("compact", |_| Ok(()))
            .expect_err("must refuse");
        assert!(err.contains("compact"), "{err}");
    }
}
