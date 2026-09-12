//! The two refusals: a machine too busy to be believed, and a filesystem with
//! no room to write into.
//!
//! Both were RFC 0060's and both survive [RFC 0065] unchanged in substance —
//! only in *where* they run. The old runner checked load after the work and
//! before recording; with the row as the unit a row records the moment it
//! finishes, so there is no "before recording" left to check at. The
//! supervisor therefore checks once, before it starts spawning rows, which is
//! also where a refusal costs nothing instead of costing the pass.
//!
//! [RFC 0065]: ../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

use crate::host::{BtrfsSpace, Host};

/// Load allowed per CPU of budget.
///
/// Half the run's own budget puts the cage at 2.00 and an uncaged 8-core run
/// at 4.00.
///
/// It stays a heuristic either way: load average counts every runnable task on
/// the machine, while `taskset` confines the benchmark to its mask, so work
/// pinned to the other cores inflates this without contending for much beyond
/// memory bandwidth and the disk.
pub const NOISE_PER_CPU: f64 = 0.5;

/// Never stricter than this, so a one- or two-CPU cage does not become
/// unrunnable on a machine that is merely awake.
pub const NOISE_FLOOR: f64 = 2.0;

/// Below this share of **unallocated** device space, btrfs can no longer carve
/// a fresh chunk when existing block groups are awkward, and a write-heavy row
/// starts measuring the allocator rather than the database.
///
/// The same reasoning as [`NOISE_PER_CPU`], applied to the other shared
/// resource. The state that correlated with a 22× spread on one machine — the
/// same 8 000-user fill at **59 s and 1 297 s** — was 8.5% unallocated *and*
/// 96% data fill. Data fill alone is the wrong guard: a `btrfs balance` packs
/// the groups it keeps, so it *raises* fill while restoring the headroom that
/// actually matters. Recorded, not guarded.
pub const UNALLOCATED_LIMIT: f64 = 0.10;

#[must_use]
pub fn noise_limit(cpu_budget: u64) -> f64 {
    (cpu_budget as f64 * NOISE_PER_CPU).max(NOISE_FLOOR)
}

/// Refuse a pass on a machine too busy for its numbers to mean anything.
///
/// # Errors
/// The one-minute load average exceeds the budget's limit.
pub fn check_load(load: f64, cpu_budget: u64) -> Result<(), String> {
    let noise = noise_limit(cpu_budget);
    if load <= noise {
        return Ok(());
    }
    Err(format!(
        "load average {load:.2} exceeds {noise:.2} ({cpu_budget} cpus × \
         {NOISE_PER_CPU}, floor {NOISE_FLOOR:.2}) — refusing to record \
         numbers this machine cannot stand behind (--force to override)"
    ))
}

/// Refuse a pass on a filesystem with no room to allocate into.
///
/// Checked **before** the work, unlike the load guard was: a benchmark only
/// ever writes, so this number cannot improve while one is running. Waiting
/// would learn nothing and burn the whole pass first — at the default sizes,
/// on a filesystem in this state, that is days.
///
/// # Errors
/// Unallocated device space is below [`UNALLOCATED_LIMIT`].
pub fn check_space(host: &Host) -> Result<(), String> {
    let Some(space) = host.btrfs else {
        return Ok(()); // not btrfs: nothing here to measure
    };
    if space.unallocated >= UNALLOCATED_LIMIT {
        return Ok(());
    }
    Err(space_refusal(space))
}

fn space_refusal(space: BtrfsSpace) -> String {
    format!(
        "only {:.1}% of the btrfs device is unallocated (limit {:.0}%), with \
         its data block groups {:.1}% full — refusing to run: with no room to \
         carve a fresh chunk, the allocator rather than the database sets the \
         write times. Free space, run `btrfs balance` to return packed groups \
         to the unallocated pool, or point the run at another filesystem \
         (--force to override, --dry-run to see the plan anyway)",
        space.unallocated * 100.0,
        UNALLOCATED_LIMIT * 100.0,
        space.data_fill * 100.0
    )
}

#[cfg(test)]
mod tests {
    use super::{NOISE_FLOOR, check_load, noise_limit};

    /// The floor exists so a small cage does not become unrunnable on a
    /// machine that is merely awake.
    #[test]
    fn the_floor_holds_for_small_budgets() {
        assert!((noise_limit(1) - NOISE_FLOOR).abs() < f64::EPSILON);
        assert!((noise_limit(2) - NOISE_FLOOR).abs() < f64::EPSILON);
        // Four cpus is exactly the floor; eight is where it starts to scale.
        assert!((noise_limit(4) - 2.0).abs() < f64::EPSILON);
        assert!((noise_limit(8) - 4.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_quiet_machine_passes_and_a_busy_one_does_not() {
        assert!(check_load(1.9, 4).is_ok());
        assert!(check_load(2.0, 4).is_ok(), "the limit itself is allowed");
        let err = check_load(2.1, 4).expect_err("must refuse");
        assert!(err.contains("2.10"), "{err}");
        assert!(err.contains("--force"), "{err}");
    }

    /// The refusal has to name the number it wants, or it is unactionable.
    #[test]
    fn the_refusal_states_the_limit_and_the_way_out() {
        let err = check_load(9.0, 8).expect_err("must refuse");
        assert!(err.contains("4.00"), "{err}");
        assert!(err.contains("8 cpus"), "{err}");
    }
}
