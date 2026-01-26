//! Which queue the page actor serves next ([RFC 0064]).
//!
//! Two classes reach the actor from outside. **Reads are high priority** —
//! a shard missed its cache and something is waiting on the far end.
//! **Maintenance is low** — settling; only a durable write may wait on
//! it. (Work the actor starts on its own behalf, such as reading a page in
//! order to merge into it, never re-enters a queue at all: that would be a
//! request waiting on a request in the same bounded queue, which is a
//! self-deadlock rather than an inefficiency.)
//!
//! ## Why strict priority is wrong
//!
//! Draining reads first, always, starves maintenance — and maintenance is not
//! optional background work. Starve it and nothing is ever settled,
//! so it grows without bound, and recovery time grows with it. Starvation here
//! is an out-of-memory, not an unfairness.
//!
//! ## The valve is driven by bytes, not by counts
//!
//! What hurts is not "maintenance is late", it is "unsettled state is
//! growing" — so the thing that opens the valve is the **volume** behind it,
//! not a message count and not a clock.
//!
//! That distinction has already cost this project once: the benchmark
//! adapter's `MAINTAIN_EVERY = 5000` operations never fired while 649 MB of
//! journal accumulated, because a count of operations says nothing about how
//! much log they produced. It became a byte threshold. The same mistake is
//! available here.
//!
//! ## It stabilises itself
//!
//! Below the low mark maintenance may be starved indefinitely, and that is
//! safe **because starving it is what ends it**: unserved maintenance is
//! exactly what makes the volume grow, so the pressure rises into the ratio
//! band and then, if reads still dominate, past the high mark where
//! maintenance becomes mandatory. No timer is needed to escape the corner.
//!
//! [RFC 0064]: ../../../../rfcs/0064-pivot-owned-concurrency-PLANNED.md

/// Which class to serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// A shard's read — someone is waiting.
    Read,
    /// A settle step — nobody is waiting, but a durable write.
    Maintenance,
}

/// What is queued right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Available {
    pub reads: bool,
    pub maintenance: bool,
}

/// Below this many unsettled bytes, reads always win.
pub const LOW_WATER_BYTES: u64 = 8 << 20;

/// At or above this many, maintenance runs even with reads waiting.
///
/// Past here what is not settled is the thing at risk: everything a crash
/// would lose, and RAM the record cache cannot give back.
pub const HIGH_WATER_BYTES: u64 = 64 << 20;

/// Reads served per maintenance step inside the middle band.
pub const READS_PER_MAINTENANCE: u32 = 8;

/// The valve.
///
/// **Unmeasured.** The three constants above are argued for in *shape* — a
/// floor below which interleaving is pointless, a ceiling above which the log
/// is what matters, a ratio between — and their magnitudes are starting
/// points, not findings. There is no benchmark row to tune them against.
#[derive(Debug, Clone, Copy)]
pub struct Priority {
    low_water: u64,
    high_water: u64,
    reads_per_maintenance: u32,
    reads_since: u32,
}

impl Default for Priority {
    fn default() -> Self {
        Self::new(LOW_WATER_BYTES, HIGH_WATER_BYTES, READS_PER_MAINTENANCE)
    }
}

impl Priority {
    /// A valve with explicit marks. `high` is clamped to at least `low`, so a
    /// misconfigured pair degrades to a single threshold rather than to a band
    /// that can never be entered.
    #[must_use]
    pub const fn new(low: u64, high: u64, reads_per_maintenance: u32) -> Self {
        Self {
            low_water: low,
            high_water: if high < low { low } else { high },
            reads_per_maintenance,
            reads_since: 0,
        }
    }

    /// The class to serve next, or `None` when nothing is queued.
    ///
    /// `unsettled` is the volume behind the maintenance queue — the bytes
    /// committed and not settled yet, which is the quantity that actually
    /// grows without bound if maintenance never runs.
    pub const fn next(
        &mut self,
        avail: Available,
        unsettled: u64,
    ) -> Option<Class> {
        match (avail.reads, avail.maintenance) {
            (false, false) => None,
            // Only one class is queued: priority has nothing to arbitrate,
            // and skipping the available one to honour a policy would leave
            // the actor idle with work in hand.
            (true, false) => Some(self.serve_read()),
            (false, true) => Some(self.serve_maintenance()),
            (true, true) => {
                if unsettled >= self.high_water {
                    Some(self.serve_maintenance())
                } else if unsettled < self.low_water
                    || self.reads_since < self.reads_per_maintenance
                {
                    Some(self.serve_read())
                } else {
                    Some(self.serve_maintenance())
                }
            }
        }
    }

    const fn serve_read(&mut self) -> Class {
        self.reads_since = self.reads_since.saturating_add(1);
        Class::Read
    }

    /// Serving maintenance resets the ratio, so the band gives reads a fresh
    /// run rather than alternating once the counter has been passed.
    const fn serve_maintenance(&mut self) -> Class {
        self.reads_since = 0;
        Class::Maintenance
    }
}

#[cfg(test)]
mod tests {
    use super::{Available, Class, Priority};

    const BOTH: Available = Available {
        reads: true,
        maintenance: true,
    };
    const NEITHER: Available = Available {
        reads: false,
        maintenance: false,
    };
    const ONLY_MAINTENANCE: Available = Available {
        reads: false,
        maintenance: true,
    };

    fn valve() -> Priority {
        Priority::new(1000, 10_000, 4)
    }

    #[test]
    fn an_empty_actor_serves_nothing() {
        assert_eq!(valve().next(NEITHER, 0), None);
        assert_eq!(valve().next(NEITHER, u64::MAX), None);
    }

    /// With only one class queued there is nothing to arbitrate — refusing to
    /// serve it would idle the actor with work in hand.
    #[test]
    fn the_only_queued_class_is_served_whatever_the_pressure() {
        let only_reads = Available {
            reads: true,
            maintenance: false,
        };
        assert_eq!(valve().next(only_reads, u64::MAX), Some(Class::Read));
        assert_eq!(valve().next(ONLY_MAINTENANCE, 0), Some(Class::Maintenance));
    }

    #[test]
    fn below_the_low_mark_reads_always_win() {
        let mut p = valve();
        for _ in 0..1000 {
            assert_eq!(p.next(BOTH, 999), Some(Class::Read));
        }
    }

    #[test]
    fn above_the_high_mark_maintenance_runs_despite_reads() {
        let mut p = valve();
        for _ in 0..1000 {
            assert_eq!(p.next(BOTH, 10_000), Some(Class::Maintenance));
        }
    }

    /// In the band, reads lead but maintenance gets its turn — the ratio is
    /// what bounds starvation.
    #[test]
    fn the_middle_band_interleaves_by_the_ratio() {
        let mut p = valve();
        let served: Vec<Class> =
            (0..10).map(|_| p.next(BOTH, 5000).unwrap()).collect();
        assert_eq!(
            served,
            vec![
                Class::Read,
                Class::Read,
                Class::Read,
                Class::Read,
                Class::Maintenance,
                Class::Read,
                Class::Read,
                Class::Read,
                Class::Read,
                Class::Maintenance,
            ]
        );
    }

    /// The property the whole valve exists for: under unrelenting read
    /// pressure, maintenance still runs. A strict-priority actor would score
    /// zero here and grow its record cache until the process died.
    #[test]
    fn maintenance_is_never_starved_under_constant_read_pressure() {
        let mut p = Priority::default();
        let unsettled = super::LOW_WATER_BYTES; // just into the band
        let served = (0..10_000)
            .filter(|_| p.next(BOTH, unsettled) == Some(Class::Maintenance))
            .count();
        assert!(served > 0, "maintenance starved");
        // Roughly one in nine (eight reads, then one maintenance).
        assert!(
            (900..1200).contains(&served),
            "{served} maintenance steps in 10 000"
        );
    }

    /// Reads keep the majority share in the band — the priority is real, not
    /// just alternation with extra steps.
    #[test]
    fn reads_keep_the_larger_share_in_the_band() {
        let mut p = Priority::default();
        let reads = (0..10_000)
            .filter(|_| {
                p.next(BOTH, super::LOW_WATER_BYTES) == Some(Class::Read)
            })
            .count();
        assert!(reads > 8000, "reads got only {reads}/10000");
    }

    /// A low mark above the high one must degrade to one threshold rather
    /// than to a band nothing can be inside.
    #[test]
    fn inverted_marks_degrade_to_a_single_threshold() {
        let mut p = Priority::new(10_000, 500, 4);
        assert_eq!(p.next(BOTH, 9_999), Some(Class::Read), "below the mark");
        assert_eq!(
            p.next(BOTH, 10_000),
            Some(Class::Maintenance),
            "at the mark"
        );
    }

    /// Serving maintenance resets the ratio, so reads get a full run again
    /// instead of the two classes alternating once the counter is passed.
    #[test]
    fn a_maintenance_step_gives_reads_a_fresh_run() {
        let mut p = valve();
        for _ in 0..4 {
            assert_eq!(p.next(BOTH, 5000), Some(Class::Read));
        }
        assert_eq!(p.next(BOTH, 5000), Some(Class::Maintenance));
        assert_eq!(p.next(BOTH, 5000), Some(Class::Read), "run restarts");
    }
}
