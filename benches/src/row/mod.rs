//! Measuring **one** row ([RFC 0065] Â§2).
//!
//! This is what runs inside a cage. It knows one configuration, produces one
//! [`RowRecord`], and stores it â no matrix, no ordering, no other system.
//!
//! ## One path, and what it replaced
//!
//! [`micro`] and [`shop`] are both the [RFC 0065] path: the producer/consumer
//! harness, a real wall-clock denominator, pooled per-operation percentiles,
//! and footprints taken by the row rather than by whatever happened to hold
//! the store last.
//!
//! RFC 0060's `run` functions are gone. Each owned its own loop, its own
//! timing and its own phase boundaries, which is exactly how five adapters
//! meant to measure one thing drifted from each other. What survives of them
//! is the part that was never a measurement: the preloads, the DDL and the
//! open helpers, which the rows here call.
//!
//! The dispatch below is a `match` on the row's identity rather than a
//! capability check: a row that quietly fell back would file one shape of
//! number under another's name, and nothing downstream could tell.
//!
//! [RFC 0065]: ../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md
//! [RFC 0060]: ../../rfcs/0060-comparative-benchmark-suite-DEPRECATED.md

pub mod embedded;
pub mod micro;
pub mod points;
#[cfg(feature = "servers")]
pub mod server;
pub mod shop;
#[cfg(feature = "servers")]
pub mod shop_server;

use crate::corpus::{RowKey, RowRecord};
use crate::systems::{Cfg, Durability};

/// What one row needs in order to run.
pub struct RowRun {
    pub key: RowKey,
    pub cfg: Cfg,
    pub shop: crate::systems::shop::ShopCfg,
    pub timestamp: String,
    pub dirty: bool,
    pub caged: bool,
    pub forced: bool,
}

/// Run the one configuration `key` names.
///
/// # Errors
/// The adapter's own failure, or a configuration this build cannot measure.
pub fn measure(run: &RowRun) -> Result<RowRecord, String> {
    let d = durability_of(&run.key.durability)?;
    if let Some(record) = micro::measure(run, d)? {
        return Ok(record);
    }
    if let Some(record) = shop::measure(run, d)? {
        return Ok(record);
    }
    Err(format!(
        "no adapter for {}/{}",
        run.key.system, run.key.workload
    ))
}

/// # Errors
/// A durability name no row uses.
pub fn durability_of(name: &str) -> Result<Durability, String> {
    match name {
        "durable" => Ok(Durability::Durable),
        "relaxed" => Ok(Durability::Relaxed),
        other => Err(format!("unknown durability {other:?}")),
    }
}

/// # Errors
/// A variant name no WaveDB row uses.
pub fn engine_of(
    variant: &str,
) -> Result<crate::systems::engine::Engine, String> {
    use crate::systems::engine::Engine;
    match variant {
        "single" => Ok(Engine::Direct),
        "multi" => Ok(Engine::Sharded),
        other => Err(format!("unknown wavedb variant {other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{durability_of, engine_of};
    use crate::systems::Durability;
    use crate::systems::engine::Engine;

    #[test]
    fn the_two_durability_rows_are_the_only_ones() {
        assert_eq!(durability_of("durable"), Ok(Durability::Durable));
        assert_eq!(durability_of("relaxed"), Ok(Durability::Relaxed));
        assert!(durability_of("fast").is_err());
    }

    #[test]
    fn the_two_wavedb_variants_are_the_only_ones() {
        assert_eq!(engine_of("single"), Ok(Engine::Direct));
        assert_eq!(engine_of("multi"), Ok(Engine::Sharded));
        assert!(engine_of("sharded").is_err());
    }
}
