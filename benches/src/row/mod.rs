//! Measuring **one** row ([RFC 0065] Â§2).
//!
//! This is what runs inside a cage. It knows one configuration, produces one
//! [`RowRecord`], and stores it â no matrix, no ordering, no other system.
//!
//! ## Two paths, and the seam between them
//!
//! [`micro`] is the [RFC 0065] path: the producer/consumer harness, a real
//! wall-clock denominator, pooled per-operation percentiles. It answers for
//! the rows whose drivers exist.
//!
//! [`bridge`] is what remains of RFC 0060: the old `run` functions, each
//! owning its own loop, its own timing and its own phase boundaries. It now
//! serves the `shop` workload only — every `micro` row goes through the
//! harness — and the rows it still serves say so in their own notes rather
//! than in a comment nobody reading the corpus will see.
//!
//! The dispatch below is the whole seam, and it is deliberately a `match` on
//! the row's identity rather than a capability check: a row that quietly fell
//! back would file a phase-1 number under a phase-2 identity, and nothing
//! downstream could tell.
//!
//! [RFC 0065]: ../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md
//! [RFC 0060]: ../../rfcs/0060-comparative-benchmark-suite-DEPRECATED.md

pub mod bridge;
pub mod embedded;
pub mod micro;
pub mod points;
#[cfg(feature = "servers")]
pub mod server;

use crate::corpus::{RowKey, RowRecord};
use crate::footprint::Point;
use crate::systems::{Cfg, Durability, SystemReport};

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
    let report = dispatch(run, d)?;
    Ok(bridge::bridge(run, report))
}

fn dispatch(run: &RowRun, d: Durability) -> Result<SystemReport, String> {
    let key = &run.key;
    match (key.system.as_str(), key.workload.as_str()) {
        ("wavedb", "shop") => shop_wavedb(run, d),
        ("sqlite", "shop") => crate::systems::shop::sqlite::run(&run.shop, d),
        #[cfg(feature = "servers")]
        ("postgres", "shop") => {
            crate::systems::shop::postgres::run(&run.shop, d)
        }
        #[cfg(feature = "servers")]
        ("mysql", "shop") => crate::systems::shop::mysql::run(&run.shop, d),
        #[cfg(feature = "servers")]
        ("mongodb", "shop") => crate::systems::shop::mongodb::run(&run.shop, d),
        #[cfg(not(feature = "servers"))]
        ("postgres" | "mysql" | "mongodb", _) => Err(format!(
            "{}: built without the `servers` feature",
            key.system
        )),
        (system, workload) => {
            Err(format!("no adapter for {system}/{workload}"))
        }
    }
}

/// The sharded engine has no shop adapter yet â and a refusal is the only
/// honest answer.
///
/// `shop::wavedb::run` takes no [`Engine`](crate::systems::engine::Engine): it
/// drives the `PageStore` directly. Falling through to it for a `multi` row
/// would record a **`single` measurement under a `multi` identity**, which is
/// worse than a missing row by exactly the amount a reader would trust it.
fn shop_wavedb(run: &RowRun, d: Durability) -> Result<SystemReport, String> {
    if run.key.variant == "multi" {
        return Err(
            "wavedb/multi on the shop workload is not built yet (RFC 0065 \
             phase 2 step 2.6): the shop adapter drives the PageStore \
             directly, so measuring it here would file a single-threaded \
             number under a multi-threaded identity"
                .into(),
        );
    }
    crate::systems::shop::wavedb::run(&run.shop, d)
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

pub(crate) const fn point_name(point: Point) -> &'static str {
    match point {
        Point::Baseline => "baseline",
        Point::Hot => "hot",
        Point::Settled => "settled",
        Point::Compacted => "compacted",
    }
}

#[cfg(test)]
mod tests {
    use super::{durability_of, engine_of, point_name};
    use crate::footprint::Point;
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

    /// The names the corpus stores, which a reader's tooling keys on.
    #[test]
    fn every_footprint_point_has_a_stable_name() {
        assert_eq!(point_name(Point::Baseline), "baseline");
        assert_eq!(point_name(Point::Hot), "hot");
        assert_eq!(point_name(Point::Settled), "settled");
        assert_eq!(point_name(Point::Compacted), "compacted");
    }
}
