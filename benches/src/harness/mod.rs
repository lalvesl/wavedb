//! The producer/consumer harness ([RFC 0065] §3).
//!
//! One generator thread builds the workload and routes it; one or more
//! consumer threads execute it and time only the execution. Everything runs
//! inside the row's cage, so the generator competes for the same CPUs the
//! database does — which is the standardisation the shape exists for.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

pub mod consumer;
pub mod driver;
pub mod micro;
pub mod run;
pub mod workload;

pub use driver::{Driver, DriverFactory};
pub use run::{PhaseResult, percentiles, run, run_with, throughput};
pub use workload::Workload;
