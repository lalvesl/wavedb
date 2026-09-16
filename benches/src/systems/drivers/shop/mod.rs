//! The `shop` workload's adapters, on the [RFC 0065] producer/consumer seam.
//!
//! ## What differs from `micro`, and why it is the point
//!
//! A `micro` op is one record. A shop op is a **customer action**, and the
//! five systems disagree about what that costs:
//!
//! - The SQL trio and MongoDB wrap a checkout in **one transaction** — the
//!   order and its line items commit together, one barrier.
//! - WaveDB has no multi-record transaction: every collection op is its own
//!   atomic `Store::apply` batch, so a checkout with five line items is
//!   **six** batches and six barriers. That is the data model, not a tuning
//!   gap, and pricing it is why this workload exists.
//!
//! Neither is visible from the operation stream: an op says *what happened*,
//! a driver says *how it is done*.
//!
//! ## The preload is not here
//!
//! It is bulk, untimed, single-threaded, and each system does it in the shape
//! its own bulk loader wants. The row runs it before the harness starts (see
//! `row::shop`), so a driver only ever sees the measured window.
//!
//! [RFC 0065]: ../../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

pub mod engine;
#[cfg(feature = "servers")]
pub mod mongo_server;
#[cfg(feature = "servers")]
pub mod mongodb;
pub mod mysql;
#[cfg(feature = "servers")]
pub mod postgres;
pub mod sqlite;
pub mod wavedb;

/// The phase after which every system quiesces and empties its read cache.
///
/// One name, in one place, because the rule has to be identical on all five:
/// the two write phases run, the system settles, and the three read phases
/// then measure a database nobody has just written to. A system that
/// checkpointed after a different phase would be answering a different
/// question under the same column heading.
pub const SETTLE_AFTER: &str = "checkout";

/// The **server's** disk writes, read from the shared handle.
///
/// One helper for all three server rows, because the shape is identical and a
/// per-system copy is a per-system chance to read the wrong process: a phase
/// boundary restarts the server, so the pid has to come out of the slot at the
/// start of each phase rather than being remembered from the first.
#[cfg(feature = "servers")]
#[must_use]
pub fn server_writer(
    held: &std::sync::Arc<
        std::sync::Mutex<Option<crate::systems::server::Server>>,
    >,
) -> crate::metrics::Writer {
    match held.lock() {
        Ok(g) => g.as_ref().map_or(crate::metrics::Writer::Current, |s| {
            crate::metrics::Writer::Pid(s.pid)
        }),
        // A poisoned mutex means a consumer panicked mid-restart; the row is
        // already failing, and a wrong pid would not make it clearer.
        Err(_) => crate::metrics::Writer::Current,
    }
}
