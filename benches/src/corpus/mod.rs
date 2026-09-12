//! The row corpus — measurements as individually addressable files
//! ([RFC 0065] §1).
//!
//! A run no longer produces *a* results file. It produces one file per **row**,
//! at `benches/results/rows/<host-key>/<digest>.json`, and a later run reads
//! back the rows it does not need to measure again. That inversion — a stored
//! measurement being an **input** — is what this module exists for, and it is
//! why [`crate::json`] grew a reader.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

mod decode;
mod record;
mod store;

pub use decode::from_json;
pub use record::{FootprintRecord, PhaseRecord, RowKey, RowRecord, SCHEMA};
pub use store::{Corpus, Unreadable};
