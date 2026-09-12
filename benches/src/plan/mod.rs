//! The plan: what a run measures, and who executes each piece of it.
//!
//! [`op`] is the first half — the descriptor a generator produces and a
//! consumer executes. Row identity, the corpus lookup and the schedule follow
//! in the same module ([RFC 0065] §1).
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

pub mod op;
pub mod resolve;
