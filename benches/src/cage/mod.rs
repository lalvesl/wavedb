//! The cage: 500 MB and four CPUs, per **row**.
//!
//! [`verify`] is the guard a measured process runs on itself — the rule that
//! an uncaged number may not enter the corpus. [`exec`] is the other side, new
//! in [RFC 0065]: the supervisor builds one cage per row and runs `bench-row`
//! inside it, rather than one cage wrapping a whole pass.
//!
//! The move from per-run to per-row is not tidiness. A fresh cgroup per row
//! means the memory budget starts empty, where previously row *k* inherited
//! whatever page cache row *k−1* left behind — and since `MemoryMax` bounds
//! page cache, that inheritance was not neutral. It also means an OOM kill or
//! a startup timeout costs one row instead of a fifty-minute pass.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

mod exec;
mod verify;

pub use exec::{
    CageSpec, MASK_ENV, Outcome, command_line, exec_row, outcome_of,
    sibling_binary, unit_name,
};
pub use verify::{init, is_caged, verify};
