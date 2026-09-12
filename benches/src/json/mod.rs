//! A minimal JSON writer and reader.
//!
//! Hand-rolled rather than `serde`: the project's stance is that byte layouts
//! are written, not derived, and a results record is a fixed shape that needs
//! no reflection. It also keeps the bench crate's dependency set to the
//! competitor drivers alone.
//!
//! The reader exists because [RFC 0065] makes a stored row an **input**: peer
//! rows are served from the corpus instead of being remeasured, and the
//! evolution table is built by reading every WaveDB row back. Until then this
//! module only ever wrote.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

mod read;
mod write;

pub use read::{Value, parse};
pub use write::{Json, fnv1a};
