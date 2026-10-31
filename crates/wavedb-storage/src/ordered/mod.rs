//! The quantized page index of an ordered lane ([RFC 0078]).
//!
//! A lane's pages hold contiguous ranges of keys, sorted, and are found
//! through this index alone: sorted [`SubHeader`]s, and under each one entry
//! per page — `[q: u16 | address and size: 16 bits]`, the page's first
//! quantum and where it lies, scaled by the sub-header's factors — plus a
//! column of per-page metadata (`()`, or a `u8` count in a list's lane).
//!
//! A lookup is two binary searches in RAM: the range, then the entry. A page
//! boundary is always a quantum boundary, so it names one page. Everything
//! that reshapes the ranges — a page cut in two, a quantum refined, a page
//! that its range cannot address — happens here, in RAM, in the round that
//! writes the pages.
//!
//! This module is the index alone (RFC 0078 phase 1); the lane that writes
//! pages through it and the header that persists it come after.
//!
//! [RFC 0078]: ../../../../rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md

mod head;
mod index;
pub mod key;
mod reshape;
#[cfg(test)]
mod tests;

pub use head::{MAX_SHIFT, QUANTA, Shape, SubHeader, q_of};
pub use index::{Entries, Found, PageIndex, Placed, Slot};
pub use reshape::{Cut, CutOutcome};
