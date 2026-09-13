//! The adapters, on the [RFC 0065] producer/consumer seam.
//!
//! One module per system, each supplying a
//! [`DriverFactory`](crate::harness::DriverFactory) and the
//! [`Driver`](crate::harness::Driver) it builds. They replace RFC 0060's `run`
//! functions, which owned their own loops, their own timing and their own
//! phase boundaries — and could therefore drift from each other.
//!
//! **The server bracket's drivers hold a client, never a server.** A server
//! process outlives every driver that connects to it: it is started before the
//! row and stopped after the last `close`, because a data directory read while
//! a server still holds it measures that server's deferral rather than its
//! storage.
//!
//! [RFC 0065]: ../../../rfcs/0065-benchmark-suite-ii-the-row-as-the-unit.md

#[cfg(feature = "servers")]
pub mod mongodb;
#[cfg(feature = "servers")]
pub mod mysql;
#[cfg(feature = "servers")]
pub mod postgres;
pub mod sqlite;
pub mod wavedb;
