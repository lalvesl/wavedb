//! Re-export of the standalone [`wavedb-wire`](wavedb_wire) crate.
//!
//! The `Wire` (de)serialization format lives in its own dependency-free crate
//! now — pure value ⇄ bytes, with no `STRUCT_HASH`, registry, or engine coupling.
//! `wavedb-core` re-exports it here so every existing `crate::wire::…` /
//! `wavedb_core::wire::…` path keeps resolving and the `#[derive(WaveWire)]`
//! codegen can target `wavedb_core::wire::{Wire, Cursor, Result}`.
//!
//! `wavedb_core::Error` wraps [`wavedb_wire::Error`] via `#[from]`, so a wire
//! decode `?`-propagates into the richer workspace error. The `Wire` impls for
//! WaveDB's own types ([`Id`](crate::Id), [`LocalId`](crate::LocalId),
//! [`U48`](crate::U48), [`Metadata`](crate::Metadata),
//! [`PermissionRef`](crate::PermissionRef)) live next to those types.

pub use wavedb_wire::{
    Cursor, Error, ListIter, ListRef, Result, WaveWire, Wire, from_wire,
    to_wire, to_wire_pair, view_wire,
};

// The crc32-framed checked encoding, behind the same-named `validation`
// feature (forwarded to `wavedb-wire/validation` in Cargo.toml).
#[cfg(feature = "validation")]
pub use wavedb_wire::{
    CRC_PREFIX_LEN, from_wire_checked, to_wire_checked, view_wire_checked,
};
