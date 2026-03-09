//! `#[derive(WaveWire)]` — emit a [`WaveWire`] impl and the type's borrowed
//! twin `NameRef<'a>` (RFC 0076).
//!
//! The expansion is `wavedb-wire-codegen`'s, the same as `wavedb-wire`'s own
//! derive, but named through absolute `::wavedb_core::wire::` paths, so a
//! schema crate needs only `wavedb-core` (and `wavedb-core` itself works via
//! `extern crate self`).
//!
//! [`WaveWire`]: wavedb_core::wire::WaveWire

use proc_macro2::TokenStream;
use quote::quote;
use syn::DeriveInput;

/// Expand `#[derive(WaveWire)]` for `input`.
pub fn derive(input: &DeriveInput) -> syn::Result<TokenStream> {
    wavedb_wire_codegen::expand(input, &quote!(::wavedb_core::wire))
}

/// [`derive`] for a type the macro builds itself, made its own view (a
/// generated `{Name}PivotId`): `Copy`, no heap bytes, so a twin would only
/// be a copy to convert back.
pub fn derive_own_view(input: &DeriveInput) -> syn::Result<TokenStream> {
    wavedb_wire_codegen::expand_as(input, &quote!(::wavedb_core::wire), true)
}
