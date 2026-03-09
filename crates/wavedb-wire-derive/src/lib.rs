//! `#[derive(WaveWire)]` — emit a [`WaveWire`] impl and the type's borrowed
//! twin `NameRef<'a>`, the companion proc-macro of the [`wavedb-wire`] crate
//! (re-exported as `wavedb_wire::WaveWire`).
//!
//! The expansion is `wavedb-wire-codegen`'s, through absolute
//! `::wavedb_wire::` paths, so it works from any crate that depends on
//! `wavedb-wire` (including `wavedb-wire` itself via
//! `extern crate self as wavedb_wire`). See that crate for the shapes:
//! structs (named, tuple, unit) and enums in the canonical tag form.
//!
//! [`WaveWire`]: https://docs.rs/wavedb-wire
//! [`wavedb-wire`]: https://docs.rs/wavedb-wire

use proc_macro::TokenStream;
use quote::quote;
use syn::parse_macro_input;

/// Derive `WaveWire` for a struct or enum, and its borrowed twin `NameRef`.
#[proc_macro_derive(WaveWire, attributes(wave_wire))]
pub fn wave_wire(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::DeriveInput);
    wavedb_wire_codegen::expand(&input, &quote!(::wavedb_wire))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
