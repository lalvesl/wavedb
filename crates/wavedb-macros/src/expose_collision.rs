//! The generated registry **collision guard** — split from [`crate::expose`]
//! for the file budget.
//!
//! Both checks are pairwise over the declared entries and both resolve at
//! **compile time**, so a clash is caught by `cargo check`, never deferred to a
//! test run:
//!
//! - **Full 64-bit `STRUCT_HASH`** — a hard error. Two entries sharing it would
//!   have one arm silently shadow the other in every dispatch `match`, and the
//!   wire could not tell the types apart at all.
//! - **15-bit `type_salt`** (the low bits of the same hash) — a warning. Reads
//!   stay correct (the full head is verified), but the pair shares archive slots
//!   and loses its separation in the browser's flat keyspace, so it is surfaced
//!   rather than enforced.
//!
//! The salt check counts **every occupant**, not one per entry: since RFC 0050
//! a NonUnique type also reserves six chain lanes (`WDB.SEG`, `WDB.REC`,
//! `WDB.DEAD`, `WDB.IDX`, `WDB.SEC`, `WDB.FZY`), each with a hash of its own and
//! therefore a salt of its own. A registry of `n` such types puts `7n` values in
//! a 15-bit space — comparing only the record hashes would leave six sevenths of them
//! unchecked, over
//! exactly the property the lanes exist to provide ("a segment id can never
//! equal a record anchor, an archive slot, or a tree node"). Hence also the
//! per-entry self-check: a type can clash with its own lane alone.

use proc_macro2::TokenStream;
use quote::{quote, quote_spanned};
use syn::spanned::Spanned as _;

use crate::expose_arms::hash_expr;
use crate::expose_parse::{Entry, Kind};

/// Emit the compile-time guards for `entries` (see the module docs).
///
/// Emitted at the invocation scope (not a nested module) so entry paths resolve
/// exactly as written in the declaration, and every diagnostic is spanned at the
/// **second entry of the clashing pair** — the compiler underlines the offending
/// line in the exposure list itself.
pub fn collision_guard(entries: &[Entry]) -> TokenStream {
    let mut guards = TokenStream::new();
    for (i, b) in entries.iter().enumerate() {
        // `fn`s share the dispatch hash space but are never stored, so the
        // storage-slot discriminator does not apply to them.
        if b.kind != Kind::Fn {
            guards.extend(self_salt_warning(b));
        }
        for a in &entries[..i] {
            guards.extend(identity_error(a, b));
            if a.kind != Kind::Fn && b.kind != Kind::Fn {
                guards.extend(salt_warning(a, b));
            }
        }
    }
    guards
}

/// The hard error: a pair sharing the full 64-bit identity.
fn identity_error(a: &Entry, b: &Entry) -> TokenStream {
    let (ha, hb) = (hash_expr(a), hash_expr(b));
    let message = format!(
        "STRUCT_HASH collision between `{}` and `{}`: the two are one identity \
         on the wire, so one would shadow the other in every dispatch match. \
         Rename the type or a field to reshuffle the hash.",
        render(&a.path),
        render(&b.path),
    );
    let span = b.path.span();
    quote_spanned! { span =>
        const _: () = ::core::assert!(#ha != #hb, #message);
    }
}

/// A path as written, without the token stream's inter-token spacing
/// (`tw1 :: Twin` → `tw1::Twin`).
fn render(path: &syn::Path) -> String {
    quote!(#path).to_string().split_whitespace().collect()
}

/// The warning: two entries sharing the 15-bit storage discriminator.
///
/// An entry occupies its record type's salt **and** one per reserved chain
/// lane its collection rides (RFC 0050), so the check runs over both entries'
/// full occupant sets rather than the two record hashes alone.
fn salt_warning(a: &Entry, b: &Entry) -> TokenStream {
    let (ha, hb) = (hash_expr(a), hash_expr(b));
    let (la, lb) = (lanes_expr(a), lanes_expr(b));
    quote_spanned! { b.path.span() =>
        const _: () = ::wavedb_core::expose::SaltGuard::<
            { ::wavedb_core::expose::salts_distinct(#ha, #la, #hb, #lb) },
        >::check();
    }
}

/// The same warning for **one** entry against itself: a type can share the
/// discriminator with one of its own lanes with no second entry in sight, so
/// this fires even on a one-item registry.
fn self_salt_warning(entry: &Entry) -> TokenStream {
    let (h, lanes) = (hash_expr(entry), lanes_expr(entry));
    quote_spanned! { entry.path.span() =>
        const _: () = ::wavedb_core::expose::SaltGuard::<
            { ::wavedb_core::expose::salts_self_distinct(#h, #lanes) },
        >::check();
    }
}

/// The reserved lane hashes an entry's storage occupies — empty for a
/// `Unique` type, which has no collection.
fn lanes_expr(entry: &Entry) -> TokenStream {
    let path = &entry.path;
    quote!(<#path as ::wavedb_core::WaveDbStruct>::LANE_HASHES)
}
