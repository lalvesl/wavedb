//! The native per-struct storage declaration: a
//! `#[cfg(not(target_arch = "wasm32"))]` `static StructStorage` emitted on
//! every `#[wavedb]` type and on each generated `{Name}Pivot`.
//!
//! It holds what is fixed at compile time — the `STRUCT_HASH` and the
//! compression policy — and no state. The statics are handed to
//! `PageStore::open` as an explicit registry (`T::storage_entries()`), the
//! same declared-not-discovered stance the exposure lists take, and each open
//! store keeps the types' caches and directories itself (RFC 0067).
//!
//! Consequence for consumers: on non-wasm targets, a crate using `#[wavedb]`
//! needs `wavedb-storage` as a (target-gated) dependency. The wasm expansion
//! omits all of this — the browser store is IndexedDB, no pages, no slots.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

/// Emit the storage-slot impl for `ty`; `hash_expr` is the `const` expression
/// yielding its `STRUCT_HASH` (inherent for structs, the `Pivot` trait const
/// for generated pivots). `compress = false` (`#[wavedb(compress = false)]`)
/// opts the type's pages out of zstd — storage policy, never schema identity;
/// `resident` keeps its pages from eviction (a Pivot: navigation, RFC 0070
/// §10) — behaviour, never schema identity either.
pub fn statics_for(
    ty: &Ident,
    hash_expr: &TokenStream,
    compress: bool,
    resident: bool,
) -> TokenStream {
    let ctor = if compress {
        quote!(new)
    } else {
        quote!(without_compression)
    };
    let resident = resident.then(|| quote!(.resident()));
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        impl #ty {
            /// This type's storage declaration — its `STRUCT_HASH` and
            /// compression policy, fixed at compile time. Register it (via
            /// `storage_entries()`) when opening a `PageStore`.
            #[must_use]
            pub fn struct_storage() -> &'static ::wavedb_storage::StructStorage {
                static SLOT: ::wavedb_storage::StructStorage =
                    ::wavedb_storage::StructStorage::#ctor(#hash_expr)#resident;
                &SLOT
            }
        }
    }
}

/// Emit `storage_entries()` on the record type: every slot the type needs
/// registered at `PageStore::open` — its own, plus its Pivot's for NonUnique.
pub fn entries_for(name: &Ident, pivot: Option<&Ident>) -> TokenStream {
    // A NonUnique type owns five ordered lanes — its declared lists, the
    // recency chain, the removal log, and the secondary and fuzzy chains
    // (RFC 0075, RFC 0078) — each an index of its own, so each needs its own
    // registered slot. Recency and `dead` are separated from the record lane
    // because they hold ids, not records (RFC 0054), and a lane exists so
    // a page holds one kind of thing.
    let (len, list) = pivot.map_or_else(
        || (quote!(1usize), quote!([Self::struct_storage()])),
        |pivot| {
            (
                quote!(7usize),
                quote!([
                    Self::struct_storage(),
                    #pivot::struct_storage(),
                    Self::records_lane_storage(),
                    Self::recency_lane_storage(),
                    Self::dead_lane_storage(),
                    Self::secondary_lane_storage(),
                    Self::fuzzy_lane_storage(),
                ]),
            )
        },
    );
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        impl #name {
            /// The storage slots to register at `PageStore::open` for this
            /// type: its own, plus the generated Pivot's for a NonUnique.
            #[must_use]
            pub fn storage_entries()
            -> [&'static ::wavedb_storage::StructStorage; #len] {
                #list
            }
        }
    }
}

/// The ordered-lane slots a NonUnique type needs registered: its lists',
/// its recency chain's, its removal log's (RFC 0050), and its secondary and
/// fuzzy chains' (RFC 0075).
///
/// Each lane is its own ordered index (RFC 0078 §7), which is what keeps a
/// page homogeneous — fat inline records and skinny `[instant][anchor]`
/// pairs never share one, so each page compresses one kind of content.
/// The hashes are derived at expansion time
/// ([`struct_hash::lane_hash`]) because a `static` needs a `const` initialiser.
pub fn lane_statics(name: &Ident, hash: u64) -> TokenStream {
    let records = crate::struct_hash::lane_hash(b"WDB.SEG", hash);
    let recency = crate::struct_hash::lane_hash(b"WDB.REC", hash);
    let dead = crate::struct_hash::lane_hash(b"WDB.DEAD", hash);
    let secondary = crate::struct_hash::lane_hash(b"WDB.SEC", hash);
    let fuzzy = crate::struct_hash::lane_hash(b"WDB.FZY", hash);
    quote! {
        #[cfg(not(target_arch = "wasm32"))]
        impl #name {
            /// Storage slot for this type's **recency chain** entries —
            /// id-only, like the removal log's.
            #[must_use]
            pub fn recency_lane_storage()
            -> &'static ::wavedb_storage::StructStorage {
                static SLOT: ::wavedb_storage::StructStorage =
                    ::wavedb_storage::StructStorage::new(#recency).resident();
                &SLOT
            }

            /// Storage slot for this type's **declared lists** — the only
            /// entries holding whole records, and the only ones counted
            /// per page, for a list's length and its pages.
            #[must_use]
            pub fn records_lane_storage()
            -> &'static ::wavedb_storage::StructStorage {
                static SLOT: ::wavedb_storage::StructStorage =
                    ::wavedb_storage::StructStorage::new(#records).counted();
                &SLOT
            }

            /// Storage slot for this type's **removal log** entries.
            #[must_use]
            pub fn dead_lane_storage()
            -> &'static ::wavedb_storage::StructStorage {
                static SLOT: ::wavedb_storage::StructStorage =
                    ::wavedb_storage::StructStorage::new(#dead).resident();
                &SLOT
            }

            /// Storage slot for this type's **secondary chains** — the
            /// `#[wavedb::pivot(...)]` indexes, `[field][anchor]` pointers
            /// (RFC 0075).
            #[must_use]
            pub fn secondary_lane_storage()
            -> &'static ::wavedb_storage::StructStorage {
                static SLOT: ::wavedb_storage::StructStorage =
                    ::wavedb_storage::StructStorage::new(#secondary).resident();
                &SLOT
            }

            /// Storage slot for this type's **fuzzy posting chains** — the
            /// `#[wavedb::fuzzy]` indexes, one pointer per gram (RFC 0075).
            #[must_use]
            pub fn fuzzy_lane_storage()
            -> &'static ::wavedb_storage::StructStorage {
                static SLOT: ::wavedb_storage::StructStorage =
                    ::wavedb_storage::StructStorage::new(#fuzzy).resident();
                &SLOT
            }
        }
    }
}
