//! Per-NonUnique generated types: the `{Name}PivotId` handle and the `{Name}Pivot`
//! roots holder.
//!
//! These carry no business data — pure addressing. `PivotId` is a `LocalId`
//! newtype callers store in a field to reference the collection; `Pivot` holds the
//! `current` / `dead` B+tree roots plus one root per secondary index, and
//! implements [`wavedb_core::index::Pivot`].
//!
//! The full `BpTree` *implementation* lives in `wavedb-storage` (it needs the page
//! engine); the macro generates only the data-carrying types here.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, parse_quote};

use crate::declarations::Declared;
use crate::secondaries;
use crate::{exec_ops, lists, storage_statics, struct_hash, wire_derive};

/// The `{Name}PivotId` newtype: `struct {Name}PivotId(pub LocalId);` with its
/// `WaveWire` impl (delegating to `LocalId`) and wrap/unwrap accessors.
fn pivot_id_tokens(pivot_id: &Ident) -> syn::Result<TokenStream> {
    let pivot_id_def: syn::DeriveInput = parse_quote! {
        #[derive(::core::clone::Clone, ::core::marker::Copy,
                 ::core::cmp::PartialEq, ::core::cmp::Eq,
                 ::core::fmt::Debug, ::core::default::Default)]
        pub struct #pivot_id(pub ::wavedb_core::LocalId);
    };
    let pivot_id_wire = wire_derive::derive(&pivot_id_def)?;
    Ok(quote! {
        #pivot_id_def
        #pivot_id_wire

        impl #pivot_id {
            /// Wrap a `LocalId` as this collection's handle.
            #[must_use]
            pub const fn new(local: ::wavedb_core::LocalId) -> Self {
                Self(local)
            }
            /// The underlying `LocalId`.
            #[must_use]
            pub const fn local_id(self) -> ::wavedb_core::LocalId {
                self.0
            }
        }

        impl ::wavedb_core::PivotHandle for #pivot_id {
            fn local_id(self) -> ::wavedb_core::LocalId { self.0 }
            fn from_local_id(local: ::wavedb_core::LocalId) -> Self {
                Self(local)
            }
        }
    })
}

/// The `natural_key` impl item of a `#[wavedb::key(...)]` type: the anchor
/// is SeaHash over the declared key fields' wire bytes, declaration order —
/// derived through core's one `natural_key_hash` fn so every build maps the
/// same field values to the same address.
fn natural_key_items(key_fields: Option<&[(Ident, syn::Type)]>) -> TokenStream {
    key_fields.map_or_else(TokenStream::new, |fields| {
        let extends = fields.iter().map(|(f, _)| {
            quote! {
                bytes.extend_from_slice(
                    &::wavedb_core::wire::to_wire(&self.#f),
                );
            }
        });
        quote! {
            fn natural_key(&self) -> ::core::option::Option<u64> {
                let mut bytes = ::std::vec::Vec::new();
                #(#extends)*
                ::core::option::Option::Some(
                    ::wavedb_core::natural_key_hash(&bytes),
                )
            }
        }
    })
}

/// The pivot record's own identity: hashed like any struct, under the
/// reserved `Pivot` shape discriminator so it can never collide with a
/// user-declared `Unique`/`NonUnique` type of the same name and fields.
fn pivot_identity(
    pivot: &Ident,
    num_secondaries: usize,
    num_lists: usize,
    num_fuzzy: usize,
) -> u64 {
    // A pivot with no fuzzy index keeps the identity it had before the kind
    // existed: the entry appears only when one is declared, exactly as the
    // record type's `#fuzzy` fold does.
    let mut shape = vec![
        ("records".into(), "ChainRoots".into()),
        ("removals".into(), "LogRoots".into()),
        ("secondaries".into(), format!("[LocalId;{num_secondaries}]")),
        ("lists".into(), format!("[ChainRoots;{num_lists}]")),
    ];
    if num_fuzzy > 0 {
        shape.push(("fuzzy".into(), format!("[LocalId;{num_fuzzy}]")));
    }
    shape.push(("permission".into(), "Option<PermissionRef>".into()));
    struct_hash::compute(&pivot.to_string(), "Pivot", &shape)
}

/// The `Pivot` trait impl for the generated roots holder: the B+tree roots
/// and the chain roots that are succeeding them (RFC 0050), plus the two
/// rewrite constructors the engine uses when a root moves.
fn pivot_impl(pivot: &Ident, pivot_hash: u64) -> TokenStream {
    quote! {
        impl ::wavedb_core::index::Pivot for #pivot {
            const STRUCT_HASH: u64 = #pivot_hash;

            fn secondaries(&self) -> &[::wavedb_core::LocalId] { &self.secondaries }
            fn recency(&self) -> ::wavedb_core::ChainRoots { self.recency }
            fn removals(&self) -> ::wavedb_core::LogRoots { self.removals }
            fn lists(&self) -> &[::wavedb_core::ChainRoots] { &self.lists }
            fn fuzzy(&self) -> &[::wavedb_core::LocalId] { &self.fuzzy }
            fn permission(&self) -> ::core::option::Option<&::wavedb_core::PermissionRef> {
                self.permission.as_ref()
            }
            fn replace_roots(
                &self,
                roots: ::wavedb_core::index::Roots<'_>,
            ) -> Self {
                let mut secs = self.secondaries;
                let mut lists = self.lists;
                let mut fuzzy = self.fuzzy;
                // The engine always passes exactly this pivot's root counts
                // (it derives every slice from the accessors); a mismatch is
                // a caller bug worth failing loudly on.
                secs.copy_from_slice(roots.secondaries);
                lists.copy_from_slice(roots.lists);
                fuzzy.copy_from_slice(roots.fuzzy);
                Self {
                    recency: roots.recency,
                    removals: roots.removals,
                    secondaries: secs,
                    lists,
                    fuzzy,
                    permission: ::core::clone::Clone::clone(&self.permission),
                }
            }
        }
    }
}
/// Emit the `PivotId` newtype and the `Pivot` roots holder for a NonUnique struct
/// named `name` with the given resolved secondary indexes and (for a
/// `#[wavedb::key(...)]` type) natural-key fields.
pub fn nonunique_types(
    name: &Ident,
    hash: u64,
    declared: &Declared,
    page: Option<usize>,
) -> syn::Result<TokenStream> {
    // `page = N` rides the trait as a const the chain builders read; unset
    // leaves the trait default (RFC 0052).
    let page_const = page.map(|n| quote!(const PAGE: usize = #n;));
    let secondaries_specs = &declared.secondaries;
    let num_secondaries = secondaries_specs.len();
    let num_lists = declared.lists.len();
    let num_fuzzy = declared.fuzzy.len();
    let pivot_id = format_ident!("{}PivotId", name);
    let pivot = format_ident!("{}Pivot", name);
    let pivot_hash =
        pivot_identity(&pivot, num_secondaries, num_lists, num_fuzzy);
    let pivot_id_tokens = pivot_id_tokens(&pivot_id)?;

    // `struct {Name}Pivot { current, dead, secondaries: [LocalId; N], permission }`.
    // `#[repr(C)]` keeps a clean layout when `N == 0` (a trailing zero-sized array
    // is otherwise a lint footgun); WaveWire never depends on the repr.
    // The pivot record is stored like any value, so it gets its own native
    // storage slot too (always compressed — pivots are tiny, cold, uniform);
    // the record type's `storage_entries()` bundles both.
    let pivot_hash_expr =
        quote!(<#pivot as ::wavedb_core::index::Pivot>::STRUCT_HASH);
    let pivot_storage =
        storage_statics::statics_for(&pivot, &pivot_hash_expr, true);
    let storage_entries = storage_statics::entries_for(name, Some(&pivot));

    // Secondary-index hooks + the typed `by_<field>` lookup surface, and the
    // declared lists' hooks + their `listed_by_<fields>` enumerations.
    let secondary_items = secondaries::trait_items(secondaries_specs);
    let by_lookups = secondaries::by_lookups(name, secondaries_specs);
    let list_items = lists::trait_items(&declared.lists);
    let listed_readers = lists::listed_readers(name, &declared.lists);
    let fuzzy_items = crate::fuzzy::trait_items(&declared.fuzzy);
    let fuzzy_lookups = crate::fuzzy::fuzzy_lookups(name, &declared.fuzzy);
    let key_items = natural_key_items(declared.key_fields.as_deref());

    // The per-command execution steps (`__wavedb_<op>`) — defined here,
    // wire-reachable only once listed in an exposure declaration.
    let exec_steps = exec_ops::nonunique_ops(name);
    let pivot_impl = pivot_impl(&pivot, pivot_hash);
    let lane_statics = storage_statics::lane_statics(name, hash);

    let pivot_def: syn::DeriveInput = parse_quote! {
        #[repr(C)]
        #[derive(::core::clone::Clone, ::core::cmp::PartialEq, ::core::cmp::Eq,
                 ::core::fmt::Debug, ::core::default::Default)]
        pub struct #pivot {
            pub recency: ::wavedb_core::ChainRoots,
            pub removals: ::wavedb_core::LogRoots,
            pub secondaries: [::wavedb_core::LocalId; #num_secondaries],
            pub lists: [::wavedb_core::ChainRoots; #num_lists],
            pub fuzzy: [::wavedb_core::LocalId; #num_fuzzy],
            pub permission: ::core::option::Option<::wavedb_core::PermissionRef>,
        }
    };
    let pivot_wire = wire_derive::derive(&pivot_def)?;

    Ok(quote! {
        #pivot_id_tokens

        #pivot_def
        #pivot_wire

        impl ::wavedb_core::NonUniqueStruct for #name {
            type Pivot = #pivot;
            #page_const
            #secondary_items
            #list_items
            #fuzzy_items
            #key_items
        }

        #by_lookups
        #fuzzy_lookups
        #listed_readers

        impl #name {
            /// The typed handle into an existing collection of this type,
            /// referenced by the `PivotId` an owning record stores. Methods
            /// take the [`DbHandle`](::wavedb_core::DbHandle) per call, so
            /// one handle value works against every execution context.
            #[must_use]
            pub const fn collection(
                pivot: #pivot_id,
            ) -> ::wavedb_core::CollectionHandle<Self> {
                ::wavedb_core::CollectionHandle::at(pivot.local_id())
            }

            /// Create a new, empty collection of this type under the
            /// context's tenant — explicit, never automatic. Store the
            /// returned `PivotId` in an owning record to keep the collection
            /// reachable.
            ///
            /// # Errors
            /// The context's failure (backend/transport).
            #[allow(clippy::future_not_send)]
            pub async fn create_pivot<D: ::wavedb_core::DbHandle>(
                db: &D,
            ) -> ::core::result::Result<#pivot_id, D::Error> {
                db.create_pivot::<Self>().await.map(#pivot_id::new)
            }
        }

        #pivot_storage
        #storage_entries
        #exec_steps
        #pivot_impl
        #lane_statics
    })
}
