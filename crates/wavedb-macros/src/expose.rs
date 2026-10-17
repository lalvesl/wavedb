//! `expose_server!` / `expose_client!` — the **declared registry**.
//!
//! ```text
//! expose_server! {
//!     AboutUser,                                   // full generated op set
//!     Order { save: audited_save, remove: never }, // per-op override / exclusion
//!     fn login,                                    // #[server] function
//!     store Credentials,                           // storage-only: engine slot, no wire
//! }
//! ```
//!
//! The list expands to one `match` on the 64-bit `STRUCT_HASH` per operation,
//! each arm calling the item's `#[wavedb]`-generated `__wavedb_<op>` step (or
//! the override path, substituted **inside the arm** at expansion time; a
//! `never` arm refuses). Static dispatch only — no `dyn`, no fn-pointer
//! tables, no runtime registration. A `store` entry contributes **only** its
//! `StructStorage` slots to the emitted `StorageRegistry` — the type stays
//! wire-unaddressable (its hash refuses like one that never existed), which
//! is the functions-only app shape: every struct storage-only, reached inside
//! `#[server]` bodies. The server emission implements the full [`Exposure`]
//! trait (`REGISTRY`); the client one implements only the reachability half
//! (`CLIENT_REGISTRY`). The declaration grammar lives in
//! [`crate::expose_parse`].
//!
//! **Side gating.** `expose_server!`'s whole output rides the schema
//! crate's `server-side` feature and `expose_client!`'s its `client-side`
//! feature (the same contract as `#[server]`, see [`crate::server`]): a
//! client artifact built with `default-features = false,
//! features = ["client-side"]` contains no dispatch arm, no `#[server]`
//! body, no server registry — not compiled, not merely optimized away.
//! `expose_server!` additionally refuses (`compile_error!`) any wasm32
//! build that has `server-side` enabled: a browser is never a node.
//!
//! [`Exposure`]: wavedb_core::expose::Exposure

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::Path;

use crate::expose_arms::{
    addressing_arm, decode_arm, execute_arms, hash_expr, node_methods,
};
use crate::expose_parse::{Declaration, Entry, Kind};

/// The `StorageRegistry` impl for a registry type — native only, flattening
/// each listed struct **and `store` entry**'s `storage_entries()` (functions
/// hold no storage; the reserved BpTree-node slot is added by
/// `PageStore::open`). The server registry opens the node's engine with it;
/// the client registry opens `Db::open`'s local cache store with it.
/// `side` is the schema-crate feature the whole registry rides
/// (`"server-side"` / `"client-side"`).
fn storage_registry_impl(
    registry: &proc_macro2::Ident,
    struct_paths: &[&Path],
    side: &str,
) -> TokenStream {
    quote! {
        #[cfg(all(not(target_arch = "wasm32"), feature = #side))]
        impl ::wavedb_storage::StorageRegistry for #registry {
            fn storage_entries(
                &self,
            ) -> ::std::vec::Vec<&'static ::wavedb_storage::StructStorage>
            {
                let mut slots = ::std::vec::Vec::new();
                #(
                    slots.extend_from_slice(&#struct_paths::storage_entries());
                )*
                slots
            }
        }
    }
}

/// Expand `expose_server!`: the full [`Exposure`] impl + `REGISTRY`.
///
/// [`Exposure`]: wavedb_core::expose::Exposure
pub fn expand_server(input: TokenStream) -> syn::Result<TokenStream> {
    let decl: Declaration = syn::parse2(input)?;
    // Storage flattens structs AND `store` entries; the wire surface (knows /
    // decode / execute) sees structs and fns only — a `store` type's hash
    // must refuse exactly like one that never existed.
    let struct_paths: Vec<&Path> = decl
        .entries
        .iter()
        .filter(|e| e.kind != Kind::Fn)
        .map(|e| &e.path)
        .collect();
    let wire_entries: Vec<&Entry> = decl
        .entries
        .iter()
        .filter(|e| e.kind != Kind::Store)
        .collect();
    let hashes: Vec<TokenStream> =
        wire_entries.iter().copied().map(hash_expr).collect();
    let storage_impl = storage_registry_impl(
        &format_ident!("ServerRegistry"),
        &struct_paths,
        "server-side",
    );
    let decode_arms = wire_entries.iter().copied().map(decode_arm);
    let execute_arms = execute_arms(&wire_entries);
    let addressing_arms = wire_entries.iter().copied().map(addressing_arm);
    let node_methods = node_methods(&struct_paths, &wire_entries);
    let collision_guard =
        crate::expose_collision::collision_guard(&decl.entries);

    Ok(quote! {
        // The declared server surface is server code: everything below
        // rides the schema crate's `server-side` feature, so a client build
        // (`default-features = false, features = ["client-side"]`) never
        // compiles the dispatch arms or the `#[server]` steps they name.
        // And a browser artifact must never carry them at all — refuse the
        // misconfiguration outright instead of shipping it.
        #[cfg(all(target_arch = "wasm32", feature = "server-side"))]
        ::core::compile_error!(
            "`expose_server!` (the node dispatch and every `#[server]` body) \
             must never reach a wasm32 artifact: depend on the schema crate \
             with `default-features = false, features = [\"client-side\"]` \
             for browser builds"
        );

        /// The node-side registry `expose_server!` declared: exactly the
        /// listed items are wire-reachable, dispatched by a per-hash `match`.
        #[cfg(feature = "server-side")]
        #[derive(Debug, Clone, Copy)]
        pub struct ServerRegistry;

        /// What a node's `.registry(…)` takes — the declared surface.
        #[cfg(feature = "server-side")]
        pub const REGISTRY: ServerRegistry = ServerRegistry;

        #[cfg(feature = "server-side")]
        impl ::wavedb_core::expose::Exposure for ServerRegistry {
            fn knows(&self, struct_hash: u64) -> bool {
                [#(#hashes),*].contains(&struct_hash)
            }

            fn addressing(
                &self,
                struct_hash: u64,
            ) -> ::core::option::Option<::wavedb_core::expose::Addressing> {
                match struct_hash {
                    #(#addressing_arms)*
                    _ => ::core::option::Option::None,
                }
            }

            fn decode_check(
                &self,
                struct_hash: u64,
                bytes: &[u8],
            ) -> ::wavedb_core::Result<()> {
                match struct_hash {
                    #(#decode_arms)*
                    other => ::core::result::Result::Err(
                        ::wavedb_core::Error::UnknownStructHash(other),
                    ),
                }
            }

            // Store-generic seam — Send only when the backing store is,
            // the same stance the core traits take.
            #[allow(clippy::future_not_send)]
            async fn execute<S: ::wavedb_core::Store>(
                &self,
                store: &S,
                caller: ::wavedb_core::Caller,
                struct_hash: u64,
                command: ::wavedb_core::expose::Command,
                payload: &[u8],
            ) -> ::wavedb_core::Result<::wavedb_core::expose::Reply> {
                match struct_hash {
                    #(#execute_arms)*
                    other => ::core::result::Result::Err(
                        ::wavedb_core::Error::UnknownStructHash(other),
                    ),
                }
            }

            #node_methods
        }

        #storage_impl

        // The node's tasks, made concrete for this registry (RFC 0066):
        // Embassy keeps tasks in static pools, and a static needs the concrete
        // type, which only this crate has. Needs `wavedb-quick-node` under
        // this crate's `server-side` feature.
        #[cfg(all(not(target_arch = "wasm32"), feature = "server-side"))]
        ::wavedb_quick_node::node_tasks!(ServerRegistry);

        #collision_guard
    })
}

/// Expand `expose_client!`: the reachability half only (`knows` +
/// `decode_check`; `execute` keeps the trait's refusing default — a client
/// never runs the engine).
pub fn expand_client(input: TokenStream) -> syn::Result<TokenStream> {
    let decl: Declaration = syn::parse2(input)?;
    for entry in &decl.entries {
        if let Some((op, _)) = entry.overrides.first() {
            return Err(syn::Error::new_spanned(
                op,
                "expose_client! entries take no per-op overrides — \
                 overrides shape the server surface",
            ));
        }
        if entry.kind == Kind::Store {
            return Err(syn::Error::new_spanned(
                &entry.path,
                "`store` entries shape the node's storage surface — the \
                 client's cache store registers exactly the wire-reachable \
                 list, so a storage-only entry has nothing to serve here",
            ));
        }
    }
    let hashes: Vec<TokenStream> = decl.entries.iter().map(hash_expr).collect();
    let decode_arms = decl.entries.iter().map(decode_arm);
    // The cache half of `Db::open`: the same declared list, flattened to the
    // per-type engine slots (functions hold no storage).
    let struct_paths: Vec<&Path> = decl
        .entries
        .iter()
        .filter(|e| e.kind != Kind::Fn)
        .map(|e| &e.path)
        .collect();
    let storage_impl = storage_registry_impl(
        &format_ident!("ClientRegistry"),
        &struct_paths,
        "client-side",
    );
    let collision_guard =
        crate::expose_collision::collision_guard(&decl.entries);

    Ok(quote! {
        /// The client-side allowlist `expose_client!` declared: which items
        /// this binary's typed stubs may route (stubs land with `#[server]`).
        #[cfg(feature = "client-side")]
        #[derive(Debug, Clone, Copy)]
        pub struct ClientRegistry;

        /// The declared client surface.
        #[cfg(feature = "client-side")]
        pub const CLIENT_REGISTRY: ClientRegistry = ClientRegistry;

        #[cfg(feature = "client-side")]
        impl ::wavedb_core::expose::Exposure for ClientRegistry {
            fn knows(&self, struct_hash: u64) -> bool {
                [#(#hashes),*].contains(&struct_hash)
            }

            fn decode_check(
                &self,
                struct_hash: u64,
                bytes: &[u8],
            ) -> ::wavedb_core::Result<()> {
                match struct_hash {
                    #(#decode_arms)*
                    other => ::core::result::Result::Err(
                        ::wavedb_core::Error::UnknownStructHash(other),
                    ),
                }
            }
        }

        #storage_impl
        #collision_guard
    })
}
