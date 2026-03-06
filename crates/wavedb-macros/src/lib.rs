//! WaveDB compile-time front door: `#[wavedb]` and `#[derive(WaveWire)]`.
//!
//! - **`#[derive(WaveWire)]`** emits a `WaveWire` impl for a
//!   struct (no serde, no `repr(C)`).
//! - **`#[wavedb]`** turns a struct into a WaveDB object: it computes the
//!   `STRUCT_HASH`, emits the `WaveWire` impl and the
//!   `WaveDbStruct` impl, and — for
//!   `NonUnique` shapes — generates the `PivotId` / `Pivot` collection types.
//!
//! `#[server]` and the build-time registry scanner are staged separately. See
//! `crates/wavedb-macros/README.md` for the full target surface.

use proc_macro::TokenStream;

mod arg_specs;
mod args;
mod declarations;
mod exec_ops;
mod expose;
mod expose_arms;
mod expose_collision;
mod expose_parse;
mod fuzzy;
mod generated;
mod lists;
mod natural_key;
mod orderings;
mod secondaries;
mod server;
mod server_args;
mod server_stream;
mod storage_statics;
mod struct_hash;
mod wavedb_attr;
mod wire_derive;

/// Derive `WaveWire` for a struct or enum, and its borrowed twin `NameRef`
/// (RFC 0076).
///
/// Field stack slots encode inline in declaration order; heap payloads append
/// depth-first. See [`docs/wire_format.md`](../../../docs/wire_format.md).
#[proc_macro_derive(WaveWire)]
pub fn wave_wire(input: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    wire_derive::derive(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Turn a struct into a WaveDB object.
///
/// ```ignore
/// #[wavedb]                 // Unique: one live record per tenant
/// pub struct AboutUser { pub name: String }
///
/// #[wavedb(NonUnique)]      // many per tenant, reached through a Pivot
/// #[wavedb::pivot(amount)]  // + a secondary index on `amount`
/// pub struct Order { pub amount: u64 }
/// ```
#[proc_macro_attribute]
pub fn wavedb(attr: TokenStream, item: TokenStream) -> TokenStream {
    wavedb_attr::expand(attr.into(), item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declare the node-side registry: exactly the listed items become
/// wire-reachable, dispatched by a per-`STRUCT_HASH` `match` (emits
/// `REGISTRY`). Entries take per-op exclusions and overrides:
///
/// ```ignore
/// expose_server! {
///     AboutUser,                                   // full generated op set
///     Order { save: audited_save, remove: never }, // harden / exclude ops
/// }
/// ```
///
/// The whole emission rides the schema crate's **`server-side`** feature
/// (declare it in `[features]`): a client build that disables it compiles
/// none of this — see the `#[server]` docs for the side-feature contract.
#[proc_macro]
pub fn expose_server(input: TokenStream) -> TokenStream {
    expose::expand_server(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declare the client-side allowlist: which items this binary's typed stubs
/// may route (emits `CLIENT_REGISTRY`). No per-op overrides — those shape
/// the server surface.
///
/// The whole emission rides the schema crate's **`client-side`** feature
/// (declare it in `[features]`) — the mirror of `expose_server!`'s
/// `server-side` gate.
#[proc_macro]
pub fn expose_client(input: TokenStream) -> TokenStream {
    expose::expand_client(input.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Turn an `async fn` into a server-only body plus a client call binding.
///
/// ```ignore
/// #[server]                         // login-required (default; guard is M8)
/// async fn pinned(db: &Db) -> Result<Vec<Note>> { /* runs on the node */ }
///
/// #[server(public)]                 // reachable before login
/// async fn login(db: &Db, user: U48, pw: String) -> Result<Tokens> { /* … */ }
/// ```
///
/// The body's `db` is retyped to the node-side `ServerDb`; the client sees a
/// stub with the same signature that ships `WaveWire`-encoded args. List the
/// function in `expose_server!` (as `fn name`) to make it reachable.
///
/// **Side features (the leak guarantee).** The schema crate must declare
/// two cargo features with these exact names, and the expansion gates on
/// them: the **body + dispatch** compile only under `server-side`, the
/// **client stub** only under `client-side` (the fn-type and its
/// `STRUCT_HASH` exist under both — identity is the protocol). A deployed
/// client depends on the schema with `default-features = false,
/// features = ["client-side"]`, so server logic is never *compiled* into
/// its artifact — a stronger guarantee than LTO/dead-code stripping.
/// Server-only helpers written outside `#[server]` bodies must carry
/// `#[cfg(feature = "server-side")]` by hand.
#[proc_macro_attribute]
pub fn server(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attr = proc_macro2::TokenStream::from(attr);
    server::expand(&attr, item.into())
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
