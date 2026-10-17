//! The per-item **execution steps** `#[wavedb]` emits: one
//! `__wavedb_<command>` fn per wire command, uniform across shapes so the
//! exposure macros can reference them without knowing a path's shape.
//!
//! Each fn has the exposure-op signature
//! `async fn(&S, U48, &[u8]) -> Result<Reply>` — the same signature a
//! `expose_server!` per-op override must have. A command the shape doesn't
//! support refuses with `UnknownStructHash` (indistinguishable from an
//! unlisted type, on purpose). Defined here, **reachable only when listed**
//! in an exposure declaration.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Ident;

/// One refusal body — the uniform "this does not exist" answer.
fn refuse(name: &Ident) -> TokenStream {
    quote! {
        ::core::result::Result::Err(
            ::wavedb_core::Error::UnknownStructHash(
                <#name as ::wavedb_core::WaveDbStruct>::STRUCT_HASH,
            ),
        )
    }
}

/// The op-fn skeleton: `#[doc(hidden)] pub async fn __wavedb_<op>(…)`.
///
/// Every struct command opens with two identity guards, in order:
///
/// 1. **Tier** — the unauthenticated caller (`user == U48::MAX`) may only
///    reach `#[server(public)]` functions; direct object ops refuse
///    uniformly, whatever the command.
/// 2. **One user per tenant** — a verified caller whose `user` is not its
///    `tenant` refuses as
///    [`IdentityMismatch`](::wavedb_core::Error::IdentityMismatch). The
///    engine isolates by tenant and stamps `user` as provenance only, so a
///    token whose halves disagree asks an intra-tenant authorization
///    question nothing answers yet.
///
/// Both bind the **client** path. `#[server]` bodies reach the engine
/// through `ServerDb`, never through these fns, so server-side code keeps
/// acting as any identity it likes (`as_tenant` — the node is the authority,
/// not a principal being checked).
fn op_fn(op: &str, body: &TokenStream) -> TokenStream {
    let ident = quote::format_ident!("__wavedb_{}", op);
    quote! {
        #[doc(hidden)]
        #[allow(clippy::future_not_send)]
        pub async fn #ident<S: ::wavedb_core::Store>(
            store: &S,
            caller: ::wavedb_core::Caller,
            payload: &[u8],
        ) -> ::wavedb_core::Result<::wavedb_core::expose::Reply> {
            if caller.is_anonymous() {
                return ::core::result::Result::Err(
                    ::wavedb_core::Error::Unauthorized(
                        ::std::string::String::from("login required"),
                    ),
                );
            }
            if caller.user.get() != caller.tenant.get() {
                return ::core::result::Result::Err(
                    ::wavedb_core::Error::IdentityMismatch(
                        caller.user,
                        caller.tenant,
                    ),
                );
            }
            #body
        }
    }
}

/// The node-operation step (`__wavedb_node`): the trusted path a
/// `#[server]` body's operations take, typed back from the `STRUCT_HASH` by
/// the registry's match. No client gate — node code is the authority — and
/// emitted for every type, so `store` entries are served too.
fn node_step(run: &TokenStream) -> TokenStream {
    quote! {
        #[doc(hidden)]
        #[allow(clippy::future_not_send)]
        pub async fn __wavedb_node<S: ::wavedb_core::Store>(
            store: &S,
            tenant: ::wavedb_core::U48,
            op: ::wavedb_core::NodeOp,
        ) -> ::wavedb_core::Result<::wavedb_core::NodeReply> {
            #run::<Self, S>(store, tenant, op).await
        }
    }
}

/// The `Unique` shape's execution steps: `get` / `save` real, the NonUnique
/// commands refuse.
pub fn unique_ops(name: &Ident) -> TokenStream {
    let refuse = refuse(name);
    let get = op_fn(
        "get",
        &quote! {
            let _ = payload;
            let anchor = ::wavedb_core::Id::new(
                <#name as ::wavedb_core::WaveDbStruct>::STRUCT_HASH,
                caller.tenant,
                true,
                0,
            );
            ::wavedb_core::expose::get_value::<#name, S>(store, anchor).await
        },
    );
    let save = op_fn(
        "save",
        &quote! {
            let value: #name = ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::collection::save_unique_as(store, caller.tenant, caller.user, &value)
                .await?;
            ::core::result::Result::Ok(::wavedb_core::expose::Reply::Done)
        },
    );
    let insert =
        op_fn("insert", &quote!(let _ = (store, caller, payload); #refuse));
    let update =
        op_fn("update", &quote!(let _ = (store, caller, payload); #refuse));
    let remove =
        op_fn("remove", &quote!(let _ = (store, caller, payload); #refuse));
    let all = op_fn("all", &quote!(let _ = (store, caller, payload); #refuse));
    let history = op_fn(
        "history",
        &quote! {
            let _ = payload;
            ::wavedb_core::expose::unique_history_values::<#name, S>(
                store, caller.tenant,
            )
            .await
        },
    );
    let changes = op_fn(
        "changes",
        &quote! {
            // The pivot half of the payload is a collection's address — a
            // Unique topic carries none and ignores one.
            let (_pivot, since): (
                ::core::option::Option<::wavedb_core::LocalId>,
                ::core::option::Option<u64>,
            ) = ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::unique_changes::<#name, S>(
                store, caller.tenant, since,
            )
            .await
        },
    );
    let listed =
        op_fn("listed", &quote!(let _ = (store, caller, payload); #refuse));
    let list_len = op_fn(
        "list_len",
        &quote!(let _ = (store, caller, payload); #refuse),
    );
    let node = node_step(&quote!(::wavedb_core::node_op::run_unique));
    quote! {
        impl #name {
            #get
            #save
            #node
            #insert
            #update
            #remove
            #all
            #history
            #changes
            #listed
            #list_len
        }
    }
}

/// The NonUnique declared-list steps (`Listed` / `ListLen`): a page of one
/// `#[wavedb::list]` in its declared order, and its length.
///
/// The index rides as a `u32` and the offset as a `u64` because `usize` is
/// never encodable on the wire — the steps widen back at the seam.
fn nonunique_list_steps(name: &Ident) -> (TokenStream, TokenStream) {
    let listed = op_fn(
        "listed",
        &quote! {
            let (pivot, index, offset, limit): (
                ::wavedb_core::LocalId, u32, u64, u32,
            ) = ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::listed_values::<#name, S>(
                store, pivot, caller.tenant, index as usize, offset, limit,
            )
            .await
        },
    );
    let list_len = op_fn(
        "list_len",
        &quote! {
            let (pivot, index): (::wavedb_core::LocalId, u32) =
                ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::list_len_value::<#name, S>(
                store, pivot, caller.tenant, index as usize,
            )
            .await
        },
    );
    (listed, list_len)
}

/// The NonUnique `changes` step: catch-up navigation over the collection's
/// recency/dead logs, addressed by the payload's pivot (a pivot-less topic
/// on a NonUnique type does not exist).
fn nonunique_changes_step(name: &Ident) -> TokenStream {
    let refuse = refuse(name);
    op_fn(
        "changes",
        &quote! {
            let (pivot, since): (
                ::core::option::Option<::wavedb_core::LocalId>,
                ::core::option::Option<u64>,
            ) = ::wavedb_core::wire::from_wire(payload)?;
            let ::core::option::Option::Some(pivot) = pivot else {
                return #refuse;
            };
            ::wavedb_core::expose::collection_changes::<#name, S>(
                store, pivot, caller.tenant, since,
            )
            .await
        },
    )
}

/// The `NonUnique` shape's execution steps: the collection ops real, `save`
/// refuses (NonUnique updates ride `Update`). Every command names its
/// collection's Pivot first, and `get`/`update`/`remove` act only on a record
/// that belongs to it — the command runs where that Pivot lives and under its
/// lock, so it may not reach into another collection.
///
/// The steps drive the engine-facing `Collection` directly (`Store` in hand)
/// — never the generated `DbHandle`-based wrappers, so the wrappers' shape
/// can evolve without touching the wire ops.
pub fn nonunique_ops(name: &Ident) -> TokenStream {
    let refuse = refuse(name);
    let get = op_fn(
        "get",
        &quote! {
            let _ = caller;
            let (pivot, id): (::wavedb_core::LocalId, ::wavedb_core::Id) =
                ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::member_value::<#name, S>(store, pivot, id)
                .await
        },
    );
    let insert = op_fn(
        "insert",
        &quote! {
            let (pivot, value): (::wavedb_core::LocalId, #name) =
                ::wavedb_core::wire::from_wire(payload)?;
            let col = ::wavedb_core::Collection::<#name>::at(pivot, caller.tenant)
                .stamped_by(caller.user);
            let id = col.insert(store, &value).await?;
            ::core::result::Result::Ok(
                ::wavedb_core::expose::Reply::Inserted(id),
            )
        },
    );
    let update = op_fn(
        "update",
        &quote! {
            let (pivot, id, value): (::wavedb_core::LocalId, ::wavedb_core::Id, #name) =
                ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::check_member::<#name, S>(store, pivot, id)
                .await?;
            let col = ::wavedb_core::Collection::<#name>::at(pivot, caller.tenant)
                .stamped_by(caller.user);
            col.save(store, id, &value).await?;
            ::core::result::Result::Ok(::wavedb_core::expose::Reply::Done)
        },
    );
    let remove = op_fn(
        "remove",
        &quote! {
            let (pivot, id): (::wavedb_core::LocalId, ::wavedb_core::Id) =
                ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::check_member::<#name, S>(store, pivot, id)
                .await?;
            let col = ::wavedb_core::Collection::<#name>::at(pivot, caller.tenant)
                .stamped_by(caller.user);
            let removed = col.remove(store, id).await?;
            ::core::result::Result::Ok(
                ::wavedb_core::expose::Reply::Removed(removed),
            )
        },
    );
    let save =
        op_fn("save", &quote!(let _ = (store, caller, payload); #refuse));
    let all = op_fn(
        "all",
        &quote! {
            let pivot: ::wavedb_core::LocalId =
                ::wavedb_core::wire::from_wire(payload)?;
            ::wavedb_core::expose::all_values::<#name, S>(store, pivot, caller.tenant)
                .await
        },
    );
    let history = op_fn(
        "history",
        &quote!(let _ = (store, caller, payload); #refuse),
    );
    let changes = nonunique_changes_step(name);
    let (listed, list_len) = nonunique_list_steps(name);
    let node = node_step(&quote!(::wavedb_core::node_op::run_nonunique));
    quote! {
        impl #name {
            #get
            #insert
            #node
            #update
            #remove
            #save
            #all
            #history
            #changes
            #listed
            #list_len
        }
    }
}
