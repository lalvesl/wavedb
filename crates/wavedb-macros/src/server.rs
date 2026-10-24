//! `#[server]` — a server-only async function with a client call binding.
//!
//! From one `async fn name(db: &Db, args…) -> Result<Ret>` the macro emits
//! three coexisting items:
//!
//! - a **fn-type** `struct name {}` (type namespace) carrying the function's
//!   `STRUCT_HASH` and a `__wavedb_dispatch` step — how the node registry
//!   routes an incoming call;
//! - the **server body** as a generic `__name_body<R: Route>(db: &ServerDb…)`
//!   — the user's body with `db` retyped to the node-side context, whose
//!   operations each run at their owner through the route (RFC 0067);
//! - the **client stub** `fn name(db: &Db, args…)` (value namespace) that
//!   ships the args over the wire and decodes the return.
//!
//! Type and value namespaces let `struct name` and `fn name` share the name.
//! The function's identity is a `STRUCT_HASH` composed from its signature, in
//! the same hash space as structs — the registry `match` disambiguates.
//!
//! **Side gating.** A schema crate compiles into the node AND every client,
//! but the three items must not: body + dispatch are emitted under
//! `#[cfg(feature = "server-side")]`, the stub under
//! `#[cfg(feature = "client-side")]` — the schema declares both features, a
//! deployment enables only its side. Stronger than LTO/DCE: with
//! `server-side` off the body is **never compiled**, so no string or code
//! path of it can survive into a client artifact. Only the fn-type and its
//! `STRUCT_HASH` are unconditional — identity is the protocol.
//!
//! **Auth (M8).** The login guard is live: a plain `#[server]` fn refuses
//! the unauthenticated tier (`caller.user == U48::MAX`) before decoding its
//! payload; `#[server(public)]` opens the fn to anonymous callers (login /
//! register / refresh). The dispatch step receives gate 1's verified
//! `Caller` and builds the node context with `ServerDb::for_caller`.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, GenericArgument, ItemFn, PathArguments, Type, parse2};

use crate::server_args::{
    Arg, decode_and_forward, encode_payload, node_context,
};
use crate::struct_hash;

/// The `Ok` type of a `Result<Ok, …>` return; the whole type otherwise.
fn ok_type(output: &syn::ReturnType) -> Type {
    let syn::ReturnType::Type(_, ty) = output else {
        return parse2(quote!(())).expect("unit parses");
    };
    result_ok_arg(ty).unwrap_or_else(|| (**ty).clone())
}

/// The item type `T` of a stream-shaped return
/// `impl Stream<Item = Result<T>>`, if the return is one. A stream-returning
/// function ships its items one frame at a time; anything else is a scalar
/// wired as one `Returned`.
fn stream_item_type(output: &syn::ReturnType) -> Option<Type> {
    let syn::ReturnType::Type(_, ty) = output else {
        return None;
    };
    let Type::ImplTrait(imp) = &**ty else {
        return None;
    };
    for bound in &imp.bounds {
        let syn::TypeParamBound::Trait(t) = bound else {
            continue;
        };
        let seg = t.path.segments.last()?;
        if seg.ident != "Stream" {
            continue;
        }
        let PathArguments::AngleBracketed(a) = &seg.arguments else {
            continue;
        };
        for arg in &a.args {
            if let GenericArgument::AssocType(assoc) = arg
                && assoc.ident == "Item"
            {
                return result_ok_arg(&assoc.ty);
            }
        }
    }
    None
}

/// The first generic argument of a `Result<…>` type, if `ty` is one.
fn result_ok_arg(ty: &Type) -> Option<Type> {
    let Type::Path(p) = ty else { return None };
    let seg = p.path.segments.last()?;
    if seg.ident != "Result" {
        return None;
    }
    let PathArguments::AngleBracketed(a) = &seg.arguments else {
        return None;
    };
    match a.args.first()? {
        GenericArgument::Type(ok) => Some(ok.clone()),
        _ => None,
    }
}

/// Split a `#[server]` fn's inputs into the `db` receiver and the args.
fn split_inputs(f: &ItemFn) -> syn::Result<(Box<syn::Pat>, Vec<Arg>)> {
    let mut inputs = f.sig.inputs.iter();
    let first = inputs.next().ok_or_else(|| {
        syn::Error::new_spanned(
            &f.sig,
            "a #[server] fn takes `db: &Db` as its first parameter",
        )
    })?;
    let FnArg::Typed(db) = first else {
        return Err(syn::Error::new_spanned(
            first,
            "a #[server] fn is a free function, not a method",
        ));
    };
    let db_pat = db.pat.clone();
    let mut args = Vec::new();
    for input in inputs {
        let FnArg::Typed(a) = input else {
            return Err(syn::Error::new_spanned(input, "unexpected receiver"));
        };
        args.push(Arg {
            pat: a.pat.clone(),
            ty: a.ty.clone(),
        });
    }
    Ok((db_pat, args))
}

/// The identity `const` expression:
/// `compose(name-seed, [arg tags…, return tag])` — an argument object's
/// `STRUCT_HASH` folds in, so a schema change to it transitively renames the
/// function. The name seed rides the reserved `fn` discriminator so a
/// function can't collide with a struct of the same name; a stream return
/// tags under its own kind (a scalar and a stream of the same item are
/// different functions).
fn composed_identity(
    name: &syn::Ident,
    args: &[Arg],
    stream_item: Option<&Type>,
    ret: &Type,
) -> TokenStream {
    let name_seed = struct_hash::compute(&name.to_string(), "fn", &[]);
    let arg_tags: Vec<TokenStream> = args
        .iter()
        .map(|a| {
            let t = &a.ty;
            quote!(<#t as ::wavedb_core::FnArgTag>::TAG)
        })
        .collect();
    let ret_tag = stream_item.map_or_else(
        || quote!(<#ret as ::wavedb_core::FnArgTag>::TAG),
        |item| {
            quote! {
                ::wavedb_core::fn_identity::container(
                    ::wavedb_core::fn_identity::STREAM_KIND,
                    <#item as ::wavedb_core::FnArgTag>::TAG,
                )
            }
        },
    );
    quote! {
        ::wavedb_core::fn_identity::compose(
            #name_seed,
            &[#(#arg_tags,)* #ret_tag],
        )
    }
}

/// Is the attribute the `public` marker? (`#[server(public)]` — the
/// unauthenticated tier may call it; everything else is login-required.)
fn is_public(attr: &TokenStream) -> syn::Result<bool> {
    if attr.is_empty() {
        return Ok(false);
    }
    let ident: syn::Ident = parse2(attr.clone())?;
    if ident == "public" {
        Ok(true)
    } else {
        Err(syn::Error::new_spanned(
            ident,
            "the only #[server] argument is `public`",
        ))
    }
}

/// The login guard injected ahead of arg decoding: a non-`public` function
/// refuses the anonymous tier (`user == U48::MAX`) before touching the
/// payload.
pub fn guard(public: bool) -> TokenStream {
    if public {
        return TokenStream::new();
    }
    quote! {
        if caller.is_anonymous() {
            return ::core::result::Result::Err(
                ::wavedb_core::Error::Unauthorized(
                    ::std::string::String::from("login required"),
                ),
            );
        }
    }
}

/// Expand `#[server]` / `#[server(public)]`.
pub fn expand(
    attr: &TokenStream,
    item: TokenStream,
) -> syn::Result<TokenStream> {
    let public = is_public(attr)?;
    let func: ItemFn = parse2(item)?;
    let name = func.sig.ident.clone();
    let vis = func.vis.clone();
    let (db_pat, args) = split_inputs(&func)?;
    let stream_item = stream_item_type(&func.sig.output);
    let ret = ok_type(&func.sig.output);

    let hash = composed_identity(&name, &args, stream_item.as_ref(), &ret);

    if let Some(item_ty) = stream_item {
        return Ok(crate::server_stream::expand(
            &func, &name, &vis, &db_pat, &args, &item_ty, &hash, public,
        ));
    }
    let auth_guard = guard(public);

    let output = func.sig.output.clone();
    let body = func.block;
    let body_fn = format_ident!("__{}_body", name);
    let (decode, forward) = decode_and_forward(&args);
    let context = node_context();
    let payload = encode_payload(&args);
    let arg_sig: Vec<TokenStream> = args
        .iter()
        .map(|a| {
            let (p, t) = (&a.pat, &a.ty);
            quote!(#p: #t)
        })
        .collect();

    Ok(quote! {
        // The server body: the user's block, `db` retyped to the node
        // context. `DbHandle` is imported so the body may use the trait
        // spellings (`db.as_tenant(..)`, `db.tenant()`) alongside the
        // generated inherent ones (`T::get(db)`). Compiled only under the
        // schema crate's `server-side` feature — a client build never even
        // parses these tokens into code.
        #[cfg(feature = "server-side")]
        #[allow(clippy::future_not_send, non_snake_case)]
        async fn #body_fn<R: ::wavedb_core::Route>(
            #db_pat: &::wavedb::ServerDb<'_, R>,
            #(#arg_sig),*
        ) #output {
            #[allow(unused_imports)]
            use ::wavedb_core::DbHandle as _;
            #body
        }

        // The fn-type: the identity both sides share (the schema IS the
        // protocol — a stub must name the same hash the node serves).
        #[allow(non_camel_case_types)]
        #vis struct #name {}

        impl #name {
            /// This function's composed identity, in the struct hash space.
            pub const STRUCT_HASH: u64 = #hash;
        }

        // The node dispatch step — server side only, like the body it runs.
        #[cfg(feature = "server-side")]
        impl #name {
            /// Decode args, run the body against a node context, wire the
            /// return. Its operations run through `route`, each at its owner.
            #[doc(hidden)]
            #[allow(clippy::future_not_send)]
            pub async fn __wavedb_dispatch<R: ::wavedb_core::Route>(
                route: &R,
                caller: ::wavedb_core::Caller,
                payload: &[u8],
            ) -> ::wavedb_core::Result<::wavedb_core::expose::Reply> {
                #auth_guard
                #decode
                #context
                match #body_fn(&db, #(#forward),*).await {
                    ::core::result::Result::Ok(value) => {
                        ::core::result::Result::Ok(
                            ::wavedb_core::expose::Reply::Returned(
                                ::wavedb_core::wire::to_wire(&value),
                            ),
                        )
                    }
                    ::core::result::Result::Err(error) => {
                        ::core::result::Result::Err(
                            ::core::convert::Into::into(error),
                        )
                    }
                }
            }
        }

        // The client stub: ship the args, decode the return.
        #[cfg(feature = "client-side")]
        #[allow(clippy::future_not_send)]
        #vis async fn #name(
            #db_pat: &::wavedb::Db,
            #(#arg_sig),*
        ) #output {
            #db_pat.call_fn::<#ret>(#name::STRUCT_HASH, #payload).await
        }
    })
}
