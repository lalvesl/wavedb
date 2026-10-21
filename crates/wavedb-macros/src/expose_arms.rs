//! The per-entry `match` arms the exposure macros assemble
//! ([`crate::expose`]): one function per operation of the [`Exposure`]
//! contract, each turning one declared entry into one arm.
//!
//! [`Exposure`]: wavedb_core::expose::Exposure

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::expose_parse::{Entry, Kind, OPS};

/// The call expression for one entry's one op: the generated step, the
/// override path, or the `never` refusal — resolved at expansion time.
fn op_call(entry: &Entry, op: &str) -> TokenStream {
    let path = &entry.path;
    match entry.overrides.iter().find(|(o, _)| o == op) {
        Some((_, Some(target))) => {
            quote!(#target(store, caller, payload).await)
        }
        Some((_, None)) => quote! {
            ::core::result::Result::Err(
                ::wavedb_core::Error::UnknownStructHash(
                    <#path as ::wavedb_core::WaveDbStruct>::STRUCT_HASH,
                ),
            )
        },
        None => {
            let step = format_ident!("__wavedb_{}", op);
            quote!(#path::#step(store, caller, payload).await)
        }
    }
}

/// The `STRUCT_HASH` expression for an entry (structs read it off
/// `WaveDbStruct`, `#[server]` fn-types carry it inherently).
pub fn hash_expr(entry: &Entry) -> TokenStream {
    let path = &entry.path;
    if entry.kind == Kind::Fn {
        quote!(#path::STRUCT_HASH)
    } else {
        quote!(<#path as ::wavedb_core::WaveDbStruct>::STRUCT_HASH)
    }
}

/// One `decode_check` arm: a struct body must parse as the declared type; a
/// function payload is an args tuple, so the header gate only proves the
/// hash is served.
pub fn decode_arm(entry: &Entry) -> TokenStream {
    let path = &entry.path;
    let h = hash_expr(entry);
    if entry.kind == Kind::Fn {
        quote!(h if h == #h => ::core::result::Result::Ok(()),)
    } else {
        quote! {
            h if h == #h => {
                ::wavedb_core::expose::decode_check::<#path>(bytes)
            }
        }
    }
}

/// One `addressing` arm: a function owns nothing; a struct is a Unique or
/// a collection by its shape.
pub fn addressing_arm(entry: &Entry) -> TokenStream {
    let path = &entry.path;
    let h = hash_expr(entry);
    let a = quote!(::wavedb_core::expose::Addressing);
    let addressing = if entry.kind == Kind::Fn {
        quote!(#a::Function)
    } else {
        quote! {
            if <#path as ::wavedb_core::WaveDbStruct>::SHAPE.is_unique() {
                #a::Unique
            } else {
                #a::Collection
            }
        }
    };
    quote!(h if h == #h => ::core::option::Option::Some(#addressing),)
}

/// One `execute` arm: a struct dispatches `match command` over its (possibly
/// overridden) steps. Functions have none — they run through `call_fn`, at
/// the edge, so a shard's future never embeds a server body.
pub fn execute_arm(entry: &Entry) -> TokenStream {
    let h = hash_expr(entry);
    let ops = OPS.iter().map(|op| {
        let variant = format_ident!("{}", crate::expose_parse::variant(op));
        let call = op_call(entry, op);
        quote!(::wavedb_core::expose::Command::#variant => #call,)
    });
    quote! {
        h if h == #h => {
            match command {
                #(#ops)*
            }
        }
    }
}

/// One `run_node` arm: every declared struct, `store` entries included —
/// node code reaches what the wire cannot.
pub fn node_arm(path: &syn::Path) -> TokenStream {
    quote! {
        h if h == <#path as ::wavedb_core::WaveDbStruct>::STRUCT_HASH => {
            #path::__wavedb_node(store, tenant, op).await
        }
    }
}

/// One `call_fn` arm: a declared `#[server]` function, its body's operations
/// running through `route`.
pub fn fn_arm(entry: &Entry) -> TokenStream {
    let path = &entry.path;
    quote! {
        h if h == #path::STRUCT_HASH => {
            #path::__wavedb_dispatch(route, caller, payload).await
        }
    }
}

/// The server registry's node half: `run_node` over every declared struct
/// (`store` entries included) and `call_fn` over the declared functions.
pub fn node_methods(structs: &[&syn::Path], wire: &[&Entry]) -> TokenStream {
    let node_arms = structs.iter().copied().map(node_arm);
    let fn_arms = wire
        .iter()
        .copied()
        .filter(|e| e.kind == Kind::Fn)
        .map(fn_arm);
    quote! {
        // A registry with no arm of a kind has nothing to await; the method
        // keeps the trait's async shape all the same.
        #[allow(clippy::future_not_send, clippy::unused_async_trait_impl)]
        async fn run_node<S: ::wavedb_core::Store>(
            &self,
            store: &S,
            tenant: ::wavedb_core::U48,
            struct_hash: u64,
            op: ::wavedb_core::NodeOp,
        ) -> ::wavedb_core::Result<::wavedb_core::NodeReply> {
            match struct_hash {
                #(#node_arms)*
                other => ::core::result::Result::Err(
                    ::wavedb_core::Error::UnknownStructHash(other),
                ),
            }
        }

        #[allow(clippy::future_not_send, clippy::unused_async_trait_impl)]
        async fn call_fn<R: ::wavedb_core::Route>(
            &self,
            route: &R,
            caller: ::wavedb_core::Caller,
            struct_hash: u64,
            payload: &[u8],
        ) -> ::wavedb_core::Result<::wavedb_core::expose::Reply> {
            match struct_hash {
                #(#fn_arms)*
                other => ::core::result::Result::Err(
                    ::wavedb_core::Error::UnknownStructHash(other),
                ),
            }
        }
    }
}

/// The `execute` arms: the wire structs only (see [`execute_arm`]).
pub fn execute_arms(wire: &[&Entry]) -> Vec<TokenStream> {
    wire.iter()
        .copied()
        .filter(|e| e.kind != Kind::Fn)
        .map(execute_arm)
        .collect()
}
