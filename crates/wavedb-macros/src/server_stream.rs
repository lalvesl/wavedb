//! The stream-returning `#[server]` expansion
//! (`-> impl Stream<Item = Result<T>>`) — split from [`crate::server`] for
//! the file budget: the node runs the body's stream and ships one item frame
//! per element; the client stub re-exposes the same async iterator, decoding
//! frames as they arrive.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{ItemFn, Type};

use crate::server::{Arg, decode_and_forward, encode_payload, guard};

/// Emit the three items for a stream-returning fn (body, fn-type + dispatch,
/// client stub). `hash` is the composed-identity `const` expression;
/// `public` opens the fn to the unauthenticated tier.
#[allow(clippy::too_many_arguments)]
pub fn expand(
    func: &ItemFn,
    name: &syn::Ident,
    vis: &syn::Visibility,
    db_pat: &syn::Pat,
    args: &[Arg],
    item_ty: &Type,
    hash: &TokenStream,
    public: bool,
) -> TokenStream {
    let output = &func.sig.output;
    let body = &func.block;
    let auth_guard = guard(public);
    let body_fn = format_ident!("__{}_body", name);
    let (decode, forward) = decode_and_forward(args);
    let payload = encode_payload(args);
    let arg_sig: Vec<TokenStream> = args
        .iter()
        .map(|a| {
            let (p, t) = (&a.pat, &a.ty);
            quote!(#p: #t)
        })
        .collect();

    quote! {
        // The server body: not async — it *returns* the stream, borrowing
        // the node context like any collection walk. Compiled only under
        // the schema crate's `server-side` feature (see `crate::server`).
        #[cfg(feature = "server-side")]
        #[allow(clippy::future_not_send, non_snake_case)]
        fn #body_fn<R: ::wavedb_core::Route>(
            #db_pat: &::wavedb::ServerDb<'_, R>,
            #(#arg_sig),*
        ) #output {
            #[allow(unused_imports)]
            use ::wavedb_core::DbHandle as _;
            #body
        }

        // The fn-type: the identity both sides share.
        #[allow(non_camel_case_types)]
        #vis struct #name {}

        impl #name {
            /// This function's composed identity, in the struct hash space.
            pub const STRUCT_HASH: u64 = #hash;
        }

        // The node dispatch step — server side only, like the body it runs.
        #[cfg(feature = "server-side")]
        impl #name {
            /// Decode args, run the body's stream, ship the items. Buffered
            /// node-side for now (a later internal change); the wire and the
            /// client already stream frame-by-frame.
            #[doc(hidden)]
            #[allow(clippy::future_not_send)]
            pub async fn __wavedb_dispatch<R: ::wavedb_core::Route>(
                route: &R,
                caller: ::wavedb_core::Caller,
                payload: &[u8],
            ) -> ::wavedb_core::Result<::wavedb_core::expose::Reply> {
                use ::wavedb_core::TryStreamExt as _;
                #auth_guard
                #decode
                let db = ::wavedb::ServerDb::for_caller(route, caller);
                let items = #body_fn(&db, #(#forward),*);
                let collected: ::std::vec::Vec<#item_ty> =
                    match items.try_collect().await {
                        ::core::result::Result::Ok(v) => v,
                        ::core::result::Result::Err(error) => {
                            return ::core::result::Result::Err(
                                ::core::convert::Into::into(error),
                            );
                        }
                    };
                let entries = collected
                    .iter()
                    .map(::wavedb_core::wire::to_wire)
                    .collect();
                ::core::result::Result::Ok(
                    ::wavedb_core::expose::Reply::Values(entries),
                )
            }
        }

        // The client stub: the same async-iterator signature, decoding item
        // frames as the node writes them.
        #vis fn #name(
            #db_pat: &::wavedb::Db,
            #(#arg_sig),*
        ) #output {
            #db_pat.call_fn_stream::<#item_ty>(#name::STRUCT_HASH, #payload)
        }
    }
}
