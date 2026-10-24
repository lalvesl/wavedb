//! A `#[server]` function's arguments and node context, shared by the
//! scalar ([`crate::server`]) and stream ([`crate::server_stream`]) shapes:
//! how the arguments ride the wire, and what a body runs against.

use proc_macro2::TokenStream;
use quote::quote;
use syn::Type;

/// One argument: its binding pattern and its type.
pub struct Arg {
    pub pat: Box<syn::Pat>,
    pub ty: Box<Type>,
}

/// `(let-bindings decoding `payload` into the args, the arg idents to pass on)`.
pub fn decode_and_forward(args: &[Arg]) -> (TokenStream, Vec<&syn::Pat>) {
    let pats: Vec<&syn::Pat> = args.iter().map(|a| a.pat.as_ref()).collect();
    let decode = match args.len() {
        0 => quote!(let _ = payload;),
        1 => {
            let (p, t) = (&args[0].pat, &args[0].ty);
            quote! {
                let #p: #t = ::wavedb_core::wire::from_wire(payload)
                    .map_err(::wavedb_core::Error::from)?;
            }
        }
        _ => {
            let types = args.iter().map(|a| &a.ty);
            quote! {
                let ( #(#pats),* ): ( #(#types),* ) =
                    ::wavedb_core::wire::from_wire(payload)
                        .map_err(::wavedb_core::Error::from)?;
            }
        }
    };
    (decode, pats)
}

/// The client stub's payload expression (args → wire bytes).
pub fn encode_payload(args: &[Arg]) -> TokenStream {
    let pats = args.iter().map(|a| a.pat.as_ref());
    match args.len() {
        0 => quote!(::std::vec::Vec::new()),
        1 => {
            let p = args[0].pat.as_ref();
            quote!(::wavedb_core::wire::to_wire(&#p))
        }
        _ => quote!(::wavedb_core::wire::to_wire(&( #(#pats),* ))),
    }
}

/// The body's node context, bound to `db`: the verified caller over the
/// route, and the owners this invocation holds — one table per call, so a
/// lock never outlives the body that took it (RFC 0067 §5).
pub fn node_context() -> TokenStream {
    quote! {
        let __wavedb_leases = ::wavedb_core::Leases::new();
        let db = ::wavedb::ServerDb::for_caller(
            route,
            caller,
            &__wavedb_leases,
        );
    }
}
