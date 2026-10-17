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
/// overridden) steps; a function runs its uniform dispatch.
pub fn execute_arm(entry: &Entry) -> TokenStream {
    let path = &entry.path;
    let h = hash_expr(entry);
    if entry.kind == Kind::Fn {
        return quote! {
            h if h == #h => {
                #path::__wavedb_dispatch(store, caller, command, payload)
                    .await
            }
        };
    }
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
