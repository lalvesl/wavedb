//! Secondary-index codegen for `#[wavedb::pivot(...)]` declarations: the
//! `NonUniqueStruct` key hooks (`NUM_SECONDARIES` + `secondary_key`). The
//! typed `by_<field>` lookups, shared with the lists, are
//! [`orderings`](crate::orderings)'.
//!
//! Everything is static dispatch: `secondary_key` is a `match` over the
//! declared indexes.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{Ident, Type};

/// One resolved `#[wavedb::pivot(...)]`: the declared fields with their types.
pub struct ResolvedPivot {
    pub fields: Vec<(Ident, Type)>,
}

impl ResolvedPivot {
    /// The `IndexKey` expression encoding `self`'s field values — a direct
    /// field borrow for a single index, a ref-tuple for a composite.
    pub fn self_key_expr(&self) -> TokenStream {
        let refs = self.fields.iter().map(|(f, _)| quote!(&self.#f));
        if self.fields.len() == 1 {
            let f = &self.fields[0].0;
            quote!(::wavedb_core::IndexKey::key_bytes(&self.#f))
        } else {
            quote!(::wavedb_core::IndexKey::key_bytes(&(#(#refs),*)))
        }
    }

    /// The generated enumeration's name: `listed_by_f1` / `listed_by_f1_f2`.
    pub fn listed_ident(&self) -> Ident {
        format_ident!("listed_by_{}", self.joined())
    }

    /// The declared fields' names joined with `_`.
    pub fn joined(&self) -> String {
        self.fields
            .iter()
            .map(|(f, _)| f.to_string())
            .collect::<Vec<_>>()
            .join("_")
    }
}

/// The items injected into the `NonUniqueStruct` impl: the index count and the
/// per-index key encoder.
pub fn trait_items(specs: &[ResolvedPivot]) -> TokenStream {
    if specs.is_empty() {
        return TokenStream::new();
    }
    let n = specs.len();
    let arms = specs.iter().enumerate().map(|(i, spec)| {
        let expr = spec.self_key_expr();
        quote!(#i => #expr,)
    });
    quote! {
        const NUM_SECONDARIES: usize = #n;

        fn secondary_key(&self, index: usize) -> ::std::vec::Vec<u8> {
            match index {
                #(#arms)*
                _ => ::std::vec::Vec::new(),
            }
        }
    }
}
