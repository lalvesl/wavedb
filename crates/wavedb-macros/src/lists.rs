//! Declared-list codegen for `#[wavedb::list]` ([RFC 0051]): taking the
//! declarations off the struct, folding them into the identity, the
//! `NonUniqueStruct` key hooks (`NUM_LISTS` + `list_key`), and the typed
//! `listed_by_<fields>` enumeration surface.
//!
//! A list and a `#[wavedb::pivot(...)]` secondary are **one structure** — the
//! same chain over the same order, `ResolvedPivot` reused verbatim — and
//! differ only in what they store under it ([RFC 0075]): a secondary stores
//! anchors and pays one random read per row, a list stores whole records
//! inline and pays one read per segment. So a list answers the same
//! `by_<fields>` lookups ([`orderings`](crate::orderings)), and declaring both
//! on one ordering is refused. Swapping one for the other is a one-word
//! change, and a new type.
//!
//! [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
//! [RFC 0075]: ../../../rfcs/0075-secondary-indexes-as-chains.md

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{Attribute, Data, DeriveInput, Ident};

use crate::arg_specs::ListSpec;
use crate::args::Shape;
use crate::secondaries::ResolvedPivot;

/// One resolved `#[wavedb::list(...)]`: the ordering, plus its own segment
/// capacity when it declared one.
pub struct ResolvedList {
    /// The ordering itself — the same shape a secondary index takes, because
    /// the two impose the same order.
    pub order: ResolvedPivot,
    /// This list's `page = N`, or `None` to inherit the struct's.
    pub page: Option<usize>,
}

/// Take every `#[wavedb::list]` declaration off `input` — the field-level
/// spelling and the struct-level one — resolve them, and fold them into the
/// STRUCT_HASH.
///
/// **Declaration order is field-level first**, in field order, then the
/// struct-level attributes in the order written. The order is load-bearing: a
/// list is addressed by its index in the `Pivot`'s root array, so reordering
/// declarations reorders chains. That is exactly why the fold exists — the
/// order is part of the identity, and changing it yields a new type rather than
/// silently re-pointing live chains at each other's data.
pub fn take_and_fold(
    input: &mut DeriveInput,
    named: &syn::FieldsNamed,
    shape: Shape,
    hash_fields: &mut Vec<(String, String)>,
) -> syn::Result<Vec<ResolvedList>> {
    let mut lists: Vec<ResolvedList> = take_field_lists(input)?
        .into_iter()
        .map(|(ident, ty, page)| ResolvedList {
            order: ResolvedPivot {
                fields: vec![(ident, ty)],
            },
            page,
        })
        .collect();
    for spec in take_list_specs(&mut input.attrs)? {
        if spec.fields.is_empty() {
            return Err(syn::Error::new(
                Span::call_site(),
                "a struct-level #[wavedb::list(...)] must name the ordering's                  field(s) — the bare form belongs on the field it orders by",
            ));
        }
        lists.push(ResolvedList {
            order: ResolvedPivot {
                fields: crate::declarations::resolve_fields(
                    &spec.fields,
                    named,
                    "#[wavedb::list(...)]",
                )?,
            },
            page: spec.page,
        });
    }
    if !lists.is_empty() && shape != Shape::NonUnique {
        return Err(syn::Error::new(
            Span::call_site(),
            "#[wavedb::list] is only valid on a #[wavedb(NonUnique)] struct — \
             a Unique type has one record per tenant and so nothing to order",
        ));
    }
    for (i, list) in lists.iter().enumerate() {
        let joined = list
            .order
            .fields
            .iter()
            .map(|(f, _)| f.to_string())
            .collect::<Vec<_>>()
            .join(",");
        // The capacity rides the same entry: it lays the chain out, so it is a
        // fact about stored bytes exactly as the struct-level `page` is.
        let entry = match list.page {
            Some(n) => format!("{joined}@{n}"),
            None => joined,
        };
        hash_fields.push((format!("#list{i}"), entry));
    }
    Ok(lists)
}

/// Strip `#[wavedb::list]` from the struct's fields, returning each marked
/// field in declaration order with the capacity it declared.
///
/// The field spelling names no fields: the field it sits on *is* the ordering.
/// It may still carry `page = N`. A composite ordering has no single field to
/// sit on, which is what the struct-level spelling is for.
fn take_field_lists(
    input: &mut DeriveInput,
) -> syn::Result<Vec<(Ident, syn::Type, Option<usize>)>> {
    let Data::Struct(data) = &mut input.data else {
        return Ok(Vec::new());
    };
    let mut marked = Vec::new();
    for field in &mut data.fields {
        let mut kept = Vec::with_capacity(field.attrs.len());
        for attr in field.attrs.drain(..) {
            if !is_list_attr(&attr) {
                kept.push(attr);
                continue;
            }
            let spec = match &attr.meta {
                syn::Meta::Path(_) => ListSpec {
                    fields: Vec::new(),
                    page: None,
                },
                _ => attr.parse_args::<ListSpec>()?,
            };
            if !spec.fields.is_empty() {
                return Err(syn::Error::new_spanned(
                    &attr,
                    "a field's #[wavedb::list] names no fields — the field it \
                     marks is the ordering; declare a composite ordering as \
                     #[wavedb::list((f1, f2))] on the struct",
                ));
            }
            let ident = field.ident.clone().ok_or_else(|| {
                syn::Error::new_spanned(&attr, "unnamed field")
            })?;
            marked.push((ident, field.ty.clone(), spec.page));
        }
        field.attrs = kept;
    }
    Ok(marked)
}

/// Remove every struct-level `#[wavedb::list(...)]`, parsing each into the
/// fields it declares.
fn take_list_specs(attrs: &mut Vec<Attribute>) -> syn::Result<Vec<ListSpec>> {
    let mut specs = Vec::new();
    let mut kept = Vec::with_capacity(attrs.len());
    for attr in attrs.drain(..) {
        if is_list_attr(&attr) {
            specs.push(attr.parse_args::<ListSpec>()?);
        } else {
            kept.push(attr);
        }
    }
    *attrs = kept;
    Ok(specs)
}

/// `true` for a `#[wavedb::list]` helper attribute, either spelling.
fn is_list_attr(attr: &Attribute) -> bool {
    let segs = &attr.path().segments;
    segs.len() == 2 && segs[0].ident == "wavedb" && segs[1].ident == "list"
}

/// The items injected into the `NonUniqueStruct` impl: the list count, the
/// per-list sort-key encoder, and the per-list segment capacity.
pub fn trait_items(lists: &[ResolvedList]) -> TokenStream {
    if lists.is_empty() {
        return TokenStream::new();
    }
    let n = lists.len();
    let key_arms = lists.iter().enumerate().map(|(i, list)| {
        let expr = list.order.self_key_expr();
        quote!(#i => #expr,)
    });
    // Only the lists that declared one need an arm; the rest fall through to
    // the struct's own `page`, which is the trait default's job.
    let page_arms = lists
        .iter()
        .enumerate()
        .filter_map(|(i, list)| list.page.map(|n| quote!(#i => #n,)));
    quote! {
        const NUM_LISTS: usize = #n;

        fn list_key(&self, index: usize) -> ::std::vec::Vec<u8> {
            match index {
                #(#key_arms)*
                _ => ::std::vec::Vec::new(),
            }
        }

        fn list_page(index: usize) -> usize {
            match index {
                #(#page_arms)*
                _ => <Self as ::wavedb_core::NonUniqueStruct>::PAGE,
            }
        }
    }
}

/// The `{Name}Lists` trait + its `CollectionHandle<{Name}>` impl: per declared
/// list, a whole-list enumeration and the paged entry point the sparse index's
/// counts make one descent.
pub fn listed_readers(name: &Ident, lists: &[ResolvedList]) -> TokenStream {
    if lists.is_empty() {
        return TokenStream::new();
    }
    let trait_ident = format_ident!("{}Lists", name);
    let decls = lists.iter().map(|l| reader_decls(name, &l.order, None));
    let impls = lists
        .iter()
        .enumerate()
        .map(|(i, l)| reader_decls(name, &l.order, Some(i)));

    quote! {
        /// Typed list enumerations for this type's collection, one per
        /// `#[wavedb::list]` (generated by `#[wavedb]`).
        pub trait #trait_ident {
            #(#decls)*
        }

        impl #trait_ident for ::wavedb_core::CollectionHandle<#name> {
            #(#impls)*
        }
    }
}

/// One list's three readers, as declarations (`index = None`) or as the
/// implementations calling into the collection at `index`.
fn reader_decls(
    name: &Ident,
    list: &ResolvedPivot,
    index: Option<usize>,
) -> TokenStream {
    let listed = list.listed_ident();
    let at_page = format_ident!("{}_at_page", listed);
    let len = format_ident!("{}_len", listed);
    let stream = quote! {
        impl ::wavedb_core::Stream<
            Item = ::core::result::Result<#name, D::Error>,
        >
    };
    let Some(i) = index else {
        return quote! {
            /// Stream every living record in this declared order — one read
            /// per segment, records inline.
            fn #listed<D: ::wavedb_core::DbHandle>(&self, db: &D) -> #stream;

            /// The same list entered at page `page` of `page_size` rows: one
            /// sparse-index descent instead of a walk. The stream runs on to
            /// the end of the list; take `page_size` from it.
            fn #at_page<D: ::wavedb_core::DbHandle>(
                &self,
                db: &D,
                page: usize,
                page_size: usize,
            ) -> #stream;

            /// How many living records this list holds — the pager's "of M".
            ///
            /// # Errors
            /// The context's failure (backend/transport).
            #[allow(clippy::future_not_send)]
            fn #len<D: ::wavedb_core::DbHandle>(
                &self,
                db: &D,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<u64, D::Error>,
            >;
        };
    };
    quote! {
        fn #listed<D: ::wavedb_core::DbHandle>(&self, db: &D) -> #stream {
            self.listed(db, #i)
        }

        fn #at_page<D: ::wavedb_core::DbHandle>(
            &self,
            db: &D,
            page: usize,
            page_size: usize,
        ) -> #stream {
            self.listed_at(db, #i, (page as u64).saturating_mul(page_size as u64))
        }

        #[allow(clippy::future_not_send)]
        fn #len<D: ::wavedb_core::DbHandle>(
            &self,
            db: &D,
        ) -> impl ::core::future::Future<
            Output = ::core::result::Result<u64, D::Error>,
        > {
            self.list_len(db, #i)
        }
    }
}
