//! Fuzzy-index codegen for `#[wavedb::fuzzy]` ([RFC 0056]): taking the
//! declarations off the struct's **fields**, folding them into the identity,
//! and the `NonUniqueStruct` hooks (`NUM_FUZZY` + `fuzzy_source` +
//! `fuzzy_profile`).
//!
//! Field-level only, unlike `#[wavedb::list]`. A list needs a struct-level
//! spelling because a composite ordering has no single field to sit on; a
//! fuzzy index is built over exactly one string, so a header form would only
//! restate a name the attribute is already sitting next to.
//!
//! [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md

use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{Attribute, Data, DeriveInput, Ident, Type};

use crate::arg_specs::FuzzySpec;
use crate::args::Shape;

/// The engine default, mirrored here so the fold can emit a **resolved** value
/// rather than eliding it. Must track `wavedb_core::fuzzy::DEFAULT_N`, and
/// `the_macro_default_matches_the_engine` in the schema fixture is what keeps
/// them together.
const DEFAULT_N: usize = 3;

/// One resolved `#[wavedb::fuzzy]`: the field it indexes and its profile.
pub struct ResolvedFuzzy {
    /// The marked field.
    pub field: Ident,
    /// The gram width, resolved (never `None` — see [`take_and_fold`]).
    pub n: usize,
    /// The normalization profile, resolved.
    pub fold: String,
}

/// Take every `#[wavedb::fuzzy]` off `input`'s fields, in field order, and
/// fold each into the STRUCT_HASH.
///
/// **The fold writes resolved values, not declared ones**, which departs from
/// how `#[wavedb(compress)]` and `page` elide their defaults — and the reason
/// is worth stating. Those knobs elide so that types predating the knob keep
/// their identity. `#fuzzy` is new, so there is no identity to preserve, and
/// eliding would create a real hazard instead: if [`DEFAULT_N`] ever changed,
/// an undeclared `#[wavedb::fuzzy]` would keep its old hash while its postings
/// were suddenly laid out at a different width — the exact silent
/// falsification the fold exists to prevent. Writing `name@3/latin` means a
/// changed default mints new types, which is the correct answer.
pub fn take_and_fold(
    input: &mut DeriveInput,
    shape: Shape,
    hash_fields: &mut Vec<(String, String)>,
) -> syn::Result<Vec<ResolvedFuzzy>> {
    let declared = take_field_fuzzy(input)?;
    if !declared.is_empty() && shape != Shape::NonUnique {
        return Err(syn::Error::new(
            Span::call_site(),
            "#[wavedb::fuzzy] is only valid on a #[wavedb(NonUnique)] struct — \
             a Unique type has one record per tenant and so nothing to search",
        ));
    }
    let mut out = Vec::with_capacity(declared.len());
    for (field, ty, spec) in declared {
        require_string(&field, &ty)?;
        out.push(ResolvedFuzzy {
            field,
            n: spec.n.unwrap_or(DEFAULT_N),
            fold: spec.fold.unwrap_or_else(|| "latin".into()),
        });
    }
    for (i, fz) in out.iter().enumerate() {
        hash_fields.push((
            format!("#fuzzy{i}"),
            format!("{}@{}/{}", fz.field, fz.n, fz.fold),
        ));
    }
    Ok(out)
}

/// Strip `#[wavedb::fuzzy]` from the struct's fields, returning each marked
/// field in declaration order with what it declared.
fn take_field_fuzzy(
    input: &mut DeriveInput,
) -> syn::Result<Vec<(Ident, Type, FuzzySpec)>> {
    let Data::Struct(data) = &mut input.data else {
        return Ok(Vec::new());
    };
    let mut marked = Vec::new();
    for field in &mut data.fields {
        let mut kept = Vec::with_capacity(field.attrs.len());
        for attr in field.attrs.drain(..) {
            if !is_fuzzy_attr(&attr) {
                kept.push(attr);
                continue;
            }
            let spec = match &attr.meta {
                syn::Meta::Path(_) => FuzzySpec::default(),
                _ => attr.parse_args::<FuzzySpec>()?,
            };
            let ident = field.ident.clone().ok_or_else(|| {
                syn::Error::new_spanned(&attr, "unnamed field")
            })?;
            marked.push((ident, field.ty.clone(), spec));
        }
        field.attrs = kept;
    }
    Ok(marked)
}

/// Refuse a non-`String` field at the declaration site.
///
/// A heuristic on the type's spelling, like the one `by_<field>` uses to pick
/// its parameter type — the macro cannot resolve types. It earns its keep by
/// turning a confusing downstream trait error into a message that names the
/// actual problem.
fn require_string(field: &Ident, ty: &Type) -> syn::Result<()> {
    let is_string = matches!(ty, Type::Path(p)
        if p.path.segments.last().is_some_and(|s| s.ident == "String"));
    if is_string {
        return Ok(());
    }
    Err(syn::Error::new_spanned(
        ty,
        format!(
            "#[wavedb::fuzzy] indexes text, so `{field}` must be a `String` — \
             there is nothing to normalize or cut into grams otherwise"
        ),
    ))
}

/// `true` for a `#[wavedb::fuzzy]` helper attribute.
fn is_fuzzy_attr(attr: &Attribute) -> bool {
    let segs = &attr.path().segments;
    segs.len() == 2 && segs[0].ident == "wavedb" && segs[1].ident == "fuzzy"
}

/// The items injected into the `NonUniqueStruct` impl: the index count, the
/// per-index text source, and the per-index profile.
pub fn trait_items(specs: &[ResolvedFuzzy]) -> TokenStream {
    if specs.is_empty() {
        return TokenStream::new();
    }
    let n = specs.len();
    let source_arms = specs.iter().enumerate().map(|(i, fz)| {
        let field = &fz.field;
        quote!(#i => self.#field.as_str(),)
    });
    let profile_arms = specs.iter().enumerate().map(|(i, fz)| {
        let width = fz.n;
        let fold = fold_variant(&fz.fold);
        quote!(#i => (#width, ::wavedb_core::fuzzy::Fold::#fold),)
    });
    quote! {
        const NUM_FUZZY: usize = #n;

        fn fuzzy_source(&self, index: usize) -> &str {
            match index {
                #(#source_arms)*
                _ => "",
            }
        }

        fn fuzzy_profile(
            index: usize,
        ) -> (usize, ::wavedb_core::fuzzy::Fold) {
            match index {
                #(#profile_arms)*
                _ => (
                    ::wavedb_core::fuzzy::DEFAULT_N,
                    ::wavedb_core::fuzzy::Fold::Latin,
                ),
            }
        }
    }
}

/// The `{Name}Fuzzy` trait + its `CollectionHandle<{Name}>` impl: one
/// `fuzzy_<field>` lookup per declaration.
pub fn fuzzy_lookups(name: &Ident, specs: &[ResolvedFuzzy]) -> TokenStream {
    if specs.is_empty() {
        return TokenStream::new();
    }
    let trait_ident = quote::format_ident!("{}Fuzzy", name);
    let decls = specs.iter().map(|f| lookup_decl(name, f, None));
    let impls = specs
        .iter()
        .enumerate()
        .map(|(i, f)| lookup_decl(name, f, Some(i)));
    quote! {
        /// Typed fuzzy lookups for this type's collection, one per
        /// `#[wavedb::fuzzy]` (generated by `#[wavedb]`).
        pub trait #trait_ident {
            #(#decls)*
        }

        impl #trait_ident for ::wavedb_core::CollectionHandle<#name> {
            #(#impls)*
        }
    }
}

/// One declaration's lookup, as a declaration (`index = None`) or as the
/// implementation calling into the collection at `index`.
fn lookup_decl(
    name: &Ident,
    spec: &ResolvedFuzzy,
    index: Option<usize>,
) -> TokenStream {
    let ident = quote::format_ident!("fuzzy_{}", spec.field);
    let ret = quote! {
        impl ::core::future::Future<
            Output = ::core::result::Result<
                ::std::vec::Vec<::wavedb_core::fuzzy::Scored<
                    (::wavedb_core::Id, ::wavedb_core::wire::Wire<#name>),
                >>,
                D::Error,
            >,
        >
    };
    let Some(i) = index else {
        let field = spec.field.to_string();
        let doc = format!(
            "Records whose `{field}` approximately matches `query`, best \
             first, at most `limit` of them.\n\n\
             Buffered and ranked — a best-first order is not known until the \
             last candidate is scored.\n\n\
             # Errors\nThe context's failure (backend/transport)."
        );
        return quote! {
            #[doc = #doc]
            #[allow(clippy::future_not_send)]
            fn #ident<D: ::wavedb_core::DbHandle>(
                &self,
                db: &D,
                query: &str,
                mode: ::wavedb_core::fuzzy::Fuzzy,
                limit: usize,
            ) -> #ret;
        };
    };
    quote! {
        #[allow(clippy::future_not_send)]
        fn #ident<D: ::wavedb_core::DbHandle>(
            &self,
            db: &D,
            query: &str,
            mode: ::wavedb_core::fuzzy::Fuzzy,
            limit: usize,
        ) -> #ret {
            self.fuzzy(db, #i, query, mode, limit)
        }
    }
}

/// The `Fold` variant a declared profile names.
fn fold_variant(fold: &str) -> Ident {
    match fold {
        "none" => Ident::new("None", Span::call_site()),
        _ => Ident::new("Latin", Span::call_site()),
    }
}
