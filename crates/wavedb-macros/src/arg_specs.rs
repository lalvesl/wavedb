//! The **helper-attribute** grammars — `#[wavedb::pivot]`, `#[wavedb::list]`,
//! `#[wavedb::fuzzy]`, `#[wavedb::key]` — split from [`args`](crate::args) for
//! the file budget.
//!
//! [`args`](crate::args) parses what rides inside `#[wavedb(...)]` itself (the
//! shape and the layout knobs); this module parses the attributes that sit
//! *beside* it, on the struct or on a field. Both halves share the literal
//! interpreters, which is why those are `pub(crate)`.

use syn::punctuated::Punctuated;
use syn::{Ident, Meta, Token, parse::ParseStream};

use crate::args::expr_as_path;

/// One `#[wavedb::pivot(...)]` declaration: the indexed field(s) in
/// declaration order — `pivot(amount)` or a composite `pivot((customer, date))`
/// of two or three fields (the `IndexKey` tuple arities).
#[derive(Debug, Clone)]
pub struct PivotSpec {
    pub fields: Vec<Ident>,
}

impl syn::parse::Parse for PivotSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.peek(syn::token::Paren) {
            let inner;
            syn::parenthesized!(inner in input);
            let idents =
                Punctuated::<Ident, Token![,]>::parse_terminated(&inner)?;
            let fields: Vec<Ident> = idents.into_iter().collect();
            if !(2..=3).contains(&fields.len()) {
                return Err(syn::Error::new(
                    inner.span(),
                    "a composite #[wavedb::pivot((..))] takes 2 or 3 fields",
                ));
            }
            Ok(Self { fields })
        } else {
            Ok(Self {
                fields: vec![input.parse()?],
            })
        }
    }
}

/// One struct-level `#[wavedb::list(...)]` declaration ([RFC 0051]): the
/// ordering's field(s).
///
/// ```text
/// #[wavedb::list]                    // on a field: that field is the ordering
/// #[wavedb::list(name)]              // on the struct
/// #[wavedb::list((city, name))]      // composite
/// ```
///
/// A list takes no capacity: a page of it is a skip and a limit on its key
/// range ([RFC 0078] §7), so nothing about its layout is the declaration's
/// to choose.
///
/// [RFC 0051]: https://github.com/wavedb/wavedb/blob/main/rfcs/0051-ordered-record-lists.md
/// [RFC 0078]: https://github.com/wavedb/wavedb/blob/main/rfcs/0078-ordered-lanes-over-a-quantized-page-index-WIP.md
#[derive(Debug, Clone)]
pub struct ListSpec {
    /// The ordering's fields; empty when the declaration named none.
    pub fields: Vec<Ident>,
}

impl syn::parse::Parse for ListSpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self { fields: Vec::new() });
        }
        if !input.peek(syn::token::Paren) {
            return Ok(Self {
                fields: vec![input.parse()?],
            });
        }
        let inner;
        syn::parenthesized!(inner in input);
        let fields: Vec<Ident> =
            Punctuated::<Ident, Token![,]>::parse_terminated(&inner)?
                .into_iter()
                .collect();
        if !(2..=3).contains(&fields.len()) {
            return Err(syn::Error::new(
                inner.span(),
                "a composite #[wavedb::list((..))] takes 2 or 3 fields",
            ));
        }
        Ok(Self { fields })
    }
}

/// One `#[wavedb::fuzzy]` declaration ([RFC 0056]) — **field-level only**:
///
/// ```text
/// #[wavedb::fuzzy]                    // n = 3, fold = latin
/// #[wavedb::fuzzy(n = 4)]
/// #[wavedb::fuzzy(fold = none)]
/// #[wavedb::fuzzy(n = 4, fold = none)]
/// ```
///
/// There is no struct-level spelling. A fuzzy index is built over exactly one
/// string, so the header form would only ever restate a field's name — where
/// `#[wavedb::list]` needs one for composites, which have no single field to
/// sit on. Whether a *composite* fuzzy index should exist at all (concatenate
/// the fields, or union their postings?) is still an open question in RFC 0056,
/// and nothing here prejudges it.
///
/// [RFC 0056]: https://github.com/wavedb/wavedb/blob/main/rfcs/0056-fuzzy-string-search-WIP.md
#[derive(Debug, Clone, Default)]
pub struct FuzzySpec {
    /// The gram width, or `None` for the engine default.
    pub n: Option<usize>,
    /// The normalization profile, or `None` for `latin`.
    pub fold: Option<String>,
}

impl syn::parse::Parse for FuzzySpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut spec = Self::default();
        for meta in Punctuated::<Meta, Token![,]>::parse_terminated(input)? {
            let Meta::NameValue(nv) = &meta else {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "expected `n = N` or `fold = latin|none`",
                ));
            };
            if nv.path.is_ident("n") {
                spec.n = Some(expr_as_gram_width(&nv.value)?);
            } else if nv.path.is_ident("fold") {
                spec.fold = Some(expr_as_fold(&nv.value)?);
            } else {
                return Err(syn::Error::new_spanned(
                    &nv.path,
                    "the #[wavedb::fuzzy] options are `n = N` and \
                     `fold = latin|none`",
                ));
            }
        }
        Ok(spec)
    }
}

/// Interpret a `fold = …` value as a declared normalization profile.
fn expr_as_fold(expr: &syn::Expr) -> syn::Result<String> {
    let path = expr_as_path(expr)?;
    match path.get_ident().map(ToString::to_string).as_deref() {
        Some("latin") => Ok("latin".into()),
        Some("none") => Ok("none".into()),
        _ => Err(syn::Error::new_spanned(
            expr,
            "expected `latin` (lowercase + Latin diacritic fold) or `none` \
             (lowercase only)",
        )),
    }
}

/// Interpret an `n = …` value as a gram width.
///
/// Refused below 2 rather than clamped, for the same reason `page` refuses 0:
/// the value folds into the identity, so a silently-corrected width would name
/// a type that is not the one declared. And `n = 1` is not a fuzzy index — it
/// files every record under each of its characters, so a query shares grams
/// with nearly everything and the filter stops filtering.
fn expr_as_gram_width(expr: &syn::Expr) -> syn::Result<usize> {
    let syn::Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Int(n),
        ..
    }) = expr
    else {
        return Err(syn::Error::new_spanned(
            expr,
            "expected an integer literal, e.g. `n = 3`",
        ));
    };
    match n.base10_parse::<usize>()? {
        0 | 1 => Err(syn::Error::new_spanned(
            expr,
            "`n` must be at least 2 — a 1-gram index files every record under \
             each of its characters, so nothing is filtered out",
        )),
        n => Ok(n),
    }
}

/// The one `#[wavedb::key(...)]` declaration a keyed struct carries: the
/// natural-key field(s), declaration order — the record's anchor is
/// SeaHash over their wire bytes, so these fields ARE the identity.
#[derive(Debug, Clone)]
pub struct KeySpec {
    pub fields: Vec<Ident>,
}

impl syn::parse::Parse for KeySpec {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let idents = Punctuated::<Ident, Token![,]>::parse_terminated(input)?;
        let fields: Vec<Ident> = idents.into_iter().collect();
        if fields.is_empty() {
            return Err(syn::Error::new(
                input.span(),
                "#[wavedb::key(...)] takes at least one field",
            ));
        }
        Ok(Self { fields })
    }
}
