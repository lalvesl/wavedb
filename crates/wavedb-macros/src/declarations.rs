//! What a struct **declares** beyond its own fields — `#[wavedb::key(...)]`,
//! the `#[wavedb(...)]` layout knobs, `#[wavedb::list]` and
//! `#[wavedb::pivot(...)]` — taken off the item, folded into the identity where
//! they reach stored bytes, and resolved against the struct's fields.
//!
//! Split from [`wavedb_attr`](crate::wavedb_attr) for the file budget, along
//! the seam the expansion already has: this module decides *what was declared*,
//! [`generated`](crate::generated) decides what that emits.

use proc_macro2::Span;
use syn::{Attribute, DeriveInput, Ident};

use crate::arg_specs::PivotSpec;
use crate::args::{Shape, WavedbArgs};
use crate::lists;
use crate::natural_key::take_and_fold_key;
use crate::secondaries::ResolvedPivot;

/// Everything a struct declares beyond its own fields, taken off the item and
/// resolved against it.
pub struct Declared {
    /// `#[wavedb::key(...)]`'s fields, when one is declared.
    pub key_fields: Option<Vec<(Ident, syn::Type)>>,
    /// One entry per `#[wavedb::pivot(...)]` secondary index.
    pub secondaries: Vec<ResolvedPivot>,
    /// One entry per `#[wavedb::list]` declared ordering (RFC 0051).
    pub lists: Vec<crate::lists::ResolvedList>,
    /// One entry per `#[wavedb::fuzzy]` declared field (RFC 0056).
    pub fuzzy: Vec<crate::fuzzy::ResolvedFuzzy>,
}

/// Take every declaration off `input`, folding the ones that reach stored bytes
/// into `hash_fields`.
///
/// Order is fixed and load-bearing: the fold order **is** part of the identity,
/// so the sequence here (key, layout knobs, lists, fuzzy, secondaries) must not
/// be shuffled for tidiness — and a new kind is **appended**, never inserted, so
/// adding one leaves every existing type's hash alone. A fuzzy index folds its
/// normalization and gram width, both stored choices; a secondary folds its
/// fields, since its chain stores their encoding (RFC 0075).
pub fn take_declarations(
    input: &mut DeriveInput,
    named: &syn::FieldsNamed,
    args: &WavedbArgs,
    hash_fields: &mut Vec<(String, String)>,
) -> syn::Result<Declared> {
    let key_fields =
        take_and_fold_key(&mut input.attrs, named, args.shape, hash_fields)?;
    fold_layout_args(args, hash_fields)?;
    let lists = lists::take_and_fold(input, named, args.shape, hash_fields)?;
    let fuzzy = crate::fuzzy::take_and_fold(input, args.shape, hash_fields)?;
    let pivot_specs = take_pivot_specs(&mut input.attrs)?;
    let secondaries = resolve_pivot_fields(&pivot_specs, named)?;
    if !secondaries.is_empty() && args.shape != Shape::NonUnique {
        return Err(syn::Error::new(
            Span::call_site(),
            "#[wavedb::pivot(...)] is only valid on a #[wavedb(NonUnique)] struct",
        ));
    }
    crate::orderings::check(&secondaries, &lists)?;
    fold_secondaries(&secondaries, hash_fields);
    Ok(Declared {
        key_fields,
        secondaries,
        lists,
        fuzzy,
    })
}

/// Fold each secondary index, in declaration order, as a synthetic
/// `#pivot{i}` entry naming its fields ([RFC 0075]): index `i` of the
/// `Pivot` names a chain whose stored keys are those fields' encoding, so
/// reordering or retyping the declarations would point `by_` at another
/// field's chain — a fact about stored bytes, so it is a new type.
///
/// [RFC 0075]: ../../../rfcs/0075-secondary-indexes-as-chains.md
fn fold_secondaries(
    secondaries: &[ResolvedPivot],
    hash_fields: &mut Vec<(String, String)>,
) {
    for (i, pivot) in secondaries.iter().enumerate() {
        let joined = pivot
            .fields
            .iter()
            .map(|(f, _)| f.to_string())
            .collect::<Vec<_>>()
            .join(",");
        hash_fields.push((format!("#pivot{i}"), joined));
    }
}

/// Remove every `#[wavedb::pivot(...)]` attribute from `attrs`, parsing each
/// into the fields it declares (declaration order preserved).
fn take_pivot_specs(attrs: &mut Vec<Attribute>) -> syn::Result<Vec<PivotSpec>> {
    let mut specs = Vec::new();
    let mut kept = Vec::with_capacity(attrs.len());
    for attr in attrs.drain(..) {
        if is_pivot_attr(&attr) {
            specs.push(attr.parse_args::<PivotSpec>()?);
        } else {
            kept.push(attr);
        }
    }
    *attrs = kept;
    Ok(specs)
}

/// Resolve each declared pivot field against the struct's named fields,
/// pairing it with its type — an unknown field is a compile error at the
/// declaration site.
fn resolve_pivot_fields(
    specs: &[PivotSpec],
    named: &syn::FieldsNamed,
) -> syn::Result<Vec<ResolvedPivot>> {
    specs
        .iter()
        .map(|spec| {
            Ok(ResolvedPivot {
                fields: resolve_fields(
                    &spec.fields,
                    named,
                    "#[wavedb::pivot(...)]",
                )?,
            })
        })
        .collect()
}

/// Pair each declared field name with its type from the struct's own fields —
/// an unknown name is a compile error at the declaration site, attributed to
/// `what` (the attribute doing the declaring).
pub fn resolve_fields(
    declared: &[Ident],
    named: &syn::FieldsNamed,
    what: &str,
) -> syn::Result<Vec<(Ident, syn::Type)>> {
    declared
        .iter()
        .map(|ident| {
            named
                .named
                .iter()
                .find(|f| f.ident.as_ref() == Some(ident))
                .map(|f| (ident.clone(), f.ty.clone()))
                .ok_or_else(|| {
                    syn::Error::new_spanned(
                        ident,
                        format!(
                            "{what} names a field this struct does not declare"
                        ),
                    )
                })
        })
        .collect()
}

/// `true` for a `#[wavedb::pivot(...)]` helper attribute.
fn is_pivot_attr(attr: &Attribute) -> bool {
    let segs = &attr.path().segments;
    segs.len() == 2 && segs[0].ident == "wavedb" && segs[1].ident == "pivot"
}

/// Fold the `#[wavedb(...)]` arguments that **reach stored bytes** into the
/// hash inputs, as synthetic `#name` entries (`#` cannot open a real field
/// name, so they can never collide with one).
///
/// This is the project rule, not a local choice: a declaration that decides how
/// bytes are laid out yields a **new type** when it changes, because the engine
/// performs no migration and the alternative is data that silently no longer
/// matches what its declaration claims (RFC 0052). Each entry is emitted only
/// when the declaration departs from the default, exactly as `#[wavedb::key]`
/// does it, so the plain spelling keeps its identity.
///
/// Arguments that never reach disk — the `validate` / `preprocess` hooks — do
/// not fold: they are behaviour, and correcting one must not orphan the data.
fn fold_layout_args(
    args: &WavedbArgs,
    hash_fields: &mut Vec<(String, String)>,
) -> syn::Result<()> {
    if !args.compress {
        hash_fields.push(("#compress".into(), "false".into()));
    }
    if let Some(page) = args.page {
        if args.shape != Shape::NonUnique {
            return Err(syn::Error::new(
                Span::call_site(),
                "`page = N` is only valid on a #[wavedb(NonUnique)] struct — \
                 a Unique type has no collection and so no record chain",
            ));
        }
        hash_fields.push(("#page".into(), page.to_string()));
    }
    Ok(())
}
