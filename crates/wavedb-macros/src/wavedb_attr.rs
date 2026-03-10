//! The `#[wavedb]` attribute expansion.
//!
//! Turns a plain struct into a WaveDB object. For every struct it emits:
//!
//! - the original struct (with `#[wavedb::pivot(...)]` helper attributes stripped),
//! - a [`WaveWire`] impl over the declared fields,
//! - inherent `STRUCT_HASH` / `SHAPE` / `HAS_VALIDATE` / `HAS_PREPROCESS` consts,
//! - a [`WaveDbStruct`] impl tying identity, shape, and the `PivotId` type together.
//!
//! For a `NonUnique` struct it additionally emits the generated `{Name}PivotId` and
//! `{Name}Pivot` types (see [`crate::generated`]).
//!
//! [`WaveWire`]: wavedb_wire::WaveWire
//! [`WaveDbStruct`]: wavedb_core::traits::WaveDbStruct

use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::parse::Parser;
use syn::{Data, DeriveInput, Fields, Ident};

use crate::args::{Shape, WavedbArgs};
use crate::declarations::take_declarations;
use crate::{exec_ops, generated, storage_statics, struct_hash, wire_derive};

/// Expand `#[wavedb(<attr>)] <item>`.
pub fn expand(
    attr: TokenStream,
    item: TokenStream,
) -> syn::Result<TokenStream> {
    let args = Parser::parse2(WavedbArgs::parse, attr)?;
    let mut input: DeriveInput = syn::parse2(item)?;

    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input,
            "#[wavedb] can only be applied to structs",
        ));
    };
    let Fields::Named(named) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &input,
            "#[wavedb] structs must have named fields",
        ));
    };

    // The declarations are taken off a *clone* of the fields, because taking
    // the field-level `#[wavedb::list]` markers mutates `input` while the
    // resolution needs to read the field list.
    let named = named.clone();

    // Field (name, normalised-type) pairs feed the STRUCT_HASH.
    let mut hash_fields: Vec<(String, String)> = named
        .named
        .iter()
        .map(|f| {
            let name = f.ident.as_ref().expect("named field").to_string();
            (name, normalise_type(&f.ty))
        })
        .collect();

    let declared =
        take_declarations(&mut input, &named, &args, &mut hash_fields)?;

    let name = input.ident.clone();
    let hash = struct_hash::compute(
        &name.to_string(),
        args.shape.as_str(),
        &hash_fields,
    );

    let wire_impl = wire_derive::derive(&input)?;
    let shape_variant =
        Ident::new(args.shape.core_variant(), Span::call_site());
    let has_validate = args.validate.is_some();
    let has_preprocess = args.preprocess.is_some();

    // The PivotId associated type: () for Unique, the generated newtype otherwise.
    // Unique types get the anchor ops (`get`/`save`); NonUnique types get their
    // collection machinery from `generated::nonunique_types`.
    let (pivot_id_ty, generated_types) = match args.shape {
        Shape::Unique => (quote!(()), unique_ops(&name)),
        Shape::NonUnique => {
            let pivot_id = format_ident!("{}PivotId", name);
            let types =
                generated::nonunique_types(&name, hash, &declared, args.page)?;
            (quote!(#pivot_id), types)
        }
    };

    let (storage_slot, storage_entries, exec_steps, shape_marker) =
        shape_scaffolding(&name, args.shape, args.compress);
    let lane_hashes = lane_hashes_const(args.shape, hash);

    Ok(quote! {
        #input

        impl #name {
            /// Compile-time identity of this type and its schema generation.
            pub const STRUCT_HASH: u64 = #hash;
            /// This type's cardinality discipline.
            pub const SHAPE: ::wavedb_core::Shape = ::wavedb_core::Shape::#shape_variant;
            /// Whether a `validate` hook is declared.
            pub const HAS_VALIDATE: bool = #has_validate;
            /// Whether a `preprocess` hook is declared.
            pub const HAS_PREPROCESS: bool = #has_preprocess;
        }

        #wire_impl

        impl ::wavedb_core::WaveDbStruct for #name {
            const STRUCT_HASH: u64 = #hash;
            const SHAPE: ::wavedb_core::Shape = ::wavedb_core::Shape::#shape_variant;
            const LANE_HASHES: &'static [u64] = #lane_hashes;
            type PivotId = #pivot_id_ty;
        }

        // As a `#[server]` signature type this struct tags as its own
        // schema identity — evolving it transitively renames every function
        // whose signature carries it.
        impl ::wavedb_core::FnArgTag for #name {
            const TAG: u64 = #hash;
        }

        #shape_marker
        #storage_slot
        #storage_entries
        #exec_steps

        #generated_types
    })
}

/// The `Unique` anchor ops: `get` / `save` / `history` inherent fns over any
/// [`DbHandle`](wavedb_core::DbHandle) — the same spelling resolves against a
/// `LocalHandle`, the client `Db`, and a node-side `ServerDb`. `save` **is**
/// the upsert — a `Unique` type has no separate create.
fn unique_ops(name: &Ident) -> TokenStream {
    quote! {
        impl #name {
            /// Fetch this tenant's record from its `STRUCT_HASH` anchor, as a
            /// `Wire` over its bytes: `.view()` reads it, `.try_into()` owns
            /// it. `None` = never saved.
            ///
            /// # Errors
            /// The context's failure (backend/transport) or a head fault; the
            /// body is checked when viewed.
            #[allow(clippy::future_not_send)]
            pub async fn get<D: ::wavedb_core::DbHandle>(
                db: &D,
            ) -> ::core::result::Result<
                ::core::option::Option<::wavedb_core::wire::Wire<Self>>,
                D::Error,
            >
            {
                db.get_unique::<Self>().await
            }

            /// Save (insert-or-overwrite) this tenant's record at its anchor.
            /// A save over an existing record archives the superseded version
            /// — the timeline stays walkable via [`history`](Self::history).
            ///
            /// # Errors
            /// The context's failure (backend/transport).
            #[allow(clippy::future_not_send)]
            pub async fn save<D: ::wavedb_core::DbHandle>(
                &self,
                db: &D,
            ) -> ::core::result::Result<(), D::Error> {
                db.save_unique(self).await
            }

            /// [`save`](Self::save), resolving once the save is durable —
            /// on the node that answers it, or in the store a local context
            /// drives. Everything committed before it there is durable with
            /// it.
            ///
            /// # Errors
            /// As [`save`](Self::save), or a failed settle.
            #[allow(clippy::future_not_send)]
            pub async fn save_durable<D: ::wavedb_core::DbHandle>(
                &self,
                db: &D,
            ) -> ::core::result::Result<(), D::Error> {
                db.save_unique_durable(self).await
            }

            /// Stream this tenant's record versions **newest-first** (the
            /// live record, then each archived version along the
            /// modification chain). Empty when never saved.
            pub fn history<D: ::wavedb_core::DbHandle>(
                db: &D,
            ) -> impl ::wavedb_core::Stream<
                Item = ::core::result::Result<
                    (::wavedb_core::Metadata, ::wavedb_core::wire::Wire<Self>),
                    D::Error,
                >,
            > {
                db.unique_history::<Self>()
            }
        }
    }
}

/// The per-shape scaffolding around the generated types: the native-only
/// `StructStorage` static (the NonUnique variant's Pivot slot and
/// `storage_entries()` are emitted with the pivot types in
/// `generated::nonunique_types`), the per-command execution steps
/// (`__wavedb_<op>` — NonUnique steps need the generated `PivotId` type,
/// so they too emit with it), and the shape marker trait (`UniqueStruct`
/// for the default shape; the NonUnique marker is emitted with the
/// collection types) client typed surfaces gate on.
fn shape_scaffolding(
    name: &Ident,
    shape: Shape,
    compress: bool,
) -> (TokenStream, TokenStream, TokenStream, TokenStream) {
    let struct_hash_expr =
        quote!(<#name as ::wavedb_core::WaveDbStruct>::STRUCT_HASH);
    let storage_slot =
        storage_statics::statics_for(name, &struct_hash_expr, compress, false);
    let (storage_entries, exec_steps, shape_marker) = match shape {
        Shape::Unique => (
            storage_statics::entries_for(name, None),
            exec_ops::unique_ops(name),
            quote!(impl ::wavedb_core::UniqueStruct for #name {}),
        ),
        Shape::NonUnique => {
            (TokenStream::new(), TokenStream::new(), TokenStream::new())
        }
    };
    (storage_slot, storage_entries, exec_steps, shape_marker)
}

/// The reserved lane hashes this shape occupies, as the `&'static [u64]`
/// initialiser for `WaveDbStruct::LANE_HASHES`.
///
/// Literals, because SeaHash is not a `const fn` — the same reason the
/// `StructStorage` slots carry them. Both derivations must agree; that is
/// what `lane_hashes_match_the_engines` pins.
fn lane_hashes_const(shape: Shape, hash: u64) -> TokenStream {
    if matches!(shape, Shape::Unique) {
        // No collection, so no chains and no lanes.
        return quote!(&[]);
    }
    let records = crate::struct_hash::lane_hash(b"WDB.SEG", hash);
    let recency = crate::struct_hash::lane_hash(b"WDB.REC", hash);
    let dead = crate::struct_hash::lane_hash(b"WDB.DEAD", hash);
    let index = crate::struct_hash::lane_hash(b"WDB.IDX", hash);
    let secondary = crate::struct_hash::lane_hash(b"WDB.SEC", hash);
    let fuzzy = crate::struct_hash::lane_hash(b"WDB.FZY", hash);
    quote!(&[#records, #recency, #dead, #index, #secondary, #fuzzy])
}

/// A whitespace-free rendering of a field type, so the same declared type always
/// hashes identically (`Vec < String >` → `Vec<String>`).
fn normalise_type(ty: &syn::Type) -> String {
    quote!(#ty).to_string().split_whitespace().collect()
}
