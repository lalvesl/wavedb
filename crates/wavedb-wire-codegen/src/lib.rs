//! The expansion behind `#[derive(WaveWire)]`, shared by the two derives:
//! `wavedb-wire-derive`, whose output names `::wavedb_wire`, and
//! `wavedb-macros`, whose output names `::wavedb_core::wire` (so a schema
//! crate needs only `wavedb-core`). Each passes its path as `k`.
//!
//! For a type `Name` it emits the `WaveWire` impl and the type's borrowed
//! twin `NameRef<'a>` (RFC 0076): one field per field, each the field type's
//! view, with `From<NameRef>` back to `Name` and `TryFrom<Wire<Name>>`.
//!
//! - **structs** (named, tuple, unit): field stack slots inline in
//!   declaration order; heap payloads append depth-first to the shared heap.
//! - **enums**: the canonical tag form. If every variant is field-less the
//!   value is a single `u8` tag; if any variant carries fields, the stack
//!   slot is `tag (u8) + payload-length (u32)` and the active variant's
//!   fields are written as a self-contained unit in the heap (all field
//!   stacks, then all field heaps), tagged by declaration order. Mirrors
//!   `docs/wire_format.md`.
//!
//! Unions are rejected.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use syn::{Data, DataStruct, DeriveInput, Fields, spanned::Spanned};

mod enums;
mod twin;

use twin::{Twin, field_list};

/// Expand `#[derive(WaveWire)]` for `input`, naming the wire crate by `k`
/// (`::wavedb_wire`, or a path that re-exports it).
///
/// `#[wave_wire(own_view)]` on the type makes it its own view, with no
/// twin: for a `Copy + Debug` type with no heap bytes (an id, a handle),
/// where a twin would only be a copy to convert back.
///
/// # Errors
/// A union, an enum of more than 256 variants, or an unknown
/// `wave_wire` option.
pub fn expand(
    input: &DeriveInput,
    k: &TokenStream,
) -> syn::Result<TokenStream> {
    let own = own_view(input)?;
    expand_as(input, k, own)
}

/// [`expand`], the type its own view when `own` — for a derive that builds
/// its input, and so has no attribute to read.
///
/// # Errors
/// As [`expand`].
pub fn expand_as(
    input: &DeriveInput,
    k: &TokenStream,
    own: bool,
) -> syn::Result<TokenStream> {
    match &input.data {
        Data::Struct(data) => Ok(expand_struct(input, data, k, own)),
        Data::Enum(data) => enums::expand_enum(input, data, k, own),
        Data::Union(_) => Err(syn::Error::new(
            input.span(),
            "WaveWire cannot be derived for unions",
        )),
    }
}

/// The bodies of one `WaveWire` impl's methods.
pub(crate) struct Bodies {
    stack_size: TokenStream,
    heap_size: TokenStream,
    encode_stack: TokenStream,
    encode_heap: TokenStream,
    decode: TokenStream,
    view: TokenStream,
    from_ref: TokenStream,
}

impl Bodies {
    /// The impl of a type that is its own view: its view is its decode.
    pub fn own(self, k: &TokenStream) -> Self {
        Self {
            view: quote!(<Self as #k::WaveWire>::decode(stack, heap)),
            from_ref: quote!(r),
            ..self
        }
    }
}

/// Whether `input` carries `#[wave_wire(own_view)]`.
fn own_view(input: &DeriveInput) -> syn::Result<bool> {
    let mut own = false;
    for attr in input
        .attrs
        .iter()
        .filter(|a| a.path().is_ident("wave_wire"))
    {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("own_view") {
                own = true;
                Ok(())
            } else {
                Err(meta
                    .error("unknown `wave_wire` option; expected `own_view`"))
            }
        })?;
    }
    Ok(own)
}

/// The `WaveWire` impl for `input`, its view the twin type `twin_ty`.
pub(crate) fn wire_impl(
    input: &DeriveInput,
    k: &TokenStream,
    twin_ty: &TokenStream,
    b: &Bodies,
) -> TokenStream {
    let name = &input.ident;
    let (impl_generics, ty_generics, where_clause) =
        input.generics.split_for_impl();
    let Bodies {
        stack_size,
        heap_size,
        encode_stack,
        encode_heap,
        decode,
        view,
        from_ref,
    } = b;
    quote! {
        #[automatically_derived]
        impl #impl_generics #k::WaveWire for #name #ty_generics #where_clause {
            const STACK_SIZE: usize = #stack_size;

            type Ref<'a> = #twin_ty;

            type SliceRef<'a> = #k::ListRef<'a, Self>;

            fn heap_size(&self) -> usize { #heap_size }

            fn encode_stack(&self, stack: &mut ::std::vec::Vec<u8>) { #encode_stack }

            fn encode_heap(&self, heap: &mut ::std::vec::Vec<u8>) { #encode_heap }

            fn decode<'a>(
        stack: &mut #k::Cursor<'a>,
        heap: &mut #k::Cursor<'a>,
    ) -> #k::Result<Self> {
                #decode
            }

            fn view<'a>(
                stack: &mut #k::Cursor<'a>,
                heap: &mut #k::Cursor<'a>,
            ) -> #k::Result<Self::Ref<'a>> {
                #view
            }

            fn from_ref(r: Self::Ref<'_>) -> Self { #from_ref }

            fn view_slice(region: &[u8]) -> #k::Result<#k::ListRef<'_, Self>> {
                #k::ListRef::view(region)
            }

            fn from_slice_ref(r: #k::ListRef<'_, Self>) -> ::std::vec::Vec<Self> {
                r.to_vec()
            }
        }
    }
}

/// `NameRef`: the twin's identifier.
pub(crate) fn twin_ident(name: &Ident) -> Ident {
    format_ident!("{name}Ref")
}

// ---- structs ----------------------------------------------------------------

fn expand_struct(
    input: &DeriveInput,
    data: &DataStruct,
    k: &TokenStream,
    own: bool,
) -> TokenStream {
    let fields = field_list(&data.fields);
    let tys: Vec<_> = fields.iter().map(|f| f.ty).collect();
    let members: Vec<_> = fields.iter().map(|f| &f.member).collect();
    let twin = Twin::new(input, k, &tys);
    let tw = &twin.ident;

    let wire = |m: &str| -> Vec<TokenStream> {
        let m = format_ident!("{m}");
        tys.iter()
            .map(|ty| quote!(<#ty as #k::WaveWire>::#m(stack, heap)?))
            .collect()
    };
    let (decoders, viewers) = (wire("decode"), wire("view"));
    let owners = fields
        .iter()
        .map(|f| {
            let (ty, m) = (f.ty, &f.member);
            quote!(<#ty as #k::WaveWire>::from_ref(r.#m))
        })
        .collect::<Vec<_>>();
    let (decode, view, from_ref) = match &data.fields {
        Fields::Named(_) => (
            quote!(::core::result::Result::Ok(Self { #(#members: #decoders,)* })),
            quote!(::core::result::Result::Ok(#tw { #(#members: #viewers,)* })),
            quote!(Self { #(#members: #owners,)* }),
        ),
        Fields::Unnamed(_) => (
            quote!(::core::result::Result::Ok(Self( #(#decoders,)* ))),
            quote!(::core::result::Result::Ok(#tw( #(#viewers,)* ))),
            quote!(Self( #(#owners,)* )),
        ),
        Fields::Unit => (
            quote!(::core::result::Result::Ok(Self)),
            quote!(::core::result::Result::Ok(#tw)),
            quote!(Self),
        ),
    };
    // A struct of no fields has nothing to borrow: its twin is a unit.
    let (decode, view, from_ref) = if fields.is_empty() {
        let own = match &data.fields {
            Fields::Named(_) => quote!(Self {}),
            Fields::Unnamed(_) => quote!(Self()),
            Fields::Unit => quote!(Self),
        };
        (
            quote!(::core::result::Result::Ok(#own)),
            quote!(::core::result::Result::Ok(#tw)),
            own,
        )
    } else {
        (decode, view, from_ref)
    };

    let bodies = Bodies {
        stack_size: quote!(0 #( + <#tys as #k::WaveWire>::STACK_SIZE )*),
        heap_size: quote!(0 #( + #k::WaveWire::heap_size(&self.#members) )*),
        encode_stack: quote!(#( #k::WaveWire::encode_stack(&self.#members, stack); )*),
        encode_heap: quote!(#( #k::WaveWire::encode_heap(&self.#members, heap); )*),
        decode,
        view,
        from_ref,
    };
    if own {
        return wire_impl(input, k, &quote!(Self), &bodies.own(k));
    }
    let def = twin.struct_def(&data.fields, &fields);
    let support = twin.support(&twin.debug_struct(&data.fields, &fields));
    let imp = wire_impl(input, k, &twin.ty(), &bodies);
    quote!(#def #support #imp)
}
