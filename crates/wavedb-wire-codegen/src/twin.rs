//! The borrowed twin `NameRef<'a>` a derive emits beside `Name` (RFC 0076):
//! its generics, its definition for a struct, and the impls every twin
//! carries — `Clone`, `Copy`, `Debug`, `From<NameRef> for Name` — and the
//! `TryFrom<Wire<Name>>` the owned type takes.

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{
    DeriveInput, Fields, Generics, Member, Type, Visibility, parse_quote,
};

use crate::twin_ident;

/// One field of a struct or variant: how to reach it, its type, and who may.
pub struct Field<'f> {
    pub member: Member,
    pub ty: &'f Type,
    pub vis: &'f Visibility,
}

/// The fields of `fields`, in declaration order.
pub fn field_list(fields: &Fields) -> Vec<Field<'_>> {
    fields
        .iter()
        .zip(fields.members())
        .map(|(f, member)| Field {
            member,
            ty: &f.ty,
            vis: &f.vis,
        })
        .collect()
}

/// What a derive needs to write `Name`'s twin.
pub struct Twin<'i> {
    input: &'i DeriveInput,
    k: TokenStream,
    pub ident: Ident,
    /// `Name`'s generics with `'a` in front — its own bounds already make
    /// every field `WaveWire`, or its impl would not compile. Empty when
    /// unit-like.
    generics: Generics,
    /// No field anywhere: nothing to borrow, so the twin is a plain copy of
    /// the shape, with no lifetime.
    unit_like: bool,
}

impl<'i> Twin<'i> {
    /// `tys` is every field type the twin holds a view of, across variants.
    pub fn new(input: &'i DeriveInput, k: &TokenStream, tys: &[&Type]) -> Self {
        let unit_like = tys.is_empty();
        let mut generics = Generics::default();
        if !unit_like {
            generics = input.generics.clone();
            generics.params.insert(0, parse_quote!('a));
        }
        Self {
            input,
            k: k.clone(),
            ident: twin_ident(&input.ident),
            generics,
            unit_like,
        }
    }

    /// `NameRef<'a, …>`, the type `Ref<'a>` names.
    pub fn ty(&self) -> TokenStream {
        let tw = &self.ident;
        let (_, tg, _) = self.generics.split_for_impl();
        quote!(#tw #tg)
    }

    /// The view type of a field of type `ty`.
    pub fn view_of(&self, ty: &Type) -> TokenStream {
        let k = &self.k;
        quote!(<#ty as #k::WaveWire>::Ref<'a>)
    }

    /// The doc line on the twin.
    pub fn doc(&self) -> String {
        format!(
            "The borrowed twin of `{}`: each field's view over the bytes, \
             built by `WaveWire::view` in one checked walk (RFC 0076). \
             `Name::from` (or `.into()`) gives the owned value back.",
            self.input.ident
        )
    }

    /// The derives a unit-like twin takes, which holds nothing.
    pub fn unit_derives() -> TokenStream {
        quote!(#[derive(
            ::core::clone::Clone,
            ::core::marker::Copy,
            ::core::fmt::Debug,
            ::core::cmp::PartialEq,
            ::core::cmp::Eq,
            ::core::hash::Hash,
        )])
    }

    /// The twin of a struct.
    pub fn struct_def(&self, shape: &Fields, fields: &[Field]) -> TokenStream {
        let (tw, vis, doc) = (&self.ident, &self.input.vis, self.doc());
        if self.unit_like {
            let derives = Self::unit_derives();
            return quote!(#derives #[doc = #doc] #vis struct #tw;);
        }
        let g = &self.generics;
        let wc = &g.where_clause;
        let views = fields.iter().map(|f| self.view_of(f.ty));
        let vises = fields.iter().map(|f| f.vis);
        if matches!(shape, Fields::Unnamed(_)) {
            return quote! {
                #[doc = #doc]
                #vis struct #tw #g ( #( #vises #views, )* ) #wc;
            };
        }
        let members = fields.iter().map(|f| &f.member);
        let docs = fields.iter().map(|f| field_doc(&f.member));
        quote! {
            #[doc = #doc]
            #vis struct #tw #g #wc {
                #( #[doc = #docs] #vises #members: #views, )*
            }
        }
    }

    /// The body of a struct twin's `Debug::fmt`.
    pub fn debug_struct(
        &self,
        shape: &Fields,
        fields: &[Field],
    ) -> TokenStream {
        let label = self.ident.to_string();
        let members: Vec<_> = fields.iter().map(|f| &f.member).collect();
        if matches!(shape, Fields::Unnamed(_)) {
            return quote!(f.debug_tuple(#label) #( .field(&self.#members) )* .finish());
        }
        let names = members.iter().map(|m| quote!(#m).to_string());
        quote!(f.debug_struct(#label) #( .field(#names, &self.#members) )* .finish())
    }

    /// `Clone`, `Copy` and `Debug` (by hand: a derive would bound every type
    /// parameter, though the twin holds only views), then the two ways back
    /// to the owned type.
    pub fn support(&self, debug_body: &TokenStream) -> TokenStream {
        let (k, tw, name) = (&self.k, &self.ident, &self.input.ident);
        let (ig, tg, wc) = self.generics.split_for_impl();
        let (_, owned_tg, _) = self.input.generics.split_for_impl();
        let traits = if self.unit_like {
            quote!()
        } else {
            quote! {
                #[automatically_derived]
                impl #ig ::core::clone::Clone for #tw #tg #wc {
                    fn clone(&self) -> Self { *self }
                }
                #[automatically_derived]
                impl #ig ::core::marker::Copy for #tw #tg #wc {}
                #[automatically_derived]
                impl #ig ::core::fmt::Debug for #tw #tg #wc {
                    fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                        #debug_body
                    }
                }
            }
        };
        // `TryFrom` per type: a blanket `impl<T> TryFrom<Wire<T>> for T`
        // is an orphan. Over the type's own generics: no view is involved.
        let (oig, _, owc) = self.input.generics.split_for_impl();
        quote! {
            #traits
            #[automatically_derived]
            impl #ig ::core::convert::From<#tw #tg> for #name #owned_tg #wc {
                fn from(r: #tw #tg) -> Self {
                    <Self as #k::WaveWire>::from_ref(r)
                }
            }
            #[automatically_derived]
            impl #oig ::core::convert::TryFrom<#k::Wire<Self>> for #name #owned_tg #owc {
                type Error = #k::Error;
                fn try_from(w: #k::Wire<Self>) -> #k::Result<Self> {
                    w.decode()
                }
            }
        }
    }

    /// The twin's generics, for an enum's definition.
    pub const fn generics(&self) -> &Generics {
        &self.generics
    }

    /// Whether the twin holds nothing.
    pub const fn is_unit_like(&self) -> bool {
        self.unit_like
    }
}

/// The doc line on a field of the twin.
pub fn field_doc(member: &Member) -> String {
    format!("`{}`, viewed.", quote!(#member))
}
