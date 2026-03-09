//! Enum expansion — the canonical tag form, and the enum's twin.
//!
//! Field-less enums encode as a single `u8` tag, and their twin is a copy of
//! the variants. If any variant carries fields, the stack slot is `tag (u8) +
//! payload-length (u32)` and the active variant's fields are written as a
//! self-contained unit in the heap (all field stacks, then all field heaps),
//! tagged by declaration order; the twin's variants hold the fields' views.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DataEnum, DeriveInput, Fields, Type, Variant, spanned::Spanned};

use crate::twin::{Twin, field_doc, field_list};
use crate::{Bodies, wire_impl};

/// The per-variant `match` arms behind an enum's methods.
#[derive(Default)]
struct EnumArms {
    heap_size: Vec<TokenStream>, // self -> active variant's payload size
    tag: Vec<TokenStream>,       // self -> tag literal (fields ignored)
    encode_heap: Vec<TokenStream>, // self -> write the unit into the heap
    decode: Vec<TokenStream>,    // tag  -> reconstruct the variant
    view: Vec<TokenStream>,      // tag  -> the variant's twin
    from_ref: Vec<TokenStream>,  // twin -> the owned variant
    debug: Vec<TokenStream>,     // twin -> its Debug output
    def: Vec<TokenStream>,       // the twin's variant declarations
}

pub fn expand_enum(
    input: &DeriveInput,
    data: &DataEnum,
    k: &TokenStream,
    own: bool,
) -> syn::Result<TokenStream> {
    if data.variants.len() > 256 {
        return Err(syn::Error::new(
            input.span(),
            "WaveWire enums are limited to 256 variants (u8 tag)",
        ));
    }
    let tys: Vec<&Type> = data
        .variants
        .iter()
        .flat_map(|v| v.fields.iter().map(|f| &f.ty))
        .collect();
    let twin = Twin::new(input, k, &tys);
    let has_fields = !twin.is_unit_like();
    let mut arms = EnumArms::default();
    for (i, v) in data.variants.iter().enumerate() {
        let tag = u8::try_from(i).map_err(|_| {
            syn::Error::new(
                v.span(),
                "WaveWire enums are limited to 256 variants",
            )
        })?;
        variant_arms(&mut arms, &twin, k, tag, v);
    }
    let EnumArms {
        heap_size,
        tag,
        encode_heap,
        decode,
        view,
        from_ref,
        debug,
        def,
    } = &arms;
    let bad_tag = quote! {
        other => ::core::result::Result::Err(#k::Error::InvalidTag(other)),
    };
    let read_tag = if has_fields {
        quote! {
            let __tag = stack.u8()?;
            let __payload_len = stack.u32()? as usize;
            let __payload = heap.take(__payload_len)?;
        }
    } else {
        quote!(let __tag = stack.u8()?;)
    };
    let bodies = Bodies {
        stack_size: if has_fields { quote!(1 + 4) } else { quote!(1) },
        heap_size: if has_fields {
            quote!(match self { #(#heap_size,)* })
        } else {
            quote!(0)
        },
        encode_stack: {
            let len = has_fields.then(|| {
                quote!(stack.extend_from_slice(
                    &(#k::WaveWire::heap_size(self) as u32).to_le_bytes(),
                );)
            });
            quote! {
                let __tag: u8 = match self { #(#tag,)* };
                stack.push(__tag);
                #len
            }
        },
        encode_heap: if has_fields {
            quote!(match self { #(#encode_heap,)* })
        } else {
            quote!()
        },
        decode: quote!(#read_tag match __tag { #(#decode,)* #bad_tag }),
        view: quote!(#read_tag match __tag { #(#view,)* #bad_tag }),
        from_ref: quote!(match r { #(#from_ref,)* }),
    };

    if own {
        return Ok(wire_impl(input, k, &quote!(Self), &bodies.own(k)));
    }
    let (tw, vis, doc) = (&twin.ident, &input.vis, twin.doc());
    let g = twin.generics();
    let wc = &g.where_clause;
    let derives = (!has_fields).then(Twin::unit_derives);
    let twin_def = quote! {
        #derives
        #[doc = #doc]
        #vis enum #tw #g #wc { #(#def,)* }
    };
    let support = twin.support(&quote!(match self { #(#debug,)* }));
    let imp = wire_impl(input, k, &twin.ty(), &bodies);
    Ok(quote!(#twin_def #support #imp))
}

/// Append one variant's arms. Tags are the declaration order.
fn variant_arms(
    arms: &mut EnumArms,
    twin: &Twin,
    k: &TokenStream,
    tag: u8,
    v: &Variant,
) {
    let (vident, tw) = (&v.ident, &twin.ident);
    let vdoc = format!("`{vident}`, viewed.");
    let fields = field_list(&v.fields);
    if fields.is_empty() {
        fieldless_arms(arms, twin, tag, v);
        return;
    }

    let tys: Vec<_> = fields.iter().map(|f| f.ty).collect();
    let braced = matches!(v.fields, Fields::Named(_));
    let binds: Vec<_> = fields
        .iter()
        .enumerate()
        .map(|(j, f)| match &f.member {
            syn::Member::Named(id) => id.clone(),
            syn::Member::Unnamed(_) => format_ident!("__f{j}"),
        })
        .collect();
    let members: Vec<_> = fields.iter().map(|f| &f.member).collect();
    // `Path { a: a, b: b }` or `Path(__f0, __f1)` over the given expressions.
    let build = |path: TokenStream, exprs: &[TokenStream]| {
        if braced {
            quote!(#path { #(#members: #exprs,)* })
        } else {
            quote!(#path( #(#exprs,)* ))
        }
    };
    // Patterns bind each field by its own name, or `__fN` in a tuple.
    let pat = |path: TokenStream| {
        if braced {
            quote!(#path { #(#binds,)* })
        } else {
            quote!(#path( #(#binds,)* ))
        }
    };
    let bind_pat = pat(quote!(Self::#vident));
    let twin_pat = pat(quote!(#tw::#vident));

    let hs = binds.iter().zip(&tys).map(|(b, ty)| {
        quote!(<#ty as #k::WaveWire>::STACK_SIZE + #k::WaveWire::heap_size(#b))
    });
    arms.heap_size.push(quote!(#bind_pat => 0 #( + #hs )*));
    arms.tag.push(quote!(Self::#vident { .. } => #tag));
    let enc_stacks = binds
        .iter()
        .map(|b| quote!(#k::WaveWire::encode_stack(#b, heap);));
    let enc_heaps = binds
        .iter()
        .map(|b| quote!(#k::WaveWire::encode_heap(#b, heap);));
    arms.encode_heap
        .push(quote!(#bind_pat => { #(#enc_stacks)* #(#enc_heaps)* }));

    let over_unit = |m: &str| -> Vec<TokenStream> {
        let m = format_ident!("{m}");
        tys.iter()
            .map(|ty| quote!(<#ty as #k::WaveWire>::#m(&mut __sc, &mut __hc)?))
            .collect()
    };
    let decoded = build(quote!(Self::#vident), &over_unit("decode"));
    let viewed = build(quote!(#tw::#vident), &over_unit("view"));
    arms.decode.push(unit_arm(k, tag, &tys, &decoded));
    arms.view.push(unit_arm(k, tag, &tys, &viewed));

    let owned: Vec<_> = binds
        .iter()
        .zip(&tys)
        .map(|(b, ty)| quote!(<#ty as #k::WaveWire>::from_ref(#b)))
        .collect();
    let owned = build(quote!(Self::#vident), &owned);
    arms.from_ref.push(quote!(#twin_pat => #owned));

    let label = vident.to_string();
    arms.debug.push(if braced {
        let names = members.iter().map(|m| quote!(#m).to_string());
        quote!(#twin_pat => f.debug_struct(#label) #( .field(#names, #binds) )* .finish())
    } else {
        quote!(#twin_pat => f.debug_tuple(#label) #( .field(#binds) )* .finish())
    });

    let views = tys.iter().map(|ty| twin.view_of(ty));
    arms.def.push(if braced {
        let docs = members.iter().map(|m| field_doc(m));
        quote!(#[doc = #vdoc] #vident { #( #[doc = #docs] #members: #views, )* })
    } else {
        quote!(#[doc = #vdoc] #vident( #(#views,)* ))
    });
}

/// A field-carrying variant's decode or view arm: split the already-taken
/// `__payload` into its stack part (the sum of the variant's field stack
/// sizes) and heap part, then `build` over two fresh cursors `__sc` / `__hc`.
fn unit_arm(
    k: &TokenStream,
    tag: u8,
    field_types: &[&Type],
    build: &TokenStream,
) -> TokenStream {
    quote! {
        #tag => {
            let __stack_size = 0usize
                #( + <#field_types as #k::WaveWire>::STACK_SIZE )*;
            if __payload.len() < __stack_size {
                return ::core::result::Result::Err(#k::Error::UnexpectedEof);
            }
            let (__vs, __vh) = __payload.split_at(__stack_size);
            let mut __sc = #k::Cursor::new(__vs);
            let mut __hc = #k::Cursor::new(__vh);
            ::core::result::Result::Ok(#build)
        }
    }
}

/// Append the arms of a variant with no fields: nothing in the heap, and
/// the same variant in the twin.
fn fieldless_arms(arms: &mut EnumArms, twin: &Twin, tag: u8, v: &Variant) {
    let (vident, tw) = (&v.ident, &twin.ident);
    let vdoc = format!("`{vident}`, viewed.");
    let own = match &v.fields {
        Fields::Named(_) => quote!(Self::#vident {}),
        Fields::Unnamed(_) => quote!(Self::#vident()),
        Fields::Unit => quote!(Self::#vident),
    };
    arms.heap_size.push(quote!(Self::#vident { .. } => 0));
    arms.tag.push(quote!(Self::#vident { .. } => #tag));
    arms.encode_heap.push(quote!(Self::#vident { .. } => {}));
    arms.decode
        .push(quote!(#tag => ::core::result::Result::Ok(#own)));
    arms.view
        .push(quote!(#tag => ::core::result::Result::Ok(#tw::#vident)));
    arms.from_ref.push(quote!(#tw::#vident => #own));
    let label = vident.to_string();
    arms.debug.push(quote!(#tw::#vident => f.write_str(#label)));
    arms.def.push(quote!(#[doc = #vdoc] #vident));
}
