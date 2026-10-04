//! Procedural macros for cordis. Use them through the `cordis` crate, which re-exports them.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{quote, quote_spanned};
use syn::meta::ParseNestedMeta;
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{DeriveInput, Token, Type, parenthesized, parse_macro_input};

/// See `cordis::component`, which re-exports this macro with its documentation.
#[proc_macro_attribute]
pub fn component(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut decl = Declaration::default();
    let parser = syn::meta::parser(|meta| decl.parse(meta));
    parse_macro_input!(attr with parser);
    let input = parse_macro_input!(item as DeriveInput);
    let expanded = decl.expand(&input);
    quote!(#input #expanded).into()
}

#[derive(Default)]
struct Declaration {
    config: Option<Type>,
    deps: Option<Vec<Type>>,
    provides: Option<Vec<Type>>,
}

impl Declaration {
    fn parse(&mut self, meta: ParseNestedMeta) -> syn::Result<()> {
        let duplicate = |name: &str| meta.error(format_args!("`{name}` is given more than once"));
        if meta.path.is_ident("config") {
            if self.config.is_some() {
                return Err(duplicate("config"));
            }
            self.config = Some(meta.value()?.parse()?);
        } else if meta.path.is_ident("deps") {
            if self.deps.is_some() {
                return Err(duplicate("deps"));
            }
            self.deps = Some(keys(&meta)?);
        } else if meta.path.is_ident("provides") {
            if self.provides.is_some() {
                return Err(duplicate("provides"));
            }
            self.provides = Some(keys(&meta)?);
        } else {
            return Err(meta.error("expected `config = Type`, `deps(...)`, or `provides(...)`"));
        }
        Ok(())
    }

    fn expand(self, input: &DeriveInput) -> TokenStream2 {
        let name = &input.ident;
        let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
        let config = self.config.map_or_else(|| quote!(()), |config| quote!(#config));
        let deps = self.deps.unwrap_or_default();
        let provides = self.provides.unwrap_or_default();
        let publish = provides.iter().map(|key| {
            // Spanned at the key, so a component that does not implement the key's value type
            // gets the error on that key.
            quote_spanned! {key.span()=> {
                let value: ::std::sync::Arc<<#key as ::cordis::Key>::Value> = component.clone();
                (::cordis::KeyId::of::<#key>(), ::cordis::Erased::new::<#key>(value))
            }}
        });
        let has = deps.iter().map(|key| {
            quote_spanned! {key.span()=>
                impl #impl_generics ::cordis::Has<#key> for #name #ty_generics #where_clause {}
            }
        });
        quote! {
            impl #impl_generics ::cordis::Manifest for #name #ty_generics #where_clause {
                type Config = #config;

                fn deps() -> ::std::vec::Vec<::cordis::KeyId> {
                    ::std::vec![#(::cordis::KeyId::of::<#deps>()),*]
                }

                fn provides() -> ::std::vec::Vec<::cordis::KeyId> {
                    ::std::vec![#(::cordis::KeyId::of::<#provides>()),*]
                }

                #[allow(unused_variables)]
                fn publish(
                    component: &::std::sync::Arc<Self>,
                ) -> ::std::vec::Vec<(::cordis::KeyId, ::cordis::Erased)> {
                    ::std::vec![#(#publish),*]
                }
            }

            #(#has)*
        }
    }
}

/// Parses a parenthesized, comma-separated list of key types: `(dyn Tools, dyn Fs)`.
fn keys(meta: &ParseNestedMeta) -> syn::Result<Vec<Type>> {
    let content;
    parenthesized!(content in meta.input);
    Ok(Punctuated::<Type, Token![,]>::parse_terminated(&content)?.into_iter().collect())
}
