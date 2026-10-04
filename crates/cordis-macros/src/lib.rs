//! Procedural macros for cordis. Use them through the `cordis` crate, which re-exports them.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::meta::ParseNestedMeta;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{DeriveInput, Ident, Token, Type, parenthesized, parse_macro_input};

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
    deps: Option<Vec<Dep>>,
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
            self.deps = Some(list(&meta)?);
        } else if meta.path.is_ident("provides") {
            if self.provides.is_some() {
                return Err(duplicate("provides"));
            }
            self.provides = Some(list(&meta)?);
        } else {
            return Err(meta.error("expected `config = Type`, `deps(name: Key, ...)`, or `provides(...)`"));
        }
        Ok(())
    }

    fn expand(self, input: &DeriveInput) -> TokenStream2 {
        let name = &input.ident;
        let vis = &input.vis;
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

        // Without dependencies the struct would be empty, so `Deps` is `()`.
        let (deps_type, deps_struct) = if deps.is_empty() {
            (quote!(()), quote!())
        } else {
            let deps_name = format_ident!("{}Deps", name);
            let doc = format!("Resolved dependencies of [`{name}`], one field per entry of its `deps(...)`.");
            let fields = deps.iter().map(|Dep { name, key }| {
                quote_spanned! {key.span()=> pub #name: ::std::sync::Arc<<#key as ::cordis::Key>::Value> }
            });
            let struct_def = quote! {
                #[doc = #doc]
                #vis struct #deps_name {
                    #(#fields),*
                }
            };
            (quote!(#deps_name), struct_def)
        };
        let resolve = deps.iter().map(|Dep { name, key }| quote! { #name: ::cordis::Erased::find::<#key>(resolved) });
        let resolve = if deps.is_empty() { quote!() } else { quote!(#deps_type { #(#resolve),* }) };
        let keys = deps.iter().map(|dep| &dep.key);
        let has = deps.iter().map(|Dep { name: field, key }| {
            quote_spanned! {key.span()=>
                impl #impl_generics ::cordis::Has<#key> for #name #ty_generics #where_clause {
                    fn dep(deps: &Self::Deps) -> &::std::sync::Arc<<#key as ::cordis::Key>::Value> {
                        &deps.#field
                    }
                }
            }
        });
        quote! {
            #deps_struct

            impl #impl_generics ::cordis::Manifest for #name #ty_generics #where_clause {
                type Config = #config;
                type Deps = #deps_type;

                fn deps() -> ::std::vec::Vec<::cordis::KeyId> {
                    ::std::vec![#(::cordis::KeyId::of::<#keys>()),*]
                }

                fn provides() -> ::std::vec::Vec<::cordis::KeyId> {
                    ::std::vec![#(::cordis::KeyId::of::<#provides>()),*]
                }

                #[allow(unused_variables)]
                fn resolve(resolved: &[(::cordis::KeyId, ::cordis::Erased)]) -> Self::Deps {
                    #resolve
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

/// One entry of `deps(...)`: the field name of the dependency and its key, `fs: dyn Fs`.
struct Dep {
    name: Ident,
    key: Type,
}

impl Parse for Dep {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        if !(input.peek(Ident) && input.peek2(Token![:]) && !input.peek2(Token![::])) {
            return Err(input.error("expected `name: Key`, for example `fs: dyn Fs`"));
        }
        let name = input.parse()?;
        input.parse::<Token![:]>()?;
        Ok(Self { name, key: input.parse()? })
    }
}

/// Parses a parenthesized, comma-separated list: `(dyn Tools, dyn Fs)` or `(fs: dyn Fs)`.
fn list<T: Parse>(meta: &ParseNestedMeta) -> syn::Result<Vec<T>> {
    let content;
    parenthesized!(content in meta.input);
    Ok(Punctuated::<T, Token![,]>::parse_terminated(&content)?.into_iter().collect())
}
