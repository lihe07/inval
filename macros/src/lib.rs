//! Attribute macros for `inval`. Use them through that crate.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::parse::{Parse, ParseStream};
use syn::{Error, Expr, Ident, ItemFn, LitStr, ReturnType, Token, parse_macro_input};

/// `#[query("user:{id}")]` caches an `async fn` returning `Result<T, E>`; it
/// then returns `Result<Arc<T>, E>`. The key may name the function's
/// arguments. `E` must implement `From<inval::Error>`.
///
/// Uses the global cache, or `#[query("user:{id}", cache = self.cache)]`.
#[proc_macro_attribute]
pub fn query(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, Kind::Query)
}

/// `#[mutation("user:{id}")]` invalidates the key after the function returns
/// `Ok`. Takes the same arguments as [`query`].
#[proc_macro_attribute]
pub fn mutation(args: TokenStream, item: TokenStream) -> TokenStream {
    expand(args, item, Kind::Mutation)
}

enum Kind {
    Query,
    Mutation,
}

struct Args {
    key: LitStr,
    cache: Option<Expr>,
}

impl Parse for Args {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let key = input.parse()?;
        let mut cache = None;
        if !input.is_empty() {
            input.parse::<Token![,]>()?;
            let name: Ident = input.parse()?;
            if name != "cache" {
                return Err(Error::new_spanned(
                    name,
                    "unexpected argument, expected `cache = ...`",
                ));
            }
            input.parse::<Token![=]>()?;
            cache = Some(input.parse()?);
        }
        Ok(Self { key, cache })
    }
}

/// Wraps the function body in `cache.<method>(key, || async move { body })`.
fn expand(args: TokenStream, item: TokenStream, kind: Kind) -> TokenStream {
    let Args { key, cache } = parse_macro_input!(args as Args);
    let ItemFn {
        attrs,
        vis,
        mut sig,
        block,
    } = parse_macro_input!(item as ItemFn);

    let ReturnType::Type(arrow, result) = sig.output.clone() else {
        return Error::new_spanned(sig, "the function must return a `Result`")
            .to_compile_error()
            .into();
    };
    if sig.asyncness.is_none() {
        return Error::new_spanned(sig, "the function must be `async`")
            .to_compile_error()
            .into();
    }

    let (method, output) = match kind {
        Kind::Query => (format_ident!("query"), format_ident!("Query")),
        Kind::Mutation => (format_ident!("mutation"), format_ident!("Mutation")),
    };
    sig.output = syn::parse_quote!(#arrow <#result as ::inval::__macro::Wrapped>::#output);
    let cache = match cache {
        Some(cache) => quote!(&#cache),
        None => quote!(::inval::global()),
    };
    let key = if key.value().contains(['{', '}']) {
        quote!(&::std::format!(#key))
    } else {
        quote!(#key)
    };

    quote! {
        #(#attrs)* #vis #sig {
            let __cache: &::inval::Cache = #cache;
            let __key: &str = #key;
            __cache.#method(__key, || ::inval::__macro::returning::<#result, _>(async move #block)).await
        }
    }
    .into()
}
