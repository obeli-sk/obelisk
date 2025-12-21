use proc_macro::TokenStream;
use quote::quote;
use syn::{Ident, ItemFn, parse_macro_input};

#[proc_macro_attribute]
pub fn expand_enum_database(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut func = parse_macro_input!(item as ItemFn);

    // Hardcoded variants
    let variants = ["Sqlite", "Memory", "Postgres"];

    // Apply #[values(...)] to the first argument
    if let Some(arg) = func.sig.inputs.iter_mut().next() {
        if let syn::FnArg::Typed(pat) = arg {
            let values = variants.iter().map(|v| {
                let ident = Ident::new(v, proc_macro::Span::call_site().into());
                quote! { Database::#ident }
            });
            pat.attrs.push(syn::parse_quote!(#[values(#(#values),*)]));
        }
    }

    TokenStream::from(quote!(#func))
}
