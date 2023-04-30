use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;

extern crate proc_macro;

mod common;
mod from_df;
mod into_df;

#[proc_macro_derive(IntoDataFrame, attributes(df))]
pub fn derive_into_df(input: TokenStream) -> TokenStream {
    let input = TokenStream2::from(input);
    proc_macro::TokenStream::from(into_df::derive(input))
}

#[proc_macro_derive(FromDataFrame, attributes(df))]
pub fn derive_from_df(input: TokenStream) -> TokenStream {
    let input = TokenStream2::from(input);
    proc_macro::TokenStream::from(from_df::derive(input))
}
