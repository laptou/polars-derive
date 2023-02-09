use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};

use crate::common::{Convert, Template};

pub fn derive(input: TokenStream2) -> TokenStream2 {
    let template: Template = match syn::parse2(input) {
        Ok(template) => template,
        Err(err) => return err.to_compile_error().into(),
    };

    let structure = template.structure;
    let name = structure.ident.clone();

    let series_impl = {
        let field_vector_names: Vec<_> = template
            .fields
            .iter()
            .map(|field| format_ident!("v_{}", field.name))
            .collect();

        let field_vector_decls =
            template
                .fields
                .iter()
                .zip(&field_vector_names)
                .map(|(field, var_name)| {
                    let target_ty = match &field.convert {
                        Some(Convert::AsRef(ty)) => ty,
                        Some(Convert::Into(ty)) => ty,
                        None => todo!(),
                    };

                    quote! { let mut #var_name: Vec<#target_ty> = vec![]; }
                });

        let field_vector_fillers =
            template
                .fields
                .iter()
                .zip(&field_vector_names)
                .map(|(field, var_name)| {
                    let name = &field.name;
                    let name_id = quote::format_ident!("{}", name);

                    let converter = match &field.convert {
                        Some(Convert::AsRef(ty)) => Some(quote! { item.#name_id.as_ref() }),
                        Some(Convert::Into(ty)) => Some(quote! { item.#name_id.into() }),
                        None => None,
                    };

                    quote_spanned! {field.span=>
                        #var_name.push(#converter);
                    }
                });

        let series_decls =
            template
                .fields
                .iter()
                .zip(&field_vector_names)
                .map(|(field, var_name)| {
                    let name = &field.name;

                    quote_spanned! {field.span=>
                        <::polars::series::Series as ::polars::prelude::NamedFrom<_, _>>::new(
                            #name,
                            #var_name.as_slice()
                        )
                    }
                });

        quote! {
            #(#field_vector_decls)*

            for item in rows {
                #(#field_vector_fillers)*
            }

            vec![
                #(#series_decls),*
            ]
        }
    };

    quote! {
        impl ::polars_derive::IntoDataFrame for #name {
            fn into_series(rows: impl Iterator<Item = Self>) -> Vec<::polars::series::Series> {
                #series_impl
            }
        }
    }
}
