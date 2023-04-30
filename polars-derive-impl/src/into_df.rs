use polars::prelude::DataType;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned, ToTokens};
use syn::spanned::Spanned;

use crate::common::{dtype_to_expr, rtype_for_dtype, ConvertInto, Template};

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
                    let target_ty = rtype_for_dtype(&field.dtype);

                    let target_ty = match &field.convert_into {
                        Some(ConvertInto::AsRef(ty)) => ty,
                        Some(ConvertInto::Into(ty)) => ty,
                        Some(ConvertInto::Custom { .. }) => &target_ty,
                        None => &field.ty,
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
                    let field_name = quote::format_ident!("{}", name);

                    let converter = match &field.convert_into {
                        Some(ConvertInto::AsRef(_)) => quote! { item.#field_name.as_ref() },
                        Some(ConvertInto::Into(_)) => quote! { item.#field_name.into() },
                        Some(ConvertInto::Custom { fun, borrow }) => {
                            if *borrow {
                                quote! { #fun(&item.#field_name) }
                            } else {
                                quote! { #fun(item.#field_name) }
                            }
                        }
                        None => quote! { item.#field_name },
                    };

                    quote_spanned! {field.span=>
                        #var_name.push(#converter);
                    }
                });

        let series_decls = template
            .fields
            .iter()
            .zip(&field_vector_names)
            .map(|(field, var_name)| vec_to_series(&field.name, var_name, &field.dtype));

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

    let field_schema_decls = template.fields.iter().map(|field| {
        let field_name = &field.name;
        let dtype = dtype_to_expr(&field.dtype);
        quote_spanned! {field.ty.span()=>
          ::polars::datatypes::Field::new(#field_name, #dtype)
        }
    });

    quote! {
        impl ::polars_derive::IntoDataFrame for #name {
            fn schema() -> ::polars::prelude::Schema {
              ::polars::prelude::Schema::from([
                #(#field_schema_decls),*
              ].into_iter())
            }

            fn into_series(rows: impl Iterator<Item = Self>) -> Vec<::polars::series::Series> {
                #series_impl
            }
        }
    }
}

fn vec_to_series(name: &str, inner: impl ToTokens, dtype: &DataType) -> TokenStream2 {
    match dtype {
        DataType::Boolean
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date
        | DataType::Datetime(_, _)
        | DataType::Duration(_)
        | DataType::Binary
        | DataType::Time => {
            // scalar data types are simple
            quote_spanned! {inner.span()=>
                {
                    let v = #inner;
                    <::polars::series::Series as ::polars::prelude::NamedFrom<_, _>>::new(
                      #name,
                      v.as_slice()
                    )
                }
            }
        }
        DataType::List(inner_dtype) => {
            // for list types, recurse
            let local = format_ident!("i");
            let inner_converter = vec_to_series(name, local.clone(), &*inner_dtype);

            quote_spanned! {inner.span()=>
                {
                    let v = #inner.into_iter().map(|#local| #inner_converter).collect::<Vec<::polars::series::Series>>();
                    <::polars::series::Series as ::polars::prelude::NamedFrom<_, _>>::new(
                      #name,
                      v.as_slice()
                    )
                }
            }
        }
        DataType::Null => todo!(),
        DataType::Struct(_) => todo!(),
        DataType::Unknown => todo!(),
    }
}
