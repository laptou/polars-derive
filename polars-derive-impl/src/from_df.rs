use polars::prelude::DataType;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};

use crate::common::Template;

pub fn derive(input: TokenStream2) -> TokenStream2 {
    let template: Template = match syn::parse2(input) {
        Ok(template) => template,
        Err(err) => return err.to_compile_error().into(),
    };

    let structure = template.structure;
    let name = structure.ident.clone();

    let out = format_ident!("out");
    let df = format_ident!("df");

    let field_iters: Vec<_> = template
        .fields
        .iter()
        .map(|field| (field, format_ident!("i_{}", field.name)))
        .collect();

    let field_iter_ids = template.fields.iter().map(|field| {
        let col_name = &field.name;
        format_ident!("c_{}", col_name)
    });

    let field_iter_inits = template.fields.iter().map(|field| {
        let col_name = &field.name;
        let var_name = format_ident!("c_{}", col_name);

        let dtype_method = match field.dtype {
            DataType::Boolean => "bool",
            DataType::UInt8 => "u8",
            DataType::UInt16 => "u16",
            DataType::UInt32 => "u32",
            DataType::UInt64 => "u64",
            DataType::Int8 => "i8",
            DataType::Int16 => "i16",
            DataType::Int32 => "i32",
            DataType::Int64 => "i64",
            DataType::Float32 => "f32",
            DataType::Float64 => "f64",
            DataType::Utf8 => "utf8",
            DataType::Date => "date",
            DataType::Datetime(_, _) => "datetime",
            DataType::Duration(_) => "duration",
            DataType::Time => "time",
            DataType::List(_) => "list",
            _ => unimplemented!("dtype not implemented"),
        };
        let dtype_method = format_ident!("{}", dtype_method);

        quote! { let mut #var_name = #df.column(#col_name).#dtype_method()?.into_iter(); }
    });

    let field_iter_pats = template.fields.iter().map(|field| {
        let col_name = &field.name;
        let var_name = format_ident!("i_{}", col_name);

        quote! { Some(#var_name) }
    });

    let field_iter_getters = template.fields.iter().map(|field| {
        let col_name = &field.name;
        let value_name = format_ident!("{}", col_name);
        let pat_name = format_ident!("i_{}", col_name);

        quote_spanned! {field.span=>
            #value_name: #pat_name
        }
    });

    quote! {
        impl ::polars_derive::FromDataFrame for #name {
          fn from_df(
            #df: polars::frame::DataFrame,
          ) -> Result<Self, ::polars::error::PolarsError> {
            let #out = vec![];

            #(#field_iter_inits)*

            while let (#(#field_iter_pats),*) = (#(#field_iter_ids),*) {
              #out.push(Self {
                #(#field_iter_getters),*
              })
            }

            Ok(#out)
          }
        }
    }
}
