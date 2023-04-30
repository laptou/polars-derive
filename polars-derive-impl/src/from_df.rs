use polars::prelude::DataType;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned, ToTokens};

use crate::common::{ConvertFrom, Template};

pub fn derive(input: TokenStream2) -> TokenStream2 {
    let template: Template = match syn::parse2(input) {
        Ok(template) => template,
        Err(err) => return err.to_compile_error().into(),
    };

    let structure = template.structure;
    let name = structure.ident.clone();

    let out = format_ident!("out");
    let df = format_ident!("df");

    let field_iter_ids = template.fields.iter().map(|field| {
        let col_name = &field.name;
        format_ident!("c_{}", col_name)
    });

    let field_iter_inits = template.fields.iter().map(|field| {
        let col_name = &field.name;
        let var_name = format_ident!("c_{}", col_name);

        let col_expr = quote! { #df.column(#col_name)? };
        let col_expr = series_to_rtype(col_expr, &field.dtype);

        quote_spanned! {field.span=> let mut #var_name = #col_expr.into_iter(); }
    });

    let field_iter_pats = template.fields.iter().map(|field| {
        let col_name = &field.name;
        let var_name = format_ident!("i_{}", col_name);

        quote_spanned! {field.span=> Some(#var_name) }
    });

    let field_iter_getters = template.fields.iter().map(|field| {
        let col_name = &field.name;
        let value_name = format_ident!("{}", col_name);
        let pat_name = format_ident!("i_{}", col_name);

        let getter = item_to_rtype(col_name, pat_name, &field.dtype, field.optional);

        let getter = match &field.convert_from {
          Some(ConvertFrom::TryFrom { borrow }) => {
            let getter = if *borrow { quote! { ::std::borrow::Borrow::borrow(&#getter) } } else { getter };
            let ty = &field.ty;

            quote! { 
              <#ty as TryFrom<_>>::try_from(#getter)
                .map_err(|err| ::polars::error::PolarsError::SchemaMismatch(::polars::error::ErrString::from(err.to_string())))?
            }
          },
          Some(ConvertFrom::Custom { fun, borrow }) => {
            let getter = if *borrow { quote! { &#getter } } else { getter };
            quote! {
              #fun(#getter).map_err(|err| ::polars::error::PolarsError::SchemaMismatch(::polars::error::ErrString::from(err.to_string())))?
            }
          },
          None => getter,
        };

        // we throw on a little .into() b/c it's a no-op when it is not needed
        quote_spanned! {field.span=>
            #value_name: #getter
        }
    });

    quote! {
        impl ::polars_derive::FromDataFrame for #name {
          fn from_df(
            #df: &polars::frame::DataFrame,
          ) -> Result<Vec<Self>, ::polars::error::PolarsError> {
            let mut #out = vec![];

            #(#field_iter_inits)*

            while let (#(#field_iter_pats),*) = (#(#field_iter_ids.next()),*) {
              #out.push(Self {
                #(#field_iter_getters),*
              })
            }

            Ok(#out)
          }
        }
    }
}

/// Returns Rust code which will convert a Polars Series into a Polars
/// ChunkedArray, which is necessary to get an iterator with a specific item
/// data type.
fn series_to_rtype(inner: impl ToTokens, dtype: &DataType) -> TokenStream2 {
    let dtype_method = match dtype {
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
        DataType::Binary => "binary",
        DataType::List(inner_dtype) => {
            let local = format_ident!("l");
            let inner_converter = series_to_rtype(local.clone(), &*inner_dtype);
            // need to clone b/c otherwise we will get a lifetime error related
            // to #local (methods like .utf8() are &self)
            return quote! { #inner.list()?.into_iter().map(|i| { i.map(|#local| { Ok(#inner_converter.clone()) }) }) };
        }
        _ => unimplemented!("dtype not implemented"),
    };

    let dtype_method = format_ident!("{}", dtype_method);

    quote! { #inner.#dtype_method()? }
}

/// Returns Rust code which will unwrap the data as extracted from the Polars
/// Series. Useful for list data types, optional data, etc.
fn item_to_rtype(
    name: &str,
    inner: impl ToTokens,
    dtype: &DataType,
    optional: bool,
) -> TokenStream2 {
    // if necessary, we will run the Option through a closure that converts it
    // to the desired type using map()

    // this is a Option((TokenStream, bool)) where the bool represents whether the converter is fallible
    let converter = match dtype {
        DataType::List(inner_dtype) => {
            let local = format_ident!("i");
            let inner_name = format!("{name}.<item>");
            let inner_converter = item_to_rtype(&inner_name, local.clone(), &*inner_dtype, false);

            // our iterator gives a Result b/c converting the individual items
            // of this List might have failed; so we need to map through the
            // result

            // we clone the inner item b/c it's referencing #local and we get
            // lifetime issues but this is cheap since Series is a wrapper
            // around Arc anyway
            Some((
                quote! {
                    |r| r.and_then(|l| {
                        l.into_iter()
                            .map(|#local| -> ::polars::error::PolarsResult<_> { Ok(#inner_converter) })
                            .collect::<Result<Vec<_>, _>>()
                    })
                },
                true,
            ))
        }
        DataType::Utf8 => {
            // for now we only support owned strings for FromDataFrame
            // Polars gives us a &str so we call to_owned()
            Some((quote! { |s| s.to_owned() }, false))
        }
        DataType::Binary => {
            // convert &[u8] to Vec<u8>
            Some((quote! { |s| Vec::from(s) }, false))
        }
        _ => None,
    };

    let getter = if let Some((converter, fallible)) = converter {
        if fallible {
            // converter could have failed, so we call transpose() to get the
            // error out if there is one
            quote! { #inner.map(#converter).transpose()? }
        } else {
            quote! { #inner.map(#converter) }
        }
    } else {
        inner.to_token_stream()
    };

    let getter = if optional {
        quote! {
          #getter
        }
    } else {
        let error_msg = format!("unexpected missing data for field {name}");
        quote! {
          #getter.ok_or(::polars::error::PolarsError::NoData(::polars::error::ErrString::from(#error_msg)))?
        }
    };

    getter
}
