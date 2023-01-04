extern crate proc_macro;
use polars::datatypes::DataType;
use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use syn::{parse::Parse, spanned::Spanned, Ident, ItemStruct, ExprStruct, ExprMethodCall};

#[proc_macro_derive(IntoDataFrame, attributes(dtype))]
pub fn derive_into_df(input: TokenStream) -> TokenStream {
    let input = TokenStream2::from(input);
    proc_macro::TokenStream::from(derive_into_df_inner(input))
}

struct DataFrameTemplate {
    cols: Vec<DataFrameTemplateColumn>,
}

impl Parse for DataFrameTemplate {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let structure: ItemStruct = input.parse()?;
        if structure.generics.type_params().count() > 0 {
            return Err(syn::Error::new(
                structure.generics.span(),
                "generic parameters are not allowed",
            ));
        }

        let mut cols = vec![];

        for field in structure.fields {
            let dtype = match field.attrs.iter().find(|attr| attr.path.is_ident("dtype")) {
                Some(attr) => get_dtype_for_spec(attr.parse_args::<ExprMethodCall>()?)?,
                None => get_dtype_for_rust_type(&field.ty)?,
            };
        }

        Ok(Self { cols })
    }
}

fn get_dtype_for_spec(spec: ExprMethodCall) -> syn::Result<DataType> {
    Ok(match spec.method.to_string().as_str() {
        "Boolean" => DataType::Boolean,
        "UInt8" => DataType::UInt8,
        "UInt16" => DataType::UInt16,
        "UInt32" => DataType::UInt32,
        "UInt64" => DataType::UInt64,
        "Int8" => DataType::Int8,
        "Int16" => DataType::Int16,
        "Int32" => DataType::Int32,
        "Int64" => DataType::Int64,
        "Float32" => DataType::Float32,
        "Float64" => DataType::Float64,
        "Utf8" => DataType::Utf8,
        "Date" => DataType::Date,
        "Time" => DataType::Time,
        "Null" => DataType::Null,
        "Unknown" => DataType::Unknown,
        _ => return Err(syn::Error::new(
            spec.span(),
            "invalid or not implemented",
        ))
    })
}

fn get_dtype_for_rust_type(ty: &syn::Type) -> syn::Result<DataType> {
    match ty {
        syn::Type::Array(arr) => match arr.len {
            #[cfg(feature = "dtype-struct")]
            syn::Expr::Lit(syn::ExprLit {
                lit: syn::Lit::Int(len),
                ..
            }) => {
                let len: usize = len.base10_parse()?;
                let element_dtype = get_dtype_for_rust_type(&*arr.elem)?;
                let fields = (0..len)
                    .map(|idx| polars::datatypes::Field::new(&idx.to_string(), element_dtype))
                    .collect();

                return Ok(DataType::Struct(fields));
            }
            _ => {
                return Ok(DataType::List(Box::new(get_dtype_for_rust_type(
                    &*arr.elem,
                )?)))
            }
        },
        syn::Type::BareFn(_) => {
            return Err(syn::Error::new(
                ty.span(),
                "this type does not have a polars equivalent",
            ))
        }
        syn::Type::Group(_) => todo!(),
        syn::Type::Macro(_) | syn::Type::Never(_) | syn::Type::Ptr(_) => {
            return Err(syn::Error::new(
                ty.span(),
                "cannot infer polars dtype from this, use an explicit #[dtype] attribute",
            ))
        }
        syn::Type::Paren(ty) => return get_dtype_for_rust_type(&*ty.elem),
        syn::Type::Path(ty) => {
            if let Some(ident) = ty.path.get_ident() {
                match ident.to_string().as_str() {
                    "String" | "str" => return Ok(DataType::Utf8),
                    "u8" => return Ok(DataType::UInt8),
                    "u16" => return Ok(DataType::UInt16),
                    "u32" => return Ok(DataType::UInt32),
                    "u64" => return Ok(DataType::UInt64),
                    "usize" => {
                        return Ok(if std::mem::size_of::<usize>() == 8 {
                            DataType::UInt64
                        } else {
                            DataType::UInt32
                        })
                    }
                    "i8" => return Ok(DataType::Int8),
                    "i16" => return Ok(DataType::Int16),
                    "i32" => return Ok(DataType::Int32),
                    "i64" => return Ok(DataType::Int64),
                    "isize" => {
                        return Ok(if std::mem::size_of::<isize>() == 8 {
                            DataType::Int64
                        } else {
                            DataType::Int32
                        })
                    }
                    "f32" => return Ok(DataType::Float32),
                    "f64" => return Ok(DataType::Float64),
                }
            }
        }
        syn::Type::Reference(_) => todo!(),
        syn::Type::Slice(_) => todo!(),
        syn::Type::Tuple(_) => todo!(),
        syn::Type::Verbatim(_) => todo!(),
        _ => {}
    };

    Err(syn::Error::new(
        ty.span(),
        "cannot infer polars dtype from this, use an explicit #[dtype] attribute",
    ))
}

fn derive_into_df_inner(input: TokenStream2) -> TokenStream2 {

}
