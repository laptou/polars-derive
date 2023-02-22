use polars::prelude::{DataType, Field, TimeUnit};
use proc_macro2::TokenStream;
use syn::{parse::Parse, spanned::Spanned, Ident, ItemStruct, Token};

pub struct Template {
    pub fields: Vec<Column>,
    pub structure: ItemStruct,
}

impl Parse for Template {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let structure: ItemStruct = input.parse()?;
        if structure.generics.type_params().count() > 0 {
            return Err(syn::Error::new(
                structure.generics.span(),
                "generic parameters are not allowed",
            ));
        }

        let mut cols = vec![];

        for (idx, field) in structure.fields.iter().enumerate() {
            let mut dtype = None;
            let mut convert_into = None;
            let mut convert_from = None;
            let mut optional = false;

            for attr in &field.attrs {
                if !attr.path.is_ident("df") {
                    continue;
                }

                let opts: Attr = attr.parse_args()?;

                for opt in opts.0 {
                    match opt {
                        AttrOption::Into(ty) => convert_into = Some(ConvertInto::Into(ty)),
                        AttrOption::AsRef(ty) => convert_into = Some(ConvertInto::AsRef(ty)),
                        AttrOption::Dtype(dt) => dtype = Some(dt),
                        AttrOption::Optional(opt) => optional = opt,
                        AttrOption::TryFrom { borrow } => {
                            convert_from = Some(ConvertFrom::TryFrom { borrow })
                        }
                        AttrOption::SerializeWith { fun, borrow } => {
                            convert_into = Some(ConvertInto::Custom { fun, borrow })
                        }
                        AttrOption::DeserializeWith { fun, borrow } => {
                            convert_from = Some(ConvertFrom::Custom { fun, borrow })
                        }
                    }
                }
            }

            if dtype.is_none() {
                if let Some(convert_into) = &convert_into {
                    match convert_into {
                        ConvertInto::Into(ty) | ConvertInto::AsRef(ty) => {
                            let (dt, opt) = dtype_for_rtype_opt(ty)?;
                            dtype = Some(dt);
                            optional = opt;
                        }
                        ConvertInto::Custom { .. } => {
                            // dtype must be specified explicitly if using a
                            // custom conversion function
                        }
                    }
                }
            }

            if dtype.is_none() {
                let (dt, opt) = dtype_for_rtype_opt(&field.ty)?;
                dtype = Some(dt);
                optional = opt;
            }

            cols.push(Column {
                span: field.span(),
                name: field
                    .ident
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| idx.to_string()),
                ty: field.ty.clone(),
                dtype: dtype.unwrap(),
                convert_into,
                convert_from,
                optional,
            })
        }

        Ok(Self {
            fields: cols,
            structure,
        })
    }
}

pub struct Column {
    pub name: String,
    pub span: proc_macro2::Span,

    pub ty: syn::Type,
    pub dtype: DataType,
    pub optional: bool,
    /// mechanism to use when converting from user struct into Polars format
    pub convert_into: Option<ConvertInto>,
    /// mechanism to use when converting from Polars format into user struct
    pub convert_from: Option<ConvertFrom>,
}

pub struct Attr(Vec<AttrOption>);

impl Parse for Attr {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let p =
            syn::punctuated::Punctuated::<AttrOption, Token![,]>::parse_separated_nonempty(input)?;
        Ok(Self(p.into_iter().collect()))
    }
}

pub enum AttrOption {
    Into(syn::Type),
    AsRef(syn::Type),
    Dtype(DataType),
    Optional(bool),
    TryFrom { borrow: bool },
    SerializeWith { fun: syn::ExprPath, borrow: bool },
    DeserializeWith { fun: syn::ExprPath, borrow: bool },
}

pub enum ConvertInto {
    Into(syn::Type),
    AsRef(syn::Type),
    /// a custom user-supplied conversion function
    Custom {
        fun: syn::ExprPath,
        borrow: bool,
    },
}

pub enum ConvertFrom {
    TryFrom {
        borrow: bool,
    },
    /// a custom user-supplied conversion function
    Custom {
        fun: syn::ExprPath,
        borrow: bool,
    },
}

impl Parse for AttrOption {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let id: Ident = input.parse()?;

        match id.to_string().as_str() {
            "into" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let ty: syn::Type = input.parse()?;
                Ok(Self::Into(ty))
            }
            "as_ref" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let ty: syn::Type = input.parse()?;
                Ok(Self::AsRef(ty))
            }
            "dtype" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let expr: syn::Expr = input.parse()?;
                Ok(Self::Dtype(expr_to_dtype(&expr)?))
            }
            "optional" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let b: syn::LitBool = input.parse()?;
                Ok(Self::Optional(b.value))
            }
            "serialize_with" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let fun = input.parse()?;
                Ok(Self::SerializeWith { fun, borrow: false })
            }
            "deserialize_with" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let fun = input.parse()?;
                Ok(Self::DeserializeWith { fun, borrow: false })
            }
            "serialize_with_borrow" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let fun = input.parse()?;
                Ok(Self::SerializeWith { fun, borrow: true })
            }
            "deserialize_with_borrow" => {
                let _ = input.parse::<Token![=]>()?; // skip '='
                let fun = input.parse()?;
                Ok(Self::DeserializeWith { fun, borrow: true })
            }
            "try_from" => Ok(Self::TryFrom { borrow: false }),
            "try_from_borrow" => Ok(Self::TryFrom { borrow: true }),
            _ => Err(syn::Error::new(id.span(), "invalid attribute parameter")),
        }
    }
}

/// Gets the corresponding Polars [`DataType`] for a given Rust type.
fn dtype_for_rtype(ty: &syn::Type) -> syn::Result<DataType> {
    match ty {
        syn::Type::Path(ty) => {
            if let Some(id) = ty.path.get_ident() {
                // type with no type params
                match id.to_string().as_str() {
                    "u8" => return Ok(DataType::UInt8),
                    "u16" => return Ok(DataType::UInt16),
                    "u32" => return Ok(DataType::UInt32),
                    "u64" => return Ok(DataType::UInt64),
                    "i8" => return Ok(DataType::Int8),
                    "i16" => return Ok(DataType::Int16),
                    "i32" => return Ok(DataType::Int32),
                    "i64" => return Ok(DataType::Int64),
                    "bool" => return Ok(DataType::Boolean),
                    "f32" => return Ok(DataType::Float32),
                    "f64" => return Ok(DataType::Float64),
                    "String" | "str" => return Ok(DataType::Utf8),
                    "NaiveDateTime" => return Ok(DataType::Datetime(TimeUnit::Milliseconds, None)),
                    _ => {}
                }
            } else if ty.path.leading_colon.is_none() && ty.path.segments.len() == 1 {
                // type with some type params
                let id = &ty.path.segments[0].ident;

                match id.to_string().as_str() {
                    "Vec" => {
                        let args = &ty.path.segments[0].arguments;

                        if let syn::PathArguments::AngleBracketed(args) = args {
                            if args.args.len() == 1 {
                                if let Some(syn::GenericArgument::Type(ty)) = args.args.first() {
                                    return Ok(DataType::List(Box::new(dtype_for_rtype(ty)?)));
                                }
                            } else {
                                return Err(syn::Error::new_spanned(
                                    args,
                                    "invalid arguments for Vec",
                                ));
                            }
                        } else {
                            return Err(syn::Error::new_spanned(args, "invalid arguments for Vec"));
                        }
                    }

                    // to support chrono
                    "DateTime" => {
                        let args = &ty.path.segments[0].arguments;

                        if let syn::PathArguments::AngleBracketed(args) = args {
                            if args.args.len() == 1 {
                                if let Some(syn::GenericArgument::Type(syn::Type::Path(_))) =
                                    args.args.first()
                                {
                                    return Ok(DataType::Datetime(
                                        TimeUnit::Milliseconds,
                                        // actual time zone will be filled in later
                                        Some("PLACEHOLDER".to_owned()),
                                    ));
                                }
                            }
                        }
                    }

                    _ => {}
                }
            }
        }
        syn::Type::Tuple(ty) => {
            // map tuples to a struct where fields are named 0, 1, and so on
            let fields: Vec<Field> = ty
                .elems
                .iter()
                .enumerate()
                .map(|(idx, elem)| -> syn::Result<Field> {
                    Ok(Field::new(idx.to_string().as_str(), dtype_for_rtype(elem)?))
                })
                .collect::<syn::Result<Vec<Field>>>()?;

            return Ok(DataType::Struct(fields));
        }
        syn::Type::Array(ty) => return Ok(DataType::List(Box::new(dtype_for_rtype(&*ty.elem)?))),
        syn::Type::Slice(ty) => return Ok(DataType::List(Box::new(dtype_for_rtype(&*ty.elem)?))),
        syn::Type::Reference(ty) => return dtype_for_rtype(&*ty.elem),
        syn::Type::Paren(ty) => return dtype_for_rtype(&*ty.elem),
        _ => {}
    };

    return Err(syn::Error::new_spanned(
        ty,
        "unknown type, please specify dtype explicitly",
    ));
}

/// Gets the corresponding Polars [`DataType`] for a given Rust type. Allows
/// `Option`, returns a tuple with a `DataType` and a bool indicating whether
/// the Rust type was optional or not.
fn dtype_for_rtype_opt(ty: &syn::Type) -> syn::Result<(DataType, bool)> {
    if let syn::Type::Path(ty) = ty {
        if ty.path.leading_colon.is_none() && ty.path.segments.len() == 1 {
            // type with some type params
            let id = &ty.path.segments[0].ident;
            if id == "Option" {
                let args = &ty.path.segments[0].arguments;

                if let syn::PathArguments::AngleBracketed(args) = args {
                    if args.args.len() == 1 {
                        if let Some(syn::GenericArgument::Type(ty)) = args.args.first() {
                            return Ok((dtype_for_rtype(ty)?, true));
                        }
                    }
                }

                return Err(syn::Error::new_spanned(
                    args,
                    "invalid arguments for Option",
                ));
            }
        }
    };

    Ok((dtype_for_rtype(ty)?, false))
}

pub(crate) fn expr_to_dtype(ex: &syn::Expr) -> syn::Result<DataType> {
    match ex {
        syn::Expr::Call(syn::ExprCall { func, args, .. }) => {
            if let syn::Expr::Path(callee) = &**func {
                if callee.path.is_ident("List") && args.len() == 1 {
                    return Ok(DataType::List(Box::new(expr_to_dtype(
                        &*args.first().unwrap(),
                    )?)));
                }

                if callee.path.is_ident("Datetime") {
                    match args.len() {
                        1 => {
                            return Ok(DataType::Datetime(
                                match &args[0] {
                                    syn::Expr::Path(p) => {
                                        let id = p.path.get_ident();
                                        let id = id.map(|i| i.to_string());
                                        let id = id.as_deref();

                                        match id {
                                            Some("Milliseconds") => TimeUnit::Milliseconds,
                                            Some("Microseconds") => TimeUnit::Microseconds,
                                            Some("Nanoseconds") => TimeUnit::Nanoseconds,
                                            _ => {
                                                return Err(syn::Error::new_spanned(
                                                    p,
                                                    "invalid time unit",
                                                ))
                                            }
                                        }
                                    }
                                    other => {
                                        return Err(syn::Error::new_spanned(
                                            other,
                                            "invalid time unit",
                                        ))
                                    }
                                },
                                None,
                            ))
                        }
                        _ => return Err(syn::Error::new_spanned(args, "Datetime takes 1 argument"))
                    }
                }
            }
        }

        syn::Expr::Path(p) => {
            if let Some(id) = p.path.get_ident() {
                match id.to_string().as_str() {
                    "Boolean" => return Ok(DataType::Boolean),
                    "UInt8" => return Ok(DataType::UInt8),
                    "UInt16" => return Ok(DataType::UInt16),
                    "UInt32" => return Ok(DataType::UInt32),
                    "UInt64" => return Ok(DataType::UInt64),
                    "Int8" => return Ok(DataType::Int8),
                    "Int16" => return Ok(DataType::Int16),
                    "Int32" => return Ok(DataType::Int32),
                    "Int64" => return Ok(DataType::Int64),
                    "Float32" => return Ok(DataType::Float32),
                    "Float64" => return Ok(DataType::Float64),
                    "Utf8" => return Ok(DataType::Utf8),
                    "Date" => return Ok(DataType::Date),
                    "Time" => return Ok(DataType::Time),
                    "Null" => return Ok(DataType::Null),
                    "Unknown" => return Ok(DataType::Unknown),
                    #[cfg(feature = "dtype-binary")]
                    "Binary" => return Ok(DataType::Binary),
                    _ => {}
                }
            }
        }

        _ => {}
    }

    return Err(syn::Error::new_spanned(ex, "invalid dtype"));
}

pub(crate) fn dtype_to_expr(dtype: &DataType) -> TokenStream {
    use quote::quote;
    match dtype {
        DataType::Boolean => quote! { ::polars::datatypes::DataType::Boolean },
        DataType::UInt8 => quote! { ::polars::datatypes::DataType::UInt8 },
        DataType::UInt16 => quote! { ::polars::datatypes::DataType::UInt16 },
        DataType::UInt32 => quote! { ::polars::datatypes::DataType::UInt32 },
        DataType::UInt64 => quote! { ::polars::datatypes::DataType::UInt64 },
        DataType::Int8 => quote! { ::polars::datatypes::DataType::Int8 },
        DataType::Int16 => quote! { ::polars::datatypes::DataType::Int16 },
        DataType::Int32 => quote! { ::polars::datatypes::DataType::Int32 },
        DataType::Int64 => quote! { ::polars::datatypes::DataType::Int64 },
        DataType::Float32 => quote! { ::polars::datatypes::DataType::Float32 },
        DataType::Float64 => quote! { ::polars::datatypes::DataType::Float64 },
        DataType::Utf8 => quote! { ::polars::datatypes::DataType::Utf8 },
        DataType::Date => quote! { ::polars::datatypes::DataType::Date },
        DataType::Datetime(tu, tz) => {
            let tu = time_unit_to_expr(*tu);
            let tz = match tz {
                Some(tz) => quote! { Some(#tz) },
                None => quote! { None },
            };
            quote! { ::polars::datatypes::DataType::Datetime(#tu, #tz) }
        }
        DataType::Duration(tu) => {
            let tu = time_unit_to_expr(*tu);
            quote! { ::polars::datatypes::DataType::Duration(#tu) }
        }
        DataType::Time => quote! { ::polars::datatypes::DataType::Time },
        DataType::List(inner) => {
            let inner = dtype_to_expr(&*inner);
            quote! { ::polars::datatypes::DataType::List(::std::boxed::Box::new(#inner)) }
        }
        DataType::Null => quote! { ::polars::datatypes::DataType::Null },
        DataType::Unknown => quote! { ::polars::datatypes::DataType::Unknown },
        _ => unimplemented!(),
    }
}

fn time_unit_to_expr(tu: TimeUnit) -> TokenStream {
    use quote::quote;
    match tu {
        TimeUnit::Nanoseconds => quote! { ::polars::datatypes::TimeUnit::Nanoseconds },
        TimeUnit::Microseconds => quote! { ::polars::datatypes::TimeUnit::Microseconds },
        TimeUnit::Milliseconds => quote! { ::polars::datatypes::TimeUnit::Milliseconds },
    }
}
