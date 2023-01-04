extern crate proc_macro;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote, quote_spanned};
use syn::{parse::Parse, spanned::Spanned, Ident, ItemStruct, Token};

#[proc_macro_derive(IntoDataFrame, attributes(df))]
pub fn derive_into_df(input: TokenStream) -> TokenStream {
    let input = TokenStream2::from(input);
    proc_macro::TokenStream::from(derive_into_df_inner(input))
}

struct DataFrameTemplate {
    fields: Vec<DataFrameTemplateColumn>,
    structure: ItemStruct,
}

struct DataFrameTemplateOptions(Vec<DataFrameTemplateOption>);

impl Parse for DataFrameTemplateOptions {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let p = syn::punctuated::Punctuated::<DataFrameTemplateOption, Token![,]>::parse_separated_nonempty(input)?;
        Ok(Self(p.into_iter().collect()))
    }
}

enum DataFrameTemplateOption {
    ConvertTo(syn::Type),
}

impl Parse for DataFrameTemplateOption {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        let id: Ident = input.parse()?;

        match id.to_string().as_str() {
            "convert_to" => {
                let ty: syn::TypeParen = input.parse()?;
                Ok(Self::ConvertTo(*ty.elem))
            }
            _ => Err(syn::Error::new(input.span(), "invalid attribute parameter")),
        }
    }
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

        for (idx, field) in structure.fields.iter().enumerate() {
            let mut target_ty = None;

            for attr in &field.attrs {
                if !attr.path.is_ident("df") {
                    continue;
                }

                let opts: DataFrameTemplateOptions = attr.parse_args()?;

                for opt in opts.0 {
                    match opt {
                        DataFrameTemplateOption::ConvertTo(ty) => target_ty = Some(ty),
                    }
                }
            }

            cols.push(DataFrameTemplateColumn {
                name: field
                    .ident
                    .as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| idx.to_string()),
                source_ty: field.ty.clone(),
                target_ty,
                span: field.span(),
            })
        }

        Ok(Self {
            fields: cols,
            structure,
        })
    }
}

struct DataFrameTemplateColumn {
    name: String,
    span: proc_macro2::Span,
    source_ty: syn::Type,
    /// type that value must be converted into before Polars will accept it
    target_ty: Option<syn::Type>,
}

fn derive_into_df_inner(input: TokenStream2) -> TokenStream2 {
    let template: DataFrameTemplate = match syn::parse2(input) {
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
                    let ty = field.target_ty.as_ref().unwrap_or(&field.source_ty);
                    quote! { let mut #var_name: Vec<#ty> = vec![]; }
                });

        let field_vector_fillers =
            template
                .fields
                .iter()
                .zip(&field_vector_names)
                .map(|(field, var_name)| {
                    let name = &field.name;
                    let name_id = quote::format_ident!("{}", name);

                    quote_spanned! {field.span=>
                        #var_name.push(item.#name_id.into());
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
        impl IntoDataFrame for #name {
            fn into_series(rows: impl Iterator<Item = Self>) -> Vec<::polars::series::Series> {
                #series_impl
            }
        }
    }
}
