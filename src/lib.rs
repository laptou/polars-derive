//! # `polars-derive`
//!
//! Contains derive macros intended to be used with `polars` so that serializing
//! and deserializing Rust structures into `DataFrame`s is less manual.

use polars::prelude::*;
use polars_derive_impl;
pub use polars_derive_impl::{FromDataFrame, IntoDataFrame};

/// This trait allows the user to convert an iterator of a structure into a
/// [`DataFrame`].
///
/// # Attributes
/// The derive macro uses the attribute `#[df]` which can be attached to fields
/// and takes the following parameters:
///
///  - `#[df(dtype = <dtype>)]`: specifies the Polars data type of this column
///    explicitly. `<dtype>` should be a [`DataType`] (ex.: `UInt16`,
///    `List(Utf8)`)
///  - `#[df(into = <type>)]`: objects will be converted into `<type>` via
///    `.into()` before they are put into the `DataFrame`
///  - `#[df(as_ref = <type>)]`: objects will be converted into `<type>` via
///    `.as_ref()` before they are put into the `DataFrame`
///  - `#[df(serialize_with = <path>)]`: objects will be converted by calling
///    `<path>(item)` before they are put into the `DataFrame`. useful for
///    custom conversion methods
///  - `#[df(serialize_with_borrow = <path>)]`: similar to `serialize_with`, but
///    borrows the item instead. this is useful for methods that only allow &str
///    and not String, for example
///  - `#[df(optional = <bool>)]`: indicates explicitly whether the data in this
///    column is considered optional or not. will cause type errors if this does
///    not match the type of the field
/// 
/// If the data type is not specified explicitly using `#[df(dtype)]`, it will
/// be inferred from the type of the field. Fields can be `Option<T>`, but inner
/// `Option`s (ex.: `Vec<Option<T>>`) are currently unsupported.
pub trait IntoDataFrame {
    fn schema() -> Schema;

    fn into_series(rows: impl Iterator<Item = Self>) -> Vec<Series>;
    fn into_df(rows: impl Iterator<Item = Self>) -> PolarsResult<DataFrame> {
        DataFrame::new(Self::into_series(rows))
    }
}

/// This trait allows the user to convert a [`DataFrame`] into a list of a
/// structure.
///
/// # Attributes
/// The derive macro uses the attribute `#[df]` which can be attached to fields
/// and takes the following parameters:
///
///  - `#[df(dtype = <dtype>)]`: specifies the Polars data type of this column
///    explicitly. `<dtype>` should be a [`DataType`] (ex.: `UInt16`,
///    `List(Utf8)`)
///  - `#[df(try_from)]`: items in this column will be converted using `TryFrom`
///    when they are being read from the `DataFrame`
///  - `#[df(try_from_borrow)]`: items in this colum will be borrowed and
///    converted using `TryFrom` when they are being read from the `DataFrame`.
///    useful for types that implement `TryFrom<&str>`, but not
///    `TryFrom<String>`, for example
///  - `#[df(deserialize_with = <path>)]`: items in this column will be
///    converted using the method at `<path>` when they are being read from the
///    `DataFrame`
///  - `#[df(deserialize_with_borrow = <path>)]`: items in this column will be
///    borrowed converted using the method at `<path>` when they are being read
///    from the `DataFrame`
///  - `#[df(optional = <bool>)]`: indicates explicitly whether the data in this
///    column is considered optional or not. will cause type errors if this does
///    not match the type of the field
/// 
/// If the data type is not specified explicitly using `#[df(dtype)]`, it will
/// be inferred from the type of the field. Fields can be `Option<T>`, but inner
/// `Option`s (ex.: `Vec<Option<T>>`) are currently unsupported.
pub trait FromDataFrame: Sized {
    fn from_df(df: &DataFrame) -> PolarsResult<Vec<Self>>;
}
