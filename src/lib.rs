use polars_derive_impl;

pub use polars_derive_impl::IntoDataFrame;

pub trait IntoPolars {
  type LogicalType;

  fn dtype() -> polars::datatypes::DataType;
  fn into_polars(&self) -> Self::LogicalType;
}
