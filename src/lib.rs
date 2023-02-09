use polars_derive_impl;
pub use polars_derive_impl::IntoDataFrame;

pub trait IntoDataFrame {
    fn into_series(rows: impl Iterator<Item = Self>) -> Vec<polars::series::Series>;
    fn into_df(
        rows: impl Iterator<Item = Self>,
    ) -> Result<polars::frame::DataFrame, polars::error::PolarsError> {
        polars::frame::DataFrame::new(Self::into_series(rows))
    }
}

pub trait FromDataFrame: Sized {
    fn from_df(
        df: polars::frame::DataFrame,
    ) -> Result<Vec<Self>, polars::error::PolarsError>;
}
