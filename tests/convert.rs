use std::str::FromStr;
use polars_derive::{IntoDataFrame, FromDataFrame};
use url::Url;

#[derive(IntoDataFrame, FromDataFrame)]
struct TestStruct {
  #[df(into = String, try_from_borrow)]
  location: Url,
}
