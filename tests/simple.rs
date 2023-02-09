use polars_derive::{IntoDataFrame, FromDataFrame};

#[derive(IntoDataFrame, FromDataFrame)]
struct TestStruct {
  #[cfg(feature = "dtype-u8")]
  field1: u8,
  #[cfg(feature = "dtype-u16")]
  field2: u16,
  field3: u32,
  field4: u64,
  #[cfg(feature = "dtype-i8")]
  field5: i8,
  #[cfg(feature = "dtype-i16")]
  field6: i16,
  field7: i32,
  field8: i64,
  field9: String,
  field10: Option<String>,
  field11: Vec<String>,
}
