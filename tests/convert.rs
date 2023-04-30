use std::borrow::Borrow;
use std::io::Cursor;
use std::str::FromStr;

use polars::export::chrono::{Local, NaiveDateTime, Timelike};
use polars::prelude::*;
use polars_derive::{FromDataFrame, IntoDataFrame};
use url::Url;

#[derive(IntoDataFrame, FromDataFrame, Clone, PartialEq, Eq, Debug)]
struct TestStruct {
    #[df(into = String, try_from_borrow)]
    location: Url,

    #[df(
        dtype = List(Utf8),
        serialize_with = convert_list,
        deserialize_with = deserialize_url_list,
    )]
    hrefs: Vec<Url>,

    blob: Vec<u8>,

    time: NaiveDateTime,
}

fn convert_list<T, U: From<T>>(v: Vec<T>) -> Vec<U> {
    v.into_iter().map(U::from).collect()
}

fn try_convert_list<T, U: TryFrom<T>>(v: Vec<T>) -> Result<Vec<U>, U::Error> {
    v.into_iter().map(U::try_from).collect()
}

fn try_convert_str_list<U: FromStr>(v: Vec<String>) -> Result<Vec<U>, U::Err> {
    v.iter().map(|s| U::from_str(s.as_str())).collect()
}

fn deserialize_url_list(v: Vec<String>) -> Result<Vec<Url>, url::ParseError> {
    try_convert_str_list(v)
}

#[test]
fn test_schema() {
    let schema = TestStruct::schema();

    let location = schema.get_field("location").unwrap();
    assert_eq!(location.dtype, DataType::Utf8);

    let blob = schema.get_field("blob").unwrap();
    assert_eq!(blob.dtype, DataType::Binary);

    let time = schema.get_field("time").unwrap();
    assert_eq!(time.dtype, DataType::Datetime(TimeUnit::Milliseconds, None));
}

#[test]
fn test_read_write() {
    let mut tmpfile: Vec<u8> = vec![];

    let schema = TestStruct::schema();

    let item = TestStruct {
        hrefs: vec![
            Url::parse("https://ditto.fyi/interior-crocodile-alligator").unwrap(),
            Url::parse("https://ditto.fyi/interior-crocodile-alligator").unwrap(),
        ],
        blob: vec![23, 24, 25, 28],
        location: Url::parse("https://ditto.fyi/interior-crocodile-alligator").unwrap(),
        time: Local::now()
            .naive_local()
            // round to 0 nanoseconds for equality test b/c nanoseconds are truncated
            .with_nanosecond(0)
            .unwrap(),
    };

    // write to an in-memory parquet file
    {
        let cursor = Cursor::new(&mut tmpfile);

        let mut writer = ParquetWriter::new(cursor)
            .batched(&schema)
            .expect("failed to initialize parquet writer");

        let df = TestStruct::into_df([item.clone()].into_iter()).unwrap();

        writer.write_batch(&df).unwrap();
        writer.finish().unwrap();
    }

    // read back from the in-memory parquet file and verify that we get the same
    // item out
    {
        let cursor = Cursor::new(&mut tmpfile);

        let df = ParquetReader::new(cursor).finish().unwrap();

        let list = TestStruct::from_df(&df).unwrap();

        assert_eq!(list.len(), 1);
        assert_eq!(item, list[0]);
    }
}
