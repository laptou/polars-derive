use std::io::Cursor;

use polars::export::chrono::{Local, NaiveDateTime, Timelike};
use polars::prelude::*;
use polars_derive::{helpers::deserialize_datetime, FromDataFrame, IntoDataFrame};
use url::Url;

#[derive(IntoDataFrame, FromDataFrame, Clone, PartialEq, Eq, Debug)]
struct TestStruct {
    #[df(into = String, try_from_borrow)]
    location: Url,

    #[df(deserialize_with = deserialize_datetime)]
    time: NaiveDateTime,
}

#[test]
fn test_schema() {
    let schema = TestStruct::schema();

    let location = schema.get_field("location").unwrap();
    assert_eq!(location.dtype, DataType::Utf8);

    let time = schema.get_field("time").unwrap();
    assert_eq!(time.dtype, DataType::Datetime(TimeUnit::Milliseconds, None));
}

#[test]
fn test_read_write() {
    let mut tmpfile: Vec<u8> = vec![];

    let schema = TestStruct::schema();

    let item = TestStruct {
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
