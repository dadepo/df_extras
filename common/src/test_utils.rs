use datafusion::arrow::array::{StringArray, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub fn set_up_test_datafusion() -> Result<SessionContext> {
    // define a schema.
    let schema = Arc::new(Schema::new(vec![
        Field::new("index", DataType::UInt8, false),
        Field::new("cidr", DataType::Utf8, true),
        Field::new("ip", DataType::Utf8, true),
    ]));

    // define data.
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(UInt8Array::from_iter_values([
                1,
                2,
                3,
                4,
                5,
                6
            ])),
            Arc::new(StringArray::from(vec![
                Some("192.168.1.5/24"),
                Some("172.16.0.0/20"),
                Some("10.0.0.0/16"),
                Some("2001:0db8::/32"),
                Some("2001:db8:abcd::/48"),
                None
            ])),
            Arc::new(StringArray::from(vec![
                Some("192.168.1.5"),
                Some("172.16.0.0"),
                Some("10.0.0.0"),
                Some("2001:0db8::"),
                Some("2001:db8:abcd::"),
                None
            ])),
        ],
    )?;

    // declare a new context
    let ctx = SessionContext::new();
    ctx.register_batch("network_table", batch)?;
    // declare a table in memory.
    Ok(ctx)
}
