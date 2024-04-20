#![cfg(feature = "sqlite")]
#![allow(deprecated)]

mod json_udfs;

use crate::sqlite::json_udfs::{json_valid, Json, JsonType};
use datafusion::arrow::datatypes::DataType::{UInt8, Utf8};
use datafusion::error::Result;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub fn register_sqlite_udfs(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::from(Json::new()));
    ctx.register_udf(ScalarUDF::from(JsonType::new()));
    register_json_valid(ctx);
    Ok(())
}

fn register_json_valid(ctx: &SessionContext) {
    let udf = make_scalar_function(json_valid);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(UInt8)));
    let json_valid_udf = ScalarUDF::new(
        "json_valid",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &udf,
    );

    ctx.register_udf(json_valid_udf);
}
