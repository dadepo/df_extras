#![cfg(feature = "sqlite")]

mod json_udfs;

use crate::json_udfs::{json, json_valid};
use datafusion::arrow::datatypes::DataType::{UInt8, Utf8};
use datafusion::error::Result;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

pub fn register_udfs(ctx: &SessionContext) -> Result<()> {
    register_json(ctx);
    register_json_valid(ctx);
    Ok(())
}

fn register_json(ctx: &SessionContext) {
    let udf = make_scalar_function(json);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let div_udf = ScalarUDF::new(
        "json",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &udf,
    );

    ctx.register_udf(div_udf);
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
