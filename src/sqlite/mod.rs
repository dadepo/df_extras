#![cfg(feature = "sqlite")]
#![allow(deprecated)]

use datafusion::error::Result;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;

use crate::sqlite::json_udfs::{Json, JsonArrayLength, JsonType, JsonValid};

mod json_udfs;

pub fn register_sqlite_udfs(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::from(Json::new()));
    ctx.register_udf(ScalarUDF::from(JsonType::new()));
    ctx.register_udf(ScalarUDF::from(JsonValid::new()));
    ctx.register_udf(ScalarUDF::from(JsonArrayLength::new()));
    Ok(())
}
