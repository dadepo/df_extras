use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringBuilder, UInt8Array};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use serde_json::Value;
use std::sync::Arc;

pub fn json(args: &[ArrayRef]) -> Result<ArrayRef> {
    let json_strings = datafusion::common::cast::as_string_array(&args[0])?;

    let mut string_builder = StringBuilder::with_capacity(json_strings.len(), u8::MAX as usize);
    json_strings.iter().flatten().try_for_each(|json_string| {
        let value: Value = serde_json::from_str(json_string).map_err(|e| {
            DataFusionError::Internal(format!("Parsing {json_string} failed with error {e}"))
        })?;
        let pretty_json = serde_json::to_string(&value).map_err(|e| {
            DataFusionError::Internal(format!("Parsing {json_string} failed with error {e}"))
        })?;
        string_builder.append_value(pretty_json);
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

pub fn json_valid(args: &[ArrayRef]) -> Result<ArrayRef> {
    let json_strings = datafusion::common::cast::as_string_array(&args[0])?;
    let mut uint_builder = UInt8Array::builder(json_strings.len());

    json_strings.iter().for_each(|json_string| {
        if let Some(json_string) = json_string {
            let json_value: serde_json::error::Result<Value> = serde_json::from_str(json_string);
            uint_builder.append_value(json_value.is_ok() as u8);
        } else {
            uint_builder.append_null();
        }
    });

    Ok(Arc::new(uint_builder.finish()) as ArrayRef)
}

#[cfg(feature = "sqlite")]
#[cfg(test)]
mod tests {
    use common::test_utils::set_up_test_datafusion;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use crate::register_udfs;

    use super::*;

    #[tokio::test]
    async fn test_json() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(r#"select json(' { "this" : "is", "a": [ "test" ] } ') as col_result"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+----------------------------+
| col_result                 |
+----------------------------+
| {"this":"is","a":["test"]} |
+----------------------------+"#
            .split('\n')
            .filter_map(|input| {
                if input.is_empty() {
                    None
                } else {
                    Some(input.trim())
                }
            })
            .collect();
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn test_json_valid() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(r#"select json_valid(null) as col_result"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
|            |
+------------+"#
            .split('\n')
            .filter_map(|input| {
                if input.is_empty() {
                    None
                } else {
                    Some(input.trim())
                }
            })
            .collect();
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    fn register_udfs_for_test() -> Result<SessionContext> {
        let ctx = set_up_test_datafusion()?;
        register_udfs(&ctx)?;
        Ok(ctx)
    }
}
