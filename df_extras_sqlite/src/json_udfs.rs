use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringBuilder, UInt8Array};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use serde_json::Value;

/// The json(X) function verifies that its argument X is a valid JSON string and returns a minified
/// version of that JSON string (with all unnecessary whitespace removed).
/// If X is not a well-formed JSON string, then this routine throws an error.
pub fn json(args: &[ArrayRef]) -> Result<ArrayRef> {
    let json_strings = datafusion::common::cast::as_string_array(&args[0])?;

    let mut string_builder = StringBuilder::with_capacity(json_strings.len(), u8::MAX as usize);
    json_strings.iter().try_for_each(|json_string| {
        if let Some(json_string) = json_string {
            let value: Value = serde_json::from_str(json_string).map_err(|_| {
                DataFusionError::Internal("Runtime error: malformed JSON".to_string())
            })?;
            let pretty_json = serde_json::to_string(&value).map_err(|_| {
                DataFusionError::Internal("Runtime error: malformed JSON".to_string())
            })?;
            string_builder.append_value(pretty_json);
            Ok::<(), DataFusionError>(())
        } else {
            string_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// The json_valid(X) function return 1 if the argument X is well-formed canonical RFC-7159 JSON
/// without any extensions, or return 0 if the argument X is not well-formed JSON or is
/// JSON that includes JSON5 extensions.
///
/// Examples:
///
/// json_valid('{"x":35}') → 1
/// json_valid('{"x":35') → 0
/// json_valid(NULL) → NULL
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
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use common::test_utils::set_up_json_data_test;

    use crate::register_udfs;

    use super::*;

    #[tokio::test]
    async fn test_json() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(
                r#"select index, json(json_data) as col_result FROM json_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+----------------------------+
| index | col_result                 |
+-------+----------------------------+
| 1     | {"this":"is","a":["test"]} |
| 2     |                            |
+-------+----------------------------+"#
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
    async fn test_invalid_json() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(
                r#"select index, json(' { "this" : "is", "a": [ "test" ]  ') as col_result FROM json_table ORDER BY index ASC"#,
            )
            .await?;

        let result = df.clone().collect().await;

        assert!(&result
            .err()
            .unwrap()
            .find_root()
            .find_root()
            .to_string()
            .contains("Internal error: Runtime error: malformed JSON"));

        Ok(())
    }

    #[tokio::test]
    async fn test_json_valid() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql(r#"select index, json_valid(json_data) as col_result FROM json_table ORDER BY index ASC"#).await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | 1          |
| 2     |            |
+-------+------------+"#
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
        let ctx = set_up_json_data_test()?;
        register_udfs(&ctx)?;
        Ok(ctx)
    }
}
