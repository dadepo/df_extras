use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, StringBuilder, UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::{UInt64, UInt8, Utf8};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::logical_expr::TypeSignature::Uniform;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use serde_json::Value;

use crate::common::{get_json_string_type, get_json_type, get_value_at_string};

/// The json(X) function verifies that its argument X is a valid JSON string and returns a minified
/// version of that JSON string (with all unnecessary whitespace removed).
/// If X is not a well-formed JSON string, then this routine throws an error.
#[derive(Debug)]
pub struct Json {
    signature: Signature,
}

impl Json {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Json {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "json"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(string_builder.finish()) as ArrayRef
        ))
    }
}

/// The json_type(X) function returns the "type" of the outermost element of X. The json_type(X,P)
/// function returns the "type" of the element in X that is selected by path P.
/// The "type" returned by json_type() is one of the following SQL
/// text values: 'null', 'true', 'false', 'integer', 'real', 'text', 'array', or 'object'.
/// If the path P in json_type(X, P) selects an element that does not exist in X,
/// then this function returns NULL.
#[derive(Debug)]
pub struct JsonType {
    signature: Signature,
}

impl JsonType {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(vec![Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonType {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "json_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        if args.is_empty() || args.len() > 2 {
            return Err(DataFusionError::Internal(
                "wrong number of arguments to function json_type()".to_string(),
            ));
        }

        let mut string_builder = StringBuilder::with_capacity(args.len(), u8::MAX as usize);
        if args.len() == 1 {
            //1. Just json and no path
            let json_strings = datafusion::common::cast::as_string_array(&args[0])?;
            json_strings.iter().try_for_each(|json_string| {
                if let Some(json_string) = json_string {
                    string_builder.append_value(
                        get_json_string_type(json_string)
                            .map_err(|err| DataFusionError::Internal(err.to_string()))?,
                    );
                    Ok::<(), DataFusionError>(())
                } else {
                    string_builder.append_null();
                    Ok::<(), DataFusionError>(())
                }
            })?;
        } else {
            //2. Json and path
            let json_strings = datafusion::common::cast::as_string_array(&args[0])?;
            let paths = datafusion::common::cast::as_string_array(&args[1])?;

            json_strings
                .iter()
                .zip(paths.iter())
                .try_for_each(|(json_string, path)| {
                    if let (Some(json_string), Some(path)) = (json_string, path) {
                        match get_value_at_string(json_string, path) {
                            Ok(json_at_path) => {
                                string_builder.append_value(
                                    get_json_type(&json_at_path).map_err(|err| {
                                        DataFusionError::Internal(err.to_string())
                                    })?,
                                );
                            }
                            Err(_) => {
                                string_builder.append_null();
                            }
                        }
                        Ok::<(), DataFusionError>(())
                    } else {
                        string_builder.append_null();
                        Ok::<(), DataFusionError>(())
                    }
                })?;
        }

        Ok(ColumnarValue::Array(
            Arc::new(string_builder.finish()) as ArrayRef
        ))
    }
}

/// The json_valid(X) function return 1 if the argument X is well-formed canonical RFC-7159 JSON
/// without any extensions, or return 0 if the argument X is not well-formed JSON or is
/// JSON that includes JSON5 extensions.
///
/// Examples:
///
/// json_valid('{"x": 35}') → 1
/// json_valid('{"x": 35') → 0
/// json_valid(NULL) → NULL
#[derive(Debug)]
pub struct JsonValid {
    signature: Signature,
}

impl JsonValid {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonValid {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "json_valid"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let json_strings = datafusion::common::cast::as_string_array(&args[0])?;
        let mut uint_builder = UInt8Array::builder(json_strings.len());

        json_strings.iter().for_each(|json_string| {
            if let Some(json_string) = json_string {
                let json_value: serde_json::error::Result<Value> =
                    serde_json::from_str(json_string);
                uint_builder.append_value(json_value.is_ok() as u8);
            } else {
                uint_builder.append_null();
            }
        });

        Ok(ColumnarValue::Array(
            Arc::new(uint_builder.finish()) as ArrayRef
        ))
    }
}

/// The json_array_length(X) function returns the number of elements in the JSON array X, or 0 if X is some kind of JSON value other than an array.
/// The json_array_length(X,P) locates the array at path P within X and returns the length of that array, or 0 if path P locates an element in X that is not a JSON array,
/// and NULL if path P does not locate any element of X. Errors are thrown if either X is not well-formed JSON or if P is not a well-formed path.
#[derive(Debug)]
pub struct JsonArrayLength {
    signature: Signature,
}

impl JsonArrayLength {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![Uniform(1, vec![Utf8]), Uniform(2, vec![Utf8])],
                Volatility::Volatile,
            ),
        }
    }
}

impl ScalarUDFImpl for JsonArrayLength {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "json_array_length"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(UInt64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let mut iter = args.iter();
        let json_strings = iter.next().ok_or(DataFusionError::Execution(
            "First input not set".to_string(),
        ))?;
        let json_strings = datafusion::common::cast::as_string_array(&json_strings)?;
        let paths = iter.next();
        let mut uint_builder = UInt64Array::builder(json_strings.len());

        match paths {
            None => {
                json_strings.iter().try_for_each(|json_string| {
                    if let Some(json_string) = json_string {
                        if let Ok(value) = get_value_at_string(json_string, "$") {
                            if let Some(value_array) = value.as_array() {
                                uint_builder.append_value(value_array.len() as u64);
                            } else {
                                uint_builder.append_value(0u64);
                            }
                        } else {
                            uint_builder.append_null();
                        }
                        Ok::<(), DataFusionError>(())
                    } else {
                        uint_builder.append_null();
                        Ok::<(), DataFusionError>(())
                    }
                })?;
            }
            Some(paths) => {
                let paths = datafusion::common::cast::as_string_array(&paths)?;
                json_strings
                    .iter()
                    .zip(paths.iter())
                    .try_for_each(|(json_string, path)| {
                        if let (Some(json_string), Some(path)) = (json_string, path) {
                            match get_value_at_string(json_string, path) {
                                Ok(json_at_path) => {
                                    if let Some(value_array) = json_at_path.as_array() {
                                        uint_builder.append_value(value_array.len() as u64);
                                    } else {
                                        uint_builder.append_value(0u64);
                                    }
                                }
                                Err(_) => {
                                    uint_builder.append_null();
                                }
                            }
                            Ok::<(), DataFusionError>(())
                        } else {
                            uint_builder.append_null();
                            Ok::<(), DataFusionError>(())
                        }
                    })?;
            }
        }

        Ok(ColumnarValue::Array(
            Arc::new(uint_builder.finish()) as ArrayRef
        ))
    }
}

#[cfg(feature = "sqlite")]
#[cfg(test)]
mod tests {
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use crate::common::test_utils::set_up_json_data_test;
    use crate::sqlite::register_sqlite_udfs;

    use super::*;

    #[tokio::test]
    async fn test_json() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(
                r#"select index, json(json_data) as col_result FROM json_values_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+-----------------------------------+
| index | col_result                        |
+-------+-----------------------------------+
| 1     | {"this":"is","a":["test"]}        |
| 2     | {"a":[2,3.5,true,false,null,"x"]} |
| 3     | ["one","two"]                     |
| 4     | 123                               |
| 5     | 12.3                              |
| 6     | true                              |
| 7     | false                             |
| 8     |                                   |
+-------+-----------------------------------+"#
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
                r#"select index, json(' { "this" : "is", "a": [ "test" ]  ') as col_result FROM json_values_table ORDER BY index ASC"#,
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
        let df = ctx.sql(r#"select index, json_valid(json_data) as col_result FROM json_values_table ORDER BY index ASC"#).await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | 1          |
| 2     | 1          |
| 3     | 1          |
| 4     | 1          |
| 5     | 1          |
| 6     | 1          |
| 7     | 1          |
| 8     |            |
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

    #[tokio::test]
    async fn test_json_type() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(
                r#"select index, json_type(json_data) as col_result FROM json_values_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | object     |
| 2     | object     |
| 3     | array      |
| 4     | integer    |
| 5     | real       |
| 6     | true       |
| 7     | false      |
| 8     |            |
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

    #[tokio::test]
    async fn test_json_type_with_path() -> Result<()> {
        // Test cases
        // json_type('{"a":[2,3.5,true,false,null,"x"]}') → 'object'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$') → 'object'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a') → 'array'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[0]') → 'integer'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[1]') → 'real'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[2]') → 'true'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[3]') → 'false'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[4]') → 'null'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[5]') → 'text'
        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[6]') → NULL

        let ctx = register_udfs_for_test()?;

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$') → 'object'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | object     |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a') → 'array'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | array      |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[0]') → 'integer'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[0]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | integer    |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[1]') → 'real'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[1]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | real       |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[2]') → 'true'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[2]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | true       |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[3]') → 'false'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[3]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | false      |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[4]') → 'null'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[4]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | null       |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[5]') → 'text'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[3]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | false      |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[4]') → 'null'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[4]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | null       |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[5]') → 'text'
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[5]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | text       |
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

        // json_type('{"a":[2,3.5,true,false,null,"x"]}','$.a[6]') → NULL
        let df = ctx
            .sql(
                r#"select index, json_type(json_data, '$.a[6]') as col_result FROM json_path_table ORDER BY index ASC"#,
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     |            |
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

    #[tokio::test]
    async fn test_json_array_length() -> Result<()> {
        let ctx = register_udfs_for_test()?;

        let df = ctx
            .sql(r#"select json_array_length('[1,2,3,4]') as len"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----+
| len |
+-----+
| 4   |
+-----+"#
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

        let df = ctx
            .sql(r#"select json_array_length('[1,2,3,4]', '$') as len"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----+
| len |
+-----+
| 4   |
+-----+"#
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

        let df = ctx
            .sql(r#"select json_array_length('[1,2,3,4]', '$[2]') as len"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----+
| len |
+-----+
| 0   |
+-----+"#
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

        let df = ctx
            .sql(r#"select json_array_length('{"one":[1,2,3]}') as len"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----+
| len |
+-----+
| 0   |
+-----+"#
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

        let df = ctx
            .sql(r#"select json_array_length('{"one":[1,2,3]}', '$.one') as len"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----+
| len |
+-----+
| 3   |
+-----+"#
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

        let df = ctx
            .sql(r#"select json_array_length('{"one":[1,2,3]}', '$.two') as len"#)
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----+
| len |
+-----+
|     |
+-----+"#
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
        register_sqlite_udfs(&ctx)?;
        Ok(ctx)
    }
}
