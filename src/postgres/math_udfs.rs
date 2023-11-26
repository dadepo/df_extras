use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::error::Result;

/// Nearest integer greater than or equal to argument (same as ceil).
pub fn ceiling(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;

    let mut float64array_builder = Float64Array::builder(args[0].len());
    values
        .iter()
        .flatten()
        .for_each(|decimal| float64array_builder.append_value(decimal.ceil()));

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Integer quotient of y/x (truncates towards zero)
pub fn div(args: &[ArrayRef]) -> Result<ArrayRef> {
    let first_values = datafusion::common::cast::as_float64_array(&args[0])?;
    let second_values = datafusion::common::cast::as_float64_array(&args[1])?;

    let mut int64array_builder = Int64Array::builder(args[0].len());

    first_values
        .iter()
        .flatten()
        .zip(second_values.iter().flatten())
        .try_for_each(|(first, second)| {
            int64array_builder.append_value((first / second).floor() as i64);
            Ok::<(), DataFusionError>(())
        })?;

    Ok(Arc::new(int64array_builder.finish()) as ArrayRef)
}

/// Error function
pub fn erf(args: &[ArrayRef]) -> Result<ArrayRef> {
    let column_data = &args[0];
    let data = column_data.into_data();
    let data_type = data.data_type();

    let mut float64array_builder = Float64Array::builder(args[0].len());
    match data_type {
        DataType::Float64 => {
            let values = datafusion::common::cast::as_float64_array(&args[0])?;
            values.iter().try_for_each(|value| {
                if let Some(value) = value {
                    float64array_builder.append_value(libm::erf(value))
                } else {
                    float64array_builder.append_null();
                }
                Ok::<(), DataFusionError>(())
            })?;
        }
        DataType::Int64 => {
            let values = datafusion::common::cast::as_int64_array(&args[0])?;
            values.iter().try_for_each(|value| {
                if let Some(value) = value {
                    float64array_builder.append_value(libm::erf(value as f64))
                } else {
                    float64array_builder.append_null();
                }
                Ok::<(), DataFusionError>(())
            })?;
        }
        DataType::UInt64 => {
            let values = datafusion::common::cast::as_uint64_array(&args[0])?;
            values.iter().try_for_each(|value| {
                if let Some(value) = value {
                    float64array_builder.append_value(libm::erf(value as f64))
                } else {
                    float64array_builder.append_null();
                }
                Ok::<(), DataFusionError>(())
            })?;
        }
        t => {
            return Err(DataFusionError::Internal(format!(
                "Unsupported type {t} for erf function"
            )))
        }
    };

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

#[cfg(feature = "postgres")]
#[cfg(test)]
mod tests {
    use crate::common::test_utils::set_up_maths_data_test;
    use crate::postgres::register_postgres_udfs;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_ceiling() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select ceiling(12.2) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 13.0       |
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

    #[tokio::test]
    async fn test_div() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select div(5, 2.0) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 2          |
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

    #[tokio::test]
    async fn test_erf() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select index, erf(uint) as uint, erf(int) as int, erf(float) as float from maths_table ORDER BY index ASC").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+--------------------+---------------------+--------------------+
| index | uint               | int                 | float              |
+-------+--------------------+---------------------+--------------------+
| 1     | 0.9953222650189527 | -0.9953222650189527 | 0.8427007929497149 |
| 2     | 0.9999779095030014 | 0.9999779095030014  | 0.9999969422902035 |
| 3     |                    |                     |                    |
+-------+--------------------+---------------------+--------------------+"#
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
        let ctx = set_up_maths_data_test()?;
        register_postgres_udfs(&ctx)?;
        Ok(ctx)
    }
}
