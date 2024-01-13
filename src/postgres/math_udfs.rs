use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use rand::distributions::Distribution;
use rand::thread_rng;
use rand_distr::Normal;

/// Inverse cosine, result in degrees.
pub fn acosd(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            if value > 1.0 {
                return Err(DataFusionError::Internal(
                    "input is out of range".to_string(),
                ));
            }
            let result = value.acos().to_degrees();
            if result.fract() < 0.9 {
                if result.fract() < 0.01 {
                    float64array_builder.append_value(result.floor());
                } else {
                    float64array_builder.append_value(result);
                }
            } else {
                float64array_builder.append_value(result.ceil());
            }
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Cosine, argument in degrees.
pub fn cosd(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            let result = value.to_radians().cos();
            float64array_builder.append_value(result);
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Cotangent, argument in degrees.
pub fn cotd(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            let result = 1.0 / value.to_radians().tan();
            if result.fract() < 0.01 {
                float64array_builder.append_value(result.floor());
            } else {
                float64array_builder.append_value(result);
            }
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Inverse sine, result in degrees.
pub fn asind(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            if value > 1.0 {
                return Err(DataFusionError::Internal(
                    "input is out of range".to_string(),
                ));
            }
            let result = value.asin().to_degrees();
            if result.fract() < 0.9 {
                if result.fract() < 0.01 {
                    float64array_builder.append_value(result.floor());
                } else {
                    float64array_builder.append_value(result);
                }
            } else {
                float64array_builder.append_value(result.ceil());
            }
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Sine, argument in degrees.
pub fn sind(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            let result = value.to_radians().sin();
            float64array_builder.append_value(result);
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Inverse tangent, result in degrees.
pub fn atand(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            let result = value.atan().to_degrees();
            if result.fract() < 0.9 {
                if result.fract() < 0.01 {
                    float64array_builder.append_value(result.floor());
                } else {
                    float64array_builder.append_value(result);
                }
            } else {
                float64array_builder.append_value(result.ceil());
            }
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

/// Tangent, argument in degrees.
pub fn tand(args: &[ArrayRef]) -> Result<ArrayRef> {
    let values = datafusion::common::cast::as_float64_array(&args[0])?;
    let mut float64array_builder = Float64Array::builder(args[0].len());

    values.iter().try_for_each(|value| {
        if let Some(value) = value {
            let result = value.to_radians().tan();
            float64array_builder.append_value(result);
            Ok::<(), DataFusionError>(())
        } else {
            float64array_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(float64array_builder.finish()) as ArrayRef)
}

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

/// Complementary error function
pub fn erfc(args: &[ArrayRef]) -> Result<ArrayRef> {
    let column_data = &args[0];
    let data = column_data.into_data();
    let data_type = data.data_type();

    let mut float64array_builder = Float64Array::builder(args[0].len());
    match data_type {
        DataType::Float64 => {
            let values = datafusion::common::cast::as_float64_array(&args[0])?;
            values.iter().try_for_each(|value| {
                if let Some(value) = value {
                    float64array_builder.append_value(libm::erfc(value))
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
                    float64array_builder.append_value(libm::erfc(value as f64))
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
                    float64array_builder.append_value(libm::erfc(value as f64))
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

    let array = float64array_builder.finish();
    Ok(Arc::new(array) as ArrayRef)
}

/// Returns a random value from the normal distribution with the given parameters;
/// mean defaults to 0.0 and stddev defaults to 1.0.
/// Example random_normal(0.0, 1.0) could return 0.051285419
pub fn random_normal(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() > 2_usize {
        return Err(DataFusionError::Internal(
            "No function matches the given name and argument types.".to_string(),
        ));
    }

    let args = &args
        .iter()
        .filter(|arg| !matches!(arg.data_type(), &DataType::Null))
        .cloned()
        .collect::<Vec<ArrayRef>>()[..];

    let means = args.first();
    let std_devs = args.get(1);

    let float64array = match (means, std_devs) {
        (Some(means), Some(std_devs)) => {
            let mut float64array_builder = Float64Array::builder(means.len());
            let means = datafusion::common::cast::as_float64_array(means)?;
            let std_devs = datafusion::common::cast::as_float64_array(std_devs)?;
            means
                .iter()
                .zip(std_devs.iter())
                .try_for_each(|(mean, std_dev)| {
                    if let (Some(mean), Some(std_dev)) = (mean, std_dev) {
                        let normal = Normal::new(mean, std_dev).map_err(|_| {
                            DataFusionError::Internal(
                                "Runtime error: Failed to create normal distribution".to_string(),
                            )
                        })?;
                        let mut rng = thread_rng();
                        let value = normal.sample(&mut rng);
                        float64array_builder.append_value(value);
                        Ok::<(), DataFusionError>(())
                    } else {
                        float64array_builder.append_null();
                        Ok::<(), DataFusionError>(())
                    }
                })?;
            float64array_builder.finish()
        }
        (Some(means), None) => {
            let mut float64array_builder = Float64Array::builder(means.len());
            let means = datafusion::common::cast::as_float64_array(means)?;
            means.iter().try_for_each(|mean| {
                if let Some(mean) = mean {
                    let normal = Normal::new(mean, 1.0_f64).map_err(|_| {
                        DataFusionError::Internal(
                            "Runtime error: Failed to create normal distribution".to_string(),
                        )
                    })?;
                    let mut rng = thread_rng();
                    let value = normal.sample(&mut rng);
                    float64array_builder.append_value(value);
                    Ok::<(), DataFusionError>(())
                } else {
                    float64array_builder.append_null();
                    Ok::<(), DataFusionError>(())
                }
            })?;
            float64array_builder.finish()
        }
        (None, Some(std_devs)) => {
            let mut float64array_builder = Float64Array::builder(std_devs.len());
            let std_devs = datafusion::common::cast::as_float64_array(std_devs)?;

            std_devs.iter().try_for_each(|std_dev| {
                if let Some(std_dev) = std_dev {
                    let normal = Normal::new(0.0_f64, std_dev).map_err(|_| {
                        DataFusionError::Internal(
                            "Runtime error: Failed to create normal distribution".to_string(),
                        )
                    })?;
                    let mut rng = thread_rng();
                    let value = normal.sample(&mut rng);
                    float64array_builder.append_value(value);
                    Ok::<(), DataFusionError>(())
                } else {
                    float64array_builder.append_null();
                    Ok::<(), DataFusionError>(())
                }
            })?;
            float64array_builder.finish()
        }
        (None, None) => {
            let mut float64array_builder = Float64Array::builder(1);
            let normal = Normal::new(0.0_f64, 1.0_f64).map_err(|_| {
                DataFusionError::Internal(
                    "Runtime error: Failed to create normal distribution".to_string(),
                )
            })?;
            let mut rng = thread_rng();
            let value = normal.sample(&mut rng);
            float64array_builder.append_value(value);
            float64array_builder.finish()
        }
    };

    Ok(Arc::new(float64array) as ArrayRef)
}

#[cfg(feature = "postgres")]
#[cfg(test)]
mod tests {
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use crate::common::test_utils::set_up_maths_data_test;
    use crate::postgres::register_postgres_udfs;

    use super::*;

    #[tokio::test]
    async fn test_acosd() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select acosd(0.5) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 60.0       |
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

        let df = ctx.sql("select acosd(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------------------+
| col_result        |
+-------------------+
| 66.42182152179817 |
+-------------------+"#
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

        let df = ctx.sql("select acosd(1.4) as col_result").await?;

        let result = df.clone().collect().await;
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("input is out of range"));

        Ok(())
    }

    #[tokio::test]
    async fn test_cosd() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select cosd(60) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 0.5000000000000001 |
+--------------------+"#
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

        let df = ctx.sql("select cosd(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 0.9999756307053947 |
+--------------------+"#
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
    async fn test_cotd() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select cotd(45) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 1.0        |
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

        let df = ctx.sql("select cotd(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 143.23712166947507 |
+--------------------+"#
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
    async fn test_asind() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select asind(0.5) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 30.0       |
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

        let df = ctx.sql("select asind(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 23.578178478201835 |
+--------------------+"#
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
    async fn test_sind() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select sind(30) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+---------------------+
| col_result          |
+---------------------+
| 0.49999999999999994 |
+---------------------+"#
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

        let df = ctx.sql("select sind(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----------------------+
| col_result            |
+-----------------------+
| 0.0069812602979615525 |
+-----------------------+"#
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
    async fn test_atand() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select atand(0.5) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 26.565051177077994 |
+--------------------+"#
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

        let df = ctx.sql("select atand(1) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 45.0       |
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
    async fn test_tand() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select tand(45) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 0.9999999999999999 |
+--------------------+"#
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

        let df = ctx.sql("select tand(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+----------------------+
| col_result           |
+----------------------+
| 0.006981430430496479 |
+----------------------+"#
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

    #[tokio::test]
    async fn test_erfc() -> Result<()> {
        let ctx = register_udfs_for_test()?;

        let df = ctx.sql("select index, erfc(uint) as uint, erfc(int) as int, erfc(float) as float from maths_table ORDER BY index ASC").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+-------------------------+-------------------------+----------------------+
| index | uint                    | int                     | float                |
+-------+-------------------------+-------------------------+----------------------+
| 1     | 0.004677734981047266    | 1.9953222650189528      | 0.15729920705028513  |
| 2     | 0.000022090496998585438 | 0.000022090496998585438 | 3.057709796438165e-6 |
| 3     |                         |                         |                      |
+-------+-------------------------+-------------------------+----------------------+"#
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
    async fn test_random_normal() -> Result<()> {
        let ctx = register_udfs_for_test()?;

        let df = ctx
            .sql(
                r#"
        select random_normal(index) as index,
               random_normal(uint) as uint,
               random_normal(int) as int,
               random_normal(float) as float
        from maths_table"#,
            )
            .await?;

        df.clone().show().await?;
        // No exception is ok.
        Ok(())
    }

    fn register_udfs_for_test() -> Result<SessionContext> {
        let ctx = set_up_maths_data_test()?;
        register_postgres_udfs(&ctx)?;
        Ok(ctx)
    }
}
