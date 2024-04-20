use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::{Float64, Int64, UInt64};

use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::logical_expr::TypeSignature::{Any, Variadic};
use datafusion::logical_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use rand::distributions::Distribution;
use rand::thread_rng;
use rand_distr::Normal;

/// Inverse cosine, result in degrees.
#[derive(Debug)]
pub struct Acosd {
    signature: Signature,
}

impl Acosd {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Acosd {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "acosd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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
                float64array_builder.append_value(result);
                Ok::<(), DataFusionError>(())
            } else {
                float64array_builder.append_null();
                Ok::<(), DataFusionError>(())
            }
        })?;

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Cosine, argument in degrees.
#[derive(Debug)]
pub struct Cosd {
    signature: Signature,
}

impl Cosd {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Cosd {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "cosd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Cotangent, argument in degrees.
#[derive(Debug)]
pub struct Cotd {
    signature: Signature,
}

impl Cotd {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Cotd {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "cotd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let values = datafusion::common::cast::as_float64_array(&args[0])?;
        let mut float64array_builder = Float64Array::builder(args[0].len());

        values.iter().try_for_each(|value| {
            if let Some(value) = value {
                let result = 1.0 / value.to_radians().tan();
                float64array_builder.append_value(result);
                Ok::<(), DataFusionError>(())
            } else {
                float64array_builder.append_null();
                Ok::<(), DataFusionError>(())
            }
        })?;

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Inverse sine, result in degrees.
#[derive(Debug)]
pub struct Asind {
    signature: Signature,
}

impl Asind {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Asind {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "asind"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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
                float64array_builder.append_value(result);
                Ok::<(), DataFusionError>(())
            } else {
                float64array_builder.append_null();
                Ok::<(), DataFusionError>(())
            }
        })?;

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Sine, argument in degrees.
#[derive(Debug)]
pub struct Sind {
    signature: Signature,
}

impl Sind {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Sind {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "sind"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Inverse tangent, result in degrees.
#[derive(Debug)]
pub struct Atand {
    signature: Signature,
}

impl Atand {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Atand {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "atand"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let values = datafusion::common::cast::as_float64_array(&args[0])?;
        let mut float64array_builder = Float64Array::builder(args[0].len());

        values.iter().try_for_each(|value| {
            if let Some(value) = value {
                let result = value.atan().to_degrees();
                float64array_builder.append_value(result);
                Ok::<(), DataFusionError>(())
            } else {
                float64array_builder.append_null();
                Ok::<(), DataFusionError>(())
            }
        })?;

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Tangent, argument in degrees.
#[derive(Debug)]
pub struct Tand {
    signature: Signature,
}

impl Tand {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Tand {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "tand"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Nearest integer greater than or equal to argument (same as ceil).
#[derive(Debug)]
pub struct Ceiling {
    signature: Signature,
}

impl Ceiling {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Ceiling {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "ceiling"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let values = datafusion::common::cast::as_float64_array(&args[0])?;

        let mut float64array_builder = Float64Array::builder(args[0].len());
        values
            .iter()
            .flatten()
            .for_each(|decimal| float64array_builder.append_value(decimal.ceil()));

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Integer quotient of y/x (truncates towards zero)
#[derive(Debug)]
pub struct Div {
    signature: Signature,
}

impl Div {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(2, vec![Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Div {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "div"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(int64array_builder.finish()) as ArrayRef
        ))
    }
}

/// Error function
#[derive(Debug)]
pub struct Erf {
    signature: Signature,
}

impl Erf {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64, UInt64, Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Erf {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "erf"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let column_data = args
            .first()
            .ok_or(DataFusionError::Internal("Empty argument".to_string()))?;

        let col_array = match column_data {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => {
                return Err(DataFusionError::Execution("Empty argument".to_string()))
            }
        };

        let mut float64array_builder = Float64Array::builder(col_array.len());
        let column_data = col_array;
        let data = column_data.into_data();
        let data_type = data.data_type();

        match data_type {
            Float64 => {
                let values = datafusion::common::cast::as_float64_array(&col_array)?;
                values.iter().try_for_each(|value| {
                    if let Some(value) = value {
                        float64array_builder.append_value(libm::erf(value))
                    } else {
                        float64array_builder.append_null();
                    }
                    Ok::<(), DataFusionError>(())
                })?;
            }
            Int64 => {
                let values = datafusion::common::cast::as_int64_array(&col_array)?;
                values.iter().try_for_each(|value| {
                    if let Some(value) = value {
                        float64array_builder.append_value(libm::erf(value as f64))
                    } else {
                        float64array_builder.append_null();
                    }
                    Ok::<(), DataFusionError>(())
                })?;
            }
            UInt64 => {
                let values = datafusion::common::cast::as_uint64_array(&col_array)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

/// Complementary error function
#[derive(Debug)]
pub struct Erfc {
    signature: Signature,
}

impl Erfc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64, UInt64, Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Erfc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "erfc"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let column_data = args
            .first()
            .ok_or(DataFusionError::Internal("Empty argument".to_string()))?;

        let col_array = match column_data {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(_) => {
                return Err(DataFusionError::Execution("Empty argument".to_string()))
            }
        };

        let mut float64array_builder = Float64Array::builder(col_array.len());
        let data = col_array.into_data();
        let data_type = data.data_type();

        match data_type {
            Float64 => {
                let values = datafusion::common::cast::as_float64_array(&col_array)?;
                values.iter().try_for_each(|value| {
                    if let Some(value) = value {
                        float64array_builder.append_value(libm::erfc(value))
                    } else {
                        float64array_builder.append_null();
                    }
                    Ok::<(), DataFusionError>(())
                })?;
            }
            Int64 => {
                let values = datafusion::common::cast::as_int64_array(&col_array)?;
                values.iter().try_for_each(|value| {
                    if let Some(value) = value {
                        float64array_builder.append_value(libm::erfc(value as f64))
                    } else {
                        float64array_builder.append_null();
                    }
                    Ok::<(), DataFusionError>(())
                })?;
            }
            UInt64 => {
                let values = datafusion::common::cast::as_uint64_array(&col_array)?;
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

        Ok(ColumnarValue::Array(
            Arc::new(float64array_builder.finish()) as ArrayRef,
        ))
    }
}

#[derive(Debug)]
pub struct RandomNormal {
    signature: Signature,
}

impl RandomNormal {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![Any(0), Variadic(vec![Float64])],
                Volatility::Volatile,
            ),
        }
    }
}

/// Returns a random value from the normal distribution with the given parameters;
/// mean defaults to 0.0 and stddev defaults to 1.0.
/// Example random_normal(0.0, 1.0) could return 0.051285419
impl ScalarUDFImpl for RandomNormal {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "random_normal"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Float64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        if args.len() > 2_usize {
            return Err(DataFusionError::Internal(
                "No function matches the given name and argument types.".to_string(),
            ));
        }

        let means = args.first();
        let std_devs = args.get(1);

        match (means, std_devs) {
            (Some(ColumnarValue::Array(means)), Some(ColumnarValue::Array(std_devs))) => {
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
                                    "Runtime error: Failed to create normal distribution"
                                        .to_string(),
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
                Ok(ColumnarValue::Array(
                    Arc::new(float64array_builder.finish()) as ArrayRef,
                ))
            }
            (Some(ColumnarValue::Array(means)), None) => {
                let row_count = means.len();
                let mut float64array_builder = Float64Array::builder(row_count);
                // No arguments were passed to the function
                if means.data_type() == &DataType::Null {
                    // mean defaults to 0.0 and stddev defaults to 1.0
                    let normal = Normal::new(0.0, 1.0).map_err(|_| {
                        DataFusionError::Internal(
                            "Runtime error: Failed to create normal distribution".to_string(),
                        )
                    })?;

                    for _ in 0..row_count {
                        let mut rng = thread_rng();
                        let value = normal.sample(&mut rng);
                        float64array_builder.append_value(value);
                    }
                } else {
                    let means = datafusion::common::cast::as_float64_array(means)?;
                    means.iter().try_for_each(|mean| {
                        if let Some(mean) = mean {
                            let normal = Normal::new(mean, 1.0_f64).map_err(|_| {
                                DataFusionError::Internal(
                                    "Runtime error: Failed to create normal distribution"
                                        .to_string(),
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
                }
                Ok(ColumnarValue::Array(
                    Arc::new(float64array_builder.finish()) as ArrayRef,
                ))
            }
            (None, Some(ColumnarValue::Array(std_devs))) => {
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
                Ok(ColumnarValue::Array(
                    Arc::new(float64array_builder.finish()) as ArrayRef,
                ))
            }
            _ => {
                unimplemented!()
            }
        }
    }
}

#[cfg(feature = "postgres")]
#[cfg(test)]
mod tests {
    use approx::ulps_eq;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::common::cast::{as_float64_array, as_int64_array};
    use datafusion::prelude::SessionContext;

    use crate::common::test_utils::set_up_maths_data_test;
    use crate::postgres::register_postgres_udfs;

    use super::*;

    #[tokio::test]
    async fn test_acosd() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select acosd(0.5) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(result, 60.0_f64, epsilon = f64::EPSILON));

        let df = ctx.sql("select acosd(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            66.42182152179817_f64,
            epsilon = f64::EPSILON
        ));

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
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            0.5000000000000001_f64,
            epsilon = f64::EPSILON
        ));

        let df = ctx.sql("select cosd(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            0.9999756307053947_f64,
            epsilon = f64::EPSILON
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_cotd() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select cotd(45) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(result, 1.0_f64, epsilon = f64::EPSILON));

        let df = ctx.sql("select cotd(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            143.23712166947507_f64,
            epsilon = f64::EPSILON
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_asind() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select asind(0.5) as col_result").await?;

        let batches = df.clone().collect().await?;

        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(result, 30.0_f64, epsilon = f64::EPSILON));

        let df = ctx.sql("select asind(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            23.578178478201835_f64,
            epsilon = f64::EPSILON
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_sind() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select sind(30) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            0.49999999999999994_f64,
            epsilon = f64::EPSILON
        ));

        let df = ctx.sql("select sind(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            0.0069812602979615525_f64,
            epsilon = f64::EPSILON
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_atand() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select atand(0.5) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            26.565051177077994_f64,
            epsilon = f64::EPSILON
        ));

        let df = ctx.sql("select atand(1) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(result, 45.0_f64, epsilon = f64::EPSILON));

        Ok(())
    }

    #[tokio::test]
    async fn test_tand() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select tand(45) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            0.9999999999999999_f64,
            epsilon = f64::EPSILON
        ));

        let df = ctx.sql("select tand(0.4) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(
            result,
            0.006981430430496479_f64,
            epsilon = f64::EPSILON
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_ceiling() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select ceiling(12.2) as col_result").await?;

        let batches = df.clone().collect().await?;
        let columns = &batches.first().unwrap().column(0);
        let result = as_float64_array(columns)?;
        let result = result.value(0);

        assert!(ulps_eq!(result, 13.0_f64, epsilon = f64::EPSILON));

        Ok(())
    }

    #[tokio::test]
    async fn test_div() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx.sql("select div(5, 2.0) as col_result").await?;

        let batches = df.clone().collect().await?;

        let columns = &batches.first().unwrap().column(0);
        let result = as_int64_array(columns)?;
        let result = result.value(0);

        assert_eq!(result, 2_i64);

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

        // +-------+------+-----+-------+
        // | index | uint | int | float |
        // +-------+------+-----+-------+
        // | 1     | 2    | -2  | 1.0   |
        // | 2     | 3    | 3   | 3.3   |
        // | 3     |      |     |       |
        // +-------+------+-----+-------+

        fn has_duplicates(values: &[Option<f64>]) -> bool {
            let len = values.len();
            for i in 0..len {
                for j in i + 1..len {
                    if values[i] == values[j] {
                        return true; // Duplicates found
                    }
                }
            }
            false // No duplicates found
        }

        // no mean and stddev
        let binding = ctx
            .sql(r#"select random_normal() from maths_table"#)
            .await?
            .collect()
            .await?;

        let batch = binding
            .first()
            .expect("Batches to contain at least one item");
        assert_eq!(batch.num_rows(), 3);
        let values = as_float64_array(batch.column(0))?;
        let mut vec_of_f64: Vec<Option<f64>> = vec![];
        for value in values {
            vec_of_f64.push(value)
        }
        assert!(!has_duplicates(&vec_of_f64[..]));

        // mean as (nullable) uint and no stddev
        let binding = ctx
            .sql(r#"select random_normal(uint) from maths_table"#)
            .await?
            .collect()
            .await?;

        let batch = binding
            .first()
            .expect("Batches to contain at least one item");
        assert_eq!(batch.num_rows(), 3);
        let values = as_float64_array(batch.column(0))?;
        let mut vec_of_f64: Vec<Option<f64>> = vec![];
        for value in values {
            vec_of_f64.push(value)
        }
        assert!(!has_duplicates(&vec_of_f64[..]));
        // Last element should be None.
        assert_eq!(vec_of_f64.last(), Some(&None));

        // mean as (nullable) int and no stddev
        let binding = ctx
            .sql(r#"select random_normal(int) from maths_table"#)
            .await?
            .collect()
            .await?;

        let batch = binding
            .first()
            .expect("Batches to contain at least one item");
        assert_eq!(batch.num_rows(), 3);
        let values = as_float64_array(batch.column(0))?;
        let mut vec_of_f64: Vec<Option<f64>> = vec![];
        for value in values {
            vec_of_f64.push(value)
        }
        assert!(!has_duplicates(&vec_of_f64[..]));
        // Last element should be None.
        assert_eq!(vec_of_f64.last(), Some(&None));

        // mean as (nullable) float and no stddev
        let binding = ctx
            .sql(r#"select random_normal(float) from maths_table"#)
            .await?
            .collect()
            .await?;

        let batch = binding
            .first()
            .expect("Batches to contain at least one item");
        assert_eq!(batch.num_rows(), 3);
        let values = as_float64_array(batch.column(0))?;
        let mut vec_of_f64: Vec<Option<f64>> = vec![];
        for value in values {
            vec_of_f64.push(value)
        }
        assert!(!has_duplicates(&vec_of_f64[..]));
        // Last element should be None.
        assert_eq!(vec_of_f64.last(), Some(&None));

        // mean as (nullable) uint and stddev as nullable float
        let binding = ctx
            .sql(r#"select random_normal(uint, float) from maths_table"#)
            .await?
            .collect()
            .await?;

        let batch = binding
            .first()
            .expect("Batches to contain at least one item");
        assert_eq!(batch.num_rows(), 3);
        let values = as_float64_array(batch.column(0))?;
        let mut vec_of_f64: Vec<Option<f64>> = vec![];
        for value in values {
            vec_of_f64.push(value)
        }
        assert!(!has_duplicates(&vec_of_f64[..]));
        // Last element should be None.
        assert_eq!(vec_of_f64.last(), Some(&None));

        Ok(())
    }

    fn register_udfs_for_test() -> Result<SessionContext> {
        let ctx = set_up_maths_data_test()?;
        register_postgres_udfs(&ctx)?;
        Ok(ctx)
    }
}
