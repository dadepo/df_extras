use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array,
};
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

#[cfg(feature = "postgres")]
#[cfg(test)]
mod tests {
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use crate::register_udfs;

    use super::*;

    #[tokio::test]
    async fn test_ceiling() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select ceiling(12.2) as col_result")
            .await?;

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

    fn set_up_test_datafusion() -> Result<SessionContext> {
        let ctx = SessionContext::new();
        register_udfs(&ctx)?;
        Ok(ctx)
    }
}
