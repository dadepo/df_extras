use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::str::FromStr;
use std::sync::Arc;

/// Gives the broadcast address for network.
pub fn broadcast(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let broadcast_address = IpNet::from_str(ip_string)
            .map_err(|e| {
                DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
            })?
            .broadcast();
        result.push(broadcast_address.to_string());
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

/// Returns the address's family: 4 for IPv4, 6 for IPv6.
pub fn family(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let family = if ip_string.parse::<Ipv4Net>().is_ok() {
            4.to_string()
        } else if ip_string.parse::<Ipv6Net>().is_ok() {
            6.to_string()
        } else {
            return Err(DataFusionError::Internal(format!("Could not parse {ip_string} to either IPv4 or IPv6")))
        };

        result.push(family);
        Ok::<(), DataFusionError>(())
    })?;
    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::register_udfs;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::from_slice::FromSlice;
    use datafusion::prelude::SessionContext;
    use datafusion::{arrow, assert_batches_sorted_eq};

    #[tokio::test]
    async fn test_broadcast() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select _broadcast(ip) as broadcast from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+----------------------------------------+
| broadcast                              |
+----------------------------------------+
| 192.168.1.255                          |
| 172.16.15.255                          |
| 10.0.255.255                           |
| 2001:db8:ffff:ffff:ffff:ffff:ffff:ffff |
| 2001:db8:abcd:ffff:ffff:ffff:ffff:ffff |
+----------------------------------------+"#
            .split("\n")
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
    async fn test_family() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select _family(ip) as family from test")
            .await?;

     let batches = df.clone().collect().await?;

     let expected: Vec<&str> = r#"
+--------+
| family |
+--------+
| 4      |
| 4      |
| 4      |
| 6      |
| 6      |
+--------+"#
            .split("\n")
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
        // define a schema.
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::Utf8, false)]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_slice([
                "192.168.1.5/24",
                "172.16.0.0/20",
                "10.0.0.0/16",
                "2001:0db8::/32",
                "2001:db8:abcd::/48",
            ]))],
        )?;

        // declare a new context
        let ctx = SessionContext::new();
        ctx.register_batch("test", batch)?;
        register_udfs(&ctx)?;
        // declare a table in memory.
        Ok(ctx)
    }
}
