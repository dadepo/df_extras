use datafusion::arrow::array::{Array, ArrayRef, StringArray, UInt8Array};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::net::IpAddr;
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

/// Gives the host address for network.
pub fn host(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let host_address = IpNet::from_str(ip_string)
            .map_err(|e| {
                DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
            })?
            .network();
        result.push(host_address.to_string());
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

/// Returns the address's family: 4 for IPv4, 6 for IPv6.
pub fn family(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<u8> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let family = if ip_string.parse::<Ipv4Net>().is_ok() {
            4
        } else if ip_string.parse::<Ipv6Net>().is_ok() {
            6
        } else {
            return Err(DataFusionError::Internal(format!(
                "Could not parse {ip_string} to either IPv4 or IPv6"
            )));
        };

        result.push(family);
        Ok::<(), DataFusionError>(())
    })?;
    Ok(Arc::new(UInt8Array::from(result)) as ArrayRef)
}

/// construct host mask for network
pub fn hostmask(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let hostmask = IpNet::from_str(ip_string)
            .map_err(|e| {
                DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
            })?
            .hostmask();
        result.push(hostmask.to_string());
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

/// extract netmask length
pub fn masklen(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<u8> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let prefix_len = IpNet::from_str(ip_string)
            .map_err(|e| {
                DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
            })?
            .prefix_len();
        result.push(prefix_len);
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(UInt8Array::from(result)) as ArrayRef)
}

/// construct netmask for network
pub fn netmask(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let netmask = IpNet::from_str(ip_string)
            .map_err(|e| {
                DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
            })?
            .netmask();
        result.push(netmask.to_string());
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

/// extract network part of address
pub fn network(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().flatten().try_for_each(|ip_string| {
        let network = IpNet::from_str(ip_string)
            .map_err(|e| {
                DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
            })?
            .network();
        result.push(network.to_string());
        Ok::<(), DataFusionError>(())
    })?;

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

pub fn set_masklen(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let cidr_strings = datafusion::common::cast::as_string_array(&args[0])?;
    let prefix_lengths = datafusion::common::cast::as_int64_array(&args[1])?;

    if cidr_strings.len() != prefix_lengths.len() {
        return Err(DataFusionError::Internal(
            "Cidr count do not match prefix length count".to_string(),
        ));
    }

    for i in 0..cidr_strings.len() {
        let cidr_string = cidr_strings.value(i);
        let prefix: u8 = prefix_lengths.value(i) as u8;
        let cidr: IpNet = IpNet::from_str(cidr_string).map_err(|e| {
            DataFusionError::Internal(format!("Parsing {cidr_string} failed with error {e}"))
        })?;

        match cidr.network() {
            IpAddr::V4(_) => {
                if prefix > 32 {
                    return Err(DataFusionError::Internal(format!(
                        "ERROR:  invalid mask length: {prefix}"
                    )));
                }
            }
            IpAddr::V6(_) => {
                if prefix > 128 {
                    return Err(DataFusionError::Internal(format!(
                        "ERROR:  invalid mask length: {prefix}"
                    )));
                }
            }
        };

        let new_cidr = IpNet::new(cidr.network(), prefix).map_err(|e| {
            DataFusionError::Internal(format!(
                "Creating CIDR from {cidr} and prefix {prefix} failed with error {e}"
            ))
        })?;

        result.push(new_cidr.to_string());
    }

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::register_udfs;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    #[tokio::test]
    async fn test_broadcast() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_broadcast(ip) as broadcast from test")
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
    async fn test_family() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx.sql("select pg_family(ip) as family from test").await?;

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
    async fn test_host() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx.sql("select pg_host(ip) as broadcast from test").await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----------------+
| broadcast       |
+-----------------+
| 192.168.1.0     |
| 172.16.0.0      |
| 10.0.0.0        |
| 2001:db8::      |
| 2001:db8:abcd:: |
+-----------------+"#
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
    async fn test_hostmask() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_hostmask(ip) as hostmask from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+---------------------------------+
| hostmask                        |
+---------------------------------+
| 0.0.0.255                       |
| 0.0.15.255                      |
| 0.0.255.255                     |
| ::ffff:ffff:ffff:ffff:ffff:ffff |
| ::ffff:ffff:ffff:ffff:ffff      |
+---------------------------------+"#
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
    async fn test_masklen() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_masklen(ip) as masklen from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+---------+
| masklen |
+---------+
| 24      |
| 20      |
| 16      |
| 32      |
| 48      |
+---------+"#
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
    async fn test_netmask() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_netmask(ip) as netmask from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------------+
| netmask          |
+------------------+
| 255.255.255.0    |
| 255.255.240.0    |
| 255.255.0.0      |
| ffff:ffff::      |
| ffff:ffff:ffff:: |
+------------------+"#
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
    async fn test_network() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_network(ip) as network from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----------------+
| network         |
+-----------------+
| 192.168.1.0     |
| 172.16.0.0      |
| 10.0.0.0        |
| 2001:db8::      |
| 2001:db8:abcd:: |
+-----------------+"#
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
    async fn test_set_masklen() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_set_masklen(ip, 32) as network from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| network            |
+--------------------+
| 10.0.0.0/32        |
| 172.16.0.0/32      |
| 192.168.1.0/32     |
| 2001:db8::/32      |
| 2001:db8:abcd::/32 |
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
    async fn test_set_masklen_invalid_ipv4_prefix() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_set_masklen(ip, 33) as network from test")
            .await?;

        let result = df.clone().collect().await;
        assert!(
            matches!(result, Err(DataFusionError::Internal(msg)) if msg == "ERROR:  invalid mask length: 33")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_masklen_invalid_ipv6_prefix() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_set_masklen(ip, 129) as network from test")
            .await?;

        let result = df.clone().collect().await;
        assert!(
            matches!(result, Err(DataFusionError::Internal(msg)) if msg == "ERROR:  invalid mask length: 129")
        );

        Ok(())
    }

    fn set_up_test_datafusion() -> Result<SessionContext> {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![Field::new("ip", DataType::Utf8, false)]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter_values([
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
