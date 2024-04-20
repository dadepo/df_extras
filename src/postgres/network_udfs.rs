use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::str::FromStr;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringBuilder, UInt8Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::datatypes::DataType::{Int64, Utf8};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::logical_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use ipnet::{IpNet, Ipv4Net, Ipv6Net};

/// Gives the broadcast address for the network.
/// Returns NULL for columns with NULL values.
pub fn broadcast(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().try_for_each(|ip_string| {
        if let Some(ip_string) = ip_string {
            let broadcast_address = IpNet::from_str(ip_string)
                .map_err(|e| {
                    DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
                })?
                .broadcast();
            string_builder.append_value(broadcast_address.to_string());
            Ok::<(), DataFusionError>(())
        } else {
            string_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Gives the host address for network.
/// Returns NULL for columns with NULL values.
pub fn host(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().try_for_each(|ip_string| {
        if let Some(ip_string) = ip_string {
            let host_address = IpNet::from_str(ip_string)
                .map_err(|e| {
                    DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
                })?
                .network();
            string_builder.append_value(host_address.to_string());
            Ok::<(), DataFusionError>(())
        } else {
            string_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Returns the address's family: 4 for IPv4, 6 for IPv6.
/// Returns NULL for columns with NULL values.
pub fn family(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut int8array = UInt8Array::builder(args[0].len());
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().try_for_each(|ip_string| {
        if let Some(ip_string) = ip_string {
            let family = if ip_string.parse::<Ipv4Net>().is_ok() {
                4
            } else if ip_string.parse::<Ipv6Net>().is_ok() {
                6
            } else {
                return Err(DataFusionError::Internal(format!(
                    "Could not parse {ip_string} to either IPv4 or IPv6"
                )));
            };

            int8array.append_value(family);
            Ok::<(), DataFusionError>(())
        } else {
            int8array.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;
    Ok(Arc::new(int8array.finish()) as ArrayRef)
}

/// Constructs host mask for network.
/// Returns NULL for columns with NULL values.
pub fn hostmask(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().try_for_each(|ip_string| {
        if let Some(ip_string) = ip_string {
            let hostmask = IpNet::from_str(ip_string)
                .map_err(|e| {
                    DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
                })?
                .hostmask();
            string_builder.append_value(hostmask.to_string());
            Ok::<(), DataFusionError>(())
        } else {
            string_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Returns the smallest network which includes both of the given networks.
/// Returns NULL if any of the columns contain NULL values.
pub fn inet_merge(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
    let first_inputs = datafusion::common::cast::as_string_array(&args[0])?;
    let second_inputs = datafusion::common::cast::as_string_array(&args[1])?;

    if first_inputs.len() != second_inputs.len() {
        return Err(DataFusionError::Internal(
            "First and second arguments are not of the same length".to_string(),
        ));
    }

    first_inputs
        .iter()
        .zip(second_inputs.iter())
        .try_for_each(|(first, second)| {
            if let (Some(first), Some(second)) = (first, second) {
                let first_net = if !first.contains('/') {
                    IpNet::from(IpAddr::from_str(first).map_err(|e| {
                        DataFusionError::Internal(format!("Parsing {first} failed with error {e}"))
                    })?)
                } else {
                    IpNet::from_str(first).map_err(|e| {
                        DataFusionError::Internal(format!("Parsing {first} failed with error {e}"))
                    })?
                };

                let second_net = if !second.contains('/') {
                    IpNet::from(IpAddr::from_str(second).map_err(|e| {
                        DataFusionError::Internal(format!("Parsing {first} failed with error {e}"))
                    })?)
                } else {
                    IpNet::from_str(second).map_err(|e| {
                        DataFusionError::Internal(format!("Parsing {second} failed with error {e}"))
                    })?
                };

                let min_bit_mask = std::cmp::min(first_net.prefix_len(), second_net.prefix_len());

                let merged = match (first_net, second_net) {
                    (IpNet::V4(first_ipv4_net), IpNet::V4(second_ipv4_net)) => {
                        let first_addr_bit = first_ipv4_net.network().octets();
                        let second_addr_bit = second_ipv4_net.network().octets();
                        let common_bits =
                            bit_in_common(&first_addr_bit, &second_addr_bit, min_bit_mask as usize);

                        let first = Ipv4Net::new(Ipv4Addr::from(first_addr_bit), common_bits as u8)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Create IPv4 failed with error {e}"
                                ))
                            })?
                            .network();

                        Ipv4Net::new(first, common_bits as u8)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Create IPv4Net failed with error {e}"
                                ))
                            })?
                            .to_string()
                    }
                    (IpNet::V6(first_ipv6_net), IpNet::V6(second_ipv6_net)) => {
                        let first_addr_bit = first_ipv6_net.network().octets();
                        let second_addr_bit = second_ipv6_net.network().octets();
                        let common_bits =
                            bit_in_common(&first_addr_bit, &second_addr_bit, min_bit_mask as usize);

                        let first = Ipv6Net::new(Ipv6Addr::from(first_addr_bit), common_bits as u8)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Create IPv6 failed with error {e}"
                                ))
                            })?
                            .network();

                        Ipv6Net::new(first, common_bits as u8)
                            .map_err(|e| {
                                DataFusionError::Internal(format!(
                                    "Create IPv6Net failed with error {e}"
                                ))
                            })?
                            .to_string()
                    }
                    _ => {
                        return Err(DataFusionError::Internal(
                            "Cannot merge addresses from different families".to_string(),
                        ))
                    }
                };

                string_builder.append_value(merged);
                Ok::<(), DataFusionError>(())
            } else {
                string_builder.append_null();
                Ok::<(), DataFusionError>(())
            }
        })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Checks if IP address are from the same family.
/// Returns NULL if any of the columns contain NULL values.
pub fn inet_same_family(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut boolean_array = BooleanArray::builder(args[0].len());
    let first_inputs = datafusion::common::cast::as_string_array(&args[0])?;
    let second_inputs = datafusion::common::cast::as_string_array(&args[1])?;

    if first_inputs.len() != second_inputs.len() {
        return Err(DataFusionError::Internal(
            "First and second arguments are not of the same length".to_string(),
        ));
    }

    first_inputs
        .iter()
        .zip(second_inputs.iter())
        .try_for_each(|(first, second)| {
            if let (Some(first), Some(second)) = (first, second) {
                let first_ip = IpAddr::from_str(first)
                    .or_else(|_e| IpNet::from_str(first).map(|ip_net| ip_net.network()))
                    .map_err(|e| {
                        DataFusionError::Internal(format!("Parsing {first} failed with error {e}"))
                    })?;

                let second_ip = IpAddr::from_str(second)
                    .or_else(|_e| IpNet::from_str(second).map(|ip_net| ip_net.network()))
                    .map_err(|e| {
                        DataFusionError::Internal(format!("Parsing {second} failed with error {e}"))
                    })?;

                boolean_array.append_value(
                    first_ip.is_ipv4() && second_ip.is_ipv4()
                        || first_ip.is_ipv6() && second_ip.is_ipv6(),
                );
                Ok::<(), DataFusionError>(())
            } else {
                boolean_array.append_null();
                Ok::<(), DataFusionError>(())
            }
        })?;

    Ok(Arc::new(boolean_array.finish()) as ArrayRef)
}

/// Extracts netmask length.
/// Returns NULL for columns with NULL values.
pub fn masklen(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut int8array = UInt8Array::builder(args[0].len());
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().try_for_each(|ip_string| {
        if let Some(ip_string) = ip_string {
            let prefix_len = IpNet::from_str(ip_string)
                .map_err(|e| {
                    DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
                })?
                .prefix_len();
            int8array.append_value(prefix_len);
            Ok::<(), DataFusionError>(())
        } else {
            int8array.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(int8array.finish()) as ArrayRef)
}

/// Constructs netmask for network.
/// Returns NULL for columns with NULL values.
pub fn netmask(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
    let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
    ip_string.iter().try_for_each(|ip_string| {
        if let Some(ip_string) = ip_string {
            let netmask = IpNet::from_str(ip_string)
                .map_err(|e| {
                    DataFusionError::Internal(format!("Parsing {ip_string} failed with error {e}"))
                })?
                .netmask();
            string_builder.append_value(netmask.to_string());
            Ok::<(), DataFusionError>(())
        } else {
            string_builder.append_null();
            Ok::<(), DataFusionError>(())
        }
    })?;

    Ok(Arc::new(string_builder.finish()) as ArrayRef)
}

/// Extracts network part of address.
/// Returns NULL for columns with NULL values.
#[derive(Debug)]
pub struct Network {
    signature: Signature,
}

impl Network {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Network {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "network"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
        let ip_string = datafusion::common::cast::as_string_array(&args[0])?;
        ip_string.iter().try_for_each(|ip_string| {
            if let Some(ip_string) = ip_string {
                let network = IpNet::from_str(ip_string)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Parsing {ip_string} failed with error {e}"
                        ))
                    })?
                    .network();
                string_builder.append_value(network.to_string());
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

/// Sets the netmask length.
/// If input is IP, The address part does not change.
/// If the input is a CIDR, Address bits to the right of the new netmask are set to zero.
/// Returns NULL if any of the columns contain NULL values.
#[derive(Debug)]
pub struct Masklen {
    signature: Signature,
}

impl Masklen {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![Utf8, Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for Masklen {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "set_masklen"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Utf8)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;
        let mut string_builder = StringBuilder::with_capacity(args[0].len(), u8::MAX as usize);
        let cidr_strings = datafusion::common::cast::as_string_array(&args[0])?;
        let prefix_lengths = datafusion::common::cast::as_int64_array(&args[1])?;

        if cidr_strings.len() != prefix_lengths.len() {
            return Err(DataFusionError::Internal(
                "Cidr count do not match prefix length count".to_string(),
            ));
        }

        for i in 0..cidr_strings.len() {
            let input_string = cidr_strings.value(i);
            let prefix: u8 = prefix_lengths.value(i) as u8;

            if input_string.is_empty() || prefix == 0 {
                string_builder.append_null();
                continue;
            }

            let is_cidr = input_string.contains('/');

            let addr = if is_cidr {
                IpNet::from_str(input_string)
                    .map_err(|e| {
                        DataFusionError::Internal(format!(
                            "Parsing {input_string} into CIDR failed with error {e}"
                        ))
                    })?
                    .network()
            } else {
                IpAddr::from_str(input_string).map_err(|e| {
                    DataFusionError::Internal(format!(
                        "Parsing {input_string} into IP address failed with error {e}"
                    ))
                })?
            };

            match addr {
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

            let mut new_cidr = IpNet::new(addr, prefix).map_err(|e| {
                DataFusionError::Internal(format!(
                    "Creating CIDR from {addr} and prefix {prefix} failed with error {e}"
                ))
            })?;

            if is_cidr {
                new_cidr = new_cidr.trunc();
            };

            string_builder.append_value(new_cidr.to_string());
        }

        Ok(ColumnarValue::Array(
            Arc::new(string_builder.finish()) as ArrayRef
        ))
    }
}

fn bit_in_common(l: &[u8], r: &[u8], n: usize) -> usize {
    let mut byte = 0;
    let mut n_bits = n % 8;

    while byte < (n / 8) {
        if l[byte] != r[byte] {
            n_bits = 7;
            break;
        }
        byte += 1;
    }

    if n_bits != 0 {
        let diff = l[byte] ^ r[byte];

        while n_bits > 0 && (diff >> (8 - n_bits)) != 0 {
            n_bits -= 1;
        }
    }

    (8 * byte) + n_bits
}

#[cfg(feature = "postgres")]
#[cfg(test)]
mod tests {
    use crate::common::test_utils::set_up_network_data_test;
    use crate::postgres::register_postgres_udfs;
    use datafusion::assert_batches_sorted_eq;
    use datafusion::prelude::SessionContext;

    use super::*;

    #[tokio::test]
    async fn test_broadcast() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql(
                "select index, broadcast(cidr) as col_result from network_table ORDER BY index ASC",
            )
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+----------------------------------------+
| index | col_result                             |
+-------+----------------------------------------+
| 1     | 192.168.1.255                          |
| 2     | 172.16.15.255                          |
| 3     | 10.0.255.255                           |
| 4     | 2001:db8:ffff:ffff:ffff:ffff:ffff:ffff |
| 5     | 2001:db8:abcd:ffff:ffff:ffff:ffff:ffff |
| 6     |                                        |
+-------+----------------------------------------+"#
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
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, family(cidr) as col_result from network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | 4          |
| 2     | 4          |
| 3     | 4          |
| 4     | 6          |
| 5     | 6          |
| 6     |            |
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
    async fn test_host() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, host(cidr) as col_result from network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+-----------------+
| index | col_result      |
+-------+-----------------+
| 1     | 192.168.1.0     |
| 2     | 172.16.0.0      |
| 3     | 10.0.0.0        |
| 4     | 2001:db8::      |
| 5     | 2001:db8:abcd:: |
| 6     |                 |
+-------+-----------------+"#
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
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, hostmask(cidr) as col_result from network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+---------------------------------+
| index | col_result                      |
+-------+---------------------------------+
| 1     | 0.0.0.255                       |
| 2     | 0.0.15.255                      |
| 3     | 0.0.255.255                     |
| 4     | ::ffff:ffff:ffff:ffff:ffff:ffff |
| 5     | ::ffff:ffff:ffff:ffff:ffff      |
| 6     |                                 |
+-------+---------------------------------+"#
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
    async fn test_inet_same_family() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, inet_same_family(cidr, '::1') as col_result FROM network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | false      |
| 2     | false      |
| 3     | false      |
| 4     | true       |
| 5     | true       |
| 6     |            |
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

        let df = ctx
            .sql("select inet_same_family('192.168.1.5/24', '192.168.1.5') as col_result")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| true       |
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

        let df = ctx
            .sql("select inet_same_family('2001:db8::ff00:42:8329/128', '::1') as col_result")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| true       |
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
    async fn test_inet_merge() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select inet_merge('192.168.1.5', '192.168.2.5/16') as col_result")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+----------------+
| col_result     |
+----------------+
| 192.168.0.0/16 |
+----------------+"#
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
            .sql("select inet_merge('2001:0db8:85a3:0000:0000:8a2e:0370:7334/64', '2001:0db8:85a3:0000:0000:8a2e:0370:8000/64') as col_result")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 2001:db8:85a3::/64 |
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

        let df = ctx
            .sql("select inet_merge('2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:0db8::1:0:0:1') as col_result")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+---------------+
| col_result    |
+---------------+
| 2001:db8::/32 |
+---------------+"#
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
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, masklen(cidr) as col_result from network_table order by index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------+
| index | col_result |
+-------+------------+
| 1     | 24         |
| 2     | 20         |
| 3     | 16         |
| 4     | 32         |
| 5     | 48         |
| 6     |            |
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
    async fn test_netmask() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, netmask(cidr) as col_result from network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+------------------+
| index | col_result       |
+-------+------------------+
| 1     | 255.255.255.0    |
| 2     | 255.255.240.0    |
| 3     | 255.255.0.0      |
| 4     | ffff:ffff::      |
| 5     | ffff:ffff:ffff:: |
| 6     |                  |
+-------+------------------+"#
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
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, network(cidr) as col_result from network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+-----------------+
| index | col_result      |
+-------+-----------------+
| 1     | 192.168.1.0     |
| 2     | 172.16.0.0      |
| 3     | 10.0.0.0        |
| 4     | 2001:db8::      |
| 5     | 2001:db8:abcd:: |
| 6     |                 |
+-------+-----------------+"#
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
    async fn test_set_masklen_cidr() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, set_masklen(cidr, 16) as col_result from network_table ORDER BY index ASC")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+----------------+
| index | col_result     |
+-------+----------------+
| 1     | 192.168.0.0/16 |
| 2     | 172.16.0.0/16  |
| 3     | 10.0.0.0/16    |
| 4     | 2001::/16      |
| 5     | 2001::/16      |
| 6     |                |
+-------+----------------+"#
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
    async fn test_set_masklen_ip() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select index, set_masklen(ip, 16) as col_result from network_table ORDER BY index desc")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-------+--------------------+
| index | col_result         |
+-------+--------------------+
| 1     | 192.168.1.5/16     |
| 2     | 172.16.0.0/16      |
| 3     | 10.0.0.0/16        |
| 4     | 2001:db8::/16      |
| 5     | 2001:db8:abcd::/16 |
| 6     |                    |
+-------+--------------------+"#
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
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select set_masklen(cidr, 33) as col_result from network_table")
            .await?;

        let result = df.clone().collect().await;
        assert!(
            matches!(result, Err(DataFusionError::Internal(msg)) if msg == "ERROR:  invalid mask length: 33")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_masklen_invalid_ipv6_prefix() -> Result<()> {
        let ctx = register_udfs_for_test()?;
        let df = ctx
            .sql("select set_masklen(cidr, 129) as col_result from network_table")
            .await?;

        let result = df.clone().collect().await;
        assert!(
            matches!(result, Err(DataFusionError::Internal(msg)) if msg == "ERROR:  invalid mask length: 129")
        );

        Ok(())
    }

    fn register_udfs_for_test() -> Result<SessionContext> {
        let ctx = set_up_network_data_test()?;
        register_postgres_udfs(&ctx)?;
        Ok(ctx)
    }
}
