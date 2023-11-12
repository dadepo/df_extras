use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt8Array};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
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

/// Returns the smallest network which includes both of the given networks
pub fn inet_merge(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<String> = vec![];
    let first_inputs = datafusion::common::cast::as_string_array(&args[0])?;
    let second_inputs = datafusion::common::cast::as_string_array(&args[1])?;

    if first_inputs.len() != second_inputs.len() {
        return Err(DataFusionError::Internal(
            "First and second arguments are not of the same length".to_string(),
        ));
    }

    first_inputs
        .iter()
        .flatten()
        .zip(second_inputs.iter().flatten())
        .try_for_each(|(first, second)| {
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
                            DataFusionError::Internal(format!("Create IPv4 failed with error {e}"))
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
                            DataFusionError::Internal(format!("Create IPv6 failed with error {e}"))
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

            result.push(merged);
            Ok::<(), DataFusionError>(())
        })?;

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
}

/// Checks if IP address are from the same family
pub fn inet_same_family(args: &[ArrayRef]) -> Result<ArrayRef> {
    let mut result: Vec<bool> = vec![];
    let first_inputs = datafusion::common::cast::as_string_array(&args[0])?;
    let second_inputs = datafusion::common::cast::as_string_array(&args[1])?;

    if first_inputs.len() != second_inputs.len() {
        return Err(DataFusionError::Internal(
            "First and second arguments are not of the same length".to_string(),
        ));
    }

    first_inputs
        .iter()
        .flatten()
        .zip(second_inputs.iter().flatten())
        .try_for_each(|(first, second)| {
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

            result.push(
                first_ip.is_ipv4() && second_ip.is_ipv4()
                    || first_ip.is_ipv6() && second_ip.is_ipv6(),
            );
            Ok::<(), DataFusionError>(())
        })?;

    Ok(Arc::new(BooleanArray::from(result)) as ArrayRef)
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

/// Sets the netmask length.
/// If input is IP, The address part does not change.
/// If the input is a CIDR, Address bits to the right of the new netmask are set to zero
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
        let input_string = cidr_strings.value(i);
        let prefix: u8 = prefix_lengths.value(i) as u8;

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

        result.push(new_cidr.to_string());
    }

    Ok(Arc::new(StringArray::from(result)) as ArrayRef)
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
            .sql("select pg_broadcast(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+----------------------------------------+
| col_result                             |
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
        let df = ctx
            .sql("select pg_family(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 4          |
| 4          |
| 4          |
| 6          |
| 6          |
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
    async fn test_host() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_host(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----------------+
| col_result      |
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
            .sql("select pg_hostmask(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+---------------------------------+
| col_result                      |
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
    async fn test_inet_same_family() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_inet_same_family('192.168.1.5/24', '::1') as col_result")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| false      |
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
            .sql("select pg_inet_same_family('192.168.1.5/24', '192.168.1.5') as col_result")
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
            .sql("select pg_inet_same_family('2001:db8::ff00:42:8329/128', '::1') as col_result")
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
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_inet_merge('192.168.1.5', '192.168.2.5/16') as col_result")
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
            .sql("select pg_inet_merge('2001:0db8:85a3:0000:0000:8a2e:0370:7334/64', '2001:0db8:85a3:0000:0000:8a2e:0370:8000/64') as col_result")
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
            .sql("select pg_inet_merge('2001:0db8:85a3:0000:0000:8a2e:0370:7334', '2001:0db8::1:0:0:1') as col_result")
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
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_masklen(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------+
| col_result |
+------------+
| 24         |
| 20         |
| 16         |
| 32         |
| 48         |
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
    async fn test_netmask() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_netmask(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+------------------+
| col_result       |
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
            .sql("select pg_network(cidr) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+-----------------+
| col_result      |
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
    async fn test_set_masklen_cidr() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_set_masklen(cidr, 16) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        // "192.168.1.5/24" -> "192.168.0.0/16",
        //  "172.16.0.0/20", -> "172.16.0.0/16"
        //  "10.0.0.0/16", -> "10.0.0.0/16"
        //  "2001:0db8::/32", -> "2001::/16"
        //  "2001:db8:abcd::/48", -> "2001::/16"

        let expected: Vec<&str> = r#"
+----------------+
| col_result     |
+----------------+
| 10.0.0.0/16    |
| 172.16.0.0/16  |
| 192.168.0.0/16 |
| 2001::/16      |
| 2001::/16      |
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
        Ok(())
    }

    #[tokio::test]
    async fn test_set_masklen_ip() -> Result<()> {
        let ctx = set_up_test_datafusion()?;
        let df = ctx
            .sql("select pg_set_masklen(ip, 16) as col_result from test")
            .await?;

        let batches = df.clone().collect().await?;

        let expected: Vec<&str> = r#"
+--------------------+
| col_result         |
+--------------------+
| 10.0.0.0/16        |
| 172.16.0.0/16      |
| 192.168.1.5/16     |
| 2001:db8::/16      |
| 2001:db8:abcd::/16 |
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
            .sql("select pg_set_masklen(cidr, 33) as col_result from test")
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
            .sql("select pg_set_masklen(cidr, 129) as col_result from test")
            .await?;

        let result = df.clone().collect().await;
        assert!(
            matches!(result, Err(DataFusionError::Internal(msg)) if msg == "ERROR:  invalid mask length: 129")
        );

        Ok(())
    }

    fn set_up_test_datafusion() -> Result<SessionContext> {
        // define a schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("cidr", DataType::Utf8, false),
            Field::new("ip", DataType::Utf8, false),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values([
                    "192.168.1.5/24",
                    "172.16.0.0/20",
                    "10.0.0.0/16",
                    "2001:0db8::/32",
                    "2001:db8:abcd::/48",
                ])),
                Arc::new(StringArray::from_iter_values([
                    "192.168.1.5",
                    "172.16.0.0",
                    "10.0.0.0",
                    "2001:0db8::",
                    "2001:db8:abcd::",
                ])),
            ],
        )?;

        // declare a new context
        let ctx = SessionContext::new();
        ctx.register_batch("test", batch)?;
        register_udfs(&ctx)?;
        // declare a table in memory.
        Ok(ctx)
    }
}
