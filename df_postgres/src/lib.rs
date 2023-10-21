use std::sync::Arc;

use datafusion::arrow::datatypes::DataType::{Int64, UInt8, Utf8};
use datafusion::error::Result;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;

use crate::network_udfs::{
    broadcast, family, host, hostmask, masklen, netmask, network, set_masklen,
};

mod network_udfs;

pub fn register_udfs(ctx: &SessionContext) -> Result<()> {
    register_network_udfs(ctx)?;
    Ok(())
}

fn register_network_udfs(ctx: &SessionContext) -> Result<()> {
    register_broadcast(ctx);
    register_family(ctx);
    register_host(ctx);
    register_hostmask(ctx);
    register_masklen(ctx);
    register_netmask(ctx);
    register_network(ctx);
    register_set_masklen(ctx);
    Ok(())
}

fn register_broadcast(ctx: &SessionContext) {
    let broadcast_udf = make_scalar_function(broadcast);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let broadcast_udf = ScalarUDF::new(
        "pg_broadcast",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &broadcast_udf,
    );

    ctx.register_udf(broadcast_udf);
}

fn register_family(ctx: &SessionContext) {
    let family_udf = make_scalar_function(family);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(UInt8)));
    let family_udf = ScalarUDF::new(
        "pg_family",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &family_udf,
    );

    ctx.register_udf(family_udf);
}

fn register_host(ctx: &SessionContext) {
    let host_udf = make_scalar_function(host);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let host_udf = ScalarUDF::new(
        "pg_host",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &host_udf,
    );

    ctx.register_udf(host_udf);
}

fn register_hostmask(ctx: &SessionContext) {
    let hostmask_udf = make_scalar_function(hostmask);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let hostmask_udf = ScalarUDF::new(
        "pg_hostmask",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &hostmask_udf,
    );

    ctx.register_udf(hostmask_udf);
}

fn register_masklen(ctx: &SessionContext) {
    let masklen_udf = make_scalar_function(masklen);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(UInt8)));
    let masklen_udf = ScalarUDF::new(
        "pg_masklen",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &masklen_udf,
    );

    ctx.register_udf(masklen_udf);
}

fn register_netmask(ctx: &SessionContext) {
    let netmask_udf = make_scalar_function(netmask);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let netmask_udf = ScalarUDF::new(
        "pg_netmask",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &netmask_udf,
    );

    ctx.register_udf(netmask_udf);
}

fn register_network(ctx: &SessionContext) {
    let network_udf = make_scalar_function(network);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let network_udf = ScalarUDF::new(
        "pg_network",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &network_udf,
    );

    ctx.register_udf(network_udf);
}

fn register_set_masklen(ctx: &SessionContext) {
    let set_masklen_udf = make_scalar_function(set_masklen);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let set_masklen_udf = ScalarUDF::new(
        "pg_set_masklen",
        &Signature::exact(vec![Utf8, Int64], Volatility::Immutable),
        &return_type,
        &set_masklen_udf,
    );

    ctx.register_udf(set_masklen_udf);
}
