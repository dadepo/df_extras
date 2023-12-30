#![cfg(feature = "postgres")]

use std::sync::Arc;

use crate::postgres::math_udfs::{acosd, asind, atand, ceiling, cosd, div, erf, erfc, sind};
use crate::postgres::network_udfs::{
    broadcast, family, host, hostmask, inet_merge, inet_same_family, masklen, netmask, network,
    set_masklen,
};
use datafusion::arrow::datatypes::DataType::{Boolean, Float64, Int64, UInt64, UInt8, Utf8};
use datafusion::error::Result;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;

mod math_udfs;
mod network_udfs;

pub fn register_postgres_udfs(ctx: &SessionContext) -> Result<()> {
    register_network_udfs(ctx)?;
    register_math_udfs(ctx)?;
    Ok(())
}

fn register_math_udfs(ctx: &SessionContext) -> Result<()> {
    register_acosd(ctx);
    register_cosd(ctx);
    register_asind(ctx);
    register_sind(ctx);
    register_atand(ctx);
    register_ceiling(ctx);
    register_erf(ctx);
    register_erfc(ctx);
    register_div(ctx);
    Ok(())
}

fn register_acosd(ctx: &SessionContext) {
    let acosd_udf = make_scalar_function(acosd);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let acosd_udf = ScalarUDF::new(
        "acosd",
        &Signature::uniform(1, vec![Float64], Volatility::Immutable),
        &return_type,
        &acosd_udf,
    );

    ctx.register_udf(acosd_udf);
}

fn register_asind(ctx: &SessionContext) {
    let asind_udf = make_scalar_function(asind);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let asind_udf = ScalarUDF::new(
        "asind",
        &Signature::uniform(1, vec![Float64], Volatility::Immutable),
        &return_type,
        &asind_udf,
    );

    ctx.register_udf(asind_udf);
}

fn register_sind(ctx: &SessionContext) {
    let sind_udf = make_scalar_function(sind);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let sind_udf = ScalarUDF::new(
        "sind",
        &Signature::uniform(1, vec![Float64], Volatility::Immutable),
        &return_type,
        &sind_udf,
    );

    ctx.register_udf(sind_udf);
}

fn register_cosd(ctx: &SessionContext) {
    let cosd_udf = make_scalar_function(cosd);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let cosd_udf = ScalarUDF::new(
        "cosd",
        &Signature::uniform(1, vec![Float64], Volatility::Immutable),
        &return_type,
        &cosd_udf,
    );

    ctx.register_udf(cosd_udf);
}

fn register_atand(ctx: &SessionContext) {
    let atand_udf = make_scalar_function(atand);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let atand_udf = ScalarUDF::new(
        "atand",
        &Signature::uniform(1, vec![Float64], Volatility::Immutable),
        &return_type,
        &atand_udf,
    );

    ctx.register_udf(atand_udf);
}

fn register_ceiling(ctx: &SessionContext) {
    let ceiling_udf = make_scalar_function(ceiling);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let ceiling_udf = ScalarUDF::new(
        "ceiling",
        &Signature::uniform(1, vec![Float64], Volatility::Immutable),
        &return_type,
        &ceiling_udf,
    );

    ctx.register_udf(ceiling_udf);
}

fn register_erf(ctx: &SessionContext) {
    let erf_udf = make_scalar_function(erf);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let erf_udf = ScalarUDF::new(
        "erf",
        &Signature::uniform(1, vec![Int64, UInt64, Float64], Volatility::Immutable),
        &return_type,
        &erf_udf,
    );

    ctx.register_udf(erf_udf);
}

fn register_erfc(ctx: &SessionContext) {
    let erfc_udf = make_scalar_function(erfc);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Float64)));
    let erfc_udf = ScalarUDF::new(
        "erfc",
        &Signature::uniform(1, vec![Int64, UInt64, Float64], Volatility::Immutable),
        &return_type,
        &erfc_udf,
    );

    ctx.register_udf(erfc_udf);
}

fn register_div(ctx: &SessionContext) {
    let udf = make_scalar_function(div);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Int64)));
    let div_udf = ScalarUDF::new(
        "div",
        &Signature::uniform(2, vec![Float64], Volatility::Immutable),
        &return_type,
        &udf,
    );

    ctx.register_udf(div_udf);
}

fn register_network_udfs(ctx: &SessionContext) -> Result<()> {
    register_broadcast(ctx);
    register_family(ctx);
    register_host(ctx);
    register_hostmask(ctx);
    register_inet_same_family(ctx);
    register_inet_merge(ctx);
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
        "broadcast",
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
        "family",
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
        "host",
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
        "hostmask",
        &Signature::uniform(1, vec![Utf8], Volatility::Immutable),
        &return_type,
        &hostmask_udf,
    );

    ctx.register_udf(hostmask_udf);
}

fn register_inet_same_family(ctx: &SessionContext) {
    let inet_same_family_udf = make_scalar_function(inet_same_family);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Boolean)));
    let inet_same_family_udf = ScalarUDF::new(
        "inet_same_family",
        &Signature::uniform(2, vec![Utf8], Volatility::Immutable),
        &return_type,
        &inet_same_family_udf,
    );

    ctx.register_udf(inet_same_family_udf);
}

fn register_inet_merge(ctx: &SessionContext) {
    let inet_merge_udf = make_scalar_function(inet_merge);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(Utf8)));
    let inet_merge_udf = ScalarUDF::new(
        "inet_merge",
        &Signature::uniform(2, vec![Utf8], Volatility::Immutable),
        &return_type,
        &inet_merge_udf,
    );

    ctx.register_udf(inet_merge_udf);
}

fn register_masklen(ctx: &SessionContext) {
    let masklen_udf = make_scalar_function(masklen);
    let return_type: ReturnTypeFunction = Arc::new(move |_| Ok(Arc::new(UInt8)));
    let masklen_udf = ScalarUDF::new(
        "masklen",
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
        "netmask",
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
        "network",
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
        "set_masklen",
        &Signature::exact(vec![Utf8, Int64], Volatility::Immutable),
        &return_type,
        &set_masklen_udf,
    );

    ctx.register_udf(set_masklen_udf);
}
