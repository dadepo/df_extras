#![cfg(feature = "postgres")]
#![allow(deprecated)]

use std::sync::Arc;

use datafusion::arrow::datatypes::DataType::{UInt8, Utf8};
use datafusion::error::Result;
use datafusion::logical_expr::{ReturnTypeFunction, ScalarUDF, Signature, Volatility};
use datafusion::physical_expr::functions::make_scalar_function;
use datafusion::prelude::SessionContext;

use crate::postgres::math_udfs::{
    Acosd, Asind, Atand, Ceiling, Cosd, Cotd, Div, Erf, Erfc, RandomNormal, Sind, Tand,
};
use crate::postgres::network_udfs::{
    broadcast, family, Host, HostMask, InetMerge, InetSameFamily, MaskLen, Netmask, Network,
    SetMaskLen,
};

mod math_udfs;
mod network_udfs;

pub fn register_postgres_udfs(ctx: &SessionContext) -> Result<()> {
    register_network_udfs(ctx)?;
    register_math_udfs(ctx)?;
    Ok(())
}

fn register_math_udfs(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::from(Acosd::new()));
    ctx.register_udf(ScalarUDF::from(Cosd::new()));
    ctx.register_udf(ScalarUDF::from(Cotd::new()));
    ctx.register_udf(ScalarUDF::from(Asind::new()));
    ctx.register_udf(ScalarUDF::from(Sind::new()));
    ctx.register_udf(ScalarUDF::from(Atand::new()));
    ctx.register_udf(ScalarUDF::from(Tand::new()));
    ctx.register_udf(ScalarUDF::from(Ceiling::new()));
    ctx.register_udf(ScalarUDF::from(Div::new()));
    ctx.register_udf(ScalarUDF::from(Erf::new()));
    ctx.register_udf(ScalarUDF::from(Erfc::new()));
    ctx.register_udf(ScalarUDF::from(RandomNormal::new()));
    Ok(())
}

fn register_network_udfs(ctx: &SessionContext) -> Result<()> {
    register_broadcast(ctx);
    register_family(ctx);
    ctx.register_udf(ScalarUDF::from(Host::new()));
    ctx.register_udf(ScalarUDF::from(HostMask::new()));
    ctx.register_udf(ScalarUDF::from(InetSameFamily::new()));
    ctx.register_udf(ScalarUDF::from(InetMerge::new()));
    ctx.register_udf(ScalarUDF::from(MaskLen::new()));
    ctx.register_udf(ScalarUDF::from(Netmask::new()));
    ctx.register_udf(ScalarUDF::from(Network::new()));
    ctx.register_udf(ScalarUDF::from(SetMaskLen::new()));
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
