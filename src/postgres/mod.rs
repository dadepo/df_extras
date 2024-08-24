#![cfg(feature = "postgres")]
#![allow(deprecated)]

use datafusion::error::Result;
use datafusion::logical_expr::ScalarUDF;
use datafusion::prelude::SessionContext;

use crate::postgres::math_udfs::{
    Acosd, Asind, Atand, Ceiling, Cosd, Cotd, Div, Erf, Erfc, Mod, RandomNormal, Sind, Tand,
};
use crate::postgres::network_udfs::{
    Broadcast, Family, Host, HostMask, InetMerge, InetSameFamily, MaskLen, Netmask, Network,
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
    ctx.register_udf(ScalarUDF::from(Mod::new()));
    Ok(())
}

fn register_network_udfs(ctx: &SessionContext) -> Result<()> {
    ctx.register_udf(ScalarUDF::from(Broadcast::new()));
    ctx.register_udf(ScalarUDF::from(Family::new()));
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
