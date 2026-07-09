use crate::mods::qot_common::{QotMarketState, Security};
use crate::mods::qot_get_market_state::{C2s, Request, MarketInfo, Response};
use prost::Message;

pub fn build_market_state_request(security: Security) -> Result<Vec<u8>, prost::EncodeError> {
    let c2s = C2s {
        header: None, // to replace with actual header
        security_list: vec![security],
    };

    let req = Request { c2s };

    Ok(req.encode_to_vec())
}

pub fn parse_market_state_response(proto_bytes: &[u8]) -> anyhow::Result<Vec<MarketInfo>> {
    let resp = Response::decode(proto_bytes)?;

    if resp.ret_type != 0 {
        anyhow::bail!("GetMarketState failed: {:?}", resp.ret_msg);
    }

    let s2c = resp
        .s2c
        .ok_or_else(|| anyhow::anyhow!("GetMarketState reply missing s2c"))?;

    Ok(s2c.market_info_list)
}