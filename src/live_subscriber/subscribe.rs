use prost::Message;

use crate::mods::qot_common::{Security, SubType};
use crate::mods::qot_sub::{C2s, Request};

pub fn build_sub_request(
    market: i32,
    code: String,
    sub_types: Vec<SubType>,
) -> Result<Vec<u8>, prost::EncodeError> {
    let c2s = C2s {
        security_list: vec![Security { market, code }],
        sub_type_list: sub_types.into_iter().map(|t| t as i32).collect(),
        is_sub_or_un_sub: true,
        is_reg_or_un_reg_push: Some(true),
        reg_push_rehab_type_list: vec![],
        is_first_push: Some(true),
        is_unsub_all: None,
        is_sub_order_book_detail: None,
        extended_time: None,
        session: None,
        header: None,
    };

    let req = Request { c2s };

    Ok(req.encode_to_vec())
}
