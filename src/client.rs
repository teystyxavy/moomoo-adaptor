use crate::mods::init_connect::C2s;
use crate::mods::init_connect::Response;
use crate::mods::init_connect::Request;
use prost::Message;


pub fn build_init_conn_req(client_ver: i32, client_id: String) -> Result<Vec<u8>, prost::EncodeError> {
    let raw = C2s {
        client_ver,
        client_id,
        recv_notify: Some(true),
        packet_enc_algo: Some(-1), // unencrypted
        push_proto_fmt: Some(0), // protobuf
        programming_language: Some("Rust".to_string()),
        ai_type: Some(0), // non-AI
    };

    let req = Request { c2s: raw };

    return Ok(req.encode_to_vec());
}

pub fn handle_response(proto_bytes: &[u8]) -> Result<Response, prost::DecodeError> {
    Response::decode(proto_bytes)
}