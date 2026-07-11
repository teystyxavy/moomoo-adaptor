use sha1::{Digest, Sha1};
use tokio::{io::AsyncReadExt, net::TcpStream};

const HEADER_FLAG: [u8; 2] = [b'F', b'T']; // "FT" in ASCII
const PROTO_FMT_PROTOBUF: u8 = 0;
const PROTO_VER: u8 = 0;

pub fn build_frame(proto_id: u32, serial_no: u32, body: &[u8]) -> Vec<u8> {
    let mut hasher = Sha1::new();
    hasher.update(body);
    let body_sha1 = hasher.finalize();

    let mut frame = Vec::with_capacity(44 + body.len());

    frame.extend_from_slice(&HEADER_FLAG);
    // le is a guess pending the InitConnect round-trip check; flip to _be_bytes if body_len/proto_id come back garbled
    frame.extend_from_slice(&proto_id.to_le_bytes());
    frame.push(PROTO_FMT_PROTOBUF);
    frame.push(PROTO_VER);
    frame.extend_from_slice(&serial_no.to_le_bytes());
    frame.extend_from_slice(&(body.len() as u32).to_le_bytes());
    frame.extend_from_slice(&body_sha1);
    frame.extend_from_slice(&[0u8; 8]); // reserved

    frame.extend_from_slice(body);

    return frame;
}

pub struct FrameHeader {
    pub proto_id: u32,
    pub proto_fmt_type: u8,
    pub proto_ver: u8,
    pub serial_no: u32,
    pub body_len: u32,
}

//verifies header bytes are of the correct size, returns a FrameHeader struct with the parsed fields, or an error string if the header is invalid
pub fn parse_header(header_bytes: &[u8; 44]) -> Result<FrameHeader, &'static str> {
    let flag: [u8; 2] = header_bytes[0..2].try_into().unwrap();
    if flag != HEADER_FLAG {
        return Err("invalid header flag");
    }

    let proto_id = u32::from_le_bytes(header_bytes[2..6].try_into().unwrap());
    let proto_fmt_type = header_bytes[6];
    let proto_ver = header_bytes[7];
    let serial_no = u32::from_le_bytes(header_bytes[8..12].try_into().unwrap());
    let body_len = u32::from_le_bytes(header_bytes[12..16].try_into().unwrap());
    // bytes[16..36] are the body's SHA1, bytes[36..44] are reserved — not validated here

    return Ok(FrameHeader {
        proto_id,
        proto_fmt_type,
        proto_ver,
        serial_no,
        body_len,
    });
}

pub async fn read_frame<R: tokio::io::AsyncRead + Unpin>(stream: &mut R) -> anyhow::Result<(FrameHeader, Vec<u8>)> {
    let mut header_buf: [u8; 44] = [0u8; 44];
    stream.read_exact(&mut header_buf).await?;
    let header = parse_header(&header_buf).map_err(anyhow::Error::msg)?;

    let mut body_buf = vec![0u8; header.body_len as usize];
    stream.read_exact(&mut body_buf).await?;

    Ok((header, body_buf))
}