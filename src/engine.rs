use crate::client::{build_init_conn_req, handle_response};
use crate::frame::{build_frame, read_frame};
use crate::live_subscriber::subscribe::{build_sub_request};
use crate::market_state_querier::market_state_querier::{build_market_state_request, parse_market_state_response};
use crate::model::proto_ids::{INIT_CONNECT, QOT_UPDATE_TICKER, QOT_SUB, QOT_GET_MARKET_STATE};
use crate::mods::qot_common::{Security, SubType};
use crate::mods::qot_get_market_state::MarketInfo;
use crate::mods::{qot_sub, qot_update_ticker};
use prost::Message;
use tokio::io::{AsyncWriteExt};
use tokio::net::TcpStream;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

const OPEND_ADDR: &str = "127.0.0.1:11111";
const CLIENT_VER: i32 = 101;
const CLIENT_ID: &str = "moomoo_adaptor";
const SEQ_ORDER: Ordering = Ordering::Relaxed;

fn get_seq_no(counter: &Arc<AtomicU32>) -> u32{
    counter.fetch_add(1, SEQ_ORDER)
}

async fn establish_connection(counter: &Arc<AtomicU32>) -> Result<TcpStream, anyhow::Error>{
    let mut stream = TcpStream::connect(OPEND_ADDR).await?;

    let init_body = build_init_conn_req(CLIENT_VER, CLIENT_ID.to_string())?;

    stream.write_all(&build_frame(INIT_CONNECT, get_seq_no(counter), &init_body))
    .await?;

    let(_header, body) = read_frame(&mut stream).await?;
    let init_resp = handle_response(&body)?;

    if init_resp.ret_type != 0 {
        anyhow::bail!("init_connect failed: {:?}", init_resp.ret_msg);
    }

    let s2c = init_resp.s2c.ok_or_else( || anyhow::anyhow!("InitConnect reply missing s2c"))?;

    println!("connected: conn_id={} keep_alive_interval={}s", s2c.conn_id, s2c.keep_alive_interval);

    Ok(stream)
}

async fn check_market_state(counter: &Arc<AtomicU32>, security: Security, stream: &mut TcpStream) -> anyhow::Result<Vec<MarketInfo>> {
    let state_body = build_market_state_request(security)?;
    stream.write_all(&build_frame(QOT_GET_MARKET_STATE, get_seq_no(counter), &state_body)).await?;
    let (_header, body) = read_frame(stream).await?;
    parse_market_state_response(&body)
}

async fn subscribe_ticker(counter: &Arc<AtomicU32>, stream: &mut TcpStream, market: i32, code: &str) -> anyhow::Result<()> {
    let sub_body = build_sub_request(market, code.to_string(), vec![SubType::Ticker])?;
    stream.write_all(&build_frame(QOT_SUB, get_seq_no(counter), &sub_body)).await?;
    let (_header, body) = read_frame(stream).await?;
    let sub_resp = qot_sub::Response::decode(body.as_slice())?;
    if sub_resp.ret_type != 0 {
        anyhow::bail!("Sub failed: {:?}", sub_resp.ret_msg);
    }
    Ok(())
}

pub async fn stream_ticker(market: i32, code: &str) -> anyhow::Result<()> {
    
    let counter = Arc::new(AtomicU32::new(1)); // start at 1 cos 0 means failed

    let target_security = Security{
        market,
        code: code.to_string(),
    };

    // init TCP handshake
    let mut stream = establish_connection(&counter).await?;

    // check market state before subscribing
    let market_info_vec = check_market_state(&counter, target_security, &mut stream).await?;
    for item in &market_info_vec{
        println!("market with id:{} and name:{} has status{}", item.security.market, item.name, item.market_state);
        println!("proceeding to subscribe to ticker")
    }

    // --- Subscribe to the trade tape (Ticker) for this security ---
    subscribe_ticker(&counter,  &mut stream, market, code).await?;
    println!("subscribed to {code} for Ticker pushes");

    // --- From here on, pushes arrive unprompted ---
    loop {
        let (header, body) = read_frame(&mut stream).await?;

        if header.proto_id != QOT_UPDATE_TICKER {
            println!("ignoring unhandled proto_id={}", header.proto_id);
            continue;
        }

        let push = qot_update_ticker::Response::decode(body.as_slice())?;
        let Some(s2c) = push.s2c else { continue };
        let ticker_code = s2c.security.code.clone();

        for t in s2c.ticker_list {
            let side = match t.dir {
                1 => "BUY",
                2 => "SELL",
                3 => "NEUTRAL",
                _ => "UNKNOWN",
            };
            println!(
                "{ticker_code} {side} seq={} price={} volume={}",
                t.sequence, t.price, t.volume
            );
        }
    }
}
