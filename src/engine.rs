use crate::client::{build_init_conn_req, build_keep_alive_ereq, handle_response};
use crate::frame::{build_frame, read_frame, FrameHeader};
use crate::live_subscriber::subscribe::{build_sub_request};
use crate::market_state_querier::market_state_querier::{build_market_state_request, parse_market_state_response};
use crate::model::proto_ids::{INIT_CONNECT, KEEP_ALIVE, QOT_UPDATE_TICKER, QOT_SUB, QOT_GET_MARKET_STATE};
use crate::mods::qot_common::{Security, SubType};
use crate::mods::qot_get_market_state::MarketInfo;
use crate::mods::{qot_sub, qot_update_ticker};
use prost::Message;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use crate::questdb::{writer, schema};
use tokio::sync::mpsc::Sender;
use crate::config::Config;


const OPEND_ADDR: &str = "127.0.0.1:11111";
const CLIENT_VER: i32 = 101;
const CLIENT_ID: &str = "moomoo_adaptor";
const SEQ_ORDER: Ordering = Ordering::Relaxed;

async fn run_once(target_security: &Security, writer: &Sender<schema::TickerTick>) -> anyhow::Result<()>{
    let market =  target_security.market;
    let code = target_security.code.to_string();
    let counter = Arc::new(AtomicU32::new(1)); // start at 1 cos 0 means failed

    // init TCP handshake
    let (mut stream, heartbeat_duration) = establish_connection(&counter).await?;

    // check market state before subscribing
    let market_info_vec = check_market_state(&counter, target_security, &mut stream).await?;
    for item in &market_info_vec{
        println!("market with id:{} and name:{} has status{}", item.security.market, item.name, item.market_state);
        println!("proceeding to subscribe to ticker")
    }

    // --- Subscribe to the trade tape (Ticker) for this security ---
    subscribe_ticker(&counter, &mut stream, market, &code).await?;
    println!("subscribed to {code} for Ticker pushes");

    // --- From here on, pushes arrive unprompted ---
    // owning split (not .split()) so the reader half can move into its own spawned task —
    // read_exact isn't cancel-safe, so it must never be raced directly against the heartbeat
    // timer inside a single select!; the channel is what's safe to select! against instead.
    let (mut read_half, mut write_half) = stream.into_split();

    let (frame_tx, mut frame_rx) = mpsc::channel::<anyhow::Result<(FrameHeader, Vec<u8>)>>(64);

    let reader_handle =  tokio::spawn(async move {
        loop {
            let result = read_frame(&mut read_half).await;
            let is_err = result.is_err();
            if frame_tx.send(result).await.is_err() {
                break; // main loop is gone, nothing left to send to
            }
            if is_err {
                break;
            }
        }
    });

    let result: anyhow::Result<()> = async {
        let mut heartbeat = interval(Duration::from_secs(heartbeat_duration as u64));
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    let ka_body = build_keep_alive_ereq()?;
                    write_half
                        .write_all(&build_frame(KEEP_ALIVE, get_seq_no(&counter), &ka_body))
                        .await?;
                }

                maybe_frame = frame_rx.recv() => {
                    let (header, body) = match maybe_frame {
                        Some(result) => result?,
                        None => anyhow::bail!("reader task ended unexpectedly"),
                    };

                    if header.proto_id == KEEP_ALIVE {
                        println!("keep-alive reply received");
                        continue;
                    }

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
                        // create new row and send to buffer to write to questdb
                        let new_row = schema::TickerTick{
                            symbol: ticker_code.clone(),
                            price: t.price,
                            volume: t.volume,
                            side: side,
                            sequence: t.sequence,
                        };
                        writer.send(new_row).await?;
                        println!(
                            "{ticker_code} {side} seq={} price={} volume={}",
                            t.sequence, t.price, t.volume
                        );
                    }
                }
            }
        }
    }.await;

    reader_handle.abort();
    result
}

fn get_seq_no(counter: &Arc<AtomicU32>) -> u32{
    counter.fetch_add(1, SEQ_ORDER)
}

async fn establish_connection(counter: &Arc<AtomicU32>) -> Result<(TcpStream, i32), anyhow::Error>{
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

    Ok((stream, s2c.keep_alive_interval))
}

async fn check_market_state(counter: &Arc<AtomicU32>, security: &Security, stream: &mut TcpStream) -> anyhow::Result<Vec<MarketInfo>> {
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

pub async fn stream_ticker(config: Config) -> anyhow::Result<()> {
    let writer = writer::spawn_writer(&config.questdb_conf)?;

    let target_security = Security{
        market: config.market,
        code: config.ticker.clone(),
    };

    let mut backoff = config.initial_backoff;
    let mut consecutive_setup_failures = 0;

    let result: anyhow::Result<()> = loop {
        let started = std::time::Instant::now();
        match run_once(&target_security, &writer).await {
            Ok(()) => break Ok(()),
            Err(e) => {
                if started.elapsed() > config.healthy_threshold {
                    // reset setup failures
                    backoff = config.initial_backoff;
                    consecutive_setup_failures = 0;
                }

                consecutive_setup_failures += 1;
                if consecutive_setup_failures > config.max_retries  {
                    break Err(e);
                }
                eprintln!("connection error: {e:?}, retrying in {backoff:?}");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(config.max_backoff);
            }
        }
    };

    result
}
