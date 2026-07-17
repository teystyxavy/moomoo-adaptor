mod client;
mod mods;
mod engine;
mod frame;
mod live_subscriber;
mod market_state_querier;
mod model;
mod questdb;
mod config;

use config::Config;
use tokio::{task::JoinHandle};
use tokio::sync::broadcast;

use questdb::writer;

use futures::future::join_all;

use crate::questdb::schema;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Loads .env into the process environment if present; .ok() because a
    // missing .env is fine — real env vars set some other way still work.
    dotenvy::dotenv().ok();

    let cfg = Config::from_env()?;
    let qdb_writer = writer::spawn_writer(&cfg.questdb_conf)?;
    let mut symbols: Vec<String> = vec![];
    let mut handles: Vec<JoinHandle<()>> = vec![];

    let (bus_tx,  _) = broadcast::channel::<schema::TickerTick>(2048);

    // QuestDB bridge: the bus's one current subscriber. Must exist before any
    // ticker starts sending — broadcast::send() with zero subscribers just
    // drops the message, it doesn't buffer it for a subscriber that joins later.
    {
        let mut bridge_rx = bus_tx.subscribe();
        let bridge_writer = qdb_writer.clone();
        tokio::spawn(async move {
            loop {
                match bridge_rx.recv().await {
                    Ok(tick) => { let _ = bridge_writer.send(tick).await; }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("QuestDB bridge lagged, dropped {n} tick(s)");
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    for sec in cfg.securities{
        let bus_tx = bus_tx.clone();
        let symbol = sec.code.clone();
        symbols.push(symbol.clone());
        handles.push(tokio::spawn(async move {
            match engine::stream_ticker(
                sec,
                cfg.initial_backoff.clone(),
                cfg.healthy_threshold.clone(),
                cfg.max_retries,
                cfg.max_backoff,
                bus_tx,
           ).await {
                Ok(()) => println!("{symbol}: finished"),
                Err(e) => eprintln!("{symbol}: gave up: {e:?}"),
            }
        }
        ))
    };

    
    let results = join_all(handles).await;
    for (symbol, result) in symbols.into_iter().zip(results) {
        if let Err(join_err) = result {
            eprintln!("{symbol}: panicked: {join_err:?}");
        }
    }

    Ok(())
}
