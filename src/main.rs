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

use questdb::writer;

use futures::future::join_all;


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Loads .env into the process environment if present; .ok() because a
    // missing .env is fine — real env vars set some other way still work.
    dotenvy::dotenv().ok();

    let cfg = Config::from_env()?;
    let writer = writer::spawn_writer(&cfg.questdb_conf)?;
    let mut symbols: Vec<String> = vec![];
    let mut handles: Vec<JoinHandle<()>> = vec![];
    
    for sec in cfg.securities{
        let c_writer = writer.clone();
        let symbol = sec.code.clone();
        symbols.push(symbol.clone());
        handles.push(tokio::spawn(async move {
            match engine::stream_ticker(
                sec, 
                cfg.initial_backoff.clone(),
                cfg.healthy_threshold.clone(),
                cfg.max_retries,
                cfg.max_backoff,
                c_writer,
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
