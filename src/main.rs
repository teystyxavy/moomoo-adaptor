mod client;
mod mods;
mod engine;
mod frame;
mod live_subscriber;
mod market_state_querier;
mod model;
mod questdb;
mod config;
mod metrics;

use std::hash::Hash;
use std::sync::Arc;

use config::Config;
use tokio::{task::JoinHandle};
use tokio::sync::{broadcast, mpsc};

use questdb::writer;

use futures::future::join_all;
use crate::questdb::schema::{self, TickerTick, OrderBookLevel};
use std::collections::HashMap;

const BUFFER_CAP: usize = 2048;

fn spawn_bridge<T>(bus: &broadcast::Sender<T>, sink: mpsc::Sender<T>, label: &'static str)
where
    T: Clone + Send + 'static,
{
    let mut bridge_rx = bus.subscribe();
    tokio::spawn(async move {
        loop {
            match bridge_rx.recv().await {
                Ok(item) => { let _ = sink.send(item).await; }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    eprintln!("{label} bridge lagged, dropped {n} item(s)");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });
}


#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Loads .env into the process environment if present; .ok() because a
    // missing .env is fine — real env vars set some other way still work.
    dotenvy::dotenv().ok();

    let cfg = Config::from_env()?;
    let tick_stats = Arc::new(metrics::QdbStats::default());
    let ob_stats = Arc::new(metrics::QdbStats::default());
    let qdb_writer = writer::spawn_writer::<TickerTick>(&cfg.questdb_conf, tick_stats.clone())?;
    let ob_writer = writer::spawn_writer::<OrderBookLevel>(&cfg.questdb_conf, ob_stats.clone())?;
    let mut symbols: Vec<String> = vec![];
    let mut handles: Vec<JoinHandle<()>> = vec![];
    let reconnect_counter: HashMap<String, Arc<metrics::ReconnectCounter>> = cfg.securities.iter()
        .map(|sec| (sec.code.clone(), Arc::new(metrics::ReconnectCounter::default())))
        .collect();

    let (bus_tx,  _) = broadcast::channel::<schema::TickerTick>(BUFFER_CAP);
    let (ob_bus_tx, _) = broadcast::channel::<schema::OrderBookLevel>(BUFFER_CAP);
    // QuestDB bridge: the bus's one current subscriber. Must exist before any
    // ticker starts sending — broadcast::send() with zero subscribers just
    // drops the message, it doesn't buffer it for a subscriber that joins later.
    spawn_bridge(&bus_tx, qdb_writer.clone(), "QuestDB");
    spawn_bridge(&ob_bus_tx, ob_writer.clone(), "order-book");

    let mut tick_rx = bus_tx.subscribe();
    let mut ob_rx = ob_bus_tx.subscribe();
    let bus_for_len = bus_tx.clone();
    let ob_bus_for_len = ob_bus_tx.clone();
    let reconnect_counter_move = reconnect_counter.clone();

    tokio::spawn(async move {
        let mut tick_counts = HashMap::new();
        let mut ob_counts = HashMap::new();
        let mut report = tokio::time::interval(std::time::Duration::from_secs(10));
        loop {
            tokio::select!{
                maybe_tick = tick_rx.recv() => {
                    match maybe_tick {
                        Ok(tick) => { *tick_counts.entry(tick.symbol).or_insert(0) += 1;},
                        Err(broadcast::error::RecvError::Lagged(n)) => eprintln!("metrics: tick bus lagged, missed{n}"),
                        Err(broadcast::error::RecvError::Closed) => {}
                    }
                }
                maybe_ob = ob_rx.recv() => {
                    match maybe_ob {
                        Ok(row) => { *ob_counts.entry(row.symbol).or_insert(0) += 1; }
                        Err(broadcast::error::RecvError::Lagged(n)) => eprintln!("metrics: order-book bus lagged, missed {n}"),
                        Err(broadcast::error::RecvError::Closed) => {}
                    }
                }

                _ = report.tick() => {
                    for (symbol, count) in tick_counts.drain() {
                        println!("{symbol}: {:.1} ticks/sec", count as f64 / 10.0);
                    }
                    for (symbol, count) in ob_counts.drain() {
                        println!("{symbol}: {:.1} order-book updates/sec", count as f64 / 10.0);
                    }
                    for (symbol, counter) in &reconnect_counter_move {
                        let n = counter.take();
                        if n > 0 { println!("{symbol}: {n} reconnect(s) in last 10s"); }
                    }
                    println!(
                        "tick bus: {} buffered | qdb ticks: last flush {}µs, {} row(s)",
                        bus_for_len.len(),
                        tick_stats.last_flush_micros.load(std::sync::atomic::Ordering::Relaxed),
                        tick_stats.rows_flushed.swap(0, std::sync::atomic::Ordering::Relaxed),
                    );
                    println!(
                        "ob bus: {} buffered | qdb order-book: last flush {}µs, {} row(s)",
                        ob_bus_for_len.len(),
                        ob_stats.last_flush_micros.load(std::sync::atomic::Ordering::Relaxed),
                        ob_stats.rows_flushed.swap(0, std::sync::atomic::Ordering::Relaxed),
                    );
                }
            
            }
        }
    });
    
    // ticker streaming loop
    for sec in cfg.securities{
        let bus_tx = bus_tx.clone();
        let ob_bus_tx = ob_bus_tx.clone();
        let symbol = sec.code.clone();
        let reconnects = Arc::clone(&reconnect_counter[&sec.code]);
        symbols.push(symbol.clone());
        handles.push(tokio::spawn(async move {
            match engine::stream_ticker(
                sec,
                cfg.initial_backoff.clone(),
                cfg.healthy_threshold.clone(),
                cfg.max_retries,
                cfg.max_backoff,
                bus_tx,
                ob_bus_tx,
                reconnects,
                cfg.verbose_ticks,
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
