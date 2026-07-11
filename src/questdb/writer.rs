use questdb::{ingress::{Sender}};
use tokio::sync::mpsc;

use crate::questdb::schema::TickerTick;

pub fn spawn_writer(quest_conf: &str) -> anyhow::Result<mpsc::Sender<TickerTick>> {
    let (db_tx, mut db_rx) = mpsc::channel::<TickerTick>(256);
    let quest_conf = quest_conf.to_string();

    tokio::task::spawn_blocking(move ||{
        let mut sender = match Sender::from_conf(&quest_conf){
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to connect to QuestDB: {e}");
                return;
            }
        };
        
        while let Some(tick) = db_rx.blocking_recv(){
            let mut buffer = sender.new_buffer();
            if let Err(e) = buffer.table("ticks")
                .and_then(|b| b.symbol("symbol", &tick.symbol))
                .and_then(|b| b.column_f64("price", tick.price))
                .and_then(|b| b.column_i64("volume", tick.volume))
                .and_then(|b| b.column_i64("sequence", tick.sequence))
                .and_then(|b| b.at_now())
            {
                eprint!("failed to build row: {e}");
                continue;
            }
            if let Err(e) = sender.flush(&mut buffer) {
                eprintln!("failed to flush to QuestDB: {e}");
            }
        }
    });

    Ok(db_tx)
}   