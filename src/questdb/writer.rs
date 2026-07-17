use questdb::ingress::{Buffer, Sender};
use tokio::sync::mpsc;

use crate::questdb::schema::{IlpRow, TickerTick};

const MAX_BATCH: usize = 500;

fn append_row<T:IlpRow>(buffer: &mut Buffer, row: &T) -> bool {
    if let Err(e) = buffer.set_marker() {
        eprint!("failed to set buffer marker {e}");
        return false;
    }


    match row.write_into(buffer) {
        Ok(_) => {buffer.clear_marker(); true},
        Err(e) => {
            eprintln!("failed to append row due to: {e}"); 
            if let Err(e2) = buffer.rewind_to_marker() {
                eprintln!("failed to rewind buffer after bad row: {e2}");
            }
            false
        },
    }
        
}

pub fn spawn_writer<T>(quest_conf: &str) -> anyhow::Result<mpsc::Sender<T>> 
where T: IlpRow + Send + 'static,
{
    let (db_tx, mut db_rx) = mpsc::channel::<T>(256);
    let quest_conf = quest_conf.to_string();

    tokio::task::spawn_blocking(move ||{
        let mut sender = match Sender::from_conf(&quest_conf){
            Ok(s) => s,
            Err(e) => {
                eprintln!("failed to connect to QuestDB: {e}");
                return;
            }
        };

        loop {
            let first = match db_rx.blocking_recv() {
                Some(t) => t,
                None => break, // all senders dropped, done
            };

            let mut buffer = sender.new_buffer();
            let mut count = 0;
            if append_row(&mut buffer, &first) {
                count += 1;
                while count < MAX_BATCH {
                    match db_rx.try_recv(){
                        Ok(tick) => {
                            if append_row(&mut buffer, &tick) { count+=1;}
                        },
                        Err(_) => break,
                    }
                }
            }

            if let Err(e) = sender.flush(&mut buffer){
                eprintln!("failed to flush {count} row(s) to QuestDB: {e}");
            }

        }
    });

    Ok(db_tx)
}   