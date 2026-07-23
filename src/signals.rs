use std::collections::VecDeque;
use crate::questdb::schema::TickerTick;

use questdb::ingress::{Buffer, TimestampNanos};
use crate::questdb::schema::IlpRow;

const WINDOW: usize=20;

pub struct Signal {
    pub symbol: String,
    pub dir: &'static str,
    pub price: f64,
    pub moving_avg: f64,
    pub received_at_ns: i64,
}

#[derive(Default)]
pub struct MovingAvgState {
    window: VecDeque<f64>,
    last_side: Option<&'static str>,
}

impl MovingAvgState{ 
    pub fn update(&mut self, tick: &TickerTick) -> Option<Signal>{
        self.window.push_back(tick.price);
        if self.window.len() > WINDOW {
            self.window.pop_front();
        }
        if self.window.len() < WINDOW{
            return None;
        }

        let ma = self.window.iter().sum::<f64>() / self.window.len() as f64;
        let side = if tick.price > ma {"BUY"} else {"SELL"};

        let signal = if self.last_side != Some(side)  {
            Some(Signal{
                symbol: tick.symbol.clone(),
                dir: side,
                price: tick.price,
                moving_avg: ma,
                received_at_ns: tick.received_at_ns,
            })
        } else {
            None
        };

        self.last_side = Some(side);
        signal
    }
}

impl IlpRow for Signal {
    fn write_into(&self, buffer: &mut Buffer) -> questdb::Result<()>{
        buffer.table("signals")
            .and_then(|b| b.symbol("symbol", &self.symbol))
            .and_then(|b| b.symbol("dir", self.dir))
            .and_then(|b| b.column_f64("price", self.price))
            .and_then(|b| b.column_f64("moving_avg", self.moving_avg))?
            .at(TimestampNanos::new(self.received_at_ns))
    }
}