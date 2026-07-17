use questdb::ingress::{Buffer, TimestampNanos};

#[derive(Clone)]
pub struct TickerTick{
    pub symbol: String,
    pub side: &'static str,
    pub price: f64,
    pub volume: i64,
    pub sequence: i64,
    pub event_time_ns: Option<i64>, // from push's own time field
    pub received_at_ns: i64, // decode-time clock
}

#[derive(Clone)]
pub struct OrderBookLevel {
    pub symbol: String,
    pub side: &'static str, // "BID" or "ASK"
    pub level: i32,
    pub price: f64,
    pub volume: i64,
    pub order_count: i32,
    pub received_at_ns: i64,
}

pub trait IlpRow {
    fn write_into(&self, buffer: &mut Buffer) -> questdb::Result<()>;
}

impl IlpRow for TickerTick {
    fn write_into(&self, buffer: &mut Buffer) -> questdb::Result<()>{
        let row = buffer.table("ticks")
            .and_then(|b| b.symbol("symbol", &self.symbol))
            .and_then(|b| b.symbol("side", self.side))
            .and_then(|b| b.column_f64("price", self.price))
            .and_then(|b| b.column_i64("volume", self.volume))
            .and_then(|b| b.column_i64("sequence", self.sequence))
            .and_then(|b| b.column_i64("received_at_ns", self.received_at_ns))?;

        match self.event_time_ns {
            Some(ns) => row.at(TimestampNanos::new(ns)),
            None => row.at_now(),
        }
    }
}

impl IlpRow for OrderBookLevel{
    fn write_into(&self, buffer: &mut Buffer) -> questdb::Result<()>{
        buffer.table("order_book_level")
            .and_then(|b| b.symbol("symbol", &self.symbol))
            .and_then(|b| b.symbol("side", self.side))
            .and_then(|b| b.column_i64("level", self.level as i64))
            .and_then(|b| b.column_f64("price", self.price))
            .and_then(|b| b.column_i64("volume", self.volume))
            .and_then(|b| b.column_i64("order_count", self.order_count as i64))
            .and_then(|b| b.column_i64("received_at_ns", self.received_at_ns))?
            .at(TimestampNanos::new(self.received_at_ns))
    }
}
