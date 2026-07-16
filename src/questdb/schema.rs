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