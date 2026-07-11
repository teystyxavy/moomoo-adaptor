pub struct TickerTick{
    pub symbol: String,
    pub side: &'static str,
    pub price: f64,
    pub volume: i64,
    pub sequence: i64,
}