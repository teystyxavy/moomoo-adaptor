mod client;
mod mods;
mod engine;
mod frame;
mod live_subscriber;
mod market_state_querier;
mod model;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // US market (11), example ticker — swap for whatever you're watching
    engine::stream_ticker(11, "AAPL").await
}
