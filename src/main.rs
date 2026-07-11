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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_env()?;
    engine::stream_ticker(cfg).await
}
