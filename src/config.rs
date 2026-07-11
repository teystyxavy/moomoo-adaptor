use std::time::Duration;

use crate::mods::qot_common::QotMarket;

pub struct Config {
    pub opend_addr: String,
    pub market: i32,
    pub ticker: String,
    pub questdb_conf: String,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retries: u8,
    pub healthy_threshold: Duration,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Config> {
        Ok(Config {
            opend_addr: env_or("OPEND_ADDR", "127.0.0.1:11111".to_string())?,
            market: env_or("MOOMOO_MARKET", QotMarket::CcSecurity as i32)?,
            ticker: env_or("MOOMOO_TICKER", "BTC".to_string())?,
            questdb_conf: env_or("QDB_CLIENT_CONF", "http::addr=localhost:9000;".to_string())?,
            initial_backoff: Duration::from_secs(env_or("RETRY_INITIAL_BACKOFF_SECS", 1)?),
            max_backoff: Duration::from_secs(env_or("RETRY_MAX_BACKOFF_SECS", 60)?),
            max_retries: env_or("RETRY_MAX_ATTEMPTS", 5)?,
            healthy_threshold: Duration::from_secs(env_or("RETRY_HEALTHY_THRESHOLD_SECS", 30)?),
        })
    }
}

/// Reads `key` from the environment and parses it as `T`; falls back to
/// `default` if unset. A value that IS set but fails to parse is a hard
/// error rather than a silent fallback — an override that's silently
/// ignored is worse than one that fails loudly.
fn env_or<T>(key: &str, default: T) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(key) {
        Ok(val) => val
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("invalid value for {key}: {e}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(anyhow::anyhow!("failed to read env var {key}: {e}")),
    }
}
