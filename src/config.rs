use std::time::Duration;

use crate::mods::qot_common::{QotMarket, Security};

pub struct Config {
    pub opend_addr: String,
    pub questdb_conf: String,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub max_retries: u8,
    pub healthy_threshold: Duration,
    pub securities: Vec<Security>,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Config> {
        Ok(Config {
            opend_addr: env_or("OPEND_ADDR", "127.0.0.1:11111".to_string())?,
            questdb_conf: env_or("QDB_CLIENT_CONF", "http::addr=localhost:9000;".to_string())?,
            initial_backoff: Duration::from_secs(env_or("RETRY_INITIAL_BACKOFF_SECS", 1)?),
            max_backoff: Duration::from_secs(env_or("RETRY_MAX_BACKOFF_SECS", 60)?),
            max_retries: env_or("RETRY_MAX_ATTEMPTS", 5)?,
            healthy_threshold: Duration::from_secs(env_or("RETRY_HEALTHY_THRESHOLD_SECS", 30)?),
            securities: match std::env::var("MOOMOO_SECURITIES") {
                Ok(val) => parse_securities(&val)?,
                Err(std::env::VarError::NotPresent) => vec![Security{
                    market: QotMarket::CcSecurity as i32,
                    code: "BTC".to_string(),
                }],
                Err(e) =>  anyhow::bail!("failed to read MOOMOO_SECURITIES: {e}"),
            },
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

fn parse_securities(val: &str) -> anyhow::Result<Vec<Security>>{
    val.split(',')
        .map(|entry| {
            let (market_str, code) = entry
                .split_once(':')
                .ok_or_else(|| anyhow::anyhow!("invalid security '{entry}', expected market:code"))?;
            let market: i32 = market_str
                .parse()
                .map_err(|e| anyhow::anyhow!("invalid market in '{entry}': {e}"))?;
            Ok(Security {market, code: code.to_string()})
        }).collect()
}
