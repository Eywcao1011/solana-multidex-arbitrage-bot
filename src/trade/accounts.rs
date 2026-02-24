use std::{collections::HashMap, str::FromStr};

use anyhow::{bail, Context, Result};
use log::debug;
use solana_pubkey::Pubkey;

use crate::dex::{PoolSnapshot, TradeSide};

use super::TradeAccounts;

/// 管理针对不同池子配置的交易账户（payer / base ATA / quote ATA）。
/// 配置来源于 `TRADE_ACCOUNTS` 环境变量，支持按方向覆盖：
/// `pool=<pubkey>|side=buy|payer=...|base=...|quote=...`。
#[derive(Debug, Default, Clone)]
pub struct TradeAccountManager {
    per_pool: HashMap<Pubkey, PoolAccountConfig>,
}

#[derive(Debug, Default, Clone)]
struct PoolAccountConfig {
    default: Option<TradeAccounts>,
    buy: Option<TradeAccounts>,
    sell: Option<TradeAccounts>,
}

impl TradeAccountManager {
    pub fn from_env() -> Self {
        match load_accounts_from_env() {
            Ok(map) => Self { per_pool: map },
            Err(err) => {
                debug!("TRADE_ACCOUNTS env parse failed: {err:?}; using defaults");
                Self::default()
            }
        }
    }

    pub fn resolve_for(&self, snapshot: &PoolSnapshot, side: TradeSide) -> TradeAccounts {
        self.per_pool
            .get(&snapshot.descriptor.address)
            .and_then(|cfg| cfg.resolve(side))
            .unwrap_or_default()
    }
}

fn load_accounts_from_env() -> Result<HashMap<Pubkey, PoolAccountConfig>> {
    let raw = std::env::var("TRADE_ACCOUNTS").unwrap_or_default();
    if raw.trim().is_empty() {
        return Ok(HashMap::new());
    }

    let mut map: HashMap<Pubkey, PoolAccountConfig> = HashMap::new();
    for entry in raw
        .split(|c| matches!(c, ';' | '\n' | ','))
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let (pool, side, accounts) = parse_entry(entry)?;
        let cfg = map.entry(pool).or_default();
        cfg.assign(side, accounts)?;
    }
    Ok(map)
}

fn parse_entry(entry: &str) -> Result<(Pubkey, ConfigSide, TradeAccounts)> {
    let mut pool = None;
    let mut payer = None;
    let mut base = None;
    let mut quote = None;
    let mut side = ConfigSide::Both;

    for part in entry.split('|') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (key, value) = part
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("missing '=' in entry segment '{part}'"))?;
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "pool" => pool = Some(parse_pubkey(value, "pool")?),
            "payer" => payer = Some(parse_pubkey(value, "payer")?),
            "base" | "user_base" | "user_base_account" => base = Some(parse_pubkey(value, "base")?),
            "quote" | "user_quote" | "user_quote_account" => {
                quote = Some(parse_pubkey(value, "quote")?)
            }
            "side" => {
                side = match value.to_ascii_lowercase().as_str() {
                    "buy" => ConfigSide::Buy,
                    "sell" => ConfigSide::Sell,
                    "both" => ConfigSide::Both,
                    other => bail!("invalid side '{other}' in TRADE_ACCOUNTS entry"),
                }
            }
            other => bail!("unknown key '{other}' in TRADE_ACCOUNTS entry"),
        }
    }

    let pool = pool.ok_or_else(|| anyhow::anyhow!("pool missing in TRADE_ACCOUNTS entry"))?;
    let payer = payer.ok_or_else(|| anyhow::anyhow!("payer missing for pool {pool}"))?;
    let base = base.ok_or_else(|| anyhow::anyhow!("base account missing for pool {pool}"))?;
    let quote = quote.ok_or_else(|| anyhow::anyhow!("quote account missing for pool {pool}"))?;

    Ok((
        pool,
        side,
        TradeAccounts {
            payer,
            user_base_account: base,
            user_quote_account: quote,
        },
    ))
}

fn parse_pubkey(value: &str, field: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("invalid pubkey for {field}: {value}"))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigSide {
    Buy,
    Sell,
    Both,
}

impl PoolAccountConfig {
    fn assign(&mut self, side: ConfigSide, accounts: TradeAccounts) -> Result<()> {
        let slot = match side {
            ConfigSide::Buy => &mut self.buy,
            ConfigSide::Sell => &mut self.sell,
            ConfigSide::Both => &mut self.default,
        };
        if slot.is_some() {
            bail!("duplicate TRADE_ACCOUNTS entry for pool (side={side:?})");
        }
        *slot = Some(accounts);
        Ok(())
    }

    fn resolve(&self, side: TradeSide) -> Option<TradeAccounts> {
        match side {
            TradeSide::Buy => self.buy.clone().or_else(|| self.default.clone()),
            TradeSide::Sell => self.sell.clone().or_else(|| self.default.clone()),
        }
    }
}

