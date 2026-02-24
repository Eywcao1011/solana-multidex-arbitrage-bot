use std::{collections::HashMap, fs, path::Path, str::FromStr};

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;
use solana_pubkey::Pubkey;

use crate::dex::TradeSide;

#[derive(Debug, Deserialize)]
struct DlmmConfigFile {
    #[serde(default)]
    signer_keypair: Option<String>,
    #[serde(default)]
    max_simulations_per_minute: Option<u32>,
    #[serde(default)]
    min_interval_secs: Option<u64>,
    #[serde(default)]
    default_slippage_bps: Option<f64>,
    #[serde(default)]
    pools: Vec<DlmmPoolEntry>,
}

#[derive(Debug, Deserialize)]
struct DlmmPoolEntry {
    pool: String,
    #[serde(default)]
    trade_side: Option<String>,
    amount_in: u64,
    user: String,
    user_token_in: String,
    user_token_out: String,
    reserve_x: String,
    reserve_y: String,
    token_x_mint: String,
    token_y_mint: String,
    oracle: String,
    host_fee_in: Option<String>,
    #[serde(default)]
    token_x_program: Option<String>,
    #[serde(default)]
    token_y_program: Option<String>,
    bin_array_bitmap_extension: Option<String>,
    #[serde(default)]
    additional_remaining_accounts: Vec<String>,
    #[serde(default)]
    slippage_bps: Option<f64>,
}

#[derive(Clone)]
pub struct DlmmConfig {
    pub signer_keypair: Option<String>,
    pub max_simulations_per_minute: Option<u32>,
    pub min_interval_secs: Option<u64>,
    pub default_slippage_bps: f64,
    pub pools: HashMap<Pubkey, DlmmPoolConfig>,
}

#[derive(Clone)]
pub struct DlmmPoolConfig {
    pub pool: Pubkey,
    pub trade_side: TradeSide,
    pub amount_in: u64,
    pub slippage_bps: Option<f64>,
    pub user: Pubkey,
    pub user_token_in: Pubkey,
    pub user_token_out: Pubkey,
    pub reserve_x: Pubkey,
    pub reserve_y: Pubkey,
    pub token_x_mint: Pubkey,
    pub token_y_mint: Pubkey,
    pub oracle: Pubkey,
    pub host_fee_in: Option<Pubkey>,
    pub token_x_program: Pubkey,
    pub token_y_program: Pubkey,
    pub bin_array_bitmap_extension: Option<Pubkey>,
    pub additional_remaining_accounts: Vec<Pubkey>,
    /// ⭐ 缓存的 bin_array 地址（来自 sidecar introspect，经过验证）
    /// Deprecated: use directional arrays instead
    pub cached_bin_arrays: Vec<Pubkey>,
    /// ⭐ Sell 方向 (X→Y) 的 bin_arrays
    pub cached_bin_arrays_x_to_y: Vec<Pubkey>,
    /// ⭐ Buy 方向 (Y→X) 的 bin_arrays
    pub cached_bin_arrays_y_to_x: Vec<Pubkey>,
}

impl DlmmConfig {
    pub fn load_from_path(path: &Path) -> Result<Self> {
        let data = fs::read_to_string(path)
            .with_context(|| format!("read DLMM config file {}", path.display()))?;
        let parsed: DlmmConfigFile = serde_json::from_str(&data)
            .with_context(|| format!("parse DLMM config file {}", path.display()))?;

        let default_slippage_bps = parsed.default_slippage_bps.unwrap_or(50.0).max(0.1);

        let mut pools = HashMap::new();
        for entry in parsed.pools {
            let pool = parse_pubkey(&entry.pool, "pool")?;
            let trade_side = match entry
                .trade_side
                .as_ref()
                .map(|s| s.trim().to_lowercase())
                .as_deref()
            {
                None | Some("sell") => TradeSide::Sell,
                Some("buy") => TradeSide::Buy,
                Some(other) => {
                    return Err(anyhow!("unknown trade_side '{}' for pool {}", other, pool));
                }
            };

            let token_x_program = entry
                .token_x_program
                .as_ref()
                .map(|value| parse_pubkey(value, "token_x_program"))
                .transpose()?;
            let token_y_program = entry
                .token_y_program
                .as_ref()
                .map(|value| parse_pubkey(value, "token_y_program"))
                .transpose()?;

            let config = DlmmPoolConfig {
                pool,
                trade_side,
                amount_in: entry.amount_in,
                slippage_bps: entry.slippage_bps,
                user: parse_pubkey(&entry.user, "user")?,
                user_token_in: parse_pubkey(&entry.user_token_in, "user_token_in")?,
                user_token_out: parse_pubkey(&entry.user_token_out, "user_token_out")?,
                reserve_x: parse_pubkey(&entry.reserve_x, "reserve_x")?,
                reserve_y: parse_pubkey(&entry.reserve_y, "reserve_y")?,
                token_x_mint: parse_pubkey(&entry.token_x_mint, "token_x_mint")?,
                token_y_mint: parse_pubkey(&entry.token_y_mint, "token_y_mint")?,
                oracle: parse_pubkey(&entry.oracle, "oracle")?,
                host_fee_in: entry
                    .host_fee_in
                    .as_ref()
                    .map(|value| parse_pubkey(value, "host_fee_in"))
                    .transpose()?,
                token_x_program: token_x_program.unwrap_or(spl_token::ID),
                token_y_program: token_y_program.unwrap_or(spl_token::ID),
                bin_array_bitmap_extension: entry
                    .bin_array_bitmap_extension
                    .as_ref()
                    .map(|value| parse_pubkey(value, "bin_array_bitmap_extension"))
                    .transpose()?,
                additional_remaining_accounts: entry
                    .additional_remaining_accounts
                    .into_iter()
                    .map(|value| parse_pubkey(&value, "additional_remaining_account"))
                    .collect::<Result<Vec<_>>>()?,
                cached_bin_arrays: vec![], // deprecated
                cached_bin_arrays_x_to_y: vec![],
                cached_bin_arrays_y_to_x: vec![],
            };

            pools.insert(pool, config);
        }

        Ok(Self {
            signer_keypair: parsed.signer_keypair,
            max_simulations_per_minute: parsed.max_simulations_per_minute,
            min_interval_secs: parsed.min_interval_secs,
            default_slippage_bps,
            pools,
        })
    }

    pub fn get_pool(&self, pool: &Pubkey) -> Option<&DlmmPoolConfig> {
        self.pools.get(pool)
    }
}

fn parse_pubkey(value: &str, field: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("parse pubkey for field {field}: {value}"))
}
