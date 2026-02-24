use std::{collections::HashMap, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Context, Result};
use once_cell::sync::OnceCell;
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::constants::{PUMP_FUN_GLOBAL_CONFIG_SEED, PUMP_FUN_PROGRAM_ID};
use crate::dex::{DexKind, PoolSnapshot, TradeSide};

#[derive(Default)]
pub struct PumpFunExecutor;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PumpFunEnvPool {
    user: Pubkey,
    fallback_base_amount: Option<f64>,
    slippage_bps: Option<f64>,
    trade_side: Option<TradeSide>,
    track_volume: bool,
    user_base_ata: Option<Pubkey>,
    user_quote_ata: Option<Pubkey>,
}

#[allow(dead_code)]
const DEFAULT_SLIPPAGE_BPS: f64 = 100.0;
#[allow(dead_code)]
const CREATOR_VAULT_SEED: &[u8] = b"creator_vault";
#[allow(dead_code)]
const GLOBAL_VOLUME_SEED: &[u8] = b"global_volume_accumulator";
#[allow(dead_code)]
const USER_VOLUME_SEED: &[u8] = b"user_volume_accumulator";
#[allow(dead_code)]
const FEE_CONFIG_SEED: &[u8] = b"fee_config";
#[allow(dead_code)]
const FEE_CONFIG_STATIC_SEED: [u8; 32] = [
    12, 20, 222, 252, 130, 94, 198, 118, 148, 37, 8, 24, 187, 101, 64, 101, 244, 41, 141, 49, 86,
    213, 113, 180, 212, 248, 9, 12, 24, 233, 168, 99,
];
#[allow(dead_code)]
const FEE_PROGRAM_ID: Pubkey = solana_pubkey::pubkey!("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
#[allow(dead_code)]
const ASSOCIATED_TOKEN_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

#[allow(dead_code)]
static EXECUTION_POOLS: OnceCell<Arc<HashMap<Pubkey, PumpFunEnvPool>>> = OnceCell::new();

#[allow(dead_code)]
struct PumpFunGlobalDefaults {
    user: Pubkey,
    fallback_base_amount: Option<f64>,
    slippage_bps: Option<f64>,
    trade_side: Option<TradeSide>,
    track_volume: bool,
    user_base_ata: Option<Pubkey>,
    user_quote_ata: Option<Pubkey>,
}

impl PumpFunGlobalDefaults {
    fn to_pool(&self) -> PumpFunEnvPool {
        PumpFunEnvPool {
            user: self.user,
            fallback_base_amount: self.fallback_base_amount,
            slippage_bps: self.slippage_bps,
            trade_side: self.trade_side,
            track_volume: self.track_volume,
            user_base_ata: self.user_base_ata,
            user_quote_ata: self.user_quote_ata,
        }
    }
}

impl TradeExecutor for PumpFunExecutor {
    fn dex(&self) -> DexKind {
        DexKind::PumpFunDlmm
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        // Basic validation
        // For exact-in Buy mode (quote_amount_in set), base_amount can be 0
        let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
        if !is_exact_in_buy && request.base_amount <= 0.0 {
            bail!("base_amount must be positive, got {}", request.base_amount);
        }

        if request.min_amount_out.is_none() {
            bail!("min_amount_out (protection) required for Pump.fun execution");
        }

        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }

        if snapshot.pump_state.is_none() {
            bail!(
                "pump_state missing for Pump.fun pool {}",
                snapshot.descriptor.label
            );
        }

        // Build pool config from request accounts
        let pool_cfg = PumpFunEnvPool {
            user: request.accounts.payer,
            fallback_base_amount: None,
            slippage_bps: Some(request.max_slippage_bps),
            trade_side: None,
            track_volume: false,
            user_base_ata: if request.accounts.user_base_account != Pubkey::default() {
                Some(request.accounts.user_base_account)
            } else {
                None
            },
            user_quote_ata: if request.accounts.user_quote_account != Pubkey::default() {
                Some(request.accounts.user_quote_account)
            } else {
                None
            },
        };

        build_trade_plan(&pool_cfg, snapshot, request)
    }
}

#[allow(dead_code)]
fn load_execution_pools() -> Result<&'static Arc<HashMap<Pubkey, PumpFunEnvPool>>> {
    EXECUTION_POOLS.get_or_try_init(|| {
        let raw = std::env::var("PUMPFUN_EXECUTION_POOLS").unwrap_or_default();
        if raw.trim().is_empty() {
            return Ok(Arc::new(HashMap::new()));
        }

        let entries: Vec<&str> = raw
            .split(|c| matches!(c, '\n' | ';' | ','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();

        if entries.is_empty() {
            return Ok(Arc::new(HashMap::new()));
        }

        let has_key_values = entries.iter().any(|entry| entry.contains('='));
        let mut map = HashMap::new();

        if has_key_values {
            for entry in &entries {
                let (pool, cfg) = parse_pool_entry(entry)?;
                if map.insert(pool, cfg).is_some() {
                    bail!("duplicate Pump.fun execution entry for pool {pool}");
                }
            }
        } else {
            let defaults = PumpFunGlobalDefaults::from_env()?;
            for entry in &entries {
                let pool = parse_pubkey(entry, "PUMPFUN_EXECUTION_POOLS")?;
                if map.insert(pool, defaults.to_pool()).is_some() {
                    bail!("duplicate PUMPFUN_EXECUTION_POOLS entry for pool {pool}");
                }
            }
        }

        Ok(Arc::new(map))
    })
}

impl PumpFunGlobalDefaults {
    #[allow(dead_code)]
    fn from_env() -> Result<Self> {
        let user = parse_pubkey_env("PUMPFUN_EXECUTION_USER")?;
        let trade_side = parse_trade_side_env("PUMPFUN_EXECUTION_SIDE")?;
        let fallback_base_amount = parse_positive_f64_env("PUMPFUN_EXECUTION_AMOUNT")?;
        let slippage_bps = parse_non_negative_f64_env("PUMPFUN_EXECUTION_SLIPPAGE_BPS")?;
        let track_volume = parse_bool_env("PUMPFUN_EXECUTION_TRACK_VOLUME")?;
        let user_base_ata = parse_optional_pubkey_env("PUMPFUN_EXECUTION_USER_BASE_ATA")?;
        let user_quote_ata = parse_optional_pubkey_env("PUMPFUN_EXECUTION_USER_QUOTE_ATA")?;

        Ok(Self {
            user,
            fallback_base_amount,
            slippage_bps,
            trade_side,
            track_volume,
            user_base_ata,
            user_quote_ata,
        })
    }
}

#[allow(dead_code)]
fn parse_pool_entry(entry: &str) -> Result<(Pubkey, PumpFunEnvPool)> {
    let mut pool: Option<Pubkey> = None;
    let mut user: Option<Pubkey> = None;
    let mut amount: Option<f64> = None;
    let mut slippage: Option<f64> = None;
    let mut trade_side: Option<TradeSide> = None;
    let mut track_volume = false;
    let mut user_base_ata: Option<Pubkey> = None;
    let mut user_quote_ata: Option<Pubkey> = None;

    for part in entry.split('|').map(str::trim).filter(|s| !s.is_empty()) {
        let (key, value) = part
            .split_once('=')
            .with_context(|| format!("invalid key=value pair '{part}'"))?;
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "pool" => pool = Some(parse_pubkey(value, "pool")?),
            "user" => user = Some(parse_pubkey(value, "user")?),
            "amount" | "amount_base" => {
                amount = Some(
                    value
                        .parse::<f64>()
                        .with_context(|| format!("parse amount '{value}' for entry {entry}"))?,
                )
            }
            "slippage" | "slippage_bps" => {
                slippage = Some(
                    value
                        .parse::<f64>()
                        .with_context(|| format!("parse slippage '{value}' for entry {entry}"))?,
                )
            }
            "side" | "trade_side" => {
                trade_side = match value.to_ascii_lowercase().as_str() {
                    "buy" => Some(TradeSide::Buy),
                    "sell" => Some(TradeSide::Sell),
                    "both" | "any" => None,
                    other => bail!("unknown trade side '{other}'"),
                };
            }
            "track_volume" => {
                track_volume = matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                );
            }
            "user_base_ata" => user_base_ata = Some(parse_pubkey(value, "user_base_ata")?),
            "user_quote_ata" => user_quote_ata = Some(parse_pubkey(value, "user_quote_ata")?),
            other => bail!("unknown Pump.fun execution key '{other}' in '{entry}'"),
        }
    }

    let pool = pool.ok_or_else(|| anyhow!("missing pool=... in Pump.fun execution entry"))?;
    let user = user.ok_or_else(|| anyhow!("missing user=... in Pump.fun execution entry"))?;

    Ok((
        pool,
        PumpFunEnvPool {
            user,
            fallback_base_amount: amount,
            slippage_bps: slippage,
            trade_side,
            track_volume,
            user_base_ata,
            user_quote_ata,
        },
    ))
}

#[allow(dead_code)]
fn parse_pubkey(value: &str, field: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("parse pubkey for field {field}: {value}"))
}

#[allow(dead_code)]
fn parse_pubkey_env(name: &str) -> Result<Pubkey> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                bail!("environment variable {name} must not be empty");
            }
            parse_pubkey(trimmed, name)
        }
        Err(std::env::VarError::NotPresent) => {
            bail!("environment variable {name} must be set for Pump.fun execution")
        }
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_optional_pubkey_env(name: &str) -> Result<Option<Pubkey>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                parse_pubkey(trimmed, name).map(Some)
            }
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_trade_side_env(name: &str) -> Result<Option<TradeSide>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            match trimmed.to_ascii_lowercase().as_str() {
                "buy" => Ok(Some(TradeSide::Buy)),
                "sell" => Ok(Some(TradeSide::Sell)),
                "both" | "any" => Ok(None),
                other => bail!("invalid {name} value '{other}'"),
            }
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_positive_f64_env(name: &str) -> Result<Option<f64>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed: f64 = trimmed
                .parse()
                .with_context(|| format!("parse numeric value for {name}: {trimmed}"))?;
            if parsed <= 0.0 {
                bail!("{name} must be > 0");
            }
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_non_negative_f64_env(name: &str) -> Result<Option<f64>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed: f64 = trimmed
                .parse()
                .with_context(|| format!("parse numeric value for {name}: {trimmed}"))?;
            if parsed < 0.0 {
                bail!("{name} must be >= 0");
            }
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_bool_env(name: &str) -> Result<bool> {
    match std::env::var(name) {
        Ok(value) => Ok(matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )),
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn build_trade_plan(
    pool_cfg: &PumpFunEnvPool,
    snapshot: &PoolSnapshot,
    request: &TradeRequest,
) -> Result<TradePlan> {
    let inverted = snapshot.normalized_pair.inverted;
    let (norm_base_decimals, norm_quote_decimals) = if inverted {
        (snapshot.quote_decimals, snapshot.base_decimals)
    } else {
        (snapshot.base_decimals, snapshot.quote_decimals)
    };

    // For exact-in Buy mode, base_amount is not needed (output is calculated from quote_amount_in)
    let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
    let base_amount_units = if is_exact_in_buy {
        0.0 // Will be ignored for exact-in Buy
    } else {
        resolve_base_amount(request.base_amount, pool_cfg.fallback_base_amount)?
    };

    // ✅ No simulate fallback: require protection value
    if request.min_amount_out.is_none() {
        bail!("min_amount_out (protection) required for Pump.fun execution");
    }

    let slippage = slippage_bps(
        request.max_slippage_bps,
        pool_cfg.slippage_bps,
        DEFAULT_SLIPPAGE_BPS,
    );

    let (instruction, description) = match request.side {
        TradeSide::Sell => {
            let amount_in_raw = units_to_amount_raw(base_amount_units, norm_base_decimals)?;
            if amount_in_raw == 0 {
                bail!("sell amount rounded to zero");
            }

            // ✅ FIX: For sell leg, use pre-calculated min_quote_out from request.min_amount_out
            let expected_out = request.min_amount_out.unwrap();
            
            // Use pre-calculated min_out if provided, otherwise compute from simulation
            let min_out_raw =
                units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;

            let ix = build_sell_instruction(pool_cfg, snapshot, amount_in_raw, min_out_raw)?;
            let desc = format!(
                "Pump.fun SELL {} base {:.6} -> min quote {:.6} (slippage {:.2} bps)",
                snapshot.descriptor.label, base_amount_units, expected_out, slippage
            );
            (ix, desc)
        }
        TradeSide::Buy => {
            // Check if we have exact-in semantics (quote_amount_in set)
            let (max_quote_in_raw, base_out_raw, quote_in_f, base_out_f, max_quote_in_f, exact_in) =
                if let Some(quote_in) = request.quote_amount_in {
                    // ✅ Spot-only mode: expected quote input; allow slippage by raising max_quote_in
                    let (base_out_raw, base_out_units) = compute_base_out_for_quote_in(
                        snapshot,
                        quote_in,
                        norm_base_decimals,
                        norm_quote_decimals,
                    )?;
                    let max_quote_in_raw =
                        compute_max_amount_in(quote_in, slippage, norm_quote_decimals)?;
                    let max_quote_in_f = quote_in * (1.0 + slippage / 10_000.0);
                    (
                        max_quote_in_raw,
                        base_out_raw,
                        quote_in,
                        base_out_units,
                        max_quote_in_f,
                        true,
                    )
                } else {
                    // ✅ Full mode: min_amount_out is max_quote_in, base_amount is expected output
                    let max_quote_in = request.min_amount_out.unwrap();
                    let max_quote_in_raw = units_to_amount_raw(max_quote_in, norm_quote_decimals)?;
                    let base_out_raw = units_to_amount_raw(base_amount_units, norm_base_decimals)?;
                    (
                        max_quote_in_raw,
                        base_out_raw,
                        max_quote_in,
                        base_amount_units,
                        max_quote_in,
                        false,
                    )
                };

            if max_quote_in_raw == 0 {
                bail!("quote amount rounded to zero");
            }
            if base_out_raw == 0 {
                bail!("target base amount rounded to zero");
            }

            let ix = build_buy_instruction(pool_cfg, snapshot, base_out_raw, max_quote_in_raw)?;
            let desc = if exact_in {
                format!(
                    "Pump.fun BUY {} spend={:.6} (max {:.6}) base_out={:.6} slippage={:.2}bps",
                    snapshot.descriptor.label, quote_in_f, max_quote_in_f, base_out_f, slippage
                )
            } else {
                format!(
                    "Pump.fun BUY {} spend<= {:.6} base_out={:.6} slippage={:.2}bps",
                    snapshot.descriptor.label, max_quote_in_f, base_out_f, slippage
                )
            };
            (ix, desc)
        }
    };

    Ok(TradePlan {
        instructions: vec![instruction],
        description,
    })
}

#[allow(dead_code)]
fn slippage_bps(request_slippage: f64, pool_slippage: Option<f64>, default_slippage: f64) -> f64 {
    if request_slippage > 0.0 {
        request_slippage
    } else {
        pool_slippage.unwrap_or(default_slippage)
    }
}

#[allow(dead_code)]
fn compute_min_amount_out(expected_out: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    let min_out_units = expected_out * (1.0 - slippage_bps / 10_000.0);
    if min_out_units <= 0.0 {
        bail!("min amount out non-positive");
    }
    let scale = 10f64.powi(decimals as i32);
    let raw = (min_out_units * scale).floor();
    if raw <= 0.0 {
        bail!("min amount out rounded to zero");
    }
    Ok(raw as u64)
}

#[allow(dead_code)]
fn compute_max_amount_in(expected_in: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    let max_in_units = expected_in * (1.0 + slippage_bps / 10_000.0);
    if max_in_units <= 0.0 {
        bail!("max amount in non-positive");
    }
    let scale = 10f64.powi(decimals as i32);
    let raw = (max_in_units * scale).ceil();
    if raw <= 0.0 {
        bail!("max amount in rounded to zero");
    }
    if raw > u64::MAX as f64 {
        bail!("max amount exceeds u64 range");
    }
    Ok(raw as u64)
}

fn compute_base_out_for_quote_in(
    snapshot: &PoolSnapshot,
    quote_in: f64,
    norm_base_decimals: u8,
    norm_quote_decimals: u8,
) -> Result<(u64, f64)> {
    if !(quote_in.is_finite() && quote_in > 0.0) {
        bail!("quote_in must be positive");
    }
    let base_reserve = snapshot
        .base_reserve
        .ok_or_else(|| anyhow!("pump.fun missing base_reserve"))? as f64;
    let quote_reserve = snapshot
        .quote_reserve
        .ok_or_else(|| anyhow!("pump.fun missing quote_reserve"))? as f64;

    let inverted = snapshot.normalized_pair.inverted;
    let (norm_base_reserve, norm_quote_reserve) = if inverted {
        (quote_reserve, base_reserve)
    } else {
        (base_reserve, quote_reserve)
    };

    let fees = snapshot
        .fees
        .as_ref()
        .ok_or_else(|| anyhow!("pump.fun missing fees"))?;
    let fee_ratio = fees.total_ratio();
    if !(fee_ratio >= 0.0 && fee_ratio < 1.0) {
        bail!("pump.fun invalid fee ratio {}", fee_ratio);
    }

    let quote_scale = 10f64.powi(norm_quote_decimals as i32);
    let base_scale = 10f64.powi(norm_base_decimals as i32);
    if !(quote_scale.is_finite() && quote_scale > 0.0 && base_scale.is_finite() && base_scale > 0.0) {
        bail!("pump.fun invalid decimals for base/quote");
    }

    let quote_in_raw = quote_in * quote_scale;
    if !(quote_in_raw.is_finite() && quote_in_raw > 0.0) {
        bail!("pump.fun quote_in_raw invalid");
    }
    let net_quote_in = quote_in_raw * (1.0 - fee_ratio);
    if !(net_quote_in.is_finite() && net_quote_in > 0.0) {
        bail!("pump.fun net_quote_in invalid");
    }

    let base_out_raw = norm_base_reserve * net_quote_in / (norm_quote_reserve + net_quote_in);
    if !(base_out_raw.is_finite() && base_out_raw > 0.0) {
        bail!("pump.fun base_out_raw invalid");
    }

    let mut base_out_raw_u64 = base_out_raw.floor() as u64;
    if base_out_raw_u64 > 0 {
        // Avoid overshooting max_quote_in due to rounding.
        base_out_raw_u64 = base_out_raw_u64.saturating_sub(1);
    }
    if base_out_raw_u64 == 0 {
        bail!("pump.fun base_out rounded to zero");
    }

    let base_out_units = (base_out_raw_u64 as f64) / base_scale;
    Ok((base_out_raw_u64, base_out_units))
}

#[allow(dead_code)]
fn resolve_base_amount(request_amount: f64, fallback_units: Option<f64>) -> Result<f64> {
    if request_amount > 0.0 {
        Ok(request_amount)
    } else if let Some(amount) = fallback_units {
        if amount <= 0.0 {
            bail!("configured base amount must be positive");
        }
        Ok(amount)
    } else {
        bail!("base amount not specified")
    }
}

#[allow(dead_code)]
fn units_to_amount_raw(amount: f64, decimals: u8) -> Result<u64> {
    if !(amount.is_finite() && amount > 0.0) {
        bail!("amount must be positive");
    }
    let scale = 10f64.powi(decimals as i32);
    let scaled = (amount * scale).floor();
    if !(scaled.is_finite() && scaled >= 0.0) {
        bail!("scaled amount invalid");
    }
    if scaled < 1.0 {
        return Ok(1);
    }
    if scaled > u64::MAX as f64 {
        bail!("amount exceeds u64 range");
    }
    Ok(scaled as u64)
}

#[allow(dead_code)]
fn build_sell_instruction(
    pool_cfg: &PumpFunEnvPool,
    snapshot: &PoolSnapshot,
    base_amount_in: u64,
    min_quote_out: u64,
) -> Result<Instruction> {
    const SELL_DISCRIMINATOR: [u8; 8] = [51, 230, 133, 164, 1, 127, 131, 173];

    let mut data = Vec::with_capacity(8 + 16);
    data.extend_from_slice(&SELL_DISCRIMINATOR);
    data.extend_from_slice(&base_amount_in.to_le_bytes());
    data.extend_from_slice(&min_quote_out.to_le_bytes());

    let accounts = assemble_accounts(snapshot, pool_cfg, false)?;

    Ok(Instruction {
        program_id: PUMP_FUN_PROGRAM_ID,
        accounts,
        data,
    })
}

#[allow(dead_code)]
fn build_buy_instruction(
    pool_cfg: &PumpFunEnvPool,
    snapshot: &PoolSnapshot,
    base_amount_out: u64,
    max_quote_in: u64,
) -> Result<Instruction> {
    const BUY_DISCRIMINATOR: [u8; 8] = [102, 6, 61, 18, 1, 218, 235, 234];

    let mut data = Vec::with_capacity(8 + 16 + 1);
    data.extend_from_slice(&BUY_DISCRIMINATOR);
    data.extend_from_slice(&base_amount_out.to_le_bytes());
    data.extend_from_slice(&max_quote_in.to_le_bytes());
    data.push(pool_cfg.track_volume as u8);

    let accounts = assemble_accounts(snapshot, pool_cfg, true)?;

    Ok(Instruction {
        program_id: PUMP_FUN_PROGRAM_ID,
        accounts,
        data,
    })
}

#[allow(dead_code)]
fn assemble_accounts(
    snapshot: &PoolSnapshot,
    pool_cfg: &PumpFunEnvPool,
    include_volume: bool,
) -> Result<Vec<AccountMeta>> {
    let state = snapshot
        .pump_state
        .as_ref()
        .ok_or_else(|| anyhow!("pump_state missing for Pump.fun pool"))?;

    let (global_config, _) =
        Pubkey::find_program_address(&[PUMP_FUN_GLOBAL_CONFIG_SEED], &PUMP_FUN_PROGRAM_ID);
    let event_authority =
        Pubkey::find_program_address(&[b"__event_authority"], &PUMP_FUN_PROGRAM_ID).0;

    let base_token_program = spl_token::ID;
    let quote_token_program = spl_token::ID;

    let user_base_ata = pool_cfg.user_base_ata.unwrap_or_else(|| {
        derive_associated_token(&pool_cfg.user, &snapshot.base_mint, &base_token_program)
    });
    let user_quote_ata = pool_cfg.user_quote_ata.unwrap_or_else(|| {
        derive_associated_token(&pool_cfg.user, &snapshot.quote_mint, &quote_token_program)
    });

    let protocol_fee_recipient = state.protocol_fee_recipient;
    let protocol_fee_recipient_token_account = derive_associated_token(
        &protocol_fee_recipient,
        &snapshot.quote_mint,
        &quote_token_program,
    );

    let coin_creator_vault_authority = derive_coin_creator_vault_authority(&state.coin_creator);
    let coin_creator_vault_ata = derive_associated_token(
        &coin_creator_vault_authority,
        &snapshot.quote_mint,
        &quote_token_program,
    );

    let fee_config = derive_fee_config_pda();

    let mut metas = vec![
        // ⭐ Pool 必须是可写的 (错误 2000: ConstraintMut)
        AccountMeta::new(snapshot.descriptor.address, false),
        AccountMeta::new(pool_cfg.user, true),
        AccountMeta::new_readonly(global_config, false),
        AccountMeta::new_readonly(snapshot.base_mint, false),
        AccountMeta::new_readonly(snapshot.quote_mint, false),
        AccountMeta::new(user_base_ata, false),
        AccountMeta::new(user_quote_ata, false),
        AccountMeta::new(state.pool_base_token_account, false),
        AccountMeta::new(state.pool_quote_token_account, false),
        AccountMeta::new_readonly(protocol_fee_recipient, false),
        AccountMeta::new(protocol_fee_recipient_token_account, false),
        AccountMeta::new_readonly(base_token_program, false),
        AccountMeta::new_readonly(quote_token_program, false),
        AccountMeta::new_readonly(solana_system_interface::program::ID, false),
        AccountMeta::new_readonly(ASSOCIATED_TOKEN_PROGRAM_ID, false),
        AccountMeta::new_readonly(event_authority, false),
        AccountMeta::new_readonly(PUMP_FUN_PROGRAM_ID, false),
        AccountMeta::new(coin_creator_vault_ata, false),
        AccountMeta::new_readonly(coin_creator_vault_authority, false),
    ];

    if include_volume {
        let global_volume = derive_global_volume_accumulator();
        let user_volume = derive_user_volume_accumulator(&pool_cfg.user);
        metas.push(AccountMeta::new(global_volume, false));
        metas.push(AccountMeta::new(user_volume, false));
    }

    // ⭐ Pump.fun 合约更新后需要 fee_config 和 fee_program 账户
    // 错误 3005 (AccountNotEnoughKeys) 表示缺少必要账户
    metas.push(AccountMeta::new_readonly(fee_config, false));
    metas.push(AccountMeta::new_readonly(FEE_PROGRAM_ID, false));

    Ok(metas)
}

#[allow(dead_code)]
fn derive_associated_token(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[owner.as_ref(), token_program.as_ref(), mint.as_ref()],
        &ASSOCIATED_TOKEN_PROGRAM_ID,
    )
    .0
}

#[allow(dead_code)]
fn derive_coin_creator_vault_authority(coin_creator: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[CREATOR_VAULT_SEED, coin_creator.as_ref()],
        &PUMP_FUN_PROGRAM_ID,
    )
    .0
}

#[allow(dead_code)]
fn derive_fee_config_pda() -> Pubkey {
    Pubkey::find_program_address(&[FEE_CONFIG_SEED, &FEE_CONFIG_STATIC_SEED], &FEE_PROGRAM_ID).0
}

#[allow(dead_code)]
fn derive_global_volume_accumulator() -> Pubkey {
    Pubkey::find_program_address(&[GLOBAL_VOLUME_SEED], &PUMP_FUN_PROGRAM_ID).0
}

#[allow(dead_code)]
fn derive_user_volume_accumulator(user: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[USER_VOLUME_SEED, user.as_ref()], &PUMP_FUN_PROGRAM_ID).0
}
