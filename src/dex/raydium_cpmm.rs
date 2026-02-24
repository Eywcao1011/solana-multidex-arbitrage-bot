//! Raydium CPMM (Constant Product Market Maker) pool decoding
//! Program ID: CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C

use anyhow::{anyhow, Result};
use chrono::Utc;
use log::debug;
use once_cell::sync::OnceCell;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::RwLock;

use super::{normalize_pair, AccountReader, PoolDescriptor, PoolFees, PoolSnapshot, SOL_MINT};
use crate::rpc::AppContext;

/// CPMM pool state layout (from raydium-cp-swap)
/// Total size: 8 (discriminator) + 10*32 (pubkeys) + 5 (u8s) + 7*8 (u64s) + 2 (creator fee fields) + 6 (padding1) + 2*8 (creator fees) + 28*8 (padding)
#[derive(Debug, Clone)]
pub struct CpmmPoolStaticInfo {
    pub program_id: Pubkey,
    pub amm_config: Pubkey,
    pub observation_key: Pubkey,
    pub token_0_vault: Pubkey,
    pub token_1_vault: Pubkey,
    pub token_0_mint: Pubkey,
    pub token_1_mint: Pubkey,
    pub token_0_program: Pubkey,
    pub token_1_program: Pubkey,
    pub lp_mint: Pubkey,
    pub mint_0_decimals: u8,
    pub mint_1_decimals: u8,
}

/// Global static info cache
static CPMM_STATIC_CACHE: OnceCell<RwLock<HashMap<Pubkey, CpmmPoolStaticInfo>>> = OnceCell::new();

fn get_static_cache() -> &'static RwLock<HashMap<Pubkey, CpmmPoolStaticInfo>> {
    CPMM_STATIC_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn get_cached_static_info(pool: &Pubkey) -> Option<CpmmPoolStaticInfo> {
    get_static_cache()
        .read()
        .ok()
        .and_then(|cache| cache.get(pool).cloned())
}

fn cache_static_info(pool: Pubkey, info: CpmmPoolStaticInfo) {
    if let Ok(mut cache) = get_static_cache().write() {
        cache.insert(pool, info);
    }
}

/// Public interface: get cached static info (for trade executor)
pub fn get_static_info(pool: &Pubkey) -> Option<CpmmPoolStaticInfo> {
    get_cached_static_info(pool)
}

/// Parse CPMM pool state from account data
fn parse_pool_state(data: &[u8]) -> Result<CpmmPoolStaticInfo> {
    // Minimum size check: 8 + 10*32 + 5 = 333 bytes minimum
    const MIN_SIZE: usize = 333;
    if data.len() < MIN_SIZE {
        anyhow::bail!("CPMM pool account too short: {} < {}", data.len(), MIN_SIZE);
    }

    let mut reader = AccountReader::new(&data[8..]); // Skip 8-byte discriminator

    let amm_config = reader.read_pubkey()?;
    let _pool_creator = reader.read_pubkey()?;
    let token_0_vault = reader.read_pubkey()?;
    let token_1_vault = reader.read_pubkey()?;
    let lp_mint = reader.read_pubkey()?;
    let token_0_mint = reader.read_pubkey()?;
    let token_1_mint = reader.read_pubkey()?;
    let token_0_program = reader.read_pubkey()?;
    let token_1_program = reader.read_pubkey()?;
    let observation_key = reader.read_pubkey()?;

    let _auth_bump = reader.read_u8()?;
    let status = reader.read_u8()?;
    let _lp_mint_decimals = reader.read_u8()?;
    let mint_0_decimals = reader.read_u8()?;
    let mint_1_decimals = reader.read_u8()?;

    // Check pool status (bit 2 = swap disabled)
    if status & 0b100 != 0 {
        anyhow::bail!("CPMM pool swap is disabled (status={})", status);
    }

    // Sanity check
    if token_0_mint == Pubkey::default() || token_1_mint == Pubkey::default() {
        anyhow::bail!("CPMM pool has default mint pubkeys (likely wrong layout)");
    }

    Ok(CpmmPoolStaticInfo {
        program_id: crate::constants::RAYDIUM_CPMM_PROGRAM_ID,
        amm_config,
        observation_key,
        token_0_vault,
        token_1_vault,
        token_0_mint,
        token_1_mint,
        token_0_program,
        token_1_program,
        lp_mint,
        mint_0_decimals,
        mint_1_decimals,
    })
}

/// Warmup: 从 RPC 获取 CPMM 池账户数据，解析出 vault 地址
///
/// 在 spot_only 模式启动时调用，返回 (vault_0, vault_1) 供订阅使用
pub async fn warmup_parse_vaults(
    pool: &solana_pubkey::Pubkey,
    ctx: &crate::rpc::AppContext,
) -> anyhow::Result<(solana_pubkey::Pubkey, solana_pubkey::Pubkey)> {
    let fetch_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.rpc_client().get_account_data(pool),
    )
    .await;

    let data = fetch_result
        .map_err(|_| anyhow!("fetch cpmm pool {} timed out (5s)", pool))?
        .map_err(|e| anyhow!("fetch cpmm pool {} failed: {}", pool, e))?;

    let info = parse_pool_state(&data)?;
    cache_static_info(*pool, info.clone());
    Ok((info.token_0_vault, info.token_1_vault))
}

/// Decode CPMM pool using cached static info + live vault balances
pub async fn decode(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    let pool = descriptor.address;

    // Try to get cached static info, or parse from account data
    let static_info = if let Some(info) = get_cached_static_info(&pool) {
        info
    } else {
        let info = parse_pool_state(data)?;
        cache_static_info(pool, info.clone());
        debug!(
            "CPMM pool {} static info cached: {} / {}",
            descriptor.label, info.token_0_mint, info.token_1_mint
        );
        info
    };

    // Get vault balances from WS cache or fetch
    let slot = Some(context.slot);
    let (vault_0_balance, vault_1_balance) = if ctx.spot_only_mode() {
        let v0 = ctx
            .get_token_account_amount_from_cache(&static_info.token_0_vault)
            .ok_or_else(|| {
                anyhow!(
                    "CPMM vault {} not in WS cache (spot_only)",
                    static_info.token_0_vault
                )
            })?;
        let v1 = ctx
            .get_token_account_amount_from_cache(&static_info.token_1_vault)
            .ok_or_else(|| {
                anyhow!(
                    "CPMM vault {} not in WS cache (spot_only)",
                    static_info.token_1_vault
                )
            })?;
        (v0, v1)
    } else {
        let (v0, v1) = tokio::try_join!(
            ctx.get_token_account_amount_with_slot(&static_info.token_0_vault, slot),
            ctx.get_token_account_amount_with_slot(&static_info.token_1_vault, slot)
        )?;
        (v0, v1)
    };

    if vault_0_balance == 0 || vault_1_balance == 0 {
        return Err(anyhow!("CPMM pool has zero reserves"));
    }

    // Determine base/quote based on SOL position
    let (base_mint, quote_mint, base_decimals, quote_decimals, base_reserve, quote_reserve) =
        if static_info.token_0_mint == *SOL_MINT {
            // token_0 is SOL (quote), token_1 is base
            (
                static_info.token_1_mint,
                static_info.token_0_mint,
                static_info.mint_1_decimals,
                static_info.mint_0_decimals,
                vault_1_balance,
                vault_0_balance,
            )
        } else {
            // token_0 is base, token_1 is quote (or SOL)
            (
                static_info.token_0_mint,
                static_info.token_1_mint,
                static_info.mint_0_decimals,
                static_info.mint_1_decimals,
                vault_0_balance,
                vault_1_balance,
            )
        };

    // Calculate price: quote per base (adjusted for decimals)
    let price_raw = (quote_reserve as f64) / (base_reserve as f64);
    let decimals_adjustment = base_decimals as i32 - quote_decimals as i32;
    let price = price_raw * 10f64.powi(decimals_adjustment);

    if !price.is_finite() || price <= 0.0 {
        return Err(anyhow!("CPMM invalid price computed"));
    }

    let (normalized_pair, normalized_price) = normalize_pair(base_mint, quote_mint, price)
        .ok_or_else(|| {
            anyhow!(
                "cannot normalize CPMM pair {} {}",
                base_mint,
                quote_mint
            )
        })?;

    let fees = match ctx.get_raydium_amm_config(&static_info.amm_config).await {
        Ok(config) => {
            let fee_den = 1_000_000.0;
            let trade_fee_ratio = (config.trade_fee_rate as f64) / fee_den;
            let protocol_fee_ratio = (config.protocol_fee_rate as f64) / fee_den;
            let fund_fee_ratio = (config.fund_fee_rate as f64) / fee_den;
            let lp_fee_ratio = (trade_fee_ratio - protocol_fee_ratio - fund_fee_ratio).max(0.0);

            PoolFees {
                lp_fee_bps: lp_fee_ratio * 10_000.0,
                protocol_fee_bps: protocol_fee_ratio * 10_000.0,
                other_fee_bps: fund_fee_ratio.max(0.0) * 10_000.0,
                meteora_dynamic: None,
            }
        }
        Err(err) => {
            debug!(
                "CPMM pool {} fee config fetch failed: {} (fallback 25 bps)",
                descriptor.label, err
            );
            PoolFees {
                lp_fee_bps: 25.0,
                protocol_fee_bps: 0.0,
                other_fee_bps: 0.0,
                meteora_dynamic: None,
            }
        }
    };

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint,
        quote_mint,
        base_decimals,
        quote_decimals,
        price,
        sqrt_price_x64: 0,
        liquidity: None,
        base_reserve: Some(base_reserve as u128),
        quote_reserve: Some(quote_reserve as u128),
        fees: Some(fees),
        slot: context.slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state: None,
        clmm_accounts: None,
        damm_state: None,
        damm_accounts: None,
        lb_state: None,
        lb_accounts: None,
        pump_state: None,
        dependent_accounts: vec![static_info.token_0_vault, static_info.token_1_vault],
    })
}

/// Warmup: pre-cache static info and subscribe to vault accounts
pub async fn warmup_static_info(
    descriptor: &PoolDescriptor,
    data: &[u8],
    _ctx: &AppContext,
) -> Result<Vec<Pubkey>> {
    let pool = descriptor.address;

    // Parse and cache static info
    let info = parse_pool_state(data)?;
    cache_static_info(pool, info.clone());

    debug!(
        "CPMM pool {} warmup: token_0={}, token_1={}, vaults=[{}, {}]",
        descriptor.label,
        info.token_0_mint,
        info.token_1_mint,
        info.token_0_vault,
        info.token_1_vault
    );

    // Return vault accounts for subscription
    Ok(vec![info.token_0_vault, info.token_1_vault])
}
