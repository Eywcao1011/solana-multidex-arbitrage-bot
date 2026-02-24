#![allow(dead_code)]

use anyhow::{anyhow, Result};
use chrono::Utc;
use log::debug;
use solana_client::rpc_response::RpcResponseContext;

use super::{
    normalize_pair, AccountReader, DammAccounts, DexKind, PoolDescriptor, PoolFees, PoolSnapshot,
    TradeQuote, TradeSide,
};
use crate::rpc::{q64_to_price, AppContext};

// Program IDs for version detection
// Note: 这些常量在异步 decode_v1/decode_v2 函数中使用，编译器静态分析可能无法追踪
#[allow(dead_code)]
const DAMM_V1_PROGRAM_ID: &str = "Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UaB";
#[allow(dead_code)]
const DAMM_V2_PROGRAM_ID: &str = "cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG";

const Q64: f64 = 18_446_744_073_709_551_616.0; // 2^64
const EPSILON: f64 = 1e-12;
const MAX_FEE_RATIO: f64 = 0.95;

#[derive(Debug, Clone)]
pub struct DammState {
    pub sqrt_price: f64,
    pub sqrt_min_price: f64,
    pub sqrt_max_price: f64,
    pub liquidity: f64,
    pub total_fee_ratio: f64,
}

#[derive(Debug, Clone)]
struct DammDynamicFee {
    bin_step: u16,
    bin_step_q64: u128,
    _filter_period: u16,
    _decay_period: u16,
    _reduction_factor: u16,
    max_volatility_accumulator: u32,
    variable_fee_control: u32,
    volatility_accumulator: u128,
    _volatility_reference: u128,
    _sqrt_price_reference: u128,
}

fn build_damm_state(
    fee_info: &DammFeeInfo,
    liquidity: u128,
    sqrt_price_x64: u128,
    sqrt_min_price_x64: u128,
    sqrt_max_price_x64: u128,
) -> Option<DammState> {
    // ⚠️ liquidity 在链上是 Q64 格式，需要除以 2^64 转换为实际值
    let liquidity_f64 = (liquidity as f64) / Q64;
    if !liquidity_f64.is_finite() || liquidity_f64 <= 0.0 {
        return None;
    }

    let sqrt_price = (sqrt_price_x64 as f64) / Q64;
    let sqrt_min_price = (sqrt_min_price_x64 as f64) / Q64;
    let sqrt_max_price = (sqrt_max_price_x64 as f64) / Q64;

    if !(sqrt_min_price.is_finite() && sqrt_max_price.is_finite() && sqrt_price.is_finite()) {
        return None;
    }
    if !(sqrt_max_price > sqrt_min_price
        && sqrt_price >= sqrt_min_price - EPSILON
        && sqrt_price <= sqrt_max_price + EPSILON)
    {
        return None;
    }

    let base_fee_ratio = fee_info.base_fee_ratio.max(0.0);
    let dynamic_fee_ratio = fee_info.dynamic_fee_ratio.max(0.0);
    let total_fee_ratio = (base_fee_ratio + dynamic_fee_ratio).clamp(0.0, MAX_FEE_RATIO);

    Some(DammState {
        sqrt_price,
        sqrt_min_price,
        sqrt_max_price,
        liquidity: liquidity_f64,
        total_fee_ratio,
    })
}

/// Meteora Vault 解析后的信息
#[derive(Debug, Clone)]
pub struct VaultInfo {
    pub token_vault: solana_pubkey::Pubkey,  // 实际的 SPL Token Account
    pub total_amount: u64,                         // Vault 中的总代币数量
    pub token_mint: solana_pubkey::Pubkey,    // ⭐ Vault 对应的 token mint
    pub lp_mint: solana_pubkey::Pubkey,       // LP token mint
}


/// 解析 Meteora Vault 账户，提取关键信息
/// 
/// Meteora Vault 账户布局 (来自 meteora-vault-sdk):
/// ```
/// discriminator: [u8; 8]   // offset: 0,   size: 8
/// enabled: u8              // offset: 8,   size: 1
/// bumps: VaultBumps {      // offset: 9,   size: 2
///     vault_bump: u8
///     token_vault_bump: u8
/// }
/// total_amount: u64        // offset: 11,  size: 8  ⭐ Vault 中的总代币数量
/// token_vault: Pubkey      // offset: 19,  size: 32  ⭐ 实际的 SPL Token Account
/// fee_vault: Pubkey        // offset: 51,  size: 32
/// token_mint: Pubkey       // offset: 83,  size: 32
/// lp_mint: Pubkey          // offset: 115, size: 32  ⭐ LP token mint
/// ...
/// ```
pub fn parse_meteora_vault(vault_data: &[u8]) -> Result<VaultInfo> {
    // Vault 账户最小长度检查 (至少到 lp_mint 结束)
    const MIN_VAULT_SIZE: usize = 115 + 32; // offset 115 + Pubkey size 32 = 147
    if vault_data.len() < MIN_VAULT_SIZE {
        anyhow::bail!("Meteora Vault data too short: {} < {}", vault_data.len(), MIN_VAULT_SIZE);
    }
    
    // total_amount 在 offset 11
    let total_amount = u64::from_le_bytes(
        vault_data[11..19].try_into()
            .map_err(|_| anyhow!("Failed to parse total_amount from Meteora Vault"))?
    );
    
    // token_vault 在 offset 19
    let token_vault_bytes: [u8; 32] = vault_data[19..51]
        .try_into()
        .map_err(|_| anyhow!("Failed to parse token_vault from Meteora Vault"))?;
    let token_vault = solana_pubkey::Pubkey::new_from_array(token_vault_bytes);
    
    // ⭐ token_mint 在 offset 83
    let token_mint_bytes: [u8; 32] = vault_data[83..115]
        .try_into()
        .map_err(|_| anyhow!("Failed to parse token_mint from Meteora Vault"))?;
    let token_mint = solana_pubkey::Pubkey::new_from_array(token_mint_bytes);
    
    // lp_mint 在 offset 115
    let lp_mint_bytes: [u8; 32] = vault_data[115..147]
        .try_into()
        .map_err(|_| anyhow!("Failed to parse lp_mint from Meteora Vault"))?;
    let lp_mint = solana_pubkey::Pubkey::new_from_array(lp_mint_bytes);
    
    Ok(VaultInfo {
        token_vault,
        total_amount,
        token_mint,
        lp_mint,
    })
}

/// 向后兼容：只获取 token_vault 地址
pub(crate) fn parse_meteora_vault_token_account(
    vault_data: &[u8],
) -> Result<solana_pubkey::Pubkey> {
    let info = parse_meteora_vault(vault_data)?;
    Ok(info.token_vault)
}

async fn decode_v1(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    // DAMM V1 是传统 AMM（使用 Vault 模式），不是 CLMM
    // 布局来自官方 IDL: https://github.com/MeteoraAg/damm-v1-sdk
    //
    // Pool 结构体布局:
    // [0-8]     discriminator
    // [8-40]    lpMint (Pubkey)
    // [40-72]   tokenAMint (Pubkey)
    // [72-104]  tokenBMint (Pubkey)
    // [104-136] aVault (Pubkey) - Meteora Vault account, not token account
    // [136-168] bVault (Pubkey)
    // [168-200] aVaultLp (Pubkey)
    // [200-232] bVaultLp (Pubkey)
    // [232]     aVaultLpBump (u8)
    // [233]     enabled (bool)
    // [234-266] protocolTokenAFee (Pubkey)
    // [266-298] protocolTokenBFee (Pubkey)
    // [298-306] feeLastUpdatedAt (u64)
    // [306-330] padding0 ([u8; 24])
    // [330-362] fees (PoolFees: 4 x u64)
    // ...

    if data.len() < 400 {
        anyhow::bail!("DAMM V1 account data too short: {} bytes", data.len());
    }

    let mut reader = AccountReader::new(&data[8..]);

    // [8-40] lpMint
    let _lp_mint = reader.read_pubkey()?;

    // [40-72] tokenAMint
    let token_a_mint = reader.read_pubkey()?;

    // [72-104] tokenBMint
    let token_b_mint = reader.read_pubkey()?;

    // [104-136] aVault (Meteora Vault account, not direct token account)
    let a_vault = reader.read_pubkey()?;

    // [136-168] bVault
    let b_vault = reader.read_pubkey()?;

    // [168-200] aVaultLp (池子在 Vault A 中的 LP token 账户)
    let a_vault_lp = reader.read_pubkey()?;

    // [200-232] bVaultLp (池子在 Vault B 中的 LP token 账户)
    let b_vault_lp = reader.read_pubkey()?;

    // [232] aVaultLpBump
    let _a_vault_lp_bump = reader.read_u8()?;

    // [233] enabled
    let enabled = reader.read_u8()? != 0;

    // [234-266] protocolTokenAFee
    let protocol_token_a_fee = reader.read_pubkey()?;

    // [266-298] protocolTokenBFee
    let protocol_token_b_fee = reader.read_pubkey()?;

    // [298-306] feeLastUpdatedAt
    let _fee_last_updated_at = reader.read_u64()?;

    // [306-330] padding0
    reader.skip(24)?;

    // [330-362] PoolFees struct
    let trade_fee_numerator = reader.read_u64()?;
    let trade_fee_denominator = reader.read_u64()?;
    let protocol_trade_fee_numerator = reader.read_u64()?;
    let protocol_trade_fee_denominator = reader.read_u64()?;

    if !enabled {
        anyhow::bail!("DAMM V1 pool {} is disabled", descriptor.address);
    }

    // 计算费率
    let trade_fee_ratio = if trade_fee_denominator > 0 {
        (trade_fee_numerator as f64 / trade_fee_denominator as f64).clamp(0.0, MAX_FEE_RATIO)
    } else {
        0.0
    };

    let protocol_fee_ratio = if protocol_trade_fee_denominator > 0 {
        (protocol_trade_fee_numerator as f64 / protocol_trade_fee_denominator as f64).clamp(0.0, MAX_FEE_RATIO)
    } else {
        0.0
    };

    // 获取 mint 信息
    let (mint_a_info, mint_b_info) = tokio::try_join!(
        ctx.get_mint_info(&token_a_mint),
        ctx.get_mint_info(&token_b_mint),
    )?;

    // ✅ DAMM V1 使用共享金库架构（Mercurial Vault）
    // 多个池子共享同一个 Vault，需要计算池子的实际份额：
    // pool_reserve = vault_total_tokens * pool_lp_balance / total_lp_supply
    //
    // 需要获取：
    // 1. Vault 数据 (total_amount, lp_mint, token_vault)
    // 2. 池子的 LP token 余额 (aVaultLp, bVaultLp)
    // 3. LP mint 总供应量
    
    let (a_token_vault, b_token_vault, a_reserve, b_reserve, lp_mint_a, lp_mint_b) = if ctx.spot_only_mode() {
        // Spot-only 模式：优先从 WS cache 获取，如果缓存为空则通过 HTTP 获取
        // ⚠️ 重要：HTTP 获取后必须 prefill_cache，否则每次都会调用 HTTP
        
        // 1. 获取 Vault 数据（优先缓存，fallback HTTP + prefill）
        let a_vault_data = match ctx.get_account_data_from_cache(&a_vault) {
            Some(data) => data,
            None => {
                debug!("DAMM V1 {}: aVault not in cache, fetching via HTTP and prefilling", descriptor.label);
                let data = ctx.rpc_client().get_account_data(&a_vault).await
                    .map_err(|e| anyhow!("DAMM V1 aVault {} HTTP fetch failed: {}", a_vault, e))?;
                // ✅ 预填充缓存，避免下次再调用 HTTP
                ctx.prefill_cache(&a_vault, data.clone(), context.slot).await;
                data
            }
        };
        let b_vault_data = match ctx.get_account_data_from_cache(&b_vault) {
            Some(data) => data,
            None => {
                debug!("DAMM V1 {}: bVault not in cache, fetching via HTTP and prefilling", descriptor.label);
                let data = ctx.rpc_client().get_account_data(&b_vault).await
                    .map_err(|e| anyhow!("DAMM V1 bVault {} HTTP fetch failed: {}", b_vault, e))?;
                ctx.prefill_cache(&b_vault, data.clone(), context.slot).await;
                data
            }
        };
        
        let a_vault_info = parse_meteora_vault(&a_vault_data)?;
        let b_vault_info = parse_meteora_vault(&b_vault_data)?;
        
        // ⭐ 验证 vault 的 token_mint 是否匹配 pool 的 tokenA/B
        // 如果不匹配，说明 aVault/bVault 的映射与 tokenA/tokenB 不一致，需要交换
        let (final_a_vault_info, final_b_vault_info, final_a_vault_lp, final_b_vault_lp) = 
            if a_vault_info.token_mint == token_a_mint && b_vault_info.token_mint == token_b_mint {
                // 正常映射：aVault -> tokenA, bVault -> tokenB
                (a_vault_info, b_vault_info, a_vault_lp, b_vault_lp)
            } else if a_vault_info.token_mint == token_b_mint && b_vault_info.token_mint == token_a_mint {
                // 反向映射：aVault -> tokenB, bVault -> tokenA，需要交换
                log::warn!(
                    "DAMM V1 {}: vault/pool mint mismatch! aVault.mint={} != tokenA={}, swapping vault mapping",
                    descriptor.label, a_vault_info.token_mint, token_a_mint
                );
                (b_vault_info, a_vault_info, b_vault_lp, a_vault_lp)
            } else {
                // 完全不匹配，报错
                anyhow::bail!(
                    "DAMM V1 {}: vault token_mint mismatch! aVault.mint={}, bVault.mint={}, pool tokenA={}, tokenB={}",
                    descriptor.label, a_vault_info.token_mint, b_vault_info.token_mint, token_a_mint, token_b_mint
                );
            };
        
        // 使用校正后的 vault info 继续计算
        
        // 2. 获取池子的 LP token 余额（优先缓存，fallback HTTP + prefill）
        let pool_lp_a = match ctx.get_token_account_amount_from_cache(&final_a_vault_lp) {
            Some(amount) => amount,
            None => {
                debug!("DAMM V1 {}: aVaultLp not in cache, fetching via HTTP and prefilling", descriptor.label);
                let data = ctx.fetch_account_data_with_slot(&final_a_vault_lp, Some(context.slot)).await
                    .map_err(|e| anyhow!("DAMM V1 aVaultLp {} HTTP fetch failed: {}", final_a_vault_lp, e))?;
                ctx.prefill_cache(&final_a_vault_lp, data.clone(), context.slot).await;
                // 解析 token account amount
                if data.len() >= 72 {
                    u64::from_le_bytes(data[64..72].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
        };
        let pool_lp_b = match ctx.get_token_account_amount_from_cache(&final_b_vault_lp) {
            Some(amount) => amount,
            None => {
                debug!("DAMM V1 {}: bVaultLp not in cache, fetching via HTTP and prefilling", descriptor.label);
                let data = ctx.fetch_account_data_with_slot(&final_b_vault_lp, Some(context.slot)).await
                    .map_err(|e| anyhow!("DAMM V1 bVaultLp {} HTTP fetch failed: {}", final_b_vault_lp, e))?;
                ctx.prefill_cache(&final_b_vault_lp, data.clone(), context.slot).await;
                if data.len() >= 72 {
                    u64::from_le_bytes(data[64..72].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
        };
        
        // 3. 获取 LP mint 总供应量（优先缓存，fallback HTTP + prefill）
        let lp_supply_a = match ctx.get_mint_supply_from_cache(&final_a_vault_info.lp_mint) {
            Some(supply) => supply,
            None => {
                debug!("DAMM V1 {}: lp_mint_a not in cache, fetching via HTTP and prefilling", descriptor.label);
                let data = ctx.fetch_account_data_with_slot(&final_a_vault_info.lp_mint, Some(context.slot)).await
                    .map_err(|e| anyhow!("DAMM V1 lp_mint_a {} HTTP fetch failed: {}", final_a_vault_info.lp_mint, e))?;
                ctx.prefill_cache(&final_a_vault_info.lp_mint, data.clone(), context.slot).await;
                // 解析 mint supply (offset 36-44)
                if data.len() >= 44 {
                    u64::from_le_bytes(data[36..44].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
        };
        let lp_supply_b = match ctx.get_mint_supply_from_cache(&final_b_vault_info.lp_mint) {
            Some(supply) => supply,
            None => {
                debug!("DAMM V1 {}: lp_mint_b not in cache, fetching via HTTP and prefilling", descriptor.label);
                let data = ctx.fetch_account_data_with_slot(&final_b_vault_info.lp_mint, Some(context.slot)).await
                    .map_err(|e| anyhow!("DAMM V1 lp_mint_b {} HTTP fetch failed: {}", final_b_vault_info.lp_mint, e))?;
                ctx.prefill_cache(&final_b_vault_info.lp_mint, data.clone(), context.slot).await;
                if data.len() >= 44 {
                    u64::from_le_bytes(data[36..44].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
        };
        
        // 4. 获取实际 token vault 余额（这是实际的 SPL token 余额，比 vault.total_amount 更准确）
        let actual_a_balance = match ctx.get_token_account_amount_from_cache(&final_a_vault_info.token_vault) {
            Some(amount) => amount,
            None => {
                debug!("DAMM V1 {}: token_vault_a {} not in cache, fetching via HTTP", descriptor.label, final_a_vault_info.token_vault);
                let data = ctx.fetch_account_data_with_slot(&final_a_vault_info.token_vault, Some(context.slot)).await
                    .map_err(|e| anyhow!("DAMM V1 token_vault_a {} HTTP fetch failed: {}", final_a_vault_info.token_vault, e))?;
                ctx.prefill_cache(&final_a_vault_info.token_vault, data.clone(), context.slot).await;
                if data.len() >= 72 {
                    u64::from_le_bytes(data[64..72].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
        };
        let actual_b_balance = match ctx.get_token_account_amount_from_cache(&final_b_vault_info.token_vault) {
            Some(amount) => amount,
            None => {
                debug!("DAMM V1 {}: token_vault_b {} not in cache, fetching via HTTP", descriptor.label, final_b_vault_info.token_vault);
                let data = ctx.fetch_account_data_with_slot(&final_b_vault_info.token_vault, Some(context.slot)).await
                    .map_err(|e| anyhow!("DAMM V1 token_vault_b {} HTTP fetch failed: {}", final_b_vault_info.token_vault, e))?;
                ctx.prefill_cache(&final_b_vault_info.token_vault, data.clone(), context.slot).await;
                if data.len() >= 72 {
                    u64::from_le_bytes(data[64..72].try_into().unwrap_or([0; 8]))
                } else {
                    0
                }
            }
        };
        
        // ⭐ 改用 total_amount 计算池子份额：pool_reserve = vault.total_amount * pool_lp / total_lp_supply
        // 之前用 actual_balance 会导致价格偏差（因为 lending 资金会让 actual_balance 偏低）
        // total_amount 代表存入 vault 的全部资金，更适合定价
        let a_reserve = if lp_supply_a > 0 {
            ((final_a_vault_info.total_amount as u128) * (pool_lp_a as u128) / (lp_supply_a as u128)) as u64
        } else {
            0
        };
        let b_reserve = if lp_supply_b > 0 {
            ((final_b_vault_info.total_amount as u128) * (pool_lp_b as u128) / (lp_supply_b as u128)) as u64
        } else {
            0
        };
        
        debug!(
            "DAMM V1 {} share calc: vault_a_total={} actual_a_bal={} pool_lp_a={} lp_supply_a={} -> a_res={}, \
             vault_b_total={} actual_b_bal={} pool_lp_b={} lp_supply_b={} -> b_res={}",
            descriptor.label,
            final_a_vault_info.total_amount, actual_a_balance, pool_lp_a, lp_supply_a, a_reserve,
            final_b_vault_info.total_amount, actual_b_balance, pool_lp_b, lp_supply_b, b_reserve
        );
        
        (final_a_vault_info.token_vault, final_b_vault_info.token_vault, a_reserve, b_reserve, 
         final_a_vault_info.lp_mint, final_b_vault_info.lp_mint)
    } else {
        // Full 模式：通过 HTTP RPC 获取所有数据
        let slot = Some(context.slot);
        let rpc = ctx.rpc_client();
        
        // 获取 Vault 账户数据
        let (a_vault_data, b_vault_data) = tokio::try_join!(
            rpc.get_account_data(&a_vault),
            rpc.get_account_data(&b_vault),
        )?;
        
        let a_vault_info = parse_meteora_vault(&a_vault_data)?;
        let b_vault_info = parse_meteora_vault(&b_vault_data)?;
        
        // 获取池子的 LP token 余额、LP mint 总供应量、以及实际 token vault 余额
        let (pool_lp_a, pool_lp_b, lp_supply_a, lp_supply_b, actual_a_balance, actual_b_balance) = tokio::try_join!(
            ctx.get_token_account_amount_with_slot(&a_vault_lp, slot),
            ctx.get_token_account_amount_with_slot(&b_vault_lp, slot),
            ctx.get_mint_supply_with_slot(&a_vault_info.lp_mint, slot),
            ctx.get_mint_supply_with_slot(&b_vault_info.lp_mint, slot),
            ctx.get_token_account_amount_with_slot(&a_vault_info.token_vault, slot),
            ctx.get_token_account_amount_with_slot(&b_vault_info.token_vault, slot),
        )?;
        
        // ⭐ 改用 total_amount 计算池子份额
        let a_reserve = if lp_supply_a > 0 {
            ((a_vault_info.total_amount as u128) * (pool_lp_a as u128) / (lp_supply_a as u128)) as u64
        } else {
            0
        };
        let b_reserve = if lp_supply_b > 0 {
            ((b_vault_info.total_amount as u128) * (pool_lp_b as u128) / (lp_supply_b as u128)) as u64
        } else {
            0
        };
        
        debug!(
            "DAMM V1 {} (full mode): actual_a_bal={} pool_lp_a={} lp_supply_a={} -> a_res={}, \
             actual_b_bal={} pool_lp_b={} lp_supply_b={} -> b_res={}",
            descriptor.label,
            actual_a_balance, pool_lp_a, lp_supply_a, a_reserve,
            actual_b_balance, pool_lp_b, lp_supply_b, b_reserve
        );
        
        (a_vault_info.token_vault, b_vault_info.token_vault, a_reserve, b_reserve,
         a_vault_info.lp_mint, b_vault_info.lp_mint)
    };

    if a_reserve == 0 || b_reserve == 0 {
        anyhow::bail!("DAMM V1 pool {} has zero reserves", descriptor.address);
    }

    // 计算价格：price = quote_reserve / base_reserve
    let price_raw = (b_reserve as f64) / (a_reserve as f64);
    if !price_raw.is_finite() || price_raw <= 0.0 {
        anyhow::bail!("DAMM V1 invalid pool ratio");
    }
    
    let decimals_adjustment = mint_a_info.decimals as i32 - mint_b_info.decimals as i32;
    let price = price_raw * 10f64.powi(decimals_adjustment);
    
    let (normalized_pair, normalized_price) = normalize_pair(token_a_mint, token_b_mint, price)
        .ok_or_else(|| anyhow!("cannot normalize meteora damm v1 pair"))?;

    let total_lp_bps = trade_fee_ratio * 10_000.0;
    let total_protocol_bps = protocol_fee_ratio * 10_000.0;

    // 验证账户 owner
    let owner = if ctx.spot_only_mode() {
        // spot_only 模式跳过 owner 验证，使用默认 program id
        use std::str::FromStr;
        solana_pubkey::Pubkey::from_str(DAMM_V1_PROGRAM_ID)?
    } else {
        let slot = Some(context.slot);
        let owner = ctx
            .get_account_owner_with_slot(&descriptor.address, slot)
            .await?;

        if owner.to_string() != DAMM_V1_PROGRAM_ID {
            anyhow::bail!(
                "Account {} is not owned by DAMM V1 program (expected {}, got {}). \
                 Check your .env configuration - this address may belong to DAMM V2 or another program.",
                descriptor.address,
                DAMM_V1_PROGRAM_ID,
                owner
            );
        }
        owner
    };

    // ⭐ DEBUG: 临时添加 INFO 级别日志来诊断价格问题
    log::info!(
        "DAMM V1 {} PRICE DEBUG: a_res={} b_res={} | price_raw={:.8} | dec_adj={} | final_price={:.8} | \
         tokenA={} (dec={}) tokenB={} (dec={})",
        descriptor.label,
        a_reserve, b_reserve,
        price_raw,
        decimals_adjustment,
        price,
        token_a_mint, mint_a_info.decimals,
        token_b_mint, mint_b_info.decimals
    );

    debug!(
        "DAMM V1 pool {}: tokenA={} (dec={}), tokenB={} (dec={}), \
         a_reserve={}, b_reserve={}, price_raw={:.12}, dec_adj={}, price={:.12}, enabled={}",
        descriptor.address,
        token_a_mint,
        mint_a_info.decimals,
        token_b_mint,
        mint_b_info.decimals,
        a_reserve,
        b_reserve,
        price_raw,
        decimals_adjustment,
        price,
        enabled
    );

    // DAMM V1 不是 CLMM，没有 sqrt_price/liquidity
    // damm_state 设为 None
    let damm_state = None;

    // ✅ DAMM V1 共享金库架构需要订阅以下账户来正确计算份额和响应 swap 更新：
    // 1. aVault, bVault - Vault 账户（含 total_amount，虽然现在用 actual_balance 但仍需订阅）
    // 2. a_vault_lp, b_vault_lp - 池子的 LP token 账户（LP 份额）
    // 3. lp_mint_a, lp_mint_b - LP mint（获取总供应量）
    // 4. a_token_vault, b_token_vault - 实际的 token 账户（swap 后余额变化触发价格刷新）
    let dependent_accounts = vec![
        a_vault, b_vault,                      // Vault 账户
        a_vault_lp, b_vault_lp,                // 池子的 LP token 账户
        lp_mint_a, lp_mint_b,                  // LP mint
        a_token_vault, b_token_vault,          // ⭐ 实际 token vault（swap 更新触发价格刷新）
    ];

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: token_a_mint,
        quote_mint: token_b_mint,
        base_decimals: mint_a_info.decimals,
        quote_decimals: mint_b_info.decimals,
        price,
        sqrt_price_x64: 0, // DAMM V1 不使用 sqrt_price
        liquidity: None,   // DAMM V1 不使用 liquidity 字段
        base_reserve: Some(a_reserve as u128),
        quote_reserve: Some(b_reserve as u128),
        fees: Some(PoolFees {
            lp_fee_bps: total_lp_bps,
            protocol_fee_bps: total_protocol_bps,
            other_fee_bps: 0.0,
            meteora_dynamic: None,
        }),
        slot: context.slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state: None,
        clmm_accounts: None,
        damm_state,
        damm_accounts: Some(DammAccounts {
            program_id: owner,
            token_vault_a: a_token_vault,  // ✅ 现在存储的是实际的 token account
            token_vault_b: b_token_vault,
            token_a_program: mint_a_info.owner,
            token_b_program: mint_b_info.owner,
            // V1-specific vault fields for swap instruction
            a_vault: Some(a_vault),
            b_vault: Some(b_vault),
            a_vault_lp: Some(a_vault_lp),
            b_vault_lp: Some(b_vault_lp),
            a_vault_lp_mint: Some(lp_mint_a),
            b_vault_lp_mint: Some(lp_mint_b),
            protocol_token_a_fee: Some(protocol_token_a_fee),
            protocol_token_b_fee: Some(protocol_token_b_fee),
        }),
        lb_state: None,
        lb_accounts: None,
        pump_state: None,
        dependent_accounts,
    })
}

// Note: DAMM V1 是传统 AMM（不是 CLMM），不使用 sqrt_price/liquidity
// build_damm_state_v1 已移除，因为 V1 不需要 DammState

pub async fn decode(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    if data.len() < 8 {
        anyhow::bail!("meteora damm account too short");
    }

    // 检测版本：根据 DexKind 或账户大小判断
    match descriptor.kind {
        DexKind::MeteoraDammV1 => decode_v1(descriptor, data, context, ctx).await,
        DexKind::MeteoraDammV2 => decode_v2(descriptor, data, context, ctx).await,
        _ => anyhow::bail!("unsupported DEX kind for Meteora DAMM decoder"),
    }
}

async fn decode_v2(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    // spot_only 模式下跳过 owner 验证（信任 DexKind 配置），避免 HTTP 调用
    let owner = if ctx.spot_only_mode() {
        use std::str::FromStr;
        solana_pubkey::Pubkey::from_str(DAMM_V2_PROGRAM_ID).unwrap()
    } else {
        // 验证账户 owner 是否真的是 V2 程序，防止配置错误导致按错误布局解析
        let slot = Some(context.slot);
        let fetched_owner = ctx
            .get_account_owner_with_slot(&descriptor.address, slot)
            .await?;

        if fetched_owner.to_string() != DAMM_V2_PROGRAM_ID {
            anyhow::bail!(
                "Account {} is not owned by DAMM V2 program (expected {}, got {}). \
                 Check your .env configuration - this address may belong to DAMM V1 or another program.",
                descriptor.address,
                DAMM_V2_PROGRAM_ID,
                fetched_owner
            );
        }
        fetched_owner
    };

    let mut reader = AccountReader::new(&data[8..]);
    let fee_info = decode_pool_fees(&mut reader)?;

    let token_a_mint = reader.read_pubkey()?;
    let token_b_mint = reader.read_pubkey()?;
    let token_a_vault = reader.read_pubkey()?;
    let token_b_vault = reader.read_pubkey()?;
    reader.skip(32)?; // whitelisted vault
    reader.skip(32)?; // partner

    let liquidity = reader.read_u128()?;
    reader.read_u128()?; // padding
    reader.read_u64()?; // protocol a fee
    reader.read_u64()?; // protocol b fee
    reader.read_u64()?; // partner a fee
    reader.read_u64()?; // partner b fee
    let sqrt_min_price_x64 = reader.read_u128()?;
    let sqrt_max_price_x64 = reader.read_u128()?;
    let sqrt_price_x64 = reader.read_u128()?;

    reader.read_u64()?; // activation point
    reader.read_u8()?; // activation type
    reader.read_u8()?; // pool status
    reader.read_u8()?; // token a flag
    reader.read_u8()?; // token b flag
    let _collect_fee_mode = reader.read_u8()?; // collect fee mode
    reader.read_u8()?; // pool type
    reader.skip(2)?; // padding
    reader.skip(32)?; // fee a per liquidity
    reader.skip(32)?; // fee b per liquidity
    reader.read_u128()?; // permanent lock liquidity
    reader.skip(80)?; // metrics
    reader.skip(32)?; // creator
    reader.skip(8 * 6)?; // padding_1
    reader.skip(192 * 2)?; // reward infos

    // spot_only 模式：只获取 mint decimals（缓存），不拉 vault 余额
    let (mint_a_info, mint_b_info, vault_a_amount, vault_b_amount) = if ctx.spot_only_mode() {
        let (mint_a_info, mint_b_info) = tokio::try_join!(
            ctx.get_mint_info(&token_a_mint),
            ctx.get_mint_info(&token_b_mint),
        )?;
        (mint_a_info, mint_b_info, 0u64, 0u64)
    } else {
        let slot = Some(context.slot);
        tokio::try_join!(
            ctx.get_mint_info_with_slot(&token_a_mint, slot),
            ctx.get_mint_info_with_slot(&token_b_mint, slot),
            ctx.get_token_account_amount_with_slot(&token_a_vault, slot),
            ctx.get_token_account_amount_with_slot(&token_b_vault, slot),
        )?
    };
    let price = q64_to_price(sqrt_price_x64, mint_a_info.decimals, mint_b_info.decimals)?;
    let (normalized_pair, normalized_price) = normalize_pair(token_a_mint, token_b_mint, price)
        .ok_or_else(|| anyhow!("cannot normalize meteora damm pair"))?;

    let damm_state = build_damm_state(
        &fee_info,
        liquidity,
        sqrt_price_x64,
        sqrt_min_price_x64,
        sqrt_max_price_x64,
    );

    // ⚠️ 费率可能因为链上数据格式问题而异常大，需要 clamp 到合理范围
    // 正常情况下 fee_ratio 应该在 0~1 之间（0%~100%）
    let lp_fee_ratio = (fee_info.lp_base_fee_ratio + fee_info.dynamic_fee_ratio).clamp(0.0, MAX_FEE_RATIO);
    let protocol_fee_ratio = fee_info.protocol_fee_ratio.clamp(0.0, MAX_FEE_RATIO);
    let total_lp_bps = lp_fee_ratio * 10_000.0;
    let total_protocol_bps = protocol_fee_ratio * 10_000.0;

    // ✅ Spot-only 模式：只订阅主池账户，不订阅任何依赖账户
    let dependent_accounts = if ctx.spot_only_mode() {
        vec![]
    } else {
        vec![token_a_vault, token_b_vault]
    };

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: token_a_mint,
        quote_mint: token_b_mint,
        base_decimals: mint_a_info.decimals,
        quote_decimals: mint_b_info.decimals,
        price,
        sqrt_price_x64: sqrt_price_x64,
        liquidity: Some(liquidity),
        base_reserve: if ctx.spot_only_mode() { None } else { Some(vault_a_amount as u128) },
        quote_reserve: if ctx.spot_only_mode() { None } else { Some(vault_b_amount as u128) },
        fees: Some(PoolFees {
            lp_fee_bps: total_lp_bps,
            protocol_fee_bps: total_protocol_bps,
            other_fee_bps: 0.0,
            meteora_dynamic: None, // DAMM doesn't use DLMM dynamic fees
        }),
        slot: context.slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state: None,
        clmm_accounts: None,
        damm_state,
        damm_accounts: Some(DammAccounts {
            program_id: owner,
            token_vault_a: token_a_vault,
            token_vault_b: token_b_vault,
            token_a_program: mint_a_info.owner,
            token_b_program: mint_b_info.owner,
            // V2 doesn't use shared vaults - all V1-specific fields are None
            a_vault: None,
            b_vault: None,
            a_vault_lp: None,
            b_vault_lp: None,
            a_vault_lp_mint: None,
            b_vault_lp_mint: None,
            protocol_token_a_fee: None,
            protocol_token_b_fee: None,
        }),
        lb_state: None,
        lb_accounts: None,
        pump_state: None,
        dependent_accounts,
    })
}

pub fn simulate_trade(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    if !(base_amount.is_finite() && base_amount > 0.0) {
        return None;
    }

    if let Some(state) = snapshot.damm_state.as_ref() {
        if let Some(quote) = simulate_trade_precise(snapshot, state, side, base_amount) {
            return Some(quote);
        }
        debug!(
            "Falling back to constant-product approximation for DAMM {}",
            snapshot.descriptor.label
        );
    }

    fallback_constant_product(snapshot, side, base_amount)
}

fn simulate_trade_precise(
    snapshot: &PoolSnapshot,
    state: &DammState,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    if !(state.total_fee_ratio >= 0.0 && state.total_fee_ratio < 1.0) {
        return None;
    }

    let fee_ratio = state.total_fee_ratio.clamp(0.0, MAX_FEE_RATIO);
    if fee_ratio >= 1.0 {
        return None;
    }

    let token_a_scale = pow10(snapshot.base_decimals);
    let token_b_scale = pow10(snapshot.quote_decimals);
    let inverted = snapshot.normalized_pair.inverted;

    let ctx = DammSwapContext::new(state)?;

    match (side, inverted) {
        (TradeSide::Buy, false) => {
            let base_out_raw = base_amount * token_a_scale;
            if base_out_raw <= 0.0 {
                return None;
            }
            if base_out_raw > ctx.available_token_a() + EPSILON {
                return None;
            }
            let step = swap_token_a_out(&ctx, base_out_raw)?;
            let net_quote_in = step.token_in;
            if !(net_quote_in.is_finite() && net_quote_in > 0.0) {
                return None;
            }
            let gross_quote_in = net_quote_in / (1.0 - fee_ratio);
            if !(gross_quote_in.is_finite() && gross_quote_in > 0.0) {
                return None;
            }
            let fee_raw = gross_quote_in - net_quote_in;
            let quote_cost = gross_quote_in / token_b_scale;
            let fee_in_input = fee_raw / token_b_scale;
            let effective_price = quote_cost / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Sell, false) => {
            let base_in_raw = base_amount * token_a_scale;
            if base_in_raw <= 0.0 {
                return None;
            }
            let net_base_in = base_in_raw * (1.0 - fee_ratio);
            if !(net_base_in.is_finite() && net_base_in > 0.0) {
                return None;
            }
            let step = swap_token_a_in(&ctx, net_base_in)?;
            let quote_out_raw = step.token_out;
            if !(quote_out_raw.is_finite() && quote_out_raw > 0.0) {
                return None;
            }
            let fee_raw = base_in_raw - net_base_in;
            let quote_proceeds = quote_out_raw / token_b_scale;
            let fee_in_input = fee_raw / token_a_scale;
            let effective_price = quote_proceeds / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Buy, true) => {
            let base_out_raw = base_amount * token_b_scale;
            if base_out_raw <= 0.0 {
                return None;
            }
            if base_out_raw > ctx.available_token_b() + EPSILON {
                return None;
            }
            let step = swap_token_b_out(&ctx, base_out_raw)?;
            let net_token_a_in = step.token_in;
            if !(net_token_a_in.is_finite() && net_token_a_in > 0.0) {
                return None;
            }
            let gross_token_a_in = net_token_a_in / (1.0 - fee_ratio);
            if !(gross_token_a_in.is_finite() && gross_token_a_in > 0.0) {
                return None;
            }
            let fee_raw = gross_token_a_in - net_token_a_in;
            let quote_cost = gross_token_a_in / token_a_scale;
            let fee_in_input = fee_raw / token_a_scale;
            let effective_price = quote_cost / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Sell, true) => {
            let base_in_raw = base_amount * token_b_scale;
            if base_in_raw <= 0.0 {
                return None;
            }
            let net_base_in = base_in_raw * (1.0 - fee_ratio);
            if !(net_base_in.is_finite() && net_base_in > 0.0) {
                return None;
            }
            let step = swap_token_b_in(&ctx, net_base_in)?;
            let quote_out_raw = step.token_out;
            if !(quote_out_raw.is_finite() && quote_out_raw > 0.0) {
                return None;
            }
            let fee_raw = base_in_raw - net_base_in;
            let quote_proceeds = quote_out_raw / token_a_scale;
            let fee_in_input = fee_raw / token_b_scale;
            let effective_price = quote_proceeds / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input,
            })
        }
    }
}

fn pow10(decimals: u8) -> f64 {
    10f64.powi(decimals as i32)
}

impl DammSwapContext {
    fn new(state: &DammState) -> Option<Self> {
        if !(state.liquidity.is_finite() && state.liquidity > 0.0) {
            return None;
        }
        if !(state.sqrt_min_price > 0.0 && state.sqrt_max_price > state.sqrt_min_price) {
            return None;
        }
        Some(Self {
            sqrt_price: state.sqrt_price,
            sqrt_min_price: state.sqrt_min_price,
            sqrt_max_price: state.sqrt_max_price,
            liquidity: state.liquidity,
        })
    }

    fn available_token_a(&self) -> f64 {
        let value = self.liquidity * (1.0 / self.sqrt_price - 1.0 / self.sqrt_max_price);
        if value.is_finite() && value > 0.0 {
            value
        } else {
            0.0
        }
    }

    fn available_token_b(&self) -> f64 {
        let value = self.liquidity * (self.sqrt_price - self.sqrt_min_price);
        if value.is_finite() && value > 0.0 {
            value
        } else {
            0.0
        }
    }
}

fn swap_token_a_out(ctx: &DammSwapContext, amount_out: f64) -> Option<SwapStep> {
    if amount_out <= 0.0 {
        return Some(SwapStep {
            token_in: 0.0,
            token_out: 0.0,
        });
    }
    if amount_out > ctx.available_token_a() + EPSILON {
        return None;
    }
    let inv_old = 1.0 / ctx.sqrt_price;
    let inv_new = inv_old - amount_out / ctx.liquidity;
    if inv_new <= 0.0 {
        return None;
    }
    let sqrt_new = 1.0 / inv_new;
    if sqrt_new > ctx.sqrt_max_price + EPSILON {
        return None;
    }
    let token_in = ctx.liquidity * (sqrt_new - ctx.sqrt_price);
    if !(token_in.is_finite() && token_in > 0.0) {
        return None;
    }
    Some(SwapStep {
        token_in,
        token_out: amount_out,
    })
}

fn swap_token_a_in(ctx: &DammSwapContext, amount_in: f64) -> Option<SwapStep> {
    if amount_in <= 0.0 {
        return Some(SwapStep {
            token_in: 0.0,
            token_out: 0.0,
        });
    }
    let inv_old = 1.0 / ctx.sqrt_price;
    let inv_new = inv_old + amount_in / ctx.liquidity;
    if inv_new <= 0.0 {
        return None;
    }
    let sqrt_new = 1.0 / inv_new;
    if sqrt_new < ctx.sqrt_min_price - EPSILON {
        return None;
    }
    let token_out = ctx.liquidity * (ctx.sqrt_price - sqrt_new);
    if !(token_out.is_finite() && token_out > 0.0) {
        return None;
    }
    Some(SwapStep {
        token_in: amount_in,
        token_out,
    })
}

fn swap_token_b_out(ctx: &DammSwapContext, amount_out: f64) -> Option<SwapStep> {
    if amount_out <= 0.0 {
        return Some(SwapStep {
            token_in: 0.0,
            token_out: 0.0,
        });
    }
    if amount_out > ctx.available_token_b() + EPSILON {
        return None;
    }
    let sqrt_new = ctx.sqrt_price - amount_out / ctx.liquidity;
    if sqrt_new < ctx.sqrt_min_price - EPSILON {
        return None;
    }
    let token_in = ctx.liquidity * (1.0 / sqrt_new - 1.0 / ctx.sqrt_price);
    if !(token_in.is_finite() && token_in > 0.0) {
        return None;
    }
    Some(SwapStep {
        token_in,
        token_out: amount_out,
    })
}

fn swap_token_b_in(ctx: &DammSwapContext, amount_in: f64) -> Option<SwapStep> {
    if amount_in <= 0.0 {
        return Some(SwapStep {
            token_in: 0.0,
            token_out: 0.0,
        });
    }
    let sqrt_new = ctx.sqrt_price + amount_in / ctx.liquidity;
    if sqrt_new > ctx.sqrt_max_price + EPSILON {
        return None;
    }
    // 当输入 token B 时，sqrt_price 增加，token A 输出 = L * (1/sqrt_old - 1/sqrt_new)
    // 注意：这里是 1/sqrt_price - 1/sqrt_new，因为 sqrt_new > sqrt_price
    let token_out = ctx.liquidity * (1.0 / ctx.sqrt_price - 1.0 / sqrt_new);
    if !(token_out.is_finite() && token_out > 0.0) {
        return None;
    }
    Some(SwapStep {
        token_in: amount_in,
        token_out,
    })
}

fn fallback_constant_product(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    let base_reserve = snapshot.base_reserve? as f64;
    let quote_reserve = snapshot.quote_reserve? as f64;
    let fees = snapshot.fees.as_ref()?;
    let fee_ratio = fees.total_ratio();
    if !(fee_ratio >= 0.0 && fee_ratio < 1.0) {
        return None;
    }

    let inverted = snapshot.normalized_pair.inverted;
    let base_decimals = snapshot.base_decimals as i32;
    let quote_decimals = snapshot.quote_decimals as i32;

    let normalized_base_decimals = if inverted {
        quote_decimals
    } else {
        base_decimals
    };
    let normalized_quote_decimals = if inverted {
        base_decimals
    } else {
        quote_decimals
    };

    let normalized_base_scale = 10f64.powi(normalized_base_decimals);
    let normalized_quote_scale = 10f64.powi(normalized_quote_decimals);

    let to_raw = |amount: f64, scale: f64| amount * scale;
    let to_units = |amount_raw: f64, scale: f64| amount_raw / scale;

    match (side, inverted) {
        (TradeSide::Buy, false) => {
            let target_base_out_raw = to_raw(base_amount, normalized_base_scale);
            if target_base_out_raw >= base_reserve {
                return None;
            }
            let net_quote_in =
                quote_reserve * target_base_out_raw / (base_reserve - target_base_out_raw);
            let gross_quote_in = net_quote_in / (1.0 - fee_ratio);
            let fee_quote = gross_quote_in - net_quote_in;

            let quote_cost = to_units(gross_quote_in, normalized_quote_scale);
            let fee_in_input = to_units(fee_quote, normalized_quote_scale);
            let effective_price = quote_cost / base_amount;

            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Sell, false) => {
            let base_in_raw = to_raw(base_amount, normalized_base_scale);
            let net_base_in = base_in_raw * (1.0 - fee_ratio);
            if net_base_in <= 0.0 {
                return None;
            }
            let fee_base = base_in_raw - net_base_in;
            let quote_out = quote_reserve * net_base_in / (base_reserve + net_base_in);

            let quote_proceeds = to_units(quote_out, normalized_quote_scale);
            let fee_in_input = to_units(fee_base, normalized_base_scale);
            let effective_price = quote_proceeds / base_amount;

            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Buy, true) => {
            let target_quote_out_raw = to_raw(base_amount, normalized_base_scale);
            if target_quote_out_raw >= quote_reserve {
                return None;
            }
            let net_base_in =
                target_quote_out_raw * base_reserve / (quote_reserve - target_quote_out_raw);
            let gross_base_in = net_base_in / (1.0 - fee_ratio);
            let fee_base = gross_base_in - net_base_in;

            let quote_cost = to_units(gross_base_in, normalized_quote_scale);
            let fee_in_input = to_units(fee_base, normalized_quote_scale);
            let effective_price = quote_cost / base_amount;

            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Sell, true) => {
            let quote_in_raw = to_raw(base_amount, normalized_base_scale);
            let net_quote_in = quote_in_raw * (1.0 - fee_ratio);
            if net_quote_in <= 0.0 {
                return None;
            }
            let fee_quote = quote_in_raw - net_quote_in;
            let base_out = base_reserve * net_quote_in / (quote_reserve + net_quote_in);

            let quote_proceeds = to_units(base_out, normalized_quote_scale);
            let fee_in_input = to_units(fee_quote, normalized_base_scale);
            let effective_price = quote_proceeds / base_amount;

            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input,
            })
        }
    }
}

struct DammFeeInfo {
    base_fee_ratio: f64,
    lp_base_fee_ratio: f64,
    protocol_fee_ratio: f64,
    dynamic_fee_ratio: f64,
}

#[derive(Clone)]
struct DammSwapContext {
    sqrt_price: f64,
    sqrt_min_price: f64,
    sqrt_max_price: f64,
    liquidity: f64,
}

struct SwapStep {
    token_in: f64,
    token_out: f64,
}

fn decode_pool_fees(reader: &mut AccountReader) -> Result<DammFeeInfo> {
    let base_fee_numerator = reader.read_u64()?;
    reader.read_u8()?; // fee_scheduler_mode
    reader.skip(5)?; // padding
    reader.read_u16()?; // number_of_period
    reader.read_u64()?; // period_frequency
    reader.read_u64()?; // reduction_factor
    reader.read_u64()?; // padding_1

    let protocol_fee_percent = reader.read_u8()?;
    let _partner_fee_percent = reader.read_u8()?;
    let referral_fee_percent = reader.read_u8()?;
    reader.skip(5)?; // padding_0
    let dynamic_fee = decode_dynamic_fee(reader)?;
    reader.skip(8 * 2)?; // padding_1

    let base_fee_ratio = (base_fee_numerator as f64) / 1_000_000.0;
    let base_fee_ratio = if base_fee_ratio.is_finite() && base_fee_ratio >= 0.0 {
        base_fee_ratio
    } else {
        0.0
    };

    let mut protocol_fee_ratio =
        base_fee_ratio * (protocol_fee_percent as f64 / 100.0).clamp(0.0, 1.0);
    // Referral fees are carved out of the protocol portion.
    let referral_ratio = protocol_fee_ratio * (referral_fee_percent as f64 / 100.0).clamp(0.0, 1.0);
    protocol_fee_ratio = (protocol_fee_ratio - referral_ratio).max(0.0);

    let lp_base_fee_ratio = (base_fee_ratio - protocol_fee_ratio).max(0.0);

    let dynamic_fee_ratio = dynamic_fee
        .as_ref()
        .map(|value| compute_dynamic_fee_ratio(value, base_fee_ratio))
        .unwrap_or(0.0);

    Ok(DammFeeInfo {
        base_fee_ratio,
        lp_base_fee_ratio,
        protocol_fee_ratio,
        dynamic_fee_ratio,
    })
}

fn decode_dynamic_fee(reader: &mut AccountReader) -> Result<Option<DammDynamicFee>> {
    let initialized = reader.read_u8()? != 0;
    reader.skip(7)?; // padding
    let max_volatility_accumulator = reader.read_u32()?;
    let variable_fee_control = reader.read_u32()?;
    let bin_step = reader.read_u16()?;
    let filter_period = reader.read_u16()?;
    let decay_period = reader.read_u16()?;
    let reduction_factor = reader.read_u16()?;
    reader.read_u64()?; // last_update_timestamp
    let bin_step_u128 = reader.read_u128()?;
    let sqrt_price_reference = reader.read_u128()?;
    let volatility_accumulator = reader.read_u128()?;
    let volatility_reference = reader.read_u128()?;

    if initialized {
        Ok(Some(DammDynamicFee {
            bin_step,
            bin_step_q64: bin_step_u128,
            _filter_period: filter_period,
            _decay_period: decay_period,
            _reduction_factor: reduction_factor,
            max_volatility_accumulator,
            variable_fee_control,
            volatility_accumulator,
            _volatility_reference: volatility_reference,
            _sqrt_price_reference: sqrt_price_reference,
        }))
    } else {
        Ok(None)
    }
}

fn compute_dynamic_fee_ratio(dynamic: &DammDynamicFee, base_fee_ratio: f64) -> f64 {
    if dynamic.variable_fee_control == 0 || dynamic.bin_step == 0 {
        return 0.0;
    }

    let mut va = dynamic.volatility_accumulator as f64;
    if !va.is_finite() || va <= 0.0 {
        va = 0.0;
    }

    let max_acc = dynamic.max_volatility_accumulator as f64;
    if max_acc.is_finite() && max_acc > 0.0 {
        va = va.min(max_acc);
    }

    if va <= 0.0 {
        return 0.0;
    }

    let mut step = if dynamic.bin_step_q64 != 0 {
        ((dynamic.bin_step_q64 as f64) / Q64) - 1.0
    } else {
        0.0
    };
    if !(step.is_finite() && step > 0.0) {
        step = (dynamic.bin_step as f64) / 10_000.0;
    }
    if !(step.is_finite() && step > 0.0) {
        return 0.0;
    }

    let c = dynamic.variable_fee_control as f64;
    let mut ratio = ((va * step).powi(2) * c) / 100_000_000_000.0;
    if !ratio.is_finite() || ratio < 0.0 {
        ratio = 0.0;
    }

    let max_ratio = (base_fee_ratio * 0.2).max(0.0);
    ratio.min(max_ratio)
}

/// Warmup: 解析 DAMM V1 池子的所有依赖账户地址，用于 spot_only 模式预订阅
/// 返回 8 个账户: (a_vault, b_vault, a_token_vault, b_token_vault, a_vault_lp, b_vault_lp, lp_mint_a, lp_mint_b)
pub async fn warmup_parse_vaults(
    pool: &solana_pubkey::Pubkey,
    ctx: &AppContext,
) -> Result<(
    solana_pubkey::Pubkey,  // a_vault
    solana_pubkey::Pubkey,  // b_vault
    solana_pubkey::Pubkey,  // a_token_vault
    solana_pubkey::Pubkey,  // b_token_vault
    solana_pubkey::Pubkey,  // a_vault_lp
    solana_pubkey::Pubkey,  // b_vault_lp
    solana_pubkey::Pubkey,  // lp_mint_a
    solana_pubkey::Pubkey,  // lp_mint_b
)> {
    // 获取主池账户数据
    let pool_data = ctx.rpc_client().get_account_data(pool).await?;
    
    if pool_data.len() < 232 {
        anyhow::bail!("DAMM V1 pool data too short for warmup parsing: {}", pool_data.len());
    }
    
    // 解析池账户布局
    // [104-136] aVault, [136-168] bVault, [168-200] aVaultLp, [200-232] bVaultLp
    let a_vault = solana_pubkey::Pubkey::try_from(&pool_data[104..136])
        .map_err(|_| anyhow!("Failed to parse a_vault from DAMM V1 pool"))?;
    let b_vault = solana_pubkey::Pubkey::try_from(&pool_data[136..168])
        .map_err(|_| anyhow!("Failed to parse b_vault from DAMM V1 pool"))?;
    let a_vault_lp = solana_pubkey::Pubkey::try_from(&pool_data[168..200])
        .map_err(|_| anyhow!("Failed to parse a_vault_lp from DAMM V1 pool"))?;
    let b_vault_lp = solana_pubkey::Pubkey::try_from(&pool_data[200..232])
        .map_err(|_| anyhow!("Failed to parse b_vault_lp from DAMM V1 pool"))?;
    
    // 获取 Vault 账户数据并解析 token_vault 和 lp_mint 地址
    let rpc = ctx.rpc_client();
    let (a_vault_data, b_vault_data) = tokio::try_join!(
        rpc.get_account_data(&a_vault),
        rpc.get_account_data(&b_vault),
    )?;
    
    let a_vault_info = parse_meteora_vault(&a_vault_data)?;
    let b_vault_info = parse_meteora_vault(&b_vault_data)?;
    
    debug!(
        "DAMM V1 warmup {}: parsed 8 accounts for subscription",
        pool
    );
    
    Ok((
        a_vault, b_vault, 
        a_vault_info.token_vault, b_vault_info.token_vault,
        a_vault_lp, b_vault_lp,
        a_vault_info.lp_mint, b_vault_info.lp_mint,
    ))
}
