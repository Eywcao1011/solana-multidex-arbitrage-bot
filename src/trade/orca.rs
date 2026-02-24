use anyhow::{anyhow, bail, Result};
use log::{debug, warn};
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::dex::{orca, DexKind, PoolSnapshot, TradeSide};
use crate::rpc::AppContext;
use crate::sidecar_client::SidecarClient;
use once_cell::sync::Lazy;
use std::str::FromStr;

const DEFAULT_ORCA_SLIPPAGE_BPS: f64 = 50.0;

// ✅ 全局缓存：warmup 时存储的 tick_arrays，prepare_swap 时使用
// 类似 DLMM 的 INJECTED_POOL_CONFIGS 模式
static ORCA_TICK_ARRAYS_CACHE: once_cell::sync::Lazy<RwLock<HashMap<Pubkey, Vec<Pubkey>>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));

/// 注入 Orca pool 的 tick_arrays（用于 warmup 缓存）
///
/// warmup 时调用，存储预取范围内的 tick_arrays
pub fn inject_tick_arrays(pool: Pubkey, tick_arrays: Vec<Pubkey>) {
    if let Ok(mut cache) = ORCA_TICK_ARRAYS_CACHE.write() {
        cache.insert(pool, tick_arrays);
    }
}

/// 获取缓存的 tick_arrays
pub fn get_cached_tick_arrays(pool: &Pubkey) -> Option<Vec<Pubkey>> {
    ORCA_TICK_ARRAYS_CACHE.read().ok()?.get(pool).cloned()
}

/// 清空 warmup 缓存的 tick_arrays
pub fn clear_tick_arrays_cache() {
    if let Ok(mut cache) = ORCA_TICK_ARRAYS_CACHE.write() {
        cache.clear();
    }
}

/// Refresh cached Orca tick arrays using on-chain state (no blocking on trade path).
pub async fn refresh_pool_cached_tick_arrays_with_state(
    ctx: &AppContext,
    pool: &Pubkey,
    program_id: &Pubkey,
    tick_current: i32,
    tick_spacing: i32,
) -> Result<usize> {
    let addresses =
        orca::compute_tick_array_subscriptions(program_id, pool, tick_current, tick_spacing);
    if addresses.is_empty() {
        return Ok(0);
    }

    let arrays = match ctx.get_orca_tick_arrays_batch(&addresses, tick_spacing).await {
        Ok(found) if !found.is_empty() => found.into_iter().map(|(addr, _)| addr).collect(),
        Ok(_) => addresses.clone(),
        Err(err) => {
            warn!(
                "Orca tick_arrays refresh failed for pool {}: {}; using derived list",
                pool, err
            );
            addresses.clone()
        }
    };

    ctx.set_orca_tick_arrays(pool, arrays.clone());
    inject_tick_arrays(*pool, arrays.clone());
    Ok(arrays.len())
}

/// Refresh cached Orca tick arrays using sidecar introspection.
pub async fn refresh_pool_cached_tick_arrays(ctx: &AppContext, pool: &Pubkey) -> Result<usize> {
    static REFRESH_SIDECAR: Lazy<Result<SidecarClient, anyhow::Error>> =
        Lazy::new(|| SidecarClient::from_env());

    let sidecar = REFRESH_SIDECAR
        .as_ref()
        .map_err(|e| anyhow!("SidecarClient unavailable for Orca refresh: {}", e))?;

    let pool_str = pool.to_string();
    let resp = sidecar.introspect_orca(&pool_str).await?;
    if !resp.ok {
        return Err(anyhow!(
            "orca introspect returned ok=false: {:?}",
            resp.error
        ));
    }

    let program_id = resp
        .program_id
        .as_deref()
        .ok_or_else(|| anyhow!("orca introspect missing program_id"))?;
    let tick_current = resp
        .tick_current_index
        .ok_or_else(|| anyhow!("orca introspect missing tick_current_index"))?;
    let tick_spacing = resp
        .tick_spacing
        .ok_or_else(|| anyhow!("orca introspect missing tick_spacing"))?;

    let program_id = Pubkey::from_str(program_id)?;
    refresh_pool_cached_tick_arrays_with_state(
        ctx,
        pool,
        &program_id,
        tick_current,
        tick_spacing,
    )
    .await
}

/// 从 sqrt_price_x64 计算当前 tick index
/// tick = log_{1.0001}(price) = ln(price) / ln(1.0001)
/// price = (sqrt_price / 2^64)^2
fn sqrt_price_x64_to_tick_index(sqrt_price_x64: u128) -> i32 {
    if sqrt_price_x64 == 0 {
        return 0;
    }
    // sqrt_price = sqrt_price_x64 / 2^64
    // price = sqrt_price^2 = sqrt_price_x64^2 / 2^128
    // 使用 f64 计算（精度足够用于 tick index）
    let sqrt_price = (sqrt_price_x64 as f64) / ((1u128 << 64) as f64);
    let price = sqrt_price * sqrt_price;
    if price <= 0.0 || !price.is_finite() {
        return 0;
    }
    // tick = ln(price) / ln(1.0001)
    let tick = (price.ln() / 1.0001f64.ln()).floor() as i32;
    tick
}

const TICK_ARRAY_SIZE: i32 = 88;
const REQUIRED_TICK_ARRAYS: usize = 3;
const EXTRA_TICK_ARRAYS: usize = 2;

/// 根据当前 tick 计算需要的 3 个 tick arrays
///
/// 按交易方向返回连续的 3 个 arrays：
/// - a_to_b=true：当前及更低的 tick arrays
/// - a_to_b=false：当前及更高的 tick arrays
fn derive_required_tick_arrays(
    program_id: &Pubkey,
    pool: &Pubkey,
    tick_current: i32,
    tick_spacing: i32,
    a_to_b: bool,
) -> Vec<Pubkey> {
    let array_span = tick_spacing * TICK_ARRAY_SIZE;
    if array_span == 0 {
        return Vec::new();
    }

    let start_index = orca::tick_array_start_index(tick_current, tick_spacing);
    let total_needed = REQUIRED_TICK_ARRAYS + EXTRA_TICK_ARRAYS;
    let mut out = Vec::with_capacity(total_needed);

    for step in 0..total_needed {
        let offset = if a_to_b {
            -(step as i32)
        } else {
            step as i32
        };
        let idx = start_index + offset * array_span;
        if let Ok(addr) = orca::tick_array_address(program_id, pool, idx) {
            out.push(addr);
        }
    }

    out
}

fn compute_sdk_tick_arrays(
    cached: Option<&[Pubkey]>,
    tick_current: i32,
    tick_spacing: i32,
    a_to_b: bool,
    program_id: &Pubkey,
    pool_address: &Pubkey,
) -> Vec<Pubkey> {
    let required =
        derive_required_tick_arrays(program_id, pool_address, tick_current, tick_spacing, a_to_b);
    if required.len() < REQUIRED_TICK_ARRAYS {
        return Vec::new();
    }

    if let Some(cached) = cached.filter(|v| !v.is_empty()) {
        let cache_set: HashSet<_> = cached.iter().copied().collect();
        let missing = required.iter().filter(|addr| !cache_set.contains(addr)).count();
        if missing > 0 {
            warn!(
                "Orca tick_arrays cache miss: pool={} missing={}/{} (tick_current={}, spacing={}, a_to_b={})",
                pool_address,
                missing,
                required.len(),
                tick_current,
                tick_spacing,
                a_to_b
            );
            return Vec::new();
        }
    }

    required.into_iter().take(REQUIRED_TICK_ARRAYS).collect()
}



/// Whirlpool swap instruction discriminator (Anchor)
/// sha256("global:swap")[0..8]
const SWAP_DISCRIMINATOR: [u8; 8] = [0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8];
/// Whirlpool swap_v2 instruction discriminator (Anchor)
/// sha256("global:swap_v2")[0..8]
const SWAP_V2_DISCRIMINATOR: [u8; 8] = [0x2b, 0x04, 0xed, 0x0b, 0x1a, 0xc9, 0x1e, 0x62];
const MEMO_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

#[derive(Default)]
pub struct OrcaExecutor;

impl TradeExecutor for OrcaExecutor {
    fn dex(&self) -> DexKind {
        DexKind::OrcaWhirlpool
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        // Basic validation
        // For exact-in Buy mode (quote_amount_in set), base_amount can be 0
        let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
        if !is_exact_in_buy && request.base_amount <= 0.0 {
            bail!("base_amount must be positive, got {}", request.base_amount);
        }

        if request.min_amount_out.is_none() {
            bail!("min_amount_out (protection) required for Orca execution");
        }

        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }

        // Get CLMM accounts from snapshot (populated by decode)
        let clmm = snapshot
            .clmm_accounts
            .as_ref()
            .ok_or_else(|| anyhow!("missing clmm_accounts in Orca snapshot"))?;

        // Determine swap direction based on normalized pair
        // In normalized view: base is the "main" token, quote is SOL/USDC
        // For Orca: token_a and token_b are the raw pool tokens
        // a_to_b = true means swap token_a -> token_b
        let inverted = snapshot.normalized_pair.inverted;
        let (norm_base_decimals, norm_quote_decimals) = if inverted {
            (snapshot.quote_decimals, snapshot.base_decimals)
        } else {
            (snapshot.base_decimals, snapshot.quote_decimals)
        };
        let is_selling_base = request.side == TradeSide::Sell;
        // When inverted=false: normalized_base=token_a, selling base means a_to_b=true
        // When inverted=true: normalized_base=token_b, selling base means a_to_b=false (b_to_a)
        let a_to_b = is_selling_base ^ inverted;

        let (amount, other_amount_threshold) = match request.side {
            TradeSide::Sell => {
                // Selling base (token A) for quote (token B)
                // amount = base amount in, other_amount_threshold = min quote out
                let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                let min_out =
                    units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;
                (amount_in, min_out)
            }
            TradeSide::Buy => {
                // Buying base (token A) with quote (token B)
                // Whirlpool swap uses amount_specified_is_input=true
                // So we specify quote in, min base out
                let (quote_in, min_base_out) = if let Some(quote_amount) = request.quote_amount_in {
                    // ✅ Spot-only exact-in mode: fixed quote input, min base output
                    let quote_in = units_to_amount_raw(quote_amount, norm_quote_decimals)?;
                    let min_base = request.min_amount_out.unwrap_or(0.0);
                    let min_base_out =
                        units_to_amount_raw(min_base.max(0.000001), norm_base_decimals)?;
                    (quote_in, min_base_out)
                } else {
                    // ✅ Full mode: min_amount_out is max_quote_in, base_amount is expected output
                    let max_quote_in =
                        units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;
                    let min_base_out =
                        units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    (max_quote_in, min_base_out)
                };
                (quote_in, min_base_out)
            }
        };

        // ✅ Map normalized accounts to token_a/token_b
        // user_base_account = ATA(normalized_base), user_quote_account = ATA(normalized_quote)
        // When inverted=false: normalized_base=token_a, normalized_quote=token_b
        // When inverted=true: normalized_base=token_b, normalized_quote=token_a
        let (user_token_a, user_token_b) = if inverted {
            // inverted: normalized_base=token_b, normalized_quote=token_a
            (request.accounts.user_quote_account, request.accounts.user_base_account)
        } else {
            // not inverted: normalized_base=token_a, normalized_quote=token_b
            (request.accounts.user_base_account, request.accounts.user_quote_account)
        };

        // Compute tick arrays needed for this swap
        // ✅ 使用链上 tick_current_index（通过 WebSocket 实时更新），比 sqrt_price 计算更可靠
        // Note: 之前尝试从 sqrt_price_x64 计算 tick 但浮点精度可能导致偏移
        let computed_tick = sqrt_price_x64_to_tick_index(snapshot.sqrt_price_x64);
        let chain_tick = clmm.tick_current_index;
        
        // 使用链上 tick，因为它是 WebSocket 订阅实时更新的
        let current_tick = chain_tick;
        
        // Debug: 记录两个值以便诊断
        if computed_tick != chain_tick {
            debug!(
                "Orca tick mismatch: computed={} chain={} sqrt_price={} pool={}",
                computed_tick, chain_tick, snapshot.sqrt_price_x64, snapshot.descriptor.address
            );
        }
        
        let cached_tick_arrays = get_cached_tick_arrays(&snapshot.descriptor.address);
        let tick_arrays = compute_sdk_tick_arrays(
            cached_tick_arrays.as_deref(),
            current_tick,
            clmm.tick_spacing,
            a_to_b,
            &clmm.program_id,
            &snapshot.descriptor.address,
        );
        if tick_arrays.len() < 3 {
            bail!(
                "Orca {} tick_arrays_cache_miss (have {}, need {})",
                snapshot.descriptor.label,
                tick_arrays.len(),
                REQUIRED_TICK_ARRAYS
            );
        }
        let tick_array_0 = tick_arrays.get(0).copied().unwrap_or(snapshot.descriptor.address);
        let tick_array_1 = tick_arrays.get(1).copied().unwrap_or(tick_array_0);
        let tick_array_2 = tick_arrays.get(2).copied().unwrap_or(tick_array_1);
        
        // ⚠️ Debug: 追踪 tick array 选择以诊断 6023 错误
        log::debug!(
            "Orca swap: pool={} tick={} a_to_b={} arrays=[{}, {}, {}]",
            snapshot.descriptor.address,
            current_tick,
            a_to_b,
            tick_array_0,
            tick_array_1,
            tick_array_2
        );

        // Derive oracle PDA
        let (oracle, _) = Pubkey::find_program_address(
            &[b"oracle", snapshot.descriptor.address.as_ref()],
            &clmm.program_id,
        );

        // Build the swap instruction
        let use_swap_v2 =
            clmm.token_a_program != spl_token::ID || clmm.token_b_program != spl_token::ID;

        let instruction = if use_swap_v2 {
            build_swap_v2_instruction(
                &clmm.program_id,
                &request.accounts.payer,
                &snapshot.descriptor.address,
                &clmm.token_mint_a,
                &clmm.token_mint_b,
                &user_token_a,
                &clmm.token_vault_a,
                &user_token_b,
                &clmm.token_vault_b,
                &clmm.token_a_program,
                &clmm.token_b_program,
                &tick_array_0,
                &tick_array_1,
                &tick_array_2,
                &oracle,
                amount,
                other_amount_threshold,
                a_to_b,
            )?
        } else {
            build_swap_instruction(
                &clmm.program_id,
                &request.accounts.payer,
                &snapshot.descriptor.address,
                &user_token_a,
                &clmm.token_vault_a,
                &user_token_b,
                &clmm.token_vault_b,
                &tick_array_0,
                &tick_array_1,
                &tick_array_2,
                &oracle,
                amount,
                other_amount_threshold,
                a_to_b,
            )?
        };

        debug!(
            "Orca local builder: pool={} a_to_b={} amount={} threshold={} tick_arrays=[{}, {}, {}]",
            snapshot.descriptor.address, a_to_b, amount, other_amount_threshold,
            tick_array_0, tick_array_1, tick_array_2
        );

        let slippage_bps = if request.max_slippage_bps > 0.0 {
            request.max_slippage_bps
        } else {
            DEFAULT_ORCA_SLIPPAGE_BPS
        };

        let description = format!(
            "Orca {} {} base {:.6} @ {:.2} bps slippage (min {} {:.6}) [local{}]",
            request.side.as_str(),
            snapshot.descriptor.label,
            request.base_amount,
            slippage_bps,
            if request.side == TradeSide::Sell { "out" } else { "in" },
            request.min_amount_out.unwrap_or(0.0),
            if use_swap_v2 { " v2" } else { "" }
        );

        Ok(TradePlan {
            instructions: vec![instruction],
            description,
        })
    }
}

/// Build Whirlpool swap instruction locally without SDK
/// 
/// Account order (from Whirlpool IDL):
/// 1. token_program - Token Program
/// 2. token_authority - Signer (user)
/// 3. whirlpool - Pool account (mut)
/// 4. token_owner_account_a - User's token A account (mut)
/// 5. token_vault_a - Pool's vault A (mut)
/// 6. token_owner_account_b - User's token B account (mut)
/// 7. token_vault_b - Pool's vault B (mut)
/// 8. tick_array_0 - Tick array (mut)
/// 9. tick_array_1 - Tick array (mut)
/// 10. tick_array_2 - Tick array (mut)
/// 11. oracle - Oracle PDA
#[allow(clippy::too_many_arguments)]
fn build_swap_instruction(
    program_id: &Pubkey,
    token_authority: &Pubkey,
    whirlpool: &Pubkey,
    token_owner_account_a: &Pubkey,
    token_vault_a: &Pubkey,
    token_owner_account_b: &Pubkey,
    token_vault_b: &Pubkey,
    tick_array_0: &Pubkey,
    tick_array_1: &Pubkey,
    tick_array_2: &Pubkey,
    oracle: &Pubkey,
    amount: u64,
    other_amount_threshold: u64,
    a_to_b: bool,
) -> Result<Instruction> {
    // Build instruction data
    // Layout: discriminator(8) + amount(8) + other_amount_threshold(8) + sqrt_price_limit(16) + amount_specified_is_input(1) + a_to_b(1)
    let mut data = Vec::with_capacity(42);
    data.extend_from_slice(&SWAP_DISCRIMINATOR);
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&other_amount_threshold.to_le_bytes());
    
    // sqrt_price_limit: use 0 for no limit (will use MIN or MAX based on direction)
    // For a_to_b (selling A): use MIN_SQRT_PRICE (price goes down)
    // For b_to_a (buying A): use MAX_SQRT_PRICE (price goes up)
    let sqrt_price_limit: u128 = if a_to_b {
        // MIN_SQRT_PRICE = 4295048016 (from Whirlpool constants)
        4295048016u128
    } else {
        // MAX_SQRT_PRICE = 79226673515401279992447579055 (from Whirlpool constants)
        79226673515401279992447579055u128
    };
    data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
    
    // amount_specified_is_input = true (we specify input amount)
    data.push(1u8);
    // a_to_b
    data.push(if a_to_b { 1u8 } else { 0u8 });

    // Build account metas
    let accounts = vec![
        AccountMeta::new_readonly(spl_token::ID, false),           // token_program
        AccountMeta::new_readonly(*token_authority, true),         // token_authority (signer)
        AccountMeta::new(*whirlpool, false),                       // whirlpool
        AccountMeta::new(*token_owner_account_a, false),           // token_owner_account_a
        AccountMeta::new(*token_vault_a, false),                   // token_vault_a
        AccountMeta::new(*token_owner_account_b, false),           // token_owner_account_b
        AccountMeta::new(*token_vault_b, false),                   // token_vault_b
        AccountMeta::new(*tick_array_0, false),                    // tick_array_0
        AccountMeta::new(*tick_array_1, false),                    // tick_array_1
        AccountMeta::new(*tick_array_2, false),                    // tick_array_2
        AccountMeta::new(*oracle, false),                          // oracle
    ];

    Ok(Instruction {
        program_id: *program_id,
        accounts,
        data,
    })
}

/// Build Whirlpool swap_v2 instruction locally (Token-2022 aware)
/// 
/// Account order (from Whirlpool IDL):
/// 1. token_program_a
/// 2. token_program_b
/// 3. memo_program
/// 4. token_authority - Signer (user)
/// 5. whirlpool - Pool account (mut)
/// 6. token_mint_a
/// 7. token_mint_b
/// 8. token_owner_account_a - User's token A account (mut)
/// 9. token_vault_a - Pool's vault A (mut)
/// 10. token_owner_account_b - User's token B account (mut)
/// 11. token_vault_b - Pool's vault B (mut)
/// 12. tick_array_0 - Tick array (mut)
/// 13. tick_array_1 - Tick array (mut)
/// 14. tick_array_2 - Tick array (mut)
/// 15. oracle - Oracle PDA (mut)
#[allow(clippy::too_many_arguments)]
fn build_swap_v2_instruction(
    program_id: &Pubkey,
    token_authority: &Pubkey,
    whirlpool: &Pubkey,
    token_mint_a: &Pubkey,
    token_mint_b: &Pubkey,
    token_owner_account_a: &Pubkey,
    token_vault_a: &Pubkey,
    token_owner_account_b: &Pubkey,
    token_vault_b: &Pubkey,
    token_program_a: &Pubkey,
    token_program_b: &Pubkey,
    tick_array_0: &Pubkey,
    tick_array_1: &Pubkey,
    tick_array_2: &Pubkey,
    oracle: &Pubkey,
    amount: u64,
    other_amount_threshold: u64,
    a_to_b: bool,
) -> Result<Instruction> {
    // Build instruction data (Anchor)
    let mut data = Vec::with_capacity(43);
    data.extend_from_slice(&SWAP_V2_DISCRIMINATOR);
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&other_amount_threshold.to_le_bytes());

    let sqrt_price_limit: u128 = if a_to_b {
        4295048016u128
    } else {
        79226673515401279992447579055u128
    };
    data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
    data.push(1u8); // amount_specified_is_input
    data.push(if a_to_b { 1u8 } else { 0u8 });
    data.push(0u8); // remaining_accounts_info = None

    let accounts = vec![
        AccountMeta::new_readonly(*token_program_a, false),        // token_program_a
        AccountMeta::new_readonly(*token_program_b, false),        // token_program_b
        AccountMeta::new_readonly(MEMO_PROGRAM_ID, false),         // memo_program
        AccountMeta::new_readonly(*token_authority, true),         // token_authority (signer)
        AccountMeta::new(*whirlpool, false),                       // whirlpool
        AccountMeta::new_readonly(*token_mint_a, false),           // token_mint_a
        AccountMeta::new_readonly(*token_mint_b, false),           // token_mint_b
        AccountMeta::new(*token_owner_account_a, false),           // token_owner_account_a
        AccountMeta::new(*token_vault_a, false),                   // token_vault_a
        AccountMeta::new(*token_owner_account_b, false),           // token_owner_account_b
        AccountMeta::new(*token_vault_b, false),                   // token_vault_b
        AccountMeta::new(*tick_array_0, false),                    // tick_array_0
        AccountMeta::new(*tick_array_1, false),                    // tick_array_1
        AccountMeta::new(*tick_array_2, false),                    // tick_array_2
        AccountMeta::new(*oracle, false),                          // oracle
    ];

    Ok(Instruction {
        program_id: *program_id,
        accounts,
        data,
    })
}

fn units_to_amount_raw(amount: f64, decimals: u8) -> Result<u64> {
    if !(amount.is_finite() && amount > 0.0) {
        bail!("amount must be positive and finite");
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

impl TradeSide {
    fn as_str(&self) -> &'static str {
        match self {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        }
    }
}

