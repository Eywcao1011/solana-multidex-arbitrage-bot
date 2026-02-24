use anyhow::{anyhow, bail, Result};
use log::{debug, warn};
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use super::{TradeExecutor, TradePlan, TradeRequest};
use crate::dex::{raydium, DexKind, PoolSnapshot, TradeSide};
use crate::rpc::AppContext;
use crate::sidecar_client::SidecarClient;
use once_cell::sync::Lazy;
use std::str::FromStr;

const DEFAULT_RAYDIUM_SLIPPAGE_BPS: f64 = 50.0;
const REQUIRED_TICK_ARRAYS: usize = 3;
const EXTRA_TICK_ARRAYS: usize = 2;

// ✅ 全局缓存：warmup 时存储的 tick_arrays，prepare_swap 时使用
// 类似 trade/orca.rs 的 ORCA_TICK_ARRAYS_CACHE 模式
static RAYDIUM_TICK_ARRAYS_CACHE: once_cell::sync::Lazy<RwLock<HashMap<Pubkey, Vec<Pubkey>>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(HashMap::new()));

/// 注入 Raydium pool 的 tick_arrays（用于 warmup 缓存）
pub fn inject_tick_arrays(pool: Pubkey, tick_arrays: Vec<Pubkey>) {
    if let Ok(mut cache) = RAYDIUM_TICK_ARRAYS_CACHE.write() {
        cache.insert(pool, tick_arrays);
    }
}

/// 获取缓存的 tick_arrays
pub fn get_cached_tick_arrays(pool: &Pubkey) -> Option<Vec<Pubkey>> {
    RAYDIUM_TICK_ARRAYS_CACHE.read().ok()?.get(pool).cloned()
}

/// 清空 warmup 缓存的 tick_arrays
pub fn clear_tick_arrays_cache() {
    if let Ok(mut cache) = RAYDIUM_TICK_ARRAYS_CACHE.write() {
        cache.clear();
    }
}

/// Refresh cached Raydium tick arrays using on-chain state (no blocking on trade path).
pub async fn refresh_pool_cached_tick_arrays_with_state(
    ctx: &AppContext,
    pool: &Pubkey,
    program_id: &Pubkey,
    tick_current: i32,
    tick_spacing: i32,
) -> Result<usize> {
    let addresses =
        raydium::compute_tick_array_subscriptions(program_id, pool, tick_current, tick_spacing);
    if addresses.is_empty() {
        return Ok(0);
    }

    let arrays = match ctx.get_raydium_tick_arrays_batch(&addresses).await {
        Ok(found) if !found.is_empty() => found.into_iter().map(|(addr, _)| addr).collect(),
        Ok(_) => addresses.clone(),
        Err(err) => {
            warn!(
                "Raydium tick_arrays refresh failed for pool {}: {}; using derived list",
                pool, err
            );
            addresses.clone()
        }
    };

    inject_tick_arrays(*pool, arrays.clone());
    Ok(arrays.len())
}

/// Refresh cached Raydium tick arrays using sidecar introspection.
pub async fn refresh_pool_cached_tick_arrays(ctx: &AppContext, pool: &Pubkey) -> Result<usize> {
    static REFRESH_SIDECAR: Lazy<Result<SidecarClient, anyhow::Error>> =
        Lazy::new(|| SidecarClient::from_env());

    let sidecar = REFRESH_SIDECAR
        .as_ref()
        .map_err(|e| anyhow!("SidecarClient unavailable for Raydium refresh: {}", e))?;

    let pool_str = pool.to_string();
    let resp = sidecar.introspect_raydium(&pool_str).await?;
    if !resp.ok {
        return Err(anyhow!(
            "raydium introspect returned ok=false: {:?}",
            resp.error
        ));
    }

    let program_id = resp
        .program_id
        .as_deref()
        .ok_or_else(|| anyhow!("raydium introspect missing program_id"))?;
    let tick_current = resp
        .tick_current_index
        .ok_or_else(|| anyhow!("raydium introspect missing tick_current_index"))?;
    let tick_spacing = resp
        .tick_spacing
        .ok_or_else(|| anyhow!("raydium introspect missing tick_spacing"))?;

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

/// Raydium CLMM swap_v2 instruction discriminator
/// sha256("global:swap_v2")[0..8]
const SWAP_V2_DISCRIMINATOR: [u8; 8] = [0x2b, 0x04, 0xed, 0x0b, 0x1a, 0xc9, 0x1e, 0x62];

#[derive(Default)]
pub struct RaydiumExecutor;

impl TradeExecutor for RaydiumExecutor {
    fn dex(&self) -> DexKind {
        DexKind::RaydiumClmm
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        // Basic validation
        // For exact-in Buy mode (quote_amount_in set), base_amount can be 0
        let is_exact_in_buy = request.quote_amount_in.is_some() && request.side == TradeSide::Buy;
        if !is_exact_in_buy && request.base_amount <= 0.0 {
            bail!("base_amount must be positive, got {}", request.base_amount);
        }

        if request.min_amount_out.is_none() && !is_exact_in_buy {
            bail!("min_amount_out (protection) required for Raydium execution");
        }

        if request.accounts.payer == Pubkey::default() {
            bail!("payer account must be set");
        }

        // Get CLMM accounts from snapshot (populated by decode)
        let clmm = snapshot
            .clmm_accounts
            .as_ref()
            .ok_or_else(|| anyhow!("missing clmm_accounts in Raydium snapshot"))?;

        // Determine swap direction based on normalized pair
        // In normalized view: base is the "main" token, quote is SOL/USDC
        // For Raydium: token_0 and token_1 are the raw pool tokens
        // zero_for_one = true means swap token_0 -> token_1
        let inverted = snapshot.normalized_pair.inverted;
        let (norm_base_decimals, norm_quote_decimals) = if inverted {
            (snapshot.quote_decimals, snapshot.base_decimals)
        } else {
            (snapshot.base_decimals, snapshot.quote_decimals)
        };
        let is_base_input = request.side == TradeSide::Sell;
        let zero_for_one = is_base_input ^ inverted;

        let (amount, other_amount_threshold) = match request.side {
            TradeSide::Sell => {
                // Selling base for quote
                let amount_in = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                let min_out =
                    units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;
                (amount_in, min_out)
            }
            TradeSide::Buy => {
                // Buying base with quote
                let (amount, threshold) = if let Some(quote_in) = request.quote_amount_in {
                    // ✅ Spot-only exact-in mode: fixed quote input, min base output
                    let amount_in = units_to_amount_raw(quote_in, norm_quote_decimals)?;
                    let min_base = request.min_amount_out.unwrap_or(0.0);
                    let min_out =
                        units_to_amount_raw(min_base.max(0.000001), norm_base_decimals)?;
                    (amount_in, min_out)
                } else {
                    // ✅ Full mode: min_amount_out is max_quote_in, base_amount is expected output
                    let amount_out = units_to_amount_raw(request.base_amount, norm_base_decimals)?;
                    let max_in =
                        units_to_amount_raw(request.min_amount_out.unwrap(), norm_quote_decimals)?;
                    (amount_out, max_in)
                };
                (amount, threshold)
            }
        };

        // ✅ Map normalized accounts to token_0/token_1
        // user_base_account = ATA(normalized_base), user_quote_account = ATA(normalized_quote)
        // When inverted=false: normalized_base=token_0, normalized_quote=token_1
        // When inverted=true: normalized_base=token_1, normalized_quote=token_0
        let (user_token_0, user_token_1) = if inverted {
            // inverted: normalized_base=token_1, normalized_quote=token_0
            (request.accounts.user_quote_account, request.accounts.user_base_account)
        } else {
            // not inverted: normalized_base=token_0, normalized_quote=token_1
            (request.accounts.user_base_account, request.accounts.user_quote_account)
        };

        // Get user token accounts based on swap direction
        let (input_token_account, output_token_account) = if zero_for_one {
            // token_0 -> token_1
            (user_token_0, user_token_1)
        } else {
            // token_1 -> token_0
            (user_token_1, user_token_0)
        };

        let cached_tick_arrays = get_cached_tick_arrays(&snapshot.descriptor.address);
        let tick_arrays = compute_sdk_tick_arrays(
            cached_tick_arrays.as_deref(),
            clmm.tick_current_index,
            clmm.tick_spacing,
            zero_for_one,
            &clmm.program_id,
            &snapshot.descriptor.address,
        );
        if tick_arrays.len() < 3 {
            bail!(
                "Raydium {} tick_arrays_cache_miss (have {}, need {})",
                snapshot.descriptor.label,
                tick_arrays.len(),
                REQUIRED_TICK_ARRAYS
            );
        }

        // We need exactly 3 tick arrays for the swap instruction
        let tick_array_0 = tick_arrays.get(0).copied().unwrap_or(snapshot.descriptor.address);
        let tick_array_1 = tick_arrays.get(1).copied().unwrap_or(tick_array_0);
        let tick_array_2 = tick_arrays.get(2).copied().unwrap_or(tick_array_1);

        // Use pool-provided observation key when available (fallback to PDA)
        let observation_state = if clmm.observation_key != Pubkey::default() {
            clmm.observation_key
        } else {
            Pubkey::find_program_address(
                &[b"observation", snapshot.descriptor.address.as_ref()],
                &clmm.program_id,
            )
            .0
        };

        // Build the swap instruction
        let (input_vault_mint, output_vault_mint) = if zero_for_one {
            (clmm.token_mint_a, clmm.token_mint_b)
        } else {
            (clmm.token_mint_b, clmm.token_mint_a)
        };

        let ex_tick_array_bitmap =
            raydium_ex_tick_array_bitmap(&clmm.program_id, &snapshot.descriptor.address);
        let instruction = build_swap_instruction(
            &clmm.program_id,
            &request.accounts.payer,
            &clmm.amm_config,
            &snapshot.descriptor.address,
            &clmm.token_vault_a,
            &clmm.token_vault_b,
            &observation_state,
            &input_token_account,
            &output_token_account,
            &input_vault_mint,
            &output_vault_mint,
            Some(&ex_tick_array_bitmap),
            &tick_array_0,
            &tick_array_1,
            &tick_array_2,
            amount,
            other_amount_threshold,
            zero_for_one,
            true, // is_base_input (amount_specified_is_input)
        )?;

        debug!(
            "Raydium CLMM local builder: pool={} zero_for_one={} amount={} threshold={} tick_arrays=[{}, {}, {}]",
            snapshot.descriptor.address, zero_for_one, amount, other_amount_threshold,
            tick_array_0, tick_array_1, tick_array_2
        );

        let slippage_bps = if request.max_slippage_bps > 0.0 {
            request.max_slippage_bps
        } else {
            DEFAULT_RAYDIUM_SLIPPAGE_BPS
        };

        let side_str = match request.side {
            TradeSide::Buy => "BUY",
            TradeSide::Sell => "SELL",
        };

        let description = format!(
            "Raydium CLMM {} {} base {:.6} @ {:.2} bps slippage (min {} {:.6}) [local]",
            side_str,
            snapshot.descriptor.label,
            request.base_amount,
            slippage_bps,
            if request.side == TradeSide::Sell { "out" } else { "in" },
            request.min_amount_out.unwrap_or(0.0)
        );

        Ok(TradePlan {
            instructions: vec![instruction],
            description,
        })
    }
}

/// Build Raydium CLMM swap instruction locally without SDK
/// 
/// Account order (from Raydium CLMM IDL):
/// 1. payer - Signer
/// 2. amm_config - AMM config account (derived)
/// 3. pool_state - Pool account (mut)
/// 4. input_token_account - User's input token account (mut)
/// 5. output_token_account - User's output token account (mut)
/// 6. input_vault - Pool's input vault (mut)
/// 7. output_vault - Pool's output vault (mut)
/// 8. observation_state - Observation state PDA (mut)
/// 9. token_program - Token Program
/// 10. tick_array_0 - Tick array (mut)
/// 11. tick_array_1 - Tick array (mut)
/// 12. tick_array_2 - Tick array (mut)
/// 13. token_program_2022 - Token-2022 Program (if any token is Token-2022)
#[allow(clippy::too_many_arguments)]
fn build_swap_instruction(
    program_id: &Pubkey,
    payer: &Pubkey,
    amm_config: &Pubkey,
    pool_state: &Pubkey,
    token_vault_0: &Pubkey,
    token_vault_1: &Pubkey,
    observation_state: &Pubkey,
    input_token_account: &Pubkey,
    output_token_account: &Pubkey,
    input_vault_mint: &Pubkey,
    output_vault_mint: &Pubkey,
    ex_tick_array_bitmap: Option<&Pubkey>,
    tick_array_0: &Pubkey,
    tick_array_1: &Pubkey,
    tick_array_2: &Pubkey,
    amount: u64,
    other_amount_threshold: u64,
    zero_for_one: bool,
    is_base_input: bool,
) -> Result<Instruction> {
    // Determine input/output vaults based on direction
    let (input_vault, output_vault) = if zero_for_one {
        (token_vault_0, token_vault_1)
    } else {
        (token_vault_1, token_vault_0)
    };

    // Build instruction data
    // Layout: discriminator(8) + amount(8) + other_amount_threshold(8) + sqrt_price_limit_x64(16) + is_base_input(1)
    let mut data = Vec::with_capacity(41);
    data.extend_from_slice(&SWAP_V2_DISCRIMINATOR);
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&other_amount_threshold.to_le_bytes());
    
    // sqrt_price_limit_x64: Raydium SDK uses MIN+1 / MAX-1 to avoid overflow
    // For zero_for_one (price goes down): use MIN_SQRT_PRICE_X64 + 1
    // For one_for_zero (price goes up): use MAX_SQRT_PRICE_X64 - 1
    const MIN_SQRT_PRICE_X64: u128 = 4_295_048_016;
    const MAX_SQRT_PRICE_X64: u128 = 79_226_673_521_066_979_257_578_248_091;
    let sqrt_price_limit: u128 = if zero_for_one {
        MIN_SQRT_PRICE_X64 + 1
    } else {
        MAX_SQRT_PRICE_X64 - 1
    };
    data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
    
    // is_base_input
    data.push(if is_base_input { 1u8 } else { 0u8 });

    // Token-2022 + Memo program IDs (Raydium CLMM swap_v2 accounts)
    const TOKEN_2022_PROGRAM_ID: Pubkey =
        solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb");
    const MEMO_PROGRAM_ID: Pubkey =
        solana_pubkey::pubkey!("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr");

    // Build account metas
    // Note: Raydium CLMM uses a different account order than Orca
    let mut accounts = vec![
        AccountMeta::new_readonly(*payer, true),                    // payer (signer)
        AccountMeta::new_readonly(*amm_config, false),              // amm_config
        AccountMeta::new(*pool_state, false),                       // pool_state
        AccountMeta::new(*input_token_account, false),              // input_token_account
        AccountMeta::new(*output_token_account, false),             // output_token_account
        AccountMeta::new(*input_vault, false),                      // input_vault
        AccountMeta::new(*output_vault, false),                     // output_vault
        AccountMeta::new(*observation_state, false),                // observation_state
        AccountMeta::new_readonly(spl_token::ID, false),            // token_program
        AccountMeta::new_readonly(TOKEN_2022_PROGRAM_ID, false),    // token_program_2022
        AccountMeta::new_readonly(MEMO_PROGRAM_ID, false),          // memo_program
        AccountMeta::new_readonly(*input_vault_mint, false),        // input_vault_mint
        AccountMeta::new_readonly(*output_vault_mint, false),       // output_vault_mint
    ];
    if let Some(ex_bitmap) = ex_tick_array_bitmap {
        accounts.push(AccountMeta::new(*ex_bitmap, false));         // ex_tick_array_bitmap
    }
    accounts.push(AccountMeta::new(*tick_array_0, false));          // tick_array_0
    accounts.push(AccountMeta::new(*tick_array_1, false));          // tick_array_1
    accounts.push(AccountMeta::new(*tick_array_2, false));          // tick_array_2

    Ok(Instruction {
        program_id: *program_id,
        accounts,
        data,
    })
}

fn raydium_ex_tick_array_bitmap(program_id: &Pubkey, pool: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"pool_tick_array_bitmap_extension", pool.as_ref()],
        program_id,
    )
    .0
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

fn derive_required_tick_arrays(
    program_id: &Pubkey,
    pool: &Pubkey,
    tick_current: i32,
    tick_spacing: i32,
    zero_for_one: bool,
) -> Vec<Pubkey> {
    const RAYDIUM_TICK_ARRAY_SIZE: i32 = 60;
    let array_span = tick_spacing * RAYDIUM_TICK_ARRAY_SIZE;
    if array_span == 0 {
        return Vec::new();
    }

    // ✅ 与 SDK 保持一致：zero_for_one=false 时需要添加 tick_spacing 偏移
    let shift = if zero_for_one { 0 } else { tick_spacing };
    let start_index =
        crate::dex::raydium::tick_array_start_index(tick_current + shift, tick_spacing);

    let total_needed = REQUIRED_TICK_ARRAYS + EXTRA_TICK_ARRAYS;
    let mut out = Vec::with_capacity(total_needed);

    for step in 0..total_needed {
        let offset = if zero_for_one {
            -(step as i32)
        } else {
            step as i32
        };
        let idx = start_index + (offset * array_span);
        if let Ok(addr) = raydium::tick_array_address(program_id, pool, idx) {
            out.push(addr);
        }
    }

    out
}

fn compute_sdk_tick_arrays(
    cached: Option<&[Pubkey]>,
    tick_current: i32,
    tick_spacing: i32,
    zero_for_one: bool,
    program_id: &Pubkey,
    pool_address: &Pubkey,
) -> Vec<Pubkey> {
    let required =
        derive_required_tick_arrays(program_id, pool_address, tick_current, tick_spacing, zero_for_one);
    if required.len() < REQUIRED_TICK_ARRAYS {
        return Vec::new();
    }

    if let Some(cached) = cached.filter(|v| !v.is_empty()) {
        let cache_set: HashSet<_> = cached.iter().copied().collect();
        let missing = required.iter().filter(|addr| !cache_set.contains(addr)).count();
        if missing > 0 {
            warn!(
                "Raydium tick_arrays cache miss: pool={} missing={}/{} (tick_current={}, spacing={}, zero_for_one={})",
                pool_address,
                missing,
                required.len(),
                tick_current,
                tick_spacing,
                zero_for_one
            );
            return Vec::new();
        }
    }

    required.into_iter().take(REQUIRED_TICK_ARRAYS).collect()
}

