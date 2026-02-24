use anyhow::{anyhow, Result};
use chrono::Utc;
use log::{debug, warn};
use once_cell::sync::Lazy;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::{
    meteora_lb::{self, LbAccounts, LbState},
    normalize_pair, AccountReader, MeteoraDynamicFeeParams, PoolDescriptor, PoolFees, PoolSnapshot, TradeQuote, TradeSide,
};
use crate::{constants::METEORA_LB_PROGRAM_ID, rpc::AppContext, sidecar_client::SidecarClient};

/// Throttle interval for boundary crossing warnings (per pool)
const BOUNDARY_WARN_INTERVAL: Duration = Duration::from_secs(60);

/// Last warning time for each pool (for log throttling)
static BOUNDARY_WARN_TIMES: Lazy<Mutex<HashMap<Pubkey, Instant>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// Meteora DLMM decoder
///
/// DLMM (Dynamic Liquidity Market Maker) uses the same program as LB (Liquidity Book),
/// but we implement a dedicated decoder with additional validation to handle:
/// - Potential account layout differences between versions
/// - Invalid/non-existent dependent accounts
/// - Better error reporting for debugging
pub async fn decode(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    if data.len() < 8 {
        anyhow::bail!("DLMM pair account too short");
    }

    // Parse discriminator to verify account type (kept for potential future validation)
    let _discriminator = &data[0..8];
    let mut reader = AccountReader::new(&data[8..]);

    // Parse fee parameters
    let base_factor = reader.read_u16()?;
    reader.read_u16()?; // filter_period
    reader.read_u16()?; // decay_period
    reader.read_u16()?; // reduction_factor
    let variable_fee_control = reader.read_u32()?;
    reader.read_u32()?; // max_volatility_accumulator
    reader.read_i32()?; // min_bin_id
    reader.read_i32()?; // max_bin_id
    let mut protocol_share = reader.read_u16()?;
    // Sanity check: protocol_share is in basis points (0-10000).
    // If we read a huge value, it's likely garbage data from a layout mismatch.
    if protocol_share > 10000 {
        warn!(
            "Parsed Meteora DLMM protocol_share is absurdly high ({}), defaulting to 0",
            protocol_share
        );
        protocol_share = 0;
    }
    let base_fee_power_factor = reader.read_u8()?;
    reader.skip(5)?; // padding

    // Parse volatility state (needed for dynamic fee calculation)
    let volatility_accumulator = reader.read_u32()?;
    let volatility_reference = reader.read_u32()?;
    reader.read_i32()?; // index_reference
    reader.skip(4)?; // padding
    reader.read_i64()?; // last_update_timestamp
    reader.skip(8)?; // padding

    // Parse pool configuration
    reader.skip(1)?; // bump_seed
    reader.skip(2)?; // bin_step_seed
    reader.read_u8()?; // pair_type
    let active_id = reader.read_i32()?;
    let bin_step = reader.read_u16()?;
    reader.read_u8()?; // status
    reader.read_u8()?; // require_base_factor_seed
    reader.skip(2)?; // base_factor_seed
    reader.read_u8()?; // activation_type
    reader.read_u8()?; // creator_on_off_control (DLMM-specific field)

    // Parse critical account addresses
    let token_x_mint = reader.read_pubkey()?;
    let token_y_mint = reader.read_pubkey()?;
    let reserve_x = reader.read_pubkey()?;
    let reserve_y = reader.read_pubkey()?;

    // Debug: 输出读取到的地址
    debug!(
        "DLMM pool {} parsed addresses: token_x={}, token_y={}, reserve_x={}, reserve_y={}",
        descriptor.address, token_x_mint, token_y_mint, reserve_x, reserve_y
    );

    // Validate that mints and reserves are non-zero
    if token_x_mint == Pubkey::default() || token_y_mint == Pubkey::default() {
        anyhow::bail!("DLMM pool has invalid token mints");
    }
    if reserve_x == Pubkey::default() || reserve_y == Pubkey::default() {
        anyhow::bail!("DLMM pool has invalid reserve accounts");
    }

    reader.read_u64()?; // protocol_fee_amount_x
    reader.read_u64()?; // protocol_fee_amount_y
    reader.skip(32)?; // padding_1
    reader.skip(144 * 2)?; // reward_infos (2 reward slots)

    let oracle = reader.read_pubkey()?;
    if oracle == Pubkey::default() {
        anyhow::bail!("DLMM pool has invalid oracle address");
    }

    // Parse inline bitmap
    let mut inline_bitmap = [0u64; 16];
    for word in inline_bitmap.iter_mut() {
        *word = reader.read_u64()?;
    }

    reader.read_i64()?; // last_updated_at
    reader.skip(32)?; // padding_2
    reader.skip(32)?; // pre_activation_swap_address
    reader.skip(32)?; // base_key
    reader.read_u64()?; // activation_point
    reader.read_u64()?; // pre_activation_duration
    reader.skip(8)?; // padding_3
    reader.read_u64()?; // padding_4
    reader.skip(32)?; // creator
    reader.read_u8()?; // token_mint_x_program_flag
    reader.read_u8()?; // token_mint_y_program_flag
    reader.skip(22)?; // reserved

    // Fetch mint info and reserve balances with slot binding
    let slot = Some(context.slot);
    let (mint_x_info, mint_y_info, reserve_x_amount, reserve_y_amount) = if ctx.spot_only_mode() {
        let (mint_x_info, mint_y_info) = tokio::try_join!(
            ctx.get_mint_info(&token_x_mint),
            ctx.get_mint_info(&token_y_mint),
        )?;
        (mint_x_info, mint_y_info, 0u64, 0u64)
    } else {
        tokio::try_join!(
            ctx.get_mint_info_with_slot(&token_x_mint, slot),
            ctx.get_mint_info_with_slot(&token_y_mint, slot),
            ctx.get_token_account_amount_with_slot(&reserve_x, slot),
            ctx.get_token_account_amount_with_slot(&reserve_y, slot),
        )?
    };

    // Calculate price from bin step and active id
    let step = 1.0 + (bin_step as f64) / 10_000.0;
    if step <= 0.0 {
        return Err(anyhow!("invalid bin step"));
    }
    let price_raw = step.powi(active_id);
    if !price_raw.is_finite() || price_raw <= 0.0 {
        return Err(anyhow!("invalid DLMM price calculation"));
    }
    let decimals_adjustment = mint_x_info.decimals as i32 - mint_y_info.decimals as i32;
    let price = price_raw * 10f64.powi(decimals_adjustment);

    let (normalized_pair, normalized_price) = normalize_pair(token_x_mint, token_y_mint, price)
        .ok_or_else(|| anyhow!("cannot normalize DLMM pair"))?;

    // Calculate fees
    let total_fee_ratio = compute_base_fee(bin_step, base_factor, base_fee_power_factor);
    let protocol_fee_ratio = total_fee_ratio * (protocol_share as f64 / 10_000.0);
    let lp_fee_ratio = (total_fee_ratio - protocol_fee_ratio).max(0.0);

    // 计算 dynamic fee ratio
    let dyn_params = MeteoraDynamicFeeParams {
        bin_step,
        base_factor,
        variable_fee_control,
        volatility_accumulator,
        volatility_reference,
    };
    let var_fee_ratio = dyn_params.variable_fee_ratio();
    let total_with_dyn = total_fee_ratio + var_fee_ratio;

    // DEBUG: 打印 fee 参数，确认计算是否正确
    debug!(
        "DLMM pool {} fee params: bin_step={}, base_factor={}, var_fee_ctrl={}, vol_acc={}, base_fee={:.4}%, var_fee={:.4}%, total={:.4}%",
        descriptor.label, bin_step, base_factor, variable_fee_control, volatility_accumulator, 
        total_fee_ratio * 100.0, var_fee_ratio * 100.0, total_with_dyn * 100.0
    );

    debug!(
        "DLMM pool {} active_id={} bin_step={} before build_lb_state",
        descriptor.address, active_id, bin_step
    );

    // Build liquidity state
    // ✅ Spot-only 模式下：如果有 SDK 缓存的 bin_arrays，仍然构建 lb_state 用于本地生成 swap 指令
    //    只是 dependent_accounts 保持为空，不订阅 bin arrays 的 WS
    // ✅ 优先使用 SDK 返回的 bin_arrays（如果有缓存）
    let sdk_bin_arrays = ctx.get_dlmm_bin_arrays(&descriptor.address);
    
    let lb_state = if let Some(ref bin_arrays) = sdk_bin_arrays {
        // 优先使用 SDK 返回的 bin_arrays 构建 lb_state
        if !bin_arrays.is_empty() {
            if ctx.spot_only_mode() {
                // ✅ spot_only 模式：使用轻量级构建，不走 RPC
                let state = build_lb_state_indexes_only(
                    &descriptor.address,
                    active_id,
                    bin_step,
                    bin_arrays,
                );
                if let Some(ref s) = state {
                    debug!(
                        "DLMM pool {} built lb_state indexes_only (no RPC): {} arrays",
                        descriptor.address, s.bin_array_indexes.len()
                    );
                }
                state
            } else {
                // full 模式：走 RPC 拉取完整 bin 数据
                match build_lb_state_from_addresses(
                    ctx,
                    &descriptor.address,
                    active_id,
                    bin_step,
                    bin_arrays,
                    context.slot,
                )
                .await
                {
                    Ok(Some(state)) => {
                        debug!(
                            "DLMM pool {} built lb_state from SDK bin_arrays: {} bins, {} arrays",
                            descriptor.address, state.bins.len(), state.bin_array_indexes.len()
                        );
                        Some(state)
                    }
                    Ok(None) => {
                        debug!(
                            "DLMM pool {} SDK bin_arrays returned no valid lb_state, trying heuristic scan",
                            descriptor.address
                        );
                        // Fallback to heuristic scan
                        build_lb_state_safe(ctx, &descriptor.address, active_id, bin_step, &inline_bitmap, context.slot)
                            .await
                            .ok()
                            .flatten()
                    }
                    Err(err) => {
                        warn!(
                            "DLMM pool {} failed to build lb_state from SDK bin_arrays: {}, trying heuristic scan",
                            descriptor.address, err
                        );
                        build_lb_state_safe(ctx, &descriptor.address, active_id, bin_step, &inline_bitmap, context.slot)
                            .await
                            .ok()
                            .flatten()
                    }
                }
            }
        } else {
            // SDK 返回空数组 - 需要走 RPC 扫描获取 lb_state
            log::debug!("DLMM {}: empty SDK cache", descriptor.label);
            if ctx.spot_only_mode() {
                // ⭐ spot_only 模式下没有 SDK 数据则无法安全构建 swap 指令
                // 返回 None，该池子将无法进行交易
                log::warn!(
                    "DLMM {}: spot_only mode but no SDK bin_arrays cached, skipping lb_state build",
                    descriptor.label
                );
                None
            } else {
                build_lb_state_safe(ctx, &descriptor.address, active_id, bin_step, &inline_bitmap, context.slot)
                    .await
                    .ok()
                    .flatten()
            }
        }
    } else {
        // 没有 SDK 缓存 - 需要走 RPC 扫描获取 lb_state
        log::debug!("DLMM {}: no SDK cache", descriptor.label);
        if ctx.spot_only_mode() {
            // ⭐ spot_only 模式下没有 SDK 数据则无法安全构建 swap 指令
            log::warn!(
                "DLMM {}: spot_only mode but no SDK bin_arrays cached, skipping lb_state build",
                descriptor.label
            );
            None
        } else {
            build_lb_state_safe(ctx, &descriptor.address, active_id, bin_step, &inline_bitmap, context.slot)
                .await
                .ok()
                .flatten()
        }
    };

    // Collect dependent accounts for WebSocket subscriptions
    // ✅ Spot-only 模式：只订阅主池账户，不订阅任何依赖账户（价格从主池 active_id 算）
    let dependent_accounts = if ctx.spot_only_mode() {
        vec![]
    } else {
        let mut deps = vec![reserve_x, reserve_y, oracle];

        // Add bitmap extension (always derived, may not exist yet but that's ok)
        let bitmap_ext_addr =
            meteora_lb::bitmap_extension_address(&METEORA_LB_PROGRAM_ID, &descriptor.address);
        deps.push(bitmap_ext_addr);

        // ✅ 优先使用 SDK 返回的 bin_arrays 地址（如果有）
        if let Some(ref bin_arrays) = sdk_bin_arrays {
            deps.extend(bin_arrays.iter().cloned());
        } else {
            // 没有 SDK 缓存时，订阅核心 ±1 bin arrays
            let core_indexes = compute_core_bin_array_indexes(active_id);
            let core_addrs =
                bin_array_indexes_to_addresses(&METEORA_LB_PROGRAM_ID, &descriptor.address, &core_indexes);
            deps.extend(core_addrs);
        }

        // Add bin array addresses from LB state if available (may have more than SDK returned)
        if let Some(ref state) = lb_state {
            for &index in &state.bin_array_indexes {
                let bin_array_addr =
                    meteora_lb::bin_array_address(&METEORA_LB_PROGRAM_ID, &descriptor.address, index);
                if !deps.contains(&bin_array_addr) {
                    deps.push(bin_array_addr);
                }
            }
        }
        deps
    };

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: token_x_mint,
        quote_mint: token_y_mint,
        base_decimals: mint_x_info.decimals,
        quote_decimals: mint_y_info.decimals,
        price,
        sqrt_price_x64: 0,
        liquidity: None,
        base_reserve: if ctx.spot_only_mode() {
            None
        } else {
            Some(reserve_x_amount as u128)
        },
        quote_reserve: if ctx.spot_only_mode() {
            None
        } else {
            Some(reserve_y_amount as u128)
        },
        fees: Some(PoolFees {
            lp_fee_bps: lp_fee_ratio * 10_000.0,
            protocol_fee_bps: protocol_fee_ratio * 10_000.0,
            other_fee_bps: 0.0,
            meteora_dynamic: Some(MeteoraDynamicFeeParams {
                bin_step,
                base_factor,
                variable_fee_control,
                volatility_accumulator,
                volatility_reference,
            }),
        }),
        slot: context.slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state: None,
        clmm_accounts: None,
        damm_state: None,
        damm_accounts: None,
        lb_state,
        lb_accounts: Some(LbAccounts {
            reserve_x,
            reserve_y,
            oracle,
        }),
        pump_state: None,
        dependent_accounts,
    })
}

fn compute_base_fee(bin_step: u16, base_factor: u16, _base_fee_power_factor: u8) -> f64 {
    if bin_step == 0 || base_factor == 0 {
        return 0.0;
    }

    // Meteora DLMM base fee formula:
    // base_fee_rate = (bin_step * base_factor) / 10_000_000_000
    // This gives the fee as a ratio (e.g., 0.001 = 0.1%)
    //
    // For example:
    //   bin_step = 100 (1% price step)
    //   base_factor = 10000
    //   base_fee = (100 * 10000) / 10_000_000_000 = 0.0001 = 0.01%
    //
    // More typical values:
    //   bin_step = 100, base_factor = 100000 → 0.001 = 0.1%
    //   bin_step = 80, base_factor = 100000 → 0.0008 = 0.08%
    let base = (bin_step as f64) * (base_factor as f64) / 10_000_000_000.0;
    
    if !base.is_finite() || base <= 0.0 {
        0.0
    } else {
        // Cap at 10% max fee (sanity check)
        base.min(0.10)
    }
}

/// Build lightweight LB state from bin array addresses (no RPC, for spot_only mode)
/// Only populates bin_array_indexes needed for swap instruction building, bins is empty
pub fn build_lb_state_indexes_only(
    pool: &Pubkey,
    active_id: i32,
    bin_step: u16,
    bin_array_addresses: &[Pubkey],
) -> Option<LbState> {
    if bin_step == 0 || bin_array_addresses.is_empty() {
        return None;
    }

    let step_factor = 1.0 + (bin_step as f64) / 10_000.0;
    if !(step_factor.is_finite() && step_factor > 0.0) {
        return None;
    }

    let active_price = step_factor.powi(active_id);
    if !active_price.is_finite() || active_price <= 0.0 {
        return None;
    }

    // Extract bin array indexes from addresses (reverse PDA derivation)
    // bin_array_address = PDA([b"bin_array", pool, index.to_le_bytes()])
    // We can't reverse PDA, so we compute indexes from the expected range around active_id
    let active_index = bin_array_index_for_dlmm(active_id);
    
    // Use the number of provided addresses to determine the range
    // Typically sidecar returns arrays around the active price
    let num_arrays = bin_array_addresses.len() as i64;
    let half = num_arrays / 2;
    
    let mut bin_array_indexes: Vec<i64> = Vec::new();
    for i in 0..num_arrays {
        // Assume indexes centered around active_index
        let offset = i as i64 - half;
        bin_array_indexes.push(active_index + offset);
    }
    bin_array_indexes.sort_unstable();
    bin_array_indexes.dedup();

    log::debug!(
        "DLMM build_lb_state_indexes_only: pool={} active_id={} built {} indexes (no RPC)",
        pool, active_id, bin_array_indexes.len()
    );

    Some(LbState {
        _active_id: active_id,
        active_price,
        _bin_step: bin_step,
        _inline_bitmap: [0u64; 16],
        _extension_bitmap: None,
        bin_array_indexes,
        bins: vec![], // Empty - not needed for swap instruction building
    })
}

/// Build LB state from a list of known bin array addresses (from SDK introspection)
/// This is useful when the SDK knows about bin arrays that our heuristic scan doesn't find
pub async fn build_lb_state_from_addresses(
    ctx: &AppContext,
    pool: &Pubkey,
    active_id: i32,
    bin_step: u16,
    bin_array_addresses: &[Pubkey],
    _current_slot: u64,
) -> Result<Option<LbState>> {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    if bin_step == 0 || bin_array_addresses.is_empty() {
        return Ok(None);
    }

    let step_factor = 1.0 + (bin_step as f64) / 10_000.0;
    if !(step_factor.is_finite() && step_factor > 0.0) {
        return Ok(None);
    }

    let active_price = step_factor.powi(active_id);
    if !active_price.is_finite() || active_price <= 0.0 {
        return Ok(None);
    }

    let mut arrays: Vec<Arc<super::meteora_lb::BinArrayState>> = Vec::new();
    
    // Fetch each bin array by address
    for addr in bin_array_addresses {
        match ctx.rpc_client().get_account_data(addr).await {
            Ok(data) if !data.is_empty() => {
                match meteora_lb::decode_bin_array(&data) {
                    Ok(array) => {
                        if array.lb_pair == *pool {
                            log::debug!("DLMM: Fetched bin array {} (idx={})", addr, array._index);
                            arrays.push(Arc::new(array));
                        }
                    }
                    Err(e) => {
                        log::debug!("DLMM: Failed to decode bin array {}: {}", addr, e);
                    }
                }
            }
            _ => {
                log::debug!("DLMM: Bin array {} not found or empty", addr);
            }
        }
    }

    if arrays.is_empty() {
        return Ok(None);
    }

    // Build LbState from the fetched arrays
    let mut bin_array_indexes: Vec<i64> = arrays.iter().map(|a| a._index).collect();
    bin_array_indexes.sort_unstable();
    bin_array_indexes.dedup();

    let mut bin_map: BTreeMap<i32, super::meteora_lb::LbBin> = BTreeMap::new();
    for array in &arrays {
        for (i, bin) in array.bins.iter().enumerate() {
            let bin_id = array._index as i32 * 70 + i as i32; // BIN_ARRAY_SPAN = 70
            let price = step_factor.powi(bin_id);
            if price.is_finite() && price > 0.0 {
                bin_map.insert(bin_id, super::meteora_lb::LbBin {
                    _bin_id: bin_id,
                    price,
                    amount_x: bin.amount_x as f64,
                    amount_y: bin.amount_y as f64,
                });
            }
        }
    }

    let bins: Vec<super::meteora_lb::LbBin> = bin_map.into_values().collect();
    
    log::debug!(
        "DLMM build_lb_state_from_addresses: pool={} active_id={} built {} bins from {} arrays",
        pool, active_id, bins.len(), arrays.len()
    );

    Ok(Some(LbState {
        _active_id: active_id,
        active_price,
        _bin_step: bin_step,
        _inline_bitmap: [0u64; 16], // Not available from SDK introspect
        _extension_bitmap: None,
        bin_array_indexes,
        bins,
    }))
}

/// Build LB state with better error handling
/// Returns None if state can't be built, but doesn't fail the entire decode
async fn build_lb_state_safe(
    ctx: &AppContext,
    pool: &Pubkey,
    active_id: i32,
    bin_step: u16,
    inline_bitmap: &[u64; 16],
    current_slot: u64,
) -> Result<Option<LbState>> {
    if bin_step == 0 {
        return Ok(None);
    }

    let step_factor = 1.0 + (bin_step as f64) / 10_000.0;
    if !(step_factor.is_finite() && step_factor > 0.0) {
        return Ok(None);
    }

    let active_price = step_factor.powi(active_id);
    if !active_price.is_finite() || active_price <= 0.0 {
        return Ok(None);
    }

    // 收集 bin array 索引：inline bitmap + extension（如有）+ 核心 active ±1
    let _extension = match ctx.get_meteora_bitmap_extension(pool, current_slot).await {
        Ok(ext) => ext,
        Err(_) => None, // 扩展可能不存在或尚未创建，忽略错误
    };

    use std::collections::{BTreeMap, BTreeSet, HashSet};
    use std::sync::Arc;

    // Fetch bitmap extension and bin arrays using existing LB logic
    const BIN_ARRAY_INDEX_STRIDE: i64 = 1;
    const BIN_ARRAY_SPAN: i32 = 70;  // 修正为正确的span
    const EPSILON: f64 = 1e-12;

    let active_index = bin_array_index_for_dlmm(active_id);
    
    // 按价格区间动态扩展：active_price ±5% 对应的 bin_id 范围
    // price = step_factor^bin_id => bin_id = ln(price) / ln(step_factor)
    let step_ln = step_factor.ln();
    let price_lower = active_price * 0.95;
    let price_upper = active_price * 1.05;
    let bin_id_lower = (price_lower.ln() / step_ln).floor() as i32;
    let bin_id_upper = (price_upper.ln() / step_ln).ceil() as i32;
    
    let index_lower = bin_array_index_for_dlmm(bin_id_lower);
    let index_upper = bin_array_index_for_dlmm(bin_id_upper);
    
    // 确保至少扫描 ±25 个 bin arrays，并保证包含 active_index（减少 3005 错误，激进缓存）
    let min_range = 25i64;
    let index_lower_expanded = index_lower.min(active_index - min_range * BIN_ARRAY_INDEX_STRIDE);
    let index_upper_expanded = index_upper.max(active_index + min_range * BIN_ARRAY_INDEX_STRIDE);
    
    log::debug!(
        "DLMM build_lb_state: active_id={} active_index={} active_price={:.9} price_range=[{:.9}, {:.9}] bin_range=[{}, {}] index_range=[{}, {}]",
        active_id, active_index, active_price, price_lower, price_upper, bin_id_lower, bin_id_upper, index_lower_expanded, index_upper_expanded
    );

    let mut candidate_indexes: BTreeSet<i64> = BTreeSet::new();
    // 从 index_lower_expanded 到 index_upper_expanded，步进 BIN_ARRAY_INDEX_STRIDE
    let mut idx = index_lower_expanded;
    while idx <= index_upper_expanded {
        candidate_indexes.insert(idx);
        idx += BIN_ARRAY_INDEX_STRIDE;
    }

    let mut arrays: Vec<Arc<super::meteora_lb::BinArrayState>> = Vec::new();
    let mut seen_indexes: HashSet<i64> = HashSet::new();

    let candidate_count = candidate_indexes.len();
    log::debug!(
        "DLMM build_lb_state: pool={} active_id={} scanning {} candidates in range [{}, {}]",
        pool, active_id, candidate_count, index_lower_expanded, index_upper_expanded
    );
    
    let mut not_found_count = 0;
    let mut error_count = 0;
    
    for index in candidate_indexes {
        if !index_in_bitmap_range(index) {
            log::trace!("DLMM: idx={} out of bitmap range", index);
            continue;
        }
        if !seen_indexes.insert(index) {
            continue;
        }
        // Bin array可能不存在，优雅地跳过
        match ctx.get_meteora_bin_array(pool, index, current_slot).await {
            Ok(Some(array)) => {
                if array.lb_pair != *pool {
                    log::trace!("DLMM: idx={} lb_pair mismatch", index);
                    continue;
                }
                // 即便暂时无流动性也保留，方便后续订阅填充
                seen_indexes.insert(array._index);
                arrays.push(array.clone());
                log::debug!("DLMM: Found BinArray idx={}", index);
            }
            Ok(None) => {
                // BinArray 不存在是正常的（可能断层），不要 warn
                not_found_count += 1;
                continue;
            }
            Err(e) => {
                // AccountNotFound 是正常的（该价格区间没有流动性）
                let err_str = format!("{:?}", e);
                if err_str.contains("AccountNotFound") {
                    not_found_count += 1;
                } else {
                    error_count += 1;
                    log::warn!("DLMM: Error fetching BinArray idx={}: {}", index, e);
                }
                continue;
            } 
        }
    }
    
    log::debug!(
        "DLMM build_lb_state: pool={} found {} arrays, {} not_found, {} errors",
        pool, arrays.len(), not_found_count, error_count
    );

    if arrays.is_empty() {
        log::debug!(
            "DLMM build_lb_state: No bin arrays found for pool {} in index range [{}, {}] (scanned {} candidates)",
            pool, index_lower_expanded, index_upper_expanded, candidate_count
        );
        return Ok(None);
    }

    let array_count = arrays.len();
    let mut bin_array_indexes: Vec<i64> = arrays.iter().map(|array| array._index).collect();
    bin_array_indexes.sort_unstable();
    bin_array_indexes.dedup();

    let mut bin_map: BTreeMap<i32, super::meteora_lb::LbBin> = BTreeMap::new();

    for array in arrays {
        // bin_array index 到 bin_id 的映射
        // index / stride 得到 quotient，quotient * span 得到起始 bin_id
        let quotient = array._index / BIN_ARRAY_INDEX_STRIDE;
        let start_bin_id = (quotient * BIN_ARRAY_SPAN as i64) as i32;
        
        log::trace!(
            "Processing bin_array idx={} quotient={} start_bin_id={}",
            array._index, quotient, start_bin_id
        );
        
        for (slot, entry) in array.bins.iter().enumerate() {
            if entry.amount_x == 0 && entry.amount_y == 0 {
                continue;
            }
            if entry.price_q64 == 0 {
                continue;
            }

            // 直接使用 slot 计算的 bin_id；价格字段（price_q64）可能与新版布局不符，避免引入噪声
            let bin_id = start_bin_id + slot as i32;
            
            // 使用 bin_id 计算理论价格，保持与 active_id 一致的刻度
            let theoretical_price = step_factor.powi(bin_id);

            let bin = bin_map
                .entry(bin_id)
                .or_insert_with(|| super::meteora_lb::LbBin {
                    _bin_id: bin_id,
                    price: theoretical_price,
                    amount_x: 0.0,
                    amount_y: 0.0,
                });

            bin.price = theoretical_price;
            bin.amount_x += entry.amount_x as f64;
            bin.amount_y += entry.amount_y as f64;
        }
    }

    let mut bins: Vec<super::meteora_lb::LbBin> = bin_map
        .into_values()
        .filter(|bin| bin.amount_x > EPSILON || bin.amount_y > EPSILON)
        .collect();

    if bins.is_empty() {
        log::warn!("DLMM: No bins with liquidity after processing {} bin arrays", array_count);
        return Ok(None);
    }

    // Sort bins by price for binary search
    bins.sort_by(|a, b| match a.price.partial_cmp(&b.price) {
        Some(order) => order,
        None => std::cmp::Ordering::Equal,
    });

    // 建立价格锚：验证 active_price 落在首尾价格区间内
    let first_price = bins.first().unwrap().price;
    let last_price = bins.last().unwrap().price;
    let first_bin_id = bins.first().unwrap()._bin_id;
    let last_bin_id = bins.last().unwrap()._bin_id;
    
    log::debug!(
        "DLMM: Built LbState with {} bins, bin_id range [{}, {}], price range [{:.9}, {:.9}], active_id={} active_price={:.9}",
        bins.len(), first_bin_id, last_bin_id, first_price, last_price, active_id, active_price
    );
    
    // 检查 active_id 是否在 bin 范围内（带节流的警告）
    if active_id < first_bin_id || active_id > last_bin_id {
        // Throttled warning - only log once per BOUNDARY_WARN_INTERVAL per pool
        let should_warn = if let Ok(mut times) = BOUNDARY_WARN_TIMES.lock() {
            let now = Instant::now();
            let last_warn = times.get(pool).copied();
            
            match last_warn {
                Some(t) if now.duration_since(t) < BOUNDARY_WARN_INTERVAL => false,
                _ => {
                    times.insert(*pool, now);
                    true
                }
            }
        } else {
            // If mutex is poisoned, default to warning
            true
        };
        
        if should_warn {
            warn!(
                "DLMM {}: active_id={} outside bins range [{}, {}], simulation may be inaccurate",
                pool, active_id, first_bin_id, last_bin_id
            );
        } else {
            debug!(
                "DLMM: active_id={} outside bins range [{}, {}], using anchor_price",
                active_id, first_bin_id, last_bin_id
            );
        }
    }

    Ok(Some(LbState {
        _active_id: active_id,
        active_price,
        _bin_step: bin_step,
        _inline_bitmap: *inline_bitmap,
        _extension_bitmap: None,  // 不再使用 bitmap extension
        bin_array_indexes,
        bins,
    }))
}

pub fn bin_array_index_for_dlmm(bin_id: i32) -> i64 {
    const BIN_ARRAY_SPAN: i32 = 70;
    const BIN_ARRAY_INDEX_STRIDE: i64 = 1; // Continuous indexes per SDK

    let mut quotient = bin_id / BIN_ARRAY_SPAN;
    if bin_id < 0 && bin_id % BIN_ARRAY_SPAN != 0 {
        quotient -= 1;
    }
    (quotient as i64) * BIN_ARRAY_INDEX_STRIDE
}

/// 计算给定 active_id 时需要订阅的核心 bin array indexes（不包括 bitmap 中的）
/// 返回当前 bin array 及其相邻的 ±window_size
/// 
/// window_size: 订阅窗口大小，例如 2 表示订阅 active ± 2 个 bin arrays
pub fn compute_core_bin_array_indexes(active_id: i32) -> Vec<i64> {
    compute_core_bin_array_indexes_with_window(active_id, get_bin_window_size())
}

/// 从环境变量获取 bin array 订阅窗口大小，默认为 2
pub fn get_bin_window_size() -> usize {
    std::env::var("METEORA_BIN_WINDOW")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
}

/// 带可配置窗口大小的版本
pub fn compute_core_bin_array_indexes_with_window(active_id: i32, window_size: usize) -> Vec<i64> {
    const BIN_ARRAY_INDEX_STRIDE: i64 = 1;

    let active_index = bin_array_index_for_dlmm(active_id);
    let window = window_size as i64;
    
    let mut indexes = Vec::with_capacity((2 * window + 1) as usize);
    for offset in -window..=window {
        indexes.push(active_index + offset * BIN_ARRAY_INDEX_STRIDE);
    }
    indexes
}

/// 将 bin array indexes 转换为 Pubkey 地址列表
pub fn bin_array_indexes_to_addresses(
    program_id: &Pubkey,
    pool: &Pubkey,
    indexes: &[i64],
) -> Vec<Pubkey> {
    indexes
        .iter()
        .map(|&index| meteora_lb::bin_array_address(program_id, pool, index))
        .collect()
}

/// 检查两个 bin array 地址列表是否相同（忽略顺序）
pub fn bin_arrays_changed(old: &[Pubkey], new: &[Pubkey]) -> bool {
    if old.len() != new.len() {
        return true;
    }

    use std::collections::HashSet;
    let old_set: HashSet<_> = old.iter().collect();
    let new_set: HashSet<_> = new.iter().collect();

    old_set != new_set
}

fn index_in_bitmap_range(index: i64) -> bool {
    const BIN_ARRAY_BITMAP_SIZE: i64 = 512;
    const EXTENSION_BITMAP_PAGES: usize = 12;
    const BIN_ARRAY_INDEX_STRIDE: i64 = 1;

    let limit = BIN_ARRAY_BITMAP_SIZE * (EXTENSION_BITMAP_PAGES as i64 + 1) - 1;
    let min = -(limit + 1) * BIN_ARRAY_INDEX_STRIDE;
    let max = limit * BIN_ARRAY_INDEX_STRIDE;
    index >= min && index <= max
}

pub fn simulate_trade(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    // Reuse LB trade simulation logic since DLMM uses same bin-based liquidity model
    meteora_lb::simulate_trade(snapshot, side, base_amount)
}

/// 预热 DLMM 池的 bin arrays 缓存
/// 
/// 在监控启动时调用此函数，使用 sidecar 的 batch introspect 获取池子的 bin arrays，
/// 然后缓存到 AppContext 中，供后续 decode 使用。
/// 
/// 返回成功预热的池数量。
pub async fn warmup_bin_arrays(
    ctx: &AppContext,
    sidecar: &SidecarClient,
    pools: &[Pubkey],
) -> usize {
    // 每批 10 个池子，超时 30 秒
    // 这样即使有 RPC 限流（每个池子 50ms 延迟 + 重试），也有足够时间
    const BATCH_SIZE: usize = 10;
    const BATCH_TIMEOUT_SECS: u64 = 30;
    let mut success_count = 0;
    for chunk in pools.chunks(BATCH_SIZE) {
        if chunk.len() == 1 {
            let pool = &chunk[0];
            match warmup_single_pool(ctx, sidecar, pool).await {
                Ok(count) => {
                    if count > 0 {
                        debug!("DLMM warmup: pool {} cached {} bin arrays", pool, count);
                        success_count += 1;
                    }
                }
                Err(e) => {
                    debug!("DLMM warmup: pool {} introspect failed: {}", pool, e);
                }
            }
            continue;
        }

        let pool_strings: Vec<String> = chunk.iter().map(|p| p.to_string()).collect();
        let batch = tokio::time::timeout(
            std::time::Duration::from_secs(BATCH_TIMEOUT_SECS),
            sidecar.batch_introspect_meteora(&pool_strings),
        )
        .await;

        let response = match batch {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                debug!("DLMM batch warmup failed: {}", e);
                continue;
            }
            Err(_) => {
                warn!("DLMM batch warmup timed out ({} pools)", chunk.len());
                continue;
            }
        };

        let results = response.results.unwrap_or_default();
        for entry in results {
            if !entry.ok {
                continue;
            }
            let pool = match Pubkey::from_str(&entry.pool) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let bin_arrays = match entry.bin_arrays {
                Some(arrays) => arrays,
                None => continue,
            };
            let parsed: Vec<Pubkey> = bin_arrays
                .iter()
                .filter_map(|s| Pubkey::from_str(s).ok())
                .collect();
            let x_to_y: Vec<Pubkey> = entry.bin_arrays_x_to_y
                .unwrap_or_default()
                .iter()
                .filter_map(|s| Pubkey::from_str(s).ok())
                .collect();
            let y_to_x: Vec<Pubkey> = entry.bin_arrays_y_to_x
                .unwrap_or_default()
                .iter()
                .filter_map(|s| Pubkey::from_str(s).ok())
                .collect();
            if parsed.is_empty() && x_to_y.is_empty() && y_to_x.is_empty() {
                continue;
            }
            let mut combined = Vec::new();
            let mut seen = HashSet::new();
            for addr in parsed.iter().chain(x_to_y.iter()).chain(y_to_x.iter()) {
                if seen.insert(*addr) {
                    combined.push(*addr);
                }
            }

            let validated = validate_bin_arrays(ctx, &pool, &combined).await;
            if validated.is_empty() {
                debug!("DLMM warmup: pool {} has no valid bin_arrays", pool);
                continue;
            }

            let valid_set: HashSet<Pubkey> = validated.iter().copied().collect();
            let filtered_x_to_y: Vec<Pubkey> = x_to_y
                .into_iter()
                .filter(|addr| valid_set.contains(addr))
                .collect();
            let filtered_y_to_x: Vec<Pubkey> = y_to_x
                .into_iter()
                .filter(|addr| valid_set.contains(addr))
                .collect();

            ctx.set_dlmm_bin_arrays(&pool, validated);
            if !filtered_x_to_y.is_empty() || !filtered_y_to_x.is_empty() {
                ctx.set_dlmm_directional_bin_arrays(&pool, filtered_x_to_y, filtered_y_to_x);
            }

            success_count += 1;
        }

    }

    success_count
}

/// 预热单个 DLMM 池
/// 
/// 预取更宽范围的 bin_arrays（±5 个 index），这样即使价格移动了，
/// 运行时仍能选择正确的 bin_arrays 而无需额外网络调用
async fn warmup_single_pool(
    ctx: &AppContext,
    sidecar: &SidecarClient,
    pool: &Pubkey,
) -> Result<usize> {
    // ⭐ 只有当方向性缓存都有数据时才跳过，避免旧进程只有 merged 缓存
    let has_x_to_y = ctx.get_dlmm_bin_arrays_for_direction(pool, true)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let has_y_to_x = ctx.get_dlmm_bin_arrays_for_direction(pool, false)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    
    if has_x_to_y && has_y_to_x {
        let total = ctx.get_dlmm_bin_arrays_for_direction(pool, true).unwrap_or_default().len()
            + ctx.get_dlmm_bin_arrays_for_direction(pool, false).unwrap_or_default().len();
        return Ok(total);
    }

    let pool_str = pool.to_string();
    
    // ⭐ sidecar introspect 也需要超时保护（5s）
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        sidecar.introspect_meteora(&pool_str)
    ).await
        .map_err(|_| anyhow!("sidecar introspect_meteora timed out"))?
        .map_err(|e| anyhow!("sidecar introspect_meteora failed: {}", e))?;
    
    if !resp.ok {
        return Err(anyhow!("introspect returned ok=false: {:?}", resp.error));
    }
    
    // 获取 sidecar 返回的初始 bin_arrays
    let initial_bin_arrays: Vec<Pubkey> = resp
        .bin_arrays
        .unwrap_or_default()
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    
    // ⭐ 直接使用 sidecar 返回的 bin_arrays（sidecar 已经获取了 5 个）
    // 不再额外调用 RPC 检查其他 bin_arrays，避免 429 限流
    // ⭐ 解析并保存方向性 bin_arrays
    let x_to_y: Vec<Pubkey> = resp.bin_arrays_x_to_y
        .unwrap_or_default()
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    let y_to_x: Vec<Pubkey> = resp.bin_arrays_y_to_x
        .unwrap_or_default()
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    if initial_bin_arrays.is_empty() && x_to_y.is_empty() && y_to_x.is_empty() {
        return Ok(0);
    }

    let mut combined = Vec::new();
    let mut seen = HashSet::new();
    for addr in initial_bin_arrays.iter().chain(x_to_y.iter()).chain(y_to_x.iter()) {
        if seen.insert(*addr) {
            combined.push(*addr);
        }
    }
    let validated = validate_bin_arrays(ctx, pool, &combined).await;
    if validated.is_empty() {
        debug!("DLMM warmup: pool {} has no valid bin_arrays", pool);
        return Ok(0);
    }

    let valid_set: HashSet<Pubkey> = validated.iter().copied().collect();
    let filtered_x_to_y: Vec<Pubkey> = x_to_y
        .into_iter()
        .filter(|addr| valid_set.contains(addr))
        .collect();
    let filtered_y_to_x: Vec<Pubkey> = y_to_x
        .into_iter()
        .filter(|addr| valid_set.contains(addr))
        .collect();

    ctx.set_dlmm_bin_arrays(pool, validated.clone());
    if !filtered_x_to_y.is_empty() || !filtered_y_to_x.is_empty() {
        ctx.set_dlmm_directional_bin_arrays(pool, filtered_x_to_y, filtered_y_to_x);
    }

    Ok(validated.len())
}

/// 预热单个 DLMM 池（公开接口，供外部调用）
pub async fn warmup_pool(
    ctx: &AppContext,
    sidecar: &SidecarClient,
    pool: &Pubkey,
) -> Result<usize> {
    warmup_single_pool(ctx, sidecar, pool).await
}

pub async fn validate_bin_arrays(
    ctx: &AppContext,
    pool: &Pubkey,
    addresses: &[Pubkey],
) -> Vec<Pubkey> {
    if addresses.is_empty() {
        return Vec::new();
    }

    match ctx.get_meteora_bin_arrays_batch(pool, addresses).await {
        Ok(found) => {
            let valid_set: HashSet<Pubkey> = found.into_iter().map(|(addr, _)| addr).collect();
            let mut out = Vec::new();
            let mut seen = HashSet::new();
            for addr in addresses {
                if valid_set.contains(addr) && seen.insert(*addr) {
                    out.push(*addr);
                }
            }
            out
        }
        Err(err) => {
            warn!("DLMM {} bin_array validation failed: {}", pool, err);
            addresses.to_vec()
        }
    }
}

