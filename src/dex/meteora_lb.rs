use anyhow::{anyhow, Result};
use chrono::Utc;
use log::debug;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
};

use super::{
    normalize_pair, AccountReader, MeteoraDynamicFeeParams, PoolDescriptor, PoolFees, PoolSnapshot, TradeQuote, TradeSide,
};
use crate::rpc::AppContext;

const BIN_ARRAY_BIN_COUNT: usize = 70;
const BIN_ARRAY_SPAN: i32 = 70; // Changed from 600 to match on-chain stride
const BIN_ARRAY_INDEX_STRIDE: i64 = 1;
const INLINE_BITMAP_WORDS: usize = 16;
const INLINE_BITMAP_HALF_WORDS: usize = INLINE_BITMAP_WORDS / 2;
const EXTENSION_BITMAP_PAGES: usize = 12;
const EXTENSION_BITMAP_WORDS: usize = 8;
const WORD_BITS: usize = 64;
const BIN_ARRAY_BITMAP_SIZE: i64 = (INLINE_BITMAP_HALF_WORDS as i64) * WORD_BITS as i64; // 512
const Q64: f64 = 18_446_744_073_709_551_616.0;
const EPSILON: f64 = 1e-9;

#[derive(Debug, Clone)]
pub struct LbState {
    pub _active_id: i32,
    pub active_price: f64,
    pub _bin_step: u16,
    pub _inline_bitmap: [u64; INLINE_BITMAP_WORDS],
    pub _extension_bitmap: Option<Arc<BitmapExtension>>,
    pub bin_array_indexes: Vec<i64>,
    pub bins: Vec<LbBin>,
}

#[derive(Debug, Clone)]
pub struct LbAccounts {
    pub reserve_x: Pubkey,
    pub reserve_y: Pubkey,
    pub oracle: Pubkey,
}

#[derive(Debug, Clone)]
pub struct LbBin {
    pub _bin_id: i32,
    pub price: f64,
    pub amount_x: f64,
    pub amount_y: f64,
}

impl LbState {
    /// Estimate maximum available liquidity for a given side (in raw token_x units)
    /// This is independent of inverted status - always calculates for token_x
    pub fn estimate_max_liquidity_x(&self, side: TradeSide) -> f64 {
        match side {
            TradeSide::Buy => {
                // Buy token_x: need bins at or below active price with x liquidity
                let mut total_x = 0.0;
                for bin in &self.bins {
                    if bin.price <= self.active_price && bin.amount_x > EPSILON {
                        total_x += bin.amount_x;
                    }
                }
                total_x
            }
            TradeSide::Sell => {
                // Sell token_x: need bins at or above active price with y liquidity
                // Available x we can sell = sum of (y / price) for bins above active
                let mut total_x_can_consume = 0.0;
                for bin in &self.bins {
                    if bin.price >= self.active_price && bin.amount_y > EPSILON && bin.price > EPSILON {
                        total_x_can_consume += bin.amount_y / bin.price;
                    }
                }
                total_x_can_consume
            }
        }
    }
    
    /// Estimate maximum available liquidity for a given side (in raw token_y units)
    pub fn estimate_max_liquidity_y(&self, side: TradeSide) -> f64 {
        match side {
            TradeSide::Buy => {
                // Buy token_y: we pull y out of bins at or below active price
                self.bins
                    .iter()
                    .filter(|bin| bin.price <= self.active_price && bin.amount_y > EPSILON)
                    .map(|bin| bin.amount_y)
                    .sum()
            }
            TradeSide::Sell => {
                // Sell token_y: we also consume y from bins at or below active price
                self.bins
                    .iter()
                    .filter(|bin| bin.price <= self.active_price && bin.amount_y > EPSILON)
                    .map(|bin| bin.amount_y)
                    .sum()
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct BinArrayState {
    pub _index: i64,
    pub lb_pair: Pubkey,
    pub bins: Vec<BinEntry>,
}

#[derive(Debug, Clone)]
pub struct BinEntry {
    pub _slot: usize,
    pub amount_x: u64,
    pub amount_y: u64,
    pub price_q64: u128,
}

#[derive(Debug, Clone)]
pub struct BitmapExtension {
    pub lb_pair: Pubkey,
    pub positive: [[u64; EXTENSION_BITMAP_WORDS]; EXTENSION_BITMAP_PAGES],
    pub negative: [[u64; EXTENSION_BITMAP_WORDS]; EXTENSION_BITMAP_PAGES],
}

pub async fn decode(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    if data.len() < 8 {
        anyhow::bail!("lb pair account too short");
    }

    let mut reader = AccountReader::new(&data[8..]);

    let base_factor = reader.read_u16()?;
    reader.read_u16()?; // filter period
    reader.read_u16()?; // decay period
    reader.read_u16()?; // reduction factor
    let variable_fee_control = reader.read_u32()?;
    reader.read_u32()?; // max volatility accumulator
    reader.read_i32()?; // min bin id
    reader.read_i32()?; // max bin id
    let mut protocol_share = reader.read_u16()?;
    // Sanity check: protocol_share is in basis points (0-10000).
    if protocol_share > 10000 {
        log::warn!(
            "Parsed Meteora LB protocol_share is absurdly high ({}), defaulting to 0",
            protocol_share
        );
        protocol_share = 0;
    }
    let base_fee_power_factor = reader.read_u8()?;
    reader.skip(5)?; // padding

    // Parse volatility state (needed for dynamic fee calculation)
    let volatility_accumulator = reader.read_u32()?;
    let volatility_reference = reader.read_u32()?;
    reader.read_i32()?; // index reference
    reader.skip(4)?; // padding
    reader.read_i64()?; // last update timestamp
    reader.skip(8)?; // padding

    reader.skip(1)?; // bump seed
    reader.skip(2)?; // bin step seed
    reader.read_u8()?; // pair type
    let active_id = reader.read_i32()?;
    let bin_step = reader.read_u16()?;
    reader.read_u8()?; // status
    reader.read_u8()?; // require base factor seed
    reader.skip(2)?; // base factor seed
    reader.read_u8()?; // activation type
    reader.read_u8()?; // creator on/off control

    let token_x_mint = reader.read_pubkey()?;
    let token_y_mint = reader.read_pubkey()?;
    let reserve_x = reader.read_pubkey()?;
    let reserve_y = reader.read_pubkey()?;
    reader.read_u64()?; // protocol fee amount x
    reader.read_u64()?; // protocol fee amount y
    reader.skip(32)?; // padding_1
    reader.skip(144 * 2)?; // reward infos
    let oracle = reader.read_pubkey()?;
    let mut inline_bitmap = [0u64; INLINE_BITMAP_WORDS];
    for word in inline_bitmap.iter_mut() {
        *word = reader.read_u64()?;
    }
    reader.read_i64()?; // last updated at
    reader.skip(32)?; // padding_2
    reader.skip(32)?; // pre activation swap address
    reader.skip(32)?; // base key
    reader.read_u64()?; // activation point
    reader.read_u64()?; // pre activation duration
    reader.skip(8)?; // padding_3
    reader.read_u64()?; // padding_4
    reader.skip(32)?; // creator
    reader.read_u8()?; // token_mint_x_program_flag
    reader.read_u8()?; // token_mint_y_program_flag
    reader.skip(22)?; // reserved

    // ✅ 并行获取所有 RPC 数据，绑定到池子更新的 slot
    // Spot-only 模式下：不拉 reserve 余额，避免每次更新触发 HTTP RPC（节省 credits）
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

    let step = 1.0 + (bin_step as f64) / 10_000.0;
    if step <= 0.0 {
        return Err(anyhow!("invalid bin step"));
    }
    let price_raw = step.powi(active_id);
    if !price_raw.is_finite() || price_raw <= 0.0 {
        return Err(anyhow!("invalid lb price"));
    }
    let decimals_adjustment = mint_x_info.decimals as i32 - mint_y_info.decimals as i32;
    let price = price_raw * 10f64.powi(decimals_adjustment);

    let (normalized_pair, normalized_price) = normalize_pair(token_x_mint, token_y_mint, price)
        .ok_or_else(|| anyhow!("cannot normalize meteora lb pair"))?;

    let total_fee_ratio = compute_base_fee(bin_step, base_factor, base_fee_power_factor);
    let protocol_fee_ratio = total_fee_ratio * (protocol_share as f64 / 10_000.0);
    let lp_fee_ratio = (total_fee_ratio - protocol_fee_ratio).max(0.0);

    // ✅ Spot-only 模式下跳过 lb_state 构建，减少 RPC 请求
    let lb_state = if ctx.spot_only_mode() {
        None
    } else {
        build_lb_state(
            ctx,
            &descriptor.address,
            active_id,
            bin_step,
            &inline_bitmap,
            context.slot,
        )
        .await?
    };

    // ✅ Spot-only 模式：只订阅主池账户，不订阅任何依赖账户（价格从主池 active_id 算）
    use crate::constants::METEORA_LB_PROGRAM_ID;
    let dependent_accounts = if ctx.spot_only_mode() {
        vec![]
    } else {
        let mut deps = vec![reserve_x, reserve_y, oracle];

        // 添加 bitmap extension
        let bitmap_ext_addr = bitmap_extension_address(&METEORA_LB_PROGRAM_ID, &descriptor.address);
        deps.push(bitmap_ext_addr);

        // 始终订阅当前 active bin array 及其相邻 ±1，用于初次构建失败时依然能通过 WebSocket 恢复
        let active_index = bin_array_index_for(active_id);
        let core_indexes = [
            active_index - BIN_ARRAY_INDEX_STRIDE,
            active_index,
            active_index + BIN_ARRAY_INDEX_STRIDE,
        ];
        for index in core_indexes {
            let bin_array_addr = bin_array_address(&METEORA_LB_PROGRAM_ID, &descriptor.address, index);
            deps.push(bin_array_addr);
        }

        // 添加 bin array 地址
        if let Some(ref state) = lb_state {
            for &index in &state.bin_array_indexes {
                let bin_array_addr =
                    bin_array_address(&METEORA_LB_PROGRAM_ID, &descriptor.address, index);
                deps.push(bin_array_addr);
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

async fn build_lb_state(
    ctx: &AppContext,
    pool: &Pubkey,
    active_id: i32,
    bin_step: u16,
    inline_bitmap: &[u64; INLINE_BITMAP_WORDS],
    current_slot: u64, // ✅ 添加 slot 参数
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

    let active_index = bin_array_index_for(active_id);

    // Only scan active_index ± range, ignore bitmap/extension to avoid fetching non-existent accounts
    // Most bitmap/extension indexes don't have actual on-chain accounts
    //
    // EXTRA_RANGE controls how many bin arrays we subscribe to:
    // - EXTRA_RANGE=1 → 3 bin arrays (active ±1), minimal but may miss liquidity
    // - EXTRA_RANGE=2 → 5 bin arrays, good balance for small trades (<100 USD)
    // - EXTRA_RANGE=10 → 21 bin arrays, full coverage for large trades
    //
    // EXTRA_RANGE=2 is recommended for small trades: covers most liquidity distributions
    // while keeping subscription costs low (~9-12 accounts per pool).
    let mut candidate_indexes: BTreeSet<i64> = BTreeSet::new();
    const EXTRA_RANGE: i64 = 2;  // ±2 = 5 bin arrays, balanced for small trades
    for k in -EXTRA_RANGE..=EXTRA_RANGE {
        candidate_indexes.insert(active_index + k * BIN_ARRAY_INDEX_STRIDE);
    }

    let mut arrays: Vec<Arc<BinArrayState>> = Vec::new();
    let mut seen_indexes: HashSet<i64> = HashSet::new();

    for index in candidate_indexes {
        if !index_in_bitmap_range(index) {
            continue;
        }
        if !seen_indexes.insert(index) {
            continue;
        }
        if let Some(array) = ctx.get_meteora_bin_array(pool, index, current_slot).await? {
            // ✅ 传入 slot
            if array.lb_pair != *pool {
                continue;
            }
            // 即便暂时没有流动性也保留，后续订阅可填充
            seen_indexes.insert(array._index);
            arrays.push(array.clone());
        }
    }

    if arrays.is_empty() {
        debug!(
            "No Meteora bin arrays available for pool {} (scanned active ±{} range)",
            pool, EXTRA_RANGE
        );
        return Ok(None);
    }

    let mut bin_array_indexes: Vec<i64> = arrays.iter().map(|array| array._index).collect();
    bin_array_indexes.sort_unstable();
    bin_array_indexes.dedup();

    let mut bin_map: BTreeMap<i32, LbBin> = BTreeMap::new();

    for array in arrays {
        // bin_array index 到 bin_id 的映射
        let quotient = array._index / BIN_ARRAY_INDEX_STRIDE;
        let start_bin_id = (quotient * BIN_ARRAY_SPAN as i64) as i32;
        
        for (slot, entry) in array.bins.iter().enumerate() {
            if entry.amount_x == 0 && entry.amount_y == 0 {
                continue;
            }
            if entry.price_q64 == 0 {
                continue;
            }

            let price_ratio = (entry.price_q64 as f64) / Q64;
            if !price_ratio.is_finite() || price_ratio <= 0.0 {
                continue;
            }

            // bin_id = start_bin_id + slot
            let bin_id = start_bin_id + slot as i32;
            
            // 使用 bin_id 计算理论价格
            let theoretical_price = step_factor.powi(bin_id);

            let bin = bin_map.entry(bin_id).or_insert_with(|| LbBin {
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

    let mut bins: Vec<LbBin> = bin_map
        .into_values()
        .filter(|bin| bin.amount_x > EPSILON || bin.amount_y > EPSILON)
        .collect();

    if bins.is_empty() {
        return Ok(None);
    }

    bins.sort_by(|a, b| match a.price.partial_cmp(&b.price) {
        Some(order) => order,
        None => Ordering::Equal,
    });

    Ok(Some(LbState {
        _active_id: active_id,
        active_price,
        _bin_step: bin_step,
        _inline_bitmap: *inline_bitmap,
        _extension_bitmap: None,  // Not fetching extension anymore to avoid RPC overhead
        bin_array_indexes,
        bins,
    }))
}

fn compute_base_fee(bin_step: u16, base_factor: u16, _base_fee_power_factor: u8) -> f64 {
    if bin_step == 0 || base_factor == 0 {
        return 0.0;
    }

    // Meteora LB/DLMM base fee formula:
    // base_fee_rate = (bin_step * base_factor) / 10_000_000_000
    // This gives the fee as a ratio (e.g., 0.001 = 0.1%)
    let base = (bin_step as f64) * (base_factor as f64) / 10_000_000_000.0;
    
    if !base.is_finite() || base <= 0.0 {
        0.0
    } else {
        // Cap at 10% max fee (sanity check)
        base.min(0.10)
    }
}

pub fn simulate_trade(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    if !(base_amount.is_finite() && base_amount > 0.0) {
        return None;
    }

    if let Some(state) = snapshot.lb_state.as_ref() {
        simulate_trade_precise(snapshot, state, side, base_amount)
    } else {
        debug!(
            "Meteora LB/DLMM {} has no lb_state, skipping trade simulation",
            snapshot.descriptor.label
        );
        None
    }
}

/// Simulate trade with an externally provided LbState
/// This is useful when lb_state is built from SDK introspection
pub fn simulate_trade_with_state(
    snapshot: &PoolSnapshot,
    state: &LbState,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    simulate_trade_precise(snapshot, state, side, base_amount)
}

fn simulate_trade_precise(
    snapshot: &PoolSnapshot,
    state: &LbState,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    let fees = snapshot.fees.as_ref()?;
    let fee_ratio = fees.total_ratio();
    if !(fee_ratio >= 0.0 && fee_ratio < 1.0) {
        debug!("DLMM simulate_trade_precise: invalid fee_ratio={}", fee_ratio);
        return None;
    }

    let inverted = snapshot.normalized_pair.inverted;
    
    // PoolSnapshot 中 base=token_x, quote=token_y（未被 normalize 交换）
    // 但 simulate_trade 的 base_amount 和返回的 quote 是 normalized 的
    // normalized 的定义：按字母序排序后的 pair
    // 
    // 当 inverted=false: normalized (base,quote) = (token_x, token_y)
    //   → actual_base_decimals = token_x decimals = snapshot.base_decimals
    //   → actual_quote_decimals = token_y decimals = snapshot.quote_decimals
    // 
    // 当 inverted=true: normalized (base,quote) = (token_y, token_x)
    //   → actual_base_decimals = token_y decimals = snapshot.quote_decimals
    //   → actual_quote_decimals = token_x decimals = snapshot.base_decimals
    let (actual_base_decimals, actual_quote_decimals) = if inverted {
        (snapshot.quote_decimals, snapshot.base_decimals)
    } else {
        (snapshot.base_decimals, snapshot.quote_decimals)
    };
    
    let base_scale = 10f64.powi(actual_base_decimals as i32);
    let quote_scale = 10f64.powi(actual_quote_decimals as i32);
    let fee_den = 1.0 - fee_ratio;
    if fee_den <= EPSILON {
        return None;
    }
    
    // bin.price = step_factor^bin_id 是纯数学比率，表示 token_y / token_x（无单位）
    // bin.amount_x 和 bin.amount_y 都是原始整数单位
    // decimals_adj=1.0，不对 bin.price 做额外调整
    let decimals_adj = 1.0;

    // 诊断日志：入口信息
    let first_bin = state.bins.first()?;
    let last_bin = state.bins.last()?;
    
    // Check if active_price is out of bins range
    let price_out_of_range = state.active_price < first_bin.price - EPSILON 
        || state.active_price > last_bin.price + EPSILON;
    
    // 将 active_price 锚定到可用价格区间，避免缺失 bin array 时把流动性看成 0
    let anchor_price = state
        .active_price
        .clamp(first_bin.price, last_bin.price);

    debug!(
        "DLMM simulate_trade_precise ENTRY: pool={} side={:?} inv={} base_amt={:.9} active_price={:.9} bins={} range=[{:.9}, {:.9}] bin_ids=[{}, {}] out_of_range={}",
        snapshot.descriptor.label, side, inverted, base_amount, state.active_price,
        state.bins.len(), first_bin.price, last_bin.price, first_bin._bin_id, last_bin._bin_id, price_out_of_range
    );

    // 如果 active_price 已经超出已知 bins 范围，bin arrays 不完整会导致“把所有流动性都当成可交易”，
    // 从而高估卖出回款/低估买入成本。直接放弃本次模拟，避免输出虚高的净利润。
    if price_out_of_range {
        return None;
    }

    // 流动性预检：如果 active_price 不在 bins 范围内，使用所有可用流动性（不过滤价格）
    let max_liquidity_raw = match (side, inverted) {
        (TradeSide::Buy, false) | (TradeSide::Sell, true) => {
            // 需要 amount_x，价格上行
            if price_out_of_range {
                // When out of range, use all available amount_x
                state.bins.iter()
                    .filter(|bin| bin.amount_x > EPSILON)
                    .map(|bin| bin.amount_x)
                    .sum::<f64>()
            } else {
                state.bins.iter()
                    .filter(|bin| bin.price >= anchor_price - EPSILON && bin.amount_x > EPSILON)
                    .map(|bin| bin.amount_x)
                    .sum::<f64>()
            }
        }
        (TradeSide::Sell, false) | (TradeSide::Buy, true) => {
            // 需要 amount_y，价格下行
            if price_out_of_range {
                // When out of range, use all available amount_y
                state.bins.iter()
                    .filter(|bin| bin.amount_y > EPSILON)
                    .map(|bin| bin.amount_y)
                    .sum::<f64>()
            } else {
                state.bins.iter()
                    .filter(|bin| bin.price <= anchor_price + EPSILON && bin.amount_y > EPSILON)
                    .map(|bin| bin.amount_y)
                    .sum::<f64>()
            }
        }
    };

    let max_liquidity_display = max_liquidity_raw / base_scale;
    debug!("DLMM: max_liquidity_raw={:.2} display={:.9}", max_liquidity_raw, max_liquidity_display);

    if max_liquidity_raw <= EPSILON {
        debug!("DLMM simulate_trade_precise FAIL: no liquidity (side={:?} inv={})", side, inverted);
        return None;
    }

    match (side, inverted) {
        (TradeSide::Buy, false) => {
            // Buy token_x: 从低价→高价，消费 amount_x，付出 token_y
            let target_x_raw = base_amount * base_scale;
            debug!("DLMM Buy(normal): target_x_raw={:.2}", target_x_raw);
            if target_x_raw <= EPSILON {
                debug!("DLMM Buy(normal) FAIL: target_x_raw too small");
                return None;
            }
            let net_quote_in = match quote_y_in_for_x_out(state, target_x_raw, decimals_adj, anchor_price) {
                Some(v) => v,
                None => {
                    debug!("DLMM Buy(normal) FAIL: quote_y_in_for_x_out returned None");
                    return None;
                }
            };
            let gross_quote_in = net_quote_in / fee_den;
            if !(gross_quote_in.is_finite() && gross_quote_in > 0.0) {
                debug!("DLMM Buy(normal) FAIL: gross_quote_in invalid");
                return None;
            }
            let fee_raw = gross_quote_in - net_quote_in;
            let quote_cost = gross_quote_in / quote_scale;
            let fee_in_input = fee_raw / quote_scale;
            let effective_price = quote_cost / base_amount;
            debug!("DLMM Buy(normal) SUCCESS: quote_cost={:.9} effective_price={:.9}", quote_cost, effective_price);
            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Sell, false) => {
            // Sell token_x: 从高价→低价，消费 amount_y，卖出 token_x
            let gross_base_in = base_amount * base_scale;
            debug!("DLMM Sell(normal): gross_base_in={:.2}", gross_base_in);
            if gross_base_in <= EPSILON {
                debug!("DLMM Sell(normal) FAIL: gross_base_in too small");
                return None;
            }
            let net_base_in = gross_base_in * fee_den;
            if net_base_in <= EPSILON {
                debug!("DLMM Sell(normal) FAIL: net_base_in too small");
                return None;
            }
            let quote_out_raw = match quote_y_out_for_x_in(state, net_base_in, decimals_adj, anchor_price) {
                Some(v) => v,
                None => {
                    debug!("DLMM Sell(normal) FAIL: quote_y_out_for_x_in returned None");
                    return None;
                }
            };
            let quote_proceeds = quote_out_raw / quote_scale;
            let fee_raw = gross_base_in - net_base_in;
            let fee_in_input = fee_raw / base_scale;
            let effective_price = quote_proceeds / base_amount;
            debug!("DLMM Sell(normal) SUCCESS: quote_proceeds={:.9} effective_price={:.9}", quote_proceeds, effective_price);
            
            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Buy, true) => {
            // inverted: base=token_y, quote=token_x
            // Buy token_y: 从高价→低价，消费 amount_y，付出 token_x
            let target_y_raw = base_amount * base_scale;
            debug!("DLMM Buy(inverted): target_y_raw={:.2}", target_y_raw);
            if target_y_raw <= EPSILON {
                debug!("DLMM Buy(inverted) FAIL: target_y_raw too small");
                return None;
            }
            let net_x_in = match quote_x_in_for_y_out(state, target_y_raw, decimals_adj, anchor_price) {
                Some(v) => v,
                None => {
                    debug!("DLMM Buy(inverted) FAIL: quote_x_in_for_y_out returned None");
                    return None;
                }
            };
            let gross_x_in = net_x_in / fee_den;
            if !(gross_x_in.is_finite() && gross_x_in > 0.0) {
                debug!("DLMM Buy(inverted) FAIL: gross_x_in invalid");
                return None;
            }
            let fee_raw = gross_x_in - net_x_in;
            let quote_cost = gross_x_in / quote_scale;
            let fee_in_input = fee_raw / quote_scale;
            let effective_price = quote_cost / base_amount;
            debug!("DLMM Buy(inverted) SUCCESS: quote_cost={:.9} effective_price={:.9}", quote_cost, effective_price);
            
            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input,
            })
        }
        (TradeSide::Sell, true) => {
            // inverted: base=token_y, quote=token_x
            // Sell token_y: 从低价→高价，消费 amount_x，卖出 token_y
            let gross_y_in = base_amount * base_scale;
            debug!("DLMM Sell(inverted): gross_y_in={:.2}", gross_y_in);
            if gross_y_in <= EPSILON {
                debug!("DLMM Sell(inverted) FAIL: gross_y_in too small");
                return None;
            }
            let net_y_in = gross_y_in * fee_den;
            if net_y_in <= EPSILON {
                debug!("DLMM Sell(inverted) FAIL: net_y_in too small");
                return None;
            }
            let base_out_raw = match quote_x_out_for_y_in(state, net_y_in, decimals_adj, anchor_price) {
                Some(v) => v,
                None => {
                    debug!("DLMM Sell(inverted) FAIL: quote_x_out_for_y_in returned None");
                    return None;
                }
            };
            let quote_proceeds = base_out_raw / quote_scale;
            let fee_raw = gross_y_in - net_y_in;
            let fee_in_input = fee_raw / base_scale;
            let effective_price = quote_proceeds / base_amount;
            debug!("DLMM Sell(inverted) SUCCESS: quote_proceeds={:.9} effective_price={:.9}", quote_proceeds, effective_price);
            
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

pub fn bin_array_index_for(bin_id: i32) -> i64 {
    let span = BIN_ARRAY_SPAN;
    let mut quotient = bin_id / span;
    if bin_id < 0 && bin_id % span != 0 {
        quotient -= 1;
    }
    (quotient as i64) * BIN_ARRAY_INDEX_STRIDE
}

pub fn bin_array_address(program_id: &Pubkey, pool: &Pubkey, index: i64) -> Pubkey {
    let mut index_bytes = [0u8; 8];
    index_bytes.copy_from_slice(&index.to_le_bytes());
    let (address, _) =
        Pubkey::find_program_address(&[b"bin_array", pool.as_ref(), &index_bytes], program_id);
    address
}

pub fn bitmap_extension_address(program_id: &Pubkey, pool: &Pubkey) -> Pubkey {
    let (address, _) = Pubkey::find_program_address(&[b"bitmap", pool.as_ref()], program_id);
    address
}

pub fn decode_bin_array(data: &[u8]) -> Result<BinArrayState> {
    if data.len() < 8 {
        anyhow::bail!("bin array account too short");
    }

    let mut reader = AccountReader::new(&data[8..]);
    let index = reader.read_i64()?;
    reader.read_u8()?; // version
    reader.skip(7)?; // padding
    let lb_pair = reader.read_pubkey()?;

    // Meteora bin array structure: 112 bytes per bin
    // 2*u64 (amount_x, amount_y) + 6*u128 = 16 + 96 = 112 bytes
    const BIN_RECORD_BYTES: usize = 112;

    let remaining = reader.remaining();
    let bin_count = (remaining / BIN_RECORD_BYTES).min(BIN_ARRAY_BIN_COUNT);
    let mut bins = Vec::with_capacity(bin_count);
    for slot in 0..bin_count {
        let amount_x = reader.read_u64()?;
        let amount_y = reader.read_u64()?;
        let price_q64 = reader.read_u128()?;
        reader.read_u128()?; // liquidity_supply
        reader.read_u128()?; // reward_per_token_stored[0]
        reader.read_u128()?; // reward_per_token_stored[1]
        reader.read_u128()?; // fee_amount_x_per_token_stored
        reader.read_u128()?; // fee_amount_y_per_token_stored
        // 2*8 + 6*16 = 112 bytes

        bins.push(BinEntry {
            _slot: slot,
            amount_x,
            amount_y,
            price_q64,
        });
    }

    Ok(BinArrayState {
        _index: index,
        lb_pair,
        bins,
    })
}

pub fn decode_bitmap_extension(data: &[u8]) -> Result<BitmapExtension> {
    if data.len() < 8 {
        anyhow::bail!("bitmap extension account too short");
    }

    let mut reader = AccountReader::new(&data[8..]);
    let lb_pair = reader.read_pubkey()?;

    let mut positive = [[0u64; EXTENSION_BITMAP_WORDS]; EXTENSION_BITMAP_PAGES];
    for page in positive.iter_mut() {
        for word in page.iter_mut() {
            *word = reader.read_u64()?;
        }
    }

    let mut negative = [[0u64; EXTENSION_BITMAP_WORDS]; EXTENSION_BITMAP_PAGES];
    for page in negative.iter_mut() {
        for word in page.iter_mut() {
            *word = reader.read_u64()?;
        }
    }

    Ok(BitmapExtension {
        lb_pair,
        positive,
        negative,
    })
}

fn index_in_bitmap_range(index: i64) -> bool {
    let limit = BIN_ARRAY_BITMAP_SIZE * (EXTENSION_BITMAP_PAGES as i64 + 1) - 1;
    let min = -(limit + 1) * BIN_ARRAY_INDEX_STRIDE;
    let max = limit * BIN_ARRAY_INDEX_STRIDE;
    index >= min && index <= max
}

// Buy token_x: 价格上行，消费 amount_x (bins with price >= anchor_price)
fn quote_y_in_for_x_out(state: &LbState, target_x_raw: f64, decimals_adj: f64, anchor_price: f64) -> Option<f64> {
    if target_x_raw <= EPSILON {
        return Some(0.0);
    }
    if state.bins.is_empty() {
        return None;
    }

    // 从 price >= anchor 开始，向高价遍历
    // If anchor_price is at or above last bin, start from last bin (fallback for clamped prices)
    let last_price = state.bins.last().map(|b| b.price).unwrap_or(f64::MAX);
    let mut idx = first_index_at_or_above(&state.bins, anchor_price);
    if idx >= state.bins.len() && anchor_price >= last_price - EPSILON * 1000.0 {
        // anchor_price was clamped to last_bin.price, start from last bin
        idx = state.bins.len() - 1;
        debug!("quote_y_in_for_x_out: anchor_price={:.9} at boundary, starting from last bin", anchor_price);
    }
    if idx >= state.bins.len() {
        debug!("quote_y_in_for_x_out: anchor_price={:.9} above all bins, no liquidity", anchor_price);
        return None;
    }
    let mut remaining = target_x_raw;
    let mut total_y = 0.0;
    let mut start_logged = false;

    while idx < state.bins.len() {
        let bin = &state.bins[idx];
        if bin.amount_x > EPSILON && bin.price > 0.0 {
            if !start_logged {
                debug!("quote_y_in_for_x_out: starting from bin_id={} price={:.9}", bin._bin_id, bin.price);
                start_logged = true;
            }
            let take_x = remaining.min(bin.amount_x);
            let price_adj = bin.price * decimals_adj;
            total_y += take_x * price_adj;
            remaining -= take_x;
            if remaining <= EPSILON {
                return Some(total_y);
            }
        }
        idx += 1;
    }

    debug!("quote_y_in_for_x_out: insufficient liquidity, remaining={:.2}", remaining);
    None
}

// Sell token_y (inverted): 价格上行，消费 amount_x (bins with price >= anchor_price)
fn quote_x_out_for_y_in(state: &LbState, y_in_raw: f64, decimals_adj: f64, anchor_price: f64) -> Option<f64> {
    if y_in_raw <= EPSILON {
        return Some(0.0);
    }
    if state.bins.is_empty() {
        return None;
    }

    // 从 price >= anchor 开始，向高价遍历
    // If anchor_price is at or above last bin, start from last bin (fallback for clamped prices)
    let last_price = state.bins.last().map(|b| b.price).unwrap_or(f64::MAX);
    let mut idx = first_index_at_or_above(&state.bins, anchor_price);
    if idx >= state.bins.len() && anchor_price >= last_price - EPSILON * 1000.0 {
        // anchor_price was clamped to last_bin.price, start from last bin
        idx = state.bins.len() - 1;
        debug!("quote_x_out_for_y_in: anchor_price={:.9} at boundary, starting from last bin", anchor_price);
    }
    if idx >= state.bins.len() {
        debug!("quote_x_out_for_y_in: anchor_price={:.9} above all bins, no liquidity", anchor_price);
        return None;
    }
    let mut remaining = y_in_raw;
    let mut total_x = 0.0;
    let mut start_logged = false;

    while idx < state.bins.len() {
        let bin = &state.bins[idx];
        if bin.amount_x > EPSILON && bin.price > 0.0 {
            if !start_logged {
                debug!("quote_x_out_for_y_in: starting from bin_id={} price={:.9}", bin._bin_id, bin.price);
                start_logged = true;
            }
            let price_adj = bin.price * decimals_adj;
            if price_adj > EPSILON {
                let y_needed_full = bin.amount_x * price_adj;
                if remaining >= y_needed_full - EPSILON {
                    total_x += bin.amount_x;
                    remaining -= y_needed_full;
                } else {
                    total_x += remaining / price_adj;
                    remaining = 0.0;
                    break;
                }
            }
        }
        idx += 1;
    }

    if remaining <= EPSILON {
        Some(total_x)
    } else {
        debug!("quote_x_out_for_y_in: insufficient liquidity, remaining={:.2}", remaining);
        None
    }
}

// Sell token_x (normal): 价格下行，消费 amount_y (bins with price <= anchor_price)
fn quote_y_out_for_x_in(state: &LbState, x_in_raw: f64, decimals_adj: f64, anchor_price: f64) -> Option<f64> {
    if x_in_raw <= EPSILON {
        return Some(0.0);
    }
    if state.bins.is_empty() {
        return None;
    }

    let first_price = state.bins.first().map(|b| b.price).unwrap_or(0.0);
    let _last_price = state.bins.last().map(|b| b.price).unwrap_or(0.0);
    
    // Check if anchor_price is at the lower boundary (clamped because active_price < first_bin.price)
    let at_lower_boundary = anchor_price <= first_price + EPSILON * 1000.0;
    
    let mut remaining = x_in_raw;
    let mut total_y = 0.0;
    let mut start_logged = false;

    if at_lower_boundary {
        // When at lower boundary, traverse ALL bins from low to high (use all amount_y)
        debug!("quote_y_out_for_x_in: at lower boundary, using all bins");
        for bin in state.bins.iter() {
            if bin.amount_y > EPSILON && bin.price > 0.0 {
                if !start_logged {
                    debug!("quote_y_out_for_x_in: starting from bin_id={} price={:.9}", bin._bin_id, bin.price);
                    start_logged = true;
                }
                let price_adj = bin.price * decimals_adj;
                if price_adj > EPSILON {
                    let x_needed_full = bin.amount_y / price_adj;
                    if remaining >= x_needed_full - EPSILON {
                        total_y += bin.amount_y;
                        remaining -= x_needed_full;
                    } else {
                        total_y += remaining * price_adj;
                        remaining = 0.0;
                    }
                }
            }
            if remaining <= EPSILON {
                return Some(total_y);
            }
        }
    } else {
        // Normal case: traverse from anchor_price downward
        let mut idx_opt = last_index_at_or_below(&state.bins, anchor_price);
        if idx_opt.is_none() {
            debug!("quote_y_out_for_x_in: anchor_price={:.9} below all bins, no liquidity", anchor_price);
            return None;
        }

        while let Some(idx) = idx_opt {
            let bin = &state.bins[idx];
            if bin.amount_y > EPSILON && bin.price > 0.0 {
                if !start_logged {
                    debug!("quote_y_out_for_x_in: starting from bin_id={} price={:.9}", bin._bin_id, bin.price);
                    start_logged = true;
                }
                let price_adj = bin.price * decimals_adj;
                if price_adj > EPSILON {
                    let x_needed_full = bin.amount_y / price_adj;
                    if remaining >= x_needed_full - EPSILON {
                        total_y += bin.amount_y;
                        remaining -= x_needed_full;
                    } else {
                        total_y += remaining * price_adj;
                        remaining = 0.0;
                    }
                }
            }

            if remaining <= EPSILON {
                return Some(total_y);
            }
            if idx == 0 {
                break;
            }
            idx_opt = Some(idx - 1);
        }
    }

    if remaining <= EPSILON {
        Some(total_y)
    } else {
        debug!("quote_y_out_for_x_in: insufficient liquidity, remaining={:.2}", remaining);
        None
    }
}

// Buy token_y (inverted): 价格下行，消费 amount_y (bins with price <= anchor_price)
fn quote_x_in_for_y_out(state: &LbState, target_y_raw: f64, decimals_adj: f64, anchor_price: f64) -> Option<f64> {
    if target_y_raw <= EPSILON {
        return Some(0.0);
    }
    if state.bins.is_empty() {
        return None;
    }

    let first_price = state.bins.first().map(|b| b.price).unwrap_or(0.0);
    
    // Check if anchor_price is at the lower boundary (clamped because active_price < first_bin.price)
    let at_lower_boundary = anchor_price <= first_price + EPSILON * 1000.0;
    
    let mut remaining = target_y_raw;
    let mut total_x = 0.0;
    let mut start_logged = false;

    if at_lower_boundary {
        // When at lower boundary, traverse ALL bins from low to high (use all amount_y)
        debug!("quote_x_in_for_y_out: at lower boundary, using all bins");
        for bin in state.bins.iter() {
            if bin.amount_y > EPSILON && bin.price > 0.0 {
                if !start_logged {
                    debug!("quote_x_in_for_y_out: starting from bin_id={} price={:.9}", bin._bin_id, bin.price);
                    start_logged = true;
                }
                let y_take = remaining.min(bin.amount_y);
                let price_adj = bin.price * decimals_adj;
                if price_adj > EPSILON {
                    total_x += y_take / price_adj;
                    remaining -= y_take;
                    if remaining <= EPSILON {
                        return Some(total_x);
                    }
                }
            }
        }
    } else {
        // Normal case: traverse from anchor_price downward
        let mut idx_opt = last_index_at_or_below(&state.bins, anchor_price);
        if idx_opt.is_none() {
            debug!("quote_x_in_for_y_out: anchor_price={:.9} below all bins, no liquidity", anchor_price);
            return None;
        }

        while let Some(idx) = idx_opt {
            let bin = &state.bins[idx];
            if bin.amount_y > EPSILON && bin.price > 0.0 {
                if !start_logged {
                    debug!("quote_x_in_for_y_out: starting from bin_id={} price={:.9}", bin._bin_id, bin.price);
                    start_logged = true;
                }
                let y_take = remaining.min(bin.amount_y);
                let price_adj = bin.price * decimals_adj;
                if price_adj > EPSILON {
                    total_x += y_take / price_adj;
                    remaining -= y_take;
                    if remaining <= EPSILON {
                        return Some(total_x);
                    }
                }
            }

            if idx == 0 {
                break;
            }
            idx_opt = Some(idx - 1);
        }
    }

    if remaining <= EPSILON {
        Some(total_x)
    } else {
        debug!("quote_x_in_for_y_out: insufficient liquidity, remaining={:.2}", remaining);
        None
    }
}

fn first_index_at_or_above(bins: &[LbBin], price: f64) -> usize {
    let mut low = 0usize;
    let mut high = bins.len();
    while low < high {
        let mid = (low + high) / 2;
        if bins[mid].price + EPSILON < price {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low
}

fn last_index_at_or_below(bins: &[LbBin], price: f64) -> Option<usize> {
    if bins.is_empty() {
        return None;
    }
    let mut low = 0usize;
    let mut high = bins.len();
    while low < high {
        let mid = (low + high) / 2;
        if bins[mid].price <= price + EPSILON {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    if low == 0 {
        None
    } else {
        Some(low - 1)
    }
}
