use std::{fmt, str::FromStr, sync::Arc};

use anyhow::{anyhow, bail, Result};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use log::debug;
use once_cell::sync::Lazy;
use serde::Serialize;
use solana_account_decoder::UiAccount;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;

use crate::rpc::AppContext;

/// SOL (wrapped) mint address - used to force SOL as wallet asset in pairs
pub static SOL_MINT: Lazy<Pubkey> = Lazy::new(|| {
    Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap()
});

pub mod clmm;

#[derive(Debug, Clone)]
pub struct ClmmAccounts {
    pub program_id: Pubkey,
    pub amm_config: Pubkey,
    pub tick_spacing: i32,
    pub tick_current_index: i32,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub observation_key: Pubkey,
    /// Token program for token A (spl_token::ID or spl_token_2022::ID)
    pub token_a_program: Pubkey,
    /// Token program for token B (spl_token::ID or spl_token_2022::ID)
    pub token_b_program: Pubkey,
}

#[derive(Debug, Clone)]
pub struct DammAccounts {
    pub program_id: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    /// Token program for token A (spl_token::ID or spl_token_2022::ID)
    pub token_a_program: Pubkey,
    /// Token program for token B (spl_token::ID or spl_token_2022::ID)
    pub token_b_program: Pubkey,
    
    // ===== DAMM V1 only fields (None for V2) =====
    /// Mercurial Vault A account (owns the token vault)
    pub a_vault: Option<Pubkey>,
    /// Mercurial Vault B account (owns the token vault)
    pub b_vault: Option<Pubkey>,
    /// Pool's LP token account in Vault A
    pub a_vault_lp: Option<Pubkey>,
    /// Pool's LP token account in Vault B
    pub b_vault_lp: Option<Pubkey>,
    /// LP token mint of Vault A
    pub a_vault_lp_mint: Option<Pubkey>,
    /// LP token mint of Vault B
    pub b_vault_lp_mint: Option<Pubkey>,
    /// Protocol fee token account for token A
    pub protocol_token_a_fee: Option<Pubkey>,
    /// Protocol fee token account for token B
    pub protocol_token_b_fee: Option<Pubkey>,
}


pub mod meteora_damm;
pub mod meteora_dlmm;
pub mod meteora_lb;
pub mod opportunity_detector; // ✅ 新的优化检测器
pub mod orca;
pub mod pump_fun;
pub mod raydium;
pub mod raydium_amm;
pub mod raydium_cpmm;


#[derive(Debug, Clone, Default)]
pub struct PoolFees {
    pub lp_fee_bps: f64,
    pub protocol_fee_bps: f64,
    pub other_fee_bps: f64,
    /// Meteora DLMM/LB dynamic fee parameters (optional)
    pub meteora_dynamic: Option<MeteoraDynamicFeeParams>,
}

/// Meteora DLMM/LB dynamic fee parameters for calculating variable fee
#[derive(Debug, Clone, Default)]
pub struct MeteoraDynamicFeeParams {
    pub bin_step: u16,
    pub base_factor: u16,
    pub variable_fee_control: u32,
    pub volatility_accumulator: u32,
    pub volatility_reference: u32,
}

impl MeteoraDynamicFeeParams {
    /// Meteora constants for fee calculation
    const FEE_PRECISION: f64 = 1_000_000_000.0; // 1e9
    const SCALE: f64 = 100_000_000_000.0; // 1e11

    /// Calculate the base fee ratio (not bps, but ratio like 0.001 = 0.1%)
    pub fn base_fee_ratio(&self) -> f64 {
        (self.bin_step as f64) * (self.base_factor as f64) / 10_000_000_000.0
    }

    /// Calculate the variable (dynamic) fee ratio based on volatility
    /// Formula: ((volatility_accumulator * bin_step)^2 * variable_fee_control) / SCALE^2
    pub fn variable_fee_ratio(&self) -> f64 {
        if self.variable_fee_control == 0 || self.volatility_accumulator == 0 {
            return 0.0;
        }
        
        // v_a * bin_step
        let va_times_step = (self.volatility_accumulator as f64) * (self.bin_step as f64);
        
        // ((v_a * bin_step)^2 * variable_fee_control) / (FEE_PRECISION * SCALE)
        let var_fee = (va_times_step * va_times_step * (self.variable_fee_control as f64))
            / (Self::FEE_PRECISION * Self::SCALE);
        
        var_fee.min(0.10) // Cap at 10% max
    }

    /// Total fee ratio (base + variable)
    pub fn total_fee_ratio(&self) -> f64 {
        self.base_fee_ratio() + self.variable_fee_ratio()
    }
}

impl PoolFees {
    pub fn total_bps(&self) -> f64 {
        self.lp_fee_bps + self.protocol_fee_bps + self.other_fee_bps
    }

    pub fn total_ratio(&self) -> f64 {
        self.total_bps() / 10_000.0
    }

    /// Get base fee ratio (for Meteora, uses dynamic params; otherwise falls back to total_ratio)
    pub fn base_fee_ratio(&self) -> f64 {
        if let Some(ref dyn_params) = self.meteora_dynamic {
            dyn_params.base_fee_ratio()
        } else {
            self.total_ratio()
        }
    }

    /// Get variable (dynamic) fee ratio (only for Meteora with dynamic params)
    pub fn variable_fee_ratio(&self) -> f64 {
        if let Some(ref dyn_params) = self.meteora_dynamic {
            dyn_params.variable_fee_ratio()
        } else {
            0.0
        }
    }

    /// Get total fee ratio including dynamic fee (for Meteora)
    pub fn total_fee_ratio_with_dynamic(&self) -> f64 {
        if let Some(ref dyn_params) = self.meteora_dynamic {
            dyn_params.total_fee_ratio()
        } else {
            self.total_ratio()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
pub enum DexKind {
    OrcaWhirlpool,
    RaydiumClmm,
    RaydiumCpmm,
    RaydiumAmmV4,
    MeteoraDammV1,
    MeteoraDammV2,
    MeteoraLb,
    MeteoraDlmm,
    PumpFunDlmm,
}

impl fmt::Display for DexKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DexKind::OrcaWhirlpool => write!(f, "Orca Whirlpool"),
            DexKind::RaydiumClmm => write!(f, "Raydium CLMM"),
            DexKind::RaydiumCpmm => write!(f, "Raydium CPMM"),
            DexKind::RaydiumAmmV4 => write!(f, "Raydium AMM V4"),
            DexKind::MeteoraDammV1 => write!(f, "Meteora DAMM V1"),
            DexKind::MeteoraDammV2 => write!(f, "Meteora DAMM V2"),
            DexKind::MeteoraLb => write!(f, "Meteora LB"),
            DexKind::MeteoraDlmm => write!(f, "Meteora DLMM"),
            DexKind::PumpFunDlmm => write!(f, "Pump.fun DLMM"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolDescriptor {
    pub label: String,
    pub address: Pubkey,
    pub kind: DexKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NormalizedPair {
    pub base: Pubkey,
    pub quote: Pubkey,
    pub inverted: bool,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PoolSnapshot {
    pub descriptor: PoolDescriptor,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub price: f64,
    pub sqrt_price_x64: u128,
    pub liquidity: Option<u128>,
    pub base_reserve: Option<u128>,
    pub quote_reserve: Option<u128>,
    pub fees: Option<PoolFees>,
    pub slot: u64,
    pub context: RpcResponseContext,
    pub timestamp: DateTime<Utc>,
    pub normalized_pair: NormalizedPair,
    pub normalized_price: f64,
    pub clmm_state: Option<clmm::ClmmState>,
    pub clmm_accounts: Option<ClmmAccounts>,
    pub damm_state: Option<meteora_damm::DammState>,
    pub damm_accounts: Option<DammAccounts>,
    pub lb_state: Option<meteora_lb::LbState>,
    pub lb_accounts: Option<meteora_lb::LbAccounts>,
    pub pump_state: Option<pump_fun::PumpFunState>,
    /// 依赖账户列表：vaults、tick arrays、bin arrays 等需要自动订阅的账户
    pub dependent_accounts: Vec<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct PriceThresholds {
    pub alert_bps: f64,

    // ✅ Base 币模式（原有）
    pub trade_size: Option<f64>,
    pub trade_min: Option<f64>,
    pub trade_max: Option<f64>,

    // ✅ Quote 币模式（新增）
    pub trade_size_quote: Option<f64>,
    pub trade_min_quote: Option<f64>,
    pub trade_max_quote: Option<f64>,

    #[allow(dead_code)]
    pub execution_enabled: bool,
}

impl Default for PriceThresholds {
    fn default() -> Self {
        Self {
            alert_bps: 5.0,
            trade_size: None,
            trade_min: None,
            trade_max: None,
            trade_size_quote: None,
            trade_min_quote: None,
            trade_max_quote: None,
            execution_enabled: false,
        }
    }
}

impl PriceThresholds {
    /// 解析交易规模：优先使用 Quote 模式，使用 **实际** 买入价格（quote/base）换算成 Base 数量。
    ///
    /// 当 `trade_size_quote` 设置为 SOL 时，`buy_price_quote_per_base` 必须是 “需要多少 SOL 才能买入一个 base token”。
    pub fn resolve_trade_size(&self, buy_price_quote_per_base: f64) -> Option<f64> {
        if buy_price_quote_per_base <= 0.0 {
            return self.trade_size;
        }

        // ✅ 优先使用 Quote 模式
        if let Some(quote_amount) = self.trade_size_quote {
            Some(quote_amount / buy_price_quote_per_base)
        } else {
            self.trade_size
        }
    }

    /// 解析交易范围：优先使用 Quote 模式，使用 **实际** 买入价格（quote/base）换算成 Base 范围。
    pub fn resolve_trade_range(
        &self,
        buy_price_quote_per_base: f64,
    ) -> (Option<f64>, Option<f64>) {
        if buy_price_quote_per_base <= 0.0 {
            return (self.trade_min, self.trade_max);
        }

        // ✅ 优先使用 Quote 模式
        match (self.trade_min_quote, self.trade_max_quote) {
            (Some(min_quote), Some(max_quote)) => (
                Some(min_quote / buy_price_quote_per_base),
                Some(max_quote / buy_price_quote_per_base),
            ),
            _ => (self.trade_min, self.trade_max),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct TradeQuote {
    pub base_amount: f64,
    pub quote_cost: f64,
    pub quote_proceeds: f64,
    pub effective_price: f64,
    pub fee_in_input: f64,
}

#[derive(Debug, Clone)]
pub struct TradeEstimates {
    pub base_amount: f64,
    pub buy_quote_cost: f64,
    pub sell_quote_proceeds: f64,
    pub net_quote_profit: f64,
    pub buy_price: f64,
    pub sell_price: f64,
    pub buy_fee: f64,
    pub sell_fee: f64,
}

#[derive(Debug, Clone)]
pub struct Opportunity {
    pub pair: NormalizedPair,
    pub buy: PoolSnapshot,
    pub sell: PoolSnapshot,
    pub spread_pct: f64,
    pub trade: Option<TradeEstimates>,
    pub tip_lamports: Option<u64>,
}

pub type SnapshotStore = Arc<DashMap<Pubkey, PoolSnapshot>>;

pub fn detect_opportunity(
    latest: &PoolSnapshot,
    store: &SnapshotStore,
    thresholds: &PriceThresholds,
) -> Option<Opportunity> {
    #[derive(Clone)]
    struct Candidate {
        snapshot: PoolSnapshot,
        price: f64,
    }

    // ✅ 修复：使用 normalized_price 进行动态价格换算
    // 首先尝试用当前快照的规范化价格作为参考
    let reference_price = latest.normalized_price;
    let mut best_buy: Option<Candidate> = None;
    let mut best_sell: Option<Candidate> = None;

    // 第一轮：使用当前参考价格进行初步选择
    // ⚠️ 声明为 mut，后续会根据最终买入池价格无条件重算
    let mut fixed_trade_amount = thresholds
        .resolve_trade_size(reference_price)
        .filter(|value| value.is_finite() && *value > 0.0);

    let mut selection_amount = fixed_trade_amount.or_else(|| {
        thresholds
            .resolve_trade_range(reference_price)
            .0
            .filter(|value| value.is_finite() && *value > 0.0)
    });

    for entry in store.iter() {
        let snapshot = entry.value();
        if snapshot.normalized_pair.base != latest.normalized_pair.base
            || snapshot.normalized_pair.quote != latest.normalized_pair.quote
        {
            continue;
        }

        // 买入选择：基于维护的快照价格
        if let Some((price, _quote)) =
            price_for(snapshot, selection_amount, TradeSide::Buy)
        {
            let replace = match &best_buy {
                Some(current) => price < current.price,
                None => true,
            };
            if replace {
                best_buy = Some(Candidate {
                    snapshot: snapshot.clone(),
                    price,
                });
            }
        }

        // 卖出选择：基于维护的快照价格
        if let Some((price, _quote)) =
            price_for(snapshot, selection_amount, TradeSide::Sell)
        {
            let replace = match &best_sell {
                Some(current) => price > current.price,
                None => true,
            };
            if replace {
                best_sell = Some(Candidate {
                    snapshot: snapshot.clone(),
                    price,
                });
            }
        }
    }

    let (mut best_buy, mut best_sell) = match (best_buy, best_sell) {
        (Some(buy), Some(sell))
            if buy.snapshot.descriptor.address != sell.snapshot.descriptor.address =>
        {
            (buy, sell)
        }
        _ => return None,
    };

    // ⚠️ 关键修复：无条件检查最终买入池价格是否与初始参考价格不同
    // 如果不同，必须用买入池价格重新计算交易量并重新评估所有池子
    // 这样可以确保：
    // 1. 即使第一个池子就是最优的（DashMap 迭代顺序不固定），也能正确重算
    // 2. Quote 模式下的交易量基于正确的买入池价格
    // 3. 最终的 trade 估算使用匹配的价格和数量
    let buy_price = best_buy.snapshot.normalized_price;
    let price_tolerance = 1e-9;

    if (buy_price - reference_price).abs() > price_tolerance {
        // 用最终买入池价格重新计算交易量
        fixed_trade_amount = thresholds
            .resolve_trade_size(buy_price)
            .filter(|value| value.is_finite() && *value > 0.0);

        selection_amount = fixed_trade_amount.or_else(|| {
            thresholds
                .resolve_trade_range(buy_price)
                .0
                .filter(|value| value.is_finite() && *value > 0.0)
        });

        // 重新遍历所有池子，用新的 selection_amount 重新比较
        let mut new_best_buy: Option<Candidate> = None;
        let mut new_best_sell: Option<Candidate> = None;

        for entry in store.iter() {
            let snapshot = entry.value();
            if snapshot.normalized_pair.base != latest.normalized_pair.base
                || snapshot.normalized_pair.quote != latest.normalized_pair.quote
            {
                continue;
            }

            // 重新计算买入选择
            if let Some((price, _quote)) =
                price_for(snapshot, selection_amount, TradeSide::Buy)
            {
                let replace = match &new_best_buy {
                    Some(current) => price < current.price,
                    None => true,
                };
                if replace {
                    new_best_buy = Some(Candidate {
                        snapshot: snapshot.clone(),
                        price,
                    });
                }
            }

            // 重新计算卖出选择
            if let Some((price, _quote)) =
                price_for(snapshot, selection_amount, TradeSide::Sell)
            {
                let replace = match &new_best_sell {
                    Some(current) => price > current.price,
                    None => true,
                };
                if replace {
                    new_best_sell = Some(Candidate {
                        snapshot: snapshot.clone(),
                        price,
                    });
                }
            }
        }

        // 使用重新计算的结果，如果有效的话
        if let Some(new_buy) = new_best_buy {
            best_buy = new_buy;
        }
        if let Some(new_sell) = new_best_sell {
            best_sell = new_sell;
        }

        // 重新检查是否是同一个池子
        if best_buy.snapshot.descriptor.address == best_sell.snapshot.descriptor.address {
            return None;
        }
    }

    if best_buy.price <= 0.0 || best_sell.price <= 0.0 {
        debug!("Skipping opportunity due to non-positive price");
        return None;
    }

    let spread = best_sell.price / best_buy.price - 1.0;

    let trade = None;

    Some(Opportunity {
        pair: latest.normalized_pair,
        buy: best_buy.snapshot,
        sell: best_sell.snapshot,
        spread_pct: spread * 100.0,
        trade,
        tip_lamports: None,
    })
}

/// 统一的 simulate_trade 分发函数：根据 DexKind 调用对应模块的 simulate_trade
/// 返回包含真实滑点和手续费的 TradeQuote
pub fn simulate_trade(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    match snapshot.descriptor.kind {
        DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm => {
            raydium_amm::simulate_trade(snapshot, side, base_amount)
        }
        DexKind::MeteoraDammV1 | DexKind::MeteoraDammV2 => {
            meteora_damm::simulate_trade(snapshot, side, base_amount)
        }
        DexKind::MeteoraLb => meteora_lb::simulate_trade(snapshot, side, base_amount),
        DexKind::MeteoraDlmm => meteora_dlmm::simulate_trade(snapshot, side, base_amount),
        DexKind::PumpFunDlmm => pump_fun::simulate_trade(snapshot, side, base_amount),
        DexKind::OrcaWhirlpool | DexKind::RaydiumClmm => {
            // CLMM 池：优先使用精确模拟（如果有 clmm_state），否则 fallback 到常数积
            clmm::simulate_trade(snapshot, side, base_amount)
        }
    }
}

fn price_for(
    snapshot: &PoolSnapshot,
    trade_amount: Option<f64>,
    side: TradeSide,
) -> Option<(f64, Option<TradeQuote>)> {
    // 如果有交易金额，使用 simulate_trade 获取真实滑点价格
    if let Some(amount) = trade_amount {
        if amount > 0.0 && amount.is_finite() {
            if let Some(quote) = simulate_trade(snapshot, side, amount) {
                // 使用模拟得到的 effective_price（含滑点）
                return Some((quote.effective_price, Some(quote)));
            }
        }
    }
    // Fallback：没有交易金额或模拟失败时，使用 normalized_price
    Some((snapshot.normalized_price, None))
}

/// Normalize a trading pair for consistent ordering.
/// 
/// For SOL-containing pairs: SOL is always the "quote" (wallet asset).
/// This ensures the trade path is always SOL → MEME → SOL.
/// 
/// For non-SOL pairs: use lexicographic ordering of mint addresses.
/// 
/// The `inverted` flag indicates whether the original base/quote were swapped.
pub fn normalize_pair(base: Pubkey, quote: Pubkey, price: f64) -> Option<(NormalizedPair, f64)> {
    if price <= 0.0 {
        return None;
    }
    if base == quote {
        return None;
    }
    
    let sol = *SOL_MINT;
    
    // Case 1: quote is SOL - already correct (MEME/SOL), no swap needed
    if quote == sol {
        return Some((
            NormalizedPair {
                base,
                quote,
                inverted: false,
            },
            price,
        ));
    }
    
    // Case 2: base is SOL - swap to make SOL the quote (MEME/SOL)
    // Original: SOL/MEME with price P (how many MEME per SOL)
    // After swap: MEME/SOL with price 1/P (how many SOL per MEME)
    if base == sol {
        return Some((
            NormalizedPair {
                base: quote,  // MEME becomes base
                quote: base,  // SOL becomes quote
                inverted: true,
            },
            1.0 / price,
        ));
    }
    
    // Case 3: Neither is SOL - use lexicographic ordering (original behavior)
    if base < quote {
        Some((
            NormalizedPair {
                base,
                quote,
                inverted: false,
            },
            price,
        ))
    } else {
        Some((
            NormalizedPair {
                base: quote,
                quote: base,
                inverted: true,
            },
            1.0 / price,
        ))
    }
}

pub async fn snapshot_from_update(
    descriptor: &PoolDescriptor,
    account: &UiAccount,
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    let data = account
        .data
        .decode()
        .ok_or_else(|| anyhow!("Account data missing binary payload"))?;

    snapshot_from_account_data(descriptor, data, context, ctx).await
}

#[allow(dead_code)]
pub async fn snapshot_from_account_data(
    descriptor: &PoolDescriptor,
    data: Vec<u8>,
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    let mut snapshot = match descriptor.kind {
        DexKind::OrcaWhirlpool => orca::decode(descriptor, &data, context, ctx).await,
        DexKind::RaydiumClmm => raydium::decode(descriptor, &data, context, ctx).await,
        DexKind::RaydiumCpmm => raydium_cpmm::decode(descriptor, &data, context, ctx).await,
        DexKind::MeteoraDammV1 | DexKind::MeteoraDammV2 => {
            meteora_damm::decode(descriptor, &data, context, ctx).await
        }
        DexKind::MeteoraLb => meteora_lb::decode(descriptor, &data, context, ctx).await,
        DexKind::MeteoraDlmm => meteora_dlmm::decode(descriptor, &data, context, ctx).await,
        DexKind::PumpFunDlmm => pump_fun::decode(descriptor, &data, context, ctx).await,
        DexKind::RaydiumAmmV4 => raydium_amm::decode(descriptor, &data, context, ctx).await,
    }?;

    if let Some(fees) = ctx.get_pool_fee_override(&descriptor.address) {
        snapshot.fees = Some(fees);
    }

    Ok(snapshot)
}

pub struct AccountReader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> AccountReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    pub fn skip(&mut self, bytes: usize) -> Result<()> {
        self.ensure(bytes)?;
        self.offset += bytes;
        Ok(())
    }

    pub fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.ensure(len)?;
        let start = self.offset;
        self.offset += len;
        Ok(&self.data[start..self.offset])
    }

    pub fn read_pubkey(&mut self) -> Result<Pubkey> {
        let bytes = self.read_bytes(32)?;
        let array: [u8; 32] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to pubkey array"))?;
        Ok(Pubkey::new_from_array(array))
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_bytes(1)?[0])
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes(2)?;
        let array: [u8; 2] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to u16 array"))?;
        Ok(u16::from_le_bytes(array))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        let array: [u8; 4] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to u32 array"))?;
        Ok(u32::from_le_bytes(array))
    }

    pub fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_bytes(8)?;
        let array: [u8; 8] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to u64 array"))?;
        Ok(u64::from_le_bytes(array))
    }

    pub fn read_i32(&mut self) -> Result<i32> {
        let bytes = self.read_bytes(4)?;
        let array: [u8; 4] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to i32 array"))?;
        Ok(i32::from_le_bytes(array))
    }

    pub fn read_i64(&mut self) -> Result<i64> {
        let bytes = self.read_bytes(8)?;
        let array: [u8; 8] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to i64 array"))?;
        Ok(i64::from_le_bytes(array))
    }

    pub fn read_u128(&mut self) -> Result<u128> {
        let bytes = self.read_bytes(16)?;
        let array: [u8; 16] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to u128 array"))?;
        Ok(u128::from_le_bytes(array))
    }

    pub fn read_i128(&mut self) -> Result<i128> {
        let bytes = self.read_bytes(16)?;
        let array: [u8; 16] = bytes
            .try_into()
            .map_err(|_| anyhow!("Failed to convert bytes to i128 array"))?;
        Ok(i128::from_le_bytes(array))
    }

    /// Returns the number of unread bytes remaining in the buffer
    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    fn ensure(&self, len: usize) -> Result<()> {
        if self.offset + len > self.data.len() {
            bail!("Unexpected end of account data at offset {}", self.offset);
        }
        Ok(())
    }
}

#[allow(dead_code)]
trait UiAccountDataExt {
    fn decode(&self) -> Option<Vec<u8>>;
}

impl UiAccountDataExt for solana_account_decoder::UiAccountData {
    fn decode(&self) -> Option<Vec<u8>> {
        match self {
            solana_account_decoder::UiAccountData::Binary(data, encoding) => match encoding {
                solana_account_decoder::UiAccountEncoding::Base64 => {
                    BASE64_STANDARD.decode(data).ok()
                }
                solana_account_decoder::UiAccountEncoding::Base58 => {
                    bs58::decode(data).into_vec().ok()
                }
                _ => None,
            },
            _ => None,
        }
    }
}

/// 收集池子交易所需的静态账户（用于 ALT 填充）
///
/// 返回该池子 swap 指令需要的所有静态账户（不包括动态的 tick/bin arrays）
pub fn collect_static_accounts_for_pool(snapshot: &PoolSnapshot) -> Vec<Pubkey> {
    use crate::constants::{
        METEORA_LB_PROGRAM_ID, ORCA_WHIRLPOOL_PROGRAM_ID, PUMP_FUN_PROGRAM_ID,
        RAYDIUM_CLMM_PROGRAM_ID, RAYDIUM_CPMM_PROGRAM_ID,
    };

    let mut accounts = Vec::new();

    // 1. 通用账户（所有 DEX 都需要）
    accounts.push(spl_token::ID);
    accounts.push(solana_pubkey::pubkey!("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")); // Token-2022
    accounts.push(solana_system_interface::program::ID);
    accounts.push(solana_pubkey::pubkey!("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")); // ATA

    // 2. 池子地址
    accounts.push(snapshot.descriptor.address);

    // 3. Token mints
    accounts.push(snapshot.base_mint);
    accounts.push(snapshot.quote_mint);

    // 4. DEX 特定账户
    match snapshot.descriptor.kind {
        DexKind::MeteoraDlmm | DexKind::MeteoraLb => {
            accounts.push(METEORA_LB_PROGRAM_ID);
            // event_authority (固定 PDA)
            let event_authority =
                Pubkey::find_program_address(&[b"__event_authority"], &METEORA_LB_PROGRAM_ID).0;
            accounts.push(event_authority);
            
            // ✅ bitmap_extension PDA（DLMM/LB 交易需要）
            let (bitmap_extension, _) = Pubkey::find_program_address(
                &[b"bitmap_extension", snapshot.descriptor.address.as_ref()],
                &METEORA_LB_PROGRAM_ID,
            );
            accounts.push(bitmap_extension);
            
            // ✅ Rent sysvar（LB swap 必需）
            accounts.push(solana_sysvar::rent::ID);

            // LB 账户（如果有）
            if let Some(ref lb) = snapshot.lb_accounts {
                accounts.push(lb.reserve_x);
                accounts.push(lb.reserve_y);
                accounts.push(lb.oracle);
            }
        }

        DexKind::RaydiumClmm => {
            accounts.push(RAYDIUM_CLMM_PROGRAM_ID);
            if let Some(ref clmm) = snapshot.clmm_accounts {
                accounts.push(clmm.amm_config);
                accounts.push(clmm.token_vault_a);
                accounts.push(clmm.token_vault_b);
            }
            // ✅ observation_state（交易需要）
            let observation_state = snapshot
                .clmm_accounts
                .as_ref()
                .and_then(|clmm| {
                    if clmm.observation_key != Pubkey::default() {
                        Some(clmm.observation_key)
                    } else {
                        None
                    }
                })
                .unwrap_or_else(|| {
                    Pubkey::find_program_address(
                        &[b"observation", snapshot.descriptor.address.as_ref()],
                        &RAYDIUM_CLMM_PROGRAM_ID,
                    )
                    .0
                });
            accounts.push(observation_state);
        }

        DexKind::OrcaWhirlpool => {
            accounts.push(ORCA_WHIRLPOOL_PROGRAM_ID);
            if let Some(ref clmm) = snapshot.clmm_accounts {
                accounts.push(clmm.token_vault_a);
                accounts.push(clmm.token_vault_b);
            }
            // ✅ oracle PDA（交易需要）
            let (oracle, _) = Pubkey::find_program_address(
                &[b"oracle", snapshot.descriptor.address.as_ref()],
                &ORCA_WHIRLPOOL_PROGRAM_ID,
            );
            accounts.push(oracle);
        }

        DexKind::RaydiumCpmm => {
            accounts.push(RAYDIUM_CPMM_PROGRAM_ID);
            
            // ✅ authority PDA（AUTH_SEED）
            let (authority, _) = Pubkey::find_program_address(
                &[b"vault_and_lp_mint_auth_seed"],
                &RAYDIUM_CPMM_PROGRAM_ID,
            );
            accounts.push(authority);
            
            if let Some(ref cpmm_info) = crate::dex::raydium_cpmm::get_static_info(&snapshot.descriptor.address) {
                accounts.push(cpmm_info.amm_config);
                accounts.push(cpmm_info.token_0_vault);
                accounts.push(cpmm_info.token_1_vault);
                // ✅ observation_key
                accounts.push(cpmm_info.observation_key);
            } else if let Some(ref clmm) = snapshot.clmm_accounts {
                accounts.push(clmm.amm_config);
                accounts.push(clmm.token_vault_a);
                accounts.push(clmm.token_vault_b);
            }
        }

        DexKind::RaydiumAmmV4 => {
            // Raydium AMM V4 程序 ID
            let amm_program = solana_pubkey::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
            accounts.push(amm_program);
            accounts.push(solana_pubkey::pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX")); // Serum
            
            // ✅ AMM authority PDA
            let (amm_authority, _) = Pubkey::find_program_address(&[b"amm authority"], &amm_program);
            accounts.push(amm_authority);
            
            // ✅ 从缓存获取 Serum 市场账户
            if let Some(ref amm_info) = crate::dex::raydium_amm::get_static_info(&snapshot.descriptor.address) {
                accounts.push(amm_info.base_vault);
                accounts.push(amm_info.quote_vault);
                accounts.push(amm_info.open_orders);
                accounts.push(amm_info.target_orders);
                accounts.push(amm_info.market_id);
                accounts.push(amm_info.market_program_id);
                // Serum 市场账户
                if amm_info.serum_bids != Pubkey::default() {
                    accounts.push(amm_info.serum_bids);
                }
                if amm_info.serum_asks != Pubkey::default() {
                    accounts.push(amm_info.serum_asks);
                }
                if amm_info.serum_event_queue != Pubkey::default() {
                    accounts.push(amm_info.serum_event_queue);
                }
                if amm_info.serum_coin_vault != Pubkey::default() {
                    accounts.push(amm_info.serum_coin_vault);
                }
                if amm_info.serum_pc_vault != Pubkey::default() {
                    accounts.push(amm_info.serum_pc_vault);
                }
                if amm_info.serum_vault_signer != Pubkey::default() {
                    accounts.push(amm_info.serum_vault_signer);
                }
            } else if let Some(ref damm) = snapshot.damm_accounts {
                accounts.push(damm.token_vault_a);
                accounts.push(damm.token_vault_b);
            }
        }

        DexKind::PumpFunDlmm => {
            accounts.push(PUMP_FUN_PROGRAM_ID);
            // global_config (固定 PDA)
            let (global_config, _) = Pubkey::find_program_address(
                &[crate::constants::PUMP_FUN_GLOBAL_CONFIG_SEED],
                &PUMP_FUN_PROGRAM_ID,
            );
            accounts.push(global_config);
            // event_authority (固定 PDA)
            let event_authority =
                Pubkey::find_program_address(&[b"__event_authority"], &PUMP_FUN_PROGRAM_ID).0;
            accounts.push(event_authority);
            // fee_program
            let fee_program = solana_pubkey::pubkey!("pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ");
            accounts.push(fee_program);
            
            // ✅ fee_config PDA（使用与 trade/pump_fun.rs 一致的种子）
            // 种子: [b"fee_config", FEE_CONFIG_STATIC_SEED]
            const FEE_CONFIG_STATIC_SEED: [u8; 32] = [
                12, 20, 222, 252, 130, 94, 198, 118, 148, 37, 8, 24, 187, 101, 64, 101, 244, 41, 141, 49, 86,
                213, 113, 180, 212, 248, 9, 12, 24, 233, 168, 99,
            ];
            let (fee_config, _) = Pubkey::find_program_address(
                &[b"fee_config", &FEE_CONFIG_STATIC_SEED],
                &fee_program,
            );
            accounts.push(fee_config);
            
            // ✅ global_volume PDA
            let (global_volume, _) = Pubkey::find_program_address(&[b"global_volume_accumulator"], &PUMP_FUN_PROGRAM_ID);
            accounts.push(global_volume);

            if let Some(ref pump) = snapshot.pump_state {
                accounts.push(pump.pool_base_token_account);
                accounts.push(pump.pool_quote_token_account);
                accounts.push(pump.protocol_fee_recipient);
                
                // ✅ coin_creator_vault_authority PDA（使用与 trade/pump_fun.rs 一致的种子）
                // 种子: [b"creator_vault", coin_creator.as_ref()]
                let (coin_creator_vault_authority, _) = Pubkey::find_program_address(
                    &[b"creator_vault", pump.coin_creator.as_ref()],
                    &PUMP_FUN_PROGRAM_ID,
                );
                accounts.push(coin_creator_vault_authority);
                
                // ✅ coin_creator_vault_ata（creator vault authority 的 quote token ATA）
                let coin_creator_vault_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                    &coin_creator_vault_authority,
                    &snapshot.quote_mint,
                    &pump.quote_token_program,
                );
                accounts.push(coin_creator_vault_ata);
                
                // ✅ protocol_fee_recipient_token_account（quote token ATA of protocol_fee_recipient）
                let protocol_fee_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                    &pump.protocol_fee_recipient,
                    &snapshot.quote_mint,
                    &pump.quote_token_program,
                );
                accounts.push(protocol_fee_ata);
            }
        }

        DexKind::MeteoraDammV1 | DexKind::MeteoraDammV2 => {
            use crate::constants::{METEORA_DAMM_V2_PROGRAM_ID, METEORA_DAMM_POOL_AUTHORITY};
            
            // ✅ DAMM V2 程序 ID 和权限账户
            accounts.push(METEORA_DAMM_V2_PROGRAM_ID);
            accounts.push(METEORA_DAMM_POOL_AUTHORITY);
            
            // ✅ DAMM V2 event_authority PDA
            let event_authority =
                Pubkey::find_program_address(&[b"__event_authority"], &METEORA_DAMM_V2_PROGRAM_ID).0;
            accounts.push(event_authority);
            
            if let Some(ref damm) = snapshot.damm_accounts {
                accounts.push(damm.token_vault_a);
                accounts.push(damm.token_vault_b);
            }
        }
    }

    // 去重
    accounts.sort();
    accounts.dedup();

    accounts
}
