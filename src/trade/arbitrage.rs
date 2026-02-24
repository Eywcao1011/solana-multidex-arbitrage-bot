use anyhow::{anyhow, bail, Result};
use solana_instruction::Instruction;

use crate::dex::{Opportunity, TradeSide};

use super::{executor_for, TradeAccounts, TradePlan, TradeRequest};

#[derive(Debug, Clone)]
pub struct AtomicTradePlan {
    pub buy_plan: TradePlan,
    pub sell_plan: TradePlan,
    pub instructions: Vec<Instruction>,
    pub description: String,
    pub net_quote_profit: f64,
    pub tip_lamports: u64,
    /// Pool address for buy leg (for error recovery/refresh)
    pub buy_pool: solana_pubkey::Pubkey,
    /// Pool address for sell leg (for error recovery/refresh)
    pub sell_pool: solana_pubkey::Pubkey,
    /// DEX type for buy leg (for ALT selection)
    pub buy_dex_kind: crate::DexKind,
    /// DEX type for sell leg (for ALT selection)
    pub sell_dex_kind: crate::DexKind,
    /// Buy leg expected input/output (normalized units, for simulate logs)
    pub buy_expected_quote_in: Option<f64>,
    pub buy_expected_base_out: Option<f64>,
    pub buy_expected_price: Option<f64>,
    /// Sell leg expected price (normalized units, for simulate logs)
    pub sell_expected_price: Option<f64>,
    pub buy_base_decimals: Option<u8>,
    pub buy_quote_decimals: Option<u8>,
}


/// Build atomic trade plan with exact-in semantics (spot_only).
///
/// - Buy leg: exact-in, spend fixed quote_amount_in, get at least min_base_out
/// - Sell leg: exact-in, sell min_base_out, get at least sell_min_out (covers cost + tip + base fee)
pub fn build_atomic_trade_plan(
    opportunity: &Opportunity,
    max_slippage_bps: f64,
    buy_accounts: TradeAccounts,
    sell_accounts: TradeAccounts,
) -> Result<AtomicTradePlan> {
    // ✅ 统一使用传入的 max_slippage_bps，避免计算和执行的不一致
    let slippage_bps = max_slippage_bps;

    // ✅ 卖出保护简化：只需本金 + tip，不再需要利润底线

    let dry_run = std::env::var("ARBITRAGE_DRY_RUN_ONLY")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    let static_tip_lamports = std::env::var("JITO_TIP_LAMPORTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);
    let tip_lamports = opportunity.tip_lamports.unwrap_or(static_tip_lamports);
    let tip_quote = tip_lamports as f64 / 1_000_000_000.0; // Convert to SOL

    let buy_executor = executor_for(opportunity.buy.descriptor.kind);
    let sell_executor = executor_for(opportunity.sell.descriptor.kind);

    // ✅ Spot-only mode: derive from ARBITRAGE_TRADE_SIZE_QUOTE + mid price
    // Buy leg: exact-in (固定花费 quote_amount_in)
    // Sell leg: exact-in (卖出 min_base_out) + sell_min_out 保护
    let trade_size_quote = std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| anyhow!("spot_only mode requires ARBITRAGE_TRADE_SIZE_QUOTE to be set"))?;

    if trade_size_quote <= 0.0 {
        bail!("ARBITRAGE_TRADE_SIZE_QUOTE must be positive");
    }

    let buy_price = opportunity.buy.normalized_price;
    let sell_price = opportunity.sell.normalized_price;

    if !(buy_price > 0.0 && buy_price.is_finite() && sell_price > 0.0 && sell_price.is_finite()) {
        bail!("invalid prices for spot_only trade: buy={} sell={}", buy_price, sell_price);
    }

    let (norm_base_decimals, norm_quote_decimals) = if opportunity.buy.normalized_pair.inverted {
        (opportunity.buy.quote_decimals, opportunity.buy.base_decimals)
    } else {
        (opportunity.buy.base_decimals, opportunity.buy.quote_decimals)
    };

    // Step 1: 买入腿 - 固定花费 trade_size_quote，仅按价格估算买到多少 base
    let expected_buy_base = trade_size_quote / buy_price;
    // 考虑买入滑点后，至少买到的 base 数量
    let min_base_out = expected_buy_base * (1.0 - slippage_bps / 10_000.0);

    // Step 2: 保护下限 - 本金 + tip + 基础交易费（不考虑利润底线）
    let base_fee_lamports = std::env::var("BASE_TX_FEE_LAMPORTS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(5_000);
    let base_fee_quote = base_fee_lamports as f64 / 1_000_000_000.0;
    let min_quote_unit = 10f64.powi(-(norm_quote_decimals as i32));
    let sell_min_out = if dry_run {
        min_quote_unit
    } else {
        (trade_size_quote + tip_quote + base_fee_quote).max(min_quote_unit)
    };

    // 不做本地收益预估
    let est_profit = 0.0;

    if dry_run {
        log::info!(
            "Spot-only exact-in (dry-run): quote_in={:.6} SOL | min_base_out={:.6} | sell_min_out={:.12} SOL",
            trade_size_quote,
            min_base_out,
            sell_min_out
        );
    } else {
        log::info!(
            "Spot-only exact-in: quote_in={:.6} SOL | min_base_out={:.6} | sell_min_out={:.6} SOL (本金+tip+base_fee={:.6})",
            trade_size_quote,
            min_base_out,
            sell_min_out,
            base_fee_quote
        );
    }

    let buy_request = TradeRequest {
        side: TradeSide::Buy,
        base_amount: min_base_out, // For executor reference (actual input is quote_amount_in)
        max_slippage_bps,
        min_amount_out: Some(min_base_out), // 至少买到这么多 base
        quote_amount_in: Some(trade_size_quote), // 固定花费这么多 quote
        accounts: buy_accounts,
    };

    let sell_request = TradeRequest {
        side: TradeSide::Sell,
        base_amount: min_base_out, // 卖出这么多 base
        max_slippage_bps,
        min_amount_out: Some(sell_min_out), // 至少拿回这么多 quote（基于 required_min_out）
        quote_amount_in: None,
        accounts: sell_accounts,
    };

    let base_amount = min_base_out;

    let buy_plan = buy_executor
        .prepare_swap(&opportunity.buy, &buy_request)
        .map_err(|e| {
            anyhow!(
                "leg=BUY prepare_swap failed: pool={} {} ({}) error={}",
                opportunity.buy.descriptor.kind,
                opportunity.buy.descriptor.label,
                opportunity.buy.descriptor.address,
                e
            )
        })?;
    let sell_plan = sell_executor
        .prepare_swap(&opportunity.sell, &sell_request)
        .map_err(|e| {
            anyhow!(
                "leg=SELL prepare_swap failed: pool={} {} ({}) error={}",
                opportunity.sell.descriptor.kind,
                opportunity.sell.descriptor.label,
                opportunity.sell.descriptor.address,
                e
            )
        })?;

    let mut instructions = buy_plan.instructions.clone();
    instructions.extend(sell_plan.instructions.clone());

    // Trade flow: quote(SOL) -> base(MEME) -> quote(SOL)
    let description = format!(
        "Atomic arb: {} via {} -> {}; MEME {:.6}; est PnL {:.6} SOL",
        opportunity.pair.base,
        opportunity.buy.descriptor.label,
        opportunity.sell.descriptor.label,
        base_amount,
        est_profit,
    );

    Ok(AtomicTradePlan {
        buy_plan,
        sell_plan,
        instructions,
        description,
        net_quote_profit: est_profit,
        tip_lamports,
        buy_pool: opportunity.buy.descriptor.address,
        sell_pool: opportunity.sell.descriptor.address,
        buy_dex_kind: opportunity.buy.descriptor.kind,
        sell_dex_kind: opportunity.sell.descriptor.kind,
        buy_expected_quote_in: Some(trade_size_quote),
        buy_expected_base_out: Some(expected_buy_base),
        buy_expected_price: Some(buy_price),
        sell_expected_price: Some(sell_price),
        buy_base_decimals: Some(norm_base_decimals),
        buy_quote_decimals: Some(norm_quote_decimals),
    })
}
