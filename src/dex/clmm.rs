use std::{collections::BTreeMap, sync::Arc};

use log::debug;

use super::{PoolSnapshot, TradeQuote, TradeSide};

const PRICE_BASE: f64 = 1.0001;
const EPSILON: f64 = 1e-12;
const Q64: f64 = 18446744073709551616.0; // 2^64

#[derive(Debug, Clone)]
pub struct Tick {
    pub index: i32,
    pub liquidity_net: i128,
}

#[derive(Debug, Clone)]
pub struct ClmmState {
    pub tick_spacing: i32,
    pub tick_current_index: i32,
    tick_map: Arc<BTreeMap<i32, i128>>,
}

impl ClmmState {
    pub fn new(tick_spacing: i32, tick_current_index: i32, ticks: Vec<Tick>) -> Self {
        let mut map = BTreeMap::new();
        for tick in ticks {
            if tick.liquidity_net != 0 {
                map.insert(tick.index, tick.liquidity_net);
            }
        }
        Self {
            tick_spacing,
            tick_current_index,
            tick_map: Arc::new(map),
        }
    }

    fn tick_map(&self) -> Arc<BTreeMap<i32, i128>> {
        Arc::clone(&self.tick_map)
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

    if let Some(state) = &snapshot.clmm_state {
        if let Some(quote) = simulate_trade_precise(snapshot, state, side, base_amount) {
            return Some(quote);
        }
        debug!(
            "Falling back to constant-product approximation for CLMM {}",
            snapshot.descriptor.label
        );
    }

    fallback_constant_product(snapshot, side, base_amount)
}

fn simulate_trade_precise(
    snapshot: &PoolSnapshot,
    state: &ClmmState,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    let fees = snapshot.fees.as_ref()?;
    let fee_ratio = fees.total_ratio();
    if !(fee_ratio >= 0.0 && fee_ratio < 1.0) {
        return None;
    }

    let ctx = SwapContext::new(snapshot, state)?;

    let base_decimals = snapshot.base_decimals as i32;
    let quote_decimals = snapshot.quote_decimals as i32;
    let token0_scale = 10f64.powi(base_decimals);
    let token1_scale = 10f64.powi(quote_decimals);

    let inverted = snapshot.normalized_pair.inverted;

    match (side, inverted) {
        (TradeSide::Buy, false) => {
            let target_token0_out = base_amount * token0_scale;
            let mut ctx = ctx.clone();
            let net_token1_in = compute_token1_in_for_token0_out(&mut ctx, target_token0_out)?;
            let gross_token1_in = net_token1_in / (1.0 - fee_ratio);
            let fee_raw = gross_token1_in - net_token1_in;
            let quote_cost = gross_token1_in / token1_scale;
            let fee_units = fee_raw / token1_scale;
            let effective_price = quote_cost / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input: fee_units,
            })
        }
        (TradeSide::Sell, false) => {
            let gross_token0_in = base_amount * token0_scale;
            let net_token0_in = gross_token0_in * (1.0 - fee_ratio);
            if net_token0_in <= 0.0 {
                return None;
            }
            let fee_raw = gross_token0_in - net_token0_in;
            let mut ctx = ctx.clone();
            let token1_out = compute_token1_out_for_token0_in(&mut ctx, net_token0_in)?;
            let quote_proceeds = token1_out / token1_scale;
            let fee_units = fee_raw / token0_scale;
            let effective_price = quote_proceeds / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input: fee_units,
            })
        }
        (TradeSide::Buy, true) => {
            let target_token1_out = base_amount * token1_scale;
            let mut ctx = ctx.clone();
            let net_token0_in = compute_token0_in_for_token1_out(&mut ctx, target_token1_out)?;
            let gross_token0_in = net_token0_in / (1.0 - fee_ratio);
            let fee_raw = gross_token0_in - net_token0_in;
            let quote_cost = gross_token0_in / token0_scale;
            let fee_units = fee_raw / token0_scale;
            let effective_price = quote_cost / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost,
                quote_proceeds: 0.0,
                effective_price,
                fee_in_input: fee_units,
            })
        }
        (TradeSide::Sell, true) => {
            let gross_token1_in = base_amount * token1_scale;
            let net_token1_in = gross_token1_in * (1.0 - fee_ratio);
            if net_token1_in <= 0.0 {
                return None;
            }
            let fee_raw = gross_token1_in - net_token1_in;
            let mut ctx = ctx.clone();
            let token0_out = compute_token0_out_for_token1_in(&mut ctx, net_token1_in)?;
            let quote_proceeds = token0_out / token0_scale;
            let fee_units = fee_raw / token1_scale;
            let effective_price = quote_proceeds / base_amount;
            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0,
                quote_proceeds,
                effective_price,
                fee_in_input: fee_units,
            })
        }
    }
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

fn compute_token1_in_for_token0_out(ctx: &mut SwapContext, mut token0_out: f64) -> Option<f64> {
    if token0_out <= 0.0 {
        return Some(0.0);
    }
    let mut total_token1_in = 0.0;
    while token0_out > EPSILON {
        let upper_tick = ctx.tick_current_index + ctx.tick_spacing;
        let sqrt_upper = tick_to_sqrt_price(upper_tick)?;
        let available_token0 = ctx.liquidity * (1.0 / ctx.sqrt_price - 1.0 / sqrt_upper);
        if available_token0 <= EPSILON {
            return None;
        }
        if available_token0 >= token0_out - EPSILON {
            let new_inv = 1.0 / ctx.sqrt_price - token0_out / ctx.liquidity;
            if new_inv <= 0.0 {
                return None;
            }
            let new_sqrt = 1.0 / new_inv;
            total_token1_in += ctx.liquidity * (new_sqrt - ctx.sqrt_price);
            ctx.sqrt_price = new_sqrt;
            token0_out = 0.0;
        } else {
            total_token1_in += ctx.liquidity * (sqrt_upper - ctx.sqrt_price);
            token0_out -= available_token0;
            ctx.sqrt_price = sqrt_upper;
            ctx.tick_current_index = upper_tick;
            ctx.liquidity += ctx.liquidity_net_at(upper_tick);
            if ctx.liquidity <= EPSILON {
                return None;
            }
        }
    }
    Some(total_token1_in)
}

fn compute_token1_out_for_token0_in(ctx: &mut SwapContext, mut token0_in: f64) -> Option<f64> {
    if token0_in <= 0.0 {
        return Some(0.0);
    }
    let mut total_token1_out = 0.0;
    while token0_in > EPSILON {
        let lower_tick = ctx.tick_current_index;
        let sqrt_lower = tick_to_sqrt_price(lower_tick)?;
        let available_token0 = ctx.liquidity * (1.0 / sqrt_lower - 1.0 / ctx.sqrt_price);
        if available_token0 <= EPSILON {
            return None;
        }
        if available_token0 >= token0_in - EPSILON {
            let new_inv = token0_in / ctx.liquidity + 1.0 / ctx.sqrt_price;
            if new_inv <= 0.0 {
                return None;
            }
            let new_sqrt = 1.0 / new_inv;
            total_token1_out += ctx.liquidity * (ctx.sqrt_price - new_sqrt);
            ctx.sqrt_price = new_sqrt;
            token0_in = 0.0;
        } else {
            total_token1_out += ctx.liquidity * (ctx.sqrt_price - sqrt_lower);
            token0_in -= available_token0;
            ctx.sqrt_price = sqrt_lower;
            ctx.tick_current_index = lower_tick - ctx.tick_spacing;
            ctx.liquidity -= ctx.liquidity_net_at(lower_tick);
            if ctx.liquidity <= EPSILON {
                return None;
            }
        }
    }
    Some(total_token1_out)
}

fn compute_token0_in_for_token1_out(ctx: &mut SwapContext, mut token1_out: f64) -> Option<f64> {
    if token1_out <= 0.0 {
        return Some(0.0);
    }
    let mut total_token0_in = 0.0;
    while token1_out > EPSILON {
        let lower_tick = ctx.tick_current_index;
        let sqrt_lower = tick_to_sqrt_price(lower_tick)?;
        let available_token1 = ctx.liquidity * (ctx.sqrt_price - sqrt_lower);
        if available_token1 <= EPSILON {
            return None;
        }
        if available_token1 >= token1_out - EPSILON {
            let new_sqrt = ctx.sqrt_price - token1_out / ctx.liquidity;
            if new_sqrt <= 0.0 {
                return None;
            }
            let delta_token0 = ctx.liquidity * (1.0 / new_sqrt - 1.0 / ctx.sqrt_price);
            total_token0_in += delta_token0;
            ctx.sqrt_price = new_sqrt;
            token1_out = 0.0;
        } else {
            total_token0_in += ctx.liquidity * (1.0 / sqrt_lower - 1.0 / ctx.sqrt_price);
            token1_out -= available_token1;
            ctx.sqrt_price = sqrt_lower;
            ctx.tick_current_index = lower_tick - ctx.tick_spacing;
            ctx.liquidity -= ctx.liquidity_net_at(lower_tick);
            if ctx.liquidity <= EPSILON {
                return None;
            }
        }
    }
    Some(total_token0_in)
}

fn compute_token0_out_for_token1_in(ctx: &mut SwapContext, mut token1_in: f64) -> Option<f64> {
    if token1_in <= 0.0 {
        return Some(0.0);
    }
    let mut total_token0_out = 0.0;
    while token1_in > EPSILON {
        let upper_tick = ctx.tick_current_index + ctx.tick_spacing;
        let sqrt_upper = tick_to_sqrt_price(upper_tick)?;
        let available_token1 = ctx.liquidity * (sqrt_upper - ctx.sqrt_price);
        if available_token1 <= EPSILON {
            return None;
        }
        if available_token1 >= token1_in - EPSILON {
            let new_sqrt = ctx.sqrt_price + token1_in / ctx.liquidity;
            let delta_token0 = ctx.liquidity * (1.0 / ctx.sqrt_price - 1.0 / new_sqrt);
            total_token0_out += delta_token0;
            ctx.sqrt_price = new_sqrt;
            token1_in = 0.0;
        } else {
            total_token0_out += ctx.liquidity * (1.0 / ctx.sqrt_price - 1.0 / sqrt_upper);
            token1_in -= available_token1;
            ctx.sqrt_price = sqrt_upper;
            ctx.tick_current_index = upper_tick;
            ctx.liquidity += ctx.liquidity_net_at(upper_tick);
            if ctx.liquidity <= EPSILON {
                return None;
            }
        }
    }
    Some(total_token0_out)
}

#[derive(Clone)]
struct SwapContext {
    tick_spacing: i32,
    tick_current_index: i32,
    sqrt_price: f64,
    liquidity: f64,
    tick_map: Arc<BTreeMap<i32, i128>>,
}

impl SwapContext {
    fn new(snapshot: &PoolSnapshot, state: &ClmmState) -> Option<Self> {
        let liquidity = snapshot.liquidity? as f64;
        if !(liquidity.is_finite() && liquidity > 0.0) {
            return None;
        }
        let sqrt_price = snapshot.sqrt_price_x64 as f64 / Q64;
        if !(sqrt_price.is_finite() && sqrt_price > 0.0) {
            return None;
        }
        Some(Self {
            tick_spacing: state.tick_spacing,
            tick_current_index: state.tick_current_index,
            sqrt_price,
            liquidity,
            tick_map: state.tick_map(),
        })
    }

    fn liquidity_net_at(&self, tick_index: i32) -> f64 {
        self.tick_map
            .get(&tick_index)
            .map(|value| *value as f64)
            .unwrap_or(0.0)
    }
}

fn tick_to_sqrt_price(tick_index: i32) -> Option<f64> {
    let exponent = tick_index as f64 / 2.0;
    let value = PRICE_BASE.powf(exponent);
    if value.is_finite() && value > 0.0 {
        Some(value)
    } else {
        None
    }
}
