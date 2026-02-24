use anyhow::{anyhow, Result};
use chrono::Utc;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;

use super::{
    normalize_pair, AccountReader, PoolDescriptor, PoolFees, PoolSnapshot, TradeQuote, TradeSide,
};
use crate::rpc::AppContext;

const PUMP_FUN_POOL_HEADER: usize = 8;

#[derive(Debug, Clone)]
pub struct PumpFunState {
    pub pool_base_token_account: Pubkey,
    pub pool_quote_token_account: Pubkey,
    pub coin_creator: Pubkey,
    pub protocol_fee_recipient: Pubkey,
    pub quote_token_program: Pubkey,
}

pub async fn decode(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    if data.len() < PUMP_FUN_POOL_HEADER + 235 {
        anyhow::bail!("pump.fun pool account too short");
    }

    let mut reader = AccountReader::new(&data[PUMP_FUN_POOL_HEADER..]);
    reader.read_u8()?; // pool bump
    reader.read_u16()?; // index
    reader.read_pubkey()?; // creator
    let base_mint = reader.read_pubkey()?;
    let quote_mint = reader.read_pubkey()?;
    reader.read_pubkey()?; // lp mint
    let pool_base_token_account = reader.read_pubkey()?;
    let pool_quote_token_account = reader.read_pubkey()?;
    let lp_supply = reader.read_u64()?;
    let coin_creator = reader.read_pubkey()?;

    // spot_only 模式：从 WS cache 读取 vault 余额，cache miss 则跳过
    let (base_mint_info, quote_mint_info, fee_config, base_amount, quote_amount) = if ctx.spot_only_mode() {
        // 从缓存获取 mint info（通常已缓存）
        let base_mint_info = ctx.get_mint_info(&base_mint).await?;
        let quote_mint_info = ctx.get_mint_info(&quote_mint).await?;
        let fee_config = ctx.get_pump_fun_fee_config().await?;
        
        // 从 WS cache 读取 vault 余额，不触发 HTTP
        let base_amount = ctx.get_token_account_amount_from_cache(&pool_base_token_account)
            .ok_or_else(|| anyhow!("pump.fun base vault not in WS cache (spot_only)"))?;
        let quote_amount = ctx.get_token_account_amount_from_cache(&pool_quote_token_account)
            .ok_or_else(|| anyhow!("pump.fun quote vault not in WS cache (spot_only)"))?;
        
        (base_mint_info, quote_mint_info, fee_config, base_amount, quote_amount)
    } else {
        // Full 模式：并行获取所有 RPC 数据，绑定到池子更新的 slot
        let slot = Some(context.slot);
        let (base_mint_info, quote_mint_info, fee_config, base_amount, quote_amount) = tokio::try_join!(
            ctx.get_mint_info_with_slot(&base_mint, slot),
            ctx.get_mint_info_with_slot(&quote_mint, slot),
            ctx.get_pump_fun_fee_config(),
            ctx.get_token_account_amount_with_slot(&pool_base_token_account, slot),
            ctx.get_token_account_amount_with_slot(&pool_quote_token_account, slot),
        )?;
        (base_mint_info, quote_mint_info, fee_config, base_amount, quote_amount)
    };

    if base_amount == 0 || quote_amount == 0 {
        return Err(anyhow!("pump.fun pool has zero reserves"));
    }

    let price_raw = (quote_amount as f64) / (base_amount as f64);
    if !price_raw.is_finite() || price_raw <= 0.0 {
        return Err(anyhow!("pump.fun invalid pool ratio"));
    }

    let decimals_adjustment = base_mint_info.decimals as i32 - quote_mint_info.decimals as i32;
    let price = price_raw * 10f64.powi(decimals_adjustment);

    if !price.is_finite() || price <= 0.0 {
        return Err(anyhow!("pump.fun invalid price computed"));
    }

    let (normalized_pair, normalized_price) = normalize_pair(base_mint, quote_mint, price)
        .ok_or_else(|| {
            anyhow!(
                "cannot normalize pump.fun pair {} {}",
                base_mint,
                quote_mint
            )
        })?;

    // ✅ Pump.fun 需要订阅 vault 账户来计算价格（主池没有价格字段）
    // spot_only 和 full 模式都需要订阅 vault
    let dependent_accounts = vec![pool_base_token_account, pool_quote_token_account];

    let market_cap_lamports = crate::rpc::compute_pump_fun_market_cap_lamports(
        lp_supply,
        base_mint_info.decimals,
        price,
        quote_mint_info.decimals,
    );
    let selected_fees = fee_config.select_fees(market_cap_lamports);

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint,
        quote_mint,
        base_decimals: base_mint_info.decimals,
        quote_decimals: quote_mint_info.decimals,
        price,
        sqrt_price_x64: 0,
        liquidity: Some(lp_supply as u128),
        base_reserve: Some(base_amount as u128),
        quote_reserve: Some(quote_amount as u128),
        fees: Some(PoolFees {
            lp_fee_bps: selected_fees.lp_fee_bps as f64,
            protocol_fee_bps: selected_fees.protocol_fee_bps as f64,
            other_fee_bps: selected_fees.coin_creator_fee_bps as f64,
            meteora_dynamic: None, // PumpFun doesn't use Meteora dynamic fees
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
        lb_state: None,
        lb_accounts: None,
        pump_state: Some(PumpFunState {
            pool_base_token_account,
            pool_quote_token_account,
            coin_creator,
            protocol_fee_recipient: selected_fees.protocol_fee_recipient,
            quote_token_program: quote_mint_info.owner,
        }),
        dependent_accounts,
    })
}

pub fn simulate_trade(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    log::debug!(
        "PumpFun simulate_trade called: {} side={:?} amount={} base_reserve={:?} quote_reserve={:?}",
        snapshot.descriptor.label, side, base_amount, snapshot.base_reserve, snapshot.quote_reserve
    );
    
    if !(base_amount.is_finite() && base_amount > 0.0) {
        log::debug!("PumpFun simulate_trade: invalid base_amount {}", base_amount);
        return None;
    }

    if snapshot.base_reserve.is_none() {
        log::debug!("PumpFun simulate_trade: missing base_reserve for {}", snapshot.descriptor.label);
        return None;
    }
    if snapshot.quote_reserve.is_none() {
        log::debug!("PumpFun simulate_trade: missing quote_reserve for {}", snapshot.descriptor.label);
        return None;
    }
    
    let base_reserve = snapshot.base_reserve? as f64;
    let quote_reserve = snapshot.quote_reserve? as f64;
    let fees = snapshot.fees.as_ref()?;
    let fee_ratio = fees.total_ratio();
    log::debug!("PumpFun fees: total_ratio={}", fee_ratio);
    if !(fee_ratio >= 0.0 && fee_ratio < 1.0) {
        log::warn!("PumpFun invalid fee_ratio: {}", fee_ratio);
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

    log::debug!("PumpFun: matching (side={:?}, inverted={})", side, inverted);
    
    match (side, inverted) {
        (TradeSide::Buy, false) => {
            log::debug!("PumpFun: executing Buy false branch");
            let target_base_out_raw = to_raw(base_amount, normalized_base_scale);
            log::debug!(
                "PumpFun Buy: base_amount={} normalized_base_scale={} target_base_out_raw={} base_reserve={}",
                base_amount, normalized_base_scale, target_base_out_raw, base_reserve
            );
            if target_base_out_raw >= base_reserve {
                log::debug!(
                    "PumpFun simulate_trade: Buy amount too large {} >= {} for {}",
                    target_base_out_raw, base_reserve, snapshot.descriptor.label
                );
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
            log::debug!(
                "PumpFun Sell: base_amount={} base_in_raw={} fee_ratio={} net_base_in={} decimals={} inverted={}",
                base_amount, base_in_raw, fee_ratio, net_base_in, normalized_base_decimals, inverted
            );
            if net_base_in <= 0.0 {
                log::debug!("PumpFun Sell: net_base_in <= 0");
                return None;
            }
            let fee_base = base_in_raw - net_base_in;
            let quote_out = quote_reserve * net_base_in / (base_reserve + net_base_in);

            let quote_proceeds = to_units(quote_out, normalized_quote_scale);
            let fee_in_input = to_units(fee_base, normalized_base_scale);
            let effective_price = quote_proceeds / base_amount;
            
            log::debug!(
                "PumpFun Sell success: quote_proceeds={} fee={} effective_price={}",
                quote_proceeds, fee_in_input, effective_price
            );

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

/// Warmup: 从 RPC 获取 Pump.fun 池账户数据，解析出 vault 地址
/// 
/// 在 spot_only 模式启动时调用，返回 (base_vault, quote_vault) 供订阅使用
pub async fn warmup_parse_vaults(
    pool: &solana_pubkey::Pubkey,
    ctx: &crate::rpc::AppContext,
) -> anyhow::Result<(solana_pubkey::Pubkey, solana_pubkey::Pubkey)> {
    use solana_client::rpc_config::RpcAccountInfoConfig;
    use solana_account_decoder::UiAccountEncoding;
    
    // 从 RPC 获取池账户数据
    let config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(ctx.rpc_client().commitment()),
        ..RpcAccountInfoConfig::default()
    };
    
    let fetch_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.rpc_client().get_ui_account_with_config(pool, config),
    )
    .await;

    let response = fetch_result
        .map_err(|_| anyhow!("pump.fun pool account fetch timed out (5s): {}", pool))?
        .map_err(|e| anyhow!("pump.fun pool account fetch failed: {}: {}", pool, e))?;

    let account = response
        .value
        .ok_or_else(|| anyhow::anyhow!("pump.fun pool account not found: {}", pool))?;

    let data = account
        .data
        .decode()
        .ok_or_else(|| anyhow::anyhow!("pump.fun pool account decode failed: {}", pool))?;
    if data.len() < PUMP_FUN_POOL_HEADER + 235 {
        anyhow::bail!("pump.fun pool account too short");
    }
    
    // 解析 vault 地址（跳过 header 和前面的字段）
    let mut reader = AccountReader::new(&data[PUMP_FUN_POOL_HEADER..]);
    reader.read_u8()?; // pool bump
    reader.read_u16()?; // index
    reader.read_pubkey()?; // creator
    reader.read_pubkey()?; // base_mint
    reader.read_pubkey()?; // quote_mint
    reader.read_pubkey()?; // lp mint
    let pool_base_token_account = reader.read_pubkey()?;
    let pool_quote_token_account = reader.read_pubkey()?;
    
    log::debug!(
        "Pump.fun pool {} warmup: vaults parsed: {}, {}",
        pool, pool_base_token_account, pool_quote_token_account
    );
    
    Ok((pool_base_token_account, pool_quote_token_account))
}
