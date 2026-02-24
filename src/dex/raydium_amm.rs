//! Raydium AMM V4 pool decoding via sidecar introspection

use std::str::FromStr;

use anyhow::{anyhow, Result};
use chrono::Utc;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::RwLock;

use super::{normalize_pair, PoolDescriptor, PoolFees, PoolSnapshot, TradeSide, TradeQuote};
use crate::rpc::AppContext;
use crate::sidecar_client::SidecarClient;

/// Raydium AMM V4 program ID
pub const RAYDIUM_AMM_V4_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

/// State specific to Raydium AMM V4 pools
#[derive(Debug, Clone)]
pub struct RaydiumAmmState {
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
    pub base_reserve: u128,
    pub quote_reserve: u128,
    pub status: u64,
}

// Static sidecar client for Raydium AMM introspection
static SIDECAR_CLIENT: OnceCell<Result<SidecarClient, String>> = OnceCell::new();

fn sidecar_client() -> Result<&'static SidecarClient> {
    match SIDECAR_CLIENT.get_or_init(|| SidecarClient::from_env().map_err(|e| e.to_string())) {
        Ok(client) => Ok(client),
        Err(err) => Err(anyhow!("SidecarClient initialization failed: {}", err)),
    }
}

/// 静态信息缓存：存储每个 Raydium AMM V4 池的不变字段（mints、vaults、decimals、fee 参数等）
/// 这些字段在池创建后不会改变，只需要从 SDK 拉一次
#[derive(Debug, Clone)]
pub struct RaydiumAmmStaticInfo {
    pub program_id: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub base_vault: Pubkey,
    pub quote_vault: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub target_orders: Pubkey,
    pub market_id: Pubkey,
    pub market_program_id: Pubkey,
    // Serum market accounts
    pub serum_bids: Pubkey,
    pub serum_asks: Pubkey,
    pub serum_event_queue: Pubkey,
    pub serum_coin_vault: Pubkey,
    pub serum_pc_vault: Pubkey,
    pub serum_vault_signer: Pubkey,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub fee_rate: f64,
}

/// 全局静态信息缓存
static AMM_STATIC_CACHE: OnceCell<RwLock<HashMap<Pubkey, RaydiumAmmStaticInfo>>> = OnceCell::new();

fn get_static_cache() -> &'static RwLock<HashMap<Pubkey, RaydiumAmmStaticInfo>> {
    AMM_STATIC_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 从缓存获取静态信息
fn get_cached_static_info(pool: &Pubkey) -> Option<RaydiumAmmStaticInfo> {
    get_static_cache()
        .read()
        .ok()
        .and_then(|cache| cache.get(pool).cloned())
}

/// 公开接口：获取缓存的静态信息（供 trade executor 使用）
pub fn get_static_info(pool: &Pubkey) -> Option<RaydiumAmmStaticInfo> {
    get_cached_static_info(pool)
}

/// 公开接口：注入静态信息（用于测试或预加载）
pub fn inject_static_info(pool: Pubkey, info: RaydiumAmmStaticInfo) {
    cache_static_info(pool, info);
}

/// 将静态信息存入缓存
fn cache_static_info(pool: Pubkey, info: RaydiumAmmStaticInfo) {
    if let Ok(mut cache) = get_static_cache().write() {
        cache.insert(pool, info);
    }
}

/// Decode a Raydium AMM V4 pool using hybrid strategy:
/// 1. If static info is cached → use local decode (fast)
/// 2. If not cached → call SDK introspect to initialize cache
/// 3. If local decode fails → fallback to SDK introspect
pub async fn decode(
    descriptor: &PoolDescriptor,
    _data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    let pool = descriptor.address;

    // 优先尝试本地 decode（如果有缓存的静态信息）
    if let Some(static_info) = get_cached_static_info(&pool) {
        match decode_local(descriptor, context.clone(), ctx, &static_info).await {
            Ok(snapshot) => {
                debug!("Raydium AMM pool {} decoded locally", descriptor.label);
                return Ok(snapshot);
            }
            Err(e) => {
                // spot_only 模式下不 fallback 到 SDK（避免 HTTP）
                if ctx.spot_only_mode() {
                    return Err(e);
                }
                warn!(
                    "Raydium AMM pool {} local decode failed: {}, falling back to SDK",
                    descriptor.label, e
                );
                // Fallback to SDK below
            }
        }
    }

    // spot_only 模式下必须有缓存的静态信息才能 decode
    if ctx.spot_only_mode() {
        anyhow::bail!("raydium amm v4 pool {} requires cached static info in spot_only mode", descriptor.label);
    }

    // 没有缓存或本地 decode 失败 → 调用 SDK introspect
    decode_via_sdk(descriptor, context, ctx).await
}

/// 通过 SDK introspect 获取完整 snapshot，并缓存静态信息
async fn decode_via_sdk(
    descriptor: &PoolDescriptor,
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    let client = sidecar_client()?;
    let pool_str = descriptor.address.to_string();

    // Call sidecar to get pool details (15s timeout, includes semaphore wait)
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        client.introspect_raydium_amm(&pool_str)
    ).await
        .map_err(|_| anyhow!("Raydium AMM introspect_raydium_amm timed out"))?
        .map_err(|e| anyhow!("Raydium AMM introspect_raydium_amm failed: {}", e))?;

    if !resp.ok {
        return Err(anyhow!(
            "Raydium AMM introspect failed: {}",
            resp.error.unwrap_or_else(|| "unknown error".to_string())
        ));
    }

    // Parse required fields from sidecar response
    let base_mint = Pubkey::from_str(
        resp.base_mint
            .as_ref()
            .ok_or_else(|| anyhow!("missing base_mint"))?,
    )?;
    let quote_mint = Pubkey::from_str(
        resp.quote_mint
            .as_ref()
            .ok_or_else(|| anyhow!("missing quote_mint"))?,
    )?;
    let base_vault = Pubkey::from_str(
        resp.base_vault
            .as_ref()
            .ok_or_else(|| anyhow!("missing base_vault"))?,
    )?;
    let quote_vault = Pubkey::from_str(
        resp.quote_vault
            .as_ref()
            .ok_or_else(|| anyhow!("missing quote_vault"))?,
    )?;
    let lp_mint = Pubkey::from_str(
        resp.lp_mint
            .as_ref()
            .ok_or_else(|| anyhow!("missing lp_mint"))?,
    )?;
    let open_orders = Pubkey::from_str(
        resp.open_orders
            .as_ref()
            .ok_or_else(|| anyhow!("missing open_orders"))?,
    )?;
    let target_orders = Pubkey::from_str(
        resp.target_orders
            .as_ref()
            .ok_or_else(|| anyhow!("missing target_orders"))?,
    )?;
    let market_id = Pubkey::from_str(
        resp.market_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing market_id"))?,
    )?;
    let market_program_id = Pubkey::from_str(
        resp.market_program_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing market_program_id"))?,
    )?;

    let base_decimals = resp.base_decimals.ok_or_else(|| anyhow!("missing base_decimals"))?;
    let quote_decimals = resp.quote_decimals.ok_or_else(|| anyhow!("missing quote_decimals"))?;

    // Parse reserves
    let base_reserve: u128 = resp
        .base_reserve
        .as_ref()
        .ok_or_else(|| anyhow!("missing base_reserve"))?
        .parse()
        .map_err(|_| anyhow!("invalid base_reserve"))?;
    let quote_reserve: u128 = resp
        .quote_reserve
        .as_ref()
        .ok_or_else(|| anyhow!("missing quote_reserve"))?
        .parse()
        .map_err(|_| anyhow!("invalid quote_reserve"))?;

    // Parse price from sidecar response
    let price: f64 = resp
        .price
        .as_ref()
        .ok_or_else(|| anyhow!("missing price"))?
        .parse()
        .map_err(|_| anyhow!("invalid price"))?;

    let status = resp.status.unwrap_or(0);
    let slot = resp.slot.unwrap_or(context.slot);

    // Calculate fee info
    let fee_rate = resp.fee_rate.unwrap_or(0.0);
    let fees = PoolFees {
        lp_fee_bps: fee_rate * 10_000.0,
        protocol_fee_bps: 0.0,
        other_fee_bps: 0.0,
        meteora_dynamic: None,
    };

    // Parse Serum market accounts (optional - may not be present in older sidecar versions)
    let serum_bids = resp.serum_bids
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();
    let serum_asks = resp.serum_asks
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();
    let serum_event_queue = resp.serum_event_queue
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();
    let serum_coin_vault = resp.serum_coin_vault
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();
    let serum_pc_vault = resp.serum_pc_vault
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();
    let serum_vault_signer = resp.serum_vault_signer
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok())
        .unwrap_or_default();

    // 缓存静态信息，下次可以本地 decode
    let static_info = RaydiumAmmStaticInfo {
        program_id: Pubkey::from_str(
            resp
                .program_id
                .as_ref()
                .ok_or_else(|| anyhow!("missing program_id"))?,
        )?,
        base_mint,
        quote_mint,
        base_vault,
        quote_vault,
        lp_mint,
        open_orders,
        target_orders,
        market_id,
        market_program_id,
        serum_bids,
        serum_asks,
        serum_event_queue,
        serum_coin_vault,
        serum_pc_vault,
        serum_vault_signer,
        base_decimals,
        quote_decimals,
        fee_rate,
    };
    cache_static_info(descriptor.address, static_info);
    debug!("Raydium AMM pool {} static info cached from SDK", descriptor.label);

    // For AMM V4, we use base/quote naming convention
    // price is quote_per_base (how much quote to buy 1 base)
    let (normalized_pair, normalized_price) =
        normalize_pair(base_mint, quote_mint, price).ok_or_else(|| anyhow!("invalid price"))?;

    // AMM V4 doesn't have sqrt_price_x64, we calculate it from price
    // sqrt_price_x64 = sqrt(price) * 2^64
    let sqrt_price = price.sqrt();
    let sqrt_price_x64 = (sqrt_price * (1u128 << 64) as f64) as u128;

    // ✅ Spot-only 模式：不订阅任何依赖账户，只订阅主池账户
    let dependent_accounts = if ctx.spot_only_mode() {
        vec![]
    } else {
        vec![base_vault, quote_vault]
    };

    // Build AMM state
    let _amm_state = RaydiumAmmState {
        base_mint,
        quote_mint,
        base_vault,
        quote_vault,
        lp_mint,
        open_orders,
        target_orders,
        market_id,
        market_program_id,
        base_reserve,
        quote_reserve,
        status,
    };

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint,
        quote_mint,
        base_decimals,
        quote_decimals,
        price,
        sqrt_price_x64,
        liquidity: None, // AMM V4 doesn't have concentrated liquidity
        base_reserve: Some(base_reserve),
        quote_reserve: Some(quote_reserve),
        fees: Some(fees),
        slot,
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
        dependent_accounts,
    })
}

/// 本地解码 Raydium AMM V4 池（需要已缓存的静态信息）
///
/// 我们不解析池账户布局，而是直接使用缓存的 vault 地址 + decimals，
/// 通过 RPC 读取 vault 余额来计算价格和储备。
async fn decode_local(
    descriptor: &PoolDescriptor,
    context: RpcResponseContext,
    ctx: &AppContext,
    static_info: &RaydiumAmmStaticInfo,
) -> Result<PoolSnapshot> {
    // 获取 vault 余额
    let (base_reserve_raw, quote_reserve_raw) = if ctx.spot_only_mode() {
        // spot_only 模式：从 WS cache 读取 vault 余额，不触发 HTTP
        let base = ctx.get_token_account_amount_from_cache(&static_info.base_vault)
            .ok_or_else(|| anyhow!("raydium amm base vault not in WS cache (spot_only)"))?;
        let quote = ctx.get_token_account_amount_from_cache(&static_info.quote_vault)
            .ok_or_else(|| anyhow!("raydium amm quote vault not in WS cache (spot_only)"))?;
        (base, quote)
    } else {
        // Full 模式：通过 RPC 获取 vault 余额
        let slot = Some(context.slot);
        tokio::try_join!(
            ctx.get_token_account_amount_with_slot(&static_info.base_vault, slot),
            ctx.get_token_account_amount_with_slot(&static_info.quote_vault, slot),
        )?
    };

    if base_reserve_raw == 0 {
        anyhow::bail!("base reserve is zero");
    }

    let base_scale = 10f64.powi(static_info.base_decimals as i32);
    let quote_scale = 10f64.powi(static_info.quote_decimals as i32);

    let base_reserve = base_reserve_raw as f64 / base_scale;
    let quote_reserve = quote_reserve_raw as f64 / quote_scale;

    if !(base_reserve.is_finite() && base_reserve > 0.0) {
        anyhow::bail!("invalid base reserve computed");
    }

    let price = quote_reserve / base_reserve;
    if !(price.is_finite() && price > 0.0) {
        anyhow::bail!("invalid price computed from reserves");
    }

    // AMM V4 doesn't have sqrt_price_x64, approximate from price
    let sqrt_price = price.sqrt();
    let sqrt_price_x64 = (sqrt_price * (1u128 << 64) as f64) as u128;

    // Normalize pair
    let (normalized_pair, normalized_price) = normalize_pair(
        static_info.base_mint,
        static_info.quote_mint,
        price,
    )
    .ok_or_else(|| anyhow!("invalid price for normalization"))?;

    // Calculate fees from cached static info
    let lp_fee_bps = static_info.fee_rate * 10_000.0;

    let fees = PoolFees {
        lp_fee_bps,
        protocol_fee_bps: 0.0,
        other_fee_bps: 0.0,
        meteora_dynamic: None,
    };

    // ✅ Raydium AMM V4 需要订阅 vault 账户来计算价格（主池没有价格字段）
    // spot_only 和 full 模式都需要订阅 vault
    let dependent_accounts = vec![static_info.base_vault, static_info.quote_vault];

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: static_info.base_mint,
        quote_mint: static_info.quote_mint,
        base_decimals: static_info.base_decimals,
        quote_decimals: static_info.quote_decimals,
        price,
        sqrt_price_x64,
        liquidity: None,
        base_reserve: Some(base_reserve_raw as u128),
        quote_reserve: Some(quote_reserve_raw as u128),
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
        dependent_accounts,
    })
}

/// Simulate a trade on Raydium AMM V4 using constant product formula (x * y = k)
/// 
/// For AMM V4:
/// - Sell base: spend base_amount of base token, receive quote token
/// - Buy base: spend quote token, receive base_amount of base token
pub fn simulate_trade(
    snapshot: &PoolSnapshot,
    side: TradeSide,
    base_amount: f64,
) -> Option<TradeQuote> {
    let base_reserve = snapshot.base_reserve? as f64;
    let quote_reserve = snapshot.quote_reserve? as f64;
    
    if base_reserve <= 0.0 || quote_reserve <= 0.0 || base_amount <= 0.0 {
        return None;
    }

    // Get fee ratio (default 0.25% = 25 bps for Raydium AMM V4)
    let fee_ratio = snapshot.fees.as_ref()
        .map(|f| f.total_ratio())
        .unwrap_or(0.0025);

    // Convert to display units
    let base_scale = 10f64.powi(snapshot.base_decimals as i32);
    let quote_scale = 10f64.powi(snapshot.quote_decimals as i32);
    
    let base_reserve_display = base_reserve / base_scale;
    let quote_reserve_display = quote_reserve / quote_scale;

    match side {
        TradeSide::Sell => {
            // Sell base for quote: x * y = k
            // new_base = base_reserve + base_amount_in
            // new_quote = k / new_base
            // quote_out = quote_reserve - new_quote
            let amount_in_after_fee = base_amount * (1.0 - fee_ratio);
            let new_base = base_reserve_display + amount_in_after_fee;
            let k = base_reserve_display * quote_reserve_display;
            let new_quote = k / new_base;
            let quote_out = quote_reserve_display - new_quote;
            
            if quote_out <= 0.0 {
                return None;
            }

            let effective_price = quote_out / base_amount;
            let fee_amount = base_amount * fee_ratio * snapshot.price;
            
            Some(TradeQuote {
                base_amount,
                quote_cost: 0.0, // Not applicable for sell
                quote_proceeds: quote_out,
                effective_price,
                fee_in_input: fee_amount,
            })
        }
        TradeSide::Buy => {
            // Buy base with quote: x * y = k
            // We want to receive base_amount of base token
            // new_base = base_reserve - base_amount
            // new_quote = k / new_base
            // quote_in = new_quote - quote_reserve
            if base_amount >= base_reserve_display {
                return None; // Can't buy more than reserve
            }
            
            let new_base = base_reserve_display - base_amount;
            let k = base_reserve_display * quote_reserve_display;
            let new_quote = k / new_base;
            let quote_in_before_fee = new_quote - quote_reserve_display;
            let quote_in = quote_in_before_fee / (1.0 - fee_ratio);
            
            if quote_in <= 0.0 {
                return None;
            }

            let effective_price = quote_in / base_amount;
            let fee_amount = quote_in * fee_ratio;
            
            Some(TradeQuote {
                base_amount,
                quote_cost: quote_in,
                quote_proceeds: 0.0, // Not applicable for buy
                effective_price,
                fee_in_input: fee_amount,
            })
        }
    }
}

/// Warmup: 通过 sidecar introspect 预热 AMM V4 池的静态信息
/// 
/// 在 spot_only 模式启动时调用，确保后续 decode 能本地执行
/// 返回 (base_vault, quote_vault) 供订阅使用
pub async fn warmup_static_info(pool: &Pubkey) -> Result<(Pubkey, Pubkey)> {
    // 如果已经有缓存，直接返回 vault 地址
    if let Some(info) = get_cached_static_info(pool) {
        return Ok((info.base_vault, info.quote_vault));
    }

    // 调用 sidecar introspect 获取静态信息 (5s timeout)
    let client = sidecar_client()?;
    let pool_str = pool.to_string();
    let resp = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        client.introspect_raydium_amm(&pool_str)
    ).await
        .map_err(|_| anyhow!("Raydium AMM warmup introspect timed out"))?
        .map_err(|e| anyhow!("Raydium AMM warmup introspect failed: {}", e))?;

    if !resp.ok {
        return Err(anyhow!(
            "Raydium AMM warmup introspect failed: {}",
            resp.error.unwrap_or_else(|| "unknown error".to_string())
        ));
    }

    // 解析必要字段
    let base_mint = Pubkey::from_str(
        resp.base_mint.as_ref().ok_or_else(|| anyhow!("missing base_mint"))?,
    )?;
    let quote_mint = Pubkey::from_str(
        resp.quote_mint.as_ref().ok_or_else(|| anyhow!("missing quote_mint"))?,
    )?;
    let base_vault = Pubkey::from_str(
        resp.base_vault.as_ref().ok_or_else(|| anyhow!("missing base_vault"))?,
    )?;
    let quote_vault = Pubkey::from_str(
        resp.quote_vault.as_ref().ok_or_else(|| anyhow!("missing quote_vault"))?,
    )?;
    let lp_mint = Pubkey::from_str(
        resp.lp_mint.as_ref().ok_or_else(|| anyhow!("missing lp_mint"))?,
    )?;
    let open_orders = Pubkey::from_str(
        resp.open_orders.as_ref().ok_or_else(|| anyhow!("missing open_orders"))?,
    )?;
    let target_orders = Pubkey::from_str(
        resp.target_orders.as_ref().ok_or_else(|| anyhow!("missing target_orders"))?,
    )?;
    let market_id = Pubkey::from_str(
        resp.market_id.as_ref().ok_or_else(|| anyhow!("missing market_id"))?,
    )?;
    let market_program_id = Pubkey::from_str(
        resp.market_program_id.as_ref().ok_or_else(|| anyhow!("missing market_program_id"))?,
    )?;
    let program_id = Pubkey::from_str(
        resp.program_id.as_ref().ok_or_else(|| anyhow!("missing program_id"))?,
    )?;

    let base_decimals = resp.base_decimals.ok_or_else(|| anyhow!("missing base_decimals"))?;
    let quote_decimals = resp.quote_decimals.ok_or_else(|| anyhow!("missing quote_decimals"))?;
    let fee_rate = resp.fee_rate.unwrap_or(0.0);

    // 解析 Serum 账户
    let serum_bids = resp.serum_bids.as_ref().and_then(|s| Pubkey::from_str(s).ok()).unwrap_or_default();
    let serum_asks = resp.serum_asks.as_ref().and_then(|s| Pubkey::from_str(s).ok()).unwrap_or_default();
    let serum_event_queue = resp.serum_event_queue.as_ref().and_then(|s| Pubkey::from_str(s).ok()).unwrap_or_default();
    let serum_coin_vault = resp.serum_coin_vault.as_ref().and_then(|s| Pubkey::from_str(s).ok()).unwrap_or_default();
    let serum_pc_vault = resp.serum_pc_vault.as_ref().and_then(|s| Pubkey::from_str(s).ok()).unwrap_or_default();
    let serum_vault_signer = resp.serum_vault_signer.as_ref().and_then(|s| Pubkey::from_str(s).ok()).unwrap_or_default();

    // 缓存静态信息
    let static_info = RaydiumAmmStaticInfo {
        program_id,
        base_mint,
        quote_mint,
        base_vault,
        quote_vault,
        lp_mint,
        open_orders,
        target_orders,
        market_id,
        market_program_id,
        serum_bids,
        serum_asks,
        serum_event_queue,
        serum_coin_vault,
        serum_pc_vault,
        serum_vault_signer,
        base_decimals,
        quote_decimals,
        fee_rate,
    };
    cache_static_info(*pool, static_info);
    debug!("Raydium AMM V4 pool {} warmup: static info cached, vaults: {}, {}", pool, base_vault, quote_vault);

    Ok((base_vault, quote_vault))
}

