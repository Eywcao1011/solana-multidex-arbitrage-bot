use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use chrono::Utc;
use futures::future::join_all;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::RwLock;

use super::{
    clmm, normalize_pair, AccountReader, ClmmAccounts, PoolDescriptor, PoolFees, PoolSnapshot,
};
use crate::rpc::{q64_to_price, AppContext};
use crate::sidecar_client::SidecarClient;

const TICK_ARRAY_SIZE: i32 = 60;
const PREFETCH_RANGE: i32 = 10; // ✅ 预取 ±10 tick array，覆盖最大化（减少 6023 错误）

// Static sidecar client for Raydium introspection
static SIDECAR_CLIENT: OnceCell<Result<SidecarClient, String>> = OnceCell::new();

fn sidecar_client() -> Result<&'static SidecarClient> {
    match SIDECAR_CLIENT.get_or_init(|| SidecarClient::from_env().map_err(|e| e.to_string())) {
        Ok(client) => Ok(client),
        Err(err) => Err(anyhow!("SidecarClient initialization failed: {}", err)),
    }
}

/// 静态信息缓存：存储每个 Raydium CLMM 池的不变字段（mints、vaults、decimals、fee 参数等）
/// 这些字段在池创建后不会改变，只需要从 SDK 拉一次
#[derive(Debug, Clone)]
pub struct RaydiumPoolStaticInfo {
    pub program_id: Pubkey,
    pub amm_config: Pubkey,
    pub token_mint_0: Pubkey,
    pub token_mint_1: Pubkey,
    pub vault_0: Pubkey,
    pub vault_1: Pubkey,
    pub observation_key: Pubkey,
    pub decimals_0: u8,
    pub decimals_1: u8,
    pub tick_spacing: i32,
    pub fee_rate: u64,
    pub protocol_fee_rate: u64,
    pub fund_fee_rate: u64,
    /// Token program for token_0 (spl_token or spl_token_2022)
    pub token_program_0: Pubkey,
    /// Token program for token_1 (spl_token or spl_token_2022)
    pub token_program_1: Pubkey,
}

/// 全局静态信息缓存
static POOL_STATIC_CACHE: OnceCell<RwLock<HashMap<Pubkey, RaydiumPoolStaticInfo>>> =
    OnceCell::new();

fn get_static_cache() -> &'static RwLock<HashMap<Pubkey, RaydiumPoolStaticInfo>> {
    POOL_STATIC_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 从缓存获取静态信息
fn get_cached_static_info(pool: &Pubkey) -> Option<RaydiumPoolStaticInfo> {
    get_static_cache()
        .read()
        .ok()
        .and_then(|cache| cache.get(pool).cloned())
}

/// 将静态信息存入缓存
fn cache_static_info(pool: Pubkey, info: RaydiumPoolStaticInfo) {
    if let Ok(mut cache) = get_static_cache().write() {
        cache.insert(pool, info);
    }
}

/// 直接从池账户数据初始化静态信息（无需 sidecar）
async fn init_static_info_from_account_data(
    data: &[u8],
    _context: &RpcResponseContext,
    ctx: &AppContext,
) -> Result<RaydiumPoolStaticInfo> {
    const MIN_SIZE: usize = 277;

    if data.len() < MIN_SIZE {
        anyhow::bail!("Raydium pool account too short: {}", data.len());
    }

    let mut reader = AccountReader::new(&data[8..]);
    reader.read_u8()?; // bump
    let amm_config = reader.read_pubkey()?;
    reader.read_pubkey()?; // owner
    let token_mint_0 = reader.read_pubkey()?;
    let token_mint_1 = reader.read_pubkey()?;
    let token_vault_0 = reader.read_pubkey()?;
    let token_vault_1 = reader.read_pubkey()?;
    let observation_key = reader.read_pubkey()?; // observation_key
    let decimals_0 = reader.read_u8()?;
    let decimals_1 = reader.read_u8()?;
    let tick_spacing = reader.read_u16()? as i32;

    if token_mint_0 == Pubkey::default()
        || token_mint_1 == Pubkey::default()
        || token_vault_0 == Pubkey::default()
        || token_vault_1 == Pubkey::default()
    {
        anyhow::bail!("Raydium pool account parsed with default pubkeys (likely wrong layout)");
    }

    let fees = ctx.get_raydium_amm_config(&amm_config).await.unwrap_or_default();
    let fee_rate = fees.trade_fee_rate as u64;
    let protocol_fee_rate = fees.protocol_fee_rate as u64;
    let fund_fee_rate = fees.fund_fee_rate as u64;

    // ✅ 获取 token program IDs，支持 Token-2022
    let token_program_0 = ctx
        .get_mint_info_from_cache(&token_mint_0)
        .map(|info| info.owner)
        .unwrap_or(spl_token::ID);
    let token_program_1 = ctx
        .get_mint_info_from_cache(&token_mint_1)
        .map(|info| info.owner)
        .unwrap_or(spl_token::ID);

    Ok(RaydiumPoolStaticInfo {
        program_id: crate::constants::RAYDIUM_CLMM_PROGRAM_ID,
        amm_config,
        token_mint_0,
        token_mint_1,
        vault_0: token_vault_0,
        vault_1: token_vault_1,
        observation_key,
        decimals_0,
        decimals_1,
        tick_spacing,
        fee_rate,
        protocol_fee_rate,
        fund_fee_rate,
        token_program_0,
        token_program_1,
    })
}

#[derive(Debug, Clone)]
pub struct TickArrayState {
    pub ticks: Vec<clmm::Tick>,
}

#[derive(Debug)]
pub struct LegacyLayoutError;

impl fmt::Display for LegacyLayoutError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Raydium CLMM account appears to use a legacy/truncated layout. Legacy pools are no longer supported; remove the pool or upgrade it."
        )
    }
}

impl std::error::Error for LegacyLayoutError {}

/// Decode Raydium CLMM pool using hybrid strategy:
/// 1. If static info is cached → use local decode (fast)
/// 2. If not cached → call SDK introspect to initialize cache
/// 3. If local decode fails → fallback to SDK introspect
pub async fn decode(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
) -> Result<PoolSnapshot> {
    let pool = descriptor.address;

    // 1) If static info is cached → use local decode (fast)
    if let Some(static_info) = get_cached_static_info(&pool) {
        match decode_local(descriptor, data, context.clone(), ctx, &static_info).await {
            Ok(snapshot) => {
                debug!(
                    "Raydium pool {} decoded locally with cached static info",
                    descriptor.label
                );
                return Ok(snapshot);
            }
            Err(e) => {
                warn!(
                    "Raydium pool {} local decode with cached static info failed: {}",
                    descriptor.label, e
                );
            }
        }
    }

    // 2) If not cached → call SDK introspect to initialize cache
    if !data.is_empty() && get_cached_static_info(&pool).is_none() {
        match init_static_info_from_account_data(data, &context, ctx).await {
            Ok(info) => {
                cache_static_info(pool, info);
                debug!(
                    "Raydium pool {} static info cached from account data",
                    descriptor.label
                );
            }
            Err(e) => {
                warn!(
                    "Raydium pool {} ({}) static init from account data failed at slot {}: {:?}",
                    descriptor.label,
                    descriptor.address,
                    context.slot,
                    e
                );
            }
        }
    }

    // 3) If local decode fails → fallback to SDK introspect
    if let Some(static_info) = get_cached_static_info(&pool) {
        match decode_local(descriptor, data, context.clone(), ctx, &static_info).await {
            Ok(snapshot) => {
                debug!(
                    "Raydium pool {} decoded locally after static info init",
                    descriptor.label
                );
                return Ok(snapshot);
            }
            Err(e) => {
                warn!(
                    "Raydium pool {} local decode after static info init failed: {}",
                    descriptor.label, e
                );
            }
        }
    }

    // 4) Fallback to SDK introspect
    match decode_via_sdk(descriptor, context, ctx).await {
        Ok(snapshot) => {
            debug!(
                "Raydium pool {} decoded via SDK introspect fallback",
                descriptor.label
            );
            Ok(snapshot)
        }
        Err(e) => Err(anyhow!(
            "Raydium decode via SDK failed for {}: {}",
            descriptor.label, e
        )),
    }
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
        client.introspect_raydium(&pool_str)
    ).await
        .map_err(|_| anyhow!("Raydium introspect_raydium timed out"))?
        .map_err(|e| anyhow!("Raydium introspect_raydium failed: {}", e))?;

    if !resp.ok {
        return Err(anyhow!(
            "Raydium introspect failed: {}",
            resp.error.unwrap_or_else(|| "unknown error".to_string())
        ));
    }

    // Parse required fields from sidecar response
    let token_mint_0 = Pubkey::from_str(
        resp
            .token_mint_0
            .as_ref()
            .ok_or_else(|| anyhow!("missing token_mint_0"))?,
    )?;
    let token_mint_1 = Pubkey::from_str(
        resp
            .token_mint_1
            .as_ref()
            .ok_or_else(|| anyhow!("missing token_mint_1"))?,
    )?;
    let vault_0 = Pubkey::from_str(
        resp
            .vault_0
            .as_ref()
            .ok_or_else(|| anyhow!("missing vault_0"))?,
    )?;
    let vault_1 = Pubkey::from_str(
        resp
            .vault_1
            .as_ref()
            .ok_or_else(|| anyhow!("missing vault_1"))?,
    )?;
    let program_id = Pubkey::from_str(
        resp
            .program_id
            .as_ref()
            .ok_or_else(|| anyhow!("missing program_id"))?,
    )?;

    let decimals_0 = resp
        .decimals_0
        .ok_or_else(|| anyhow!("missing decimals_0"))?;
    let decimals_1 = resp
        .decimals_1
        .ok_or_else(|| anyhow!("missing decimals_1"))?;

    let reserve_0: u64 = resp
        .reserve_0
        .as_ref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let reserve_1: u64 = resp
        .reserve_1
        .as_ref()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let sqrt_price_x64: u128 = resp
        .sqrt_price_x64
        .as_ref()
        .ok_or_else(|| anyhow!("missing sqrt_price_x64"))?
        .parse()
        .map_err(|_| anyhow!("invalid sqrt_price_x64"))?;

    let price: f64 = resp
        .price
        .as_ref()
        .ok_or_else(|| anyhow!("missing price"))?
        .parse()
        .map_err(|_| anyhow!("invalid price"))?;

    let liquidity: u128 = resp
        .liquidity
        .as_ref()
        .ok_or_else(|| anyhow!("missing liquidity"))?
        .parse()
        .map_err(|_| anyhow!("invalid liquidity"))?;

    let tick_spacing = resp
        .tick_spacing
        .ok_or_else(|| anyhow!("missing tick_spacing"))?;
    let tick_current = resp
        .tick_current_index
        .ok_or_else(|| anyhow!("missing tick_current_index"))?;

    let fee_rate = resp
        .fee_rate
        .ok_or_else(|| anyhow!("missing fee_rate"))?;
    let protocol_fee_rate = resp
        .protocol_fee_rate
        .ok_or_else(|| anyhow!("missing protocol_fee_rate"))?;
    let fund_fee_rate = resp
        .fund_fee_rate
        .ok_or_else(|| anyhow!("missing fund_fee_rate"))?;

    let slot = resp.slot.unwrap_or(context.slot);

    // ✅ 修复：sidecar 不返回 amm_config，但我们不应覆盖已有的正确值
    // 如果缓存中已有 amm_config（来自本地解码），保留它
    let cached_info = get_cached_static_info(&descriptor.address);
    let existing_amm_config = cached_info
        .as_ref()
        .map(|info| info.amm_config)
        .filter(|c| *c != Pubkey::default());
    let cached_observation_key = cached_info
        .as_ref()
        .map(|info| info.observation_key)
        .filter(|key| *key != Pubkey::default());

    let amm_config = existing_amm_config.unwrap_or_default();
    
    if amm_config == Pubkey::default() {
        warn!(
            "Raydium pool {} via sidecar has no valid amm_config - swap building will fail!",
            descriptor.label
        );
    }

    let static_info = RaydiumPoolStaticInfo {
        program_id,
        amm_config,
        token_mint_0,
        token_mint_1,
        vault_0,
        vault_1,
        // ✅ sidecar 不返回 observation_key，优先使用缓存值
        observation_key: cached_observation_key.unwrap_or_default(),
        decimals_0,
        decimals_1,
        tick_spacing,
        fee_rate,
        protocol_fee_rate,
        fund_fee_rate,
        // ✅ SDK 解码时暂时使用默认值，后续会从 local decode 更新
        token_program_0: get_cached_static_info(&descriptor.address)
            .map(|info| info.token_program_0)
            .unwrap_or(spl_token::ID),
        token_program_1: get_cached_static_info(&descriptor.address)
            .map(|info| info.token_program_1)
            .unwrap_or(spl_token::ID),
    };
    cache_static_info(descriptor.address, static_info);
    debug!("Raydium pool {} static info cached from SDK", descriptor.label);

    // Normalize pair and compute normalized price
    let (normalized_pair, normalized_price) =
        normalize_pair(token_mint_0, token_mint_1, price)
            .ok_or_else(|| anyhow!("cannot normalize raydium pair"))?;

    // Fee ratios are expressed in 1e6 denominator in Raydium config
    let fee_den = 1_000_000.0;
    let trade_fee_ratio = (fee_rate as f64) / fee_den;
    let protocol_fee_ratio_f = (protocol_fee_rate as f64) / fee_den;
    let fund_fee_ratio_f = (fund_fee_rate as f64) / fee_den;
    let lp_fee_ratio = (trade_fee_ratio - protocol_fee_ratio_f - fund_fee_ratio_f).max(0.0);

    // ✅ Spot-only 模式：不订阅任何依赖账户，只订阅主池账户
    let dependent_accounts = if ctx.spot_only_mode() {
        vec![]
    } else {
        vec![vault_0, vault_1]
    };

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: token_mint_0,
        quote_mint: token_mint_1,
        base_decimals: decimals_0,
        quote_decimals: decimals_1,
        price,
        sqrt_price_x64,
        liquidity: Some(liquidity),
        base_reserve: Some(reserve_0 as u128),
        quote_reserve: Some(reserve_1 as u128),
        fees: Some(PoolFees {
            lp_fee_bps: lp_fee_ratio * 10_000.0,
            protocol_fee_bps: protocol_fee_ratio_f * 10_000.0,
            other_fee_bps: fund_fee_ratio_f.max(0.0) * 10_000.0,
            meteora_dynamic: None,
        }),
        slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state: None, // sidecar introspect does not currently return tick arrays
        clmm_accounts: Some(ClmmAccounts {
            program_id,
            amm_config, // ✅ 使用保留的 amm_config
            tick_spacing,
            tick_current_index: tick_current,
            token_mint_a: token_mint_0,
            token_mint_b: token_mint_1,
            token_vault_a: vault_0,
            token_vault_b: vault_1,
            observation_key: get_cached_static_info(&descriptor.address)
                .map(|info| info.observation_key)
                .unwrap_or_default(),
            // ✅ 使用缓存中的 token programs（支持 Token-2022）
            token_a_program: get_cached_static_info(&descriptor.address)
                .map(|info| info.token_program_0)
                .unwrap_or(spl_token::ID),
            token_b_program: get_cached_static_info(&descriptor.address)
                .map(|info| info.token_program_1)
                .unwrap_or(spl_token::ID),
        }),
        damm_state: None,
        damm_accounts: None,
        lb_state: None,
        lb_accounts: None,
        pump_state: None,
        dependent_accounts,
    })
}

/// 本地解码 Raydium CLMM 池账户数据（需要已缓存的静态信息）
///
/// PoolState 账户布局（discriminator 后）：
/// - bump: [u8; 1]
/// - amm_config: Pubkey (32)
/// - owner: Pubkey (32)
/// - token_mint_0: Pubkey (32)
/// - token_mint_1: Pubkey (32)
/// - token_vault_0: Pubkey (32)
/// - token_vault_1: Pubkey (32)
/// - observation_key: Pubkey (32)
/// - mint_decimals_0: u8
/// - mint_decimals_1: u8
/// - tick_spacing: u16
/// - liquidity: u128
/// - sqrt_price_x64: u128
/// - tick_current: i32
/// - padding3: u16
/// - padding4: u16
async fn decode_local(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
    static_info: &RaydiumPoolStaticInfo,
) -> Result<PoolSnapshot> {
    // PoolState 账户最小长度检查
    // discriminator(8) + bump(1) + amm_config(32) + owner(32) + token_mint_0(32) + token_mint_1(32)
    // + token_vault_0(32) + token_vault_1(32) + observation_key(32)
    // + mint_decimals_0(1) + mint_decimals_1(1) + tick_spacing(2)
    // + liquidity(16) + sqrt_price_x64(16) + tick_current(4) + padding3(2) + padding4(2)
    const MIN_POOL_SIZE: usize = 277;

    if data.len() < MIN_POOL_SIZE {
        // 对于老的 / 截断布局，直接返回 LegacyLayoutError，避免误解析
        return Err(LegacyLayoutError.into());
    }

    let mut reader = AccountReader::new(&data[8..]); // Skip discriminator

    reader.read_u8()?; // bump
    reader.read_pubkey()?; // amm_config
    reader.read_pubkey()?; // owner
    let _token_mint_0 = reader.read_pubkey()?;
    let _token_mint_1 = reader.read_pubkey()?;
    let _vault_0 = reader.read_pubkey()?;
    let _vault_1 = reader.read_pubkey()?;
    let observation_key = reader.read_pubkey()?; // observation_key
    let _mint_decimals_0 = reader.read_u8()?;
    let _mint_decimals_1 = reader.read_u8()?;
    let _tick_spacing = reader.read_u16()?;

    // 动态字段
    let liquidity = reader.read_u128()?;
    let sqrt_price_x64 = reader.read_u128()?;
    let tick_current = reader.read_i32()?;
    reader.read_u16()?; // padding3
    reader.read_u16()?; // padding4

    // 计算价格
    let price = q64_to_price(
        sqrt_price_x64,
        static_info.decimals_0,
        static_info.decimals_1,
    )?;

    if !price.is_finite() || price <= 0.0 {
        anyhow::bail!("Invalid price calculated from sqrt_price_x64");
    }

    let observation_key = if observation_key == Pubkey::default() {
        static_info.observation_key
    } else {
        observation_key
    };

    if observation_key != Pubkey::default() && observation_key != static_info.observation_key {
        cache_static_info(
            descriptor.address,
            RaydiumPoolStaticInfo {
                observation_key,
                ..static_info.clone()
            },
        );
    }

    // spot_only 模式：不拉 vault 余额，只用主池 sqrt_price 算价格
    let (reserve_0, reserve_1) = if ctx.spot_only_mode() {
        (0u64, 0u64)
    } else {
        let slot = Some(context.slot);
        tokio::try_join!(
            ctx.get_token_account_amount_with_slot(&static_info.vault_0, slot),
            ctx.get_token_account_amount_with_slot(&static_info.vault_1, slot),
        )?
    };

    // Normalize pair
    let (normalized_pair, normalized_price) = normalize_pair(
        static_info.token_mint_0,
        static_info.token_mint_1,
        price,
    )
    .ok_or_else(|| anyhow!("cannot normalize raydium pair"))?;

    // Calculate fees from cached static info
    let fee_den = 1_000_000.0;
    let trade_fee_ratio = (static_info.fee_rate as f64) / fee_den;
    let protocol_fee_ratio = (static_info.protocol_fee_rate as f64) / fee_den;
    let fund_fee_ratio = (static_info.fund_fee_rate as f64) / fee_den;
    let lp_fee_ratio = (trade_fee_ratio - protocol_fee_ratio - fund_fee_ratio).max(0.0);

    // ✅ 非 spot_only 模式时，构建 ClmmState 以支持精确滑点模拟
    let (clmm_state, dependent_accounts) = if ctx.spot_only_mode() {
        // Spot-only 模式：不订阅任何依赖账户，只订阅主池账户
        (None, vec![])
    } else {
        // 完整模式：拉取 tick arrays 并构建 ClmmState
        let ticks = prefetch_tick_arrays(
            ctx,
            &static_info.program_id,
            &descriptor.address,
            static_info.tick_spacing,
            tick_current,
            context.slot,
        )
        .await
        .unwrap_or_else(|e| {
            warn!("Raydium {} failed to prefetch tick arrays: {}", descriptor.label, e);
            Vec::new()
        });

        let state = if !ticks.is_empty() {
            Some(clmm::ClmmState::new(
                static_info.tick_spacing,
                tick_current,
                ticks,
            ))
        } else {
            None
        };

        // 订阅 vaults + tick arrays
        let mut deps = vec![static_info.vault_0, static_info.vault_1];
        let tick_array_addrs = compute_tick_array_subscriptions(
            &static_info.program_id,
            &descriptor.address,
            tick_current,
            static_info.tick_spacing,
        );
        deps.extend(tick_array_addrs);

        (state, deps)
    };

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: static_info.token_mint_0,
        quote_mint: static_info.token_mint_1,
        base_decimals: static_info.decimals_0,
        quote_decimals: static_info.decimals_1,
        price,
        sqrt_price_x64,
        liquidity: Some(liquidity),
        base_reserve: if ctx.spot_only_mode() { None } else { Some(reserve_0 as u128) },
        quote_reserve: if ctx.spot_only_mode() { None } else { Some(reserve_1 as u128) },
        fees: Some(PoolFees {
            lp_fee_bps: lp_fee_ratio * 10_000.0,
            protocol_fee_bps: protocol_fee_ratio * 10_000.0,
            other_fee_bps: fund_fee_ratio.max(0.0) * 10_000.0,
            meteora_dynamic: None,
        }),
        slot: context.slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state,
        clmm_accounts: Some(ClmmAccounts {
            program_id: static_info.program_id,
            amm_config: static_info.amm_config,
            tick_spacing: static_info.tick_spacing,
            tick_current_index: tick_current,
            token_mint_a: static_info.token_mint_0,
            token_mint_b: static_info.token_mint_1,
            token_vault_a: static_info.vault_0,
            token_vault_b: static_info.vault_1,
            observation_key,
            // ✅ 使用静态信息中的 token programs（支持 Token-2022）
            token_a_program: static_info.token_program_0,
            token_b_program: static_info.token_program_1,
        }),
        damm_state: None,
        damm_accounts: None,
        lb_state: None,
        lb_accounts: None,
        pump_state: None,
        dependent_accounts,
    })
}

async fn prefetch_tick_arrays(
    ctx: &AppContext,
    program_id: &Pubkey,
    pool: &Pubkey,
    tick_spacing: i32,
    tick_current_index: i32,
    current_slot: u64, // ✅ 添加 slot 参数
) -> Result<Vec<clmm::Tick>> {
    let array_span = tick_spacing * TICK_ARRAY_SIZE;
    if array_span == 0 {
        return Ok(Vec::new());
    }
    let current_start = tick_array_start_index(tick_current_index, tick_spacing);
    let mut starts = Vec::new();
    for offset in -PREFETCH_RANGE..=PREFETCH_RANGE {
        starts.push(current_start + offset * array_span);
    }
    starts.sort_unstable();
    starts.dedup();

    let futures = starts
        .iter()
        .map(|start| ctx.get_raydium_tick_array(program_id, pool, *start, current_slot));

    let results = join_all(futures).await;
    let mut ticks = Vec::new();
    for result in results {
        if let Some(state) = result? {
            ticks.extend(state.ticks.iter().cloned());
        }
    }
    Ok(ticks)
}

pub fn tick_array_start_index(tick_index: i32, tick_spacing: i32) -> i32 {
    let span = tick_spacing * TICK_ARRAY_SIZE;
    if span == 0 {
        return 0;
    }
    let mut start = (tick_index / span) * span;
    if tick_index < 0 && tick_index % span != 0 {
        start -= span;
    }
    start
}

pub fn tick_array_address(
    program_id: &Pubkey,
    pool: &Pubkey,
    start_tick_index: i32,
) -> Result<Pubkey> {
    let tick_bytes = start_tick_index.to_be_bytes();
    let (address, _) = Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), tick_bytes.as_ref()],
        program_id,
    );
    Ok(address)
}

pub fn decode_tick_array(data: &[u8]) -> Result<TickArrayState> {
    if data.len() < 8 {
        anyhow::bail!("raydium tick array account too short");
    }
    let mut reader = AccountReader::new(&data[8..]);
    reader.read_pubkey()?; // pool id
    reader.read_i32()?; // start tick index

    let mut ticks = Vec::with_capacity(TICK_ARRAY_SIZE as usize);
    for _ in 0..TICK_ARRAY_SIZE {
        let tick_index = reader.read_i32()?;
        let liquidity_net = reader.read_i128()?;
        reader.read_u128()?; // liquidity gross
        reader.read_u128()?; // fee growth outside 0
        reader.read_u128()?; // fee growth outside 1
        reader.skip(16 * 3)?; // reward growths outside
        if liquidity_net != 0 {
            ticks.push(clmm::Tick {
                index: tick_index,
                liquidity_net,
            });
        }
    }

    reader.read_u8()?; // initialized tick count
    reader.read_u64()?; // recent epoch
    reader.skip(107)?; // padding

    Ok(TickArrayState { ticks })
}

/// 计算给定 tick_current 时需要订阅的 tick array 地址列表（当前 ±PREFETCH_RANGE）
///
/// ✅ 缓存两个方向的起点：
/// - zero_for_one=true: tick_current 为起点
/// - zero_for_one=false: tick_current + tick_spacing 为起点（与 SDK 一致）
/// 返回的地址已去重
pub fn compute_tick_array_subscriptions(
    program_id: &Pubkey,
    pool: &Pubkey,
    tick_current: i32,
    tick_spacing: i32,
) -> Vec<Pubkey> {
    let array_span = tick_spacing * TICK_ARRAY_SIZE;
    if array_span == 0 {
        return vec![];
    }

    // ✅ 计算两个方向的起点
    let start_zero_for_one = tick_array_start_index(tick_current, tick_spacing);
    let start_one_for_zero = tick_array_start_index(tick_current + tick_spacing, tick_spacing);

    let mut addresses = Vec::new();

    // ✅ 添加 zero_for_one 方向的 tick arrays
    for offset in -PREFETCH_RANGE..=PREFETCH_RANGE {
        let start_index = start_zero_for_one + offset * array_span;
        if let Ok(addr) = tick_array_address(program_id, pool, start_index) {
            addresses.push(addr);
        }
    }

    // ✅ 添加 one_for_zero 方向的 tick arrays（如果起点不同）
    if start_one_for_zero != start_zero_for_one {
        for offset in -PREFETCH_RANGE..=PREFETCH_RANGE {
            let start_index = start_one_for_zero + offset * array_span;
            if let Ok(addr) = tick_array_address(program_id, pool, start_index) {
                addresses.push(addr);
            }
        }
    }

    // ✅ 去重
    addresses.sort();
    addresses.dedup();

    addresses
}

/// 预热并验证 Raydium tick arrays（仅保留实际存在的账号）
pub async fn warmup_tick_arrays(
    ctx: &AppContext,
    program_id: &Pubkey,
    pool: &Pubkey,
    tick_current: i32,
    tick_spacing: i32,
) -> Vec<Pubkey> {
    let addresses = compute_tick_array_subscriptions(program_id, pool, tick_current, tick_spacing);
    if addresses.is_empty() {
        return addresses;
    }

    match ctx.get_raydium_tick_arrays_batch(&addresses).await {
        Ok(found) if !found.is_empty() => found.into_iter().map(|(addr, _)| addr).collect(),
        Ok(_) => {
            warn!(
                "Raydium warmup: no tick arrays found for pool {}, using derived list",
                pool
            );
            addresses
        }
        Err(err) => {
            warn!(
                "Raydium warmup: batch fetch failed for pool {}: {}, using derived list",
                pool, err
            );
            addresses
        }
    }
}

/// 检查两个 tick array 地址列表是否相同（忽略顺序）
pub fn tick_arrays_changed(old: &[Pubkey], new: &[Pubkey]) -> bool {
    if old.len() != new.len() {
        return true;
    }

    use std::collections::HashSet;
    let old_set: HashSet<_> = old.iter().collect();
    let new_set: HashSet<_> = new.iter().collect();

    old_set != new_set
}

