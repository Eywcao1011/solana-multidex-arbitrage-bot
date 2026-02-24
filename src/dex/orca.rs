use anyhow::{anyhow, Result};
use chrono::Utc;
use futures::future::join_all;
use log::{debug, warn};
use once_cell::sync::OnceCell;
use solana_client::rpc_response::RpcResponseContext;
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::RwLock;

use super::{
    clmm, normalize_pair, AccountReader, ClmmAccounts, PoolDescriptor, PoolFees, PoolSnapshot,
};
use crate::rpc::{q64_to_price, AppContext};
use crate::sidecar_client::SidecarClient;

const TICK_ARRAY_SIZE: i32 = 88;
// ⭐ 扩大范围到 ±10 以应对极端波动（缓存开销小，覆盖最大化）
const PREFETCH_RANGE: i32 = 10;

// Static sidecar client for Orca introspection
static SIDECAR_CLIENT: OnceCell<Result<SidecarClient, String>> = OnceCell::new();

fn sidecar_client() -> Result<&'static SidecarClient> {
    match SIDECAR_CLIENT.get_or_init(|| {
        SidecarClient::from_env().map_err(|e| e.to_string())
    }) {
        Ok(client) => Ok(client),
        Err(err) => Err(anyhow!("SidecarClient initialization failed: {}", err)),
    }
}

/// 静态信息缓存：存储每个池的不变字段（mints、vaults、decimals、fee 参数等）
/// 这些字段在池创建后不会改变，只需要从 SDK 拉一次
#[derive(Debug, Clone)]
pub struct OrcaPoolStaticInfo {
    pub program_id: Pubkey,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    pub decimals_a: u8,
    pub decimals_b: u8,
    pub tick_spacing: i32,
    pub fee_rate: u16,
    pub protocol_fee_rate: u16,
}

/// 全局静态信息缓存
static POOL_STATIC_CACHE: OnceCell<RwLock<HashMap<Pubkey, OrcaPoolStaticInfo>>> = OnceCell::new();

fn get_static_cache() -> &'static RwLock<HashMap<Pubkey, OrcaPoolStaticInfo>> {
    POOL_STATIC_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

/// 从缓存获取静态信息
fn get_cached_static_info(pool: &Pubkey) -> Option<OrcaPoolStaticInfo> {
    get_static_cache()
        .read()
        .ok()
        .and_then(|cache| cache.get(pool).cloned())
}

/// 将静态信息存入缓存
fn cache_static_info(pool: Pubkey, info: OrcaPoolStaticInfo) {
    if let Ok(mut cache) = get_static_cache().write() {
        cache.insert(pool, info);
    }
}

#[derive(Debug, Clone)]
pub struct TickArrayState {
    pub ticks: Vec<clmm::Tick>,
}

/// Decode Orca Whirlpool pool using hybrid strategy:
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

    // 策略 3：先尝试本地 decode（如果有缓存的静态信息）
    if let Some(static_info) = get_cached_static_info(&pool) {
        match decode_local(descriptor, data, context.clone(), ctx, &static_info).await {
            Ok(snapshot) => {
                debug!("Orca pool {} decoded locally", descriptor.label);
                return Ok(snapshot);
            }
            Err(e) => {
                warn!(
                    "Orca pool {} local decode failed: {}, falling back to SDK",
                    descriptor.label, e
                );
                // Fallback to SDK below
            }
        }
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
        client.introspect_orca(&pool_str)
    ).await
        .map_err(|_| anyhow!("Orca introspect_orca timed out"))?
        .map_err(|e| anyhow!("Orca introspect_orca failed: {}", e))?;

    if !resp.ok {
        return Err(anyhow!(
            "Orca introspect failed: {}",
            resp.error.unwrap_or_else(|| "unknown error".to_string())
        ));
    }

    // Parse required fields
    let token_mint_a = Pubkey::from_str(
        resp.token_mint_a.as_ref().ok_or_else(|| anyhow!("missing token_mint_a"))?,
    )?;
    let token_mint_b = Pubkey::from_str(
        resp.token_mint_b.as_ref().ok_or_else(|| anyhow!("missing token_mint_b"))?,
    )?;
    let token_vault_a = Pubkey::from_str(
        resp.token_vault_a.as_ref().ok_or_else(|| anyhow!("missing token_vault_a"))?,
    )?;
    let token_vault_b = Pubkey::from_str(
        resp.token_vault_b.as_ref().ok_or_else(|| anyhow!("missing token_vault_b"))?,
    )?;
    let program_id = Pubkey::from_str(
        resp.program_id.as_ref().ok_or_else(|| anyhow!("missing program_id"))?,
    )?;

    let decimals_a = resp.decimals_a.ok_or_else(|| anyhow!("missing decimals_a"))?;
    let decimals_b = resp.decimals_b.ok_or_else(|| anyhow!("missing decimals_b"))?;

    let reserve_a: u64 = resp.reserve_a
        .as_ref()
        .ok_or_else(|| anyhow!("missing reserve_a"))?
        .parse()
        .map_err(|_| anyhow!("invalid reserve_a"))?;
    let reserve_b: u64 = resp.reserve_b
        .as_ref()
        .ok_or_else(|| anyhow!("missing reserve_b"))?
        .parse()
        .map_err(|_| anyhow!("invalid reserve_b"))?;

    let sqrt_price_x64: u128 = resp.sqrt_price_x64
        .as_ref()
        .ok_or_else(|| anyhow!("missing sqrt_price_x64"))?
        .parse()
        .map_err(|_| anyhow!("invalid sqrt_price_x64"))?;

    let price: f64 = resp.price
        .as_ref()
        .ok_or_else(|| anyhow!("missing price"))?
        .parse()
        .map_err(|_| anyhow!("invalid price"))?;

    let liquidity: u128 = resp.liquidity
        .as_ref()
        .ok_or_else(|| anyhow!("missing liquidity"))?
        .parse()
        .map_err(|_| anyhow!("invalid liquidity"))?;

    let tick_spacing = resp.tick_spacing.ok_or_else(|| anyhow!("missing tick_spacing"))?;
    let tick_current = resp.tick_current_index.ok_or_else(|| anyhow!("missing tick_current_index"))?;
    let fee_rate = resp.fee_rate.ok_or_else(|| anyhow!("missing fee_rate"))?;
    let protocol_fee_rate = resp.protocol_fee_rate.ok_or_else(|| anyhow!("missing protocol_fee_rate"))?;
    let slot = resp.slot.unwrap_or(context.slot);

    // 缓存静态信息，下次可以本地 decode
    let static_info = OrcaPoolStaticInfo {
        program_id,
        token_mint_a,
        token_mint_b,
        token_vault_a,
        token_vault_b,
        decimals_a,
        decimals_b,
        tick_spacing,
        fee_rate,
        protocol_fee_rate,
    };
    cache_static_info(descriptor.address, static_info);
    debug!("Orca pool {} static info cached from SDK", descriptor.label);

    // Normalize pair
    let (normalized_pair, normalized_price) = normalize_pair(token_mint_a, token_mint_b, price)
        .ok_or_else(|| anyhow!("cannot normalize Orca pair"))?;

    // Calculate fees
    let fee_denominator = 1_000_000.0;
    let total_fee_ratio = (fee_rate as f64) / fee_denominator;
    let protocol_fee_ratio = (protocol_fee_rate as f64) / fee_denominator;
    let lp_fee_ratio = (total_fee_ratio - protocol_fee_ratio).max(0.0);

    // ✅ Spot-only 模式：不订阅任何依赖账户，只订阅主池账户
    let dependent_accounts = if ctx.spot_only_mode() {
        vec![]
    } else {
        vec![token_vault_a, token_vault_b]
    };

    let token_a_program = ctx
        .get_mint_info_from_cache(&token_mint_a)
        .map(|info| info.owner)
        .unwrap_or(spl_token::ID);
    let token_b_program = ctx
        .get_mint_info_from_cache(&token_mint_b)
        .map(|info| info.owner)
        .unwrap_or(spl_token::ID);

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: token_mint_a,
        quote_mint: token_mint_b,
        base_decimals: decimals_a,
        quote_decimals: decimals_b,
        price,
        sqrt_price_x64,
        liquidity: Some(liquidity),
        base_reserve: Some(reserve_a as u128),
        quote_reserve: Some(reserve_b as u128),
        fees: Some(PoolFees {
            lp_fee_bps: lp_fee_ratio * 10_000.0,
            protocol_fee_bps: protocol_fee_ratio * 10_000.0,
            other_fee_bps: 0.0,
            meteora_dynamic: None,
        }),
        slot,
        context,
        timestamp: Utc::now(),
        normalized_pair,
        normalized_price,
        clmm_state: None,
        clmm_accounts: Some(ClmmAccounts {
            program_id,
            amm_config: Pubkey::default(),
            tick_spacing,
            tick_current_index: tick_current,
            token_mint_a,
            token_mint_b,
            token_vault_a,
            token_vault_b,
            observation_key: Pubkey::default(),
            token_a_program,
            token_b_program,
        }),
        damm_state: None,
        damm_accounts: None,
        lb_state: None,
        lb_accounts: None,
        pump_state: None,
        dependent_accounts,
    })
}

/// 本地解码 Orca Whirlpool 账户数据（需要已缓存的静态信息）
/// 
/// Whirlpool 账户布局（discriminator 后）：
/// - whirlpools_config: Pubkey (32)
/// - whirlpool_bump: [u8; 1] (1)
/// - tick_spacing: u16 (2)
/// - tick_spacing_seed: [u8; 2] (2)
/// - fee_rate: u16 (2)
/// - protocol_fee_rate: u16 (2)
/// - liquidity: u128 (16)
/// - sqrt_price: u128 (16)
/// - tick_current_index: i32 (4)
/// - protocol_fee_owed_a: u64 (8)
/// - protocol_fee_owed_b: u64 (8)
/// - token_mint_a: Pubkey (32)
/// - token_vault_a: Pubkey (32)
/// - fee_growth_global_a: u128 (16)
/// - token_mint_b: Pubkey (32)
/// - token_vault_b: Pubkey (32)
/// - fee_growth_global_b: u128 (16)
/// - reward_last_updated_timestamp: u64 (8)
/// - reward_infos: [RewardInfo; 3] (3 * 128 = 384)
async fn decode_local(
    descriptor: &PoolDescriptor,
    data: &[u8],
    context: RpcResponseContext,
    ctx: &AppContext,
    static_info: &OrcaPoolStaticInfo,
) -> Result<PoolSnapshot> {
    // Whirlpool 账户最小长度检查
    // discriminator(8) + config(32) + bump(1) + tick_spacing(2) + tick_spacing_seed(2) 
    // + fee_rate(2) + protocol_fee_rate(2) + liquidity(16) + sqrt_price(16) + tick_current(4)
    // + protocol_fee_owed_a(8) + protocol_fee_owed_b(8) + token_mint_a(32) + token_vault_a(32)
    // + fee_growth_global_a(16) + token_mint_b(32) + token_vault_b(32) + fee_growth_global_b(16)
    // = 8 + 32 + 1 + 2 + 2 + 2 + 2 + 16 + 16 + 4 + 8 + 8 + 32 + 32 + 16 + 32 + 32 + 16 = 251
    const MIN_WHIRLPOOL_SIZE: usize = 251;
    
    if data.len() < MIN_WHIRLPOOL_SIZE {
        anyhow::bail!("Whirlpool account too short: {} < {}", data.len(), MIN_WHIRLPOOL_SIZE);
    }

    let mut reader = AccountReader::new(&data[8..]); // Skip discriminator

    // Skip to dynamic fields
    reader.skip(32)?; // whirlpools_config
    reader.skip(1)?;  // whirlpool_bump
    reader.skip(2)?;  // tick_spacing (we have it in static_info)
    reader.skip(2)?;  // tick_spacing_seed
    reader.skip(2)?;  // fee_rate (we have it in static_info)
    reader.skip(2)?;  // protocol_fee_rate (we have it in static_info)
    
    // Read dynamic fields
    let liquidity = reader.read_u128()?;
    let sqrt_price_x64 = reader.read_u128()?;
    let tick_current = reader.read_i32()?;

    // 计算价格
    let price = q64_to_price(
        sqrt_price_x64,
        static_info.decimals_a,
        static_info.decimals_b,
    )?;

    if !price.is_finite() || price <= 0.0 {
        anyhow::bail!("Invalid price calculated from sqrt_price_x64");
    }

    // spot_only 模式：不拉 vault 余额，只用主池 sqrt_price 算价格
    let (reserve_a, reserve_b) = if ctx.spot_only_mode() {
        (0u64, 0u64)
    } else {
        let slot = Some(context.slot);
        tokio::try_join!(
            ctx.get_token_account_amount_with_slot(&static_info.token_vault_a, slot),
            ctx.get_token_account_amount_with_slot(&static_info.token_vault_b, slot),
        )?
    };

    // Normalize pair
    let (normalized_pair, normalized_price) = normalize_pair(
        static_info.token_mint_a,
        static_info.token_mint_b,
        price,
    )
    .ok_or_else(|| anyhow!("cannot normalize Orca pair"))?;

    // Calculate fees from cached static info
    let fee_denominator = 1_000_000.0;
    let total_fee_ratio = (static_info.fee_rate as f64) / fee_denominator;
    let protocol_fee_ratio = (static_info.protocol_fee_rate as f64) / fee_denominator;
    let lp_fee_ratio = (total_fee_ratio - protocol_fee_ratio).max(0.0);

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
            warn!("Orca {} failed to prefetch tick arrays: {}", descriptor.label, e);
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
        let mut deps = vec![static_info.token_vault_a, static_info.token_vault_b];
        let tick_array_addrs = compute_tick_array_subscriptions(
            &static_info.program_id,
            &descriptor.address,
            tick_current,
            static_info.tick_spacing,
        );
        deps.extend(tick_array_addrs);

        (state, deps)
    };

    let token_a_program = ctx
        .get_mint_info_from_cache(&static_info.token_mint_a)
        .map(|info| info.owner)
        .unwrap_or(spl_token::ID);
    let token_b_program = ctx
        .get_mint_info_from_cache(&static_info.token_mint_b)
        .map(|info| info.owner)
        .unwrap_or(spl_token::ID);

    Ok(PoolSnapshot {
        descriptor: descriptor.clone(),
        base_mint: static_info.token_mint_a,
        quote_mint: static_info.token_mint_b,
        base_decimals: static_info.decimals_a,
        quote_decimals: static_info.decimals_b,
        price,
        sqrt_price_x64,
        liquidity: Some(liquidity),
        base_reserve: if ctx.spot_only_mode() { None } else { Some(reserve_a as u128) },
        quote_reserve: if ctx.spot_only_mode() { None } else { Some(reserve_b as u128) },
        fees: Some(PoolFees {
            lp_fee_bps: lp_fee_ratio * 10_000.0,
            protocol_fee_bps: protocol_fee_ratio * 10_000.0,
            other_fee_bps: 0.0,
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
            amm_config: Pubkey::default(),
            tick_spacing: static_info.tick_spacing,
            tick_current_index: tick_current,
            token_mint_a: static_info.token_mint_a,
            token_mint_b: static_info.token_mint_b,
            token_vault_a: static_info.token_vault_a,
            token_vault_b: static_info.token_vault_b,
            observation_key: Pubkey::default(),
            token_a_program,
            token_b_program,
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
    current_slot: u64,
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
        .map(|start| ctx.get_orca_tick_array(program_id, pool, *start, tick_spacing, current_slot));

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
    let tick_str = start_tick_index.to_string();
    let (address, _) = Pubkey::find_program_address(
        &[b"tick_array", pool.as_ref(), tick_str.as_bytes()],
        program_id,
    );
    Ok(address)
}

pub fn decode_tick_array(data: &[u8], tick_spacing: i32) -> Result<TickArrayState> {
    if data.len() < 8 {
        anyhow::bail!("tick array account too short");
    }
    let mut reader = AccountReader::new(&data[8..]);
    let start_tick_index = reader.read_i32()?;
    let mut ticks = Vec::with_capacity(TICK_ARRAY_SIZE as usize);
    for i in 0..TICK_ARRAY_SIZE {
        let initialized = reader.read_u8()? != 0;
        let liquidity_net = reader.read_i128()?;
        reader.read_u128()?; // liquidity gross
        reader.read_u128()?; // fee growth outside A
        reader.read_u128()?; // fee growth outside B
        reader.skip(16 * 3)?; // reward growths outside
        let tick_index = start_tick_index + i * tick_spacing;
        if initialized && liquidity_net != 0 {
            ticks.push(clmm::Tick {
                index: tick_index,
                liquidity_net,
            });
        }
    }
    reader.read_pubkey()?; // whirlpool
    Ok(TickArrayState { ticks })
}

/// 计算给定 tick_current 时需要订阅的 tick array 地址列表
/// 返回当前 tick array 及其相邻的 ±PREFETCH_RANGE（当前为 ±10）
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

    let current_start = tick_array_start_index(tick_current, tick_spacing);
    let mut addresses = Vec::new();

    for offset in -PREFETCH_RANGE..=PREFETCH_RANGE {
        let start_index = current_start + offset * array_span;
        if let Ok(addr) = tick_array_address(program_id, pool, start_index) {
            addresses.push(addr);
        }
    }

    addresses
}

/// 预热并验证 Orca tick arrays（仅保留实际存在的账号）
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

    match ctx.get_orca_tick_arrays_batch(&addresses, tick_spacing).await {
        Ok(found) if !found.is_empty() => found.into_iter().map(|(addr, _)| addr).collect(),
        Ok(_) => {
            warn!(
                "Orca warmup: no tick arrays found for pool {}, using derived list",
                pool
            );
            addresses
        }
        Err(err) => {
            warn!(
                "Orca warmup: batch fetch failed for pool {}: {}, using derived list",
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

