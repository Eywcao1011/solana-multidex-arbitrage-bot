use std::{
    collections::{HashMap, HashSet},
    path::Path,
    str::FromStr,
    sync::Arc,
};

use anyhow::{anyhow, bail, Context, Result};
use once_cell::sync::OnceCell;
use solana_instruction::{AccountMeta, Instruction};
use solana_pubkey::Pubkey;
use spl_associated_token_account::get_associated_token_address_with_program_id;

use crate::{
    config::dlmm::{DlmmConfig, DlmmPoolConfig},
    constants::METEORA_LB_PROGRAM_ID,
    dex::{meteora_lb, DexKind, PoolSnapshot, TradeSide},
    rpc::AppContext,
    sidecar_client::{Side, SidecarClient},
};

use super::{TradeExecutor, TradePlan, TradeRequest};

#[derive(Default)]
pub struct MeteoraDlmmExecutor;

impl TradeExecutor for MeteoraDlmmExecutor {
    fn dex(&self) -> DexKind {
        DexKind::MeteoraDlmm
    }

    fn prepare_swap(&self, snapshot: &PoolSnapshot, request: &TradeRequest) -> Result<TradePlan> {
        // ✅ 优先检查运行时注入的配置（warmup 自动生成或测试注入）
        if let Some(pool_cfg) = get_injected_pool_config(&snapshot.descriptor.address) {
            log::info!(
                "prepare_swap for {}: using injected config with {} cached_bin_arrays",
                snapshot.descriptor.address,
                pool_cfg.cached_bin_arrays.len()
            );
            let mut cfg = pool_cfg;
            cfg.trade_side = request.side;
            return build_trade_plan(50.0, &cfg, snapshot, request);
        }

        if let Some(config) = load_execution_config()?.as_ref() {
            if let Some(pool_cfg) = config.get_pool(&snapshot.descriptor.address) {
                return build_trade_plan(config.default_slippage_bps, pool_cfg, snapshot, request);
            }
        }

        if let Some(env_config) = load_env_execution_config()?.as_ref() {
            if let Some(pool_cfg) = env_config.pool_config(snapshot)? {
                return build_trade_plan(
                    env_config.default_slippage_bps,
                    &pool_cfg,
                    snapshot,
                    request,
                );
            }
        }

        Err(anyhow!(
            "DLMM pool {} missing in injected config, DLMM_EXECUTION_CONFIG and DLMM_EXECUTION_POOLS",
            snapshot.descriptor.address
        ))
    }
}

static EXECUTION_CONFIG: OnceCell<Option<Arc<DlmmConfig>>> = OnceCell::new();
static ENV_EXECUTION_CONFIG: OnceCell<Option<Arc<DlmmEnvExecution>>> = OnceCell::new();
#[allow(dead_code)]
static SIDECAR_CLIENT: OnceCell<Result<SidecarClient>> = OnceCell::new();

// ✅ 运行时注入的 pool config 缓存（用于 warmup 自动生成或测试注入）
static INJECTED_POOL_CONFIGS: once_cell::sync::Lazy<std::sync::RwLock<HashMap<Pubkey, DlmmPoolConfig>>> =
    once_cell::sync::Lazy::new(|| std::sync::RwLock::new(HashMap::new()));

/// 注入 DLMM pool 配置（用于 warmup 自动生成或测试）
/// 
/// 注入后，`prepare_swap` 会优先使用注入的配置，不再依赖 DLMM_EXECUTION_CONFIG
pub fn inject_pool_config(pool: Pubkey, config: DlmmPoolConfig) {
    if let Ok(mut cache) = INJECTED_POOL_CONFIGS.write() {
        cache.insert(pool, config);
    }
}

/// 获取注入的 pool 配置
pub fn get_injected_pool_config(pool: &Pubkey) -> Option<DlmmPoolConfig> {
    INJECTED_POOL_CONFIGS.read().ok()?.get(pool).cloned()
}

/// 检查池是否已有注入的配置
pub fn has_injected_pool_config(pool: &Pubkey) -> bool {
    INJECTED_POOL_CONFIGS
        .read()
        .ok()
        .map(|cache| cache.contains_key(pool))
        .unwrap_or(false)
}

/// 刷新池子的 cached_bin_arrays（用于 bin_array 相关错误后的自动恢复）
/// 
/// 从 sidecar 重新获取 bin_arrays 并更新已注入的配置。
/// 返回刷新后的 bin_arrays 数量，如果刷新失败返回 Err。
/// 注意：只对有注入配置的 DLMM 池子生效，其他池子会早期返回 Ok(0)。
pub async fn refresh_pool_cached_bin_arrays(
    ctx: &AppContext,
    pool: &Pubkey,
) -> Result<usize> {
    // 检查是否有注入配置
    let has_injected = has_injected_pool_config(pool);
    
    // 如果没有注入配置，检查是否有文件/环境变量配置
    let mut file_config_to_promote: Option<DlmmPoolConfig> = None;
    if !has_injected {
        if let Ok(Some(config)) = load_execution_config() {
            if let Some(pool_cfg) = config.get_pool(pool) {
                file_config_to_promote = Some(pool_cfg.clone());
            }
        }
    }

    // ⭐ 既没有注入配置也不是文件配置的池子（可能是非 DLMM 或未配置），跳过
    if !has_injected && file_config_to_promote.is_none() {
        return Ok(0);
    }
    
    use once_cell::sync::Lazy;
    static REFRESH_SIDECAR: Lazy<Result<SidecarClient, anyhow::Error>> =
        Lazy::new(|| SidecarClient::from_env());
    
    let sidecar = REFRESH_SIDECAR.as_ref()
        .map_err(|e| anyhow!("SidecarClient unavailable for refresh: {}", e))?;
    
    // 从 sidecar 获取最新的 bin_arrays
    let pool_str = pool.to_string();
    let resp = sidecar.introspect_meteora(&pool_str).await?;
    
    if !resp.ok {
        return Err(anyhow!("sidecar introspect returned ok=false: {:?}", resp.error));
    }
    
    let bin_arrays: Vec<Pubkey> = resp
        .bin_arrays
        .unwrap_or_default()
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    
    // ⭐ 解析方向性 bin_arrays
    let x_to_y: Vec<Pubkey> = resp
        .bin_arrays_x_to_y
        .unwrap_or_default()
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();
    let y_to_x: Vec<Pubkey> = resp
        .bin_arrays_y_to_x
        .unwrap_or_default()
        .iter()
        .filter_map(|s| Pubkey::from_str(s).ok())
        .collect();

    let mut combined = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for addr in bin_arrays.iter().chain(x_to_y.iter()).chain(y_to_x.iter()) {
        if seen.insert(*addr) {
            combined.push(*addr);
        }
    }
    let validated = crate::dex::meteora_dlmm::validate_bin_arrays(ctx, pool, &combined).await;
    let valid_set: std::collections::HashSet<Pubkey> = validated.iter().copied().collect();
    let x_to_y: Vec<Pubkey> = x_to_y
        .into_iter()
        .filter(|addr| valid_set.contains(addr))
        .collect();
    let y_to_x: Vec<Pubkey> = y_to_x
        .into_iter()
        .filter(|addr| valid_set.contains(addr))
        .collect();

    let count = validated.len();
    let directional_count = x_to_y.len() + y_to_x.len();
    
    if count > 0 || directional_count > 0 {
        // 更新 AppContext 缓存（merged + directional）
        ctx.set_dlmm_bin_arrays(pool, validated.clone());
        if !x_to_y.is_empty() || !y_to_x.is_empty() {
            ctx.set_dlmm_directional_bin_arrays(pool, x_to_y.clone(), y_to_x.clone());
        }
        
        if has_injected {
            // 更新已注入的配置
            if let Ok(mut cache) = INJECTED_POOL_CONFIGS.write() {
                if let Some(config) = cache.get_mut(pool) {
                    config.cached_bin_arrays = validated.clone();
                    config.cached_bin_arrays_x_to_y = x_to_y;
                    config.cached_bin_arrays_y_to_x = y_to_x;
                    log::info!(
                        "DLMM pool {} refreshed bin_arrays: {} merged, {} x_to_y, {} y_to_x",
                        pool, count, config.cached_bin_arrays_x_to_y.len(), config.cached_bin_arrays_y_to_x.len()
                    );
                }
            }
        } else if let Some(mut cfg) = file_config_to_promote {
            // ⭐ 这是一个文件配置的池子，我们将其"升级"为注入配置
            cfg.cached_bin_arrays = validated;
            cfg.cached_bin_arrays_x_to_y = x_to_y;
            cfg.cached_bin_arrays_y_to_x = y_to_x;
            inject_pool_config(*pool, cfg);
            log::info!(
                "Promoted file-configured DLMM pool {} to injected config with {} refreshed bin_arrays",
                pool, count
            );
        }
    }
    
    Ok(count)
}

/// 检查模拟错误是否是 bin_array 相关的（可通过刷新解决）
pub fn is_bin_array_error(err: &solana_transaction_error::TransactionError) -> bool {
    use solana_instruction_error::InstructionError;
    
    match err {
        solana_transaction_error::TransactionError::InstructionError(_, InstructionError::Custom(code)) => {
            // 3005 = AccountNotEnoughKeys
            // 3007 = AccountOwnedByWrongProgram
            *code == 3005 || *code == 3007
        }
        _ => false,
    }
}

/// 自动生成并注入 DLMM 池配置（用于 spot-only 模式）
/// 
/// 从 PoolSnapshot 自动推导所有技术字段，只需传入签名者公钥。
/// 该配置适用于 Buy 和 Sell 两个方向（trade_side 在运行时覆盖）。
/// 
/// 成功返回 Ok(true) 表示新注入，Ok(false) 表示已存在。
pub fn auto_inject_pool_config(
    ctx: &AppContext,
    snapshot: &PoolSnapshot,
    user: Pubkey,
) -> Result<bool> {
    let pool = snapshot.descriptor.address;
    
    // 已存在则跳过
    if has_injected_pool_config(&pool) {
        return Ok(false);
    }
    
    let lb_accounts = snapshot
        .lb_accounts
        .as_ref()
        .ok_or_else(|| anyhow!("lb_accounts missing for DLMM auto-inject: {}", pool))?;
    
    // 自动派生用户 ATA（token_x/y 保持池子原始顺序）
    let token_x_mint = snapshot.base_mint;
    let token_y_mint = snapshot.quote_mint;

    let user_token_x = derive_associated_token(&user, &token_x_mint, &spl_token::ID);
    let user_token_y = derive_associated_token(&user, &token_y_mint, &spl_token::ID);
    
    // 派生 bin_array_bitmap_extension
    let bitmap_extension = meteora_lb::bitmap_extension_address(
        &METEORA_LB_PROGRAM_ID,
        &pool,
    );
    
    // ⭐ 从 AppContext 获取 sidecar 缓存的 bin_arrays 地址（方向性）
    let cached_bin_arrays = ctx.get_dlmm_bin_arrays(&pool).unwrap_or_default();
    let cached_x_to_y = ctx.get_dlmm_bin_arrays_for_direction(&pool, true).unwrap_or_default();
    let cached_y_to_x = ctx.get_dlmm_bin_arrays_for_direction(&pool, false).unwrap_or_default();
    if !cached_x_to_y.is_empty() || !cached_y_to_x.is_empty() {
        log::debug!(
            "DLMM auto-inject: using directional bin_arrays for pool {} (x_to_y={}, y_to_x={})",
            pool,
            cached_x_to_y.len(),
            cached_y_to_x.len()
        );
    }
    
    let config = DlmmPoolConfig {
        pool,
        trade_side: TradeSide::Sell, // 默认值，运行时会被覆盖
        amount_in: 0,
        slippage_bps: None,
        user,
        user_token_in: user_token_x,
        user_token_out: user_token_y,
        reserve_x: lb_accounts.reserve_x,
        reserve_y: lb_accounts.reserve_y,
        token_x_mint,
        token_y_mint,
        oracle: lb_accounts.oracle,
        host_fee_in: None,
        token_x_program: spl_token::ID,
        token_y_program: spl_token::ID,
        bin_array_bitmap_extension: Some(bitmap_extension),
        additional_remaining_accounts: vec![],
        cached_bin_arrays: cached_bin_arrays.clone(), // deprecated but keep for compat
        cached_bin_arrays_x_to_y: cached_x_to_y.clone(), // ⭐ Sell direction
        cached_bin_arrays_y_to_x: cached_y_to_x.clone(), // ⭐ Buy direction
    };
    
    let bin_arrays_count = cached_x_to_y.len() + cached_y_to_x.len();
    inject_pool_config(pool, config);
    log::info!(
        "DLMM auto-injected config for pool {} (user={}, bin_arrays={})",
        pool,
        user,
        bin_arrays_count
    );
    
    Ok(true)
}

#[allow(dead_code)]
fn sidecar_client() -> Option<&'static SidecarClient> {
    match SIDECAR_CLIENT.get_or_init(SidecarClient::from_env) {
        Ok(client) => Some(client),
        Err(err) => {
            log::warn!("Failed to initialize SidecarClient: {err:?}");
            None
        }
    }
}


#[allow(dead_code)]
fn load_execution_config() -> Result<&'static Option<Arc<DlmmConfig>>> {
    EXECUTION_CONFIG.get_or_try_init(|| {
        let path = std::env::var("DLMM_EXECUTION_CONFIG")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                std::env::var("DLMM_CALIBRATION_CONFIG")
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            });

        match path {
            Some(path) => {
                DlmmConfig::load_from_path(Path::new(&path)).map(|cfg| Some(Arc::new(cfg)))
            }
            None => Ok(None),
        }
    })
}

#[allow(dead_code)]
struct DlmmEnvExecution {
    default_slippage_bps: f64,
    per_pool: HashMap<Pubkey, DlmmEnvPool>,
    global_defaults: Option<DlmmGlobalDefaults>,
}

#[allow(dead_code)]
struct DlmmGlobalDefaults {
    pools: HashSet<Pubkey>,
    user: Pubkey,
    trade_side: TradeSide,
    base_amount: Option<f64>,
    slippage_bps: Option<f64>,
    user_token_in: Option<Pubkey>,
    user_token_out: Option<Pubkey>,
    host_fee_in: Option<Pubkey>,
    token_x_program: Option<Pubkey>,
    token_y_program: Option<Pubkey>,
    bin_array_bitmap_extension: Option<Pubkey>,
    additional_remaining_accounts: Vec<Pubkey>,
}

#[allow(dead_code)]
#[derive(Clone)]
struct DlmmEnvPool {
    pool: Pubkey,
    user: Pubkey,
    trade_side: TradeSide,
    base_amount: Option<f64>,
    slippage_bps: Option<f64>,
    user_token_in: Option<Pubkey>,
    user_token_out: Option<Pubkey>,
    host_fee_in: Option<Pubkey>,
    token_x_program: Option<Pubkey>,
    token_y_program: Option<Pubkey>,
    bin_array_bitmap_extension: Option<Pubkey>,
    additional_remaining_accounts: Vec<Pubkey>,
}

impl DlmmEnvExecution {
    #[allow(dead_code)]
    fn pool_config(&self, snapshot: &PoolSnapshot) -> Result<Option<DlmmPoolConfig>> {
        let pool = snapshot.descriptor.address;
        if let Some(entry) = self.per_pool.get(&pool) {
            return Ok(Some(resolve_env_pool(entry, snapshot)?));
        }
        if let Some(defaults) = &self.global_defaults {
            if defaults.pools.contains(&pool) {
                let entry = defaults.to_env_pool(pool);
                return Ok(Some(resolve_env_pool(&entry, snapshot)?));
            }
        }
        Ok(None)
    }
}

impl DlmmGlobalDefaults {
    #[allow(dead_code)]
    fn to_env_pool(&self, pool: Pubkey) -> DlmmEnvPool {
        DlmmEnvPool {
            pool,
            user: self.user,
            trade_side: self.trade_side,
            base_amount: self.base_amount,
            slippage_bps: self.slippage_bps,
            user_token_in: self.user_token_in,
            user_token_out: self.user_token_out,
            host_fee_in: self.host_fee_in,
            token_x_program: self.token_x_program,
            token_y_program: self.token_y_program,
            bin_array_bitmap_extension: self.bin_array_bitmap_extension,
            additional_remaining_accounts: self.additional_remaining_accounts.clone(),
        }
    }
}

#[allow(dead_code)]
fn load_env_execution_config() -> Result<&'static Option<Arc<DlmmEnvExecution>>> {
    ENV_EXECUTION_CONFIG.get_or_try_init(|| {
        let default_slippage = std::env::var("DLMM_EXECUTION_DEFAULT_SLIPPAGE_BPS")
            .ok()
            .and_then(|raw| raw.parse::<f64>().ok())
            .unwrap_or(50.0)
            .max(0.1);

        let raw = std::env::var("DLMM_EXECUTION_POOLS").unwrap_or_default();
        let entries: Vec<&str> = raw
            .split(|c| matches!(c, '\n' | ';' | ','))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();

        if entries.is_empty() {
            return Ok(None);
        }

        let has_key_values = entries.iter().any(|entry| entry.contains('='));
        let mut per_pool = HashMap::new();
        let mut global_defaults: Option<DlmmGlobalDefaults> = None;

        if has_key_values {
            for entry in &entries {
                let (pool_key, cfg) = parse_dlmm_env_entry(entry)?;
                if per_pool.insert(pool_key, cfg).is_some() {
                    bail!("duplicate DLMM execution entry for pool {pool_key}");
                }
            }
        } else {
            let mut pools = HashSet::new();
            for entry in &entries {
                let pool = parse_pubkey(entry, "DLMM_EXECUTION_POOLS")?;
                if !pools.insert(pool) {
                    bail!("duplicate DLMM_EXECUTION_POOLS entry for pool {pool}");
                }
            }

            let user = parse_pubkey_env("DLMM_EXECUTION_USER")?;
            let trade_side =
                parse_trade_side_env("DLMM_EXECUTION_SIDE")?.unwrap_or(TradeSide::Sell);
            let base_amount = parse_positive_f64_env("DLMM_EXECUTION_AMOUNT")?;
            let slippage_override = parse_non_negative_f64_env("DLMM_EXECUTION_SLIPPAGE_BPS")?;
            let user_token_in = parse_optional_pubkey_env("DLMM_EXECUTION_USER_TOKEN_IN")?;
            let user_token_out = parse_optional_pubkey_env("DLMM_EXECUTION_USER_TOKEN_OUT")?;
            let host_fee_in = parse_optional_pubkey_env("DLMM_EXECUTION_HOST_FEE_IN")?;
            let token_x_program = parse_optional_pubkey_env("DLMM_EXECUTION_TOKEN_X_PROGRAM")?;
            let token_y_program = parse_optional_pubkey_env("DLMM_EXECUTION_TOKEN_Y_PROGRAM")?;
            let bin_extension = parse_optional_pubkey_env("DLMM_EXECUTION_BIN_EXTENSION")?;
            let additional_remaining_accounts =
                parse_additional_accounts_env("DLMM_EXECUTION_ADDITIONAL_ACCOUNTS")?;

            global_defaults = Some(DlmmGlobalDefaults {
                pools,
                user,
                trade_side,
                base_amount,
                slippage_bps: slippage_override,
                user_token_in,
                user_token_out,
                host_fee_in,
                token_x_program,
                token_y_program,
                bin_array_bitmap_extension: bin_extension,
                additional_remaining_accounts,
            });
        }

        Ok(Some(Arc::new(DlmmEnvExecution {
            default_slippage_bps: default_slippage,
            per_pool,
            global_defaults,
        })))
    })
}

#[allow(dead_code)]
fn parse_pubkey_env(name: &str) -> Result<Pubkey> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                bail!("environment variable {name} must not be empty");
            }
            parse_pubkey(trimmed, name)
        }
        Err(std::env::VarError::NotPresent) => {
            bail!("environment variable {name} must be set for DLMM execution")
        }
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_optional_pubkey_env(name: &str) -> Result<Option<Pubkey>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                parse_pubkey(trimmed, name).map(Some)
            }
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_trade_side_env(name: &str) -> Result<Option<TradeSide>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            match trimmed.to_ascii_lowercase().as_str() {
                "sell" => Ok(Some(TradeSide::Sell)),
                "buy" => Ok(Some(TradeSide::Buy)),
                "both" | "any" => Ok(None),
                other => bail!("invalid {name} value '{other}'"),
            }
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_positive_f64_env(name: &str) -> Result<Option<f64>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed: f64 = trimmed
                .parse()
                .with_context(|| format!("parse numeric value for {name}: {trimmed}"))?;
            if parsed <= 0.0 {
                bail!("{name} must be > 0");
            }
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_non_negative_f64_env(name: &str) -> Result<Option<f64>> {
    match std::env::var(name) {
        Ok(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Ok(None);
            }
            let parsed: f64 = trimmed
                .parse()
                .with_context(|| format!("parse numeric value for {name}: {trimmed}"))?;
            if parsed < 0.0 {
                bail!("{name} must be >= 0");
            }
            Ok(Some(parsed))
        }
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_additional_accounts_env(name: &str) -> Result<Vec<Pubkey>> {
    match std::env::var(name) {
        Ok(value) => value
            .split(|c| matches!(c, '\n' | ';' | ',' | '+' | '&'))
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|entry| parse_pubkey(entry, name))
            .collect(),
        Err(std::env::VarError::NotPresent) => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

#[allow(dead_code)]
fn parse_dlmm_env_entry(entry: &str) -> Result<(Pubkey, DlmmEnvPool)> {
    let mut pool: Option<Pubkey> = None;
    let mut user: Option<Pubkey> = None;
    let mut trade_side: Option<TradeSide> = None;
    let mut base_amount: Option<f64> = None;
    let mut slippage: Option<f64> = None;
    let mut user_token_in: Option<Pubkey> = None;
    let mut user_token_out: Option<Pubkey> = None;
    let mut host_fee_in: Option<Pubkey> = None;
    let mut token_x_program: Option<Pubkey> = None;
    let mut token_y_program: Option<Pubkey> = None;
    let mut bin_array_extension: Option<Pubkey> = None;
    let mut additional_accounts: Vec<Pubkey> = Vec::new();

    for part in entry.split('|').map(str::trim).filter(|s| !s.is_empty()) {
        let (key, value) = part
            .split_once('=')
            .with_context(|| format!("invalid key=value pair '{part}' in DLMM_EXECUTION_POOLS"))?;
        let key = key.trim().to_ascii_lowercase();
        let value = value.trim();
        match key.as_str() {
            "pool" => pool = Some(parse_pubkey(value, "pool")?),
            "user" => user = Some(parse_pubkey(value, "user")?),
            "side" | "trade_side" => {
                trade_side = Some(match value.to_ascii_lowercase().as_str() {
                    "sell" => TradeSide::Sell,
                    "buy" => TradeSide::Buy,
                    other => bail!("unknown trade side '{other}' in DLMM_EXECUTION_POOLS entry"),
                });
            }
            "amount" | "amount_base" => {
                base_amount = Some(
                    value
                        .parse::<f64>()
                        .with_context(|| format!("parse amount '{value}' for entry '{entry}'"))?,
                );
            }
            "slippage" | "slippage_bps" => {
                slippage =
                    Some(value.parse::<f64>().with_context(|| {
                        format!("parse slippage '{value}' for entry '{entry}'")
                    })?);
            }
            "user_token_in" => user_token_in = Some(parse_pubkey(value, "user_token_in")?),
            "user_token_out" => user_token_out = Some(parse_pubkey(value, "user_token_out")?),
            "host_fee_in" | "host_fee" => host_fee_in = Some(parse_pubkey(value, "host_fee_in")?),
            "token_x_program" | "token_in_program" => {
                token_x_program = Some(parse_pubkey(value, "token_x_program")?);
            }
            "token_y_program" | "token_out_program" => {
                token_y_program = Some(parse_pubkey(value, "token_y_program")?);
            }
            "bin_extension" | "bin_array_bitmap_extension" => {
                bin_array_extension = Some(parse_pubkey(value, "bin_array_bitmap_extension")?);
            }
            "extra" | "additional" | "remaining" => {
                for addr in value
                    .split(|c| matches!(c, '+' | '&' | ','))
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                {
                    additional_accounts.push(parse_pubkey(addr, "additional_remaining_account")?);
                }
            }
            other => bail!("unknown DLMM execution key '{other}' in entry '{entry}'"),
        }
    }

    let pool =
        pool.ok_or_else(|| anyhow!("missing pool=... in DLMM_EXECUTION_POOLS entry '{entry}'"))?;
    let user = user
        .ok_or_else(|| anyhow!("missing user=... in DLMM_EXECUTION_POOLS entry for pool {pool}"))?;
    let trade_side = trade_side.unwrap_or(TradeSide::Sell);

    Ok((
        pool,
        DlmmEnvPool {
            pool,
            user,
            trade_side,
            base_amount,
            slippage_bps: slippage,
            user_token_in,
            user_token_out,
            host_fee_in,
            token_x_program,
            token_y_program,
            bin_array_bitmap_extension: bin_array_extension,
            additional_remaining_accounts: additional_accounts,
        },
    ))
}

#[allow(dead_code)]
fn resolve_env_pool(env_pool: &DlmmEnvPool, snapshot: &PoolSnapshot) -> Result<DlmmPoolConfig> {
    if snapshot.lb_state.is_none() {
        bail!("lb_state missing for DLMM pool");
    }

    let lb_accounts = snapshot
        .lb_accounts
        .as_ref()
        .ok_or_else(|| anyhow!("lb account metadata missing for DLMM pool"))?;

    let amount_in = if let Some(amount) = env_pool.base_amount {
        units_to_amount_raw(amount, snapshot.base_decimals, Rounding::Floor)?
    } else {
        0
    };

    let token_x_program = env_pool.token_x_program.unwrap_or(spl_token::ID);
    let token_y_program = env_pool.token_y_program.unwrap_or(spl_token::ID);

    let (user_token_in, user_token_out) = match env_pool.trade_side {
        TradeSide::Sell => {
            let token_in = env_pool.user_token_in.unwrap_or_else(|| {
                derive_associated_token(&env_pool.user, &snapshot.base_mint, &token_x_program)
            });
            let token_out = env_pool.user_token_out.unwrap_or_else(|| {
                derive_associated_token(&env_pool.user, &snapshot.quote_mint, &token_y_program)
            });
            (token_in, token_out)
        }
        TradeSide::Buy => {
            let token_in = env_pool.user_token_in.unwrap_or_else(|| {
                derive_associated_token(&env_pool.user, &snapshot.quote_mint, &token_y_program)
            });
            let token_out = env_pool.user_token_out.unwrap_or_else(|| {
                derive_associated_token(&env_pool.user, &snapshot.base_mint, &token_x_program)
            });
            (token_in, token_out)
        }
    };

    Ok(DlmmPoolConfig {
        pool: env_pool.pool,
        trade_side: env_pool.trade_side,
        amount_in,
        slippage_bps: env_pool.slippage_bps,
        user: env_pool.user,
        user_token_in,
        user_token_out,
        reserve_x: lb_accounts.reserve_x,
        reserve_y: lb_accounts.reserve_y,
        token_x_mint: snapshot.base_mint,
        token_y_mint: snapshot.quote_mint,
        oracle: lb_accounts.oracle,
        host_fee_in: env_pool.host_fee_in,
        token_x_program,
        token_y_program,
        bin_array_bitmap_extension: env_pool.bin_array_bitmap_extension,
        additional_remaining_accounts: env_pool.additional_remaining_accounts.clone(),
        cached_bin_arrays: vec![], // deprecated, will be filled during warmup
        cached_bin_arrays_x_to_y: vec![],
        cached_bin_arrays_y_to_x: vec![],
    })
}

#[allow(dead_code)]
fn derive_associated_token(owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Pubkey {
    get_associated_token_address_with_program_id(owner, mint, token_program)
}

#[allow(dead_code)]
fn parse_pubkey(value: &str, field: &str) -> Result<Pubkey> {
    Pubkey::from_str(value).with_context(|| format!("parse pubkey for field {field}: {value}"))
}

fn resolve_user_tokens_for_request(
    snapshot: &PoolSnapshot,
    pool_cfg: &DlmmPoolConfig,
    request: &TradeRequest,
) -> Result<(Pubkey, Pubkey)> {
    let inverted = snapshot.normalized_pair.inverted;
    let has_request_accounts = request.accounts.user_base_account != Pubkey::default()
        && request.accounts.user_quote_account != Pubkey::default();
    let has_config_accounts = pool_cfg.user_token_in != Pubkey::default()
        && pool_cfg.user_token_out != Pubkey::default();

    let user = pool_cfg.user;
    let derived_token_x =
        derive_associated_token(&user, &snapshot.base_mint, &pool_cfg.token_x_program);
    let derived_token_y =
        derive_associated_token(&user, &snapshot.quote_mint, &pool_cfg.token_y_program);

    // request.accounts is normalized (base/quote). Map to token_x/token_y.
    let (user_token_x, user_token_y) = if has_request_accounts {
        if inverted {
            (
                request.accounts.user_quote_account,
                request.accounts.user_base_account,
            )
        } else {
            (
                request.accounts.user_base_account,
                request.accounts.user_quote_account,
            )
        }
    } else if has_config_accounts {
        if pool_cfg.user_token_in == derived_token_x && pool_cfg.user_token_out == derived_token_y {
            (derived_token_x, derived_token_y)
        } else {
            return Ok((pool_cfg.user_token_in, pool_cfg.user_token_out));
        }
    } else {
        (derived_token_x, derived_token_y)
    };

    let (user_token_in, user_token_out) = match request.side {
        TradeSide::Sell => {
            if inverted {
                (user_token_y, user_token_x)
            } else {
                (user_token_x, user_token_y)
            }
        }
        TradeSide::Buy => {
            if inverted {
                (user_token_x, user_token_y)
            } else {
                (user_token_y, user_token_x)
            }
        }
    };

    Ok((user_token_in, user_token_out))
}

#[allow(dead_code)]
fn build_trade_plan(
    default_slippage_bps: f64,
    pool_cfg: &DlmmPoolConfig,
    snapshot: &PoolSnapshot,
    request: &TradeRequest,
) -> Result<TradePlan> {
    if pool_cfg.trade_side != request.side {
        return Err(anyhow!(
            "trade side mismatch: request {:?}, config {:?}",
            request.side,
            pool_cfg.trade_side
        ));
    }

    if snapshot.lb_state.is_none() {
        return Err(anyhow!("lb_state missing for DLMM pool"));
    }

    let _lb_state = snapshot.lb_state.as_ref().unwrap();

    let inverted = snapshot.normalized_pair.inverted;
    let (norm_base_decimals, norm_quote_decimals) = if inverted {
        (snapshot.quote_decimals, snapshot.base_decimals)
    } else {
        (snapshot.base_decimals, snapshot.quote_decimals)
    };

    let (user_token_in, user_token_out) =
        resolve_user_tokens_for_request(snapshot, pool_cfg, request)?;
    let mut cfg = pool_cfg.clone();
    cfg.user_token_in = user_token_in;
    cfg.user_token_out = user_token_out;

    let base_amount_units =
        resolve_base_amount(request.base_amount, pool_cfg.amount_in, norm_base_decimals)?;

    if request.min_amount_out.is_none() {
        bail!("min_amount_out (protection) required for Meteora DLMM execution");
    }

    log::info!(
        "[DLMM DEBUG] build_trade_plan: side={:?} base_amount={:.6} quote_in={:?} min_out={:?} inverted={}",
        request.side,
        request.base_amount,
        request.quote_amount_in,
        request.min_amount_out,
        inverted
    );

    let (
        _token_in_decimals,
        token_out_decimals,
        amount_in_raw,
        amount_in_units,
        expected_out_units,
    ) = match request.side {
        TradeSide::Sell => {
            let amount_in_raw =
                units_to_amount_raw(base_amount_units, norm_base_decimals, Rounding::Floor)?;
            
            // ✅ FIX: For sell leg, use pre-calculated min_quote_out from request.min_amount_out
            let expected_out = request.min_amount_out.unwrap();
            
            (
                norm_base_decimals,
                norm_quote_decimals,
                amount_in_raw,
                base_amount_units,
                expected_out,
            )
        }
        TradeSide::Buy => {
            // Check if we have exact-in semantics (quote_amount_in set)
            let (amount_in_raw, amount_in_units, expected_out) = if let Some(quote_in) = request.quote_amount_in {
                // ✅ Spot-only exact-in mode: fixed quote input, min base output
                let raw = units_to_amount_raw(quote_in, norm_quote_decimals, Rounding::Ceil)?;
                let min_base_out = request.min_amount_out.unwrap_or(0.0);
                (raw, quote_in, min_base_out)
            } else {
                // ✅ Full mode: min_amount_out is max_quote_in, base_amount is expected output
                let raw = units_to_amount_raw(
                    request.min_amount_out.unwrap(),
                    norm_quote_decimals,
                    Rounding::Ceil,
                )?;
                (raw, request.min_amount_out.unwrap(), request.base_amount)
            };
            
            (
                norm_quote_decimals,
                norm_base_decimals,
                amount_in_raw,
                amount_in_units,
                expected_out,
            )
        }
    };

    let slippage = slippage_bps(
        request.max_slippage_bps,
        pool_cfg.slippage_bps,
        default_slippage_bps,
    );
    
    // ✅ FIX: min_amount_out_raw 需要根据 side 分开处理
    // - Sell: min_out 是 quote（SOL），用 quote_decimals
    // - Buy: min_out 应该是 base（MEME），用 base_decimals，从 base_amount 计算
    let min_amount_out_raw = match request.side {
        TradeSide::Sell => {
            // 卖腿：min_out 是预计算的 sell_min_out（quote 数量）
            units_to_amount_raw(request.min_amount_out.unwrap(), token_out_decimals, Rounding::Floor)?
        }
        TradeSide::Buy => {
            if request.quote_amount_in.is_some() {
                // Spot-only exact-in: min_amount_out 已经是保护值，不要再次缩小
                let min_base_out = request.min_amount_out.unwrap();
                units_to_amount_raw(min_base_out, token_out_decimals, Rounding::Floor)?
            } else {
                // Full mode: 根据预期输出和滑点生成保护值
                let min_base_out = expected_out_units * (1.0 - slippage / 10_000.0);
                units_to_amount_raw(min_base_out, token_out_decimals, Rounding::Floor)?
            }
        }
    };

    log::info!(
        "[DLMM DEBUG] build_trade_plan amounts: amount_in_raw={} ({:.6} units) min_out_raw={}",
        amount_in_raw,
        amount_in_units,
        min_amount_out_raw
    );

    // ⭐ 使用本地构建（零延迟）+ 动态 bin_array 选择
    // 从预取的 bin_arrays 中选择当前 active_id 最近的 3 个
    let lb_state = snapshot
        .lb_state
        .as_ref()
        .ok_or_else(|| anyhow!("DLMM {} missing lb_state for local build", snapshot.descriptor.label))?;

    let instruction = build_swap_instruction(
        &cfg,
        snapshot,
        lb_state,
        amount_in_raw,
        min_amount_out_raw,
    )?;

    let description = match request.side {
        TradeSide::Sell => format!(
            "Meteora DLMM SELL {} base {:.6} -> min quote {:.6} (slippage {:.2} bps)",
            snapshot.descriptor.label, amount_in_units, expected_out_units, slippage
        ),
        TradeSide::Buy => format!(
            "Meteora DLMM BUY {} spend {:.6} -> min base {:.6} (slippage {:.2} bps)",
            snapshot.descriptor.label, amount_in_units, expected_out_units, slippage
        ),
    };

    Ok(TradePlan {
        instructions: vec![instruction],
        description,
    })
}

#[allow(dead_code)]
fn slippage_bps(request_slippage: f64, pool_slippage: Option<f64>, default_slippage: f64) -> f64 {
    if request_slippage > 0.0 {
        request_slippage
    } else {
        pool_slippage.unwrap_or(default_slippage)
    }
    .max(0.1)
}

#[allow(dead_code)]
fn compute_min_amount_out(expected_out: f64, slippage_bps: f64, decimals: u8) -> Result<u64> {
    let min_out_units = expected_out * (1.0 - slippage_bps / 10_000.0);
    if min_out_units <= 0.0 {
        return Err(anyhow!("min amount out non-positive"));
    }
    let scale = 10f64.powi(decimals as i32);
    let raw = (min_out_units * scale).floor();
    if raw <= 0.0 {
        return Err(anyhow!("min amount out rounded to zero"));
    }
    Ok(raw as u64)
}

#[allow(dead_code)]
fn resolve_base_amount(request_amount: f64, fallback_raw: u64, base_decimals: u8) -> Result<f64> {
    if request_amount > 0.0 {
        Ok(request_amount)
    } else if fallback_raw > 0 {
        let scale = 10f64.powi(base_decimals as i32);
        Ok(fallback_raw as f64 / scale)
    } else {
        Err(anyhow!("base amount not specified"))
    }
}

#[allow(dead_code)]
enum Rounding {
    Floor,
    Ceil,
}

#[allow(dead_code)]
fn units_to_amount_raw(amount: f64, decimals: u8, rounding: Rounding) -> Result<u64> {
    if !(amount.is_finite() && amount > 0.0) {
        bail!("amount must be positive");
    }
    let scale = 10f64.powi(decimals as i32);
    let scaled = match rounding {
        Rounding::Floor => (amount * scale).floor(),
        Rounding::Ceil => (amount * scale).ceil(),
    };
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

/// Build swap instruction via sidecar HTTP call (uses Meteora SDK for real-time bin_arrays)
/// Kept as fallback option if local build fails
#[allow(dead_code)]
fn build_via_sidecar(
    snapshot: &PoolSnapshot,
    pool_cfg: &DlmmPoolConfig,
    side: TradeSide,
    amount_in_raw: u64,
    min_amount_out_raw: u64,
) -> Result<Instruction> {
    let client = sidecar_client()
        .ok_or_else(|| anyhow!("SidecarClient required for Meteora DLMM sidecar fallback"))?;

    let pool_str = snapshot.descriptor.address.to_string();
    let side = match side {
        TradeSide::Sell => Side::InBase,
        TradeSide::Buy => Side::InQuote,
    };

    let user = pool_cfg.user;
    let user_token_in = pool_cfg.user_token_in;
    let user_token_out = pool_cfg.user_token_out;

    // Synchronous bridge into async SidecarClient without blocking the async runtime
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| {
        handle.block_on(client.build_meteora_dlmm_swap_ix(
            &pool_str,
            side,
            amount_in_raw,
            min_amount_out_raw,
            &user,
            Some(&user_token_in),
            Some(&user_token_out),
        ))
    })
}

/// Build swap instruction locally (zero network latency)
fn build_swap_instruction(
    pool_cfg: &DlmmPoolConfig,
    snapshot: &PoolSnapshot,
    lb_state: &meteora_lb::LbState,
    amount_in: u64,
    min_amount_out: u64,
) -> Result<Instruction> {
    const SWAP_DISCRIMINATOR: [u8; 8] = [248, 198, 158, 145, 225, 117, 135, 200];

    let mut data = Vec::with_capacity(24);
    data.extend_from_slice(&SWAP_DISCRIMINATOR);
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_amount_out.to_le_bytes());

    log::info!(
        "[DLMM DEBUG] build_swap_instruction: pool={} amount_in={} min_out={} token_x={} token_y={}",
        snapshot.descriptor.address,
        amount_in,
        min_amount_out,
        pool_cfg.token_x_mint,
        pool_cfg.token_y_mint
    );

    use crate::dex::meteora_lb::bin_array_index_for;
    let active_index = bin_array_index_for(lb_state._active_id);
    
    // ⭐ 只保留已确认存在的 bin_arrays（基于 lb_state），避免传入未初始化账号导致 3007
    let mut valid_arrays: HashSet<Pubkey> = HashSet::new();
    for idx in lb_state.bin_array_indexes.iter().copied() {
        let addr = meteora_lb::bin_array_address(
            &METEORA_LB_PROGRAM_ID,
            &snapshot.descriptor.address,
            idx,
        );
        valid_arrays.insert(addr);
    }
    let filter_to_valid = !valid_arrays.is_empty();

    // ⭐ 处理 bin_array_bitmap_extension：
    // - 只有当 bin_array index 超出默认 bitmap 范围时才需要扩展
    // - 需要时使用派生 PDA，否则使用 program ID 占位符
    let bitmap_extension_pda = pool_cfg.bin_array_bitmap_extension.unwrap_or_else(|| {
        meteora_lb::bitmap_extension_address(&METEORA_LB_PROGRAM_ID, &snapshot.descriptor.address)
    });
    const DEFAULT_BIN_ARRAY_BITMAP_SIZE: i64 = 512; // Meteora SDK BIN_ARRAY_BITMAP_SIZE
    let min_allowed = -DEFAULT_BIN_ARRAY_BITMAP_SIZE;
    let max_allowed = DEFAULT_BIN_ARRAY_BITMAP_SIZE - 1;
    let needs_extension = {
        let mut min_idx = active_index;
        let mut max_idx = active_index;
        for &idx in &lb_state.bin_array_indexes {
            if idx < min_idx {
                min_idx = idx;
            }
            if idx > max_idx {
                max_idx = idx;
            }
        }
        active_index < min_allowed
            || active_index > max_allowed
            || min_idx < min_allowed
            || max_idx > max_allowed
    };
    let bitmap_extension = if needs_extension {
        bitmap_extension_pda
    } else {
        METEORA_LB_PROGRAM_ID
    };

    let mut metas = vec![
        AccountMeta::new(snapshot.descriptor.address, false),
        AccountMeta::new(bitmap_extension, false),
        AccountMeta::new(pool_cfg.reserve_x, false),
        AccountMeta::new(pool_cfg.reserve_y, false),
        AccountMeta::new(pool_cfg.user_token_in, false),
        AccountMeta::new(pool_cfg.user_token_out, false),
        AccountMeta::new_readonly(pool_cfg.token_x_mint, false),
        AccountMeta::new_readonly(pool_cfg.token_y_mint, false),
        AccountMeta::new(pool_cfg.oracle, false),
    ];

    // ⭐ host_fee_in 必须始终占用这个位置来保持账户顺序正确
    // 如果没有 host_fee_in，使用 program ID 作为占位符（这是 Meteora SDK 的做法）
    let host_fee_account = pool_cfg.host_fee_in.unwrap_or(METEORA_LB_PROGRAM_ID);
    metas.push(AccountMeta::new(host_fee_account, false));

    metas.push(AccountMeta::new_readonly(pool_cfg.user, true));
    metas.push(AccountMeta::new_readonly(pool_cfg.token_x_program, false));
    metas.push(AccountMeta::new_readonly(pool_cfg.token_y_program, false));

    let event_authority =
        Pubkey::find_program_address(&[b"__event_authority"], &METEORA_LB_PROGRAM_ID).0;
    metas.push(AccountMeta::new_readonly(event_authority, false));

    // ⭐ IDL 要求的 program 账户 - 必须在 bin_arrays 之前！
    // 这是 3005 错误的根本原因：缺少这个账户会导致 AccountNotEnoughKeys
    metas.push(AccountMeta::new_readonly(METEORA_LB_PROGRAM_ID, false));

    // ⭐ 根据交易方向选择正确的 bin_arrays 列表
    // SDK 的 getBinArrayForSwap 已经返回正确顺序的数组，所以直接取前 N 个即可
    // 默认 10 个，可通过环境变量 DLMM_MAX_BIN_ARRAYS 调整
    let max_bin_arrays: usize = std::env::var("DLMM_MAX_BIN_ARRAYS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    
    // ⭐ 基于已知的 bin arrays 选择 active 附近的数组，避免传入未初始化的 PDA
    let active_range: i64 = std::env::var("DLMM_ACTIVE_RANGE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2)
        .max(1); // active ±N 范围

    let mut known_arrays: HashSet<Pubkey> = HashSet::new();
    known_arrays.extend(pool_cfg.cached_bin_arrays.iter().copied());
    known_arrays.extend(pool_cfg.cached_bin_arrays_x_to_y.iter().copied());
    known_arrays.extend(pool_cfg.cached_bin_arrays_y_to_x.iter().copied());
    for idx in lb_state.bin_array_indexes.iter().copied() {
        let addr = meteora_lb::bin_array_address(
            &METEORA_LB_PROGRAM_ID,
            &snapshot.descriptor.address,
            idx,
        );
        known_arrays.insert(addr);
    }

    let mut active_derived_arrays: Vec<Pubkey> = lb_state
        .bin_array_indexes
        .iter()
        .copied()
        .filter(|idx| (idx - active_index).abs() <= active_range)
        .map(|idx| {
            meteora_lb::bin_array_address(
                &METEORA_LB_PROGRAM_ID,
                &snapshot.descriptor.address,
                idx,
            )
        })
        .collect();

    if active_derived_arrays.is_empty() && !known_arrays.is_empty() {
        for offset in -active_range..=active_range {
            let idx = active_index + offset;
            let addr = meteora_lb::bin_array_address(
                &METEORA_LB_PROGRAM_ID,
                &snapshot.descriptor.address,
                idx,
            );
            if known_arrays.contains(&addr) {
                active_derived_arrays.push(addr);
            }
        }
    }
    
    let mut remaining: Vec<Pubkey> = {
        // 根据 trade_side 选择正确方向的 bin_arrays
        let directional_arrays = match pool_cfg.trade_side {
            TradeSide::Sell => &pool_cfg.cached_bin_arrays_x_to_y,  // X→Y
            TradeSide::Buy => &pool_cfg.cached_bin_arrays_y_to_x,   // Y→X
        };
        
        let mut directional_arrays: Vec<Pubkey> = directional_arrays.iter().copied().collect();
        if filter_to_valid {
            let before = directional_arrays.len();
            directional_arrays.retain(|addr| valid_arrays.contains(addr));
            let after = directional_arrays.len();
            if before != after {
                log::debug!(
                    "DLMM {} ({:?}): filtered directional bin_arrays {before} -> {after} (valid={})",
                    snapshot.descriptor.label,
                    pool_cfg.trade_side,
                    valid_arrays.len()
                );
            }
        }
        
        if !directional_arrays.is_empty() {
            // ⭐ 合并: 先添加 active 派生的数组，再添加 sidecar 的 directional 数组
            let mut combined: Vec<Pubkey> = active_derived_arrays.clone();
            for addr in directional_arrays.iter() {
                if !combined.contains(addr) {
                    combined.push(*addr);
                }
            }
            
            // 按 bin_array index 排序（升序），取前 max_bin_arrays 个
            combined.sort_by_key(|addr| {
                for offset in -100i64..=100 {
                    let idx = active_index + offset;
                    let derived = meteora_lb::bin_array_address(
                        &METEORA_LB_PROGRAM_ID,
                        &snapshot.descriptor.address,
                        idx,
                    );
                    if derived == *addr {
                        return idx;
                    }
                }
                i64::MAX
            });
            
            let selected: Vec<Pubkey> = combined.into_iter().take(max_bin_arrays).collect();
            
            log::info!(
                "DLMM {} ({:?}): using {} bin_arrays (active_derived={}, directional={}, max={})",
                snapshot.descriptor.label,
                pool_cfg.trade_side,
                selected.len(),
                active_derived_arrays.len(),
                directional_arrays.len(),
                max_bin_arrays
            );
            
            selected
        } else if !pool_cfg.cached_bin_arrays.is_empty() {
            // Fallback: 使用 merged cached_bin_arrays，并与 active_derived 合并
            // 为每个 bin_array 计算其 index
            let cached_arrays: Vec<Pubkey> = if filter_to_valid {
                pool_cfg
                    .cached_bin_arrays
                    .iter()
                    .copied()
                    .filter(|addr| valid_arrays.contains(addr))
                    .collect()
            } else {
                pool_cfg.cached_bin_arrays.clone()
            };
            if filter_to_valid && cached_arrays.is_empty() {
                log::debug!(
                    "DLMM {}: filtered all cached bin_arrays (valid={})",
                    snapshot.descriptor.label,
                    valid_arrays.len()
                );
            }
            
            let mut arrays_with_index: Vec<(Pubkey, i64)> = cached_arrays
                .iter()
                .filter_map(|addr| {
                    // 尝试反推 bin_array 的 index
                    for offset in -100i64..=100 {
                        let idx = active_index + offset;
                        let derived = meteora_lb::bin_array_address(
                            &METEORA_LB_PROGRAM_ID,
                            &snapshot.descriptor.address,
                            idx,
                        );
                        if derived == *addr {
                            return Some((*addr, idx));
                        }
                    }
                    None
                })
                .collect();
            
            // 添加 active_derived_arrays
            for addr in active_derived_arrays.iter() {
                let exists = arrays_with_index.iter().any(|(a, _)| a == addr);
                if !exists {
                    // 计算 index
                    for offset in -active_range..=active_range {
                        let idx = active_index + offset;
                        let derived = meteora_lb::bin_array_address(
                            &METEORA_LB_PROGRAM_ID,
                            &snapshot.descriptor.address,
                            idx,
                        );
                        if derived == *addr {
                            arrays_with_index.push((*addr, idx));
                            break;
                        }
                    }
                }
            }
            
            // 按距离 active_index 排序（越近越前）
            arrays_with_index.sort_by_key(|(_, idx)| (*idx - active_index).abs());
            
            // 取最近的 max_bin_arrays 个
            let selected: Vec<Pubkey> = arrays_with_index
                .into_iter()
                .take(max_bin_arrays)
                .map(|(addr, _)| addr)
                .collect();
            
            log::info!(
                "DLMM {} bin_arrays (fallback+active): {} cached + {} active_derived, selected {} (active_id={})",
                snapshot.descriptor.label,
                pool_cfg.cached_bin_arrays.len(),
                active_derived_arrays.len(),
                selected.len(),
                lb_state._active_id
            );
            
            // 必须按 bin_array_index 升序排列！否则会报 3007 错误
            let mut sorted = selected;
            sorted.sort_by_key(|addr| {
                for offset in -100i64..=100 {
                    let idx = active_index + offset;
                    let derived = meteora_lb::bin_array_address(
                        &METEORA_LB_PROGRAM_ID,
                        &snapshot.descriptor.address,
                        idx,
                    );
                    if derived == *addr {
                        return idx;
                    }
                }
                i64::MAX
            });
            
            sorted
        } else {
            // ⚠️ 缓存全空，无法安全构建交易
            // Fallback 到 lb_state.bin_array_indexes 可能包含太多 bin_arrays 导致交易过大
            bail!(
                "DLMM {} has no cached bin_arrays (directional or merged); run warmup_bin_arrays first",
                snapshot.descriptor.label
            );
        }
    };
    remaining.extend(pool_cfg.additional_remaining_accounts.iter().copied());

    let mut seen: HashSet<Pubkey> = metas.iter().map(|meta| meta.pubkey).collect();
    for key in remaining {
        if seen.insert(key) {
            metas.push(AccountMeta::new(key, false));
        }
    }

    Ok(Instruction {
        program_id: METEORA_LB_PROGRAM_ID,
        accounts: metas,
        data,
    })
}

// Tests removed - now using sidecar SDK exclusively

