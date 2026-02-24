use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use chrono::{DateTime, Duration as ChronoDuration, Local, Utc};
use dashmap::DashMap;
use log::{debug, error, info, warn};
use once_cell::sync::Lazy;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::{
    rpc_config::RpcAccountInfoConfig,
    rpc_response::{Response as RpcResponse, RpcResponseContext},
};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use solana_keypair::Keypair;
use solana_signer::Signer;
use tokio::sync::{OnceCell, Semaphore};
use tokio::task::spawn_blocking;
use tokio::time::{sleep, MissedTickBehavior};

use crate::calibration;
use crate::subscription::AccountUpdate;
use crate::log_utils::append_metrics_log;
use crate::dex::{
    opportunity_detector::OpportunityDetector, // ✅ 新的优化检测器
    snapshot_from_account_data,
    snapshot_from_update,
    DexKind,
    NormalizedPair,
    Opportunity,
    PoolSnapshot,
    PoolDescriptor,
    TradeSide,
    SOL_MINT,
    PriceThresholds,
    SnapshotStore,
};
use crate::pool_manager::PoolManager;
use crate::rpc::AppContext;
use crate::sidecar_client::SidecarClient;
use crate::subscription::{AccountSubscriptionManager, SubscriptionProvider};
use crate::grpc_subscription::GrpcSubscriptionManager;
use crate::log_utils::{append_opportunity_log, append_pool_state_log, append_trade_log, rotate_trade_log};
use crate::trade::{
    accounts::TradeAccountManager,
    arbitrage::AtomicTradePlan,
    dispatcher::TradeDispatcher,
    execution::AtomicExecutor,
    strategy::OpportunityFilter,
    TradeAccounts,
    TradePlan,
    TradeRequest,
};

static SIGNER_KEYPAIR: OnceCell<Option<Arc<Keypair>>> = OnceCell::const_new();

async fn load_signer_keypair_from_env() -> Option<Arc<Keypair>> {
    let keypair = SIGNER_KEYPAIR
        .get_or_init(|| async {
            let path = std::env::var("SOLANA_SIGNER_KEYPAIR").unwrap_or_default();
            if path.trim().is_empty() {
                return None;
            }
            let path = path.trim().to_string();
            match spawn_blocking(move || solana_keypair::read_keypair_file(&path).ok()).await {
                Ok(Some(keypair)) => Some(Arc::new(keypair)),
                Ok(None) => {
                    warn!("Failed to load signer keypair");
                    None
                }
                Err(err) => {
                    warn!("Failed to spawn keypair load task: {}", err);
                    None
                }
            }
        })
        .await;

    keypair.clone()
}

async fn signer_pubkey_from_env() -> Option<Pubkey> {
    load_signer_keypair_from_env().await.map(|kp| kp.pubkey())
}

async fn init_subscription_provider(
    ws_url: &str,
    commitment: CommitmentConfig,
) -> Result<(Arc<SubscriptionProvider>, String)> {
    let subscription_mode = std::env::var("SUBSCRIPTION_MODE")
        .unwrap_or_else(|_| "ws".to_string())
        .trim()
        .to_lowercase();

    let (manager, effective_mode) = match subscription_mode.as_str() {
        "grpc" => {
            let grpc_endpoint = std::env::var("GRPC_ENDPOINT").unwrap_or_default();
            if grpc_endpoint.trim().is_empty() {
                anyhow::bail!("GRPC_ENDPOINT is required when SUBSCRIPTION_MODE=grpc");
            }
            let grpc_x_token = std::env::var("GRPC_X_TOKEN")
                .ok()
                .filter(|token| !token.trim().is_empty());
            info!("Using gRPC subscription mode (endpoint: {})", grpc_endpoint);
            match GrpcSubscriptionManager::new(grpc_endpoint, grpc_x_token, commitment).await {
                Ok(mgr) => (Arc::new(SubscriptionProvider::Grpc(mgr)), "grpc".to_string()),
                Err(e) => {
                    error!("Failed to initialize gRPC subscription manager: {:?}", e);
                    anyhow::bail!("gRPC initialization failed: {}", e);
                }
            }
        }
        _ => {
            info!("Using WebSocket subscription mode (url: {})", ws_url);
            (
                Arc::new(SubscriptionProvider::WebSocket(
                    AccountSubscriptionManager::new(ws_url.to_string(), commitment),
                )),
                "ws".to_string(),
            )
        }
    };

    Ok((manager, effective_mode))
}

/// 获取当前运行模式：local_only / simulate / live
fn get_run_mode() -> &'static str {
    use std::sync::OnceLock;
    static MODE: OnceLock<&'static str> = OnceLock::new();
    *MODE.get_or_init(|| {
        let exec_enabled = std::env::var("ENABLE_TRADE_EXECUTION")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        let dry_run = std::env::var("ARBITRAGE_DRY_RUN_ONLY")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);

        if !exec_enabled {
            "local_only"
        } else if dry_run {
            "simulate"
        } else {
            "live"
        }
    })
}

fn is_dry_run_mode() -> bool {
    std::env::var("ARBITRAGE_DRY_RUN_ONLY")
        .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false)
}

fn get_pool_slippage_filter_bps() -> f64 {
    std::env::var("POOL_FILTER_MAX_SLIPPAGE_BPS")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(50.0)
}

fn get_pool_slippage_trade_size_quote() -> f64 {
    if let Ok(raw) = std::env::var("POOL_FILTER_TRADE_SIZE_QUOTE") {
        if let Ok(value) = raw.parse::<f64>() {
            if value > 0.0 {
                return value;
            }
        }
    }
    std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| *value > 0.0)
        .unwrap_or(0.5)
}

fn get_dry_run_trade_size_quote() -> Result<f64> {
    let raw = std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
        .map_err(|_| anyhow::anyhow!("ARBITRAGE_TRADE_SIZE_QUOTE is required for dry run"))?;
    let value = raw
        .trim()
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("invalid ARBITRAGE_TRADE_SIZE_QUOTE: {}", raw))?;
    if !(value.is_finite() && value > 0.0) {
        anyhow::bail!("ARBITRAGE_TRADE_SIZE_QUOTE must be positive");
    }
    Ok(value)
}

/// 滑点刷新间隔（分钟），默认 30 分钟。设为 0 禁用定时刷新。
fn get_slippage_refresh_interval_mins() -> u64 {
    std::env::var("SLIPPAGE_REFRESH_INTERVAL_MINS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(30)
}

/// Tick/bin 缓存刷新阈值（slot），默认 300。设为 0 禁用基于 slot 的刷新。
fn get_tick_bin_cache_refresh_slots() -> u64 {
    std::env::var("TICK_BIN_CACHE_REFRESH_SLOTS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(300)
}

/// Tick/bin 缓存刷新最小间隔（slot），防止过于频繁刷新。
fn get_tick_bin_cache_refresh_min_gap_slots() -> u64 {
    std::env::var("TICK_BIN_CACHE_REFRESH_MIN_GAP_SLOTS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(50)
}

/// 买腿滑点额外缓冲 (bps)，默认 10 bps = 0.1%
fn get_buy_slippage_buffer_bps() -> f64 {
    std::env::var("BUY_SLIPPAGE_BUFFER_BPS")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(10.0)
}

fn get_static_tip_lamports() -> u64 {
    std::env::var("JITO_TIP_LAMPORTS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .unwrap_or(0)
}

fn get_dynamic_tip_profit_pct() -> f64 {
    std::env::var("DYNAMIC_TIP_PROFIT_PCT")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(30.0)
}

fn get_update_concurrency() -> usize {
    if let Ok(raw) = std::env::var("PROCESS_UPDATE_CONCURRENCY") {
        if let Ok(value) = raw.trim().parse::<usize>() {
            if value > 0 {
                return value;
            }
        }
        warn!("Invalid PROCESS_UPDATE_CONCURRENCY={}, using default", raw.trim());
    }
    std::thread::available_parallelism()
        .map(|value| (value.get() / 2).max(1))
        .unwrap_or(1)
}

fn get_update_process_timeout() -> Option<Duration> {
    match std::env::var("PROCESS_UPDATE_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
    {
        Some(0) => None,
        Some(secs) => Some(Duration::from_secs(secs)),
        None => Some(Duration::from_secs(15)),
    }
}

static UPDATE_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(get_update_concurrency()));

fn compute_dynamic_tip_lamports(est_profit_pct: f64, quote_mint: &Pubkey) -> u64 {
    if !est_profit_pct.is_finite() || est_profit_pct <= 0.0 {
        return 0;
    }
    if *quote_mint != *SOL_MINT {
        return 0;
    }

    let trade_size_quote = std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value > 0.0)
        .unwrap_or(0.0);
    if trade_size_quote <= 0.0 {
        return 0;
    }

    let dynamic_tip_profit_pct = get_dynamic_tip_profit_pct();
    if dynamic_tip_profit_pct <= 0.0 {
        return 0;
    }

    // Dynamic tip: est_profit_pct * (DYNAMIC_TIP_PROFIT_PCT%)
    let dynamic_tip_pct = est_profit_pct * (dynamic_tip_profit_pct / 100.0);
    if dynamic_tip_pct <= 0.0 {
        return 0;
    }
    let tip_sol = trade_size_quote * (dynamic_tip_pct / 100.0);
    if !tip_sol.is_finite() || tip_sol <= 0.0 {
        return 0;
    }

    (tip_sol * 1_000_000_000.0).round().max(0.0) as u64
}

#[derive(Debug, Clone)]
struct ActiveOpportunity {
    started_at: DateTime<Utc>,
    last_seen: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
struct PoolCacheState {
    last_seen_slot: u64,
    last_refresh_slot: u64,
    last_tick_start: Option<i32>,
    last_tick_spacing: Option<i32>,
    last_active_index: Option<i64>,
}

/// 池隔离状态：跟踪连续失败次数和隔离时间
#[derive(Debug, Clone)]
struct QuarantineState {
    consecutive_failures: usize,
    last_error: String,
    quarantined_until: Option<DateTime<Utc>>,
}

impl QuarantineState {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            last_error: String::new(),
            quarantined_until: None,
        }
    }

    fn is_quarantined(&self) -> bool {
        if let Some(until) = self.quarantined_until {
            Utc::now() < until
        } else {
            false
        }
    }

    fn record_failure(&mut self, error: &str) {
        let normalized = normalize_quarantine_error(error);
        // 如果是相同的错误，增加计数；如果是不同错误，重置计数
        if self.last_error == normalized {
            self.consecutive_failures += 1;
        } else {
            self.consecutive_failures = 1;
            self.last_error = normalized;
        }
    }

    fn enter_quarantine(&mut self, duration_minutes: i64) {
        self.quarantined_until = Some(Utc::now() + ChronoDuration::minutes(duration_minutes));
    }

    fn reset(&mut self) {
        self.consecutive_failures = 0;
        self.last_error.clear();
        self.quarantined_until = None;
    }
}

fn normalize_quarantine_error(error: &str) -> String {
    if let Some(idx) = error.find("task ") {
        return error[..idx].trim_end().to_string();
    }
    if error.contains("AccountNotFound") {
        return "AccountNotFound".to_string();
    }

    let s = if error.len() > 200 { &error[..200] } else { error };
    s.to_string()
}

fn append_problem_pool_log(descriptor: &PoolDescriptor, error: &str) {
    let ts = Local::now().to_rfc3339();
    // 额外写入池子状态日志，方便统一查看问题池
    append_pool_state_log(&format!(
        "PROBLEM\t{}\t{:?}\t{}\t{}\t{}",
        ts,
        descriptor.kind,
        descriptor.label,
        descriptor.address,
        error.replace('\n', " "),
    ));
}

/// 确保 ATA 存在，如果不存在则创建（用于 warmup 阶段）
async fn ensure_ata_exists(
    ctx: &AppContext,
    owner: &Pubkey,
    mint: &Pubkey,
) -> Result<()> {
    use spl_associated_token_account::get_associated_token_address_with_program_id;
    use solana_transaction::Transaction;
    use solana_signer::Signer;  // ← for pubkey()
    use std::sync::OnceLock;
    use tokio::sync::Mutex;
    
    // ✅ 创建中锁：防止并发 warmup 对同一 ATA 发送多个交易
    static CREATING_ATAS: OnceLock<Mutex<HashSet<Pubkey>>> = OnceLock::new();
    let creating_atas = CREATING_ATAS.get_or_init(|| Mutex::new(HashSet::new()));

    // ⚠️ 添加超时 (5s)
    let token_program = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.get_mint_info(mint)
    ).await {
        Ok(Ok(info)) => info.owner,
        Ok(Err(err)) => {
            warn!(
                "[ATA] Failed to fetch mint info for {}: {:?}; skipping ATA ensure",
                mint, err
            );
            return Ok(());
        }
         Err(_) => {
            warn!("[ATA] Timeout fetching mint info for {}; skipping", mint);
            return Ok(());
        }
    };

    let ata = get_associated_token_address_with_program_id(owner, mint, &token_program);
    
    // ✅ 检查是否正在创建中（避免并发发送多个交易）
    {
        let mut guard = creating_atas.lock().await;
        if guard.contains(&ata) {
            debug!("[ATA] Already being created: {} for mint {}", ata, mint);
            return Ok(());
        }
        // 标记为正在创建
        guard.insert(ata);
    }
    
    // ✅ 检查 ATA 是否已存在于链上 (5s 超时)
    let ata_check_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.rpc_client().get_account(&ata)
    ).await;

    match ata_check_result {
        Ok(Ok(_)) => {
            debug!("[ATA] Already exists on-chain: {} for mint {}", ata, mint);
            // 移除创建中标记
            let mut guard = creating_atas.lock().await;
            guard.remove(&ata);
            return Ok(());
        }
        Ok(Err(e)) => {
            let err_str = e.to_string();
            let err_lower = err_str.to_lowercase();
            
            // 检查是否是"账户不存在"类型的错误
            let is_not_found = err_lower.contains("accountnotfound")
                || err_lower.contains("account not found")
                || err_lower.contains("could not find account")
                || err_lower.contains("account does not exist");
            
            if is_not_found {
                // ATA 确实不存在，需要创建
                debug!("[ATA] Account not found, will create: {}", ata);
            } else {
                // 其他 RPC 错误（网络问题等），跳过避免发送多余交易
                warn!("[ATA] RPC error checking ATA {}: {}; skipping to avoid duplicate tx", ata, err_str);
                let mut guard = creating_atas.lock().await;
                guard.remove(&ata);
                return Ok(());
            }
        }
        Err(_) => {
            warn!("[ATA] Timeout checking ATA {} for mint {}; skipping", ata, mint);
            let mut guard = creating_atas.lock().await;
            guard.remove(&ata);
            return Ok(());
        }
    }

    
    // 创建 ATA
    info!("[ATA] Creating ATA {} for mint {}", ata, mint);
    
    let create_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
        owner,
        owner,
        mint,
        &token_program,
    );
    
    // ✅ 获取 blockhash (5s 超时)
    let blockhash_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.rpc_client().get_latest_blockhash()
    ).await;

    let blockhash = match blockhash_result {
        Ok(Ok(bh)) => bh,
        Ok(Err(e)) => {
            warn!("[ATA] Failed to get blockhash for ATA {}: {}", ata, e);
            let mut guard = creating_atas.lock().await;
            guard.remove(&ata);
            return Err(anyhow::anyhow!("Failed to get blockhash: {}", e));
        }
        Err(_) => {
            warn!("[ATA] Timeout getting blockhash for ATA {}", ata);
            let mut guard = creating_atas.lock().await;
            guard.remove(&ata);
            return Err(anyhow::anyhow!("Timeout getting blockhash"));
        }
    };

    let signer = match load_signer_keypair_from_env().await {
        Some(kp) => kp,
        None => {
            warn!("[ATA] No signer keypair available for ATA creation");
            let mut guard = creating_atas.lock().await;
            guard.remove(&ata);
            return Ok(());
        }
    };

    let tx = Transaction::new_signed_with_payer(
        &[create_ix],
        Some(&signer.pubkey()),
        &[&*signer],
        blockhash,
    );

    // ⭐ 使用 send_transaction 而不是 send_and_confirm（避免阻塞）
    // ATA 创建是 idempotent 的，失败了也没关系，添加 5s 超时
    match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.rpc_client().send_transaction(&tx)
    ).await {
        Ok(Ok(sig)) => {
            info!("[ATA] Sent ATA creation tx for {} (sig: {})", mint, sig);
        }
        Ok(Err(e)) => {
            // idempotent 创建，即使失败也可能是因为已存在
            debug!("[ATA] ATA creation tx failed (may already exist): {}", e);
        }
        Err(_) => {
            warn!("[ATA] Timeout sending ATA creation tx for {}", mint);
        }
    }

    // 清理创建中标记
    let mut guard = creating_atas.lock().await;
    guard.remove(&ata);
    Ok(())
}

async fn token_program_for_mint(ctx: &AppContext, mint: &Pubkey) -> Option<Pubkey> {
    if let Some(info) = ctx.get_mint_info_from_cache(mint) {
        return Some(info.owner);
    }

    let fetch_result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        ctx.get_mint_info(mint),
    )
    .await;

    match fetch_result {
        Ok(Ok(info)) => Some(info.owner),
        Ok(Err(err)) => {
            warn!("Failed to fetch mint info for {}: {:?}; skipping ATA handling", mint, err);
            None
        }
        Err(_) => {
            warn!("Timeout fetching mint info for {}; skipping ATA handling", mint);
            None
        }
    }
}

async fn collect_user_accounts_for_pool(
    ctx: &AppContext,
    account_manager: &TradeAccountManager,
    snapshot: &PoolSnapshot,
    default_payer: Option<Pubkey>,
) -> Vec<Pubkey> {
    use spl_associated_token_account::get_associated_token_address_with_program_id;

    let mut accounts = HashSet::new();

    let (norm_base_mint, norm_quote_mint) = if snapshot.normalized_pair.inverted {
        (snapshot.quote_mint, snapshot.base_mint)
    } else {
        (snapshot.base_mint, snapshot.quote_mint)
    };

    let mut base_program: Option<Pubkey> = None;
    let mut quote_program: Option<Pubkey> = None;

    for side in [TradeSide::Buy, TradeSide::Sell] {
        let mut user_accounts = account_manager.resolve_for(snapshot, side);

        if user_accounts.payer == Pubkey::default() {
            if let Some(payer) = default_payer {
                user_accounts.payer = payer;
            }
        }

        if user_accounts.payer != Pubkey::default() {
            accounts.insert(user_accounts.payer);
        }

        if user_accounts.user_base_account != Pubkey::default() {
            accounts.insert(user_accounts.user_base_account);
        }
        if user_accounts.user_quote_account != Pubkey::default() {
            accounts.insert(user_accounts.user_quote_account);
        }

        if user_accounts.payer != Pubkey::default() {
            if user_accounts.user_base_account == Pubkey::default() {
                let program = if let Some(program) = base_program {
                    Some(program)
                } else {
                    let program = token_program_for_mint(ctx, &norm_base_mint).await;
                    if let Some(program) = program {
                        base_program = Some(program);
                    }
                    program
                };
                if let Some(program) = program {
                    let ata = get_associated_token_address_with_program_id(
                        &user_accounts.payer,
                        &norm_base_mint,
                        &program,
                    );
                    accounts.insert(ata);
                } else {
                    debug!(
                        "Skipping base ATA injection for {}: token program unknown",
                        norm_base_mint
                    );
                }
            }

            if user_accounts.user_quote_account == Pubkey::default() {
                let program = if let Some(program) = quote_program {
                    Some(program)
                } else {
                    let program = token_program_for_mint(ctx, &norm_quote_mint).await;
                    if let Some(program) = program {
                        quote_program = Some(program);
                    }
                    program
                };
                if let Some(program) = program {
                    let ata = get_associated_token_address_with_program_id(
                        &user_accounts.payer,
                        &norm_quote_mint,
                        &program,
                    );
                    accounts.insert(ata);
                } else {
                    debug!(
                        "Skipping quote ATA injection for {}: token program unknown",
                        norm_quote_mint
                    );
                }
            }
        }

        if snapshot.descriptor.kind == DexKind::PumpFunDlmm && user_accounts.payer != Pubkey::default() {
            let user_volume = Pubkey::find_program_address(
                &[b"user_volume_accumulator", user_accounts.payer.as_ref()],
                &crate::constants::PUMP_FUN_PROGRAM_ID,
            )
            .0;
            accounts.insert(user_volume);
        }
        
        // ✅ 始终添加用户的 wSOL ATA（所有交易都需要）
        if user_accounts.payer != Pubkey::default() {
            let wsol_mint = *crate::dex::SOL_MINT;
            let wsol_ata = get_associated_token_address_with_program_id(
                &user_accounts.payer,
                &wsol_mint,
                &spl_token::ID,  // wSOL 使用标准 Token 程序
            );
            accounts.insert(wsol_ata);
        }
    }

    accounts.into_iter().collect()
}

async fn resolve_dry_run_accounts(
    ctx: &AppContext,
    account_manager: &TradeAccountManager,
    snapshot: &PoolSnapshot,
    executor: &AtomicExecutor,
) -> TradeAccounts {
    let mut accounts = account_manager.resolve_for(snapshot, TradeSide::Buy);

    let owner = if accounts.payer != Pubkey::default() {
        accounts.payer
    } else {
        executor.signer_pubkey()
    };
    accounts.payer = owner;

    let (norm_base_mint, norm_quote_mint) = if snapshot.normalized_pair.inverted {
        (snapshot.quote_mint, snapshot.base_mint)
    } else {
        (snapshot.base_mint, snapshot.quote_mint)
    };

    if accounts.user_base_account == Pubkey::default() {
        let program = token_program_for_mint(ctx, &norm_base_mint)
            .await
            .unwrap_or(spl_token::ID);
        accounts.user_base_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner,
                &norm_base_mint,
                &program,
            );
    }

    if accounts.user_quote_account == Pubkey::default() {
        let program = token_program_for_mint(ctx, &norm_quote_mint)
            .await
            .unwrap_or(spl_token::ID);
        accounts.user_quote_account =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner,
                &norm_quote_mint,
                &program,
            );
    }

    accounts
}

fn min_base_out_for_snapshot(snapshot: &PoolSnapshot) -> f64 {
    let decimals = if snapshot.normalized_pair.inverted {
        snapshot.quote_decimals
    } else {
        snapshot.base_decimals
    };
    let scale = 10f64.powi(decimals as i32);
    let min_unit = if scale.is_finite() && scale > 0.0 {
        1.0 / scale
    } else {
        0.000001
    };
    if min_unit.is_finite() && min_unit > 0.0 {
        min_unit
    } else {
        0.000001
    }
}

fn build_wsol_wrap_instructions(
    payer: Pubkey,
    quote_amount: f64,
    skip_wrap: bool,
) -> Vec<solana_instruction::Instruction> {
    if skip_wrap || payer == Pubkey::default() {
        return Vec::new();
    }

    let lamports = (quote_amount * 1_000_000_000.0).round();
    if !(lamports.is_finite() && lamports > 0.0 && lamports <= u64::MAX as f64) {
        return Vec::new();
    }
    let lamports = lamports as u64;

    let wsol_ata = spl_associated_token_account::get_associated_token_address_with_program_id(
        &payer,
        &SOL_MINT,
        &spl_token::ID,
    );

    let mut instructions = Vec::new();
    instructions.push(
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &payer,
            &payer,
            &SOL_MINT,
            &spl_token::ID,
        ),
    );
    instructions.push(solana_system_interface::instruction::transfer(
        &payer,
        &wsol_ata,
        lamports,
    ));
    instructions.push(
        spl_token::instruction::sync_native(&spl_token::ID, &wsol_ata)
            .expect("sync_native instruction"),
    );

    instructions
}

type OpportunityTracker = Arc<DashMap<NormalizedPair, ActiveOpportunity>>;
type QuarantineTracker = Arc<DashMap<solana_pubkey::Pubkey, QuarantineState>>;
type SlippageCache = Arc<DashMap<Pubkey, PoolSlippage>>;
type PoolCacheTracker = Arc<DashMap<Pubkey, PoolCacheState>>;

#[derive(Clone, Copy, Debug)]
struct PoolSlippage {
    buy_slippage_pct: f64,
    sell_slippage_pct: f64,
}

#[derive(Clone, Debug)]
struct SimulatedPoolResult {
    descriptor: PoolDescriptor,
    pair: NormalizedPair,
    buy_slippage_pct: f64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
struct PoolSimStats {
    total: usize,
    ok: usize,
    failed: usize,
    skipped: usize,
    unpaired_dropped: usize,
    sampled_updated: usize,
    sampled_errors: usize,
    used_simulated: bool,
}

fn collect_descriptors_from_active_pools(
    active_pools: &Arc<DashMap<Pubkey, PoolSubscriptionHandle>>,
) -> Vec<PoolDescriptor> {
    active_pools
        .iter()
        .map(|entry| entry.value().descriptor.clone())
        .collect()
}

async fn refresh_tick_bin_caches_for_pools(
    ctx: &AppContext,
    pools: &[PoolDescriptor],
    clear_cache: bool,
) {
    if clear_cache {
        ctx.clear_warmup_caches();
        crate::trade::orca::clear_tick_arrays_cache();
        crate::trade::raydium::clear_tick_arrays_cache();
    }

    if !ctx.spot_only_mode() {
        return;
    }

    let dlmm_pools: Vec<Pubkey> = pools
        .iter()
        .filter(|descriptor| descriptor.kind == crate::dex::DexKind::MeteoraDlmm)
        .map(|descriptor| descriptor.address)
        .collect();
    let clmm_pools: Vec<(Pubkey, crate::dex::DexKind)> = pools
        .iter()
        .filter(|descriptor| {
            descriptor.kind == crate::dex::DexKind::OrcaWhirlpool
                || descriptor.kind == crate::dex::DexKind::RaydiumClmm
        })
        .map(|descriptor| (descriptor.address, descriptor.kind))
        .collect();

    prewarm_meteora_dlmm_batch(ctx, &dlmm_pools).await;
    prewarm_clmm_tick_arrays_batch(ctx, &clmm_pools).await;
}

pub async fn run_monitor(
    ctx: AppContext,
    ws_url: String,
    pools: Vec<PoolDescriptor>,
    thresholds: PriceThresholds,
    commitment: CommitmentConfig,
) -> Result<()> {
    let mut pools = pools;
    if pools.is_empty() {
        anyhow::bail!("no pools configured");
    }

    info!("Preparing RPC subscriptions via {}", ws_url);
    let slippage_cache: SlippageCache = Arc::new(DashMap::new());
    let pool_slippage_bps = get_pool_slippage_filter_bps();
    let pool_slippage_trade_size_quote = get_pool_slippage_trade_size_quote();
    if pool_slippage_trade_size_quote > 0.0 {
        info!(
            "[PoolSlippage] Sampling enabled: trade_size_quote={}, buffer_pct=0.5 (max_slippage_bps={})",
            pool_slippage_trade_size_quote,
            pool_slippage_bps
        );
    } else {
        info!("[PoolSlippage] Sampling disabled (POOL_FILTER_TRADE_SIZE_QUOTE<=0)");
    }

    let store: SnapshotStore = Arc::new(Default::default());
    let tracker: OpportunityTracker = Arc::new(DashMap::new());
    let quarantine: QuarantineTracker = Arc::new(DashMap::new());
    let cache_tracker: PoolCacheTracker = Arc::new(DashMap::new());
    // ✅ 创建优化的检测器
    let detector = Arc::new(OpportunityDetector::new());
    let account_manager = Arc::new(TradeAccountManager::from_env());
    let atomic_executor = if thresholds.execution_enabled {
        match AtomicExecutor::from_env(ctx.clone()).await? {
            Some(exec) => Some(Arc::new(exec)),
            None => {
                warn!("ENABLE_TRADE_EXECUTION=true but SOLANA_SIGNER_KEYPAIR not set; execution disabled");
                None
            }
        }
    } else {
        None
    };

    // 创建机会筛选策略
    let strategy = if thresholds.execution_enabled {
        match OpportunityFilter::from_env() {
            Ok(filter) => Some(filter),
            Err(err) => {
                warn!(
                    "Failed to load opportunity filter config: {:?}; using no filter",
                    err
                );
                None
            }
        }
    } else {
        None
    };

    let dispatcher = TradeDispatcher::from_env(
        Arc::clone(&account_manager),
        atomic_executor.clone(),
        strategy.clone(),
        ctx.clone(),
    );
    // ✅ 创建订阅提供器（根据 SUBSCRIPTION_MODE 环境变量选择 WS 或 gRPC）
    let (subscription_manager, subscription_mode) =
        init_subscription_provider(&ws_url, commitment).await?;
    info!("SubscriptionProvider initialized (mode: {})", subscription_mode);

    // ✅ 设置到 AppContext 以便优先使用订阅缓存
    ctx.set_subscription_manager(Arc::clone(&subscription_manager))
        .await;

    let mut handles = Vec::new();

    pools = warmup_pools_with_retry(&ctx, pools, Duration::from_secs(30)).await;
    pools = filter_pool_descriptors_by_pair_key(&ctx, pools, None).await;
    if pools.is_empty() {
        anyhow::bail!("no pools available after warmup/pair filter");
    }

    if pool_slippage_trade_size_quote > 0.0 {
        let (filtered, _stats) = apply_simulated_slippage_filter(
            &ctx,
            commitment,
            account_manager.as_ref(),
            atomic_executor.as_deref(),
            &slippage_cache,
            pool_slippage_trade_size_quote,
            pools.clone(),
            None,
            "",
        )
        .await;
        let keep: HashSet<Pubkey> = filtered.iter().map(|d| d.address).collect();
        pools.retain(|descriptor| keep.contains(&descriptor.address));
    }

    // ✅ ALT 自动填充：在开始订阅前，收集所有池子的静态账户并确保在 ALT 中
    if std::env::var("ALT_AUTO_EXTEND")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
    {
        info!("ALT_AUTO_EXTEND enabled, collecting static accounts from pools...");
        
        let account_manager = TradeAccountManager::from_env();
        let signer_keypair = load_signer_keypair_from_env().await;
        let default_payer = signer_keypair.as_ref().map(|kp| kp.pubkey());

        // ✅ 第一步：收集所有 pair 的 mint，检查/创建 ATA
        if let Some(ref keypair) = signer_keypair {
            info!("Step 1: Checking/creating ATAs for all mints in pairs...");
            
            // 收集所有唯一的 mint
            let mut all_mints: HashSet<Pubkey> = HashSet::new();
            for descriptor in &pools {
                if let Ok(account) = ctx.rpc_client().get_account(&descriptor.address).await {
                    let context = RpcResponseContext {
                        slot: 0,
                        api_version: None,
                    };
                    if let Ok(snapshot) = crate::dex::snapshot_from_account_data(descriptor, account.data, context, &ctx).await {
                        all_mints.insert(snapshot.base_mint);
                        all_mints.insert(snapshot.quote_mint);
                    }
                }
            }
            
            info!("Found {} unique mints across all pools", all_mints.len());
            
            // 检查/创建每个 mint 的 ATA
            let mut created_count = 0;
            let mut existing_count = 0;
            
            for mint in &all_mints {
                // 获取 token program
                let token_program = match token_program_for_mint(&ctx, mint).await {
                    Some(program) => program,
                    None => {
                        warn!(
                            "[ATA] Skip creation for mint {}: token program unavailable",
                            mint
                        );
                        continue;
                    }
                };
                
                // 计算 ATA 地址
                let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                    &keypair.pubkey(),
                    mint,
                    &token_program,
                );
                
                // 检查 ATA 是否存在
                match ctx.rpc_client().get_account(&ata).await {
                    Ok(_) => {
                        existing_count += 1;
                    }
                    Err(_) => {
                        // ATA 不存在，创建它
                        let create_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                            &keypair.pubkey(),
                            &keypair.pubkey(),
                            mint,
                            &token_program,
                        );
                        
                        match ctx.rpc_client().get_latest_blockhash().await {
                            Ok(blockhash) => {
                                let tx = solana_transaction::Transaction::new_signed_with_payer(
                                    &[create_ata_ix],
                                    Some(&keypair.pubkey()),
                                    &[&**keypair],
                                    blockhash,
                                );
                                
                                match ctx.rpc_client().send_and_confirm_transaction(&tx).await {
                                    Ok(sig) => {
                                        created_count += 1;
                                        debug!("Created ATA for mint {} (sig: {})", mint, sig);
                                    }
                                    Err(e) => {
                                        warn!("Failed to create ATA for mint {}: {:?}", mint, e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to get blockhash for ATA creation: {:?}", e);
                            }
                        }
                    }
                }
            }
            
            info!("ATA check complete: {} existing, {} created", existing_count, created_count);
        }

        // ✅ 第二步：收集所有池子账户并添加到 ALT
        // 注意：在 spot_only 模式下，需要先调用 warmup 来填充静态缓存
        info!("Step 2: Collecting pool accounts for ALT...");
        
        // 按 DEX 类型收集账户：Meteora → ALT #1，其他 → ALT #2
        let mut pool_accounts: Vec<(String, crate::DexKind, Vec<Pubkey>)> = Vec::new();
        
        // 遍历池子获取初始快照并收集静态账户
        for descriptor in &pools {
            match ctx.rpc_client().get_account(&descriptor.address).await {
                Ok(account) => {
                    let context = RpcResponseContext {
                        slot: 0,
                        api_version: None,
                    };
                    
                    // ✅ 对于需要缓存的 DEX，先调用 warmup 填充静态信息
                    // 这样即使在 spot_only 模式下也能正确收集账户
                    match descriptor.kind {
                        DexKind::RaydiumAmmV4 => {
                            // Raydium AMM V4 需要通过 sidecar 获取静态信息
                            if let Err(e) = crate::dex::raydium_amm::warmup_static_info(&descriptor.address).await {
                                warn!("Failed to warmup Raydium AMM V4 pool {} for ALT: {:?}", descriptor.label, e);
                                continue;
                            }
                        }
                        DexKind::RaydiumCpmm => {
                            // Raydium CPMM 需要解析账户获取静态信息
                            if let Err(e) = crate::dex::raydium_cpmm::warmup_static_info(descriptor, &account.data, &ctx).await {
                                warn!("Failed to warmup Raydium CPMM pool {} for ALT: {:?}", descriptor.label, e);
                                continue;
                            }
                        }
                        _ => {}
                    }
                    
                    // ✅ 选择合适的 context：
                    // - Pump.fun / Raydium AMM/CPMM: 使用非 spot_only（需要 RPC 获取 vault 余额）
                    // - 其他: 使用 spot_only 避免 CLMM tick array prefetch
                    let alt_ctx = if matches!(
                        descriptor.kind,
                        DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
                    ) {
                        ctx.clone_non_spot_only()
                    } else {
                        ctx.clone_for_alt_collection()
                    };
                    
                    match crate::dex::snapshot_from_account_data(descriptor, account.data, context, &alt_ctx).await {
                        Ok(snapshot) => {
                            let mut accounts = crate::dex::collect_static_accounts_for_pool(&snapshot);
                            let user_accounts = collect_user_accounts_for_pool(
                                &ctx,
                                &account_manager,
                                &snapshot,
                                default_payer,
                            )
                            .await;
                            if !user_accounts.is_empty() {
                                accounts.extend(user_accounts);
                                accounts.sort();
                                accounts.dedup();
                            }
                            pool_accounts.push((descriptor.label.clone(), descriptor.kind, accounts));
                        }
                        Err(e) => {
                            warn!("Failed to get snapshot for pool {} for ALT: {:?}", descriptor.label, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch pool {} for ALT collection: {:?}", descriptor.label, e);
                }
            }
        }
        
        info!("Collected accounts for {} pools", pool_accounts.len());
        
        // 尝试获取 signer keypair 用作 ALT authority
        if let Some(ref exec) = atomic_executor {
            if let Some(ref keypair) = signer_keypair {
                match crate::alt_manager::AltManager::from_env(
                    ctx.rpc_client(),
                    Arc::clone(keypair),
                )
                .await
                {
                    Ok(mut alt_manager) => {
                        match alt_manager.ensure_pool_accounts(&pool_accounts).await {
                            Ok(()) => {
                                let alt_addrs = alt_manager.alt_addresses();
                                if !alt_addrs.is_empty() {
                                    info!(
                                        "ALT setup complete: {} ALT(s) with {} total addresses",
                                        alt_addrs.len(),
                                        alt_manager.total_addresses()
                                    );
                                    for (i, addr) in alt_addrs.iter().enumerate() {
                                        info!("  ALT #{}: {}", i + 1, addr);
                                    }
                                }
                                match exec.refresh_lookup_tables().await {
                                    Ok(count) => {
                                        info!("Refreshed ALT cache after population ({} table(s))", count);
                                    }
                                    Err(e) => {
                                        warn!("Failed to refresh ALT cache after population: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to ensure pool accounts in ALT: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to initialize ALT manager: {:?}", e);
                    }
                };
            } else {
                warn!("ALT_AUTO_EXTEND enabled but SOLANA_SIGNER_KEYPAIR is missing; skipping ALT population");
            }
        } else {
            debug!("No atomic executor, skipping ALT auto-extend");
        }
    }

    let refresh_pools = pools.clone();
    let slippage_refresh_handle = {
        let slippage_refresh_mins = get_slippage_refresh_interval_mins();
        if slippage_refresh_mins > 0 && pool_slippage_trade_size_quote > 0.0 {
            let ctx_clone = ctx.clone();
            let slippage_cache_clone = Arc::clone(&slippage_cache);
            let account_manager_clone = Arc::clone(&account_manager);
            let atomic_executor_clone = atomic_executor.clone();
            let pools_clone = refresh_pools.clone();
            let trade_size_quote = pool_slippage_trade_size_quote;

            info!(
                "[PoolSlippage] Background refresh enabled: interval={}min",
                slippage_refresh_mins
            );
            Some(tokio::spawn(async move {
                let interval = Duration::from_secs(slippage_refresh_mins * 60);
                let mut ticker = tokio::time::interval(interval);
                ticker.tick().await;

                loop {
                    ticker.tick().await;
                    let start = std::time::Instant::now();
                    refresh_tick_bin_caches_for_pools(&ctx_clone, &pools_clone, false).await;
                    let _ = apply_simulated_slippage_filter(
                        &ctx_clone,
                        commitment,
                        account_manager_clone.as_ref(),
                        atomic_executor_clone.as_deref(),
                        &slippage_cache_clone,
                        trade_size_quote,
                        pools_clone.clone(),
                        None,
                        " refresh",
                    )
                    .await;
                    info!(
                        "[PoolSlippage] Background refresh done (elapsed={:?})",
                        start.elapsed()
                    );
                }
            }))
        } else {
            info!(
                "[PoolSlippage] Background refresh disabled (interval={}min, trade_size={})",
                slippage_refresh_mins, pool_slippage_trade_size_quote
            );
            None
        }
    };

    // ✅ 串行化启动池子订阅，避免同时发起大量 WS 连接导致 429
    const POOL_SPAWN_INTERVAL_MS: u64 = 200;
    let pool_count = pools.len();
    info!("Starting {} pool subscriptions with {}ms interval between each...", pool_count, POOL_SPAWN_INTERVAL_MS);

    for (i, descriptor) in pools.into_iter().enumerate() {
        let ctx = ctx.clone();
        let store = Arc::clone(&store);
        let thresholds = thresholds.clone();
        let cmt = commitment;
        let ws_url = ws_url.clone();
        let tracker = Arc::clone(&tracker);
        let detector = Arc::clone(&detector);
        let subscription_manager = Arc::clone(&subscription_manager);
        let quarantine = Arc::clone(&quarantine);
        let dispatcher = Arc::clone(&dispatcher);
        let strategy_clone = strategy.clone();
        let slippage_cache = Arc::clone(&slippage_cache);
        let cache_tracker = Arc::clone(&cache_tracker);

        handles.push(tokio::spawn(async move {
            if let Err(err) = subscribe_pool(
                ws_url,
                ctx,
                store,
                descriptor,
                thresholds,
                cmt,
                tracker,
                detector,
                subscription_manager,
                quarantine,
                dispatcher,
                strategy_clone,
                slippage_cache,
                None,
                cache_tracker,
            )
            .await
            {
                error!("Subscription ended with error: {err:?}");
            }
        }));

        // 每启动一个池子后等待一小段时间，避免同时发起太多 WS 连接
        if i + 1 < pool_count {
            sleep(Duration::from_millis(POOL_SPAWN_INTERVAL_MS)).await;
        }
    }

    info!("Subscriptions started; press Ctrl+C to stop");
    info!("Background subscriptions running");
    tokio::signal::ctrl_c().await?;
    info!("Shutdown requested; cancelling subscriptions");

    if let Some(handle) = slippage_refresh_handle {
        handle.abort();
    }

    // ✅ 先关闭订阅管理器
    subscription_manager.shutdown().await;

    // 然后取消所有池子订阅任务
    for handle in handles {
        handle.abort();
    }
    Ok(())
}

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(120);
// ✅ 增加到 10 分钟，因为低流动性池子可能很久没有交易但连接仍然正常
const MAX_IDLE_BEFORE_RECONNECT: Duration = Duration::from_secs(600);
const SHORT_RECONNECT_DELAY: Duration = Duration::from_secs(3);
const CONNECT_RETRY_INITIAL: Duration = Duration::from_secs(1);
const CONNECT_RETRY_MAX: Duration = Duration::from_secs(30);
fn get_slippage_buffer_pct() -> f64 {
    std::env::var("SLIPPAGE_BUFFER_PCT")
        .ok()
        .and_then(|raw| raw.parse::<f64>().ok())
        .unwrap_or(0.5)
}

/// 订阅上下文：封装订阅管理器和跟踪状态
struct SubscriptionContext<'a> {
    manager: &'a SubscriptionProvider,
    subscribed_accounts: &'a mut HashSet<solana_pubkey::Pubkey>,
    raydium_tick_arrays: &'a mut Vec<solana_pubkey::Pubkey>,
    orca_tick_arrays: &'a mut Vec<solana_pubkey::Pubkey>,
    meteora_bin_arrays: &'a mut Vec<solana_pubkey::Pubkey>,
}

async fn subscribe_pool(
    _ws_url: String,
    ctx: AppContext,
    store: SnapshotStore,
    descriptor: PoolDescriptor,
    thresholds: PriceThresholds,
    _commitment: CommitmentConfig,
    tracker: OpportunityTracker,
    detector: Arc<OpportunityDetector>, // ✅ 接收检测器
    subscription_manager: Arc<SubscriptionProvider>, // ✅ 接收订阅提供器
    quarantine: QuarantineTracker,      // ✅ 接收隔离跟踪器
    dispatcher: Arc<TradeDispatcher>,
    strategy: Option<Arc<OpportunityFilter>>, // ✅ 接收策略过滤器
    slippage_cache: SlippageCache,
    pool_manager: Option<Arc<PoolManager>>,   // 可选的 PoolManager，用于运行时黑名单
    cache_tracker: PoolCacheTracker,
) -> Result<()> {
    const MAX_CONSECUTIVE_FAILURES: usize = 3;
    const QUARANTINE_DURATION_MINUTES: i64 = 10;

    let mut connect_backoff = CONNECT_RETRY_INITIAL;
    let update_timeout = get_update_process_timeout();

    // ✅ 将订阅跟踪集合移到外层 loop 之外，避免重连时重复订阅导致 ref_count 泄漏
    let mut subscribed_accounts: HashSet<solana_pubkey::Pubkey> = HashSet::new();
    let mut raydium_tick_arrays: Vec<solana_pubkey::Pubkey> = Vec::new();
    let mut orca_tick_arrays: Vec<solana_pubkey::Pubkey> = Vec::new();
    let mut meteora_bin_arrays: Vec<solana_pubkey::Pubkey> = Vec::new();
    let mut has_initial_snapshot = false;

    // ✅ spot_only 模式下的启动 warmup：预热依赖账户并提前订阅
    if ctx.spot_only_mode() {
        match descriptor.kind {
            crate::dex::DexKind::RaydiumAmmV4 => {
                // AMM V4: 通过 sidecar introspect 获取静态信息并订阅 vault (5s 超时)
                let warmup_result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    crate::dex::raydium_amm::warmup_static_info(&descriptor.address)
                ).await;

                match warmup_result {
                    Ok(Ok((base_vault, quote_vault))) => {
                        info!(
                            "Warmup {} {}: AMM V4 static info cached, subscribing vaults",
                            descriptor.kind, descriptor.label
                        );
                        // 提前订阅 vault 账户并通过 HTTP 预取数据写入缓存
                        for vault in [base_vault, quote_vault] {
                            if subscribed_accounts.insert(vault) {
                                if let Err(e) = subscription_manager.ensure_subscription(vault).await {
                                    warn!("Warmup: failed to subscribe vault {}: {}", vault, e);
                                    let msg = format!(
                                        "WARMUP\tfailed_to_subscribe_vault={}\terr={}",
                                        vault, e
                                    );
                                    append_problem_pool_log(&descriptor, &msg);
                                    subscribed_accounts.remove(&vault);
                                } else {
                                    // ✅ 通过 HTTP 预取 vault 数据并写入缓存
                                    // 解决时序问题：WS 可能还没收到数据，但 decode 需要 vault 余额
                                    // ⚠️ 添加超时防止阻塞（3秒）
                                    let prefill_result = tokio::time::timeout(
                                        std::time::Duration::from_secs(3),
                                        ctx.rpc_client().get_account(&vault)
                                    ).await;
                                    
                                    match prefill_result {
                                        Ok(Ok(account_data)) => {
                                            subscription_manager
                                                .prefill_cache(&vault, account_data.data, 0)
                                                .await;
                                            debug!("Warmup: prefilled cache for vault {}", vault);
                                        }
                                        Ok(Err(e)) => {
                                            debug!("Warmup: failed to get vault {}: {}", vault, e);
                                        }
                                        Err(_) => {
                                            debug!("Warmup: vault {} prefill timed out (3s)", vault);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "Warmup {} {}: AMM V4 introspect failed: {}",
                            descriptor.kind, descriptor.label, e
                        );
                        let msg = format!("WARMUP\tamm_v4_introspect_failed\terr={}", e);
                        append_problem_pool_log(&descriptor, &msg);
                    }
                    Err(_) => {
                        warn!(
                            "Warmup {} {}: AMM V4 introspect timed out",
                            descriptor.kind, descriptor.label
                        );
                    }
                }
            }
            crate::dex::DexKind::RaydiumCpmm => {
                // CPMM: 从 RPC 解析 vault 地址并订阅 (5s 超时)
                let warmup_result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    crate::dex::raydium_cpmm::warmup_parse_vaults(&descriptor.address, &ctx)
                ).await;

                match warmup_result {
                    Ok(Ok((vault_0, vault_1))) => {
                        info!(
                            "Warmup {} {}: Raydium CPMM vaults parsed, subscribing",
                            descriptor.kind, descriptor.label
                        );
                        for vault in [vault_0, vault_1] {
                            if subscribed_accounts.insert(vault) {
                                if let Err(e) =
                                    subscription_manager.ensure_subscription(vault).await
                                {
                                    warn!("Warmup: failed to subscribe vault {}: {}", vault, e);
                                    let msg = format!(
                                        "WARMUP\tfailed_to_subscribe_vault={}\terr={}",
                                        vault, e
                                    );
                                    append_problem_pool_log(&descriptor, &msg);
                                    subscribed_accounts.remove(&vault);
                                } else {
                                    // ✅ 通过 HTTP 预取 vault 数据并写入缓存
                                    // ⚠️ 添加超时防止阻塞（3秒）
                                    let prefill_result = tokio::time::timeout(
                                        std::time::Duration::from_secs(3),
                                        ctx.rpc_client().get_account(&vault)
                                    ).await;
                                    if let Ok(Ok(account_data)) = prefill_result {
                                        subscription_manager
                                            .prefill_cache(&vault, account_data.data, 0)
                                            .await;
                                        debug!("Warmup: prefilled cache for vault {}", vault);
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "Warmup {} {}: Raydium CPMM vault parse failed: {}",
                            descriptor.kind, descriptor.label, e
                        );
                        let msg = format!("WARMUP\tray_cpmm_vault_parse_failed\terr={}", e);
                        append_problem_pool_log(&descriptor, &msg);
                    }
                    Err(_) => {
                        warn!(
                            "Warmup {} {}: Raydium CPMM vault parse timed out",
                            descriptor.kind, descriptor.label
                        );
                    }
                }
            }
            crate::dex::DexKind::PumpFunDlmm => {
                // Pump.fun: 从 RPC 解析 vault 地址并订阅 (5s 超时)
                let warmup_result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    crate::dex::pump_fun::warmup_parse_vaults(&descriptor.address, &ctx)
                ).await;
                
                match warmup_result {
                    Ok(Ok((base_vault, quote_vault))) => {
                        info!(
                            "Warmup {} {}: Pump.fun vaults parsed, subscribing",
                            descriptor.kind, descriptor.label
                        );
                        for vault in [base_vault, quote_vault] {
                            if subscribed_accounts.insert(vault) {
                                if let Err(e) = subscription_manager.ensure_subscription(vault).await {
                                    warn!("Warmup: failed to subscribe vault {}: {}", vault, e);
                                    let msg = format!(
                                        "WARMUP\tfailed_to_subscribe_vault={}\terr={}",
                                        vault, e
                                    );
                                    append_problem_pool_log(&descriptor, &msg);
                                    subscribed_accounts.remove(&vault);
                                } else {
                                    // ✅ 通过 HTTP 预取 vault 数据并写入缓存
                                    // ⚠️ 添加超时防止阻塞（3秒）
                                    let prefill_result = tokio::time::timeout(
                                        std::time::Duration::from_secs(3),
                                        ctx.rpc_client().get_account(&vault)
                                    ).await;
                                    if let Ok(Ok(account_data)) = prefill_result {
                                        subscription_manager
                                            .prefill_cache(&vault, account_data.data, 0)
                                            .await;
                                        debug!("Warmup: prefilled cache for vault {}", vault);
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "Warmup {} {}: Pump.fun vault parse failed: {}",
                            descriptor.kind, descriptor.label, e
                        );
                        let msg = format!("WARMUP\tpump_fun_vault_parse_failed\terr={}", e);
                        append_problem_pool_log(&descriptor, &msg);
                    }
                    Err(_) => {
                        warn!(
                            "Warmup {} {}: Pump.fun vault parse timed out",
                            descriptor.kind, descriptor.label
                        );
                    }
                }
            }
            crate::dex::DexKind::MeteoraDlmm => {
                // DLMM: 需要通过 sidecar introspect 获取 bin_arrays 并缓存
                // 这样在 spot_only 模式下构建 swap 指令时才有正确的 bin_array 地址
                use once_cell::sync::Lazy;
                static WARMUP_SIDECAR: Lazy<Result<crate::sidecar_client::SidecarClient, anyhow::Error>> =
                    Lazy::new(|| crate::sidecar_client::SidecarClient::from_env());
                
                match WARMUP_SIDECAR.as_ref() {
                    Ok(sidecar) => {
                        // 5s 超时
                        let warmup_result = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            crate::dex::meteora_dlmm::warmup_pool(&ctx, sidecar, &descriptor.address)
                        ).await;
                        
                        match warmup_result {
                            Ok(Ok(count)) if count > 0 => {
                                info!(
                                    "Warmup {} {}: cached {} bin_arrays via sidecar",
                                    descriptor.kind, descriptor.label, count
                                );
                            }
                            Ok(Ok(_)) => {
                                debug!(
                                    "Warmup {} {}: sidecar returned no bin_arrays",
                                    descriptor.kind, descriptor.label
                                );
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    "Warmup {} {}: failed to cache bin_arrays: {}",
                                    descriptor.kind, descriptor.label, e
                                );
                            }
                            Err(_) => {
                                warn!(
                                    "Warmup {} {}: sidecar warmup timed out",
                                    descriptor.kind, descriptor.label
                                );
                            }
                        }
                    }
                    Err(e) => {
                        // 只在首次失败时打印警告，后续池子静默跳过
                        debug!(
                            "Warmup {} {}: SidecarClient unavailable: {}",
                            descriptor.kind, descriptor.label, e
                        );
                    }
                }
            }
            crate::dex::DexKind::MeteoraDammV1 => {
                // DAMM V1: 需要订阅 Meteora Vault 账户和内部的 token_vault 账户 (5s 超时)
                let warmup_result = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    crate::dex::meteora_damm::warmup_parse_vaults(&descriptor.address, &ctx)
                ).await;

                match warmup_result {
                    Ok(Ok((a_vault, b_vault, a_token_vault, b_token_vault, _a_vault_lp, _b_vault_lp, _a_vault_lp_mint, _b_vault_lp_mint))) => {
                        info!(
                            "Warmup {} {}: DAMM V1 vaults parsed, subscribing 4 accounts",
                            descriptor.kind, descriptor.label
                        );
                        // 订阅所有 4 个账户：2 个 Meteora Vault + 2 个 token_vault
                        for vault in [a_vault, b_vault, a_token_vault, b_token_vault] {
                            if subscribed_accounts.insert(vault) {
                                if let Err(e) = subscription_manager.ensure_subscription(vault).await {
                                    warn!("Warmup: failed to subscribe vault {}: {}", vault, e);
                                    let msg = format!(
                                        "WARMUP\tfailed_to_subscribe_vault={}\terr={}",
                                        vault, e
                                    );
                                    append_problem_pool_log(&descriptor, &msg);
                                    subscribed_accounts.remove(&vault);
                                } else {
                                    // ✅ 通过 HTTP 预取数据并写入缓存
                                    // ⚠️ 添加超时防止阻塞（3秒）
                                    let prefill_result = tokio::time::timeout(
                                        std::time::Duration::from_secs(3),
                                        ctx.rpc_client().get_account(&vault)
                                    ).await;
                                    if let Ok(Ok(account_data)) = prefill_result {
                                        subscription_manager
                                            .prefill_cache(&vault, account_data.data, 0)
                                            .await;
                                        debug!("Warmup: prefilled cache for DAMM V1 vault {}", vault);
                                    }
                                }
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(
                            "Warmup {} {}: DAMM V1 vault parse failed: {}",
                            descriptor.kind, descriptor.label, e
                        );
                        let msg = format!("WARMUP\tdamm_v1_vault_parse_failed\terr={}", e);
                        append_problem_pool_log(&descriptor, &msg);
                    }
                    Err(_) => {
                        warn!(
                            "Warmup {} {}: DAMM V1 vault parse timed out",
                            descriptor.kind, descriptor.label
                        );
                    }
                }
            }
            _ => {
                // 其他 DEX 类型在 spot_only 下不需要额外 warmup
            }
        }
    }

    loop {
        // ✅ 检查是否被隔离
        if let Some(state) = quarantine.get(&descriptor.address) {
            if state.is_quarantined() {
                if let Some(until) = state.quarantined_until {
                    let remaining = (until - Utc::now()).num_seconds();
                    debug!(
                        "Pool {} {} is quarantined for {} more seconds",
                        descriptor.kind, descriptor.label, remaining
                    );
                    sleep(Duration::from_secs(30)).await; // 每30秒检查一次
                    continue;
                }
            }
        }
        // ✅ 使用共享的 SubscriptionProvider 订阅主池账户，避免创建独立 WS 连接
        if let Err(err) = subscription_manager.ensure_subscription(descriptor.address).await {
            warn!(
                "Failed to subscribe to {} {}: {err:?}; retrying in {:?}",
                descriptor.kind, descriptor.label, connect_backoff
            );
            let msg = format!("WS\tsubscribe_failed\terr={err:?}");
            append_problem_pool_log(&descriptor, &msg);
            let sleep_duration = connect_backoff;
            sleep(sleep_duration).await;
            connect_backoff = (connect_backoff * 2).min(CONNECT_RETRY_MAX);
            continue;
        }

        info!(
            "Subscribed to {} {} ({})",
            descriptor.kind, colorize_label(&descriptor.label), descriptor.address
        );
        connect_backoff = CONNECT_RETRY_INITIAL;

        // ✅ 获取更新广播接收器
        let mut update_rx = subscription_manager.subscribe_updates();

        let descriptor_for_run = descriptor.clone();
        let tracker_for_run = Arc::clone(&tracker);
        let subscription_manager_for_run = Arc::clone(&subscription_manager);
        let dispatcher_for_run = Arc::clone(&dispatcher);

        // ✅ HTTP warmup: 为所有池子类型在启动时立即获取快照，避免等待 WS 通知
        // 这样可以确保价格在订阅后立即可用，而不是等待链上状态变化
        if !has_initial_snapshot {
            let http_config = RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(ctx.rpc_client().commitment()),
                ..RpcAccountInfoConfig::default()
            };

            // ⚠️ 添加超时防止阻塞（5秒）
            let http_fetch_result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                ctx.rpc_client().get_ui_account_with_config(&descriptor_for_run.address, http_config),
            )
            .await;

            match http_fetch_result {
                Ok(Ok(RpcResponse { context, value })) => {
                    if let Some(account) = value {
                        let decoded = account.data.decode().ok_or_else(|| {
                            anyhow::anyhow!(
                                "Failed to decode account data for {}",
                                descriptor_for_run.address
                            )
                        })?;
                        subscription_manager_for_run
                            .prefill_cache(
                                &descriptor_for_run.address,
                                decoded.clone(),
                                context.slot,
                            )
                            .await;
                        let ui_account = account;
                        let mut sub_ctx = SubscriptionContext {
                            manager: &subscription_manager_for_run,
                            subscribed_accounts: &mut subscribed_accounts,
                            raydium_tick_arrays: &mut raydium_tick_arrays,
                            orca_tick_arrays: &mut orca_tick_arrays,
                            meteora_bin_arrays: &mut meteora_bin_arrays,
                        };

                        let warmup_slot = context.slot; // ✅ 保存 slot，避免 context 被移动后无法访问
                        let _permit = UPDATE_SEMAPHORE.acquire().await;
                        match process_update(
                            &descriptor_for_run,
                            context,
                            &ui_account,
                            &ctx,
                            &store,
                            &thresholds,
                            &tracker_for_run,
                            &detector,
                            Arc::clone(&dispatcher_for_run),
                            strategy.clone(),
                            &slippage_cache,
                            &mut sub_ctx,
                            &cache_tracker,
                        )
                        .await
                        {
                            Ok(_) => {
                                has_initial_snapshot = true;
                                // ✅ 记录池子初始化成功和当前价格到 pool log
                                if let Some(snapshot) = store.get(&descriptor_for_run.address) {
                                    let price = snapshot.normalized_price;
                                    let ts = chrono::Utc::now().to_rfc3339();
                                    let log_line = format!(
                                        "{}\tINIT_OK\t{}\t{}\tprice={:.10}\tslot={}",
                                        ts,
                                        descriptor_for_run.kind,
                                        descriptor_for_run.label,
                                        price,
                                        warmup_slot
                                    );
                                    append_pool_state_log(&log_line);
                                    info!(
                                        "✅ Pool init OK: {} {} price={:.6}",
                                        descriptor_for_run.kind,
                                        descriptor_for_run.label,
                                        price
                                    );

                                    // ✅ 为 CLMM 池缓存 tick arrays（用于 spot_only 模式的交易构建）
                                    // 正确区分 Orca (tick_array_size=88) 和 Raydium (tick_array_size=60)
                                    // ⚠️ 使用超时防止 RPC 调用卡住
                                    if let Some(clmm) = &snapshot.clmm_accounts {
                                        let owner_result = tokio::time::timeout(
                                            std::time::Duration::from_secs(5),
                                            ctx.get_account_owner(&descriptor_for_run.address)
                                        ).await;
                                        
                                        if let Ok(Ok(program_id)) = owner_result {
                                            match descriptor_for_run.kind {
                                                crate::dex::DexKind::OrcaWhirlpool => {
                                                    // Orca: tick_array_size=88
                                                    use crate::dex::orca;
                                                    let tick_arrays = if ctx.spot_only_mode() {
                                                        orca::warmup_tick_arrays(
                                                            &ctx,
                                                            &program_id,
                                                            &descriptor_for_run.address,
                                                            clmm.tick_current_index,
                                                            clmm.tick_spacing,
                                                        )
                                                        .await
                                                    } else {
                                                        orca::compute_tick_array_subscriptions(
                                                            &program_id,
                                                            &descriptor_for_run.address,
                                                            clmm.tick_current_index,
                                                            clmm.tick_spacing,
                                                        )
                                                    };
                                                    if !tick_arrays.is_empty() {
                                                        ctx.set_orca_tick_arrays(&descriptor_for_run.address, tick_arrays.clone());
                                                        crate::trade::orca::inject_tick_arrays(descriptor_for_run.address, tick_arrays.clone());
                                                        info!(
                                                            "Warmup {}: cached {} tick_arrays (Orca)",
                                                            descriptor_for_run.label,
                                                            tick_arrays.len()
                                                        );
                                                    }
                                                }
                                                crate::dex::DexKind::RaydiumClmm => {
                                                    // Raydium: tick_array_size=60
                                                    use crate::dex::raydium;
                                                    let tick_arrays = if ctx.spot_only_mode() {
                                                        raydium::warmup_tick_arrays(
                                                            &ctx,
                                                            &program_id,
                                                            &descriptor_for_run.address,
                                                            clmm.tick_current_index,
                                                            clmm.tick_spacing,
                                                        )
                                                        .await
                                                    } else {
                                                        raydium::compute_tick_array_subscriptions(
                                                            &program_id,
                                                            &descriptor_for_run.address,
                                                            clmm.tick_current_index,
                                                            clmm.tick_spacing,
                                                        )
                                                    };
                                                    if !tick_arrays.is_empty() {
                                                        crate::trade::raydium::inject_tick_arrays(descriptor_for_run.address, tick_arrays.clone());
                                                        info!(
                                                            "Warmup {}: cached {} tick_arrays (Raydium)",
                                                            descriptor_for_run.label,
                                                            tick_arrays.len()
                                                        );
                                                    }
                                                }
                                                _ => {}
                                            }
                                        } else {
                                            debug!(
                                                "CLMM warmup skipped for {}: get_account_owner timeout or failed",
                                                descriptor_for_run.label
                                            );
                                        }
                                    }

                                    // ✅ 预创建 base token 的 ATA（确保交易时已存在）
                                    if let Some(signer) = signer_pubkey_from_env().await {
                                        if snapshot.base_mint != *SOL_MINT {
                                            // ⚠️ 添加超时防止阻塞（5秒）
                                            match tokio::time::timeout(
                                                std::time::Duration::from_secs(5),
                                                ensure_ata_exists(&ctx, &signer, &snapshot.base_mint)
                                            ).await {
                                                Ok(Err(e)) => {
                                                    warn!("[ATA] Failed to ensure base token ATA for {}: {}", snapshot.base_mint, e);
                                                }
                                                Err(_) => {
                                                    warn!("[ATA] Timeout ensuring base token ATA for {}", snapshot.base_mint);
                                                }
                                                _ => {}
                                            }
                                        }
                                        // 非 SOL 的 quote token 也需要 ATA
                                        if snapshot.quote_mint != *SOL_MINT {
                                            // ⚠️ 添加超时防止阻塞（5秒）
                                            match tokio::time::timeout(
                                                std::time::Duration::from_secs(5),
                                                ensure_ata_exists(&ctx, &signer, &snapshot.quote_mint)
                                            ).await {
                                                Ok(Err(e)) => {
                                                    warn!("[ATA] Failed to ensure quote token ATA for {}: {}", snapshot.quote_mint, e);
                                                }
                                                Err(_) => {
                                                    warn!("[ATA] Timeout ensuring quote token ATA for {}", snapshot.quote_mint);
                                                }
                                                _ => {}
                                            }
                                        }
                                    }
                                }
                            }
                            Err(err) => {
                                warn!(
                                    "Failed to process HTTP warmup snapshot for {} {}: {:?}",
                                    descriptor_for_run.kind, descriptor_for_run.label, err
                                );
                                let msg = format!("HTTP_WARMUP\tsnapshot_process_failed\terr={err:?}");
                                append_problem_pool_log(&descriptor_for_run, &msg);
                            }
                        }
                    }
                }
                Ok(Err(err)) => {
                    warn!(
                        "HTTP warmup fetch failed for {} {}: {:?}",
                        descriptor_for_run.kind, descriptor_for_run.label, err
                    );
                    let msg = format!("HTTP_WARMUP\tfetch_failed\terr={err:?}");
                    append_problem_pool_log(&descriptor_for_run, &msg);
                }
                Err(_) => {
                    warn!(
                        "HTTP warmup timed out (5s) for {} {}",
                        descriptor_for_run.kind, descriptor_for_run.label
                    );
                }
            }
        }

        let mut last_update = Instant::now();
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {
                    // ✅ 使用共享订阅的广播接收器接收更新
                    update_result = update_rx.recv() => {
                        match update_result {
                            Ok(AccountUpdate { account, data, slot }) => {
                                // 检查更新是否与本池相关：主池地址或已订阅的依赖账户（vault/tick array等）
                                let is_main_pool = account == descriptor_for_run.address;
                                let is_dependent = subscribed_accounts.contains(&account);
                                
                                if !is_main_pool && !is_dependent {
                                    continue;
                                }
                                last_update = Instant::now();

                                // 如果是依赖账户（vault）更新，需要重新获取主池数据来触发 decode
                                // 传统 AMM（Raydium AMM V4/CPMM、Pump.fun）的价格由 vault 余额决定
                                // 主池状态几乎不变，但 vault 每次交易都会变化
                                let (ui_account, context) = if is_main_pool {
                                    // 主池更新：直接使用收到的数据
                                    let ui = UiAccount {
                                        lamports: 0,
                                        data: solana_account_decoder::UiAccountData::Binary(
                                            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &data),
                                            solana_account_decoder::UiAccountEncoding::Base64,
                                        ),
                                        owner: String::new(),
                                        executable: false,
                                        rent_epoch: 0,
                                        space: Some(data.len() as u64),
                                    };
                                    (ui, RpcResponseContext { slot, api_version: None })
                                } else {
                                    // 依赖账户（vault）更新：从 WS cache 获取主池数据
                                    // 这样 decode 时会读取最新的 vault 余额
                                    match subscription_manager_for_run.get_cached_sync(&descriptor_for_run.address) {
                                        Some(cached) => {
                                            let ui = UiAccount {
                                                lamports: 0,
                                                data: solana_account_decoder::UiAccountData::Binary(
                                                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &cached.data),
                                                    solana_account_decoder::UiAccountEncoding::Base64,
                                                ),
                                                owner: String::new(),
                                                executable: false,
                                                rent_epoch: 0,
                                                space: Some(cached.data.len() as u64),
                                            };
                                            (ui, RpcResponseContext { slot: cached.slot, api_version: None })
                                        }
                                        None => {
                                            // 主池尚未缓存，跳过本次 vault 更新
                                            debug!(
                                                "Vault update for {} {} but main pool not cached yet, skipping",
                                                descriptor_for_run.kind, descriptor_for_run.label
                                            );
                                            continue;
                                        }
                                    }
                                };

                                let mut sub_ctx = SubscriptionContext {
                                    manager: &subscription_manager_for_run,
                                    subscribed_accounts: &mut subscribed_accounts,
                                    raydium_tick_arrays: &mut raydium_tick_arrays,
                                    orca_tick_arrays: &mut orca_tick_arrays,
                                    meteora_bin_arrays: &mut meteora_bin_arrays,
                                };

                                let _permit = UPDATE_SEMAPHORE.acquire().await;
                                let update_result = if let Some(timeout) = update_timeout {
                                    match tokio::time::timeout(
                                        timeout,
                                        process_update(
                                            &descriptor_for_run,
                                            context,
                                            &ui_account,
                                            &ctx,
                                            &store,
                                            &thresholds,
                                            &tracker_for_run,
                                            &detector,
                                            Arc::clone(&dispatcher_for_run),
                                            strategy.clone(),
                                            &slippage_cache,
                                            &mut sub_ctx,
                                            &cache_tracker,
                                        ),
                                    )
                                    .await
                                    {
                                        Ok(result) => result,
                                        Err(_) => Err(anyhow::anyhow!(
                                            "process_update timed out after {:?}",
                                            timeout
                                        )),
                                    }
                                } else {
                                    process_update(
                                        &descriptor_for_run,
                                        context,
                                        &ui_account,
                                        &ctx,
                                        &store,
                                        &thresholds,
                                        &tracker_for_run,
                                        &detector,
                                        Arc::clone(&dispatcher_for_run),
                                        strategy.clone(),
                                        &slippage_cache,
                                        &mut sub_ctx,
                                        &cache_tracker,
                                    )
                                    .await
                                };

                                match update_result {
                                    Ok(_) => {
                                        // 成功解码，重置隔离状态
                                        if let Some(mut state) = quarantine.get_mut(&descriptor_for_run.address) {
                                            if state.consecutive_failures > 0 {
                                                debug!(
                                                    "Pool {} {} recovered after {} failures",
                                                    descriptor_for_run.kind,
                                                    colorize_label(&descriptor_for_run.label),
                                                    state.consecutive_failures
                                                );
                                                state.reset();
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        let error_msg = format!("{:?}", err);
                                        if error_msg.contains("not in WS cache (spot_only)") {
                                            debug!(
                                                "Expected spot_only WS cache miss for {} {}: {}",
                                                descriptor_for_run.kind,
                                                colorize_label(&descriptor_for_run.label),
                                                error_msg
                                            );
                                        } else {
                                            warn!(
                                                "Failed to process update for {} {}: {}",
                                                descriptor_for_run.kind,
                                                colorize_label(&descriptor_for_run.label),
                                                error_msg
                                            );
                                            append_problem_pool_log(&descriptor_for_run, &error_msg);

                                            if matches!(descriptor_for_run.kind, crate::dex::DexKind::MeteoraDammV1)
                                                && error_msg.contains("fetch mint")
                                                && error_msg.contains("AccountNotFound")
                                            {
                                                warn!(
                                                    "⚠️  Blacklisting pool {} {} due to missing mint (AccountNotFound)",
                                                    descriptor_for_run.kind,
                                                    colorize_label(&descriptor_for_run.label)
                                                );

                                                let mut state = quarantine
                                                    .entry(descriptor_for_run.address)
                                                    .or_insert_with(QuarantineState::new);
                                                state.record_failure("AccountNotFound");
                                                state.enter_quarantine(QUARANTINE_DURATION_MINUTES);

                                                if let Some(pm) = &pool_manager {
                                                    pm.blacklist_pool(&descriptor_for_run.address).await;
                                                }

                                                break;
                                            }

                                            // 记录失败并检查是否需要隔离
                                            let mut state = quarantine.entry(descriptor_for_run.address)
                                                .or_insert_with(QuarantineState::new);
                                            state.record_failure(&error_msg);

                                            if state.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                                                state.enter_quarantine(QUARANTINE_DURATION_MINUTES);

                                                let short_error = state
                                                    .last_error
                                                    .chars()
                                                    .take(200)
                                                    .collect::<String>();

                                                warn!(
                                                    "⚠️  Quarantining pool {} {} after {} consecutive failures (same error: {}). Will retry in {} minutes.",
                                                    descriptor_for_run.kind,
                                                    colorize_label(&descriptor_for_run.label),
                                                    state.consecutive_failures,
                                                    short_error,
                                                    QUARANTINE_DURATION_MINUTES
                                                );

                                                // 追加写入问题池日志文件，便于离线排查
                                                append_problem_pool_log(&descriptor_for_run, &short_error);

                                                // 如果集成了 PoolManager，则将该池加入运行时黑名单，
                                                // 在下一次 refresh 时级联移除相关套利对
                                                if let Some(pm) = &pool_manager {
                                                    pm.blacklist_pool(&descriptor_for_run.address).await;
                                                }

                                                // 断开当前连接，回到外层loop等待隔离期结束
                                                break;
                                            }
                                        }
                                    }
                                }

                                tokio::task::yield_now().await;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                warn!(
                                    "Broadcast channel closed for {} {}; scheduling reconnect",
                                    descriptor_for_run.kind,
                                    descriptor_for_run.label
                                );
                                append_problem_pool_log(&descriptor_for_run, "WS\tbroadcast_closed");
                                break;
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                // 消息积压，跳过落后的消息
                                debug!(
                                    "Broadcast lagged {} messages for {} {}",
                                    n, descriptor_for_run.kind, descriptor_for_run.label
                                );
                                continue;
                            }
                        }
                    }
                    _ = heartbeat.tick() => {
                        let idle_for = last_update.elapsed();
                        if idle_for >= MAX_IDLE_BEFORE_RECONNECT {
                            warn!(
                                "No account updates for {} {} in {:?}; forcing resubscribe",
                                descriptor_for_run.kind,
                                descriptor_for_run.label,
                                idle_for
                            );
                            let msg = format!("WS\tno_updates_for={:?}", idle_for);
                            append_problem_pool_log(&descriptor_for_run, &msg);
                            break;
                        } else {
                            debug!(
                                "Heartbeat check for {} {} ok (idle {:?})",
                                descriptor_for_run.kind,
                                colorize_label(&descriptor_for_run.label),
                                idle_for
                            );
                        }
                    }
                }
        }

        // ✅ 释放共享订阅引用前先检查后台任务健康状态
        if !subscription_manager.is_healthy() {
            error!(
                "⚠️ Subscription manager is not healthy for {} {}! Pausing before retry...",
                descriptor.kind, descriptor.label
            );
            // 如果后台任务已死，等待更长时间让它有机会恢复
            sleep(Duration::from_secs(10)).await;
        }
        
        subscription_manager.release_subscription(&descriptor.address).await;
        info!(
            "Released subscription for {} {}, will resubscribe after {:?}",
            descriptor.kind, descriptor.label, SHORT_RECONNECT_DELAY
        );
        sleep(SHORT_RECONNECT_DELAY).await;
    }
}

async fn process_update(
    descriptor: &PoolDescriptor,
    context: RpcResponseContext,
    account: &solana_account_decoder::UiAccount,
    ctx: &AppContext,
    store: &SnapshotStore,
    thresholds: &PriceThresholds,
    tracker: &OpportunityTracker,
    detector: &OpportunityDetector,
    dispatcher: Arc<TradeDispatcher>,
    strategy: Option<Arc<OpportunityFilter>>,
    slippage_cache: &SlippageCache,
    sub_ctx: &mut SubscriptionContext<'_>,
    cache_tracker: &PoolCacheTracker,
) -> Result<()> {
    let snapshot = snapshot_from_update(descriptor, account, context, ctx).await?;
    store.insert(descriptor.address, snapshot.clone());

    // ✅ 自动订阅所有依赖账户（仅订阅一次，避免ref_count泄漏）
    for account_pubkey in &snapshot.dependent_accounts {
        // 只在首次遇到该账户时订阅
        if sub_ctx.subscribed_accounts.insert(*account_pubkey) {
            if let Err(err) = sub_ctx.manager.ensure_subscription(*account_pubkey).await {
                debug!(
                    "Failed to ensure subscription for dependent account {}: {:?}",
                    account_pubkey, err
                );
                let msg = format!(
                    "SUB\tdependent_ensure_failed\taccount={}\terr={:?}",
                    account_pubkey, err
                );
                append_problem_pool_log(descriptor, &msg);
                // 订阅失败，从集合中移除以便下次重试
                sub_ctx.subscribed_accounts.remove(account_pubkey);
            } else {
                debug!(
                    "Subscribed to dependent account {} for pool {}",
                    account_pubkey, descriptor.address
                );
            }
        }
    }

    // ✅ Raydium CLMM 动态 tick array 订阅切换
    // spot_only 模式下跳过：价格从主池获取，tick array 地址可派生
    if !ctx.spot_only_mode() && descriptor.kind == crate::dex::DexKind::RaydiumClmm {
        if let Some(clmm_state) = &snapshot.clmm_state {
            // 获取 program_id（Raydium CLMM 池子的 owner）
            if let Ok(program_id) = ctx.get_account_owner(&descriptor.address).await {
                use crate::dex::raydium;

                // 计算新的 tick array 订阅列表
                let new_tick_arrays = raydium::compute_tick_array_subscriptions(
                    &program_id,
                    &descriptor.address,
                    clmm_state.tick_current_index,
                    clmm_state.tick_spacing,
                );

                // 检查是否需要更新订阅
                if raydium::tick_arrays_changed(sub_ctx.raydium_tick_arrays, &new_tick_arrays) {
                    info!(
                        "Raydium CLMM {} tick array boundary crossed: tick_current={}, updating subscriptions",
                        descriptor.label, clmm_state.tick_current_index
                    );

                    // 找出要释放和要添加的 tick arrays
                    use std::collections::HashSet;
                    let old_set: HashSet<_> = sub_ctx.raydium_tick_arrays.iter().collect();
                    let new_set: HashSet<_> = new_tick_arrays.iter().collect();

                    // 释放不再需要的 tick arrays
                    for old_addr in old_set.difference(&new_set) {
                        sub_ctx.manager.release_subscription(old_addr).await;
                        sub_ctx.subscribed_accounts.remove(old_addr);
                        debug!(
                            "Released tick array subscription {} for pool {}",
                            old_addr, descriptor.address
                        );
                    }

                    // 订阅新的 tick arrays
                    for new_addr in new_set.difference(&old_set) {
                        if sub_ctx.subscribed_accounts.insert(**new_addr) {
                            if let Err(err) = sub_ctx.manager.ensure_subscription(**new_addr).await
                            {
                                warn!(
                                    "Failed to subscribe to new tick array {}: {:?}",
                                    new_addr, err
                                );
                                let msg = format!(
                                    "SUB\ttick_array_subscribe_failed\taccount={}\terr={:?}",
                                    new_addr, err
                                );
                                append_problem_pool_log(descriptor, &msg);
                                sub_ctx.subscribed_accounts.remove(new_addr);
                            } else {
                                debug!(
                                    "Subscribed to new tick array {} for pool {}",
                                    new_addr, descriptor.address
                                );
                            }
                        }
                    }

                    // 更新跟踪列表
                    *sub_ctx.raydium_tick_arrays = new_tick_arrays;

                    info!(
                        "Raydium CLMM {} tick array subscriptions updated: {} arrays active",
                        descriptor.label,
                        sub_ctx.raydium_tick_arrays.len()
                    );
                }
            }
        }
    }

    // ✅ Orca CLMM 动态 tick array 订阅切换
    // spot_only 模式下跳过：价格从主池获取，tick array 地址可派生
    if !ctx.spot_only_mode() && descriptor.kind == crate::dex::DexKind::OrcaWhirlpool {
        if let Some(clmm_state) = &snapshot.clmm_state {
            // 获取 program_id（Orca CLMM 池子的 owner）
            if let Ok(program_id) = ctx.get_account_owner(&descriptor.address).await {
                use crate::dex::orca;

                // 计算新的 tick array 订阅列表
                let new_tick_arrays = orca::compute_tick_array_subscriptions(
                    &program_id,
                    &descriptor.address,
                    clmm_state.tick_current_index,
                    clmm_state.tick_spacing,
                );

                // 检查是否需要更新订阅
                if orca::tick_arrays_changed(sub_ctx.orca_tick_arrays, &new_tick_arrays) {
                    info!(
                        "Orca CLMM {} tick array boundary crossed: tick_current={}, updating subscriptions",
                        descriptor.label, clmm_state.tick_current_index
                    );

                    // 找出要释放和要添加的 tick arrays
                    use std::collections::HashSet;
                    let old_set: HashSet<_> = sub_ctx.orca_tick_arrays.iter().collect();
                    let new_set: HashSet<_> = new_tick_arrays.iter().collect();

                    // 释放不再需要的 tick arrays
                    for old_addr in old_set.difference(&new_set) {
                        sub_ctx.manager.release_subscription(old_addr).await;
                        sub_ctx.subscribed_accounts.remove(old_addr);
                        debug!(
                            "Released tick array subscription {} for pool {}",
                            old_addr, descriptor.address
                        );
                    }

                    // 订阅新的 tick arrays
                    for new_addr in new_set.difference(&old_set) {
                        if sub_ctx.subscribed_accounts.insert(**new_addr) {
                            if let Err(err) = sub_ctx.manager.ensure_subscription(**new_addr).await
                            {
                                warn!(
                                    "Failed to subscribe to new tick array {}: {:?}",
                                    new_addr, err
                                );
                                sub_ctx.subscribed_accounts.remove(new_addr);
                            } else {
                                debug!(
                                    "Subscribed to new tick array {} for pool {}",
                                    new_addr, descriptor.address
                                );
                            }
                        }
                    }

                    // 更新跟踪列表
                    *sub_ctx.orca_tick_arrays = new_tick_arrays;

                    info!(
                        "Orca CLMM {} tick array subscriptions updated: {} arrays active",
                        descriptor.label,
                        sub_ctx.orca_tick_arrays.len()
                    );
                }
            }
        }
    }

    // ✅ Meteora DLMM 动态 bin array 订阅切换
    // spot_only 模式下跳过动态订阅：价格从主池的 active_id 获取，bin array 地址可派生
    if descriptor.kind == crate::dex::DexKind::MeteoraDlmm {
        // ✅ spot-only 模式下自动注入执行配置（首次成功 decode 时）
        if ctx.spot_only_mode() && snapshot.lb_accounts.is_some() {
            use crate::trade::{execution::get_signer_pubkey_from_env, meteora_dlmm::auto_inject_pool_config};
            
            if let Some(signer) = get_signer_pubkey_from_env() {
                match auto_inject_pool_config(&ctx, &snapshot, signer) {
                    Ok(true) => {
                        debug!(
                            "Auto-injected DLMM execution config for pool {} (signer={})",
                            descriptor.address, signer
                        );
                    }
                    Ok(false) => {
                        // 已存在，跳过
                    }
                    Err(err) => {
                        warn!(
                            "Failed to auto-inject DLMM config for {}: {:?}",
                            descriptor.address, err
                        );
                    }
                }
            }
        }
        
        // ✅ 动态 bin array 订阅只在非 spot_only 模式下执行
        if !ctx.spot_only_mode() {
        if let Some(lb_state) = &snapshot.lb_state {
            if let Ok(program_id) = ctx.get_account_owner(&descriptor.address).await {
                use crate::dex::meteora_dlmm;

                // 计算新的核心 bin array indexes（当前 ±1）
                let new_indexes = meteora_dlmm::compute_core_bin_array_indexes(lb_state._active_id);
                let new_bin_arrays = meteora_dlmm::bin_array_indexes_to_addresses(
                    &program_id,
                    &descriptor.address,
                    &new_indexes,
                );

                // 检查是否需要更新订阅
                if meteora_dlmm::bin_arrays_changed(sub_ctx.meteora_bin_arrays, &new_bin_arrays) {
                    info!(
                        "Meteora DLMM {} bin array boundary crossed: active_id={}, updating subscriptions",
                        descriptor.label, lb_state._active_id
                    );

                    // 找出要释放和要添加的 bin arrays
                    use std::collections::HashSet;
                    let old_set: HashSet<_> = sub_ctx.meteora_bin_arrays.iter().collect();
                    let new_set: HashSet<_> = new_bin_arrays.iter().collect();

                    // 释放不再需要的 bin arrays
                    for old_addr in old_set.difference(&new_set) {
                        sub_ctx.manager.release_subscription(old_addr).await;
                        sub_ctx.subscribed_accounts.remove(old_addr);
                        debug!(
                            "Released bin array subscription {} for pool {}",
                            old_addr, descriptor.address
                        );
                    }

                    // 订阅新的 bin arrays
                    for new_addr in new_set.difference(&old_set) {
                        if sub_ctx.subscribed_accounts.insert(**new_addr) {
                            if let Err(err) = sub_ctx.manager.ensure_subscription(**new_addr).await
                            {
                                warn!(
                                    "Failed to subscribe to new bin array {}: {:?}",
                                    new_addr, err
                                );
                                sub_ctx.subscribed_accounts.remove(new_addr);
                            } else {
                                debug!(
                                    "Subscribed to new bin array {} for pool {}",
                                    new_addr, descriptor.address
                                );
                            }
                        }
                    }

                    // 更新跟踪列表
                    *sub_ctx.meteora_bin_arrays = new_bin_arrays;

                    info!(
                        "Meteora DLMM {} bin array subscriptions updated: {} arrays active",
                        descriptor.label,
                        sub_ctx.meteora_bin_arrays.len()
                    );
                }
            }
        }
        } // end if !ctx.spot_only_mode()
    }

    maybe_refresh_tick_bin_cache(descriptor, &snapshot, ctx, cache_tracker);

    let pair_key = snapshot.normalized_pair;

    // ✅ 使用优化的 O(1) 检测器，总是重新评估机会
    let opportunity_opt = detector.update_pool(snapshot.clone());

    match opportunity_opt {
        Some(mut opportunity) => {
            // 检查价差是否超过阈值：使用动态滑点阈值
            let spread_pct = opportunity.spread_pct;

            let buy_slip = slippage_cache
                .get(&opportunity.buy.descriptor.address)
                .map(|v| v.buy_slippage_pct)
                .filter(|v| v.is_finite() && *v >= 0.0);
            let sell_slip = slippage_cache
                .get(&opportunity.sell.descriptor.address)
                .map(|v| v.sell_slippage_pct)
                .filter(|v| v.is_finite() && *v >= 0.0);

            // 如果没有滑点数据，跳过此机会
            let min_spread_pct = match (buy_slip, sell_slip) {
                (Some(buy), Some(sell)) => buy + sell + get_slippage_buffer_pct(),
                _ => {
                    // 没有滑点数据，无法判断，跳过
                    calibration::schedule(snapshot.clone(), ctx.clone());
                    return Ok(());
                }
            };

            if spread_pct < min_spread_pct {
                // ✅ 价差低于阈值，结束机会
                if let Some((_, ended_state)) = tracker.remove(&pair_key) {
                    log_opportunity_end(pair_key, ended_state);
                }
                calibration::schedule(snapshot.clone(), ctx.clone());
                return Ok(());
            }

            let est_profit_pct = spread_pct - min_spread_pct;

            // ✅ 关闭 trade estimates：仅使用 spot-only 交易路径
            opportunity.trade = None;

            let observed_at = opportunity.buy.timestamp.max(opportunity.sell.timestamp);

            // 检查是否是新机会（用于监控模式下的日志去重）
            let is_new_opportunity = !tracker.contains_key(&opportunity.pair);

            tracker
                .entry(opportunity.pair)
                .and_modify(|state| {
                    state.last_seen = observed_at;
                })
                .or_insert_with(|| ActiveOpportunity {
                    started_at: observed_at,
                    last_seen: observed_at,
                });

            // ✅ 监控模式：即使不执行交易，也记录新发现的机会
            if !thresholds.execution_enabled && is_new_opportunity {
                log_opportunity(&opportunity, buy_slip, sell_slip, min_spread_pct);
            }

            if thresholds.execution_enabled {
                // 策略筛选：评估机会是否应该执行
                let should_execute = if let Some(ref strategy_filter) = strategy {
                    use crate::trade::strategy::{FilterDecision, RejectReason};
                    match strategy_filter.evaluate(&opportunity) {
                        FilterDecision::Accept => true,
                        FilterDecision::Reject(reason) => match &reason {
                            RejectReason::PoolInCooldown { log_once: true, .. } => {
                                info!(
                                    "Opportunity rejected by strategy for pair {} -> {}: {}",
                                    opportunity.pair.base, opportunity.pair.quote, reason
                                );
                                false
                            }
                            RejectReason::PoolInCooldown { log_once: false, .. } => false,
                            _ => {
                                info!(
                                    "Opportunity rejected by strategy for pair {} -> {}: {}",
                                    opportunity.pair.base, opportunity.pair.quote, reason
                                );
                                false
                            }
                        },
                    }
                } else {
                    // 没有策略过滤器，默认执行
                    true
                };

                if should_execute {
                    let static_tip = get_static_tip_lamports();
                    let dynamic_tip = compute_dynamic_tip_lamports(est_profit_pct, &opportunity.pair.quote);
                    let tip_lamports = static_tip.saturating_add(dynamic_tip);
                    if tip_lamports > 0 {
                        opportunity.tip_lamports = Some(tip_lamports);
                    }

                    // ⭐ 使用缓存的买入滑点 + BUY_SLIPPAGE_BUFFER_BPS 作为保护滑点
                    // buy_slip 是百分比（如 0.5 表示 0.5%），转成 bps 需要乘以 100
                    let buffer_bps = get_buy_slippage_buffer_bps();
                    let max_slippage_bps = buy_slip
                        .map(|s| s * 100.0 + buffer_bps)
                        .unwrap_or(50.0);  // fallback: 0.5%
                    
                    match dispatcher.enqueue(opportunity.clone(), max_slippage_bps).await {
                        Ok(job_id) => {
                            // ✅ 只有成功入队后才记录机会，避免重复 slot 被去重后仍然打 WARN
                            log_opportunity(&opportunity, buy_slip, sell_slip, min_spread_pct);
                            debug!(
                                "Enqueued atomic job {} for pair {} -> {} (spread: {:.3}%)",
                                job_id,
                                opportunity.pair.base,
                                opportunity.pair.quote,
                                opportunity.spread_pct
                            );
                        }
                        Err(err) => {
                            let err_msg = err.to_string();
                            if err_msg.contains("already traded")
                                || err_msg.contains("in cooldown")
                                || err_msg.contains("in flight")
                            {
                                debug!(
                                    "Skipped enqueue for pair {} -> {}: {}",
                                    opportunity.pair.base,
                                    opportunity.pair.quote,
                                    err_msg
                                );
                            } else {
                                warn!(
                                    "Failed to enqueue atomic trade plan for pair {} -> {}: {} (queue may be full)",
                                    opportunity.pair.base,
                                    opportunity.pair.quote,
                                    err_msg
                                );
                            }
                        }
                    }
                }
            }
        }
        None => {
            // ✅ None 表示该交易对没有两边的报价（例如只有买方没有卖方）
            // 这时才清除跟踪
            if let Some((_, ended_state)) = tracker.remove(&pair_key) {
                log_opportunity_end(pair_key, ended_state);
            }
        }
    }

    calibration::schedule(snapshot.clone(), ctx.clone());

    Ok(())
}

fn maybe_refresh_tick_bin_cache(
    descriptor: &PoolDescriptor,
    snapshot: &PoolSnapshot,
    ctx: &AppContext,
    cache_tracker: &PoolCacheTracker,
) {
    if !ctx.spot_only_mode() {
        return;
    }

    let refresh_slots = get_tick_bin_cache_refresh_slots();
    let min_gap_slots = get_tick_bin_cache_refresh_min_gap_slots();
    let pool = descriptor.address;
    let slot = snapshot.slot;

    let mut refresh_dlmm = false;
    let mut refresh_orca: Option<(Pubkey, i32, i32)> = None;
    let mut refresh_raydium: Option<(Pubkey, i32, i32)> = None;

    {
        let mut state = cache_tracker
            .entry(pool)
            .or_insert_with(PoolCacheState::default);

        let slot_delta = slot.saturating_sub(state.last_seen_slot);
        let due_to_age = refresh_slots > 0 && slot_delta >= refresh_slots;
        let gap_ok = slot.saturating_sub(state.last_refresh_slot) >= min_gap_slots;

        match descriptor.kind {
            crate::dex::DexKind::MeteoraDlmm => {
                if let Some(lb_state) = &snapshot.lb_state {
                    let active_index =
                        crate::dex::meteora_dlmm::bin_array_index_for_dlmm(lb_state._active_id);
                    let boundary_crossed = state
                        .last_active_index
                        .map(|prev| prev != active_index)
                        .unwrap_or(true);
                    let cached_arrays = ctx.get_dlmm_bin_arrays(&pool).unwrap_or_default();
                    let required_indexes =
                        crate::dex::meteora_dlmm::compute_core_bin_array_indexes(lb_state._active_id);
                    let required_addrs = crate::dex::meteora_dlmm::bin_array_indexes_to_addresses(
                        &crate::constants::METEORA_LB_PROGRAM_ID,
                        &pool,
                        &required_indexes,
                    );
                    let missing_required = required_addrs
                        .iter()
                        .any(|addr| !cached_arrays.contains(addr));

                    if gap_ok && (boundary_crossed || due_to_age || missing_required) {
                        refresh_dlmm = true;
                        state.last_refresh_slot = slot;
                    }
                    state.last_active_index = Some(active_index);
                }
            }
            crate::dex::DexKind::OrcaWhirlpool => {
                if let Some(clmm) = &snapshot.clmm_accounts {
                    let start_index =
                        crate::dex::orca::tick_array_start_index(clmm.tick_current_index, clmm.tick_spacing);
                    let boundary_crossed = state
                        .last_tick_start
                        .map(|prev| prev != start_index)
                        .unwrap_or(true)
                        || state.last_tick_spacing != Some(clmm.tick_spacing);
                    if gap_ok && (boundary_crossed || due_to_age) {
                        refresh_orca = Some((
                            clmm.program_id,
                            clmm.tick_current_index,
                            clmm.tick_spacing,
                        ));
                        state.last_refresh_slot = slot;
                    }
                    state.last_tick_start = Some(start_index);
                    state.last_tick_spacing = Some(clmm.tick_spacing);
                }
            }
            crate::dex::DexKind::RaydiumClmm => {
                if let Some(clmm) = &snapshot.clmm_accounts {
                    let start_index =
                        crate::dex::raydium::tick_array_start_index(clmm.tick_current_index, clmm.tick_spacing);
                    let boundary_crossed = state
                        .last_tick_start
                        .map(|prev| prev != start_index)
                        .unwrap_or(true)
                        || state.last_tick_spacing != Some(clmm.tick_spacing);
                    if gap_ok && (boundary_crossed || due_to_age) {
                        refresh_raydium = Some((
                            clmm.program_id,
                            clmm.tick_current_index,
                            clmm.tick_spacing,
                        ));
                        state.last_refresh_slot = slot;
                    }
                    state.last_tick_start = Some(start_index);
                    state.last_tick_spacing = Some(clmm.tick_spacing);
                }
            }
            _ => {}
        }

        state.last_seen_slot = slot;
    }

    if refresh_dlmm {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = crate::trade::meteora_dlmm::refresh_pool_cached_bin_arrays(&ctx, &pool).await;
        });
    }
    if let Some((program_id, tick_current, tick_spacing)) = refresh_orca {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = crate::trade::orca::refresh_pool_cached_tick_arrays_with_state(
                &ctx,
                &pool,
                &program_id,
                tick_current,
                tick_spacing,
            )
            .await;
        });
    }
    if let Some((program_id, tick_current, tick_spacing)) = refresh_raydium {
        let ctx = ctx.clone();
        tokio::spawn(async move {
            let _ = crate::trade::raydium::refresh_pool_cached_tick_arrays_with_state(
                &ctx,
                &pool,
                &program_id,
                tick_current,
                tick_spacing,
            )
            .await;
        });
    }
}

fn log_opportunity(opportunity: &Opportunity, buy_slip: Option<f64>, sell_slip: Option<f64>, min_spread_pct: f64) {
    let buy = &opportunity.buy;
    let sell = &opportunity.sell;
    
    // Check if quote is SOL for display labeling
    let is_sol_quote = opportunity.pair.quote == *SOL_MINT;
    let _quote_label = if is_sol_quote { "SOL" } else { "quote" };
    
    // 计算预估利润
    let est_profit_pct = opportunity.spread_pct - min_spread_pct;
    
    let buy_slip_str = buy_slip.map_or("N/A".to_string(), |v| format!("{:.3}%", v));
    let sell_slip_str = sell_slip.map_or("N/A".to_string(), |v| format!("{:.3}%", v));
    
    let mut message = format!(
        "\n==================== ARB OPPORTUNITY ====================\nPair: {} / {}\nBuy MEME: {} @ {:.6} ({:?}) [slot {}]\nSell MEME: {} @ {:.6} ({:?}) [slot {}]\nSpread: {:.3}% | BuySlip: {} | SellSlip: {} | MinSpread: {:.3}% | Est.Profit: {:.3}%",
        opportunity.pair.base,
        opportunity.pair.quote,
        colorize_label(&buy.descriptor.label),
        buy.normalized_price,
        buy.descriptor.kind,
        buy.slot,
        colorize_label(&sell.descriptor.label),
        sell.normalized_price,
        sell.descriptor.kind,
        sell.slot,
        opportunity.spread_pct,
        buy_slip_str,
        sell_slip_str,
        min_spread_pct,
        est_profit_pct
    );

    message.push_str("\n==========================================================\n");
    // 主 log：完整机会详情
    warn!("{}", message);

    // 机会专用日志：只记录机会和本地 quote 信息
    append_opportunity_log(&message);

    // ===== 结构化 OPP 行（多行），方便人工阅读 =====
    // 以 SOL 为主视角：对包含 SOL 的交易对，quote = SOL，PnL 单位也是 SOL
    let quote_label = if opportunity.pair.quote == *SOL_MINT {
        "SOL"
    } else {
        "quote"
    };

    let mode = get_run_mode();
    let ts = Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();

    // 从模拟结果中提取：base 数量、买入花费、卖出收益、净利润、手续费
    let (base_amt, trade_quote_in, sell_proceeds, net_pnl, _buy_fee, _sell_fee) = opportunity
        .trade
        .as_ref()
        .map(|t| (
            t.base_amount,
            t.buy_quote_cost,
            t.sell_quote_proceeds,
            t.net_quote_profit,
            t.buy_fee,
            t.sell_fee,
        ))
        .unwrap_or((0.0, 0.0, 0.0, 0.0, 0.0, 0.0));

    // ==== 价差 & 成本拆分（全部以 quote 资产计价，SOL 为主）====
    let mut gross_spread_mid_pct = opportunity.spread_pct;
    let mut theoretical_pnl = 0.0_f64;       // 基于 mid 价差的理论利润（无手续费、无滑点）
    let mut base_fee_cost_total = 0.0_f64;   // 基础手续费成本（base fee 部分）
    let mut dyn_fee_cost_total = 0.0_f64;    // 动态手续费成本（dynamic fee 部分）
    let mut fee_cost_total = 0.0_f64;        // 手续费总成本
    let mut slippage_cost = 0.0_f64;         // 滑点成本（单位 = quote/SOL）
    let mut base_fee_pct_of_spread = 0.0_f64;
    let mut dyn_fee_pct_of_spread = 0.0_f64;
    let mut fee_pct_of_spread = 0.0_f64;
    let mut slip_pct_of_spread = 0.0_f64;

    if base_amt > 0.0 && buy.normalized_price > 0.0 && sell.normalized_price > 0.0 {
        let buy_mid = buy.normalized_price;
        let sell_mid = sell.normalized_price;

        // 理论价差（不含手续费与滑点），基于快照 mid price
        gross_spread_mid_pct = (sell_mid / buy_mid - 1.0) * 100.0;
        theoretical_pnl = base_amt * (sell_mid - buy_mid);

        // 理论总成本 = 理论利润 - 实际净利润
        let total_cost = (theoretical_pnl - net_pnl).max(0.0);

        // 获取 fee ratios（对 Meteora 会包含 dynamic fee）
        let buy_base_fee_ratio = buy
            .fees
            .as_ref()
            .map(|f| f.base_fee_ratio())
            .unwrap_or(0.0);
        let buy_var_fee_ratio = buy
            .fees
            .as_ref()
            .map(|f| f.variable_fee_ratio())
            .unwrap_or(0.0);
        let sell_base_fee_ratio = sell
            .fees
            .as_ref()
            .map(|f| f.base_fee_ratio())
            .unwrap_or(0.0);
        let sell_var_fee_ratio = sell
            .fees
            .as_ref()
            .map(|f| f.variable_fee_ratio())
            .unwrap_or(0.0);

        // 总费率 = base + variable
        let buy_fee_ratio = buy_base_fee_ratio + buy_var_fee_ratio;
        let sell_fee_ratio = sell_base_fee_ratio + sell_var_fee_ratio;

        // DEBUG: 打印 fee_ratio 确认单位是否正确
        debug!(
            "OPP FEE DEBUG: buy_fee_ratio={:.6} ({:.2}%), sell_fee_ratio={:.6} ({:.2}%), trade_quote_in={:.6}, sell_proceeds={:.6}",
            buy_fee_ratio,
            buy_fee_ratio * 100.0,
            sell_fee_ratio,
            sell_fee_ratio * 100.0,
            trade_quote_in,
            sell_proceeds
        );

        // 买腿/卖腿：基础 fee 与动态 fee 成本拆分（按 quote 金额 * 对应费率 近似）
        let buy_base_fee_cost = trade_quote_in.abs() * buy_base_fee_ratio;
        let buy_dyn_fee_cost = trade_quote_in.abs() * buy_var_fee_ratio;
        let sell_base_fee_cost = sell_proceeds.abs() * sell_base_fee_ratio;
        let sell_dyn_fee_cost = sell_proceeds.abs() * sell_var_fee_ratio;

        let raw_base_fee_cost_total = buy_base_fee_cost + sell_base_fee_cost;
        let raw_dyn_fee_cost_total = buy_dyn_fee_cost + sell_dyn_fee_cost;
        let raw_fee_cost_total = raw_base_fee_cost_total + raw_dyn_fee_cost_total;

        if total_cost <= 0.0 || raw_fee_cost_total <= 0.0 {
            // 没有利润或费率无效：全部视为 0 成本
            base_fee_cost_total = 0.0;
            dyn_fee_cost_total = 0.0;
            fee_cost_total = 0.0;
            slippage_cost = 0.0;
        } else if raw_fee_cost_total <= total_cost {
            // 费率估算的手续费 <= 理论总成本：剩余部分视为滑点
            base_fee_cost_total = raw_base_fee_cost_total;
            dyn_fee_cost_total = raw_dyn_fee_cost_total;
            fee_cost_total = raw_fee_cost_total;
            slippage_cost = total_cost - fee_cost_total;
        } else {
            // 费率估算大于理论总成本：按比例缩放 fee，使其和 total_cost 匹配，滑点记为 0
            let scale = total_cost / raw_fee_cost_total;
            base_fee_cost_total = raw_base_fee_cost_total * scale;
            dyn_fee_cost_total = raw_dyn_fee_cost_total * scale;
            fee_cost_total = total_cost;
            slippage_cost = 0.0;
        }

        if theoretical_pnl > 0.0 {
            base_fee_pct_of_spread = (base_fee_cost_total / theoretical_pnl) * 100.0;
            dyn_fee_pct_of_spread = (dyn_fee_cost_total / theoretical_pnl) * 100.0;
            fee_pct_of_spread = (fee_cost_total / theoretical_pnl) * 100.0;
            slip_pct_of_spread = (slippage_cost / theoretical_pnl) * 100.0;
        }
    }

    // 基础流动性信息（用于理解滑点成因）
    let (buy_base_liq, buy_base_liq_str) = if let Some(v) = buy.base_reserve {
        let scale = 10f64.powi(buy.base_decimals as i32);
        let val = v as f64 / scale;
        (Some(val), format!("{:.3}", val))
    } else {
        (None, "n/a".to_string())
    };
    let (_buy_quote_liq, buy_quote_liq_str) = if let Some(v) = buy.quote_reserve {
        let scale = 10f64.powi(buy.quote_decimals as i32);
        let val = v as f64 / scale;
        (Some(val), format!("{:.3}", val))
    } else {
        (None, "n/a".to_string())
    };

    let (sell_base_liq, sell_base_liq_str) = if let Some(v) = sell.base_reserve {
        let scale = 10f64.powi(sell.base_decimals as i32);
        let val = v as f64 / scale;
        (Some(val), format!("{:.3}", val))
    } else {
        (None, "n/a".to_string())
    };
    let (_sell_quote_liq, sell_quote_liq_str) = if let Some(v) = sell.quote_reserve {
        let scale = 10f64.powi(sell.quote_decimals as i32);
        let val = v as f64 / scale;
        (Some(val), format!("{:.3}", val))
    } else {
        (None, "n/a".to_string())
    };

    // 交易规模相对于 base 流动性的占比（买/卖腿），帮助解释滑点
    let trade_share_buy_base_pct = if let Some(liq) = buy_base_liq {
        if liq > 0.0 { (base_amt / liq) * 100.0 } else { 0.0 }
    } else {
        0.0
    };
    let trade_share_sell_base_pct = if let Some(liq) = sell_base_liq {
        if liq > 0.0 { (base_amt / liq) * 100.0 } else { 0.0 }
    } else {
        0.0
    };

    let trade_share_base_pct_str = if buy_base_liq.is_some() && sell_base_liq.is_some() {
        format!("{:.4}% / {:.4}%", trade_share_buy_base_pct, trade_share_sell_base_pct)
    } else if buy_base_liq.is_some() {
        format!("{:.4}% / n/a", trade_share_buy_base_pct)
    } else if sell_base_liq.is_some() {
        format!("n/a / {:.4}%", trade_share_sell_base_pct)
    } else {
        "n/a / n/a".to_string()
    };

    // 获取 fee ratios 用于显示（包含 base + dynamic）
    let buy_base_fee_pct = buy.fees.as_ref().map(|f| f.base_fee_ratio() * 100.0).unwrap_or(0.0);
    let buy_var_fee_pct = buy.fees.as_ref().map(|f| f.variable_fee_ratio() * 100.0).unwrap_or(0.0);
    let sell_base_fee_pct = sell.fees.as_ref().map(|f| f.base_fee_ratio() * 100.0).unwrap_or(0.0);
    let sell_var_fee_pct = sell.fees.as_ref().map(|f| f.variable_fee_ratio() * 100.0).unwrap_or(0.0);

    // 构建 fee 显示字符串
    let buy_fee_str = if buy_var_fee_pct > 0.0001 {
        format!("{:.4}% + {:.4}% dyn", buy_base_fee_pct, buy_var_fee_pct)
    } else {
        format!("{:.4}%", buy_base_fee_pct)
    };
    let sell_fee_str = if sell_var_fee_pct > 0.0001 {
        format!("{:.4}% + {:.4}% dyn", sell_base_fee_pct, sell_var_fee_pct)
    } else {
        format!("{:.4}%", sell_base_fee_pct)
    };

    // Check if spot_only mode - simplified log without local quote details
    let spot_only = true;

    let opp_block = if spot_only {
        // spot_only 模式：只显示基本信息，不显示本地 quote 估算
        format!(
            "OPP\n  Time: {}\n  Mode: {} (spot_only)\n  Buy: {} @ {:.6} ({:?})\n  Sell: {} @ {:.6} ({:?})\n  Spread (mid): {:.4}%\n  Fee ratios (buy/sell): {} / {}\n  [No local quote - direct execution]\n",
            ts,
            mode,
            colorize_label(&buy.descriptor.label),
            buy.normalized_price,
            buy.descriptor.kind,
            colorize_label(&sell.descriptor.label),
            sell.normalized_price,
            sell.descriptor.kind,
            gross_spread_mid_pct,
            buy_fee_str,
            sell_fee_str,
        )
    } else {
        // 完整版本地 quote 摘要：聚焦在价差、成本拆分和流动性
        format!(
            "OPP\n  Time: {}\n  Mode: {}\n  Gross spread (mid, no fee/slip): {:.4}%\n  MEME amount (base): {:.6}\n  Trade size ({}): {:.6}\n  Theoretical PnL ({}): {:.6}\n  Fee ratios (buy/sell): {} / {}\n  Base fee cost ({}): {:.6} ({:.2}% of spread)\n  Dynamic fee cost ({}): {:.6} ({:.2}% of spread)\n  Total fee cost ({}): {:.6} ({:.2}% of spread)\n  Slippage cost (price impact, {}): {:.6} ({:.2}% of spread)\n  Trade share of base liquidity (buy/sell): {}\n  Buy pool liquidity (base/quote): {} / {}\n  Sell pool liquidity (base/quote): {} / {}\n  Net PnL ({}): {:.6}\n",
            ts,
            mode,
            gross_spread_mid_pct,
            base_amt,
            quote_label,
            trade_quote_in,
            quote_label,
            theoretical_pnl,
            buy_fee_str,
            sell_fee_str,
            quote_label,
            base_fee_cost_total,
            base_fee_pct_of_spread,
            quote_label,
            dyn_fee_cost_total,
            dyn_fee_pct_of_spread,
            quote_label,
            fee_cost_total,
            fee_pct_of_spread,
            quote_label,
            slippage_cost,
            slip_pct_of_spread,
            trade_share_base_pct_str,
            buy_base_liq_str,
            buy_quote_liq_str,
            sell_base_liq_str,
            sell_quote_liq_str,
            quote_label,
            net_pnl,
        )
    };

    // 盈利时，在 Time ~ Net PnL 这一段右侧画一个 ASCII 大星星
    let final_opp_block = if net_pnl > 0.0 {
        // 先按行拆分
        let mut lines: Vec<String> = opp_block.lines().map(|s| s.to_string()).collect();

        // 找到 "Time" 和 "Net PnL" 所在的行区间
        let start_idx = lines
            .iter()
            .position(|l| l.trim_start().starts_with("Time:"))
            .unwrap_or(0);
        let end_idx = lines
            .iter()
            .rposition(|l| l.trim_start().starts_with("Net PnL"))
            .unwrap_or_else(|| lines.len().saturating_sub(1));

        if end_idx > start_idx {
            let height = end_idx - start_idx + 1;

            // 一个 8 行的星星图案，会居中塞进 height 行里
            const STAR_PATTERN: [&str; 8] = [
                "    *",
                "   ***",
                "  *****",
                " *******",
                "*********",
                "   ***",
                "   ***",
                "   ***",
            ];

            let mut star_lines = vec![String::new(); height];
            let pattern_len = STAR_PATTERN.len();
            let start_pattern = if height > pattern_len {
                (height - pattern_len) / 2
            } else {
                0
            };

            for (i, pattern) in STAR_PATTERN.iter().enumerate() {
                let idx = start_pattern + i;
                if idx >= height {
                    break;
                }
                star_lines[idx] = (*pattern).to_string();
            }

            // 计算 Time~Net PnL 这些行里最长的一行，用于右侧对齐星星
            let max_len = (start_idx..=end_idx)
                .filter_map(|i| lines.get(i).map(|s| s.len()))
                .max()
                .unwrap_or(0);

            for (offset, i) in (start_idx..=end_idx).enumerate() {
                if let Some(star) = star_lines.get(offset) {
                    if !star.is_empty() {
                        let padded = format!("{:<width$}  {}", lines[i], star, width = max_len);
                        lines[i] = padded;
                    }
                }
            }

            let mut combined = lines.join("\n");
            combined.push('\n');
            combined
        } else {
            opp_block
        }
    } else {
        opp_block
    };

    if !spot_only {
        append_opportunity_log(&final_opp_block);
    }
}

fn log_opportunity_end(pair: NormalizedPair, state: ActiveOpportunity) {
    let duration = if state.last_seen >= state.started_at {
        state.last_seen.signed_duration_since(state.started_at)
    } else {
        ChronoDuration::zero()
    };
    let seconds = duration.num_milliseconds() as f64 / 1000.0;
    let msg = format!(
        "Arb window ended {} -> {}; duration {:.3}s ({} -> {})",
        pair.base, pair.quote, seconds, state.started_at, state.last_seen
    );
    info!("{}", msg);
    append_opportunity_log(&msg);
    append_trade_log(&format!(
        "OPP_WINDOW {} -> {} duration {:.3}s ({} -> {})",
        pair.base, pair.quote, seconds, state.started_at, state.last_seen
    ));
}

fn colorize_label(label: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    label.hash(&mut hasher);
    let hash = hasher.finish();
    // Use colors 31-36 (Red, Green, Yellow, Blue, Magenta, Cyan)
    // 91-96 are bright versions which might be better
    let color_code = 31 + (hash % 6);
    format!("\x1b[1;{}m{}\x1b[0m", color_code, label)
}

// ==================== Dynamic Pool Manager Integration ====================

/// Pool subscription handle for tracking active pool subscriptions
struct PoolSubscriptionHandle {
    descriptor: PoolDescriptor,
    handle: tokio::task::JoinHandle<()>,
}

/// Run monitor with dynamic pool management via PoolManager
///
/// This function integrates with PoolManager to automatically:
/// - Fetch pool lists from sidecar periodically
/// - Filter pools based on configuration
/// - Subscribe to new pools and unsubscribe from removed ones
///
/// The static `pools` parameter provides initial pools that are always monitored
/// in addition to dynamically discovered pools.
pub async fn run_monitor_with_pool_manager(
    ctx: AppContext,
    ws_url: String,
    static_pools: Vec<PoolDescriptor>,
    thresholds: PriceThresholds,
    commitment: CommitmentConfig,
) -> Result<()> {
    let mut static_pools = static_pools;
    let mut thresholds = thresholds;
    // Initialize PoolManager
    let pool_manager = match PoolManager::from_env() {
        Ok(pm) => {
            info!("[PoolManager] Initialized from environment");
            Some(Arc::new(pm))
        }
        Err(err) => {
            warn!(
                "[PoolManager] Failed to initialize: {:?}; using static pools only",
                err
            );
            None
        }
    };

    // If no pool manager and no static pools, bail
    if pool_manager.is_none() && static_pools.is_empty() {
        anyhow::bail!("No pools configured and PoolManager failed to initialize");
    }

    let dry_run = is_dry_run_mode();
    if dry_run {
        // Dry-run uses the same pipeline as live execution; only submission is replaced by simulate.
        thresholds.execution_enabled = true;
    }
    let pool_slippage_bps = get_pool_slippage_filter_bps();
    let pool_slippage_trade_size_quote = get_pool_slippage_trade_size_quote();
    if pool_slippage_trade_size_quote > 0.0 {
        info!(
            "[PoolSlippage] Sampling enabled: trade_size_quote={}, buffer_pct=0.5 (max_slippage_bps={})",
            pool_slippage_trade_size_quote,
            pool_slippage_bps
        );
    } else {
        info!("[PoolSlippage] Sampling disabled (POOL_FILTER_TRADE_SIZE_QUOTE<=0)");
    }

    let slippage_cache: SlippageCache = Arc::new(DashMap::new());
    let mut use_simulated_buy_slippage = false;

    if dry_run {
        info!("[DryRun] Enabled: using live pipeline (simulate only)");
    }
    info!("Preparing RPC subscriptions via {}", ws_url);
    info!(
        "[Update] process_update concurrency limit: {}",
        UPDATE_SEMAPHORE.available_permits()
    );
    if ctx.spot_only_mode() {
        let dlmm_pools: Vec<Pubkey> = static_pools
            .iter()
            .filter(|descriptor| descriptor.kind == crate::dex::DexKind::MeteoraDlmm)
            .map(|descriptor| descriptor.address)
            .collect();
        prewarm_meteora_dlmm_batch(&ctx, &dlmm_pools).await;
    }

    let store: SnapshotStore = Arc::new(Default::default());
    let tracker: OpportunityTracker = Arc::new(DashMap::new());
    let quarantine: QuarantineTracker = Arc::new(DashMap::new());
    let cache_tracker: PoolCacheTracker = Arc::new(DashMap::new());
    let detector = Arc::new(OpportunityDetector::new());
    let account_manager = Arc::new(TradeAccountManager::from_env());
    let execution_enabled = thresholds.execution_enabled || dry_run;
    let atomic_executor = if execution_enabled {
        match AtomicExecutor::from_env(ctx.clone()).await? {
            Some(exec) => Some(Arc::new(exec)),
            None => {
                warn!("Execution enabled but SOLANA_SIGNER_KEYPAIR not set; executor disabled");
                None
            }
        }
    } else {
        None
    };

    let strategy = if thresholds.execution_enabled {
        match OpportunityFilter::from_env() {
            Ok(filter) => Some(filter),
            Err(err) => {
                warn!(
                    "Failed to load opportunity filter config: {:?}; using no filter",
                    err
                );
                None
            }
        }
    } else {
        None
    };

    let dispatcher = TradeDispatcher::from_env(
        Arc::clone(&account_manager),
        atomic_executor.clone(),
        strategy.clone(),
        ctx.clone(),
    );

    // ✅ 在启动订阅前，先完成 PoolManager 初次刷新（如果启用）
    // 但如果 PoolManager 已经有池子（比如 main.rs 已经刷新过），则跳过
    let mut initial_refresh_result = None;
    if let Some(ref pm) = pool_manager {
        let existing_pools = pm.get_pools().await;
        if !existing_pools.is_empty() {
            info!(
                "[PoolManager] Skipping initial refresh: already have {} pools from previous refresh",
                existing_pools.len()
            );
            // 构造一个虚拟的 refresh result，表示池子已存在
            initial_refresh_result = Some(crate::pool_manager::RefreshResult {
                candidates: existing_pools.len(),
                filtered: existing_pools.len(),
                intersection_filtered: existing_pools.len(),
                added: 0,
                removed: 0,
                added_keys: Vec::new(),
                removed_keys: Vec::new(),
            });
        } else {
            info!("[PoolManager] Performing initial refresh before subscriptions...");
            let mut retry_count = 0;
            const MAX_INITIAL_RETRIES: usize = 5;
            const RETRY_DELAY: Duration = Duration::from_secs(5);

            loop {
                match pm.refresh().await {
                    Ok(result) => {
                        info!(
                            "[PoolManager] Initial refresh: candidates={}, filtered={}, added={}, removed={}",
                            result.candidates, result.filtered, result.added, result.removed
                        );
                        initial_refresh_result = Some(result);
                        break;
                    }
                    Err(err) => {
                        retry_count += 1;
                        if retry_count >= MAX_INITIAL_RETRIES {
                            error!(
                                "[PoolManager] Initial refresh failed after {} retries: {:?}",
                                retry_count, err
                            );
                            break;
                        }
                        warn!(
                            "[PoolManager] Initial refresh failed (retry {}/{}): {:?}",
                            retry_count, MAX_INITIAL_RETRIES, err
                        );
                        sleep(RETRY_DELAY).await;
                    }
                }
            }
        }
    }

    // ✅ 采样滑点：在 ATA/ALT 之前更新池子滑点缓存
    let mut filtered_dynamic_pools: Vec<(Pubkey, crate::pool_manager::PoolMeta)> = Vec::new();
    if let (Some(ref pm), Some(ref result)) = (&pool_manager, &initial_refresh_result) {
        let mut added_meta = Vec::new();
        if result.added_keys.is_empty() {
            let existing_pools = pm.get_pools().await;
            if !existing_pools.is_empty() {
                info!(
                    "[PoolManager] No new pools added, found {} existing pools for slippage sampling",
                    existing_pools.len()
                );
                for (key, meta) in existing_pools {
                    added_meta.push((key, meta));
                }
            }
        } else {
            for key in &result.added_keys {
                if let Some(meta) = pm.get_pool(key).await {
                    added_meta.push((*key, meta));
                }
            }
        }
        filtered_dynamic_pools = added_meta;
    }

    let warmup_candidates = collect_dry_run_pools(&static_pools, &filtered_dynamic_pools);
    let warmed = warmup_pools_with_retry(&ctx, warmup_candidates, Duration::from_secs(30)).await;
    let warmed_set: HashSet<Pubkey> = warmed.iter().map(|d| d.address).collect();
    static_pools.retain(|descriptor| warmed_set.contains(&descriptor.address));
    filtered_dynamic_pools.retain(|(key, _)| warmed_set.contains(key));

    let (pair_static, pair_dynamic) =
        filter_static_dynamic_by_pair_key(&ctx, static_pools, filtered_dynamic_pools).await;
    static_pools = pair_static;
    filtered_dynamic_pools = pair_dynamic;

    if pool_slippage_trade_size_quote > 0.0 {
        let mut candidates: Vec<PoolDescriptor> = Vec::new();
        let mut seen: HashSet<Pubkey> = HashSet::new();
        for descriptor in &static_pools {
            if seen.insert(descriptor.address) {
                candidates.push(descriptor.clone());
            }
        }
        for (key, meta) in &filtered_dynamic_pools {
            if seen.insert(*key) {
                candidates.push(PoolDescriptor {
                    label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                    address: *key,
                    kind: meta.dex,
                });
            }
        }

        let (filtered, stats) = apply_simulated_slippage_filter(
            &ctx,
            commitment,
            account_manager.as_ref(),
            atomic_executor.as_deref(),
            &slippage_cache,
            pool_slippage_trade_size_quote,
            candidates,
            None,
            "",
        )
        .await;

        let keep: HashSet<Pubkey> = filtered.iter().map(|d| d.address).collect();
        static_pools.retain(|descriptor| keep.contains(&descriptor.address));
        filtered_dynamic_pools.retain(|(key, _)| keep.contains(key));
        if stats.used_simulated {
            use_simulated_buy_slippage = true;
        }
    }

    // ✅ ALT 自动填充：在开始订阅前，收集所有池子的静态账户并确保在 ALT 中
    if std::env::var("ALT_AUTO_EXTEND")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false)
    {
        info!("ALT_AUTO_EXTEND enabled, collecting static accounts from pools...");
        
        let signer_keypair = load_signer_keypair_from_env().await;
        let default_payer = signer_keypair.as_ref().map(|kp| kp.pubkey());

        // ✅ 获取用于 ALT 填充的池子列表
        // 合并静态池和已过滤的动态池，仅对通过滑点过滤的池子进行 ATA/ALT
        let alt_pools: Vec<PoolDescriptor> = {
            let mut pools = static_pools.clone(); // 先添加静态池
            
            // 使用已经过滤的动态池（避免重复调用 PoolManager 获取未过滤的池子）
            let existing_addrs: HashSet<Pubkey> = pools.iter().map(|p| p.address).collect();
            for (key, meta) in &filtered_dynamic_pools {
                if !existing_addrs.contains(key) {
                    pools.push(PoolDescriptor {
                        label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                        address: *key,
                        kind: meta.dex,
                    });
                }
            }
            
            // 如果没有池子，发出警告
            if pools.is_empty() {
                warn!("No pools available for ALT population, skipping");
            }
            
            pools
        };
        
        if alt_pools.is_empty() {
            warn!("ALT population skipped: no pools available");
        } else {
            info!("Found {} pools for ALT population", alt_pools.len());
        }

        // ✅ 第一步：收集所有 pair 的 mint，检查/创建 ATA
        if let Some(ref keypair) = signer_keypair {
            info!("Step 1: Checking/creating ATAs for all mints in pairs...");
            
            // 收集所有唯一的 mint
            // 对缓存依赖型 DEX 需要特殊处理
            let mut all_mints: HashSet<Pubkey> = HashSet::new();
            for descriptor in &alt_pools {
                if let Ok(account) = ctx.rpc_client().get_account(&descriptor.address).await {
                    // ✅ 对需要缓存的 DEX，先调用 warmup
                    match descriptor.kind {
                        DexKind::RaydiumAmmV4 => {
                            if let Err(_) = crate::dex::raydium_amm::warmup_static_info(&descriptor.address).await {
                                continue; // warmup 失败，跳过
                            }
                        }
                        DexKind::RaydiumCpmm => {
                            if let Err(_) = crate::dex::raydium_cpmm::warmup_static_info(descriptor, &account.data, &ctx).await {
                                continue;
                            }
                        }
                        DexKind::PumpFunDlmm => {
                            // Pump.fun: warmup 只解析地址不填缓存，使用非 spot_only context
                        }
                        _ => {}
                    }
                    
                    // ✅ 选择合适的 context：
                    // - Pump.fun / Raydium AMM/CPMM: 使用非 spot_only（需要 RPC 获取 vault 余额）
                    // - Meteora DLMM: 使用 spot_only (虽然有警告但不影响 mint 获取，且避免大量 RPC)
                    // - 其他: 使用 spot_only
                    let decode_ctx = if matches!(
                        descriptor.kind,
                        DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
                    ) {
                        ctx.clone_non_spot_only()
                    } else {
                        ctx.clone_for_alt_collection()
                    };
                    
                    let context = RpcResponseContext {
                        slot: 0,
                        api_version: None,
                    };
                    if let Ok(snapshot) = crate::dex::snapshot_from_account_data(descriptor, account.data, context, &decode_ctx).await {
                        all_mints.insert(snapshot.base_mint);
                        all_mints.insert(snapshot.quote_mint);
                    }
                }
            }
            
            info!("Found {} unique mints across all pools", all_mints.len());
            
            // 检查/创建每个 mint 的 ATA
            let mut created_count = 0;
            let mut existing_count = 0;
            
            for mint in &all_mints {
                // 获取 token program
                let token_program = match token_program_for_mint(&ctx, mint).await {
                    Some(program) => program,
                    None => {
                        warn!(
                            "[ATA] Skip creation for mint {}: token program unavailable",
                            mint
                        );
                        continue;
                    }
                };
                
                // 计算 ATA 地址
                let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                    &keypair.pubkey(),
                    mint,
                    &token_program,
                );
                
                // 检查 ATA 是否存在
                match ctx.rpc_client().get_account(&ata).await {
                    Ok(_) => {
                        existing_count += 1;
                    }
                    Err(_) => {
                        // ATA 不存在，创建它
                        let create_ata_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                            &keypair.pubkey(),
                            &keypair.pubkey(),
                            mint,
                            &token_program,
                        );
                        
                        match ctx.rpc_client().get_latest_blockhash().await {
                            Ok(blockhash) => {
                                let tx = solana_transaction::Transaction::new_signed_with_payer(
                                    &[create_ata_ix],
                                    Some(&keypair.pubkey()),
                                    &[&**keypair],
                                    blockhash,
                                );
                                
                                match ctx.rpc_client().send_and_confirm_transaction(&tx).await {
                                    Ok(sig) => {
                                        created_count += 1;
                                        debug!("Created ATA for mint {} (sig: {})", mint, sig);
                                    }
                                    Err(e) => {
                                        warn!("Failed to create ATA for mint {}: {:?}", mint, e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to get blockhash for ATA creation: {:?}", e);
                            }
                        }
                    }
                }
            }
            
            info!("ATA check complete: {} existing, {} created", existing_count, created_count);
        }

        // ✅ 第二步：收集所有池子账户并添加到 ALT
        // 注意：在 spot_only 模式下，需要先调用 warmup 来填充静态缓存
        info!("Step 2: Collecting pool accounts for ALT...");
        
        // 按 DEX 类型收集账户
        let mut pool_accounts: Vec<(String, crate::DexKind, Vec<Pubkey>)> = Vec::new();
        
        // 遍历池子获取初始快照并收集静态账户
        for descriptor in &alt_pools {
            match ctx.rpc_client().get_account(&descriptor.address).await {
                Ok(account) => {
                    let context = RpcResponseContext {
                        slot: 0,
                        api_version: None,
                    };
                    
                    // ✅ 对于需要缓存的 DEX，先调用 warmup 填充静态信息
                    // 这样即使在 spot_only 模式下也能正确收集账户
                    match descriptor.kind {
                        DexKind::RaydiumAmmV4 => {
                            // Raydium AMM V4 需要通过 sidecar 获取静态信息
                            if let Err(e) = crate::dex::raydium_amm::warmup_static_info(&descriptor.address).await {
                                warn!("Failed to warmup Raydium AMM V4 pool {} for ALT: {:?}", descriptor.label, e);
                                continue;
                            }
                        }
                        DexKind::RaydiumCpmm => {
                            // Raydium CPMM 需要解析账户获取静态信息
                            if let Err(e) = crate::dex::raydium_cpmm::warmup_static_info(descriptor, &account.data, &ctx).await {
                                warn!("Failed to warmup Raydium CPMM pool {} for ALT: {:?}", descriptor.label, e);
                                continue;
                            }
                        }
                        DexKind::PumpFunDlmm => {
                            // Pump.fun 需要 vault 余额，warmup 只解析地址不填缓存
                            // 将在下面使用非 spot_only context 直接通过 RPC 获取
                        }
                        _ => {}
                    }
                    
                    // ✅ 选择合适的 context：
                    // - Pump.fun / Raydium AMM/CPMM: 使用非 spot_only（需要 RPC 获取 vault 余额）
                    // - Meteora DLMM: 使用 spot_only (虽然有警告但不影响 mint 获取，且避免大量 RPC)
                    // - 其他: 使用 spot_only
                    let decode_ctx = if matches!(
                        descriptor.kind,
                        DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
                    ) {
                        ctx.clone_non_spot_only()
                    } else {
                        ctx.clone_for_alt_collection()
                    };
                    
                    match crate::dex::snapshot_from_account_data(descriptor, account.data, context, &decode_ctx).await {
                        Ok(snapshot) => {
                            let mut accounts = crate::dex::collect_static_accounts_for_pool(&snapshot);
                            let user_accounts = collect_user_accounts_for_pool(
                                &ctx,
                                &account_manager,
                                &snapshot,
                                default_payer,
                            )
                            .await;
                            if !user_accounts.is_empty() {
                                accounts.extend(user_accounts);
                                accounts.sort();
                                accounts.dedup();
                            }
                            pool_accounts.push((descriptor.label.clone(), descriptor.kind, accounts));
                        }
                        Err(e) => {
                            warn!("Failed to get snapshot for pool {} for ALT: {:?}", descriptor.label, e);
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to fetch pool {} for ALT collection: {:?}", descriptor.label, e);
                }
            }
        }
        
        info!("Collected accounts for {} pools", pool_accounts.len());
        
        // 尝试获取 signer keypair 用作 ALT authority
        if let Some(ref exec) = atomic_executor {
            if let Some(ref keypair) = signer_keypair {
                match crate::alt_manager::AltManager::from_env(
                    ctx.rpc_client(),
                    Arc::clone(keypair),
                )
                .await
                {
                    Ok(mut alt_manager) => {
                        match alt_manager.ensure_pool_accounts(&pool_accounts).await {
                            Ok(()) => {
                                let alt_addrs = alt_manager.alt_addresses();
                                if !alt_addrs.is_empty() {
                                    info!(
                                        "ALT setup complete: {} ALT(s) with {} total addresses",
                                        alt_addrs.len(),
                                        alt_manager.total_addresses()
                                    );
                                    for (i, addr) in alt_addrs.iter().enumerate() {
                                        info!("  ALT #{}: {}", i + 1, addr);
                                    }
                                }
                                match exec.refresh_lookup_tables().await {
                                    Ok(count) => {
                                        info!("Refreshed ALT cache after population ({} table(s))", count);
                                    }
                                    Err(e) => {
                                        warn!("Failed to refresh ALT cache after population: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Failed to ensure pool accounts in ALT: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to initialize ALT manager: {:?}", e);
                    }
                };
            } else {
                warn!("ALT_AUTO_EXTEND enabled but SOLANA_SIGNER_KEYPAIR is missing; skipping ALT population");
            }
        } else {
            debug!("No atomic executor, skipping ALT auto-extend");
        }
    }

    if dry_run {
        let use_sweep = std::env::var("DRY_RUN_SWEEP")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false);
        if use_sweep {
            let executor = atomic_executor.as_ref().ok_or_else(|| {
                anyhow::anyhow!("dry run requires SOLANA_SIGNER_KEYPAIR (AtomicExecutor unavailable)")
            })?;
            let dry_run_pools = collect_dry_run_pools(&static_pools, &filtered_dynamic_pools);
            if dry_run_pools.is_empty() {
                warn!("[DryRun] No pools available after filtering; exiting");
                return Ok(());
            }
            let dlmm_pools: Vec<Pubkey> = dry_run_pools
                .iter()
                .filter(|d| d.kind == crate::dex::DexKind::MeteoraDlmm)
                .map(|d| d.address)
                .collect();
            let clmm_pools: Vec<(Pubkey, crate::dex::DexKind)> = dry_run_pools
                .iter()
                .filter(|d| {
                    d.kind == crate::dex::DexKind::OrcaWhirlpool
                        || d.kind == crate::dex::DexKind::RaydiumClmm
                })
                .map(|d| (d.address, d.kind))
                .collect();

            // ✅ 开机时预热一次缓存，后续不再刷新
            ctx.clear_warmup_caches();
            crate::trade::orca::clear_tick_arrays_cache();
            crate::trade::raydium::clear_tick_arrays_cache();
            prewarm_meteora_dlmm_batch(&ctx, &dlmm_pools).await;
            prewarm_clmm_tick_arrays_batch(&ctx, &clmm_pools).await;
            let log_path = rotate_trade_log();
            info!("[DryRun] Sweep start (log={})", log_path);
            run_dry_run_sweep(
                &ctx,
                dry_run_pools.clone(),
                commitment,
                account_manager.as_ref(),
                executor,
                false,
            )
            .await?;
            info!("[DryRun] Sweep done; exiting");
            return Ok(());
        }
    }

    let (subscription_manager, subscription_mode) =
        init_subscription_provider(&ws_url, commitment).await?;
    info!("SubscriptionProvider initialized (mode: {})", subscription_mode);

    if let Err(err) = crate::trade::execution::refresh_jito_tip_account(&ctx).await {
        warn!("[JITO] Failed to refresh tip account cache: {:?}", err);
    }

    ctx.set_subscription_manager(Arc::clone(&subscription_manager))
        .await;

    // Track active pool subscriptions
    let active_pools: Arc<DashMap<Pubkey, PoolSubscriptionHandle>> = Arc::new(DashMap::new());

    let last_stale_removed = Arc::new(tokio::sync::RwLock::new(0usize));

    // Periodic status log (active pools, subs, pairs) + metrics file
    let status_handle = {
        let active_pools_status = Arc::clone(&active_pools);
        let detector_status = Arc::clone(&detector);
        let subscription_manager_status = Arc::clone(&subscription_manager);
        let last_stale_removed = Arc::clone(&last_stale_removed);
        let store_status = Arc::clone(&store);
        let quarantine_status = Arc::clone(&quarantine);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                interval.tick().await;
                let active = active_pools_status.len();
                let det_stats = detector_status.stats();
                let sub_count = subscription_manager_status.total_subscriptions();
                let stale = *last_stale_removed.read().await;
                let pools_with_snapshots = store_status.len();
                let now = Utc::now();
                let mut newest_age_s: i64 = 0;
                let mut oldest_age_s: i64 = 0;
                let mut updated_last_60s: usize = 0;
                let mut updated_last_2m: usize = 0;  // ✅ 健康池子（2分钟内更新）
                let mut stale_over_5m: usize = 0;
                let mut stale_pool_names: Vec<String> = Vec::new();  // ✅ 记录陈旧池子名称

                if pools_with_snapshots > 0 {
                    let mut newest = i64::MAX;
                    let mut oldest = 0i64;
                    for entry in store_status.iter() {
                        let age = (now - entry.value().timestamp).num_seconds().max(0);
                        if age <= 60 {
                            updated_last_60s += 1;
                        }
                        if age <= 120 {
                            updated_last_2m += 1;
                        }
                        if age < newest {
                            newest = age;
                        }
                        if age > oldest {
                            oldest = age;
                        }
                        if age >= 300 {
                            stale_over_5m += 1;
                            // ✅ 收集陈旧池子名称（最多记录前 5 个）
                            if stale_pool_names.len() < 5 {
                                stale_pool_names.push(format!(
                                    "{}({}s)",
                                    entry.value().descriptor.label,
                                    age
                                ));
                            }
                        }
                    }
                    newest_age_s = if newest == i64::MAX { 0 } else { newest };
                    oldest_age_s = oldest;
                }

                // ✅ 计算健康率（2分钟内有更新的池子占比）
                let health_pct = if pools_with_snapshots > 0 {
                    (updated_last_2m as f64 / pools_with_snapshots as f64 * 100.0) as usize
                } else {
                    0
                };

                // ✅ 当有陈旧池子时打印警告
                if stale_over_5m > 0 && !stale_pool_names.is_empty() {
                    warn!(
                        "⚠️ {} stale pools (no updates for 5+ min): {}{}",
                        stale_over_5m,
                        stale_pool_names.join(", "),
                        if stale_over_5m > 5 { " ..." } else { "" }
                    );
                }
                // 统计当前隔离中的池子数量
                let quarantined_count = quarantine_status
                    .iter()
                    .filter(|entry| entry.value().is_quarantined())
                    .count();

                // ✅ 计算初始化完成率
                let ready_pct = if active > 0 {
                    (pools_with_snapshots as f64 / active as f64 * 100.0) as usize
                } else {
                    0
                };

                // ✅ 检测是否所有池子都已初始化（只在首次达到 100% 时打印）
                static ALL_READY_LOGGED: std::sync::atomic::AtomicBool =
                    std::sync::atomic::AtomicBool::new(false);

                if active > 0 && pools_with_snapshots >= active &&
                   !ALL_READY_LOGGED.load(std::sync::atomic::Ordering::SeqCst) {
                    ALL_READY_LOGGED.store(true, std::sync::atomic::Ordering::SeqCst);
                    info!(
                        "\x1b[1;32m🎉 ALL POOLS READY!\x1b[0m {}/{} pools initialized with prices",
                        pools_with_snapshots, active
                    );
                    // 记录到 pool log
                    let ts = Utc::now().to_rfc3339();
                    let log_line = format!(
                        "{}\tALL_READY\tpools={}\tsnapshots={}\tpairs={}",
                        ts, active, pools_with_snapshots, det_stats.total_pairs
                    );
                    append_pool_state_log(&log_line);
                }

                info!(
                    "\x1b[1;36m[STATUS]\x1b[0m pools={} subs={} pairs={} both={} snapshots={} ready={}% health={}% updated_last_60s={} stale_over_5m={} quarantined={} stale_removed_last={}",
                    active,
                    sub_count,
                    det_stats.total_pairs,
                    det_stats.pairs_with_both,
                    pools_with_snapshots,
                    ready_pct,
                    health_pct,
                    updated_last_60s,
                    stale_over_5m,
                    quarantined_count,
                    stale
                );

                // Append compact metrics line to arb_metrics.txt for offline analysis
                let ts = Utc::now().to_rfc3339();
                let line = format!(
                    "{}\tactive_pools={}\tsnapshots={}\tupdated_last_60s={}\tnewest_age_s={}\toldest_age_s={}\tstale_over_5m={}\tsubs={}\tpairs={}\tpairs_both={}\tquarantined={}\tstale_removed_last={}",
                    ts,
                    active,
                    pools_with_snapshots,
                    updated_last_60s,
                    newest_age_s,
                    oldest_age_s,
                    stale_over_5m,
                    sub_count,
                    det_stats.total_pairs,
                    det_stats.pairs_with_both,
                    quarantined_count,
                    stale,
                );

                // 写入原有 metrics 文本
                append_metrics_log(&line);

                // 追加到池子状态日志
                append_pool_state_log(&line);
            }
        })
    };

    // Start static pool subscriptions
    for descriptor in static_pools {
        let handle = spawn_pool_subscription(
            descriptor.clone(),
            ws_url.clone(),
            ctx.clone(),
            Arc::clone(&store),
            thresholds.clone(),
            commitment,
            Arc::clone(&tracker),
            Arc::clone(&detector),
            Arc::clone(&subscription_manager),
            Arc::clone(&quarantine),
            Arc::clone(&dispatcher),
            strategy.clone(),
            Arc::clone(&slippage_cache),
            None,
            Arc::clone(&cache_tracker),
        );

        active_pools.insert(
            descriptor.address,
            PoolSubscriptionHandle { descriptor, handle },
        );
    }

    // If we already refreshed and filtered PoolManager pools, subscribe them now
    // ✅ 使用之前已过滤的 filtered_dynamic_pools，避免重复滑点过滤

    if let Some(ref pm) = &pool_manager {
        if !filtered_dynamic_pools.is_empty() {
            info!(
                "[PoolManager] Subscribing to {} filtered dynamic pools",
                filtered_dynamic_pools.len()
            );

            // Subscribe to filtered pools
            // ✅ 速率限制：每秒最多启动 2 个池，避免超过 RPC 限制
            const POOL_SPAWN_INTERVAL_MS: u64 = 500;
            let pool_count = filtered_dynamic_pools.len();
            for (i, (key, meta)) in filtered_dynamic_pools.into_iter().enumerate() {
                let descriptor = PoolDescriptor {
                    label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                    address: key,
                    kind: meta.dex,
                };

                info!(
                    "[PoolManager] Subscribing to new pool ({}/{}): {} {} ({})",
                    i + 1,
                    pool_count,
                    descriptor.kind,
                    descriptor.label,
                    key
                );

                let handle = spawn_pool_subscription(
                    descriptor.clone(),
                    ws_url.clone(),
                    ctx.clone(),
                    Arc::clone(&store),
                    thresholds.clone(),
                    commitment,
                    Arc::clone(&tracker),
                    Arc::clone(&detector),
                    Arc::clone(&subscription_manager),
                    Arc::clone(&quarantine),
                    Arc::clone(&dispatcher),
                    strategy.clone(),
                    Arc::clone(&slippage_cache),
                    Some(Arc::clone(pm)),
                    Arc::clone(&cache_tracker),
                );

                active_pools.insert(key, PoolSubscriptionHandle { descriptor, handle });

                // ✅ 每启动一个池子后等待，避免同时发起太多 RPC 请求
                if i + 1 < pool_count {
                    tokio::time::sleep(Duration::from_millis(POOL_SPAWN_INTERVAL_MS)).await;
                }
            }

            info!(
                "[PoolManager] Finished spawning {} dynamic pool subscriptions",
                pool_count
            );


            let clear_cache = !ctx.spot_only_mode();
            refresh_runtime_caches(&ctx, &active_pools, clear_cache).await;
            write_filtered_pools(pm, &active_pools).await;
        }
    }

    // Start PoolManager refresh task if available
    let skip_initial_refresh = initial_refresh_result.is_some();
    // 为后续 slippage refresh 任务提前 clone
    let slippage_cache_for_refresh = Arc::clone(&slippage_cache);
    let active_pools_for_refresh = Arc::clone(&active_pools);
    let pool_manager_handle = if let Some(pm) = pool_manager {
        let pm_clone = Arc::clone(&pm);
        let active_pools_clone = Arc::clone(&active_pools);
        let ws_url_clone = ws_url.clone();
        let ctx_clone = ctx.clone();
        let store_clone = Arc::clone(&store);
        let thresholds_clone = thresholds.clone();
        let tracker_clone = Arc::clone(&tracker);
        let detector_clone = Arc::clone(&detector);
        let subscription_manager_clone = Arc::clone(&subscription_manager);
        let quarantine_clone = Arc::clone(&quarantine);
        let dispatcher_clone = Arc::clone(&dispatcher);
        let strategy_clone = strategy.clone();
        let account_manager_clone = Arc::clone(&account_manager);
        let atomic_executor_clone = atomic_executor.clone();
        let pool_slippage_bps_value = pool_slippage_bps;
        let pool_slippage_trade_size_quote_value = pool_slippage_trade_size_quote;
        
        // 获取 signer_keypair（如果可用）
        let signer_keypair_clone = load_signer_keypair_from_env().await;

        Some(tokio::spawn(async move {
            run_pool_manager_loop(
                pm_clone,
                active_pools_clone,
                ws_url_clone,
                ctx_clone,
                store_clone,
                thresholds_clone,
                commitment,
                tracker_clone,
                detector_clone,
                subscription_manager_clone,
                quarantine_clone,
                dispatcher_clone,
                strategy_clone,
                atomic_executor_clone,
                signer_keypair_clone,
                account_manager_clone,
                Arc::clone(&slippage_cache),
                Arc::clone(&cache_tracker),
                pool_slippage_bps_value,
                pool_slippage_trade_size_quote_value,
                use_simulated_buy_slippage,
                skip_initial_refresh,
            )
            .await;
        }))
    } else {
        None
    };

    // 启动定时滑点刷新任务（绑定模拟 + quote + tick/bin 缓存）
    let slippage_refresh_handle = {
        let slippage_refresh_mins = get_slippage_refresh_interval_mins();
        if slippage_refresh_mins > 0 && pool_slippage_trade_size_quote > 0.0 {
            let slippage_cache_clone = slippage_cache_for_refresh;
            let active_pools_clone = active_pools_for_refresh;
            let trade_size_quote = pool_slippage_trade_size_quote;
            let ctx_clone = ctx.clone();
            let account_manager_clone = Arc::clone(&account_manager);
            let atomic_executor_clone = atomic_executor.clone();
            
            info!(
                "[PoolSlippage] Background refresh enabled: interval={}min",
                slippage_refresh_mins
            );
            
            Some(tokio::spawn(async move {
                let interval = Duration::from_secs(slippage_refresh_mins * 60);
                let mut ticker = tokio::time::interval(interval);
                ticker.tick().await; // 跳过第一次立即触发

                loop {
                    ticker.tick().await;
                    let start = std::time::Instant::now();
                    refresh_runtime_caches(&ctx_clone, &active_pools_clone, false).await;
                    let descriptors = collect_descriptors_from_active_pools(&active_pools_clone);
                    let stats = apply_simulated_slippage_filter(
                        &ctx_clone,
                        commitment,
                        account_manager_clone.as_ref(),
                        atomic_executor_clone.as_deref(),
                        &slippage_cache_clone,
                        trade_size_quote,
                        descriptors,
                        None,
                        " refresh",
                    )
                    .await
                    .1;
                    info!(
                        "[PoolSlippage] Background refresh: updated={}, errors={}, elapsed={:?}",
                        stats.sampled_updated, stats.sampled_errors, start.elapsed()
                    );
                }
            }))
        } else {
            info!(
                "[PoolSlippage] Background refresh disabled (interval={}min, trade_size={})",
                slippage_refresh_mins, pool_slippage_trade_size_quote
            );
            None
        }
    };

    info!("Dynamic pool monitoring started; press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;
    info!("Shutdown requested; cancelling subscriptions");

    // Shutdown
    subscription_manager.shutdown().await;
    status_handle.abort();

    if let Some(handle) = pool_manager_handle {
        handle.abort();
    }

    if let Some(handle) = slippage_refresh_handle {
        handle.abort();
    }

    // Abort all pool subscription tasks
    for entry in active_pools.iter() {
        entry.value().handle.abort();
    }
    active_pools.clear();

    Ok(())
}

/// Spawn a subscription task for a single pool
fn spawn_pool_subscription(
    descriptor: PoolDescriptor,
    ws_url: String,
    ctx: AppContext,
    store: SnapshotStore,
    thresholds: PriceThresholds,
    commitment: CommitmentConfig,
    tracker: OpportunityTracker,
    detector: Arc<OpportunityDetector>,
    subscription_manager: Arc<SubscriptionProvider>,
    quarantine: QuarantineTracker,
    dispatcher: Arc<TradeDispatcher>,
    strategy: Option<Arc<OpportunityFilter>>,
    slippage_cache: SlippageCache,
    pool_manager: Option<Arc<PoolManager>>,
    cache_tracker: PoolCacheTracker,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if let Err(err) = subscribe_pool(
            ws_url,
            ctx,
            store,
            descriptor.clone(),
            thresholds,
            commitment,
            tracker,
            detector,
            subscription_manager,
            quarantine,
            dispatcher,
            strategy,
            slippage_cache,
            pool_manager,
            cache_tracker,
        )
        .await
        {
            error!(
                "Pool subscription ended with error for {} {}: {err:?}",
                descriptor.kind, descriptor.label
            );
        }
    })
}

async fn prewarm_meteora_dlmm_batch(ctx: &AppContext, pools: &[Pubkey]) {
    if pools.is_empty() {
        return;
    }

    use once_cell::sync::Lazy;
    static BATCH_SIDECAR: Lazy<Result<crate::sidecar_client::SidecarClient, anyhow::Error>> =
        Lazy::new(|| crate::sidecar_client::SidecarClient::from_env());

    let sidecar = match BATCH_SIDECAR.as_ref() {
        Ok(client) => client,
        Err(e) => {
            debug!("DLMM batch warmup skipped: SidecarClient unavailable: {}", e);
            return;
        }
    };

    let cached = crate::dex::meteora_dlmm::warmup_bin_arrays(ctx, sidecar, pools).await;
    if cached > 0 {
        info!(
            "DLMM batch warmup cached bin_arrays for {}/{} pools",
            cached,
            pools.len()
        );
    } else {
        debug!(
            "DLMM batch warmup cached no bin_arrays for {} pools",
            pools.len()
        );
    }
}

fn collect_dry_run_pools(
    static_pools: &[PoolDescriptor],
    dynamic_pools: &[(Pubkey, crate::pool_manager::PoolMeta)],
) -> Vec<PoolDescriptor> {
    let mut pools = Vec::new();
    let mut seen: HashSet<Pubkey> = HashSet::new();

    for descriptor in static_pools {
        if seen.insert(descriptor.address) {
            pools.push(descriptor.clone());
        }
    }

    for (key, meta) in dynamic_pools {
        if seen.insert(*key) {
            pools.push(PoolDescriptor {
                label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                address: *key,
                kind: meta.dex,
            });
        }
    }

    pools
}

async fn run_dry_run_sweep(
    ctx: &AppContext,
    pools: Vec<PoolDescriptor>,
    commitment: CommitmentConfig,
    account_manager: &TradeAccountManager,
    executor: &AtomicExecutor,
    refresh_warmup: bool,
) -> Result<()> {
    let trade_size_quote = get_dry_run_trade_size_quote()?;
    let skip_wsol_wrap = std::env::var("SKIP_WSOL_WRAP")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    info!(
        "[DryRun] Simulating {} pools with quote_in={:.6} SOL",
        pools.len(),
        trade_size_quote
    );

    let mut trade_id: u64 = 0;
    for descriptor in pools {
        trade_id += 1;

        let response = match ctx
            .rpc_client()
            .get_account_with_commitment(&descriptor.address, commitment)
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                warn!(
                    "[DryRun] Failed to fetch pool {} {}: {:?}",
                    descriptor.kind, descriptor.label, err
                );
                continue;
            }
        };

        let RpcResponse { context, value } = response;
        let account = match value {
            Some(account) => account,
            None => {
                warn!(
                    "[DryRun] Pool account missing {} {} ({})",
                    descriptor.kind, descriptor.label, descriptor.address
                );
                continue;
            }
        };

        match descriptor.kind {
            DexKind::RaydiumAmmV4 => {
                if let Err(err) = crate::dex::raydium_amm::warmup_static_info(&descriptor.address).await
                {
                    warn!(
                        "[DryRun] Raydium AMM warmup failed for {}: {:?}",
                        descriptor.label, err
                    );
                }
            }
            DexKind::RaydiumCpmm => {
                if let Err(err) = crate::dex::raydium_cpmm::warmup_static_info(
                    &descriptor,
                    &account.data,
                    ctx,
                )
                .await
                {
                    warn!(
                        "[DryRun] Raydium CPMM warmup failed for {}: {:?}",
                        descriptor.label, err
                    );
                }
            }
            _ => {}
        }

        let decode_ctx = if matches!(
            descriptor.kind,
            DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
        ) {
            ctx.clone_non_spot_only()
        } else {
            (*ctx).clone()
        };

        let snapshot = match snapshot_from_account_data(
            &descriptor,
            account.data,
            context,
            &decode_ctx,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                warn!(
                    "[DryRun] Decode failed for {} {}: {:?}",
                    descriptor.kind, descriptor.label, err
                );
                continue;
            }
        };

        if snapshot.normalized_pair.quote != *SOL_MINT {
            continue;
        }

        if descriptor.kind == DexKind::MeteoraDlmm && snapshot.lb_accounts.is_some() {
            if let Err(err) = crate::trade::meteora_dlmm::auto_inject_pool_config(
                ctx,
                &snapshot,
                executor.signer_pubkey(),
            ) {
                warn!(
                    "[DryRun] DLMM auto-inject failed for {}: {:?}",
                    descriptor.label, err
                );
            }
        }

        if refresh_warmup {
            if let Some(clmm) = &snapshot.clmm_accounts {
                match descriptor.kind {
                    DexKind::OrcaWhirlpool => {
                        use crate::dex::orca;
                        let tick_arrays = if ctx.spot_only_mode() {
                            orca::warmup_tick_arrays(
                                ctx,
                                &clmm.program_id,
                                &descriptor.address,
                                clmm.tick_current_index,
                                clmm.tick_spacing,
                            )
                            .await
                        } else {
                            orca::compute_tick_array_subscriptions(
                                &clmm.program_id,
                                &descriptor.address,
                                clmm.tick_current_index,
                                clmm.tick_spacing,
                            )
                        };
                        if !tick_arrays.is_empty() {
                            ctx.set_orca_tick_arrays(&descriptor.address, tick_arrays.clone());
                            crate::trade::orca::inject_tick_arrays(
                                descriptor.address,
                                tick_arrays.clone(),
                            );
                        }
                    }
                    DexKind::RaydiumClmm => {
                        use crate::dex::raydium;
                        let tick_arrays = if ctx.spot_only_mode() {
                            raydium::warmup_tick_arrays(
                                ctx,
                                &clmm.program_id,
                                &descriptor.address,
                                clmm.tick_current_index,
                                clmm.tick_spacing,
                            )
                            .await
                        } else {
                            raydium::compute_tick_array_subscriptions(
                                &clmm.program_id,
                                &descriptor.address,
                                clmm.tick_current_index,
                                clmm.tick_spacing,
                            )
                        };
                        if !tick_arrays.is_empty() {
                            crate::trade::raydium::inject_tick_arrays(
                                descriptor.address,
                                tick_arrays.clone(),
                            );
                        }
                    }
                    _ => {}
                }
            }
        }

        let accounts = resolve_dry_run_accounts(ctx, account_manager, &snapshot, executor).await;
        let min_base_out = min_base_out_for_snapshot(&snapshot);
        let expected_base_out = if snapshot.normalized_price > 0.0 && snapshot.normalized_price.is_finite() {
            trade_size_quote / snapshot.normalized_price
        } else {
            0.0
        };
        let (norm_base_decimals, norm_quote_decimals) = if snapshot.normalized_pair.inverted {
            (snapshot.quote_decimals, snapshot.base_decimals)
        } else {
            (snapshot.base_decimals, snapshot.quote_decimals)
        };

        let request = TradeRequest {
            side: TradeSide::Buy,
            base_amount: min_base_out,
            max_slippage_bps: 0.0001,
            min_amount_out: Some(min_base_out),
            quote_amount_in: Some(trade_size_quote),
            accounts: accounts.clone(),
        };

        let wrap_instructions = if snapshot.normalized_pair.quote == *SOL_MINT {
            build_wsol_wrap_instructions(accounts.payer, trade_size_quote, skip_wsol_wrap)
        } else {
            Vec::new()
        };

        let mut local_plan = None;
        match crate::trade::executor_for(descriptor.kind).prepare_swap(&snapshot, &request) {
            Ok(plan) => {
                local_plan = Some(plan);
            }
            Err(err) => {
                let msg = format!(
                    "\n========== TRADE #{} ❌ ==========\n\
                     STATUS: ❌ BUILD_FAILED\n\
                     POOL: {} {} ({})\n\
                     ERROR: {:?}\n\
                     =====================================",
                    trade_id,
                    descriptor.kind,
                    descriptor.label,
                    descriptor.address,
                    err
                );
                append_trade_log(&msg);
            }
        };

        if let Some(buy_plan) = local_plan {
            let mut instructions = wrap_instructions.clone();
            instructions.extend(buy_plan.instructions.clone());
            let plan = AtomicTradePlan {
                buy_plan,
                sell_plan: TradePlan::empty("dry_run"),
                instructions,
                description: format!(
                    "Dry-run swap {} spend {:.6} SOL",
                    descriptor.label, trade_size_quote
                ),
                net_quote_profit: 0.0,
                tip_lamports: 0,
                buy_pool: descriptor.address,
                sell_pool: descriptor.address,
                buy_dex_kind: descriptor.kind,
                sell_dex_kind: descriptor.kind,
                buy_expected_quote_in: Some(trade_size_quote),
                buy_expected_base_out: Some(expected_base_out),
                buy_expected_price: Some(snapshot.normalized_price),
                sell_expected_price: None,
                buy_base_decimals: Some(norm_base_decimals),
                buy_quote_decimals: Some(norm_quote_decimals),
            };

            if let Err(err) = executor
                .simulate_atomic_plan(&plan, &accounts, &accounts, trade_id)
                .await
            {
                warn!(
                    "[DryRun] Simulation failed for {} {}: {:?}",
                    descriptor.kind, descriptor.label, err
                );
            }
        }
    }

    Ok(())
}

async fn simulate_pools_for_buy_slippage(
    ctx: &AppContext,
    pools: Vec<PoolDescriptor>,
    commitment: CommitmentConfig,
    account_manager: &TradeAccountManager,
    executor: &AtomicExecutor,
    trade_size_quote: f64,
) -> (Vec<SimulatedPoolResult>, Vec<PoolDescriptor>, usize, usize) {
    if pools.is_empty() || trade_size_quote <= 0.0 || !trade_size_quote.is_finite() {
        return (Vec::new(), Vec::new(), 0, 0);
    }

    let skip_wsol_wrap = std::env::var("SKIP_WSOL_WRAP")
        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
        .unwrap_or(false);

    let dlmm_pools: Vec<Pubkey> = pools
        .iter()
        .filter(|d| d.kind == DexKind::MeteoraDlmm)
        .map(|d| d.address)
        .collect();
    if !dlmm_pools.is_empty() {
        prewarm_meteora_dlmm_batch(ctx, &dlmm_pools).await;
    }

    let clmm_pools: Vec<(Pubkey, DexKind)> = pools
        .iter()
        .filter(|d| d.kind == DexKind::OrcaWhirlpool || d.kind == DexKind::RaydiumClmm)
        .map(|d| (d.address, d.kind))
        .collect();
    if !clmm_pools.is_empty() {
        prewarm_clmm_tick_arrays_batch(ctx, &clmm_pools).await;
    }

    let mut results = Vec::new();
    let mut fallback = Vec::new();
    let mut failed = 0usize;
    let mut skipped = 0usize;
    let mut trade_id: u64 = 0;

    for descriptor in pools {
        trade_id += 1;

        let response = match ctx
            .rpc_client()
            .get_account_with_commitment(&descriptor.address, commitment)
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                debug!(
                    "[PoolSim] Failed to fetch pool {} {}: {:?}",
                    descriptor.kind, descriptor.label, err
                );
                failed += 1;
                fallback.push(descriptor);
                continue;
            }
        };

        let RpcResponse { context, value } = response;
        let account = match value {
            Some(account) => account,
            None => {
                debug!(
                    "[PoolSim] Pool account missing {} {} ({})",
                    descriptor.kind, descriptor.label, descriptor.address
                );
                failed += 1;
                fallback.push(descriptor);
                continue;
            }
        };

        match descriptor.kind {
            DexKind::RaydiumAmmV4 => {
                if let Err(err) =
                    crate::dex::raydium_amm::warmup_static_info(&descriptor.address).await
                {
                    debug!(
                        "[PoolSim] Raydium AMM warmup failed for {}: {:?}",
                        descriptor.label, err
                    );
                }
            }
            DexKind::RaydiumCpmm => {
                if let Err(err) =
                    crate::dex::raydium_cpmm::warmup_static_info(&descriptor, &account.data, ctx)
                        .await
                {
                    debug!(
                        "[PoolSim] Raydium CPMM warmup failed for {}: {:?}",
                        descriptor.label, err
                    );
                }
            }
            _ => {}
        }

        let decode_ctx = if matches!(
            descriptor.kind,
            DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
        ) {
            ctx.clone_non_spot_only()
        } else {
            (*ctx).clone()
        };

        let snapshot = match snapshot_from_account_data(
            &descriptor,
            account.data,
            context,
            &decode_ctx,
        )
        .await
        {
            Ok(snapshot) => snapshot,
            Err(err) => {
                debug!(
                    "[PoolSim] Decode failed for {} {}: {:?}",
                    descriptor.kind, descriptor.label, err
                );
                failed += 1;
                fallback.push(descriptor);
                continue;
            }
        };

        if snapshot.normalized_pair.quote != *SOL_MINT {
            skipped += 1;
            continue;
        }

        if descriptor.kind == DexKind::MeteoraDlmm && snapshot.lb_accounts.is_some() {
            if let Err(err) = crate::trade::meteora_dlmm::auto_inject_pool_config(
                ctx,
                &snapshot,
                executor.signer_pubkey(),
            ) {
                debug!(
                    "[PoolSim] DLMM auto-inject failed for {}: {:?}",
                    descriptor.label, err
                );
            }
        }

        let accounts = resolve_dry_run_accounts(ctx, account_manager, &snapshot, executor).await;
        let min_base_out = min_base_out_for_snapshot(&snapshot);
        let expected_base_out = if snapshot.normalized_price > 0.0 && snapshot.normalized_price.is_finite() {
            trade_size_quote / snapshot.normalized_price
        } else {
            0.0
        };
        let (norm_base_decimals, norm_quote_decimals) = if snapshot.normalized_pair.inverted {
            (snapshot.quote_decimals, snapshot.base_decimals)
        } else {
            (snapshot.base_decimals, snapshot.quote_decimals)
        };

        let request = TradeRequest {
            side: TradeSide::Buy,
            base_amount: min_base_out,
            max_slippage_bps: 0.0001,
            min_amount_out: Some(min_base_out),
            quote_amount_in: Some(trade_size_quote),
            accounts: accounts.clone(),
        };

        let wrap_instructions = if snapshot.normalized_pair.quote == *SOL_MINT {
            build_wsol_wrap_instructions(accounts.payer, trade_size_quote, skip_wsol_wrap)
        } else {
            Vec::new()
        };

        let buy_plan = match crate::trade::executor_for(descriptor.kind).prepare_swap(&snapshot, &request) {
            Ok(plan) => plan,
            Err(err) => {
                debug!(
                    "[PoolSim] Build failed for {} {}: {:?}",
                    descriptor.kind, descriptor.label, err
                );
                failed += 1;
                fallback.push(descriptor);
                continue;
            }
        };

        let mut instructions = wrap_instructions.clone();
        instructions.extend(buy_plan.instructions.clone());
        let plan = AtomicTradePlan {
            buy_plan,
            sell_plan: TradePlan::empty("pool_sim"),
            instructions,
            description: format!("pool_sim {} spend {:.6} SOL", descriptor.label, trade_size_quote),
            net_quote_profit: 0.0,
            tip_lamports: 0,
            buy_pool: descriptor.address,
            sell_pool: descriptor.address,
            buy_dex_kind: descriptor.kind,
            sell_dex_kind: descriptor.kind,
            buy_expected_quote_in: Some(trade_size_quote),
            buy_expected_base_out: Some(expected_base_out),
            buy_expected_price: Some(snapshot.normalized_price),
            sell_expected_price: None,
            buy_base_decimals: Some(norm_base_decimals),
            buy_quote_decimals: Some(norm_quote_decimals),
        };

        // ✅ Retry logic: try simulate once, if fails wait 5s and retry
        let mut summary_result = None;
        for attempt in 1..=2 {
            match executor
                .simulate_atomic_plan_with_summary(&plan, &accounts, &accounts, trade_id, false)
                .await
            {
                Ok(summary) => {
                    summary_result = Some(summary);
                    break;
                }
                Err(err) => {
                    if attempt == 1 {
                        debug!(
                            "[PoolSim] Simulation failed for {} {}: {:?}, retrying in 5s",
                            descriptor.kind, descriptor.label, err
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    } else {
                        debug!(
                            "[PoolSim] Simulation failed for {} {} after retry: {:?}",
                            descriptor.kind, descriptor.label, err
                        );
                    }
                }
            }
        }

        let summary = match summary_result {
            Some(s) => s,
            None => {
                failed += 1;
                fallback.push(descriptor);
                continue;
            }
        };

        let mut buy_slippage_pct = match summary.and_then(|s| s.slippage_pct) {
            Some(value) if value.is_finite() => value,
            _ => {
                failed += 1;
                fallback.push(descriptor);
                continue;
            }
        };
        if buy_slippage_pct < 0.0 {
            buy_slippage_pct = 0.0;
        }

        results.push(SimulatedPoolResult {
            descriptor,
            pair: snapshot.normalized_pair,
            buy_slippage_pct,
        });
    }

    (results, fallback, failed, skipped)
}

fn filter_simulated_pairs(
    results: Vec<SimulatedPoolResult>,
    existing_pairs: Option<&HashSet<NormalizedPair>>,
) -> (Vec<SimulatedPoolResult>, usize) {
    if results.is_empty() {
        return (results, 0);
    }

    let mut counts: HashMap<NormalizedPair, usize> = HashMap::new();
    for entry in &results {
        *counts.entry(entry.pair).or_insert(0) += 1;
    }

    let mut kept = Vec::new();
    let mut dropped = 0usize;
    for entry in results {
        let has_existing = existing_pairs
            .map(|pairs| pairs.contains(&entry.pair))
            .unwrap_or(false);
        let count = counts.get(&entry.pair).copied().unwrap_or(0);
        if has_existing || count >= 2 {
            kept.push(entry);
        } else {
            dropped += 1;
        }
    }

    (kept, dropped)
}

fn collect_existing_pairs(store: &SnapshotStore) -> HashSet<NormalizedPair> {
    store
        .iter()
        .map(|entry| entry.value().normalized_pair)
        .collect()
}

fn pair_key(base: Pubkey, quote: Pubkey) -> (Pubkey, Pubkey) {
    if base < quote {
        (base, quote)
    } else {
        (quote, base)
    }
}

fn collect_existing_pair_keys(store: &SnapshotStore) -> HashSet<(Pubkey, Pubkey)> {
    collect_existing_pairs(store)
        .into_iter()
        .map(|pair| pair_key(pair.base, pair.quote))
        .collect()
}

fn dlmm_cache_ready(ctx: &AppContext, pool: &Pubkey) -> bool {
    let merged = ctx
        .get_dlmm_bin_arrays(pool)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let x_to_y = ctx
        .get_dlmm_bin_arrays_for_direction(pool, true)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    let y_to_x = ctx
        .get_dlmm_bin_arrays_for_direction(pool, false)
        .map(|v| !v.is_empty())
        .unwrap_or(false);
    merged || x_to_y || y_to_x
}

async fn warmup_dlmm_pools(ctx: &AppContext, pools: &[Pubkey]) -> HashSet<Pubkey> {
    if pools.is_empty() {
        return HashSet::new();
    }

    use once_cell::sync::Lazy;
    static WARMUP_SIDECAR: Lazy<Result<crate::sidecar_client::SidecarClient, anyhow::Error>> =
        Lazy::new(|| crate::sidecar_client::SidecarClient::from_env());

    let sidecar = match WARMUP_SIDECAR.as_ref() {
        Ok(client) => client,
        Err(e) => {
            warn!("DLMM warmup skipped: SidecarClient unavailable: {}", e);
            return HashSet::new();
        }
    };

    let _ = crate::dex::meteora_dlmm::warmup_bin_arrays(ctx, sidecar, pools).await;

    let mut ok = HashSet::new();
    for pool in pools {
        if dlmm_cache_ready(ctx, pool) {
            ok.insert(*pool);
        }
    }
    ok
}

async fn warmup_clmm_pools(
    ctx: &AppContext,
    pools: &[(Pubkey, crate::dex::DexKind)],
) -> HashSet<Pubkey> {
    use crate::dex::{orca, raydium};

    if pools.is_empty() {
        return HashSet::new();
    }

    use once_cell::sync::Lazy;
    static WARMUP_SIDECAR: Lazy<Result<crate::sidecar_client::SidecarClient, anyhow::Error>> =
        Lazy::new(|| crate::sidecar_client::SidecarClient::from_env());

    let sidecar = match WARMUP_SIDECAR.as_ref() {
        Ok(client) => client,
        Err(e) => {
            warn!("CLMM warmup skipped: SidecarClient unavailable: {}", e);
            return HashSet::new();
        }
    };

    let mut ok = HashSet::new();

    for (pool_address, dex_kind) in pools {
        let program_id = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ctx.get_account_owner(pool_address),
        )
        .await
        {
            Ok(Ok(pid)) => pid,
            _ => continue,
        };

        let pool_str = pool_address.to_string();
        let (tick_current, tick_spacing) = match dex_kind {
            crate::dex::DexKind::OrcaWhirlpool => {
                let resp = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sidecar.introspect_orca(&pool_str),
                )
                .await
                {
                    Ok(Ok(resp)) if resp.ok => resp,
                    _ => continue,
                };
                match (resp.tick_current_index, resp.tick_spacing) {
                    (Some(t), Some(s)) => (t, s),
                    _ => continue,
                }
            }
            crate::dex::DexKind::RaydiumClmm => {
                let resp = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sidecar.introspect_raydium(&pool_str),
                )
                .await
                {
                    Ok(Ok(resp)) if resp.ok => resp,
                    _ => continue,
                };
                match (resp.tick_current_index, resp.tick_spacing) {
                    (Some(t), Some(s)) => (t, s),
                    _ => continue,
                }
            }
            _ => continue,
        };

        let tick_arrays = match dex_kind {
            crate::dex::DexKind::OrcaWhirlpool => {
                let addresses =
                    orca::compute_tick_array_subscriptions(&program_id, pool_address, tick_current, tick_spacing);
                if addresses.is_empty() {
                    continue;
                }
                let found = match ctx
                    .get_orca_tick_arrays_batch(&addresses, tick_spacing)
                    .await
                {
                    Ok(found) => found,
                    Err(_) => continue,
                };
                if found.is_empty() {
                    continue;
                }
                let arrays: Vec<Pubkey> = found.into_iter().map(|(addr, _)| addr).collect();
                ctx.set_orca_tick_arrays(pool_address, arrays.clone());
                crate::trade::orca::inject_tick_arrays(*pool_address, arrays.clone());
                arrays
            }
            crate::dex::DexKind::RaydiumClmm => {
                let addresses =
                    raydium::compute_tick_array_subscriptions(&program_id, pool_address, tick_current, tick_spacing);
                if addresses.is_empty() {
                    continue;
                }
                let found = match ctx.get_raydium_tick_arrays_batch(&addresses).await {
                    Ok(found) => found,
                    Err(_) => continue,
                };
                if found.is_empty() {
                    continue;
                }
                let arrays: Vec<Pubkey> = found.into_iter().map(|(addr, _)| addr).collect();
                crate::trade::raydium::inject_tick_arrays(*pool_address, arrays.clone());
                arrays
            }
            _ => Vec::new(),
        };

        if !tick_arrays.is_empty() {
            ok.insert(*pool_address);
        }
    }

    ok
}

async fn warmup_pools_once(ctx: &AppContext, pools: &[PoolDescriptor]) -> HashSet<Pubkey> {
    let mut ok = HashSet::new();
    let mut dlmm_pools = Vec::new();
    let mut clmm_pools = Vec::new();

    for descriptor in pools {
        match descriptor.kind {
            crate::dex::DexKind::MeteoraDlmm => dlmm_pools.push(descriptor.address),
            crate::dex::DexKind::OrcaWhirlpool | crate::dex::DexKind::RaydiumClmm => {
                clmm_pools.push((descriptor.address, descriptor.kind));
            }
            _ => {
                ok.insert(descriptor.address);
            }
        }
    }

    ok.extend(warmup_dlmm_pools(ctx, &dlmm_pools).await);
    ok.extend(warmup_clmm_pools(ctx, &clmm_pools).await);

    ok
}

async fn warmup_pools_with_retry(
    ctx: &AppContext,
    pools: Vec<PoolDescriptor>,
    retry_delay: Duration,
) -> Vec<PoolDescriptor> {
    let ok_first = warmup_pools_once(ctx, &pools).await;
    let mut failed: Vec<PoolDescriptor> = pools
        .iter()
        .filter(|p| !ok_first.contains(&p.address))
        .cloned()
        .collect();

    if failed.is_empty() {
        return pools;
    }

    warn!(
        "[Warmup] {} pools failed warmup; retrying in {:?}",
        failed.len(),
        retry_delay
    );
    sleep(retry_delay).await;

    let ok_retry = warmup_pools_once(ctx, &failed).await;
    let mut keep = ok_first;
    keep.extend(ok_retry);

    failed.retain(|p| !keep.contains(&p.address));
    if !failed.is_empty() {
        warn!(
            "[Warmup] Dropping {} pools after retry (warmup failed)",
            failed.len()
        );
    }

    pools
        .into_iter()
        .filter(|p| keep.contains(&p.address))
        .collect()
}

async fn resolve_pool_pair_key(
    ctx: &AppContext,
    descriptor: &PoolDescriptor,
) -> Option<(Pubkey, Pubkey)> {
    let account = ctx.rpc_client().get_account(&descriptor.address).await.ok()?;

    match descriptor.kind {
        DexKind::RaydiumAmmV4 => {
            let _ = crate::dex::raydium_amm::warmup_static_info(&descriptor.address).await;
        }
        DexKind::RaydiumCpmm => {
            let _ =
                crate::dex::raydium_cpmm::warmup_static_info(descriptor, &account.data, ctx).await;
        }
        DexKind::PumpFunDlmm => {}
        _ => {}
    }

    let decode_ctx = if matches!(
        descriptor.kind,
        DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
    ) {
        ctx.clone_non_spot_only()
    } else {
        ctx.clone_for_alt_collection()
    };

    let context = RpcResponseContext {
        slot: 0,
        api_version: None,
    };
    let snapshot = snapshot_from_account_data(descriptor, account.data, context, &decode_ctx)
        .await
        .ok()?;

    Some(pair_key(snapshot.base_mint, snapshot.quote_mint))
}

async fn filter_pool_descriptors_by_pair_key(
    ctx: &AppContext,
    pools: Vec<PoolDescriptor>,
    existing_pairs: Option<&HashSet<(Pubkey, Pubkey)>>,
) -> Vec<PoolDescriptor> {
    let mut pair_counts: HashMap<(Pubkey, Pubkey), usize> = HashMap::new();
    let mut pool_pairs: HashMap<Pubkey, (Pubkey, Pubkey)> = HashMap::new();
    let mut decoded = Vec::new();
    let mut decode_failed = 0usize;

    for descriptor in pools.into_iter() {
        if let Some(pair) = resolve_pool_pair_key(ctx, &descriptor).await {
            *pair_counts.entry(pair).or_insert(0) += 1;
            pool_pairs.insert(descriptor.address, pair);
            decoded.push(descriptor);
        } else {
            decode_failed += 1;
        }
    }

    let mut kept = Vec::new();
    let mut dropped_pairs = 0usize;
    for descriptor in decoded {
        let Some(pair) = pool_pairs.get(&descriptor.address) else {
            continue;
        };
        let count = pair_counts.get(pair).copied().unwrap_or(0);
        let has_existing = existing_pairs
            .map(|pairs| pairs.contains(pair))
            .unwrap_or(false);
        if count >= 2 || has_existing {
            kept.push(descriptor);
        } else {
            dropped_pairs += 1;
        }
    }

    if decode_failed > 0 || dropped_pairs > 0 {
        info!(
            "[PoolFilter] Dropped pools: decode_failed={} single_pair={}",
            decode_failed, dropped_pairs
        );
    }

    kept
}

async fn filter_static_dynamic_by_pair_key(
    ctx: &AppContext,
    static_pools: Vec<PoolDescriptor>,
    dynamic_pools: Vec<(Pubkey, crate::pool_manager::PoolMeta)>,
) -> (Vec<PoolDescriptor>, Vec<(Pubkey, crate::pool_manager::PoolMeta)>) {
    let mut pair_counts: HashMap<(Pubkey, Pubkey), usize> = HashMap::new();
    let mut static_pairs: HashMap<Pubkey, (Pubkey, Pubkey)> = HashMap::new();
    let mut dynamic_pairs: HashMap<Pubkey, (Pubkey, Pubkey)> = HashMap::new();
    let mut decode_failed = 0usize;

    for (key, meta) in dynamic_pools.iter() {
        let pair = pair_key(meta.base_mint, meta.quote_mint);
        dynamic_pairs.insert(*key, pair);
        *pair_counts.entry(pair).or_insert(0) += 1;
    }

    for descriptor in static_pools.iter() {
        if let Some(pair) = resolve_pool_pair_key(ctx, descriptor).await {
            static_pairs.insert(descriptor.address, pair);
            *pair_counts.entry(pair).or_insert(0) += 1;
        } else {
            decode_failed += 1;
        }
    }

    let mut kept_static = Vec::new();
    let mut kept_dynamic = Vec::new();
    let mut dropped_pairs = 0usize;

    for descriptor in static_pools {
        let Some(pair) = static_pairs.get(&descriptor.address) else {
            continue;
        };
        if pair_counts.get(pair).copied().unwrap_or(0) >= 2 {
            kept_static.push(descriptor);
        } else {
            dropped_pairs += 1;
        }
    }

    for (key, meta) in dynamic_pools {
        let Some(pair) = dynamic_pairs.get(&key) else {
            continue;
        };
        if pair_counts.get(pair).copied().unwrap_or(0) >= 2 {
            kept_dynamic.push((key, meta));
        } else {
            dropped_pairs += 1;
        }
    }

    if decode_failed > 0 || dropped_pairs > 0 {
        info!(
            "[PoolFilter] Dropped pools: decode_failed={} single_pair={}",
            decode_failed, dropped_pairs
        );
    }

    (kept_static, kept_dynamic)
}

fn filter_dynamic_by_pair_key(
    dynamic_pools: Vec<(Pubkey, crate::pool_manager::PoolMeta)>,
    existing_pairs: &HashSet<(Pubkey, Pubkey)>,
) -> Vec<(Pubkey, crate::pool_manager::PoolMeta)> {
    let mut pair_counts: HashMap<(Pubkey, Pubkey), usize> = HashMap::new();
    let mut pool_pairs: HashMap<Pubkey, (Pubkey, Pubkey)> = HashMap::new();

    for (key, meta) in dynamic_pools.iter() {
        let pair = pair_key(meta.base_mint, meta.quote_mint);
        pool_pairs.insert(*key, pair);
        *pair_counts.entry(pair).or_insert(0) += 1;
    }

    let mut kept = Vec::new();
    let mut dropped = 0usize;
    for (key, meta) in dynamic_pools {
        let Some(pair) = pool_pairs.get(&key) else {
            continue;
        };
        let count = pair_counts.get(pair).copied().unwrap_or(0);
        if count >= 2 || existing_pairs.contains(pair) {
            kept.push((key, meta));
        } else {
            dropped += 1;
        }
    }

    if dropped > 0 {
        info!("[PoolFilter] Dropped {} single-pair pools", dropped);
    }

    kept
}

async fn apply_simulated_slippage_filter(
    ctx: &AppContext,
    commitment: CommitmentConfig,
    account_manager: &TradeAccountManager,
    executor: Option<&AtomicExecutor>,
    slippage_cache: &SlippageCache,
    trade_size_quote: f64,
    candidates: Vec<PoolDescriptor>,
    existing_pairs: Option<&HashSet<NormalizedPair>>,
    log_prefix: &str,
) -> (Vec<PoolDescriptor>, PoolSimStats) {
    let total = candidates.len();
    if total == 0 || trade_size_quote <= 0.0 || !trade_size_quote.is_finite() {
        return (
            candidates,
            PoolSimStats {
                total,
                ok: 0,
                failed: 0,
                skipped: 0,
                unpaired_dropped: 0,
                sampled_updated: 0,
                sampled_errors: 0,
                used_simulated: false,
            },
        );
    }

    if let Some(exec) = executor {
        let (sim_results, fallback_pools, failed, skipped) = simulate_pools_for_buy_slippage(
            ctx,
            candidates,
            commitment,
            account_manager,
            exec,
            trade_size_quote,
        )
        .await;
        let (sim_results, dropped_unpaired) =
            filter_simulated_pairs(sim_results, existing_pairs);

        let mut candidate_map: HashMap<Pubkey, PoolDescriptor> = HashMap::new();
        info!(
            "[PoolSim]{} total={} ok={} failed={} skipped={} unpaired_dropped={}",
            log_prefix,
            total,
            sim_results.len(),
            failed,
            skipped,
            dropped_unpaired
        );

        for entry in &sim_results {
            candidate_map.insert(entry.descriptor.address, entry.descriptor.clone());
            slippage_cache.insert(
                entry.descriptor.address,
                PoolSlippage {
                    buy_slippage_pct: entry.buy_slippage_pct,
                    sell_slippage_pct: f64::NAN,
                },
            );
        }
        for descriptor in fallback_pools {
            candidate_map
                .entry(descriptor.address)
                .or_insert(descriptor);
        }

        let candidate_count = candidate_map.len();
        let targets: Vec<(Pubkey, DexKind)> = candidate_map
            .values()
            .map(|entry| (entry.address, entry.kind))
            .collect();
        let (success_pools, updated, errors) = if !targets.is_empty() {
            sample_pool_slippage(
                ctx,
                slippage_cache,
                targets,
                trade_size_quote,
                true,
            )
            .await
        } else {
            (HashSet::new(), 0, 0)
        };
        if errors > 0 {
            warn!("[PoolSlippage] Sampled {} pools, {} errors", updated, errors);
        } else if updated > 0 {
            info!("[PoolSlippage] Sampled {} pools", updated);
        }

        // ✅ Keep pools only when quote slippage succeeded (simulate or fallback)
        let kept: Vec<PoolDescriptor> = candidate_map
            .into_iter()
            .filter(|(address, _)| success_pools.contains(address))
            .map(|(_, descriptor)| descriptor)
            .collect();
        let kept_count = kept.len();
        
        if errors > 0 {
            info!(
                "[PoolSlippage] Filtered out {} pools with failed sell slippage",
                candidate_count.saturating_sub(kept_count)
            );
        }
        
        return (
            kept,
            PoolSimStats {
                total,
                ok: kept_count,
                failed,
                skipped,
                unpaired_dropped: dropped_unpaired,
                sampled_updated: updated,
                sampled_errors: errors,
                used_simulated: true,
            },
        );
    }

    warn!(
        "[PoolSlippage]{} AtomicExecutor unavailable; falling back to sidecar buy/sell",
        log_prefix
    );
    let targets: Vec<(Pubkey, DexKind)> = candidates
        .iter()
        .map(|descriptor| (descriptor.address, descriptor.kind))
        .collect();
    let (success_pools, updated, errors) = if !targets.is_empty() {
        sample_pool_slippage(ctx, slippage_cache, targets, trade_size_quote, true).await
    } else {
        (HashSet::new(), 0, 0)
    };
    if errors > 0 {
        warn!("[PoolSlippage] Sampled {} pools, {} errors", updated, errors);
    } else if updated > 0 {
        info!("[PoolSlippage] Sampled {} pools", updated);
    }

    // ✅ Filter candidates: only keep pools that succeeded in slippage sampling
    let kept: Vec<PoolDescriptor> = candidates
        .into_iter()
        .filter(|d| success_pools.contains(&d.address))
        .collect();
    let kept_count = kept.len();

    (
        kept,
        PoolSimStats {
            total,
            ok: kept_count,
            failed: total - kept_count,
            skipped: 0,
            unpaired_dropped: 0,
            sampled_updated: updated,
            sampled_errors: errors,
            used_simulated: false,
        },
    )
}

async fn sample_pool_slippage(
    _ctx: &AppContext,
    slippage_cache: &SlippageCache,
    pools: Vec<(Pubkey, DexKind)>,
    trade_size_quote: f64,
    update_buy: bool,
) -> (HashSet<Pubkey>, usize, usize) {
    if trade_size_quote <= 0.0 || pools.is_empty() {
        return (HashSet::new(), 0, 0);
    }
    if !trade_size_quote.is_finite() {
        warn!("[PoolSlippage] Invalid trade_size_quote: {}", trade_size_quote);
        return (HashSet::new(), 0, pools.len());
    }

    use once_cell::sync::Lazy;
    static SIDECAR: Lazy<Result<SidecarClient, anyhow::Error>> =
        Lazy::new(|| SidecarClient::from_env());

    let sidecar = match SIDECAR.as_ref() {
        Ok(client) => client,
        Err(err) => {
            warn!(
                "[PoolSlippage] Sampling skipped: SidecarClient unavailable: {}",
                err
            );
            return (HashSet::new(), 0, pools.len());
        }
    };

    let mut success_pools: HashSet<Pubkey> = HashSet::new();
    let mut updated = 0usize;
    let mut errors = 0usize;
    
    let select_buy_value = |key: &Pubkey, update_buy: bool, quoted: f64| -> f64 {
        let existing = slippage_cache
            .get(key)
            .map(|v| v.buy_slippage_pct)
            .unwrap_or(f64::NAN);
        if !update_buy {
            return existing;
        }
        let quote_ok = quoted.is_finite() && quoted >= 0.0;
        let existing_ok = existing.is_finite() && existing >= 0.0;
        match (quote_ok, existing_ok) {
            (true, true) => quoted.max(existing),
            (true, false) => quoted,
            (false, true) => existing,
            _ => f64::NAN,
        }
    };

    for (key, dex_kind) in pools {
        let dex = match dex_kind {
            DexKind::OrcaWhirlpool => "orca_whirlpool",
            DexKind::RaydiumClmm => "raydium_clmm",
            DexKind::RaydiumCpmm => "raydium_cpmm",
            DexKind::RaydiumAmmV4 => "raydium_amm_v4",
            DexKind::MeteoraDlmm => "meteora_dlmm",
            DexKind::MeteoraDammV1 | DexKind::MeteoraDammV2 => "meteora_damm_v2",
            DexKind::PumpFunDlmm => "pump_fun_dlmm",
            _ => {
                debug!(
                    "[PoolSlippage] Skipped unsupported dex {:?} for {}",
                    dex_kind, key
                );
                errors += 1;
                continue;
            }
        };

        // ✅ Retry logic: 5 seconds delay between attempts
        let mut quote_result = None;
        for attempt in 1..=2 {
            match sidecar
                .quote_slippage(&key.to_string(), dex, trade_size_quote)
                .await
            {
                Ok(resp) => {
                    quote_result = Some(resp);
                    break;
                }
                Err(err) => {
                    if attempt == 1 {
                        debug!(
                            "[PoolSlippage] {} ({:?}) slippage query failed: {}, retrying in 5s",
                            key, dex_kind, err
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    } else {
                        warn!(
                            "[PoolSlippage] {} ({:?}) slippage query failed after retry: {}",
                            key, dex_kind, err
                        );
                        errors += 1;
                    }
                }
            }
        }

        let quote = match quote_result {
            Some(q) => q,
            None => continue,
        };

        // ✅ sell_ok is the critical check for success
        let sell_ok = quote.sell_slippage_pct.is_finite() && quote.sell_slippage_pct >= 0.0;
        
        if !sell_ok {
            // Sell slippage invalid - retry after 5s
            debug!(
                "[PoolSlippage] {} ({:?}) invalid sell slippage {:.4}%, retrying in 5s",
                key, dex_kind, quote.sell_slippage_pct
            );
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            
            // Retry quote
            match sidecar.quote_slippage(&key.to_string(), dex, trade_size_quote).await {
                Ok(retry_quote) => {
                    let retry_sell_ok = retry_quote.sell_slippage_pct.is_finite() && retry_quote.sell_slippage_pct >= 0.0;
                    if !retry_sell_ok {
                        warn!(
                            "[PoolSlippage] {} ({:?}) sell slippage failed after retry: {:.4}%",
                            key, dex_kind, retry_quote.sell_slippage_pct
                        );
                        errors += 1;
                        continue;
                    }
                    // Use retry result
                    let buy_value =
                        select_buy_value(&key, update_buy, retry_quote.buy_slippage_pct);
                    slippage_cache.insert(key, PoolSlippage {
                        buy_slippage_pct: buy_value,
                        sell_slippage_pct: retry_quote.sell_slippage_pct,
                    });
                    success_pools.insert(key);
                    updated += 1;
                }
                Err(err) => {
                    warn!(
                        "[PoolSlippage] {} ({:?}) sell slippage retry failed: {}",
                        key, dex_kind, err
                    );
                    errors += 1;
                    continue;
                }
            }
        } else {
            // Sell slippage is valid
            let buy_value = select_buy_value(&key, update_buy, quote.buy_slippage_pct);
            slippage_cache.insert(key, PoolSlippage {
                buy_slippage_pct: buy_value,
                sell_slippage_pct: quote.sell_slippage_pct,
            });
            success_pools.insert(key);
            updated += 1;
        }
    }

    (success_pools, updated, errors)
}

/// 批量预热 Orca/Raydium CLMM 池子的 tick_arrays
/// 
/// 与 prewarm_meteora_dlmm_batch 类似，用于动态发现的 CLMM 池子
/// ✅ 正确区分 Orca (tick_array_size=88) 和 Raydium (tick_array_size=60)
async fn prewarm_clmm_tick_arrays_batch(ctx: &AppContext, pools: &[(Pubkey, crate::dex::DexKind)]) {
    use crate::dex::{orca, raydium};
    
    if pools.is_empty() {
        return;
    }

    use once_cell::sync::Lazy;
    static SIDECAR: Lazy<Result<crate::sidecar_client::SidecarClient, anyhow::Error>> =
        Lazy::new(|| crate::sidecar_client::SidecarClient::from_env());
    
    let sidecar = match SIDECAR.as_ref() {
        Ok(client) => client,
        Err(_) => return,
    };
    
    for (pool_address, dex_kind) in pools {
        // ⚠️ 获取 program_id（使用超时防止卡住）
        let program_id = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            ctx.get_account_owner(pool_address)
        ).await {
            Ok(Ok(pid)) => pid,
            _ => continue,
        };
        
        let pool_str = pool_address.to_string();
        
        // ✅ 根据 DEX 类型调用对应的 sidecar introspect 和 compute 函数
        let (tick_current, tick_spacing) = match dex_kind {
            crate::dex::DexKind::OrcaWhirlpool => {
                // Orca: 使用 introspect_orca
                let resp = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sidecar.introspect_orca(&pool_str)
                ).await {
                    Ok(Ok(resp)) if resp.ok => resp,
                    _ => continue,
                };
                match (resp.tick_current_index, resp.tick_spacing) {
                    (Some(t), Some(s)) => (t, s),
                    _ => continue,
                }
            }
            crate::dex::DexKind::RaydiumClmm => {
                // Raydium: 使用 introspect_raydium
                let resp = match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    sidecar.introspect_raydium(&pool_str)
                ).await {
                    Ok(Ok(resp)) if resp.ok => resp,
                    _ => continue,
                };
                match (resp.tick_current_index, resp.tick_spacing) {
                    (Some(t), Some(s)) => (t, s),
                    _ => continue,
                }
            }
            _ => continue, // 其他类型跳过
        };
        
        // ✅ 根据 DEX 类型计算 tick_arrays 并注入对应的缓存
        match dex_kind {
            crate::dex::DexKind::OrcaWhirlpool => {
                // Orca: tick_array_size=88
                let tick_arrays = if ctx.spot_only_mode() {
                    orca::warmup_tick_arrays(
                        &ctx,
                        &program_id,
                        pool_address,
                        tick_current,
                        tick_spacing,
                    )
                    .await
                } else {
                    orca::compute_tick_array_subscriptions(
                        &program_id,
                        pool_address,
                        tick_current,
                        tick_spacing,
                    )
                };
                if !tick_arrays.is_empty() {
                    crate::trade::orca::inject_tick_arrays(*pool_address, tick_arrays.clone());
                    ctx.set_orca_tick_arrays(pool_address, tick_arrays.clone());
                }
            }
            crate::dex::DexKind::RaydiumClmm => {
                // Raydium: tick_array_size=60
                let tick_arrays = if ctx.spot_only_mode() {
                    raydium::warmup_tick_arrays(
                        &ctx,
                        &program_id,
                        pool_address,
                        tick_current,
                        tick_spacing,
                    )
                    .await
                } else {
                    raydium::compute_tick_array_subscriptions(
                        &program_id,
                        pool_address,
                        tick_current,
                        tick_spacing,
                    )
                };
                if !tick_arrays.is_empty() {
                    crate::trade::raydium::inject_tick_arrays(*pool_address, tick_arrays.clone());
                    // ✅ TODO: 如果 AppContext 需要单独的 Raydium cache，可以加
                }
            }
            _ => {}
        }
    }
}

/// 导出当前活跃池子到 filtered_pools.txt
async fn write_filtered_pools(
    pool_manager: &PoolManager,
    active_pools: &Arc<DashMap<Pubkey, PoolSubscriptionHandle>>,
) {
    let pools_map = pool_manager.get_pools().await;
    if pools_map.is_empty() {
        return;
    }

    let mut active_metas: Vec<crate::pool_manager::PoolMeta> = Vec::new();
    for entry in active_pools.iter() {
        if let Some(meta) = pools_map.get(entry.key()) {
            active_metas.push(meta.clone());
        }
    }

    if active_metas.is_empty() {
        return;
    }

    // 按币对分组
    let mut pair_groups: HashMap<(Pubkey, Pubkey), Vec<crate::pool_manager::PoolMeta>> = HashMap::new();
    for pool in active_metas {
        let (min_mint, max_mint) = if pool.base_mint <= pool.quote_mint {
            (pool.base_mint, pool.quote_mint)
        } else {
            (pool.quote_mint, pool.base_mint)
        };
        pair_groups.entry((min_mint, max_mint)).or_default().push(pool);
    }

    let mut grouped: Vec<_> = pair_groups
        .into_iter()
        .filter(|(_, pools)| pools.len() >= 2)
        .collect();
    grouped.sort_by_key(|((a, b), _)| (*a, *b));

    let mut buf = String::new();
    let mut total_pools = 0usize;
    let pair_count = grouped.len();

    let mut dex_counts: HashMap<DexKind, usize> = HashMap::new();
    for ((base, quote), mut pools) in grouped {
        pools.sort_by_key(|p| p.dex as u8);
        let pair_key = format!("{}-{}", base, quote);
        for pool in pools {
            let dex_str = format!("{}", pool.dex);
            let label = pool
                .label
                .clone()
                .unwrap_or_else(|| pool.key.to_string());
            *dex_counts.entry(pool.dex).or_insert(0) += 1;
            buf.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                pair_key,
                dex_str,
                label,
                pool.key,
            ));
            total_pools += 1;
        }
        buf.push('\n');
    }

    let ts = Local::now().to_rfc3339();
    let mut header = String::new();
    header.push_str(&format!("# Export time: {}\n", ts));
    header.push_str(&format!("# Total active pools: {}\n", total_pools));
    header.push_str(&format!("# Arbitrage pairs (>=2 pools): {}\n", pair_count));
    header.push_str("# Per-DEX counts:\n");

    let mut dex_entries: Vec<_> = dex_counts.into_iter().collect();
    dex_entries.sort_by_key(|(dex, _)| *dex as u8);
    for (dex, count) in dex_entries {
        header.push_str(&format!("#   {:?}: {}\n", dex, count));
    }
    header.push_str("#\n");

    let final_buf = format!("{}{}", header, buf);
    if let Err(err) = tokio::fs::write("filtered_pools.txt", final_buf).await {
        warn!(
            "[PoolManager] Failed to write filtered_pools.txt: {:?}",
            err
        );
    } else {
        info!(
            "[PoolManager] Wrote filtered_pools.txt (pools={}, pairs={})",
            total_pools, pair_count
        );
    }
}

/// 每次 refresh 后刷新 warmup 缓存
async fn refresh_runtime_caches(
    ctx: &AppContext,
    active_pools: &Arc<DashMap<Pubkey, PoolSubscriptionHandle>>,
    clear_cache: bool,
) {
    let pools = collect_descriptors_from_active_pools(active_pools);
    refresh_tick_bin_caches_for_pools(ctx, &pools, clear_cache).await;

    if let Err(err) = crate::trade::execution::refresh_jito_tip_account(ctx).await {
        warn!("[JITO] Failed to refresh tip account cache: {:?}", err);
    }
}

/// Run the PoolManager refresh loop, updating subscriptions on changes
async fn run_pool_manager_loop(
    pool_manager: Arc<PoolManager>,
    active_pools: Arc<DashMap<Pubkey, PoolSubscriptionHandle>>,
    ws_url: String,
    ctx: AppContext,
    store: SnapshotStore,
    thresholds: PriceThresholds,
    commitment: CommitmentConfig,
    tracker: OpportunityTracker,
    detector: Arc<OpportunityDetector>,
    subscription_manager: Arc<SubscriptionProvider>,
    quarantine: QuarantineTracker,
    dispatcher: Arc<TradeDispatcher>,
    strategy: Option<Arc<OpportunityFilter>>,
    atomic_executor: Option<Arc<AtomicExecutor>>,
    // ✅ 新增参数：用于 ATA 创建和 ALT 填充
    signer_keypair: Option<Arc<Keypair>>,
    account_manager: Arc<TradeAccountManager>,
    slippage_cache: SlippageCache,
    cache_tracker: PoolCacheTracker,
    _pool_slippage_bps: f64,
    pool_slippage_trade_size_quote: f64,
    _use_simulated_buy_slippage: bool,
    skip_initial_refresh: bool,
) {
    let refresh_interval = pool_manager.refresh_interval();
    info!(
        "[PoolManager] Starting refresh loop (interval: {:?})",
        refresh_interval
    );


    if !skip_initial_refresh {
        // Initial refresh with retry
        let mut retry_count = 0;
        const MAX_INITIAL_RETRIES: usize = 5;
        const RETRY_DELAY: Duration = Duration::from_secs(5);

        loop {
            match pool_manager.refresh().await {
                Ok(result) => {
                    info!(
                        "[PoolManager] Initial refresh: candidates={}, filtered={}, added={}, removed={}",
                        result.candidates, result.filtered, result.added, result.removed
                    );

                    // ✅ 全量获取所有池子，不依赖 added_keys
                    let all_pools = pool_manager.get_pools().await;
                    if all_pools.is_empty() {
                        warn!("[PoolManager] No pools from PoolManager after initial refresh");
                        break;
                    }

                    // ✅ 过滤掉已在 active_pools 中的池子，避免重复订阅
                    let mut added_meta: Vec<(Pubkey, crate::pool_manager::PoolMeta)> = all_pools
                        .into_iter()
                        .filter(|(key, _)| !active_pools.contains_key(key))
                        .collect();
                    
                    info!(
                        "[PoolManager] Initial refresh: {} total pools, {} new (not in active_pools)",
                        pool_manager.pool_count().await,
                        added_meta.len()
                    );

                    if added_meta.is_empty() {
                        info!("[PoolManager] All pools already subscribed, skipping");
                        break;
                    }

                    // ✅ Step 1: Warmup
                    {
                        let warmup_pools: Vec<PoolDescriptor> = added_meta
                            .iter()
                            .map(|(key, meta)| PoolDescriptor {
                                label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                                address: *key,
                                kind: meta.dex,
                            })
                            .collect();
                        let warmed = warmup_pools_with_retry(&ctx, warmup_pools, Duration::from_secs(30)).await;
                        let warmed_set: HashSet<Pubkey> = warmed.iter().map(|d| d.address).collect();
                        let before = added_meta.len();
                        added_meta.retain(|(key, _)| warmed_set.contains(key));
                        if added_meta.len() < before {
                            info!("[PoolManager] Warmup dropped {} pools", before - added_meta.len());
                        }
                    }

                    // ✅ Step 2: Pair filter
                    {
                        let before = added_meta.len();
                        let existing_pairs = collect_existing_pair_keys(&store);
                        added_meta = filter_dynamic_by_pair_key(added_meta, &existing_pairs);
                        if added_meta.len() < before {
                            info!("[PoolManager] Pair filter dropped {} pools", before - added_meta.len());
                        }
                    }

                    // ✅ Step 3: Slippage filter
                    if pool_slippage_trade_size_quote > 0.0 && !added_meta.is_empty() {
                        let descriptors: Vec<PoolDescriptor> = added_meta
                            .iter()
                            .map(|(key, meta)| PoolDescriptor {
                                label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                                address: *key,
                                kind: meta.dex,
                            })
                            .collect();
                        let (filtered, _stats) = apply_simulated_slippage_filter(
                            &ctx,
                            commitment,
                            account_manager.as_ref(),
                            atomic_executor.as_deref(),
                            &slippage_cache,
                            pool_slippage_trade_size_quote,
                            descriptors,
                            None,
                            "",
                        )
                        .await;
                        let keep: HashSet<Pubkey> = filtered.iter().map(|d| d.address).collect();
                        let before = added_meta.len();
                        added_meta.retain(|(key, _)| keep.contains(key));
                        if added_meta.len() < before {
                            info!("[PoolManager] Slippage filter dropped {} pools", before - added_meta.len());
                        }
                    }

                    // ✅ Step 4: ATA/ALT (if ALT_AUTO_EXTEND enabled)
                    let alt_auto_extend = std::env::var("ALT_AUTO_EXTEND")
                        .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                        .unwrap_or(false);
                    
                    if alt_auto_extend && !added_meta.is_empty() {
                        if let Some(ref keypair) = signer_keypair {
                            // Step 4a: Collect mints and create ATAs
                            let mut all_mints: HashSet<Pubkey> = HashSet::new();
                            for (_key, meta) in &added_meta {
                                all_mints.insert(meta.base_mint);
                                all_mints.insert(meta.quote_mint);
                            }
                            
                            let mut created_atas = 0;
                            for mint in &all_mints {
                                let token_program = match token_program_for_mint(&ctx, mint).await {
                                    Some(program) => program,
                                    None => continue,
                                };
                                let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                                    &keypair.pubkey(), mint, &token_program,
                                );
                                if ctx.rpc_client().get_account(&ata).await.is_err() {
                                    let create_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                                        &keypair.pubkey(), &keypair.pubkey(), mint, &token_program,
                                    );
                                    if let Ok(blockhash) = ctx.rpc_client().get_latest_blockhash().await {
                                        let tx = solana_transaction::Transaction::new_signed_with_payer(
                                            &[create_ix], Some(&keypair.pubkey()), &[&**keypair], blockhash,
                                        );
                                        if ctx.rpc_client().send_and_confirm_transaction(&tx).await.is_ok() {
                                            created_atas += 1;
                                        }
                                    }
                                }
                            }
                            if created_atas > 0 {
                                info!("[PoolManager] Initial refresh: created {} ATAs", created_atas);
                            }
                            
                            // Step 4b: Collect pool accounts and update ALT
                            let mut pool_accounts: Vec<(String, crate::DexKind, Vec<Pubkey>)> = Vec::new();
                            for (key, meta) in &added_meta {
                                if let Ok(account) = ctx.rpc_client().get_account(key).await {
                                    let descriptor = PoolDescriptor {
                                        label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                                        address: *key,
                                        kind: meta.dex,
                                    };
                                    let decode_ctx = if matches!(
                                        meta.dex,
                                        DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
                                    ) {
                                        ctx.clone_non_spot_only()
                                    } else {
                                        ctx.clone_for_alt_collection()
                                    };
                                    let context = RpcResponseContext { slot: 0, api_version: None };
                                    if let Ok(snapshot) = crate::dex::snapshot_from_account_data(&descriptor, account.data, context, &decode_ctx).await {
                                        let mut accounts = crate::dex::collect_static_accounts_for_pool(&snapshot);
                                        let user_accts = collect_user_accounts_for_pool(&ctx, &account_manager, &snapshot, Some(keypair.pubkey())).await;
                                        accounts.extend(user_accts);
                                        accounts.sort();
                                        accounts.dedup();
                                        pool_accounts.push((descriptor.label.clone(), descriptor.kind, accounts));
                                    }
                                }
                            }
                            
                            if !pool_accounts.is_empty() {
                                if let Ok(mut alt_manager) = crate::alt_manager::AltManager::from_env(
                                    ctx.rpc_client(), Arc::clone(keypair),
                                ).await {
                                    if let Ok(()) = alt_manager.ensure_pool_accounts(&pool_accounts).await {
                                        info!("[PoolManager] Initial refresh: ALT updated with {} pool accounts", pool_accounts.len());
                                        if let Some(ref exec) = atomic_executor {
                                            if let Ok(count) = exec.refresh_lookup_tables().await {
                                                info!("[PoolManager] Refreshed ALT cache ({} table(s))", count);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // ✅ Step 5: Subscribe
                    const POOL_SPAWN_INTERVAL_MS: u64 = 500;
                    let pool_count = added_meta.len();
                    info!("[PoolManager] Subscribing to {} pools after initial refresh", pool_count);
                    
                    for (i, (key, meta)) in added_meta.into_iter().enumerate() {
                        let descriptor = PoolDescriptor {
                            label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                            address: key,
                            kind: meta.dex,
                        };

                        let handle = spawn_pool_subscription(
                            descriptor.clone(),
                            ws_url.clone(),
                            ctx.clone(),
                            Arc::clone(&store),
                            thresholds.clone(),
                            commitment,
                            Arc::clone(&tracker),
                            Arc::clone(&detector),
                            Arc::clone(&subscription_manager),
                            Arc::clone(&quarantine),
                            Arc::clone(&dispatcher),
                            strategy.clone(),
                            Arc::clone(&slippage_cache),
                            Some(Arc::clone(&pool_manager)),
                            Arc::clone(&cache_tracker),
                        );

                        active_pools.insert(key, PoolSubscriptionHandle { descriptor, handle });

                        if i + 1 < pool_count {
                            tokio::time::sleep(Duration::from_millis(POOL_SPAWN_INTERVAL_MS)).await;
                        }
                    }



                refresh_runtime_caches(&ctx, &active_pools, true).await;
                write_filtered_pools(&pool_manager, &active_pools).await;

                    break;
                }
                Err(err) => {
                    retry_count += 1;
                    if retry_count >= MAX_INITIAL_RETRIES {
                        error!(
                            "[PoolManager] Initial refresh failed after {} retries: {:?}; static pools remain active, dynamic discovery disabled",
                            retry_count, err
                        );
                        break;
                    }
                    warn!(
                        "[PoolManager] Initial refresh failed (retry {}/{}): {:?}",
                        retry_count, MAX_INITIAL_RETRIES, err
                    );
                    tokio::time::sleep(RETRY_DELAY).await;
                }
            }
        }
    } else {
        info!("[PoolManager] Initial refresh already completed; skipping pre-subscription refresh");
    }

    // Periodic refresh loop
    let mut interval = tokio::time::interval(refresh_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    interval.tick().await;

    loop {
        interval.tick().await;

        match pool_manager.refresh().await {
            Ok(result) => {
                info!(
                    "[PoolManager] Periodic refresh: candidates={}, filtered={}, +{} -{}",
                    result.candidates, result.filtered, result.added, result.removed
                );

                // ✅ 2h 全量刷新：取消所有现有订阅，对所有池子重新处理
                info!("[PoolManager] Full refresh: cancelling all {} existing subscriptions", active_pools.len());
                for entry in active_pools.iter() {
                    entry.value().handle.abort();
                }
                active_pools.clear();
                slippage_cache.clear();

                // ✅ 获取 PoolManager 中的所有池子（已经过 PoolManager 筛选）
                let all_pools = pool_manager.get_pools().await;
                if all_pools.is_empty() {
                    warn!("[PoolManager] No pools after refresh, waiting for next interval");
                    continue;
                }

                let mut all_meta: Vec<(Pubkey, crate::pool_manager::PoolMeta)> = 
                    all_pools.into_iter().collect();
                info!("[PoolManager] Processing {} pools from refresh", all_meta.len());

                // ✅ Step 1: Warmup all pools
                {
                    let warmup_pools: Vec<PoolDescriptor> = all_meta
                        .iter()
                        .map(|(key, meta)| PoolDescriptor {
                            label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                            address: *key,
                            kind: meta.dex,
                        })
                        .collect();
                    let warmed = warmup_pools_with_retry(&ctx, warmup_pools, Duration::from_secs(30)).await;
                    let warmed_set: HashSet<Pubkey> = warmed.iter().map(|d| d.address).collect();
                    let before = all_meta.len();
                    all_meta.retain(|(key, _)| warmed_set.contains(key));
                    if all_meta.len() < before {
                        info!("[PoolManager] Warmup dropped {} pools", before - all_meta.len());
                    }
                }

                // ✅ Step 2: Pair filter (drop single-pool pairs)
                {
                    let before = all_meta.len();
                    all_meta = filter_dynamic_by_pair_key(all_meta, &HashSet::new());
                    if all_meta.len() < before {
                        info!("[PoolManager] Pair filter dropped {} pools", before - all_meta.len());
                    }
                }

                // ✅ Step 3: Slippage filter
                if pool_slippage_trade_size_quote > 0.0 && !all_meta.is_empty() {
                    let descriptors: Vec<PoolDescriptor> = all_meta
                        .iter()
                        .map(|(key, meta)| PoolDescriptor {
                            label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                            address: *key,
                            kind: meta.dex,
                        })
                        .collect();
                    let (filtered, _stats) = apply_simulated_slippage_filter(
                        &ctx,
                        commitment,
                        account_manager.as_ref(),
                        atomic_executor.as_deref(),
                        &slippage_cache,
                        pool_slippage_trade_size_quote,
                        descriptors,
                        None,
                        " refresh",
                    )
                    .await;
                    let keep: HashSet<Pubkey> = filtered.iter().map(|d| d.address).collect();
                    let before = all_meta.len();
                    all_meta.retain(|(key, _)| keep.contains(key));
                    if all_meta.len() < before {
                        info!("[PoolManager] Slippage filter dropped {} pools", before - all_meta.len());
                    }
                }

                if all_meta.is_empty() {
                    warn!("[PoolManager] No pools after filtering, waiting for next interval");
                    continue;
                }

                // ✅ Step 4: ATA/ALT (same logic as initial refresh)
                let alt_auto_extend = std::env::var("ALT_AUTO_EXTEND")
                    .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
                    .unwrap_or(false);
                
                if alt_auto_extend {
                    if let Some(ref keypair) = signer_keypair {
                        // Step 4a: Collect mints and create ATAs
                        let mut new_mints: HashSet<Pubkey> = HashSet::new();
                        for (_key, meta) in &all_meta {
                            new_mints.insert(meta.base_mint);
                            new_mints.insert(meta.quote_mint);
                        }
                        
                        let mut created_atas = 0;
                        for mint in &new_mints {
                            let token_program = match token_program_for_mint(&ctx, mint).await {
                                Some(program) => program,
                                None => continue,
                            };
                            let ata = spl_associated_token_account::get_associated_token_address_with_program_id(
                                &keypair.pubkey(), mint, &token_program,
                            );
                            if ctx.rpc_client().get_account(&ata).await.is_err() {
                                let create_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                                    &keypair.pubkey(), &keypair.pubkey(), mint, &token_program,
                                );
                                if let Ok(blockhash) = ctx.rpc_client().get_latest_blockhash().await {
                                    let tx = solana_transaction::Transaction::new_signed_with_payer(
                                        &[create_ix], Some(&keypair.pubkey()), &[&**keypair], blockhash,
                                    );
                                    if ctx.rpc_client().send_and_confirm_transaction(&tx).await.is_ok() {
                                        created_atas += 1;
                                    }
                                }
                            }
                        }
                        if created_atas > 0 {
                            info!("[PoolManager] Periodic refresh: created {} ATAs", created_atas);
                        }
                        
                        // Step 4b: Collect pool accounts and update ALT
                        let mut pool_accounts: Vec<(String, crate::DexKind, Vec<Pubkey>)> = Vec::new();
                        for (key, meta) in &all_meta {
                            if let Ok(account) = ctx.rpc_client().get_account(key).await {
                                let descriptor = PoolDescriptor {
                                    label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                                    address: *key,
                                    kind: meta.dex,
                                };
                                let decode_ctx = if matches!(
                                    meta.dex,
                                    DexKind::PumpFunDlmm | DexKind::RaydiumAmmV4 | DexKind::RaydiumCpmm
                                ) {
                                    ctx.clone_non_spot_only()
                                } else {
                                    ctx.clone_for_alt_collection()
                                };
                                let context = RpcResponseContext { slot: 0, api_version: None };
                                if let Ok(snapshot) = crate::dex::snapshot_from_account_data(&descriptor, account.data, context, &decode_ctx).await {
                                    let mut accounts = crate::dex::collect_static_accounts_for_pool(&snapshot);
                                    let user_accts = collect_user_accounts_for_pool(&ctx, &account_manager, &snapshot, Some(keypair.pubkey())).await;
                                    accounts.extend(user_accts);
                                    accounts.sort();
                                    accounts.dedup();
                                    pool_accounts.push((descriptor.label.clone(), descriptor.kind, accounts));
                                }
                            }
                        }
                        
                        if !pool_accounts.is_empty() {
                            if let Ok(mut alt_manager) = crate::alt_manager::AltManager::from_env(
                                ctx.rpc_client(), Arc::clone(keypair),
                            ).await {
                                if let Ok(()) = alt_manager.ensure_pool_accounts(&pool_accounts).await {
                                    info!("[PoolManager] Periodic refresh: ALT updated with {} pool accounts", pool_accounts.len());
                                    if let Some(ref exec) = atomic_executor {
                                        if let Ok(count) = exec.refresh_lookup_tables().await {
                                            info!("[PoolManager] Refreshed ALT cache ({} table(s))", count);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // ✅ Step 5: Subscribe to all pools
                const POOL_SPAWN_INTERVAL_MS: u64 = 500;
                let pool_count = all_meta.len();
                info!("[PoolManager] Subscribing to {} pools after full refresh", pool_count);
                
                for (i, (key, meta)) in all_meta.into_iter().enumerate() {
                    let descriptor = PoolDescriptor {
                        label: meta.label.clone().unwrap_or_else(|| key.to_string()),
                        address: key,
                        kind: meta.dex,
                    };

                    let handle = spawn_pool_subscription(
                        descriptor.clone(),
                        ws_url.clone(),
                        ctx.clone(),
                        Arc::clone(&store),
                        thresholds.clone(),
                        commitment,
                        Arc::clone(&tracker),
                        Arc::clone(&detector),
                        Arc::clone(&subscription_manager),
                        Arc::clone(&quarantine),
                        Arc::clone(&dispatcher),
                        strategy.clone(),
                        Arc::clone(&slippage_cache),
                        Some(Arc::clone(&pool_manager)),
                        Arc::clone(&cache_tracker),
                    );

                    active_pools.insert(key, PoolSubscriptionHandle { descriptor, handle });

                    if i + 1 < pool_count {
                        tokio::time::sleep(Duration::from_millis(POOL_SPAWN_INTERVAL_MS)).await;
                    }
                }

                refresh_runtime_caches(&ctx, &active_pools, true).await;
                write_filtered_pools(&pool_manager, &active_pools).await;
                info!("[PoolManager] Full refresh complete: {} active pools", active_pools.len());
            }
            Err(err) => {
                warn!(
                    "[PoolManager] Refresh failed (keeping {} pools): {:?}",
                    active_pools.len(),
                    err
                );
                // Keep existing subscriptions on failure
            }
        }
    }
}
