use std::{
    env,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::{anyhow, Result};
use dashmap::{DashMap, mapref::entry::Entry};

use log::{debug, info, warn};
use serde::Deserialize;
use serde_json::json;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use tokio::{
    sync::{mpsc, Semaphore},
    time::{sleep, timeout, Duration},
};

use crate::{
    dex::{NormalizedPair, Opportunity, PoolSnapshot, TradeSide},
    trade::arbitrage::AtomicTradePlan,
    rpc::AppContext,
    log_utils::append_trade_log,
};

use super::{
    accounts::TradeAccountManager,
    arbitrage,
    execution::{AtomicExecutor, SubmissionSummary},
    strategy::OpportunityFilter,
    TradeAccounts,
};

use spl_associated_token_account::get_associated_token_address;
use crate::dex::SOL_MINT;

const DEFAULT_QUEUE_CAPACITY: usize = 128;
const DEFAULT_MAX_CONCURRENT: usize = 1;
const DEFAULT_STATUS_POLL_MS: u64 = 1000;
const JOB_RECORD_TTL_SECONDS: u64 = 300; // 5分钟后清理已完成/失败的 job 记录
const DEFAULT_PAIR_COOLDOWN_SECS: u64 = 5; // 同一套利对两次交易之间的最小间隔
const POOL_SLOT_COOLDOWN_SECONDS: u64 = 120; // 2分钟冷却
const BUNDLE_STATUS_TIMEOUT_SECS: u64 = 5;

/// 异步调度器：接收套利机会，排队构建并提交交易，带并发控制。
pub struct TradeDispatcher {
    account_manager: Arc<TradeAccountManager>,
    executor: Option<Arc<AtomicExecutor>>,
    strategy: Option<Arc<OpportunityFilter>>,
    semaphore: Arc<Semaphore>,
    job_counter: AtomicU64,
    queue_tx: mpsc::Sender<DispatchJob>,
    statuses: Arc<DashMap<u64, JobRecord>>,
    ctx: AppContext,
    status_poll_interval: Duration,
    dry_run_only: bool,
    disable_bundle_status_rpc: bool,
    immediate_mode: bool,
    force_immediate: bool,
    ignore_guards: bool,
    /// 每个套利对上次发送交易的时间，用于冷却控制
    pair_last_trade: Arc<DashMap<NormalizedPair, Instant>>,
    pool_slot_last_trade: Arc<DashMap<(Pubkey, u64), Instant>>,
    /// 同一套利对两次交易之间的最小间隔
    pair_cooldown: Duration,
    /// 同一套利对的 in-flight 去重（防止重复入队）
    pair_in_flight: Arc<DashMap<NormalizedPair, Instant>>,
}

impl TradeDispatcher {
    pub fn from_env(
        account_manager: Arc<TradeAccountManager>,
        executor: Option<Arc<AtomicExecutor>>,
        strategy: Option<Arc<OpportunityFilter>>,
        ctx: AppContext,
    ) -> Arc<Self> {
        let cfg = DispatcherConfig::from_env();
        Self::with_config(account_manager, executor, strategy, ctx, cfg)
    }

    fn with_config(
        account_manager: Arc<TradeAccountManager>,
        executor: Option<Arc<AtomicExecutor>>,
        strategy: Option<Arc<OpportunityFilter>>,
        ctx: AppContext,
        cfg: DispatcherConfig,
    ) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(cfg.queue_capacity);
        let pair_cooldown_secs = env::var("PAIR_COOLDOWN_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_PAIR_COOLDOWN_SECS);

        let dispatcher = Arc::new(Self {
            account_manager,
            executor,
            strategy,
            semaphore: Arc::new(Semaphore::new(cfg.max_concurrent)),
            job_counter: AtomicU64::new(0),
            queue_tx: tx,
            statuses: Arc::new(DashMap::new()),
            ctx,
            status_poll_interval: cfg.status_poll_interval,
            dry_run_only: cfg.dry_run_only,
            disable_bundle_status_rpc: cfg.disable_bundle_status_rpc,
            immediate_mode: cfg.immediate_mode,
            force_immediate: cfg.force_immediate,
            ignore_guards: cfg.ignore_guards,
            pair_last_trade: Arc::new(DashMap::new()),
            pool_slot_last_trade: Arc::new(DashMap::new()),
            pair_cooldown: Duration::from_secs(pair_cooldown_secs),
            pair_in_flight: Arc::new(DashMap::new()),
        });
        dispatcher.spawn_worker(rx);
        dispatcher.spawn_status_poller();
        dispatcher
    }

    fn spawn_worker(self: &Arc<Self>, mut rx: mpsc::Receiver<DispatchJob>) {
        let dispatcher = Arc::clone(self);
        tokio::spawn(async move {
            while let Some(job) = rx.recv().await {
                // 先获取信号量，再处理 job，避免无限制创建 task
                let permit = match dispatcher.semaphore.clone().acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => {
                        dispatcher.fail_job(job.id, "semaphore closed".to_string());
                        continue;
                    }
                };

                let dispatcher = Arc::clone(&dispatcher);
                tokio::spawn(async move {
                    dispatcher.process_job_with_permit(job, permit).await;
                });
            }
        });
    }

    async fn process_job_with_permit(
        self: Arc<Self>,
        job: DispatchJob,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        self.process_job(job).await;
        drop(permit);
    }

    async fn process_job(self: Arc<Self>, job: DispatchJob) {
        self.update_status(job.id, JobStatus::Running, job.attempts, None);

        // 通知策略：job 开始
        let buy_pool = job.opportunity.buy.descriptor.address;
        let sell_pool = job.opportunity.sell.descriptor.address;
        if let Some(strategy) = &self.strategy {
            strategy.on_job_started(&buy_pool, &sell_pool);
        }

        let result = self.execute_job(&job).await;

        match result {
            Ok(Some(summary)) => {
                self.record_submission(job.id, &summary, job.attempts + 1);
                // ✅ 交易已提交，更新 cooldown
                self.update_cooldown(&job.opportunity);
            }
            Ok(None) => {
                self.update_status(job.id, JobStatus::Confirmed, job.attempts + 1, None);
                self.release_in_flight(job.id);
                // ✅ dry-run 成功，更新 cooldown
                self.update_cooldown(&job.opportunity);
                // 通知策略：job 成功（仅当没有提交 bundle 时，表示 dry-run 成功）
                if let Some(strategy) = &self.strategy {
                    strategy.on_job_succeeded(&buy_pool, &sell_pool);
                }
            }
            Err(err) => {
                let err_msg = format!("{err:?}");
                // ✅ 滑点错误不重试（价格已经变了，重试没意义）
                let is_slippage_error = err_msg.contains("Custom(6003)")  // Meteora DLMM: ExceededAmountSlippageTolerance
                    || err_msg.contains("Custom(6004)")  // Pump.fun: ExceededSlippage
                    || err_msg.contains("Custom(30)");   // Raydium AMM V4: exceeds desired slippage limit
                
                if is_slippage_error {
                    debug!("Slippage error detected, skipping retry: {}", err_msg);
                } else {
                    debug!("Non-slippage error detected, skipping retry: {}", err_msg);
                }
                self.fail_job(job.id, err_msg); // fail_job 会自动通知策略
            }
        }
    }

    fn spawn_status_poller(self: &Arc<Self>) {
        let dispatcher = Arc::clone(self);
        tokio::spawn(async move {
            let mut cleanup_counter = 0u64;
            loop {
                sleep(dispatcher.status_poll_interval).await;
                if let Err(err) = dispatcher.check_pending_signatures().await {
                    warn!("dispatcher signature poll failed: {err:?}");
                }

                // 每 30 次轮询清理一次过期记录（约每 30 秒）
                cleanup_counter += 1;
                if cleanup_counter >= 30 {
                    dispatcher.cleanup_stale_records();
                    cleanup_counter = 0;
                }
            }
        });
    }

    fn cleanup_stale_records(&self) {
        let now = Instant::now();
        let ttl = Duration::from_secs(JOB_RECORD_TTL_SECONDS);
        let mut cleaned = 0;

        self.statuses.retain(|_job_id, record| {
            let is_terminal = matches!(record.status, JobStatus::Confirmed | JobStatus::Failed);
            let elapsed = now.duration_since(record.last_updated);
            let should_keep = !is_terminal || elapsed < ttl;
            if !should_keep {
                cleaned += 1;
            }
            should_keep
        });

        if cleaned > 0 {
            debug!("Cleaned up {} stale job records", cleaned);
        }

        let pool_slot_ttl = Duration::from_secs(POOL_SLOT_COOLDOWN_SECONDS);
        self.pool_slot_last_trade
            .retain(|_k, last_time| now.duration_since(*last_time) < pool_slot_ttl);
    }

    /// 更新 cooldown（在交易成功/提交后调用）
    fn update_cooldown(&self, opportunity: &Opportunity) {
        if self.ignore_guards {
            return;
        }
        let now = Instant::now();
        let pair = opportunity.pair;
        let buy_pool = opportunity.buy.descriptor.address;
        let sell_pool = opportunity.sell.descriptor.address;
        let buy_slot = opportunity.buy.slot;
        let sell_slot = opportunity.sell.slot;

        // 更新 pool_slot cooldown
        self.pool_slot_last_trade.insert((buy_pool, buy_slot), now);
        if (sell_pool, sell_slot) != (buy_pool, buy_slot) {
            self.pool_slot_last_trade.insert((sell_pool, sell_slot), now);
        }

        // 更新 pair cooldown
        self.pair_last_trade.insert(pair, now);

        debug!(
            "Updated cooldown for pair {} (buy_pool={} sell_pool={})",
            pair.base, buy_pool, sell_pool
        );
    }

    /// 将机会推入队列，返回 job_id
    /// 如果同一套利对在冷却期内，返回错误
    pub async fn enqueue(self: &Arc<Self>, opportunity: Opportunity, max_slippage_bps: f64) -> Result<u64> {
        let pair = opportunity.pair;
        let now = Instant::now();

        let buy_pool = opportunity.buy.descriptor.address;
        let sell_pool = opportunity.sell.descriptor.address;
        let buy_slot = opportunity.buy.slot;
        let sell_slot = opportunity.sell.slot;

        if !self.ignore_guards {
            // ✅ in-flight 去重：同一套利对已有任务在执行/确认中则拒绝
            match self.pair_in_flight.entry(pair) {
                Entry::Occupied(_) => {
                    return Err(anyhow!("pair {} already in flight", pair.base));
                }
                Entry::Vacant(entry) => {
                    entry.insert(now);
                }
            }
        }

        // 检查同一套利对的冷却时间
        if !self.ignore_guards {
            if let Some(last_trade) = self.pair_last_trade.get(&pair) {
                let elapsed = now.duration_since(*last_trade);
                if elapsed < self.pair_cooldown {
                    let remaining = self.pair_cooldown - elapsed;
                    debug!(
                        "Pair {} in cooldown, {:.1}s remaining",
                        pair.base,
                        remaining.as_secs_f64()
                    );
                    self.pair_in_flight.remove(&pair);
                    return Err(anyhow!(
                        "pair {} in cooldown ({:.1}s remaining)",
                        pair.base,
                        remaining.as_secs_f64()
                    ));
                }
            }
        }

        let pool_slot_cooldown = Duration::from_secs(POOL_SLOT_COOLDOWN_SECONDS);

        // ✅ 只检查 pool_slot cooldown，不更新（成功后才更新）
        if !self.ignore_guards {
            let buy_key = (buy_pool, buy_slot);
            if let Some(last_time) = self.pool_slot_last_trade.get(&buy_key) {
                let elapsed = now.duration_since(*last_time);
                if elapsed < pool_slot_cooldown {
                    self.pair_in_flight.remove(&pair);
                    return Err(anyhow!(
                        "buy pool {} slot {} already traded {:.1}s ago",
                        buy_pool,
                        buy_slot,
                        elapsed.as_secs_f64()
                    ));
                }
            }

            let sell_key = (sell_pool, sell_slot);
            if sell_key != buy_key {
                if let Some(last_time) = self.pool_slot_last_trade.get(&sell_key) {
                    let elapsed = now.duration_since(*last_time);
                    if elapsed < pool_slot_cooldown {
                        self.pair_in_flight.remove(&pair);
                        return Err(anyhow!(
                            "sell pool {} slot {} already traded {:.1}s ago",
                            sell_pool,
                            sell_slot,
                            elapsed.as_secs_f64()
                        ));
                    }
                }
            }
        }

        // ✅ 只检查 pair cooldown，不更新（成功后才更新）
        // pair_last_trade 已在上面检查过

        let job_id = self.job_counter.fetch_add(1, Ordering::Relaxed) + 1;

        self.statuses.insert(
            job_id,
            JobRecord::pending(Instant::now(), buy_pool, sell_pool, pair),
        );

        let job = DispatchJob {
            id: job_id,
            opportunity: opportunity.clone(),
            max_slippage_bps,
            attempts: 0,
        };

        if self.force_immediate {
            let dispatcher = Arc::clone(self);
            tokio::spawn(async move {
                dispatcher.process_job(job).await;
            });
            debug!(
                "Force immediate dispatch for job {} (pair {} -> {})",
                job_id, opportunity.pair.base, opportunity.pair.quote
            );
            return Ok(job_id);
        }

        if self.immediate_mode {
            if let Ok(permit) = self.semaphore.clone().try_acquire_owned() {
                let dispatcher = Arc::clone(self);
                tokio::spawn(async move {
                    dispatcher.process_job_with_permit(job, permit).await;
                });
                debug!(
                    "Immediate dispatch for job {} (pair {} -> {})",
                    job_id, opportunity.pair.base, opportunity.pair.quote
                );
                return Ok(job_id);
            }
        }

        debug!(
            "Enqueued job {} for pair {} -> {}",
            job_id, opportunity.pair.base, opportunity.pair.quote
        );

        self.queue_tx
            .send(job)
            .await
            .map_err(|_| {
                self.pair_in_flight.remove(&pair);
                anyhow!("dispatcher queue closed")
            })?;

        Ok(job_id)
    }

    /// 查询 job 状态（调试用途）
    pub fn job_status(&self, job_id: u64) -> Option<JobStatusSnapshot> {
        self.statuses.get(&job_id).map(|record| JobStatusSnapshot {
            id: job_id,
            status: record.status,
            attempts: record.attempts,
            last_error: record.last_error.clone(),
            signatures: record.signatures.clone(),
            relay: record.relay.clone(),
            last_updated: record.last_updated,
        })
    }

    async fn execute_job(&self, job: &DispatchJob) -> Result<Option<SubmissionSummary>> {
        let job_start = Instant::now();
        let resolve_start = Instant::now();
        let (buy_accounts, sell_accounts) = self.resolve_accounts(&job.opportunity);
        let resolve_ms = resolve_start.elapsed();

        let build_start = Instant::now();
        let plan_result = arbitrage::build_atomic_trade_plan(
            &job.opportunity,
            job.max_slippage_bps,
            buy_accounts.clone(),
            sell_accounts.clone(),
        );
        let build_ms = build_start.elapsed();
        let mut plan = match plan_result {
            Ok(p) => p,
            Err(e) => {
                let err_msg = format!("{e:?}");
                if err_msg.contains("tick_arrays_cache_miss")
                    || err_msg.contains("missing cached tick_arrays")
                {
                    let ctx = self.ctx.clone();
                    let buy_pool = job.opportunity.buy.descriptor.address;
                    let sell_pool = job.opportunity.sell.descriptor.address;
                    let buy_kind = job.opportunity.buy.descriptor.kind;
                    let sell_kind = job.opportunity.sell.descriptor.kind;
                    tokio::spawn(async move {
                        match buy_kind {
                            crate::DexKind::OrcaWhirlpool => {
                                let _ = crate::trade::orca::refresh_pool_cached_tick_arrays(&ctx, &buy_pool).await;
                            }
                            crate::DexKind::RaydiumClmm => {
                                let _ = crate::trade::raydium::refresh_pool_cached_tick_arrays(&ctx, &buy_pool).await;
                            }
                            _ => {}
                        }
                        if sell_pool != buy_pool {
                            match sell_kind {
                                crate::DexKind::OrcaWhirlpool => {
                                    let _ = crate::trade::orca::refresh_pool_cached_tick_arrays(&ctx, &sell_pool).await;
                                }
                                crate::DexKind::RaydiumClmm => {
                                    let _ = crate::trade::raydium::refresh_pool_cached_tick_arrays(&ctx, &sell_pool).await;
                                }
                                _ => {}
                            }
                        }
                    });
                }
                // ✅ 构建交易失败时，报出具体池子信息
                warn!(
                    "❌ Trade build FAILED: buy_pool={} {} ({}) | sell_pool={} {} ({}) | error={:?}",
                    job.opportunity.buy.descriptor.kind,
                    job.opportunity.buy.descriptor.label,
                    job.opportunity.buy.descriptor.address,
                    job.opportunity.sell.descriptor.kind,
                    job.opportunity.sell.descriptor.label,
                    job.opportunity.sell.descriptor.address,
                    e
                );
                let total_ms = job_start.elapsed();
                append_trade_log(&format!(
                    "TRADE_TIMING trade_id={} status=build_failed resolve_ms={} build_ms={} total_ms={}",
                    job.id,
                    resolve_ms.as_millis(),
                    build_ms.as_millis(),
                    total_ms.as_millis()
                ));
                return Err(e);
            }
        };

        // 计算买入需要的 SOL 数量（lamports）
        // Full mode: 从 trade.buy_quote_cost 获取
        // Spot-only mode: 从 ARBITRAGE_TRADE_SIZE_QUOTE 获取
        let sol_amount_needed = if job.opportunity.buy.quote_mint == *SOL_MINT {
            job.opportunity
                .trade
                .as_ref()
                .map(|t| (t.buy_quote_cost * 1.05 * 1_000_000_000.0) as u64) // +5% buffer
                .unwrap_or_else(|| {
                    // Spot-only mode: 使用环境变量
                    std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok())
                        .map(|q| (q * 1.05 * 1_000_000_000.0) as u64) // +5% buffer
                        .unwrap_or(0)
                })
        } else {
            0
        };

        // ✅ 查询 wSOL ATA 余额（仅使用缓存，零网络调用）
        // wSOL ATA 应在 warmup 阶段预创建，这里只检查余额决定 wrap 多少
        let skip_wsol_wrap = std::env::var("SKIP_WSOL_WRAP")
            .map(|v| v.eq_ignore_ascii_case("true") || v == "1")
            .unwrap_or(false);
        
        let sol_amount_to_wrap = if job.opportunity.buy.quote_mint == *SOL_MINT && sol_amount_needed > 0 {
            let wsol_ata = get_associated_token_address(&buy_accounts.payer, &SOL_MINT);
            
            // 只使用缓存，不做 RPC 调用
            match self.ctx.get_token_account_amount_from_cache(&wsol_ata) {
                Some(cached) if cached >= sol_amount_needed => {
                    debug!("wSOL balance {} >= needed {}, skipping wrap", cached, sol_amount_needed);
                    0
                }
                Some(cached) => {
                    if skip_wsol_wrap {
                        // ✅ SKIP_WSOL_WRAP=true: 假设用户已预先 fund wSOL，不做任何 wrap
                        warn!("SKIP_WSOL_WRAP=true but wSOL balance {} < needed {}! Trade may fail.", cached, sol_amount_needed);
                        0
                    } else {
                        let diff = sol_amount_needed - cached;
                        debug!("wSOL balance {} < needed {}, will wrap {} lamports", cached, sol_amount_needed, diff);
                        diff
                    }
                }
                None => {
                    if skip_wsol_wrap {
                        // ✅ SKIP_WSOL_WRAP=true: 假设 wSOL 已预先 fund
                        debug!("SKIP_WSOL_WRAP=true, assuming wSOL is pre-funded");
                        0
                    } else {
                        // 缓存缺失，假设余额为 0，做全量 wrap
                        debug!("wSOL balance not in cache, assuming 0, will wrap {} lamports", sol_amount_needed);
                        sol_amount_needed
                    }
                }
            }
        } else {
            0
        };

        // ✅ 当 SKIP_WSOL_WRAP=true 时，完全跳过 ATA 创建指令（假设 wSOL ATA 已存在）
        let need_create_wsol_ata = if skip_wsol_wrap {
            false
        } else {
            job.opportunity.buy.quote_mint == *SOL_MINT
        };

        // ✅ ATA 创建在 warmup 阶段完成，交易构建时不做网络调用
        // wSOL ATA 如果需要创建，在 build_ata_and_wrap_instructions 中使用 idempotent 指令

        // 构建 SOL wrap 指令（如果需要则先创建 wSOL ATA）
        let wrap_start = Instant::now();
        let (pre_instructions, post_instructions) = self.build_ata_and_wrap_instructions(
            &job.opportunity,
            &buy_accounts,
            &sell_accounts,
            sol_amount_to_wrap,
            need_create_wsol_ata,
        );
        let wrap_ms = wrap_start.elapsed();

        // 组合指令顺序：pre (wSOL ATA创建 + SOL wrap) -> swap -> post
        if !pre_instructions.is_empty() || !post_instructions.is_empty() {
            debug!(
                "Auto setup: {} pre-instructions, {} post-instructions",
                pre_instructions.len(),
                post_instructions.len()
            );
            let mut combined = pre_instructions;
            combined.extend(plan.instructions);
            combined.extend(post_instructions);
            plan.instructions = combined;
        }

        // Write BUILD log to trade log with trade_id (简化版，详细结果在 SIM 块)
        let base_amount = if let Some(trade) = job.opportunity.trade.as_ref() {
            trade.base_amount
        } else {
            let quote_in = std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
                .ok()
                .and_then(|s| s.parse::<f64>().ok())
                .unwrap_or(0.0);
            let buy_price = job.opportunity.buy.normalized_price;
            if quote_in > 0.0 && buy_price.is_finite() && buy_price > 0.0 {
                let expected_base = quote_in / buy_price;
                expected_base * (1.0 - job.max_slippage_bps / 10_000.0)
            } else {
                0.0
            }
        };
        let build_log = format!(
            "[TRADE#{}] 🔨 BUILD: {} -> {} | {} {:.6} | mode={}",
            job.id,
            job.opportunity.buy.descriptor.label,
            job.opportunity.sell.descriptor.label,
            job.opportunity.pair.base,
            base_amount,
            if self.dry_run_only { "simulate" } else { "live" },
        );
        append_trade_log(&build_log);
        self.log_trade_detail(job, &plan, &buy_accounts, &sell_accounts);
        info!("Prepared atomic trade plan ({} instructions): {}", plan.instructions.len(), plan.description);
        debug!("Buy leg: {}", plan.buy_plan.description);
        debug!("Sell leg: {}", plan.sell_plan.description);

        let mut sim_ms: Option<Duration> = None;
        let mut submit_ms: Option<Duration> = None;
        let result = match &self.executor {
            Some(executor) => {
                if self.dry_run_only {
                    // Dry-run 模式：在链上 simulate 整笔原子套利交易，不提交 bundle
                    let sim_start = Instant::now();
                    executor
                        .simulate_atomic_plan(&plan, &buy_accounts, &sell_accounts, job.id)
                        .await?;
                    sim_ms = Some(sim_start.elapsed());
                    Ok(None)
                } else {
                    // 正常模式：构建 bundle 并通过 Jito 或日志输出提交
                    let submit_start = Instant::now();
                    match executor.submit_plan(&plan).await {
                        Ok(summary) => {
                            submit_ms = Some(submit_start.elapsed());
                            Ok(Some(summary))
                        }
                        Err(err) => {
                            submit_ms = Some(submit_start.elapsed());
                            let err_msg = format!("{err:?}");
                            let submit_log = format!(
                                "\n========== TRADE #{} ❌ ==========\n\
                                 STATUS: ❌ SUBMIT_FAILED\n\
                                 ERROR: {}\n\
                                 =====================================",
                                job.id,
                                err_msg,
                            );
                            append_trade_log(&submit_log);
                            Err(err)
                        }
                    }
                }
            }
            None => {
                warn!("Atomic executor not configured; plan logged only");
                Ok(None)
            }
        };

        let total_ms = job_start.elapsed();
        let fmt_opt_ms = |value: Option<Duration>| -> String {
            value
                .map(|v| v.as_millis().to_string())
                .unwrap_or_else(|| "N/A".to_string())
        };
        append_trade_log(&format!(
            "TRADE_TIMING trade_id={} status={} resolve_ms={} build_ms={} wrap_ms={} sim_ms={} submit_ms={} total_ms={}",
            job.id,
            if result.is_ok() { "ok" } else { "error" },
            resolve_ms.as_millis(),
            build_ms.as_millis(),
            wrap_ms.as_millis(),
            fmt_opt_ms(sim_ms),
            fmt_opt_ms(submit_ms),
            total_ms.as_millis()
        ));

        result
    }

    fn resolve_accounts(&self, opportunity: &Opportunity) -> (TradeAccounts, TradeAccounts) {
        let buy_raw = self
            .account_manager
            .resolve_for(&opportunity.buy, TradeSide::Buy);
        let sell_raw = self
            .account_manager
            .resolve_for(&opportunity.sell, TradeSide::Sell);

        let buy_accounts = self.fill_atas_for_leg(&opportunity.buy, buy_raw);
        let sell_accounts = self.fill_atas_for_leg(&opportunity.sell, sell_raw);
        (buy_accounts, sell_accounts)
    }

    /// 构建 SOL wrap 指令
    /// 如果 need_create_wsol_ata 为 true，先添加 idempotent 创建指令
    /// 返回 (前置指令, 后置指令)
    fn build_ata_and_wrap_instructions(
        &self,
        opportunity: &Opportunity,
        buy_accounts: &TradeAccounts,
        _sell_accounts: &TradeAccounts,
        sol_amount_lamports: u64,
        need_create_wsol_ata: bool,
    ) -> (Vec<solana_instruction::Instruction>, Vec<solana_instruction::Instruction>) {
        let mut pre_instructions = Vec::new();
        let post_instructions = Vec::new();

        let owner = buy_accounts.payer;
        if owner == Pubkey::default() {
            return (pre_instructions, post_instructions);
        }

        if opportunity.buy.quote_mint != *SOL_MINT {
            return (pre_instructions, post_instructions);
        }

        let wsol_ata = get_associated_token_address(&owner, &SOL_MINT);

        // ✅ 如果需要创建 wSOL ATA，使用 idempotent 创建指令
        if need_create_wsol_ata {
            let create_ix = spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                &owner,
                &owner,
                &SOL_MINT,
                &spl_token::ID,
            );
            pre_instructions.push(create_ix);
            debug!("Added idempotent wSOL ATA creation: {}", wsol_ata);
        }

        // ✅ 如果需要 wrap SOL
        if sol_amount_lamports > 0 {
            // 1. 转 SOL 到 wSOL ATA（system transfer）
            let transfer_ix = solana_system_interface::instruction::transfer(
                &owner,
                &wsol_ata,
                sol_amount_lamports,
            );
            pre_instructions.push(transfer_ix);

            // 2. Sync native（把转入的 SOL 同步为 wSOL 余额）
            let sync_ix = spl_token::instruction::sync_native(&spl_token::ID, &wsol_ata)
                .expect("sync_native instruction");
            pre_instructions.push(sync_ix);

            debug!(
                "Added SOL wrap: {} lamports -> wSOL ATA {}",
                sol_amount_lamports, wsol_ata
            );
        }

        (pre_instructions, post_instructions)
    }

    fn fill_atas_for_leg(&self, snapshot: &PoolSnapshot, mut accounts: TradeAccounts) -> TradeAccounts {
        let owner = if accounts.payer != Pubkey::default() {
            accounts.payer
        } else if let Some(executor) = &self.executor {
            executor.signer_pubkey()
        } else {
            return accounts;
        };

        if accounts.payer == Pubkey::default() {
            accounts.payer = owner;
        }

        // ✅ 使用 normalized mint 而不是原始 token0/token1
        // 当 inverted=true 时，normalized base 是 snapshot.quote_mint，normalized quote 是 snapshot.base_mint
        let (norm_base_mint, norm_quote_mint) = if snapshot.normalized_pair.inverted {
            (snapshot.quote_mint, snapshot.base_mint)
        } else {
            (snapshot.base_mint, snapshot.quote_mint)
        };

        // ✅ 使用缓存获取 token program，支持 Token-2022
        if accounts.user_base_account == Pubkey::default() {
            let token_program = self.ctx
                .get_mint_info_from_cache(&norm_base_mint)
                .map(|info| info.owner)
                .unwrap_or(spl_token::ID);
            accounts.user_base_account = spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner,
                &norm_base_mint,
                &token_program,
            );
        }
        if accounts.user_quote_account == Pubkey::default() {
            let token_program = self.ctx
                .get_mint_info_from_cache(&norm_quote_mint)
                .map(|info| info.owner)
                .unwrap_or(spl_token::ID);
            accounts.user_quote_account = spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner,
                &norm_quote_mint,
                &token_program,
            );
        }

        accounts
    }

    fn update_status(&self, job_id: u64, status: JobStatus, attempts: usize, error: Option<&str>) {
        if let Some(mut record) = self.statuses.get_mut(&job_id) {
            record.status = status;
            record.attempts = attempts;
            record.last_error = error.map(|s| s.to_string());
            record.last_updated = Instant::now();
        }
    }

    fn release_in_flight(&self, job_id: u64) {
        if let Some(record) = self.statuses.get(&job_id) {
            let pair = record.pair;
            self.pair_in_flight.remove(&pair);
        }
    }

    fn record_submission(&self, job_id: u64, summary: &SubmissionSummary, attempts: usize) {
        if let Some(mut record) = self.statuses.get_mut(&job_id) {
            record.status = JobStatus::Submitted;
            record.attempts = attempts;
            record.last_error = None;
            record.signatures = summary.signatures.clone();
            // ✅ 保留全量 bundle_base64 用于调试排查
            record.bundle_base64 = summary.bundle_base64.clone();
            record.relay = summary.relay.clone();
            record.bundle_id = summary.bundle_id.clone();
            record.last_updated = Instant::now();

            // Write SUBMIT log to trade log
            let submit_log = format!(
                "SUBMIT trade_id={} attempt={} sig_count={} relay={} bundle_id={} sigs={}",
                job_id,
                attempts,
                summary.signatures.len(),
                summary.relay.as_deref().unwrap_or("none"),
                summary.bundle_id.as_deref().unwrap_or("none"),
                summary.signatures.first().cloned().unwrap_or_default(),
            );
            append_trade_log(&submit_log);
            let sigs_json = serde_json::to_string(&summary.signatures)
                .unwrap_or_else(|_| format!("{:?}", summary.signatures));
            append_trade_log(&format!(
                "BUNDLE_SIGS trade_id={} sigs={}",
                job_id, sigs_json
            ));
            let base64_json = serde_json::to_string(&summary.bundle_base64)
                .unwrap_or_else(|_| format!("{:?}", summary.bundle_base64));
            append_trade_log(&format!(
                "BUNDLE_BASE64 trade_id={} txs={}",
                job_id, base64_json
            ));
            info!("Job {} submitted (attempt {}): {} signature(s)", job_id, attempts, summary.signatures.len());
        }
    }

    fn mark_confirmed(&self, job_id: u64) {
        if let Some(record) = self.statuses.get(&job_id) {
            let attempts = record.attempts;
            let buy_pool = record.buy_pool;
            let sell_pool = record.sell_pool;
            let sigs = record.signatures.clone();
            drop(record); // 释放读锁

            self.update_status(job_id, JobStatus::Confirmed, attempts, None);
            self.release_in_flight(job_id);
            
            // Write CONFIRM log to trade log
            let confirm_log = format!(
                "CONFIRM trade_id={} attempt={} sig={}",
                job_id,
                attempts,
                sigs.first().cloned().unwrap_or_default(),
            );
            append_trade_log(&confirm_log);
            info!("Job {} confirmed after {} attempt(s)", job_id, attempts);

            // 通知策略：job 成功确认
            if let Some(strategy) = &self.strategy {
                strategy.on_job_succeeded(&buy_pool, &sell_pool);
            }
        }
    }

    async fn check_pending_signatures(&self) -> Result<()> {
        let now = Instant::now();
        // ✅ 超时时间：如果签名 60 秒内没有上链，认为 bundle 失败
        let submission_timeout = Duration::from_secs(60);

        let pending: Vec<(u64, Vec<String>, Instant)> = self
            .statuses
            .iter()
            .filter(|entry| entry.status == JobStatus::Submitted)
            .map(|entry| (entry.key().clone(), entry.signatures.clone(), entry.last_updated))
            .collect();

        for (job_id, sig_strings, submitted_at) in pending {
            if sig_strings.is_empty() {
                continue;
            }
            let mut parsed = Vec::with_capacity(sig_strings.len());
            for sig_str in &sig_strings {
                match Signature::from_str(sig_str) {
                    Ok(sig) => parsed.push(sig),
                    Err(err) => {
                        warn!(
                            "invalid signature '{}' for job {}: {}",
                            sig_str, job_id, err
                        );
                    }
                }
            }
            if parsed.is_empty() {
                continue;
            }

            let response = match self.ctx.rpc_client().get_signature_statuses(&parsed).await {
                Ok(resp) => resp,
                Err(err) => {
                    warn!("get_signature_statuses failed for job {}: {err:?}", job_id);
                    continue;
                }
            };

            let mut all_confirmed = true;
            let mut failure: Option<String> = None;
            for status_opt in response.value.iter() {
                match status_opt {
                    Some(status) => {
                        if let Some(err) = status.err.as_ref() {
                            failure = Some(format!("{err:?}"));
                            break;
                        }
                        // ✅ 检查确认级别：只有 Confirmed 或 Finalized 才算确认
                        // 部分 RPC 节点 confirmation_status 可能为空但 confirmations > 0
                        use solana_transaction_status::TransactionConfirmationStatus;
                        let is_confirmed = status
                            .confirmation_status
                            .as_ref()
                            .map(|s| matches!(
                                s,
                                TransactionConfirmationStatus::Confirmed
                                    | TransactionConfirmationStatus::Finalized
                            ))
                            .unwrap_or_else(|| {
                                // ✅ Fallback: 如果 confirmation_status 为空，检查 confirmations 字段
                                status.confirmations.map(|c| c > 0).unwrap_or(false)
                            });
                        if !is_confirmed {
                            all_confirmed = false;
                        }
                    }
                    None => {
                        all_confirmed = false;
                    }
                }
            }

            if let Some(err_msg) = failure {
                self.fail_job(job_id, format!("signature error: {err_msg}"));
                self.log_bundle_status(job_id).await;
            } else if all_confirmed {
                self.mark_confirmed(job_id);
            } else if now.duration_since(submitted_at) > submission_timeout {
                // ✅ 超时：任何签名未确认超过 60 秒，认为 bundle 失败
                self.fail_job(job_id, format!(
                    "bundle timeout: not all signatures confirmed after {:.0}s",
                    now.duration_since(submitted_at).as_secs_f64()
                ));
                self.log_bundle_status(job_id).await;
            }
        }

        Ok(())
    }

    fn log_trade_detail(
        &self,
        job: &DispatchJob,
        plan: &AtomicTradePlan,
        buy_accounts: &TradeAccounts,
        sell_accounts: &TradeAccounts,
    ) {
        let quote_mint = job.opportunity.pair.quote;
        let trade_size_quote = std::env::var("ARBITRAGE_TRADE_SIZE_QUOTE")
            .ok()
            .and_then(|raw| raw.parse::<f64>().ok())
            .filter(|value| value.is_finite() && *value > 0.0)
            .unwrap_or(0.0);

        let buy_price = job.opportunity.buy.normalized_price;
        let max_slippage_bps = job.max_slippage_bps;
        let min_base_out = if trade_size_quote > 0.0 && buy_price.is_finite() && buy_price > 0.0 {
            let expected_base = trade_size_quote / buy_price;
            expected_base * (1.0 - max_slippage_bps / 10_000.0)
        } else {
            0.0
        };
        let (_norm_base_decimals, norm_quote_decimals) = if job.opportunity.buy.normalized_pair.inverted {
            (job.opportunity.buy.quote_decimals, job.opportunity.buy.base_decimals)
        } else {
            (job.opportunity.buy.base_decimals, job.opportunity.buy.quote_decimals)
        };
        let min_quote_unit = 10f64.powi(-(norm_quote_decimals as i32));

        let base_fee_lamports = std::env::var("BASE_TX_FEE_LAMPORTS")
            .ok()
            .and_then(|raw| raw.parse::<u64>().ok())
            .unwrap_or(5_000);
        let base_fee_quote = base_fee_lamports as f64 / 1_000_000_000.0;
        let tip_quote = plan.tip_lamports as f64 / 1_000_000_000.0;
        let sell_min_out = if self.dry_run_only {
            min_quote_unit
        } else if trade_size_quote > 0.0 {
            (trade_size_quote + tip_quote + base_fee_quote).max(min_quote_unit)
        } else {
            0.0
        };

        let programs: Vec<String> = plan
            .instructions
            .iter()
            .map(|ix| ix.program_id.to_string())
            .collect();

        let detail = json!({
            "trade_id": job.id,
            "mode": if self.dry_run_only { "simulate" } else { "live" },
            "pair": {
                "base": job.opportunity.pair.base.to_string(),
                "quote": quote_mint.to_string(),
            },
            "buy": {
                "label": job.opportunity.buy.descriptor.label,
                "kind": format!("{:?}", job.opportunity.buy.descriptor.kind),
                "address": job.opportunity.buy.descriptor.address.to_string(),
                "slot": job.opportunity.buy.slot,
                "price": job.opportunity.buy.normalized_price,
            },
            "sell": {
                "label": job.opportunity.sell.descriptor.label,
                "kind": format!("{:?}", job.opportunity.sell.descriptor.kind),
                "address": job.opportunity.sell.descriptor.address.to_string(),
                "slot": job.opportunity.sell.slot,
                "price": job.opportunity.sell.normalized_price,
            },
            "spread_pct": job.opportunity.spread_pct,
            "trade_size_quote": trade_size_quote,
            "max_slippage_bps": max_slippage_bps,
            "min_base_out": min_base_out,
            "sell_min_out": sell_min_out,
            "tip_lamports": plan.tip_lamports,
            "base_fee_lamports": base_fee_lamports,
            "instruction_count": plan.instructions.len(),
            "programs": programs,
            "buy_plan": plan.buy_plan.description,
            "sell_plan": plan.sell_plan.description,
            "accounts": {
                "buy": {
                    "payer": buy_accounts.payer.to_string(),
                    "user_base": buy_accounts.user_base_account.to_string(),
                    "user_quote": buy_accounts.user_quote_account.to_string(),
                },
                "sell": {
                    "payer": sell_accounts.payer.to_string(),
                    "user_base": sell_accounts.user_base_account.to_string(),
                    "user_quote": sell_accounts.user_quote_account.to_string(),
                },
            },
        });

        append_trade_log(&format!("TRADE_DETAIL {}", detail));
    }
    fn fail_job(&self, job_id: u64, error: String) {
        let (attempts, buy_pool, sell_pool) = if let Some(record) = self.statuses.get(&job_id) {
            (record.attempts, record.buy_pool, record.sell_pool)
        } else {
            (
                0,
                solana_pubkey::Pubkey::default(),
                solana_pubkey::Pubkey::default(),
            )
        };

        self.update_status(job_id, JobStatus::Failed, attempts, Some(&error));
        self.release_in_flight(job_id);
        
        // Write FAIL log to trade log (skip duplicate SIM_FAILED in dry-run mode)
        let is_sim_failed = error.contains("Simulation failed with program error");
        if !(self.dry_run_only && is_sim_failed) {
            let fail_log = format!(
                "\n========== TRADE #{} ❌ ==========\n\
                 STATUS: ❌ FAILED (attempt={})\n\
                 ERROR: {}\n\
                 =====================================",
                job_id,
                attempts,
                error,
            );
            append_trade_log(&fail_log);
        }
        warn!("Job {job_id} failed: {error}");

        // 通知策略：job 失败
        if let Some(strategy) = &self.strategy {
            strategy.on_job_failed(&buy_pool, &sell_pool);
        }
    }

    async fn log_bundle_status(&self, job_id: u64) {
        if self.disable_bundle_status_rpc {
            return;
        }

        let (bundle_id, relay) = if let Some(record) = self.statuses.get(&job_id) {
            (record.bundle_id.clone(), record.relay.clone())
        } else {
            append_trade_log(&format!(
                "BUNDLE_STATUS trade_id={} result=skipped reason=missing_job",
                job_id
            ));
            return;
        };

        let bundle_id = match bundle_id {
            Some(id) if !id.trim().is_empty() => id,
            _ => {
                append_trade_log(&format!(
                    "BUNDLE_STATUS trade_id={} result=skipped reason=missing_bundle_id",
                    job_id
                ));
                return;
            }
        };

        let relay = relay
            .or_else(|| env::var("JITO_RELAY_URL").ok())
            .unwrap_or_default();
        if relay.trim().is_empty() {
            append_trade_log(&format!(
                "BUNDLE_STATUS trade_id={} result=skipped reason=missing_relay",
                job_id
            ));
            return;
        }

        let url = jito_api_url(&relay, "/api/v1/getBundleStatuses");
        let client = match reqwest::Client::builder().build() {
            Ok(client) => client,
            Err(err) => {
                append_trade_log(&format!(
                    "BUNDLE_STATUS trade_id={} result=client_error err={:?}",
                    job_id, err
                ));
                return;
            }
        };

        let payload = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [[bundle_id]],
        });

        let response = match timeout(
            Duration::from_secs(BUNDLE_STATUS_TIMEOUT_SECS),
            client.post(&url).json(&payload).send(),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(err)) => {
                append_trade_log(&format!(
                    "BUNDLE_STATUS trade_id={} result=http_error url={} err={:?}",
                    job_id, url, err
                ));
                return;
            }
            Err(_) => {
                append_trade_log(&format!(
                    "BUNDLE_STATUS trade_id={} result=timeout url={} timeout={}s",
                    job_id, url, BUNDLE_STATUS_TIMEOUT_SECS
                ));
                return;
            }
        };

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            append_trade_log(&format!(
                "BUNDLE_STATUS trade_id={} result=http_status url={} status={} body={}",
                job_id, url, status, body
            ));
            return;
        }

        let parsed: JitoBundleStatusResponse = match serde_json::from_str(&body) {
            Ok(parsed) => parsed,
            Err(err) => {
                append_trade_log(&format!(
                    "BUNDLE_STATUS trade_id={} result=parse_error err={} raw={}",
                    job_id, err, body
                ));
                return;
            }
        };

        if let Some(err) = parsed.error {
            append_trade_log(&format!(
                "BUNDLE_STATUS trade_id={} result=rpc_error code={} message={}",
                job_id, err.code, err.message
            ));
        }

        let result = match parsed.result {
            Some(result) => result,
            None => {
                append_trade_log(&format!(
                    "BUNDLE_STATUS trade_id={} result=null",
                    job_id
                ));
                return;
            }
        };

        if result.value.is_empty() {
            append_trade_log(&format!(
                "BUNDLE_STATUS trade_id={} result=empty context_slot={}",
                job_id, result.context.slot
            ));
            return;
        }

        for entry in result.value {
            let confirmation = entry
                .confirmation_status
                .unwrap_or_else(|| "unknown".to_string());
            let err_text = entry
                .err
                .as_ref()
                .map(|value| value.to_string())
                .unwrap_or_else(|| "null".to_string());
            let txs = serde_json::to_string(&entry.transactions)
                .unwrap_or_else(|_| format!("{:?}", entry.transactions));

            append_trade_log(&format!(
                "BUNDLE_STATUS trade_id={} bundle_id={} context_slot={} slot={} confirmation={} tx_count={} err={} txs={}",
                job_id,
                entry.bundle_id,
                result.context.slot,
                entry.slot,
                confirmation,
                entry.transactions.len(),
                err_text,
                txs
            ));
        }
    }
}

fn jito_api_url(endpoint: &str, path: &str) -> String {
    let trimmed = endpoint.trim_end_matches('/');
    if let Some(idx) = trimmed.find("/api/v1") {
        let base = &trimmed[..idx];
        return format!("{}{}", base, path);
    }
    format!("{}{}", trimmed, path)
}

#[derive(Deserialize)]
struct JitoBundleStatusResponse {
    #[serde(default)]
    result: Option<JitoBundleStatusResult>,
    #[serde(default)]
    error: Option<JitoRpcError>,
}

#[derive(Deserialize)]
struct JitoBundleStatusResult {
    context: JitoBundleStatusContext,
    value: Vec<JitoBundleStatusEntry>,
}

#[derive(Deserialize)]
struct JitoBundleStatusContext {
    slot: u64,
}

#[derive(Deserialize)]
struct JitoBundleStatusEntry {
    bundle_id: String,
    transactions: Vec<String>,
    slot: u64,
    #[serde(rename = "confirmation_status", alias = "confirmationStatus")]
    confirmation_status: Option<String>,
    err: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct JitoRpcError {
    code: i64,
    message: String,
}

struct DispatcherConfig {
    max_concurrent: usize,
    queue_capacity: usize,
    status_poll_interval: Duration,
    dry_run_only: bool,
    disable_bundle_status_rpc: bool,
    immediate_mode: bool,
    force_immediate: bool,
    ignore_guards: bool,
}

impl DispatcherConfig {
    fn from_env() -> Self {
        let max_concurrent = env::var("DISPATCHER_MAX_CONCURRENT")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_MAX_CONCURRENT);

        let queue_capacity = env::var("DISPATCHER_QUEUE_CAPACITY")
            .ok()
            .and_then(|raw| raw.trim().parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_QUEUE_CAPACITY);

        let status_poll_ms = env::var("DISPATCHER_STATUS_POLL_MS")
            .ok()
            .and_then(|raw| raw.trim().parse::<u64>().ok())
            .unwrap_or(DEFAULT_STATUS_POLL_MS)
            .max(100);

        let dry_run_only = env::var("ARBITRAGE_DRY_RUN_ONLY")
            .ok()
            .map(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let disable_bundle_status_rpc = env::var("DISABLE_JITO_BUNDLE_STATUS_RPC")
            .ok()
            .map(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let immediate_mode = env::var("DISPATCHER_IMMEDIATE")
            .ok()
            .map(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let force_immediate = env::var("DISPATCHER_FORCE_IMMEDIATE")
            .ok()
            .map(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let ignore_guards = env::var("DISPATCHER_IGNORE_GUARDS")
            .ok()
            .map(|raw| {
                matches!(
                    raw.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false);

        let config = Self {
            max_concurrent,
            queue_capacity,
            status_poll_interval: Duration::from_millis(status_poll_ms),
            dry_run_only,
            disable_bundle_status_rpc,
            immediate_mode,
            force_immediate,
            ignore_guards,
        };

        info!(
            "TradeDispatcher config: max_concurrent={}, queue_capacity={}, poll_interval={:?}, dry_run_only={}, disable_bundle_status_rpc={}, immediate_mode={}, force_immediate={}, ignore_guards={}",
            config.max_concurrent,
            config.queue_capacity,
            config.status_poll_interval,
            config.dry_run_only,
            config.disable_bundle_status_rpc,
            config.immediate_mode,
            config.force_immediate,
            config.ignore_guards
        );

        config
    }
}

struct DispatchJob {
    id: u64,
    opportunity: Opportunity,
    max_slippage_bps: f64,
    attempts: usize,
}

#[derive(Clone, Debug)]
struct JobRecord {
    status: JobStatus,
    attempts: usize,
    last_error: Option<String>,
    signatures: Vec<String>,
    bundle_base64: Vec<String>,
    relay: Option<String>,
    bundle_id: Option<String>,
    last_updated: Instant,
    buy_pool: solana_pubkey::Pubkey,
    sell_pool: solana_pubkey::Pubkey,
    pair: NormalizedPair,
}

impl JobRecord {
    fn pending(
        now: Instant,
        buy_pool: solana_pubkey::Pubkey,
        sell_pool: solana_pubkey::Pubkey,
        pair: NormalizedPair,
    ) -> Self {
        Self {
            status: JobStatus::Pending,
            attempts: 0,
            last_error: None,
            signatures: Vec::new(),
            bundle_base64: Vec::new(),
            relay: None,
            bundle_id: None,
            last_updated: now,
            buy_pool,
            sell_pool,
            pair,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobStatus {
    Pending,
    Running,
    Retrying,
    Submitted,
    Confirmed,
    Failed,
}

#[derive(Clone, Debug)]
pub struct JobStatusSnapshot {
    pub id: u64,
    pub status: JobStatus,
    pub attempts: usize,
    pub last_error: Option<String>,
    pub signatures: Vec<String>,
    pub relay: Option<String>,
    pub last_updated: Instant,
}

