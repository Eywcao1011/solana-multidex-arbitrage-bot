use std::{
    collections::HashSet,
    env,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use dashmap::DashMap;
use log::{debug, info, warn};
use solana_pubkey::Pubkey;

use crate::dex::Opportunity;

/// 机会筛选策略配置
#[derive(Clone, Debug)]
pub struct StrategyConfig {
    /// 最小净利润（扣除 tip 后），单位：quote token
    pub min_net_quote_profit: f64,
    /// 最小价差（百分比），可选，默认使用全局 alert_bps
    pub min_spread_pct: Option<f64>,
    /// 池子白名单（如果非空，只允许白名单中的池子）
    pub pool_whitelist: HashSet<Pubkey>,
    /// 池子黑名单（拒绝这些池子）
    pub pool_blacklist: HashSet<Pubkey>,
    /// 冷却时间：池子失败后多久可以重试（秒）
    pub cooldown_duration_secs: u64,
    /// 触发冷却的连续失败次数
    pub cooldown_failure_threshold: usize,
    /// 每个池子的最大活跃 job 数
    pub max_active_jobs_per_pool: usize,
    /// Jito tip 金额（用于计算净利润），单位：lamports
    pub tip_lamports: u64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            min_net_quote_profit: 0.0,
            min_spread_pct: None,
            pool_whitelist: HashSet::new(),
            pool_blacklist: HashSet::new(),
            cooldown_duration_secs: 120, // 2 分钟
            cooldown_failure_threshold: 3,
            max_active_jobs_per_pool: 1,
            tip_lamports: 0,
        }
    }
}

impl StrategyConfig {
    pub fn from_env() -> Result<Self> {
        let min_net_quote_profit = env::var("STRATEGY_MIN_NET_QUOTE")
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok())
            .unwrap_or(0.0);

        let min_spread_pct = env::var("STRATEGY_MIN_SPREAD_PCT")
            .ok()
            .and_then(|s| s.trim().parse::<f64>().ok());

        let pool_whitelist =
            parse_pubkey_list(&env::var("STRATEGY_POOL_WHITELIST").unwrap_or_default())?;
        let pool_blacklist =
            parse_pubkey_list(&env::var("STRATEGY_POOL_BLACKLIST").unwrap_or_default())?;

        let cooldown_duration_secs = env::var("STRATEGY_COOLDOWN_DURATION_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(120);

        let cooldown_failure_threshold = env::var("STRATEGY_COOLDOWN_FAILURE_THRESHOLD")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(3);

        let max_active_jobs_per_pool = env::var("STRATEGY_MAX_ACTIVE_JOBS_PER_POOL")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(1);

        let tip_lamports = env::var("JITO_TIP_LAMPORTS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0);

        let config = Self {
            min_net_quote_profit,
            min_spread_pct,
            pool_whitelist,
            pool_blacklist,
            cooldown_duration_secs,
            cooldown_failure_threshold,
            max_active_jobs_per_pool,
            tip_lamports,
        };

        info!(
            "OpportunityFilter config: min_net_quote={:.6}, min_spread_pct={:?}, whitelist={}, blacklist={}, cooldown={}s (threshold={}), max_jobs_per_pool={}, tip={}",
            config.min_net_quote_profit,
            config.min_spread_pct,
            config.pool_whitelist.len(),
            config.pool_blacklist.len(),
            config.cooldown_duration_secs,
            config.cooldown_failure_threshold,
            config.max_active_jobs_per_pool,
            config.tip_lamports
        );

        Ok(config)
    }
}

fn parse_pubkey_list(raw: &str) -> Result<HashSet<Pubkey>> {
    let mut set = HashSet::new();
    for part in raw.split(|c| matches!(c, ',' | ';' | '\n')) {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        let pubkey = Pubkey::from_str(trimmed)
            .with_context(|| format!("invalid pubkey in list: {}", trimmed))?;
        set.insert(pubkey);
    }
    Ok(set)
}

/// 池子状态跟踪
#[derive(Clone, Debug)]
struct PoolState {
    /// 连续失败次数
    consecutive_failures: usize,
    /// 最后失败时间
    last_failure_time: Option<Instant>,
    /// 当前活跃的 job 数量
    active_jobs: usize,
    /// 冷却期日志是否已打印
    cooldown_logged: bool,
}

impl Default for PoolState {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            last_failure_time: None,
            active_jobs: 0,
            cooldown_logged: false,
        }
    }
}

impl PoolState {
    fn is_in_cooldown(&self, threshold: usize, cooldown_duration: Duration) -> bool {
        if self.consecutive_failures < threshold {
            return false;
        }
        if let Some(last_failure) = self.last_failure_time {
            last_failure.elapsed() < cooldown_duration
        } else {
            false
        }
    }

    fn cooldown_remaining(&self, cooldown_duration: Duration) -> Option<Duration> {
        if let Some(last_failure) = self.last_failure_time {
            let elapsed = last_failure.elapsed();
            if elapsed < cooldown_duration {
                return Some(cooldown_duration - elapsed);
            }
        }
        None
    }
}

/// 筛选结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterDecision {
    Accept,
    Reject(RejectReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectReason {
    MissingTradeEstimates,
    InsufficientNetProfit {
        expected: String,
        actual: String,
    },
    InsufficientSpread {
        expected: String,
        actual: String,
    },
    PoolNotWhitelisted {
        pool: String,
    },
    PoolBlacklisted {
        pool: String,
    },
    PoolInCooldown {
        pool: String,
        remaining_secs: u64,
        log_once: bool,
    },
    TooManyActiveJobs {
        pool: String,
        current: usize,
        max: usize,
    },
}

impl std::fmt::Display for RejectReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RejectReason::MissingTradeEstimates => write!(f, "missing trade estimates"),
            RejectReason::InsufficientNetProfit { expected, actual } => {
                write!(
                    f,
                    "insufficient net profit (expected ≥ {}, got {})",
                    expected, actual
                )
            }
            RejectReason::InsufficientSpread { expected, actual } => {
                write!(
                    f,
                    "insufficient spread (expected ≥ {}%, got {}%)",
                    expected, actual
                )
            }
            RejectReason::PoolNotWhitelisted { pool } => {
                write!(f, "pool {} not in whitelist", pool)
            }
            RejectReason::PoolBlacklisted { pool } => {
                write!(f, "pool {} is blacklisted", pool)
            }
            RejectReason::PoolInCooldown {
                pool,
                remaining_secs,
                log_once: _,
            } => {
                write!(
                    f,
                    "pool {} in cooldown ({}s remaining)",
                    pool, remaining_secs
                )
            }
            RejectReason::TooManyActiveJobs { pool, current, max } => {
                write!(
                    f,
                    "pool {} has too many active jobs ({}/{})",
                    pool, current, max
                )
            }
        }
    }
}

/// 机会筛选器
pub struct OpportunityFilter {
    config: StrategyConfig,
    pool_states: Arc<DashMap<Pubkey, PoolState>>,
}

impl OpportunityFilter {
    pub fn new(config: StrategyConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            pool_states: Arc::new(DashMap::new()),
        })
    }

    pub fn from_env() -> Result<Arc<Self>> {
        let config = StrategyConfig::from_env()?;
        Ok(Self::new(config))
    }

    /// 评估机会是否应该执行
    pub fn evaluate(&self, opportunity: &Opportunity) -> FilterDecision {
        // 1. 检查净利润（扣除 tip），仅在有 trade estimates 时执行
        if let Some(trade) = opportunity.trade.as_ref() {
            let tip_quote_cost = lamports_to_quote(self.config.tip_lamports, trade);
            let net_profit = trade.net_quote_profit - tip_quote_cost;
            if net_profit < self.config.min_net_quote_profit {
                return FilterDecision::Reject(RejectReason::InsufficientNetProfit {
                    expected: format!("{:.6}", self.config.min_net_quote_profit),
                    actual: format!("{:.6}", net_profit),
                });
            }
        } else if self.config.min_net_quote_profit > 0.0 {
            return FilterDecision::Reject(RejectReason::MissingTradeEstimates);
        }

        // 2. 检查价差（可选）
        if let Some(min_spread_pct) = self.config.min_spread_pct {
            let spread_pct = opportunity.spread_pct;
            if spread_pct < min_spread_pct {
                return FilterDecision::Reject(RejectReason::InsufficientSpread {
                    expected: format!("{:.2}", min_spread_pct),
                    actual: format!("{:.2}", spread_pct),
                });
            }
        }

        // 3. 检查白名单（如果配置了）
        if !self.config.pool_whitelist.is_empty() {
            let buy_in_whitelist = self
                .config
                .pool_whitelist
                .contains(&opportunity.buy.descriptor.address);
            let sell_in_whitelist = self
                .config
                .pool_whitelist
                .contains(&opportunity.sell.descriptor.address);
            if !buy_in_whitelist {
                return FilterDecision::Reject(RejectReason::PoolNotWhitelisted {
                    pool: opportunity.buy.descriptor.address.to_string(),
                });
            }
            if !sell_in_whitelist {
                return FilterDecision::Reject(RejectReason::PoolNotWhitelisted {
                    pool: opportunity.sell.descriptor.address.to_string(),
                });
            }
        }

        // 5. 检查黑名单
        if self
            .config
            .pool_blacklist
            .contains(&opportunity.buy.descriptor.address)
        {
            return FilterDecision::Reject(RejectReason::PoolBlacklisted {
                pool: opportunity.buy.descriptor.address.to_string(),
            });
        }
        if self
            .config
            .pool_blacklist
            .contains(&opportunity.sell.descriptor.address)
        {
            return FilterDecision::Reject(RejectReason::PoolBlacklisted {
                pool: opportunity.sell.descriptor.address.to_string(),
            });
        }

        // 6. 检查冷却状态
        let cooldown_duration = Duration::from_secs(self.config.cooldown_duration_secs);
        for pool_addr in [
            &opportunity.buy.descriptor.address,
            &opportunity.sell.descriptor.address,
        ] {
            if let Some(mut state) = self.pool_states.get_mut(pool_addr) {
                if state.is_in_cooldown(self.config.cooldown_failure_threshold, cooldown_duration) {
                    let remaining = state
                        .cooldown_remaining(cooldown_duration)
                        .map(|d| d.as_secs())
                        .unwrap_or(0);
                    let log_once = !state.cooldown_logged;
                    if log_once {
                        state.cooldown_logged = true;
                    }
                    return FilterDecision::Reject(RejectReason::PoolInCooldown {
                        pool: pool_addr.to_string(),
                        remaining_secs: remaining,
                        log_once,
                    });
                }
                if state.cooldown_logged {
                    state.cooldown_logged = false;
                }
            }
        }

        // 7. 检查活跃 job 数量
        for pool_addr in [
            &opportunity.buy.descriptor.address,
            &opportunity.sell.descriptor.address,
        ] {
            let state = self.pool_states.entry(*pool_addr).or_default();
            if state.active_jobs >= self.config.max_active_jobs_per_pool {
                return FilterDecision::Reject(RejectReason::TooManyActiveJobs {
                    pool: pool_addr.to_string(),
                    current: state.active_jobs,
                    max: self.config.max_active_jobs_per_pool,
                });
            }
        }

        FilterDecision::Accept
    }

    /// 通知 job 开始（增加活跃计数）
    pub fn on_job_started(&self, buy_pool: &Pubkey, sell_pool: &Pubkey) {
        for pool in [buy_pool, sell_pool] {
            self.pool_states
                .entry(*pool)
                .and_modify(|state| {
                    state.active_jobs += 1;
                })
                .or_insert_with(|| {
                    let mut state = PoolState::default();
                    state.active_jobs = 1;
                    state
                });
        }
        debug!(
            "Job started for pools {} and {} (active jobs incremented)",
            buy_pool, sell_pool
        );
    }

    /// 通知 job 成功（减少活跃计数，重置失败计数）
    pub fn on_job_succeeded(&self, buy_pool: &Pubkey, sell_pool: &Pubkey) {
        for pool in [buy_pool, sell_pool] {
            if let Some(mut state) = self.pool_states.get_mut(pool) {
                if state.active_jobs > 0 {
                    state.active_jobs -= 1;
                }
                // 成功则重置失败计数
                if state.consecutive_failures > 0 {
                    info!(
                        "Pool {} recovered after {} failures",
                        pool, state.consecutive_failures
                    );
                    state.consecutive_failures = 0;
                    state.last_failure_time = None;
                    state.cooldown_logged = false;
                }
            }
        }
        debug!("Job succeeded for pools {} and {}", buy_pool, sell_pool);
    }

    /// 通知 job 失败（减少活跃计数，增加失败计数，可能进入冷却）
    pub fn on_job_failed(&self, buy_pool: &Pubkey, sell_pool: &Pubkey) {
        let cooldown_duration = Duration::from_secs(self.config.cooldown_duration_secs);
        for pool in [buy_pool, sell_pool] {
            let mut state = self.pool_states.entry(*pool).or_default();
            if state.active_jobs > 0 {
                state.active_jobs -= 1;
            }
            let was_in_cooldown =
                state.is_in_cooldown(self.config.cooldown_failure_threshold, cooldown_duration);
            if !was_in_cooldown {
                state.cooldown_logged = false;
            }
            state.consecutive_failures += 1;
            state.last_failure_time = Some(Instant::now());

            let now_in_cooldown =
                state.is_in_cooldown(self.config.cooldown_failure_threshold, cooldown_duration);
            if now_in_cooldown && !state.cooldown_logged {
                warn!(
                    "⚠️  Pool {} entered cooldown after {} consecutive failures (cooldown: {}s)",
                    pool, state.consecutive_failures, self.config.cooldown_duration_secs
                );
                state.cooldown_logged = true;
            } else {
                debug!(
                    "Pool {} failure recorded ({}/{})",
                    pool, state.consecutive_failures, self.config.cooldown_failure_threshold
                );
            }
        }
    }

    /// 获取池子统计信息（用于调试）
    pub fn pool_stats(&self, pool: &Pubkey) -> Option<(usize, usize, Option<u64>)> {
        self.pool_states.get(pool).map(|state| {
            let cooldown_remaining = state
                .cooldown_remaining(Duration::from_secs(self.config.cooldown_duration_secs))
                .map(|d| d.as_secs());
            (
                state.active_jobs,
                state.consecutive_failures,
                cooldown_remaining,
            )
        })
    }
}

/// 估算 tip 在 quote token 中的成本
/// 简化版本：假设 SOL = quote，或者忽略 tip（如果 quote 不是 SOL）
fn lamports_to_quote(lamports: u64, _trade: &crate::dex::TradeEstimates) -> f64 {
    // 简化：假设 1 SOL = 1 quote token 单位（或者需要从 trade 中获取 SOL 价格）
    // 这里只是示例，实际应用可能需要根据 quote mint 判断
    // 如果 quote 是 SOL，则直接转换；否则可能需要查询 SOL 价格
    const LAMPORTS_PER_SOL: f64 = 1_000_000_000.0;
    lamports as f64 / LAMPORTS_PER_SOL
}

