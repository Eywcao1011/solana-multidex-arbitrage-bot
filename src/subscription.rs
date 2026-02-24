use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use dashmap::DashMap;
use futures::StreamExt;
use log::{debug, info, warn, error};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::pubsub_client::PubsubClient, rpc_config::RpcAccountInfoConfig,
    rpc_response::Response as RpcResponse,
};
use solana_commitment_config::CommitmentConfig;
use solana_pubkey::Pubkey;
use tokio::sync::{broadcast, mpsc, Mutex, RwLock};
use tokio::time::sleep;

/// 后台任务心跳超时（秒）
/// 如果超过这个时间没有心跳更新，认为后台任务已死
const HEARTBEAT_TIMEOUT_SECS: u64 = 30;

/// 订阅请求间隔（毫秒）
///
/// ✅ 使用共享 WebSocket 连接后，仍保持限流避免瞬间订阅过多账户
/// 调大到 300ms 以减少触发 provider WS 限流/断连的概率
const SUBSCRIPTION_INTERVAL_MS: u64 = 300;

/// Unsubscribe 操作超时（秒）
///
/// 防止 WS 连接处于坏状态时 unsubscribe future 永远卡住，导致 handler 死锁
const UNSUBSCRIBE_TIMEOUT_SECS: u64 = 5;
/// WebSocket 连接建立超时（秒）
///
/// 防止 PubsubClient::new() 在网络异常时无限期挂起
const WS_CONNECT_TIMEOUT_SECS: u64 = 10;

/// 每个 WebSocket 连接最大订阅数（默认 30）
/// 可通过环境变量 WS_MAX_SUBS_PER_CONNECTION 覆盖
fn max_subs_per_connection() -> usize {
    std::env::var("WS_MAX_SUBS_PER_CONNECTION")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30)
}

/// 全局订阅限流器
///
/// 确保所有 WebSocket 订阅请求（包括首次订阅和重连）都通过限流器，
/// 避免同时监控多个池子时瞬间发出大量订阅请求触发 rate limit。
#[derive(Debug)]
struct SubscriptionRateLimiter {
    /// 最后一次允许订阅的时间
    last_subscription_time: Mutex<Option<Instant>>,
    /// 最小订阅间隔
    min_interval: Duration,
}

impl SubscriptionRateLimiter {
    fn new(interval_ms: u64) -> Self {
        Self {
            last_subscription_time: Mutex::new(None),
            min_interval: Duration::from_millis(interval_ms),
        }
    }

    /// 等待直到允许下一次订阅
    ///
    /// 如果距离上次订阅不足 min_interval，则 sleep 等待。
    /// 返回后更新 last_subscription_time。
    async fn acquire(&self, _account: &Pubkey) {
        loop {
            let mut last_time = self.last_subscription_time.lock().await;
            let now = Instant::now();

            if let Some(last) = *last_time {
                let elapsed = now.duration_since(last);
                if elapsed < self.min_interval {
                    let wait_duration = self.min_interval - elapsed;
                    drop(last_time);
                    sleep(wait_duration).await;
                    continue;
                }
            }

            *last_time = Some(now);
            break;
        }
    }
}

/// 全局限流器实例（懒加载单例）
static RATE_LIMITER: once_cell::sync::Lazy<Arc<SubscriptionRateLimiter>> =
    once_cell::sync::Lazy::new(|| Arc::new(SubscriptionRateLimiter::new(SUBSCRIPTION_INTERVAL_MS)));

/// 缓存的账户数据（带 slot 验证）
#[derive(Debug, Clone)]
pub struct CachedAccount {
    pub data: Vec<u8>,
    pub slot: u64,
    pub cached_at: Instant,
}

/// 账户更新事件（用于广播给订阅者）
#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub account: Pubkey,
    pub data: Vec<u8>,
    pub slot: u64,
}

impl CachedAccount {
    pub fn new(data: Vec<u8>, slot: u64) -> Self {
        Self {
            data,
            slot,
            cached_at: Instant::now(),
        }
    }
}

/// 订阅状态（不再需要独立任务句柄）
struct SubscriptionState {
    /// 缓存的账户数据
    cached: Option<CachedAccount>,
    /// 引用计数（有多少个池子依赖此账户）
    ref_count: usize,
}

/// 订阅命令（通过 channel 发送到后台任务）
enum SubscriptionCommand {
    Subscribe(Pubkey),
    Unsubscribe(Pubkey),
}

/// 单个 WebSocket 连接的信息
struct ConnectionInfo {
    /// 连接 ID
    id: usize,
    /// 命令发送器
    command_tx: mpsc::UnboundedSender<SubscriptionCommand>,
    /// 当前订阅数
    sub_count: Arc<AtomicUsize>,
    /// 此连接管理的账户集合
    accounts: Arc<RwLock<std::collections::HashSet<Pubkey>>>,
    /// 后台任务句柄
    _task_handle: tokio::task::JoinHandle<()>,
    /// 心跳时间戳
    heartbeat: Arc<AtomicU64>,
}

/// 账户订阅管理器
///
/// ✅ 多连接架构：使用连接池管理多个 WebSocket 连接
/// 每个连接最多 30 个订阅，减少单连接负载
pub struct AccountSubscriptionManager {
    ws_url: String,
    commitment: CommitmentConfig,
    /// 订阅状态 Map：Pubkey -> SubscriptionState
    subscriptions: Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
    /// 账户到连接 ID 的映射
    account_to_conn: Arc<DashMap<Pubkey, usize>>,
    /// 连接池
    connections: Arc<RwLock<Vec<ConnectionInfo>>>,
    /// 全局关闭信号
    shutdown: Arc<RwLock<bool>>,
    /// 更新广播发送器
    update_tx: broadcast::Sender<AccountUpdate>,
}

impl AccountSubscriptionManager {
    pub fn new(ws_url: String, commitment: CommitmentConfig) -> Self {
        let subscriptions = Arc::new(DashMap::new());
        let account_to_conn = Arc::new(DashMap::new());
        let connections = Arc::new(RwLock::new(Vec::new()));
        let shutdown = Arc::new(RwLock::new(false));

        // 创建更新广播 channel（容量 1024，满了会丢弃旧消息）
        let (update_tx, _) = broadcast::channel(1024);

        info!(
            "[WS-POOL] AccountSubscriptionManager initialized (max {} subs/connection)",
            max_subs_per_connection()
        );

        Self {
            ws_url,
            commitment,
            subscriptions,
            account_to_conn,
            connections,
            shutdown,
            update_tx,
        }
    }

    /// 获取更新接收器（用于主池订阅）
    pub fn subscribe_updates(&self) -> broadcast::Receiver<AccountUpdate> {
        self.update_tx.subscribe()
    }

    /// 检查连接池是否健康
    pub fn is_healthy(&self) -> bool {
        let connections = match self.connections.try_read() {
            Ok(c) => c,
            Err(_) => return true,
        };
        
        if connections.is_empty() {
            return true; // 空池允许创建
        }
        
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        for conn in connections.iter() {
            let elapsed = now_secs.saturating_sub(conn.heartbeat.load(Ordering::SeqCst));
            if elapsed <= HEARTBEAT_TIMEOUT_SECS {
                return true;
            }
        }
        
        error!("[WS-POOL] All connections appear dead!");
        false
    }

    /// 获取或创建一个有空位的连接
    async fn get_or_create_connection(&self) -> Result<(usize, mpsc::UnboundedSender<SubscriptionCommand>)> {
        let max_subs = max_subs_per_connection();
        
        // 查找有空位的现有连接
        {
            let connections = self.connections.read().await;
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
                
            for conn in connections.iter() {
                // 检查是否已满且健康
                let is_healthy = now_secs.saturating_sub(conn.heartbeat.load(Ordering::SeqCst)) <= HEARTBEAT_TIMEOUT_SECS;
                if is_healthy && conn.sub_count.load(Ordering::SeqCst) < max_subs {
                    return Ok((conn.id, conn.command_tx.clone()));
                }
            }
        }
        
        // 没有空位，创建新连接
        let mut connections = self.connections.write().await;
        let now_secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // 双重检查
        for conn in connections.iter() {
            let is_healthy = now_secs.saturating_sub(conn.heartbeat.load(Ordering::SeqCst)) <= HEARTBEAT_TIMEOUT_SECS;
            if is_healthy && conn.sub_count.load(Ordering::SeqCst) < max_subs {
                return Ok((conn.id, conn.command_tx.clone()));
            }
        }
        
        // 创建新连接
        let conn_id = connections.len();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let sub_count = Arc::new(AtomicUsize::new(0));
        let accounts = Arc::new(RwLock::new(std::collections::HashSet::new()));
        let heartbeat = Arc::new(AtomicU64::new(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        ));
        
        let ws_url = self.ws_url.clone();
        let commitment = self.commitment;
        let subscriptions = Arc::clone(&self.subscriptions);
        let shutdown = Arc::clone(&self.shutdown);
        let update_tx = self.update_tx.clone();
        let heartbeat_clone = Arc::clone(&heartbeat);
        let accounts_clone = Arc::clone(&accounts);
        
        let task_handle = tokio::spawn(async move {
            Self::run_connection_manager(
                conn_id,
                ws_url,
                commitment,
                subscriptions,
                shutdown,
                command_rx,
                update_tx,
                heartbeat_clone,
                accounts_clone,
            )
            .await;
        });
        
        info!("[WS-POOL] Created connection {} (total: {})", conn_id, conn_id + 1);
        
        connections.push(ConnectionInfo {
            id: conn_id,
            command_tx: command_tx.clone(),
            sub_count,
            accounts,
            _task_handle: task_handle,
            heartbeat,
        });
        
        Ok((conn_id, command_tx))
    }

    /// 确保账户已订阅
    pub async fn ensure_subscription(&self, account: Pubkey) -> Result<()> {
        let entry = self.subscriptions.entry(account).or_insert_with(|| {
            Arc::new(RwLock::new(SubscriptionState {
                cached: None,
                ref_count: 0,
            }))
        });
        let state_lock = Arc::clone(entry.value());
        drop(entry);

        let should_subscribe = {
            let mut state = state_lock.write().await;
            if state.ref_count == 0 {
                state.ref_count = 1;
                true
            } else {
                state.ref_count += 1;
                false
            }
        };

        if should_subscribe {
            let (conn_id, command_tx) = self.get_or_create_connection().await?;
            self.account_to_conn.insert(account, conn_id);

            let (conn_sub_count, conn_accounts) = {
                let connections = self.connections.read().await;
                if let Some(conn) = connections.get(conn_id) {
                    (
                        Some(Arc::clone(&conn.sub_count)),
                        Some(Arc::clone(&conn.accounts)),
                    )
                } else {
                    (None, None)
                }
            };

            if let (Some(sub_count), Some(accounts)) =
                (conn_sub_count.as_ref(), conn_accounts.as_ref())
            {
                sub_count.fetch_add(1, Ordering::SeqCst);
                accounts.write().await.insert(account);
            }

            if let Err(_) = command_tx.send(SubscriptionCommand::Subscribe(account)) {
                // 回滚：避免出现 ref_count>0 但实际上未订阅成功，导致后续无法再次订阅
                {
                    let mut state = state_lock.write().await;
                    state.ref_count = 0;
                }

                self.account_to_conn.remove(&account);
                if let (Some(sub_count), Some(accounts)) =
                    (conn_sub_count.as_ref(), conn_accounts.as_ref())
                {
                    sub_count.fetch_sub(1, Ordering::SeqCst);
                    accounts.write().await.remove(&account);
                }

                return Err(anyhow::anyhow!("Failed to send subscribe command"));
            }
                
            debug!("[WS-POOL] Assigned {} to connection {}", account, conn_id);
        }

        Ok(())
    }

    /// 减少引用计数，如果降到 0 则取消订阅
    pub async fn release_subscription(&self, account: &Pubkey) {
        let should_unsubscribe = {
            let state_lock = match self.subscriptions.get(account) {
                Some(entry) => Arc::clone(entry.value()),
                None => return,
            };
            let mut state = state_lock.write().await;
                if state.ref_count > 0 {
                    state.ref_count -= 1;
                    state.ref_count == 0
                } else {
                    false
                }
        };

        if should_unsubscribe {
            if let Some((_, conn_id)) = self.account_to_conn.remove(account) {
                let (command_tx, sub_count, accounts) = {
                    let connections = self.connections.read().await;
                    if let Some(conn) = connections.get(conn_id) {
                        (
                            Some(conn.command_tx.clone()),
                            Some(Arc::clone(&conn.sub_count)),
                            Some(Arc::clone(&conn.accounts)),
                        )
                    } else {
                        (None, None, None)
                    }
                };

                if let (Some(sub_count), Some(accounts)) =
                    (sub_count.as_ref(), accounts.as_ref())
                {
                    sub_count.fetch_sub(1, Ordering::SeqCst);
                    accounts.write().await.remove(account);
                }

                if let Some(command_tx) = command_tx {
                    let _ = command_tx.send(SubscriptionCommand::Unsubscribe(*account));
                    debug!("[WS-POOL] Unsubscribed {} from connection {}", account, conn_id);
                }
            }
            self.subscriptions.remove(account);
        }
    }

    /// 从缓存中获取账户数据
    ///
    /// 如果缓存不存在或 slot 太旧则返回 None
    pub async fn get_cached(&self, account: &Pubkey, min_slot: u64) -> Option<CachedAccount> {
        let state_lock = self.subscriptions.get(account).map(|entry| Arc::clone(entry.value()))?;
        let state = state_lock.read().await;

        let cached = state.cached.as_ref()?;

        // 验证 slot 是否满足要求
        if cached.slot >= min_slot {
            Some(cached.clone())
        } else {
            debug!(
                "Cached account {} slot {} < min_slot {}",
                account, cached.slot, min_slot
            );
            None
        }
    }

    /// 同步从缓存中获取账户数据（不要求 slot 一致性）
    ///
    /// 用于 spot_only 模式下的零延迟读取
    /// 如果缓存不存在则返回 None
    pub fn get_cached_sync(&self, account: &Pubkey) -> Option<CachedAccount> {
        let state_lock = self.subscriptions.get(account)?;
        // 使用 try_read 避免阻塞
        let state = state_lock.try_read().ok()?;
        state.cached.clone()
    }

    /// 预填充缓存（用于 warmup 阶段通过 HTTP 获取 vault 数据）
    ///
    /// 解决时序问题：WS 订阅后可能还没收到数据，但 decode 需要 vault 余额
    /// 通过 HTTP 预取并写入缓存，确保 decode 能立即读取到数据
    pub async fn prefill_cache(&self, account: &Pubkey, data: Vec<u8>, slot: u64) {
        let entry = self.subscriptions
            .entry(*account)
            .or_insert_with(|| Arc::new(RwLock::new(SubscriptionState {
                cached: None,
                ref_count: 0,
            })));
        let state_lock = Arc::clone(entry.value());
        drop(entry);
        let mut state = state_lock.write().await;
        // 只有当缓存为空或新数据 slot 更新时才更新
        let should_update = match &state.cached {
            None => true,
            Some(cached) => slot > cached.slot,
        };
        
        if should_update {
            state.cached = Some(CachedAccount::new(data, slot));
            debug!("[WS-POOL] Prefilled cache for {} at slot {}", account, slot);
        }
    }

    /// 获取订阅统计信息
    pub fn stats(&self) -> SubscriptionStats {
        SubscriptionStats {
            total_subscriptions: self.subscriptions.len(),
        }
    }

    /// 优雅关闭所有订阅
    pub async fn shutdown(&self) {
        info!("[WS-POOL] Shutting down AccountSubscriptionManager...");
        *self.shutdown.write().await = true;

        self.subscriptions.clear();
        self.account_to_conn.clear();

        info!("[WS-POOL] AccountSubscriptionManager shutdown complete");
    }

    /// ✅ 单个连接的管理器
    async fn run_connection_manager(
        conn_id: usize,
        ws_url: String,
        commitment: CommitmentConfig,
        subscriptions: Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
        shutdown: Arc<RwLock<bool>>,
        mut command_rx: mpsc::UnboundedReceiver<SubscriptionCommand>,
        update_tx: broadcast::Sender<AccountUpdate>,
        heartbeat: Arc<AtomicU64>,
        accounts: Arc<RwLock<std::collections::HashSet<Pubkey>>>,
    ) {
        loop {
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            heartbeat.store(now_secs, Ordering::SeqCst);

            if *shutdown.read().await {
                info!("[WS-POOL][C{}] Shutting down", conn_id);
                break;
            }

            info!("[WS-POOL][C{}] Connecting to {}", conn_id, ws_url);
            let client = match tokio::time::timeout(
                Duration::from_secs(WS_CONNECT_TIMEOUT_SECS),
                PubsubClient::new(&ws_url),
            )
            .await
            {
                Ok(Ok(c)) => {
                    info!("[WS-POOL][C{}] Connected", conn_id);
                    c
                }
                Ok(Err(err)) => {
                    warn!("[WS-POOL][C{}] Connect failed: {:?}", conn_id, err);
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
                Err(_) => {
                    warn!(
                        "[WS-POOL][C{}] Connect timed out after {}s",
                        conn_id, WS_CONNECT_TIMEOUT_SECS
                    );
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    continue;
                }
            };

            // 恢复此连接的订阅
            let accounts_to_restore: Vec<Pubkey> = accounts.read().await.iter().cloned().collect();

            if !accounts_to_restore.is_empty() {
                info!("[WS-POOL][C{}] Restoring {} subscriptions", conn_id, accounts_to_restore.len());
            }

            if let Err(err) = Self::handle_subscriptions_incremental(
                conn_id,
                client,
                commitment,
                &subscriptions,
                &shutdown,
                &mut command_rx,
                accounts_to_restore,
                &update_tx,
                &heartbeat,
            )
            .await
            {
                warn!("[WS-POOL][C{}] Handler error: {:?}", conn_id, err);
            }

            if *shutdown.read().await {
                break;
            }
            info!("[WS-POOL][C{}] Reconnecting in 1s...", conn_id);
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
        
        error!("[WS-POOL][C{}] Exiting!", conn_id);
    }

    /// ✅ 增量式处理订阅命令和账户更新
    async fn handle_subscriptions_incremental(
        conn_id: usize,
        client: PubsubClient,
        commitment: CommitmentConfig,
        subscriptions: &Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
        shutdown: &Arc<RwLock<bool>>,
        command_rx: &mut mpsc::UnboundedReceiver<SubscriptionCommand>,
        accounts_to_restore: Vec<Pubkey>,
        update_tx: &broadcast::Sender<AccountUpdate>,
        heartbeat: &Arc<AtomicU64>,
    ) -> Result<()> {
        type UnsubscribeFn = Box<
            dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send,
        >;
        let mut active_subs: HashMap<
            Pubkey,
            (
                futures::stream::BoxStream<'_, RpcResponse<solana_account_decoder::UiAccount>>,
                UnsubscribeFn,
            ),
        > = HashMap::new();

        let mut retry_queue: HashMap<Pubkey, (usize, Instant)> = HashMap::new();
        const MAX_RETRIES: usize = 5;
        const RETRY_DELAY: Duration = Duration::from_secs(2);

        let mut consecutive_failures: usize = 0;
        const RECONNECT_THRESHOLD: usize = 3;

        info!("[WS-POOL][C{}] Handler started", conn_id);

        // 恢复订阅
        if !accounts_to_restore.is_empty() {
            info!("[WS-POOL][C{}] Restoring {} subscriptions", conn_id, accounts_to_restore.len());
            for account in accounts_to_restore {
                RATE_LIMITER.acquire(&account).await;

                let config = RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    commitment: Some(commitment),
                    ..RpcAccountInfoConfig::default()
                };

                match client.account_subscribe(&account, Some(config)).await {
                    Ok((stream, unsubscribe)) => {
                        debug!("[WS-POOL][C{}] Restored: {}", conn_id, account);
                        active_subs.insert(
                            account,
                            (stream.boxed(), Box::new(move || Box::pin(unsubscribe()))),
                        );
                    }
                    Err(err) => {
                        warn!("[WS-POOL][C{}] Restore failed {}: {:?}", conn_id, account, err);
                        retry_queue.insert(account, (1, Instant::now() + RETRY_DELAY));
                    }
                }
            }
        }

        loop {
            // ✅ 更新心跳时间戳
            let now_secs = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            heartbeat.store(now_secs, Ordering::SeqCst);

            // 检查关闭信号
            if *shutdown.read().await {
                // ✅ 清理所有订阅
                info!(
                    "[WS-POOL] Cleaning up {} active subscriptions",
                    active_subs.len()
                );
                for (_, (_, unsub)) in active_subs {
                    let fut = unsub();
                    let _ = tokio::time::timeout(
                        Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                        fut,
                    ).await;
                }
                return Ok(());
            }

            // ✅ 处理重试队列
            let now = Instant::now();
            let accounts_to_retry: Vec<Pubkey> = retry_queue
                .iter()
                .filter(|(_, (_, next_retry))| now >= *next_retry)
                .map(|(acc, _)| *acc)
                .collect();

            for account in accounts_to_retry {
                if let Some((retry_count, _)) = retry_queue.remove(&account) {
                    // ✅ 超过基础重试次数后使用指数退避，永不放弃
                    let backoff = if retry_count >= MAX_RETRIES {
                        // 避免 2^n 溢出：将指数上限为 8（256s），再与 300s 取最小
                        let exp: u32 = ((retry_count - MAX_RETRIES) as u32).min(8);
                        let extra_backoff_secs = 2u64.pow(exp);
                        let extra_backoff = Duration::from_secs(extra_backoff_secs.min(300)); // 最多 5 分钟
                        warn!(
                            "[WS-POOL] Account {} exceeded {} retries (attempt {}), using exponential backoff: {:?}",
                            account, MAX_RETRIES, retry_count + 1, extra_backoff
                        );
                        extra_backoff
                    } else {
                        info!(
                            "[WS-POOL] Retrying subscription for {} (attempt {})",
                            account,
                            retry_count + 1
                        );
                        RETRY_DELAY
                    };

                    // ✅ 直接重试，不消费 command_rx
                    RATE_LIMITER.acquire(&account).await;

                    let config = RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        commitment: Some(commitment),
                        ..RpcAccountInfoConfig::default()
                    };

                    match client.account_subscribe(&account, Some(config)).await {
                        Ok((stream, unsubscribe)) => {
                            info!(
                                "[WS-POOL] ✅ Retry successful for account {} after {} attempts",
                                account,
                                retry_count + 1
                            );
                            active_subs.insert(
                                account,
                                (stream.boxed(), Box::new(move || Box::pin(unsubscribe()))),
                            );
                            // ✅ 成功后重置连续失败计数
                            consecutive_failures = 0;
                        }
                        Err(err) => {
                            warn!("[WS-POOL] ❌ Retry failed for {}: {:?}", account, err);
                            // ✅ 继续重试，使用指数退避，使用当前时间而不是循环开始的旧时间
                            retry_queue
                                .insert(account, (retry_count + 1, Instant::now() + backoff));

                            // ✅ 检测连续失败，如果达到阈值则触发重连
                            consecutive_failures += 1;
                            if consecutive_failures >= RECONNECT_THRESHOLD {
                                warn!(
                                    "[WS-POOL] 🔄 {} consecutive failures detected, triggering reconnect",
                                    consecutive_failures
                                );
                                // 清理所有订阅，让外层重建连接
                                for (_, (_, unsub)) in active_subs {
                                    let fut = unsub();
                                    let _ = tokio::time::timeout(
                                        Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                                        fut,
                                    ).await;
                                }
                                return Ok(());
                            }
                        }
                    }
                }
            }

            // ✅ 处理新的订阅/取消订阅命令（增量式）
            // 如果当前没有任何活跃订阅且没有待重试账户，阻塞等待一条命令，或在 shutdown 时提前返回
            // ✅ 添加超时分支，确保 heartbeat 定期更新
            if active_subs.is_empty() && retry_queue.is_empty() {
                tokio::select! {
                    // 优先响应关闭信号，避免卡在 recv().await
                    _ = async {
                        loop {
                            if *shutdown.read().await { break; }
                            tokio::time::sleep(Duration::from_millis(50)).await;
                        }
                    } => {
                        return Ok(());
                    }
                    // ✅ 定期超时以更新 heartbeat，防止被误判为死亡
                    _ = tokio::time::sleep(Duration::from_secs(HEARTBEAT_TIMEOUT_SECS / 2)) => {
                        continue;
                    }
                    cmd_opt = command_rx.recv() => {
                        match cmd_opt {
                            Some(cmd) => {
                                match cmd {
                                    SubscriptionCommand::Subscribe(account) => {
                                        if !active_subs.contains_key(&account) {
                                            RATE_LIMITER.acquire(&account).await;
                                            let config = RpcAccountInfoConfig {
                                                encoding: Some(UiAccountEncoding::Base64),
                                                commitment: Some(commitment),
                                                ..RpcAccountInfoConfig::default()
                                            };
                                            match client.account_subscribe(&account, Some(config)).await {
                                                Ok((stream, unsubscribe)) => {
                                                    info!("[WS-POOL] ➕ Incremental subscribe: {}", account);
                                                    active_subs.insert(account, (stream.boxed(), Box::new(move || Box::pin(unsubscribe()))));
                                                    retry_queue.remove(&account);
                                                    consecutive_failures = 0;
                                                }
                                                Err(err) => {
                                                    warn!("[WS-POOL] ❌ Failed to subscribe {}: {:?}", account, err);
                                                    retry_queue.insert(account, (1, Instant::now() + RETRY_DELAY));
                                                    consecutive_failures += 1;
                                                    if consecutive_failures >= RECONNECT_THRESHOLD {
                                                        warn!("[WS-POOL] 🔄 {} consecutive failures in blocking recv, triggering reconnect", consecutive_failures);
                                                        for (_, (_, unsub)) in active_subs {
                                                            let fut = unsub();
                                                            let _ = tokio::time::timeout(
                                                                Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                                                                fut,
                                                            ).await;
                                                        }
                                                        return Ok(());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    SubscriptionCommand::Unsubscribe(account) => {
                                        if let Some((_, unsub)) = active_subs.remove(&account) {
                                            info!("[WS-POOL] ➖ Incremental unsubscribe: {}", account);
                                            let fut = unsub();
                                            let _ = tokio::time::timeout(
                                                Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                                                fut,
                                            ).await;
                                        }
                                        retry_queue.remove(&account);
                                    }
                                }
                            }
                            None => {
                                // 发送端已关闭，结束本次 handler
                                return Ok(());
                            }
                        }
                    }
                }
            }

            while let Ok(cmd) = command_rx.try_recv() {
                match cmd {
                    SubscriptionCommand::Subscribe(account) => {
                        if active_subs.contains_key(&account) {
                            debug!("[WS-POOL] Account {} already subscribed", account);
                            continue;
                        }

                        // ✅ 增量订阅：直接在现有连接上订阅
                        RATE_LIMITER.acquire(&account).await;

                        let config = RpcAccountInfoConfig {
                            encoding: Some(UiAccountEncoding::Base64),
                            commitment: Some(commitment),
                            ..RpcAccountInfoConfig::default()
                        };

                        match client.account_subscribe(&account, Some(config)).await {
                            Ok((stream, unsubscribe)) => {
                                info!("[WS-POOL] ➕ Incremental subscribe: {}", account);
                                active_subs.insert(
                                    account,
                                    (stream.boxed(), Box::new(move || Box::pin(unsubscribe()))),
                                );
                                // 从重试队列移除（如果存在）
                                retry_queue.remove(&account);
                                // ✅ 成功后重置连续失败计数
                                consecutive_failures = 0;
                            }
                            Err(err) => {
                                warn!("[WS-POOL] ❌ Failed to subscribe {}: {:?}", account, err);
                                // ✅ 加入重试队列
                                retry_queue.insert(account, (1, Instant::now() + RETRY_DELAY));

                                // ✅ 检测连续失败，如果达到阈值则触发重连
                                consecutive_failures += 1;
                                if consecutive_failures >= RECONNECT_THRESHOLD {
                                    warn!(
                                        "[WS-POOL] 🔄 {} consecutive failures detected, triggering reconnect",
                                        consecutive_failures
                                    );
                                    // 清理所有订阅，让外层重建连接
                                    for (_, (_, unsub)) in active_subs {
                                        let fut = unsub();
                                        let _ = tokio::time::timeout(
                                            Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                                            fut,
                                        ).await;
                                    }
                                    return Ok(());
                                }
                            }
                        }
                    }
                    SubscriptionCommand::Unsubscribe(account) => {
                        // ✅ 增量取消：直接调用 unsubscribe，无需重连
                        if let Some((_, unsub)) = active_subs.remove(&account) {
                            info!("[WS-POOL] ➖ Incremental unsubscribe: {}", account);
                            let fut = unsub();
                            let _ = tokio::time::timeout(
                                Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                                fut,
                            ).await;
                        }
                        // 同时从重试队列移除
                        retry_queue.remove(&account);
                    }
                }
            }

            // ✅ 轮询所有活跃 stream（使用 timeout 避免阻塞）
            let mut any_update = false;
            let mut closed_accounts = Vec::new();

            for (account, (stream, _)) in &mut active_subs {
                match tokio::time::timeout(Duration::from_millis(1), stream.next()).await {
                    Ok(Some(response)) => {
                        Self::process_account_update(account, response, subscriptions, update_tx).await;
                        any_update = true;
                    }
                    Ok(None) => {
                        // Stream 关闭
                        warn!("[WS-POOL] Stream closed for account {}", account);
                        closed_accounts.push(*account);
                    }
                    Err(_) => {
                        // Timeout，继续下一个
                    }
                }
            }

            // 处理关闭的 stream：仅当全部关闭时才重连，否则对单个账户做增量恢复
            if !closed_accounts.is_empty() {
                let total = active_subs.len();
                if total > 0 && closed_accounts.len() >= total {
                    warn!(
                        "[WS-POOL] All {} streams closed; reconnecting connection",
                        total
                    );
                    // 清理所有订阅
                    for (_, (_, unsub)) in active_subs {
                        let fut = unsub();
                        let _ = tokio::time::timeout(
                            Duration::from_secs(UNSUBSCRIBE_TIMEOUT_SECS),
                            fut,
                        ).await;
                    }
                    return Ok(());
                } else {
                    for account in closed_accounts {
                        warn!(
                            "[WS-POOL] Stream closed for {}; scheduling resubscribe",
                            account
                        );
                        active_subs.remove(&account);
                        retry_queue.insert(account, (1, Instant::now() + RETRY_DELAY));
                    }
                }
            }

            // 如果没有任何更新，稍微等待以降低 CPU 使用
            if !any_update {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    }

    /// 处理账户更新
    async fn process_account_update(
        account: &Pubkey,
        response: RpcResponse<solana_account_decoder::UiAccount>,
        subscriptions: &Arc<DashMap<Pubkey, Arc<RwLock<SubscriptionState>>>>,
        update_tx: &broadcast::Sender<AccountUpdate>,
    ) {
        let state_lock = match subscriptions.get(account) {
            Some(entry) => Arc::clone(entry.value()),
            None => return,
        };
        {
            let RpcResponse { context, value } = response;

            if let Some(data) = value.data.decode() {
                let mut state = state_lock.write().await;
                let should_update = match &state.cached {
                    None => true,
                    Some(prev) => context.slot >= prev.slot,
                };

                if !should_update {
                    return;
                }

                state.cached = Some(CachedAccount::new(data.clone(), context.slot));

                debug!(
                    "[WS-POOL] Updated cache for account {} at slot {}",
                    account, context.slot
                );

                // ✅ 广播更新给所有监听者（忽略发送错误，因为可能没有接收者）
                let _ = update_tx.send(AccountUpdate {
                    account: *account,
                    data,
                    slot: context.slot,
                });
            }
        }
    }
}

#[derive(Debug)]
pub struct SubscriptionStats {
    pub total_subscriptions: usize,
}

/// 用于解码 UiAccountData
#[allow(dead_code)]
trait UiAccountDataExt {
    fn decode(&self) -> Option<Vec<u8>>;
}

impl UiAccountDataExt for solana_account_decoder::UiAccountData {
    fn decode(&self) -> Option<Vec<u8>> {
        use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};

        match self {
            solana_account_decoder::UiAccountData::Binary(data, encoding) => match encoding {
                solana_account_decoder::UiAccountEncoding::Base64 => {
                    BASE64_STANDARD.decode(data).ok()
                }
                solana_account_decoder::UiAccountEncoding::Base58 => {
                    bs58::decode(data).into_vec().ok()
                }
                _ => None,
            },
            _ => None,
        }
    }
}

// ============================================================================
// SubscriptionProvider - Unified abstraction over WS and gRPC
// ============================================================================

use crate::grpc_subscription::GrpcSubscriptionManager;

/// 订阅提供器：统一 WebSocket 和 gRPC 订阅接口
pub enum SubscriptionProvider {
    WebSocket(AccountSubscriptionManager),
    Grpc(GrpcSubscriptionManager),
}

impl SubscriptionProvider {
    /// 获取更新接收器
    pub fn subscribe_updates(&self) -> broadcast::Receiver<AccountUpdate> {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.subscribe_updates(),
            SubscriptionProvider::Grpc(mgr) => mgr.subscribe_updates(),
        }
    }

    /// 确保账户已订阅
    pub async fn ensure_subscription(&self, account: Pubkey) -> Result<()> {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.ensure_subscription(account).await,
            SubscriptionProvider::Grpc(mgr) => mgr.ensure_subscription(account).await,
        }
    }

    /// 减少引用计数
    pub async fn release_subscription(&self, account: &Pubkey) {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.release_subscription(account).await,
            SubscriptionProvider::Grpc(mgr) => mgr.release_subscription(account).await,
        }
    }

    /// 从缓存中获取账户数据
    pub async fn get_cached(&self, account: &Pubkey, min_slot: u64) -> Option<CachedAccount> {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.get_cached(account, min_slot).await,
            SubscriptionProvider::Grpc(mgr) => mgr.get_cached(account, min_slot).await,
        }
    }

    /// 同步从缓存中获取账户数据
    pub fn get_cached_sync(&self, account: &Pubkey) -> Option<CachedAccount> {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.get_cached_sync(account),
            SubscriptionProvider::Grpc(mgr) => mgr.get_cached_sync(account),
        }
    }

    /// 预填充缓存
    pub async fn prefill_cache(&self, account: &Pubkey, data: Vec<u8>, slot: u64) {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.prefill_cache(account, data, slot).await,
            SubscriptionProvider::Grpc(mgr) => mgr.prefill_cache(account, data, slot).await,
        }
    }

    /// 检查连接是否健康
    pub fn is_healthy(&self) -> bool {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.is_healthy(),
            SubscriptionProvider::Grpc(mgr) => mgr.is_healthy(),
        }
    }

    /// 优雅关闭
    pub async fn shutdown(&self) {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.shutdown().await,
            SubscriptionProvider::Grpc(mgr) => mgr.shutdown().await,
        }
    }

    /// 获取订阅统计
    pub fn total_subscriptions(&self) -> usize {
        match self {
            SubscriptionProvider::WebSocket(mgr) => mgr.stats().total_subscriptions,
            SubscriptionProvider::Grpc(mgr) => mgr.stats().total_subscriptions,
        }
    }
}

